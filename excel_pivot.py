"""
CRPUtils/excel_pivot.py - Real (native) Excel PivotTables from a DataFrame
==========================================================================

Why this exists
---------------
Neither ``openpyxl`` nor ``xlsxwriter`` can create a *real*, interactive Excel
PivotTable - they can only write a pre-computed static table that looks like
one. A genuine native PivotTable requires driving Excel's COM object model.
This module wraps ``xlwings`` (which sits on ``pywin32``) to do exactly that,
in a way that is safe to call from any CRPAF script.

The key trick that makes it work: value fields MUST be added with
``PivotTable.AddDataField(...)``. Setting a field's ``Orientation`` to
``xlDataField`` (the way row/column fields are set) silently fails.

Requirements
------------
- Windows, with a licensed copy of **Excel installed on the machine running
  the script**. This will NOT work on a headless server without Excel
  (e.g. BI-SQL001). ``build_pivot_workbook`` raises a clear ``RuntimeError``
  in that case rather than failing mysteriously.
- ``xlwings`` (see requirements.txt) and ``pywin32``.

Usage
-----
    from CRPUtils.excel_pivot import build_pivot_workbook

    build_pivot_workbook(
        df,
        output_file="Sales_Pivot_Report.xlsx",
        rows=["Region", "Product"],
        values=[("Revenue", "sum"), ("Units", "sum")],
        columns=["Salesperson"],          # optional
        filters=["Date"],                 # optional (page fields)
        number_formats={"Revenue": "$#,##0.00"},
    )

Design notes for BIG data
-------------------------
- The DataFrame is written to the sheet in a single bulk COM assignment,
  not cell-by-cell.
- The PivotCache source range is computed from the DataFrame's shape instead
  of ``used_range`` (which is slow/unreliable on large sheets).
- Screen updating, auto-calculation and display-alerts are disabled while
  building, then restored.
- ``try/finally`` guarantees the invisible Excel instance is always closed
  (``quit()`` then ``kill()`` fallback) so a mid-build error cannot strand
  orphaned ``EXCEL.EXE`` processes.
"""

from __future__ import annotations

import os
import shutil
import sys
import tempfile
from typing import Iterable, Mapping, Optional, Sequence, Tuple, Union

# xlwings is imported lazily inside build_pivot_workbook so that merely
# importing this module never fails on a machine without Excel/xlwings.

# A "value" spec is either a bare field name (defaults to sum) or a
# (field, aggregation) pair.
ValueSpec = Union[str, Tuple[str, str]]

# Friendly aggregation names -> Excel xlConsolidationFunction integer codes.
# (Codes are stable Excel constants; using the ints avoids depending on the
#  exact xw.constants layout across xlwings versions.)
_AGG_FUNCS = {
    "sum": -4157,       # xlSum
    "count": -4112,     # xlCount
    "average": -4106,   # xlAverage
    "avg": -4106,       # xlAverage (alias)
    "max": -4136,       # xlMax
    "min": -4139,       # xlMin
    "product": -4149,   # xlProduct
    "count_nums": -4113,  # xlCountNums
    "countnums": -4113,   # alias
    "stdev": -4155,     # xlStDev
    "stdevp": -4156,    # xlStDevP
    "var": -4164,       # xlVar
    "varp": -4165,      # xlVarP
}

# Excel PivotField orientation codes.
_XL_ROW_FIELD = 1     # xlRowField
_XL_COLUMN_FIELD = 2  # xlColumnField
_XL_PAGE_FIELD = 3    # xlPageField (filter)

# PivotCache source type.
_XL_DATABASE = 1      # xlDatabase


def _normalize_values(values: Sequence[ValueSpec]) -> list[Tuple[str, str, int]]:
    """Turn the values spec into a list of (field, agg_name, agg_code)."""
    normalized: list[Tuple[str, str, int]] = []
    for spec in values:
        if isinstance(spec, str):
            field, agg = spec, "sum"
        else:
            field, agg = spec
        agg_key = str(agg).strip().lower()
        if agg_key not in _AGG_FUNCS:
            raise ValueError(
                f"Unknown aggregation '{agg}' for field '{field}'. "
                f"Valid options: {', '.join(sorted(_AGG_FUNCS))}"
            )
        normalized.append((field, agg_key, _AGG_FUNCS[agg_key]))
    return normalized


def _validate_fields(df_columns: Iterable[str], *field_groups: Sequence[str]) -> None:
    """Ensure every referenced field actually exists as a DataFrame column."""
    available = set(map(str, df_columns))
    missing = []
    for group in field_groups:
        for field in group:
            if str(field) not in available:
                missing.append(str(field))
    if missing:
        raise ValueError(
            "These pivot fields are not columns in the DataFrame: "
            f"{sorted(set(missing))}. Available columns: {sorted(available)}"
        )


def _populate_pivot(
    pt,
    rows: Sequence[str],
    columns: Sequence[str],
    filters: Sequence[str],
    value_specs: Sequence[Tuple[str, str, int]],
    number_formats: Mapping[str, str],
    show_row_grand: bool,
    show_col_grand: bool,
) -> None:
    """Set the row/column/filter/value fields on an existing (empty) PivotTable.

    Shared by build_pivot_workbook (new-workbook path) and add_pivot_to_workbook
    (embed-into-existing path) so both stay identical. Value fields MUST be added
    with AddDataField - setting Orientation to xlDataField silently fails.
    """
    for field in rows:
        pt.PivotFields(field).Orientation = _XL_ROW_FIELD
    for field in columns:
        pt.PivotFields(field).Orientation = _XL_COLUMN_FIELD
    for field in filters:
        pt.PivotFields(field).Orientation = _XL_PAGE_FIELD

    for field, agg_name, agg_code in value_specs:
        caption = f"{agg_name.capitalize()} of {field}"
        data_field = pt.AddDataField(pt.PivotFields(field), caption, agg_code)
        fmt = number_formats.get(field)
        if fmt:
            data_field.NumberFormat = fmt

    pt.ColumnGrand = show_col_grand
    pt.RowGrand = show_row_grand


def build_pivot_workbook(
    df,
    output_file: str,
    rows: Sequence[str],
    values: Sequence[ValueSpec],
    columns: Optional[Sequence[str]] = None,
    filters: Optional[Sequence[str]] = None,
    *,
    data_sheet_name: str = "Data",
    pivot_sheet_name: str = "Pivot",
    pivot_table_name: str = "Pivot1",
    show_row_grand: bool = True,
    show_col_grand: bool = True,
    number_formats: Optional[Mapping[str, str]] = None,
    visible: bool = False,
    logger=None,
) -> str:
    """
    Write ``df`` to a new workbook and build a real Excel PivotTable from it.

    Parameters
    ----------
    df : pandas.DataFrame
        The source data. Written to ``data_sheet_name`` without its index.
    output_file : str
        Path to the .xlsx to create (overwritten if it exists).
    rows : sequence of str
        Field names placed on the pivot's rows, in order.
    values : sequence of (str | (str, agg))
        Value/data fields. A bare field name defaults to 'sum'. Otherwise pass
        (field, agg) where agg is one of: sum, count, average, max, min,
        product, count_nums, stdev, stdevp, var, varp.
    columns : sequence of str, optional
        Field names placed on the pivot's columns.
    filters : sequence of str, optional
        Field names placed as page (filter) fields.
    data_sheet_name, pivot_sheet_name, pivot_table_name : str
        Sheet / table naming.
    show_row_grand, show_col_grand : bool
        Toggle grand totals.
    number_formats : mapping of field -> Excel format string, optional
        Applied to the corresponding value field, e.g. {"Revenue": "$#,##0.00"}.
    visible : bool
        Run Excel visibly (useful for debugging). Default False (headless).
    logger : logging.Logger, optional
        If given, progress is logged; otherwise messages are printed.

    Returns
    -------
    str
        The absolute path to the saved workbook.
    """
    log = logger.info if logger is not None else print

    if sys.platform != "win32":
        raise RuntimeError(
            "build_pivot_workbook requires Windows + Excel (COM automation). "
            f"Current platform is '{sys.platform}'."
        )

    try:
        import xlwings as xw
    except ImportError as exc:  # pragma: no cover - environment dependent
        raise RuntimeError(
            "xlwings is required to build native Excel PivotTables. "
            "Install it (pip install xlwings) on a machine that also has "
            "Excel installed."
        ) from exc

    columns = list(columns or [])
    filters = list(filters or [])
    rows = list(rows)
    number_formats = dict(number_formats or {})

    if df is None or len(df.columns) == 0:
        raise ValueError("df must be a non-empty DataFrame with named columns.")
    if not rows and not columns:
        raise ValueError("Provide at least one field in `rows` or `columns`.")
    if not values:
        raise ValueError("Provide at least one field in `values`.")

    value_specs = _normalize_values(values)
    _validate_fields(
        df.columns, rows, columns, filters, [f for f, _, _ in value_specs]
    )

    # Resolve to an absolute path: Excel's SaveAs treats a relative path as
    # relative to its own default directory, not the current working dir.
    output_file = os.path.abspath(output_file)

    n_rows, n_cols = df.shape
    log(f"Building pivot workbook: {n_rows:,} rows x {n_cols} cols -> {output_file}")

    app = None
    try:
        # Isolated, invisible Excel instance (add_book=False so we don't touch
        # any workbook the user may already have open).
        app = xw.App(visible=visible, add_book=False)
        app.display_alerts = False
        app.screen_updating = False
        try:
            app.calculation = "manual"
        except Exception:
            pass  # non-fatal; some Excel builds are picky before a book exists

        wb = app.books.add()

        # --- Data sheet: single bulk write, no index ---
        ws_data = wb.sheets[0]
        ws_data.name = data_sheet_name
        ws_data.range("A1").options(index=False).value = df
        log(f"Data written to '{data_sheet_name}' ({n_rows:,} rows)")

        # Source range computed from shape: header row + n_rows data rows.
        source_range = ws_data.range((1, 1), (n_rows + 1, n_cols))

        # --- Pivot sheet ---
        ws_pivot = wb.sheets.add(pivot_sheet_name, after=ws_data)

        pivot_cache = wb.api.PivotCaches().Create(
            SourceType=_XL_DATABASE,
            SourceData=source_range.api,
        )
        pivot_cache.CreatePivotTable(
            TableDestination=ws_pivot.range("A3").api,
            TableName=pivot_table_name,
        )
        pt = ws_pivot.api.PivotTables(pivot_table_name)

        _populate_pivot(pt, rows, columns, filters, value_specs,
                        number_formats, show_row_grand, show_col_grand)

        # Restore calculation and refresh once before saving.
        try:
            app.calculation = "automatic"
        except Exception:
            pass

        # Save (overwrite silently thanks to display_alerts=False).
        wb.save(output_file)
        saved_path = wb.fullname
        wb.close()
        log(f"PivotTable '{pivot_table_name}' created. Saved: {saved_path}")
        return saved_path

    finally:
        if app is not None:
            try:
                app.screen_updating = True
            except Exception:
                pass
            try:
                app.quit()
            except Exception:
                pass
            # Hard fallback so a wedged COM call can't strand EXCEL.EXE.
            try:
                app.kill()
            except Exception:
                pass


def _apply_pivot(wb, spec: Mapping, log) -> None:
    """Build ONE native PivotTable on an already-open workbook (``wb`` is an
    xlwings Book). ``spec`` is a mapping with the same keys as
    ``add_pivot_to_workbook``'s parameters (source_sheet, rows, values, and the
    optional columns/filters/pivot_sheet_name/pivot_table_name/source_range/
    after_sheet/replace_existing/show_row_grand/show_col_grand/number_formats).
    Raises on any problem (missing sheet, unknown field, empty values)."""
    source_sheet = spec["source_sheet"]
    rows = list(spec.get("rows") or [])
    columns = list(spec.get("columns") or [])
    filters = list(spec.get("filters") or [])
    values = spec.get("values") or []
    pivot_sheet_name = spec.get("pivot_sheet_name", "Pivot")
    pivot_table_name = spec.get("pivot_table_name", "Pivot1")
    source_range = spec.get("source_range")
    after_sheet = spec.get("after_sheet")
    replace_existing = spec.get("replace_existing", True)
    show_row_grand = spec.get("show_row_grand", True)
    show_col_grand = spec.get("show_col_grand", True)
    number_formats = dict(spec.get("number_formats") or {})

    if not rows and not columns:
        raise ValueError("Provide at least one field in `rows` or `columns`.")
    if not values:
        raise ValueError("Provide at least one field in `values`.")
    value_specs = _normalize_values(values)

    # Recompute the sheet list each call: an earlier spec in a batch may have
    # added the sheet this one wants to anchor after.
    sheet_names = [s.name for s in wb.sheets]
    if source_sheet not in sheet_names:
        raise ValueError(
            f"Source sheet '{source_sheet}' not found. "
            f"Available sheets: {sheet_names}"
        )
    ws_source = wb.sheets[source_sheet]

    if source_range:
        src_rng = ws_source.range(source_range)
    else:
        # Contiguous table anchored at A1 (header row + data). Fast and
        # reliable for a sheet that holds one table starting at A1.
        src_rng = ws_source.range("A1").current_region

    header = src_rng.rows[0].value
    if not isinstance(header, (list, tuple)):
        header = [header]
    _validate_fields(header, rows, columns, filters,
                     [f for f, _, _ in value_specs])

    if pivot_sheet_name in sheet_names:
        if replace_existing:
            wb.sheets[pivot_sheet_name].delete()
        else:
            raise ValueError(
                f"Sheet '{pivot_sheet_name}' already exists "
                f"(pass replace_existing=True to overwrite)."
            )

    anchor = after_sheet if after_sheet in sheet_names else source_sheet
    ws_pivot = wb.sheets.add(pivot_sheet_name, after=wb.sheets[anchor])

    pivot_cache = wb.api.PivotCaches().Create(
        SourceType=_XL_DATABASE,
        SourceData=src_rng.api,
    )
    pivot_cache.CreatePivotTable(
        TableDestination=ws_pivot.range("A3").api,
        TableName=pivot_table_name,
    )
    pt = ws_pivot.api.PivotTables(pivot_table_name)
    _populate_pivot(pt, rows, columns, filters, value_specs,
                    number_formats, show_row_grand, show_col_grand)
    log(f"  + pivot '{pivot_table_name}' on sheet '{pivot_sheet_name}' "
        f"(source '{source_sheet}')")


def add_pivots_to_workbook(
    workbook_path: str,
    pivots: Sequence[Mapping],
    *,
    strict: bool = False,
    visible: bool = False,
    logger=None,
) -> str:
    """
    Embed one OR MORE native Excel PivotTables into an existing workbook, in a
    single Excel session. Every other sheet (and its formatting) is preserved.

    This is the path for the common CRPAF case: a script builds a multi-sheet
    report with pandas/openpyxl, then - as a FINAL step - stamps pivot sheets on
    top of some of those sheets. It MUST run after all openpyxl writes are done:
    re-opening a pivot-bearing workbook in openpyxl (e.g. pandas ``mode='a'``)
    strips the pivot cache.

    Parameters
    ----------
    workbook_path : str
        Path to the existing .xlsx. Saved in place (all sheets preserved).
    pivots : sequence of mapping
        One spec per pivot. Each mapping's keys mirror ``add_pivot_to_workbook``:
        required ``source_sheet``, ``rows``, ``values``; optional ``columns``,
        ``filters``, ``pivot_sheet_name``, ``pivot_table_name``, ``source_range``,
        ``after_sheet``, ``replace_existing``, ``show_row_grand``,
        ``show_col_grand``, ``number_formats``. Each source sheet must have its
        header in row 1 with data starting at A1 (the CRPAF report convention).
    strict : bool
        If True, the first bad spec aborts the whole call (original workbook left
        untouched). If False (default), a bad spec - e.g. its source sheet is
        absent this run - is logged and skipped; the good pivots are still added.
    visible, logger :
        As in ``build_pivot_workbook``.

    Returns
    -------
    str
        The absolute path to the saved workbook.
    """
    log = logger.info if logger is not None else print

    if sys.platform != "win32":
        raise RuntimeError(
            "add_pivots_to_workbook requires Windows + Excel (COM automation). "
            f"Current platform is '{sys.platform}'."
        )

    try:
        import xlwings as xw
    except ImportError as exc:  # pragma: no cover - environment dependent
        raise RuntimeError(
            "xlwings is required to build native Excel PivotTables. "
            "Install it (pip install xlwings) on a machine that also has "
            "Excel installed."
        ) from exc

    pivots = list(pivots or [])
    if not pivots:
        raise ValueError("Provide at least one pivot spec in `pivots`.")

    workbook_path = os.path.abspath(workbook_path)
    if not os.path.exists(workbook_path):
        raise FileNotFoundError(f"Workbook does not exist: {workbook_path}")

    log(f"Embedding {len(pivots)} pivot(s) into {workbook_path}")

    # Work on a copy in a plain temp dir, then copy it back over the original.
    # This sidesteps two Excel/COM quirks: (1) SaveAs to the same path a workbook
    # is open from raises "Cannot access"; (2) a bare Save() on a OneDrive-synced
    # path can be redirected by AutoSave to the user's Documents folder.
    work_dir = tempfile.mkdtemp(prefix="crp_pivot_")
    work_path = os.path.join(work_dir, os.path.basename(workbook_path))
    shutil.copyfile(workbook_path, work_path)

    app = None
    try:
        app = xw.App(visible=visible, add_book=False)
        app.display_alerts = False
        app.screen_updating = False

        wb = app.books.open(work_path)
        try:
            app.calculation = "manual"
        except Exception:
            pass

        applied = 0
        for spec in pivots:
            try:
                _apply_pivot(wb, spec, log)
                applied += 1
            except Exception as e:
                if strict:
                    raise
                log(f"  ! skipped pivot for source sheet "
                    f"{spec.get('source_sheet')!r}: {e}")

        try:
            app.calculation = "automatic"
        except Exception:
            pass

        # In-place save of the temp copy (plain dir, so no AutoSave redirect),
        # then close before copying back over the original.
        wb.save()
        wb.close()

        shutil.copyfile(work_path, workbook_path)
        log(f"Embedded {applied}/{len(pivots)} pivot(s). Saved: {workbook_path}")
        return workbook_path

    finally:
        if app is not None:
            try:
                app.screen_updating = True
            except Exception:
                pass
            try:
                app.quit()
            except Exception:
                pass
            try:
                app.kill()
            except Exception:
                pass
        try:
            shutil.rmtree(work_dir, ignore_errors=True)
        except Exception:
            pass


def add_pivot_to_workbook(
    workbook_path: str,
    source_sheet: str,
    rows: Sequence[str],
    values: Sequence[ValueSpec],
    columns: Optional[Sequence[str]] = None,
    filters: Optional[Sequence[str]] = None,
    *,
    pivot_sheet_name: str = "Pivot",
    pivot_table_name: str = "Pivot1",
    source_range: Optional[str] = None,
    after_sheet: Optional[str] = None,
    replace_existing: bool = True,
    show_row_grand: bool = True,
    show_col_grand: bool = True,
    number_formats: Optional[Mapping[str, str]] = None,
    visible: bool = False,
    logger=None,
) -> str:
    """
    Embed a single native Excel PivotTable into an existing workbook. Thin
    convenience wrapper over ``add_pivots_to_workbook`` (strict=True, so a bad
    spec raises rather than being silently skipped). See that function for the
    behavioural details; parameters here map 1:1 to a single pivot spec.
    """
    spec = {
        "source_sheet": source_sheet,
        "rows": rows,
        "values": values,
        "columns": columns,
        "filters": filters,
        "pivot_sheet_name": pivot_sheet_name,
        "pivot_table_name": pivot_table_name,
        "source_range": source_range,
        "after_sheet": after_sheet,
        "replace_existing": replace_existing,
        "show_row_grand": show_row_grand,
        "show_col_grand": show_col_grand,
        "number_formats": number_formats,
    }
    return add_pivots_to_workbook(
        workbook_path, [spec], strict=True, visible=visible, logger=logger
    )


# =============================================================================
# REVERSE ENGINEERING: read an existing pivot -> regenerate the Python call
# =============================================================================
#
# This path uses openpyxl (NOT Excel/COM), so it works headless anywhere.
# The workflow: build a pivot by hand in Excel (the easy, visual part), save,
# then run describe_pivots()/pivot_to_code() to get the build_pivot_workbook()
# call that recreates it.

# openpyxl stores a value field's aggregation as a "subtotal" string; map those
# back to the friendly agg names build_pivot_workbook understands.
_SUBTOTAL_TO_AGG = {
    "sum": "sum",
    "count": "count",
    "countNums": "count_nums",
    "average": "average",
    "max": "max",
    "min": "min",
    "product": "product",
    "stdDev": "stdev",
    "stdDevp": "stdevp",
    "var": "var",
    "varp": "varp",
}


def describe_pivots(path: str) -> list[dict]:
    """
    Read every PivotTable in an .xlsx and return each one's structure.

    Uses openpyxl only (no Excel required). Returns a list of dicts, one per
    pivot, with keys: name, sheet, rows, columns, filters, values
    (list of {field, agg, caption}), show_row_grand, show_col_grand.
    """
    from openpyxl import load_workbook

    wb = load_workbook(path)
    results: list[dict] = []

    for ws in wb.worksheets:
        for pt in getattr(ws, "_pivots", None) or []:
            cache_fields = [f.name for f in pt.cache.cacheFields]

            def name_at(idx):
                # Excel uses -2 as the "values axis" placeholder; real fields
                # are non-negative indices into the cache field list.
                if idx is None or idx < 0 or idx >= len(cache_fields):
                    return None
                return cache_fields[idx]

            rows = [name_at(f.x) for f in (pt.rowFields or [])]
            columns = [name_at(f.x) for f in (pt.colFields or [])]
            filters = [name_at(f.fld) for f in (pt.pageFields or [])]

            values = []
            for d in pt.dataFields or []:
                agg = _SUBTOTAL_TO_AGG.get(d.subtotal or "sum", "sum")
                values.append(
                    {"field": name_at(d.fld), "agg": agg, "caption": d.name}
                )

            results.append(
                {
                    "name": pt.name,
                    "sheet": ws.title,
                    "rows": [r for r in rows if r],
                    "columns": [c for c in columns if c],
                    "filters": [f for f in filters if f],
                    "values": [v for v in values if v["field"]],
                    # Excel quirk: COM RowGrand (build's show_row_grand) is
                    # stored in the XML as colGrandTotals, and vice-versa.
                    # Cross them back so the round-trip stays faithful.
                    "show_row_grand": bool(pt.colGrandTotals),
                    "show_col_grand": bool(pt.rowGrandTotals),
                }
            )

    return results


def pivot_to_code(path: str, df_var: str = "df", output_file: str = "output.xlsx") -> str:
    """
    Return ready-to-paste Python that recreates the pivots in ``path`` via
    build_pivot_workbook(). Assumes the caller has a DataFrame named ``df_var``
    holding the same source columns.
    """
    pivots = describe_pivots(path)
    if not pivots:
        return f"# No PivotTables found in {path!r}"

    header = (
        "from CRPUtils.excel_pivot import build_pivot_workbook\n"
        f"\n# NOTE: assumes `{df_var}` holds the same columns as the source "
        f"data behind\n# the pivot(s) in {os.path.basename(path)!r}.\n"
        "# Field structure/aggregations round-trip exactly; cosmetic number\n"
        "# formats/styling may not be recoverable and are omitted.\n"
    )

    blocks = [header]
    for p in pivots:
        values_items = ", ".join(
            f"({v['field']!r}, {v['agg']!r})" for v in p["values"]
        )
        lines = [
            f"# Recreates PivotTable {p['name']!r} (sheet {p['sheet']!r})",
            "build_pivot_workbook(",
            f"    {df_var},",
            f"    output_file={output_file!r},",
            f"    rows={p['rows']!r},",
        ]
        if p["columns"]:
            lines.append(f"    columns={p['columns']!r},")
        if p["filters"]:
            lines.append(f"    filters={p['filters']!r},")
        lines.append(f"    values=[{values_items}],")
        if not p["show_row_grand"]:
            lines.append("    show_row_grand=False,")
        if not p["show_col_grand"]:
            lines.append("    show_col_grand=False,")
        lines.append(f"    pivot_sheet_name={p['sheet']!r},")
        lines.append(f"    pivot_table_name={p['name']!r},")
        lines.append(")")
        blocks.append("\n".join(lines))

    return "\n\n".join(blocks)


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="Build native Excel PivotTables, or reverse-engineer the "
        "Python call from an existing pivot workbook."
    )
    parser.add_argument(
        "--reverse",
        metavar="XLSX",
        help="Print the build_pivot_workbook(...) code that recreates the "
        "pivots in this workbook (uses openpyxl, no Excel needed).",
    )
    parser.add_argument(
        "--demo",
        action="store_true",
        help="Build the sample Sales_Pivot_Report.xlsx from data.csv.",
    )
    args = parser.parse_args()

    if args.reverse:
        print(pivot_to_code(args.reverse))
    elif args.demo:
        import pandas as pd

        demo_df = pd.read_csv("data.csv")
        if "Date" in demo_df.columns:
            demo_df["Date"] = pd.to_datetime(demo_df["Date"])
        saved = build_pivot_workbook(
            demo_df,
            output_file="Sales_Pivot_Report.xlsx",
            rows=["Region", "Product"],
            values=[("Revenue", "sum"), ("Units", "sum")],
            number_formats={"Revenue": "$#,##0.00"},
        )
        print(f"\nReal PivotTable created successfully!\nFile saved as: {saved}")
    else:
        parser.print_help()
