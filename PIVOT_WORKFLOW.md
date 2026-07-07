# Adding native Excel PivotTables to a report (CRPUtils `excel_pivot`)

How to put a **real, interactive** Excel PivotTable into a workbook a script
produces — the way we did it for the LostSales Early Warning report
(`ComponentCoverRisk.xlsx`, LS-018).

Neither `openpyxl` nor `xlsxwriter` can create a genuine PivotTable; they only
write a static table that looks like one. A real pivot requires driving Excel
via COM, which `excel_pivot` does through `xlwings`.

## Requirements

- **Windows with Excel installed** on the machine that runs the embed step.
  This will NOT work on a headless server without Excel (e.g. BI-SQL001) — the
  functions raise a clear `RuntimeError` there instead of failing silently.
- `xlwings` + `pywin32` in the runtime venv (`pip install xlwings`).

## The two-phase workflow ("the dance")

### Phase 1 — Design (one-time, by hand + the GUI)

You build the pivot visually in Excel; the GUI reads it back and hands you the
Python spec. You do **not** hand-code `rows`/`values` from scratch.

1. Run the report so you have a real output workbook with the data sheet(s)
   (e.g. `ComponentCoverRisk.xlsx` → `At-Risk Detail`).
2. Open it in **Excel**. Build the pivot(s) you want, dragging fields into
   **Rows / Columns / Filters / Values** on the sheet you want as each pivot's
   source. **Don't forget a Value field** — a pivot with no value field
   reverse-engineers to `values=[]`, which the embed rejects. Save.
3. Run the reverse-engineer GUI:
   ```
   .venv\Scripts\python.exe PivotReverseGUI.py     # in the CRPUtils folder
   ```
   Browse to the workbook → it lists **every** pivot it finds and generates a
   spec for each. Copy the `rows` / `columns` / `filters` / `values`.
   - Reading uses `openpyxl` (no Excel needed), so this part runs anywhere.
   - It recovers **structure** (fields + aggregations) but not which *sheet* the
     data came from, and not cosmetic number formats — you supply
     `source_sheet` and `number_formats` yourself.
4. Bake the spec(s) into the report script as constants (see Phase 2).

### Phase 2 — Runtime (every run, automatic)

The script builds all its sheets with pandas/openpyxl as usual, then — as the
**final step** — stamps the native pivot(s) onto the finished workbook.

```python
from CRPUtils.excel_pivot import add_pivots_to_workbook

add_pivots_to_workbook(
    workbook_path,                 # the .xlsx the script already wrote
    pivots=[
        {
            "source_sheet": "At-Risk Detail",
            "rows": ["Severity", "VendorName", "ProdGroupID",
                     "CauseEvidence", "AllShortComponents"],
            "values": [("FGRevAtRisk$", "sum")],
            "number_formats": {"FGRevAtRisk$": "$#,##0"},
            "pivot_sheet_name": "Component Pivot",
            "pivot_table_name": "ComponentPivot",
            "after_sheet": "At-Risk Detail",
        },
        {
            "source_sheet": "PUR At-Risk Detail",
            "rows": ["Severity", "VendorName", "ProdGroupID", "CauseEvidence"],
            "values": [("RevAtRisk$", "sum")],
            "number_formats": {"RevAtRisk$": "$#,##0"},
            "pivot_sheet_name": "PUR Pivot",
            "pivot_table_name": "PURPivot",
            "after_sheet": "PUR At-Risk Detail",
        },
    ],
    strict=False,     # skip a spec whose source sheet is absent this run
    logger=log,       # optional logging.Logger; prints otherwise
)
```

For a single pivot, `add_pivot_to_workbook(path, source_sheet, rows, values, ...)`
is a thin convenience wrapper (it raises on a bad spec instead of skipping).

## The rules that matter

1. **Embed LAST.** The pivot step must run after *every* pandas/openpyxl write to
   the file. If anything reopens the workbook with openpyxl afterward
   (e.g. `pd.ExcelWriter(..., mode="a")`), the pivot cache is **stripped**. In a
   multi-part pipeline, put the embed at the end of the part that writes the file
   last. (LostSales: end of Pt04's `run()`, after Pt04 appends its PUR tabs on
   top of Pt03's sheets.)
2. **Never let openpyxl re-save a pivot-bearing file.** Beyond stripping the
   pivot, deleting a pivot sheet in openpyxl can leave an **orphaned pivot-cache
   relationship**, which makes Excel show a "we found a problem / repair" prompt
   on open. Do the hand-building and any pivot edits in Excel; do the
   reproduction via `add_pivots_to_workbook` (COM).
3. **Source sheets must start at A1.** Header in row 1, data contiguous from A1
   (the CRPAF report convention). The cache range is taken from `A1`'s
   `CurrentRegion` unless you pass an explicit `source_range="A1:AN1168"`.
4. **Value columns must be numeric** for `sum`/`average`/etc. Coerce to float
   before writing the sheet if needed.
5. **Make it best-effort in production.** Wrap the call so a host without Excel
   just logs and still ships the pivot-less (but valid) report:
   ```python
   try:
       from CRPUtils.excel_pivot import add_pivots_to_workbook
       add_pivots_to_workbook(path, specs, strict=False, logger=log)
   except Exception as e:
       log.warning(f"pivot embed skipped (report still valid): {e}")
   ```

## Two Excel/COM quirks (handled for you)

`add_pivots_to_workbook` works on a copy in a plain temp dir and copies it back,
which sidesteps both of these — you don't need to do anything:

- `SaveAs` to the same path a workbook is open from raises *"Cannot access"*.
- A bare `Save()` on a **OneDrive-synced** path can be silently redirected by
  AutoSave to the user's `Documents` folder.

## Adding pivots to an already-generated workbook (ad-hoc)

Same as Phase 1 → Phase 2, but the "report" is any workbook you already have:
open it in Excel, build one or more pivots by hand, run `PivotReverseGUI` to get
the specs, then call `add_pivots_to_workbook(path, [spec1, spec2, ...])`.

## API summary

| Function | Use |
|----------|-----|
| `add_pivots_to_workbook(path, pivots, strict=False, ...)` | Embed one or more pivots into an existing workbook, one Excel session. |
| `add_pivot_to_workbook(path, source_sheet, rows, values, ...)` | Single-pivot convenience wrapper (raises on a bad spec). |
| `build_pivot_workbook(df, output_file, rows, values, ...)` | Build a *fresh* 2-sheet (Data + Pivot) workbook from a DataFrame. |
| `describe_pivots(path)` | Read every pivot's structure from an .xlsx (openpyxl, no Excel). |
| `pivot_to_code(path)` | Emit ready-to-paste `build_pivot_workbook(...)` calls from existing pivots. |
| `PivotReverseGUI.py` | Point-and-click front end over `describe_pivots`/`pivot_to_code`. |
