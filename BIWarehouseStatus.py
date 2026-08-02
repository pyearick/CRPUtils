"""
biwarehouse_status.py - Preflight gate for replicated source data on BI-SQL001.

BIWarehouse and Pricebooks are replicated from CRPREPORTS\\BI via push SNAPSHOT
subscriptions. Snapshot replication TRUNCATEs and BULK INSERTs the destination
tables sequentially, article by article. During delivery any combination of three
states can be true across articles in the publication: already-loaded (fresh),
currently-loading (empty/partial), or not-yet-loaded (yesterday's data).

A single-table row-count check is necessary but not sufficient — a table not yet
processed will pass the check at moment T and be TRUNCATEd at moment T+1. The
gate addresses this by sampling twice with a delay and requiring identical row
counts across both samples. Stable counts means delivery is quiescent.

Authoritative source: dbo.vw_BIWarehouseReadiness on BI-SQL001/CRPAF. This
module is a Python convenience wrapper.

Usage:
    from biwarehouse_status import is_ready, get_readiness_detail

    # Scope to the specific tables a job reads (preferred): an unrelated empty
    # source (e.g. BIWarehouse.FillRate) won't block it.
    ready, reason = is_ready(required_tables=['BIWarehouse.ProductMaster',
                                              'BIWarehouse.ActiveProductVendors'])

    # Or scope by whole source DB (blocks on ANY table under that source):
    ready, reason = is_ready(required_sources=['BIWarehouse'])

    if not ready:
        logger.error(f"Source data not ready: {reason}")
        sys.exit(2)
"""

import logging
import time
import pyodbc
from typing import Tuple, List, Optional, Dict

logger = logging.getLogger(__name__)

SQL_SERVER = "BI-SQL001"
SQL_DATABASE = "CRPAF"

DEFAULT_STABILITY_SECONDS = 15


def _get_connection() -> pyodbc.Connection:
    conn_str = (
        f"DRIVER={{ODBC Driver 17 for SQL Server}};"
        f"SERVER={SQL_SERVER};DATABASE={SQL_DATABASE};Trusted_Connection=yes;"
    )
    return pyodbc.connect(conn_str)


def get_readiness_detail() -> List[Dict]:
    """
    Return per-table readiness as a list of dicts.
        [{'table': str, 'actual': int, 'expected': int, 'ready': bool}, ...]
    """
    try:
        with _get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT TableName, ActualRows, MinExpectedRows, IsReady
                FROM dbo.vw_BIWarehouseReadiness
                ORDER BY IsReady, TableName
            """)
            return [
                {
                    'table':    r[0],
                    'actual':   int(r[1]) if r[1] is not None else 0,
                    'expected': int(r[2]) if r[2] is not None else 0,
                    'ready':    bool(r[3]),
                }
                for r in cursor.fetchall()
            ]
    except Exception as e:
        logger.error(f"get_readiness_detail failed: {e}")
        return []


def _filter_by_source(detail: List[Dict], required_sources: Optional[List[str]]) -> List[Dict]:
    """Filter readiness rows to only those matching required_sources prefixes."""
    if not required_sources:
        return detail
    return [
        d for d in detail
        if any(d['table'].startswith(s + '.') for s in required_sources)
    ]


def _filter_by_table(detail: List[Dict],
                     required_tables: Optional[List[str]]) -> Tuple[List[Dict], List[str]]:
    """Filter readiness rows to those whose table name starts with one of the
    requested table prefixes (e.g. 'BIWarehouse.ProductMaster').

    Unlike _filter_by_source this matches the raw prefix (no forced '.'), so a
    caller names the exact source-qualified tables it reads and an unrelated
    empty source (e.g. FillRate) can't block it.

    The readiness view only tracks ~10 tables, so a requested table may have no
    row at all. Returns (matched_rows, untracked_prefixes) where untracked_prefixes
    are the requested prefixes that matched nothing — the caller decides what to do
    with those (here: warn and treat as un-gateable rather than block)."""
    if not required_tables:
        return detail, []
    matched = [
        d for d in detail
        if any(d['table'].startswith(t) for t in required_tables)
    ]
    untracked = [
        t for t in required_tables
        if not any(d['table'].startswith(t) for d in detail)
    ]
    return matched, untracked


def is_ready(required_sources: Optional[List[str]] = None,
             required_tables: Optional[List[str]] = None,
             stability_seconds: int = DEFAULT_STABILITY_SECONDS) -> Tuple[bool, str]:
    """
    Two-phase readiness check: row counts above threshold AND stable across samples.

    Phase 1: Sample readiness view. Abort if any required table is below threshold.
    Phase 2: Wait stability_seconds, sample again. Abort if any row count changed
             (replicated tables on BI-SQL001 are read-only on the subscriber side,
             so any change means the distribution agent is mid-delivery).

    Args:
        required_sources: Optional list of source DB prefixes to scope the check.
                          E.g. ['BIWarehouse'] excludes Pricebooks from gating.
                          None means all sources must be ready. NOTE: this scopes by
                          whole source DB, so ANY table under that source being empty
                          (e.g. BIWarehouse.FillRate) blocks the check — even a table
                          the caller never reads. Prefer required_tables for a job
                          that only touches a few tables.
        required_tables: Optional list of source-qualified table prefixes to scope the
                          check to exactly the tables this job reads, e.g.
                          ['BIWarehouse.ProductMaster', 'BIWarehouse.ActiveProductVendors'].
                          An unrelated empty source can't block it. Applied together with
                          required_sources (AND) when both are given. The readiness view
                          only tracks ~10 tables; a requested table the view doesn't track
                          is logged and skipped (un-gateable) rather than treated as failed
                          — if NONE of the requested tables are tracked, the check warns and
                          returns ready (there is nothing gate-able to check).
        stability_seconds: Delay between the two samples. Default 15s.

    Returns:
        (is_ready, reason) - reason is empty when ready, descriptive otherwise.
    """
    def _scope(detail: List[Dict]) -> List[Dict]:
        """Apply both filters; used for the second sample (no re-warning)."""
        scoped = _filter_by_source(detail, required_sources)
        scoped, _ = _filter_by_table(scoped, required_tables)
        return scoped

    # Phase 1
    first = get_readiness_detail()
    if not first:
        return False, "Readiness view returned no rows"

    # Source scoping keeps its strict semantics: a requested source with no rows blocks.
    first = _filter_by_source(first, required_sources)
    if required_sources and not first:
        return False, f"No readiness rows match required_sources={required_sources}"

    # Table scoping: warn on prefixes the view doesn't track, and if NONE of the
    # requested tables are tracked, treat as un-gateable (warn + pass) rather than
    # false-block — the whole point of table-level scoping is to not be blocked by
    # tables this job doesn't depend on.
    first, untracked = _filter_by_table(first, required_tables)
    if untracked:
        logger.warning(
            "Readiness view does not track requested table(s): %s — not gating on these.",
            ', '.join(untracked)
        )
    if required_tables and not first:
        logger.warning(
            "None of required_tables=%s are tracked by the readiness view — "
            "cannot gate, treating as ready.", required_tables
        )
        return True, ""

    if not first:
        return False, "No readiness rows to check after scoping"

    not_ready_1 = [d for d in first if not d['ready']]
    if not_ready_1:
        detail = '; '.join(
            f"{d['table']} ({d['actual']:,}/{d['expected']:,})" for d in not_ready_1
        )
        return False, f"Below threshold: {detail}"

    # Wait for stability window
    time.sleep(stability_seconds)

    # Phase 2
    second = _scope(get_readiness_detail())
    if not second:
        return False, "Readiness view returned no rows on second sample"

    not_ready_2 = [d for d in second if not d['ready']]
    if not_ready_2:
        detail = '; '.join(
            f"{d['table']} ({d['actual']:,}/{d['expected']:,})" for d in not_ready_2
        )
        return False, f"Dropped below threshold during sample: {detail}"

    # Stability comparison
    first_map  = {d['table']: d['actual'] for d in first}
    second_map = {d['table']: d['actual'] for d in second}
    changing = [
        f"{t}: {first_map[t]:,} -> {second_map[t]:,}"
        for t in first_map
        if first_map[t] != second_map[t]
    ]
    if changing:
        return False, f"Row counts changing (delivery in progress): {'; '.join(changing)}"

    return True, ""


if __name__ == "__main__":
    import sys
    logging.basicConfig(level=logging.INFO, format='%(message)s')

    print("=" * 70)
    print("BIWarehouse / Pricebooks readiness check")
    print("=" * 70)

    print(f"\nSampling row counts (twice, {DEFAULT_STABILITY_SECONDS}s apart)...")
    ready, reason = is_ready()

    print(f"\nAll sources ready: {ready}")
    if reason:
        print(f"Reason: {reason}")

    print("\nPer-source detail:")
    for d in get_readiness_detail():
        flag = "OK " if d['ready'] else "LOW"
        print(f"  [{flag}] {d['table']:<48} {d['actual']:>14,} / {d['expected']:>12,}")

    sys.exit(0 if ready else 1)