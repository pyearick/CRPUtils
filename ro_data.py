"""ro_data.py - Read-Only Data Discovery Runner (CRP-014)
=========================================================

A single, stable, allow-listed entrypoint for the read-only queries Claude runs
during data discovery, so they don't each need a fresh bash approval.

Workflow
--------
1. Overwrite ``ro_query.sql`` (sibling file) with the current SELECT query.
2. Run:  ``.venv\\Scripts\\python.exe ro_data.py``
3. Read the printed table.

Never edit this file to change the query - only ``ro_query.sql`` changes. This
harness is the safety guard and is meant to stay put (that is why the query lives
in a separate file: overwriting *this* file would overwrite the guard).

Choosing the database
---------------------
Put a directive comment on any line of ``ro_query.sql``::

    -- db: BIWarehouse

Recognised values: ``CRPAF`` (default) and ``BIWarehouse``.

Read-only enforcement (defense in depth - CRP-014)
--------------------------------------------------
This runs under your own write-capable Windows login, so read-only is enforced
structurally rather than by DB permissions:

1. **Statement guard** - every ``;``-separated statement must begin with
   ``SELECT`` or ``WITH``. Blocks ``INSERT``/``UPDATE``/``DELETE``/``DROP``/
   ``MERGE``/``EXEC`` and stacked statements (``SELECT 1; DELETE ...``).
2. **Rollback backstop** - the query runs inside an explicit transaction that is
   *always* rolled back and never committed (autocommit off). Anything that
   somehow slipped past the guard is undone.
"""

import os
import re
import sys

import pandas as pd
from sqlalchemy import text

import database_utils

QUERY_FILE = "ro_query.sql"

# db directive value -> database_utils engine factory
ENGINES = {
    "CRPAF": database_utils.get_sqlalchemy_engine,
    "BIWAREHOUSE": database_utils.get_sqlalchemy_engine_BIWarehouse,
}
DEFAULT_DB = "CRPAF"

# Cap on rows printed to stdout so a broad SELECT can't flood the transcript.
MAX_DISPLAY_ROWS = 200


def _strip_for_guard(sql: str) -> str:
    """Return SQL with comments and string literals blanked out, so the
    statement-start guard isn't fooled by ``;`` inside a comment or a literal."""
    # Block comments /* ... */
    sql = re.sub(r"/\*.*?\*/", " ", sql, flags=re.DOTALL)
    # Line comments -- ... to end of line
    sql = re.sub(r"--[^\n]*", " ", sql)
    # Single-quoted string literals (handle doubled '' escapes)
    sql = re.sub(r"'(?:[^']|'')*'", "''", sql)
    return sql


def assert_read_only(sql: str) -> None:
    """Raise ValueError unless every statement is a SELECT/WITH read."""
    scrubbed = _strip_for_guard(sql)
    for raw in scrubbed.split(";"):
        stmt = raw.strip().lstrip("(").strip()  # allow leading '(' e.g. (SELECT ...)
        if not stmt:
            continue
        first = stmt.split(None, 1)[0].upper()
        if first not in ("SELECT", "WITH"):
            raise ValueError(
                f"ro_data.py is read-only: refusing a statement that starts with "
                f"'{first}'. Only SELECT / WITH queries are allowed."
            )


def parse_db(sql: str) -> str:
    """Read the ``-- db: <name>`` directive; default CRPAF. Validated."""
    m = re.search(r"--\s*db\s*:\s*([A-Za-z_]+)", sql)
    name = (m.group(1) if m else DEFAULT_DB).upper()
    if name not in ENGINES:
        raise ValueError(
            f"Unknown db directive '{name}'. Recognised: {', '.join(ENGINES)}."
        )
    return name


def run(sql: str) -> pd.DataFrame:
    assert_read_only(sql)
    db = parse_db(sql)
    engine = ENGINES[db]()
    print(f"[ro_data] db={db}")
    with engine.connect() as conn:
        trans = conn.begin()
        try:
            result = conn.execute(text(sql))
            if result.returns_rows:
                df = pd.DataFrame(result.fetchall(), columns=list(result.keys()))
            else:
                df = pd.DataFrame()
        finally:
            trans.rollback()  # never commit - read-only backstop
    return df


def _display(df: pd.DataFrame) -> None:
    pd.set_option("display.max_columns", None)
    pd.set_option("display.width", None)
    pd.set_option("display.max_colwidth", 60)
    total = len(df)
    if df.empty:
        print("(0 rows)")
        return
    shown = df.head(MAX_DISPLAY_ROWS)
    print(shown.to_string(index=False))
    if total > MAX_DISPLAY_ROWS:
        print(f"\n... {total} rows total; showing first {MAX_DISPLAY_ROWS}.")
    else:
        print(f"\n({total} rows)")


def main() -> int:
    here = os.path.dirname(os.path.abspath(__file__))
    path = os.path.join(here, QUERY_FILE)
    if not os.path.exists(path):
        print(f"No {QUERY_FILE} found next to ro_data.py. Write your SELECT there first.")
        return 1
    with open(path, "r", encoding="utf-8") as fh:
        sql = fh.read().strip()
    if not sql:
        print(f"{QUERY_FILE} is empty. Write a SELECT query there first.")
        return 1
    try:
        df = run(sql)
    except ValueError as e:
        print(f"REFUSED: {e}")
        return 2
    _display(df)
    return 0


if __name__ == "__main__":
    sys.exit(main())
