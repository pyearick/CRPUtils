# CRPUtils project instructions

## Read-only data discovery — use `ro_data.py` (CRP-014)
For any read-only SQL discovery in this project, DO NOT write inline `python -c`
queries or one-off `_scratch_*.py` scripts. Instead:

1. Overwrite `ro_query.sql` with the current `SELECT` (that file is meant to be
   thrown away and rewritten each time).
2. Run the fixed, allow-listed command: `.venv\Scripts\python.exe ro_data.py`

Pick the database with a directive line in `ro_query.sql`: `-- db: CRPAF`
(default) or `-- db: BIWarehouse`. The runner enforces read-only (every
`;`-statement must start SELECT/WITH; runs inside an always-rolled-back
transaction) and reuses `database_utils` engines. This is why it's allow-listed —
one stable entrypoint means no per-query bash approval.
