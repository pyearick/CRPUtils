"""
Database Archaeology Script
============================
Analyzes all tables in a database to find:
1. All columns (especially date-like columns)
2. Latest dates in each date column
3. Row counts
4. Freshness assessment

This helps identify which tables are current vs stale/abandoned.

Usage:
    python db_archaeology.py

Output:
    - Console summary
    - Excel report with multiple tabs
"""

import pyodbc
import pandas as pd
from datetime import datetime, timedelta
from pathlib import Path
import warnings

warnings.filterwarnings('ignore', message='.*pandas only supports SQLAlchemy.*')

# === CONFIG ===
# Change these to point to the database you want to analyze
SQL_SERVER = r"CRPReports\BI"  # or "BI-SQL001" for local
DATABASE = "BIWarehouse"
SCHEMA = "BIData"  # Set to None to scan all schemas

OUTPUT_FILE = r"C:\Logs\DB_Archaeology_Report.xlsx"

# Date column name patterns (case-insensitive)
DATE_PATTERNS = [
    'date', 'dt', 'time', 'timestamp', 'created', 'modified', 'updated',
    'invdate', 'orderdate', 'shipdate', 'closedate', 'capturedate',
    'invyear', 'year', 'month', 'quarter', 'yyyymm', 'period',
    'snapshot', 'asof', 'effective', 'expir', 'start', 'end',
    'retrieval', 'refresh', 'load', 'etl', 'lastday', 'firstday'
]

# SQL data types that are date-related
DATE_TYPES = ['date', 'datetime', 'datetime2', 'smalldatetime', 'datetimeoffset', 'time']

# Freshness thresholds
FRESH_DAYS = 7  # Updated within last week = Fresh
STALE_DAYS = 90  # Not updated in 90+ days = Stale
DEAD_DAYS = 365  # Not updated in 1+ year = Dead

# === CONNECT ===
print(f"Connecting to {SQL_SERVER}/{DATABASE}...")
conn_str = f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={SQL_SERVER};DATABASE={DATABASE};Trusted_Connection=yes"
conn = pyodbc.connect(conn_str)
cursor = conn.cursor()

# =============================================================================
# STEP 1: Get all tables
# =============================================================================
print("\n[1] Getting table list...")

if SCHEMA:
    table_query = f"""
    SELECT TABLE_SCHEMA, TABLE_NAME 
    FROM INFORMATION_SCHEMA.TABLES 
    WHERE TABLE_TYPE = 'BASE TABLE' AND TABLE_SCHEMA = '{SCHEMA}'
    ORDER BY TABLE_SCHEMA, TABLE_NAME
    """
else:
    table_query = """
    SELECT TABLE_SCHEMA, TABLE_NAME 
    FROM INFORMATION_SCHEMA.TABLES 
    WHERE TABLE_TYPE = 'BASE TABLE'
    ORDER BY TABLE_SCHEMA, TABLE_NAME
    """

df_tables = pd.read_sql(table_query, conn)
print(f"  Found {len(df_tables)} tables")

# =============================================================================
# STEP 2: Get columns for each table and identify date columns
# =============================================================================
print("\n[2] Analyzing table columns...")

all_columns = []
date_columns = []

for idx, row in df_tables.iterrows():
    schema = row['TABLE_SCHEMA']
    table = row['TABLE_NAME']

    col_query = f"""
    SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE, ORDINAL_POSITION
    FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_SCHEMA = '{schema}' AND TABLE_NAME = '{table}'
    ORDER BY ORDINAL_POSITION
    """

    df_cols = pd.read_sql(col_query, conn)

    for _, col_row in df_cols.iterrows():
        col_name = col_row['COLUMN_NAME']
        data_type = col_row['DATA_TYPE']

        all_columns.append({
            'Schema': schema,
            'Table': table,
            'Column': col_name,
            'DataType': data_type,
            'Position': col_row['ORDINAL_POSITION']
        })

        # Check if this looks like a date column
        is_date_type = data_type.lower() in DATE_TYPES
        is_date_name = any(pattern in col_name.lower() for pattern in DATE_PATTERNS)

        # Also check for year columns (int type but named Year, InvYear, etc.)
        is_year_column = ('year' in col_name.lower() and data_type.lower() in ['int', 'smallint', 'bigint'])

        if is_date_type or is_date_name or is_year_column:
            date_columns.append({
                'Schema': schema,
                'Table': table,
                'Column': col_name,
                'DataType': data_type,
                'IsDateType': is_date_type,
                'IsDateName': is_date_name,
                'IsYearColumn': is_year_column
            })

print(f"  Total columns: {len(all_columns)}")
print(f"  Date-like columns identified: {len(date_columns)}")

df_all_columns = pd.DataFrame(all_columns)
df_date_columns = pd.DataFrame(date_columns)

# =============================================================================
# STEP 3: Check freshness of each date column
# =============================================================================
print("\n[3] Checking data freshness (this may take a while)...")

freshness_results = []
errors = []

for idx, row in df_date_columns.iterrows():
    schema = row['Schema']
    table = row['Table']
    column = row['Column']
    data_type = row['DataType']
    is_year = row['IsYearColumn']

    full_table = f"[{schema}].[{table}]"

    try:
        if is_year:
            # For year columns, get max year
            query = f"SELECT MAX([{column}]) AS MaxVal, MIN([{column}]) AS MinVal, COUNT(*) AS RecordCount FROM {full_table}"
        else:
            # For date columns, get max date
            query = f"SELECT MAX([{column}]) AS MaxVal, MIN([{column}]) AS MinVal, COUNT(*) AS RecordCount FROM {full_table}"

        result = pd.read_sql(query, conn)
        max_val = result['MaxVal'].iloc[0]
        min_val = result['MinVal'].iloc[0]
        record_count = result['RecordCount'].iloc[0]

        # Calculate freshness
        freshness = 'Unknown'
        days_old = None

        if max_val is not None:
            if is_year:
                # For year columns, compare to current year
                current_year = datetime.now().year
                if isinstance(max_val, (int, float)):
                    years_old = current_year - int(max_val)
                    days_old = years_old * 365  # Approximate
                    if years_old <= 0:
                        freshness = 'Current Year'
                    elif years_old == 1:
                        freshness = 'Last Year'
                    else:
                        freshness = f'{years_old} Years Old'
            else:
                # For date columns, calculate days since max date
                if isinstance(max_val, datetime):
                    days_old = (datetime.now() - max_val).days
                elif isinstance(max_val, str):
                    try:
                        max_date = pd.to_datetime(max_val)
                        days_old = (datetime.now() - max_date).days
                    except:
                        pass

                if days_old is not None:
                    if days_old <= FRESH_DAYS:
                        freshness = 'Fresh'
                    elif days_old <= STALE_DAYS:
                        freshness = 'Recent'
                    elif days_old <= DEAD_DAYS:
                        freshness = 'Stale'
                    else:
                        freshness = 'Dead'

        freshness_results.append({
            'Schema': schema,
            'Table': table,
            'DateColumn': column,
            'DataType': data_type,
            'MinValue': str(min_val) if min_val else None,
            'MaxValue': str(max_val) if max_val else None,
            'DaysOld': days_old,
            'Freshness': freshness,
            'RecordCount': record_count
        })

        # Progress indicator
        if (idx + 1) % 10 == 0:
            print(f"    Processed {idx + 1}/{len(df_date_columns)} date columns...")

    except Exception as e:
        errors.append({
            'Schema': schema,
            'Table': table,
            'Column': column,
            'Error': str(e)
        })

print(f"  Completed. Errors: {len(errors)}")

df_freshness = pd.DataFrame(freshness_results)
df_errors = pd.DataFrame(errors) if errors else pd.DataFrame()

# =============================================================================
# STEP 4: Create table summary (best freshness per table)
# =============================================================================
print("\n[4] Creating table summary...")

if not df_freshness.empty:
    # Define freshness function
    def get_freshness(days):
        if pd.isna(days):
            return 'Unknown'
        if days <= FRESH_DAYS:
            return 'Fresh'
        elif days <= STALE_DAYS:
            return 'Recent'
        elif days <= DEAD_DAYS:
            return 'Stale'
        else:
            return 'Dead'


    # Get the "best" (most recent) date column per table
    # Use custom aggregation to avoid issues with mixed types
    table_summary_list = []

    for (schema, table), group in df_freshness.groupby(['Schema', 'Table']):
        # Get the row with minimum DaysOld (freshest)
        valid_days = group[group['DaysOld'].notna()]

        if not valid_days.empty:
            best_row = valid_days.loc[valid_days['DaysOld'].idxmin()]
            days_old = best_row['DaysOld']
            max_value = best_row['MaxValue']
            date_column = best_row['DateColumn']
        else:
            # No valid dates, just take first row
            best_row = group.iloc[0]
            days_old = None
            max_value = best_row['MaxValue']
            date_column = best_row['DateColumn']

        # Get max record count from any column
        record_count = group['RecordCount'].max()

        table_summary_list.append({
            'Schema': schema,
            'Table': table,
            'BestDateColumn': date_column,
            'MaxValue': max_value,
            'DaysOld': days_old,
            'Freshness': get_freshness(days_old),
            'RecordCount': record_count
        })

    table_summary = pd.DataFrame(table_summary_list)
    table_summary = table_summary.sort_values('DaysOld')
else:
    table_summary = pd.DataFrame()

# =============================================================================
# STEP 5: Summary statistics
# =============================================================================
print("\n" + "=" * 70)
print("DATABASE ARCHAEOLOGY SUMMARY")
print("=" * 70)
print(f"\nServer: {SQL_SERVER}")
print(f"Database: {DATABASE}")
print(f"Schema: {SCHEMA or 'All'}")
print(f"Tables analyzed: {len(df_tables)}")
print(f"Total columns: {len(df_all_columns)}")
print(f"Date-like columns: {len(df_date_columns)}")

if not df_freshness.empty:
    print(f"\nFreshness Distribution:")
    freshness_counts = df_freshness['Freshness'].value_counts()
    for status, count in freshness_counts.items():
        print(f"  {status}: {count} columns")

    if not table_summary.empty:
        print(f"\nTable Freshness (by best date column):")
        table_freshness = table_summary['Freshness'].value_counts()
        for status, count in table_freshness.items():
            print(f"  {status}: {count} tables")

        print(f"\n--- FRESH TABLES (updated within {FRESH_DAYS} days) ---")
        fresh_tables = table_summary[table_summary['Freshness'] == 'Fresh']
        for _, row in fresh_tables.head(20).iterrows():
            print(f"  {row['Schema']}.{row['Table']}: {row['MaxValue']} ({row['RecordCount']:,} rows)")

        print(f"\n--- DEAD TABLES (not updated in {DEAD_DAYS}+ days) ---")
        dead_tables = table_summary[table_summary['Freshness'] == 'Dead']
        for _, row in dead_tables.head(20).iterrows():
            print(f"  {row['Schema']}.{row['Table']}: {row['MaxValue']} ({row['RecordCount']:,} rows)")

# =============================================================================
# STEP 6: Export to Excel
# =============================================================================
print(f"\n[5] Exporting to Excel: {OUTPUT_FILE}")

with pd.ExcelWriter(OUTPUT_FILE, engine='openpyxl') as writer:
    # Summary sheet
    if not table_summary.empty:
        table_summary.to_excel(writer, sheet_name='Table Summary', index=False)

    # Freshness details
    if not df_freshness.empty:
        df_freshness.to_excel(writer, sheet_name='Date Column Freshness', index=False)

    # All columns
    df_all_columns.to_excel(writer, sheet_name='All Columns', index=False)

    # Date columns identified
    if not df_date_columns.empty:
        df_date_columns.to_excel(writer, sheet_name='Date Columns', index=False)

    # Errors
    if not df_errors.empty:
        df_errors.to_excel(writer, sheet_name='Errors', index=False)

    # Tables list
    df_tables.to_excel(writer, sheet_name='Tables', index=False)

print(f"\n✓ Done! Report saved to: {OUTPUT_FILE}")

conn.close()