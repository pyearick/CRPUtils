"""
ebayWT_TransmissionFilters.py
Queries ebayWT for transmission filter listings and saves results to Excel.
"""

import pyodbc
import pandas as pd
import logging
import os
from datetime import datetime

# --- Logging ---
LOG_DIR = r"C:\Logs"
os.makedirs(LOG_DIR, exist_ok=True)
logging.basicConfig(
    filename=os.path.join(LOG_DIR, "ebayWT_TransmissionFilters.log"),
    level=logging.INFO,
    format="%(asctime)s  %(levelname)s  %(message)s"
)
log = logging.getLogger(__name__)

# --- Config ---
SERVER = "BI-SQL001"
DATABASE = "CRPAF"
OUTPUT_DIR = os.path.dirname(os.path.abspath(__file__))

QUERY = """
SELECT [CaptureDate]
      ,[Brand]
      ,[ebayCats]
      ,[ebayArgs]
      ,[Keywords]
      ,[ItemID]
      ,[Title]
      ,[CategoryID]
      ,[CategoryName]
      ,[InterchangePN]
      ,[OEOEMPN]
      ,[OEAN]
      ,[ListingURL]
      ,[Condition]
      ,[UnitPrice]
      ,[Seller]
      ,[Quantity]
      ,[Sold]
      ,[ImagePath]
      ,[ImageDownloaded]
      ,[CompatibilitySummary]
      ,[FitmentTableID]
  FROM [CRPAF].[dbo].[ebayWT]
  WHERE Title LIKE '%transmission%filter%'
     OR Title LIKE '%filter%transmission%'
"""


def main():
    log.info("Starting ebayWT Transmission Filters export")

    conn_str = (
        f"DRIVER={{ODBC Driver 17 for SQL Server}};"
        f"SERVER={SERVER};DATABASE={DATABASE};Trusted_Connection=yes;"
    )

    try:
        conn = pyodbc.connect(conn_str)
        log.info("Connected to %s.%s", SERVER, DATABASE)
    except Exception as e:
        log.error("Connection failed: %s", e)
        raise

    try:
        df = pd.read_sql(QUERY, conn)
        log.info("Query returned %d rows", len(df))
    finally:
        conn.close()

    if df.empty:
        log.warning("No rows returned — skipping file creation")
        print("No rows returned.")
        return

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"ebayWT_TransmissionFilters_{timestamp}.xlsx"
    filepath = os.path.join(OUTPUT_DIR, filename)

    df.to_excel(filepath, index=False, sheet_name="TransmissionFilters")
    log.info("Saved %d rows to %s", len(df), filepath)
    print(f"Saved {len(df)} rows to {filepath}")


if __name__ == "__main__":
    main()