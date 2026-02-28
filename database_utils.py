# database_utils.py
import pyodbc
import urllib.parse
from sqlalchemy import create_engine, text


def get_connection():
    database_config = {
        'driver': 'SQL Server',
        'server': 'BI-SQL001',
        'database': 'CRPAF'
    }
    if 'user' in database_config and 'password' in database_config:
        connection_string = (
            f"DRIVER={database_config['driver']};"
            f"SERVER={database_config['server']};"
            f"DATABASE={database_config['database']};"
            f"UID={database_config['user']};"
            f"PWD={database_config['password']}"
        )
    else:
        # For trusted connection
        connection_string = (
            f"DRIVER={database_config['driver']};"
            f"SERVER={database_config['server']};"
            f"DATABASE={database_config['database']};"
            f"Trusted_Connection=yes"
        )

    return pyodbc.connect(connection_string)


def fetch_data(query):
    with get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(query)
        return cursor.fetchall()


def get_sqlalchemy_engine():
    params = urllib.parse.quote_plus(
        "DRIVER={ODBC Driver 17 for SQL Server};"
        "SERVER=BI-SQL001;"
        "DATABASE=CRPAF;"
        "Trusted_Connection=yes;"
    )
    return create_engine(f"mssql+pyodbc:///?odbc_connect={params}")


def get_sqlalchemy_engine_PLM():
    params = urllib.parse.quote_plus(
        "DRIVER={ODBC Driver 17 for SQL Server};"
        "SERVER=BI-SQL001;"
        "DATABASE=PLMReports;"
        "Trusted_Connection=yes;"
    )
    return create_engine(f"mssql+pyodbc:///?odbc_connect={params}")


def get_sqlalchemy_engine_BIWarehouse():
    params = urllib.parse.quote_plus(
        "DRIVER={ODBC Driver 17 for SQL Server};"
        "SERVER=BI-SQL001;"
        "DATABASE=BIWarehouse;"
        "Trusted_Connection=yes;"
    )
    return create_engine(f"mssql+pyodbc:///?odbc_connect={params}")


def get_sqlalchemy_engine_Pricebooks():
    params = urllib.parse.quote_plus(
        "DRIVER={ODBC Driver 17 for SQL Server};"
        "SERVER=BI-SQL001;"
        "DATABASE=Pricebooks;"
        "Trusted_Connection=yes;"
    )
    return create_engine(f"mssql+pyodbc:///?odbc_connect={params}")


def get_sqlalchemy_engine_CRPREPORTSPricebooks():
    params = urllib.parse.quote_plus(
        "DRIVER={ODBC Driver 17 for SQL Server};"
        "SERVER=CRPREPORTS\\BI;"
        "DATABASE=Pricebooks;"
        "Trusted_Connection=yes;"
    )
    return create_engine(f"mssql+pyodbc:///?odbc_connect={params}")


def main():
    # Test pyodbc connection
    print("Testing pyodbc get_connection()...")
    conn = None
    try:
        conn = get_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT 1")
        result = cursor.fetchone()
        print(f"get_connection() SELECT 1: {result[0]}")
    except Exception as e:
        print(f"Error testing get_connection(): {e}")
    finally:
        if conn:
            conn.close()

    # Test fetch_data
    print("\nTesting fetch_data()...")
    try:
        data = fetch_data("SELECT 1")
        print(f"fetch_data() SELECT 1: {data}")
    except Exception as e:
        print(f"Error testing fetch_data(): {e}")

    # Test SQLAlchemy engines
    engines = {
        'default': get_sqlalchemy_engine(),
        'PLM': get_sqlalchemy_engine_PLM(),
        'BIWarehouse': get_sqlalchemy_engine_BIWarehouse(),
        'Pricebooks': get_sqlalchemy_engine_Pricebooks(),
        'CRPREPORTSPricebooks': get_sqlalchemy_engine_CRPREPORTSPricebooks()
    }

    for name, engine in engines.items():
        print(f"\nTesting SQLAlchemy engine '{name}'...")
        try:
            with engine.connect() as conn_sql:
                result = conn_sql.execute(text("SELECT 1")).scalar()
                print(f"{name} engine SELECT 1: {result}")
        except Exception as e:
            print(f"Error testing {name} engine: {e}")

if __name__ == "__main__":
    main()
