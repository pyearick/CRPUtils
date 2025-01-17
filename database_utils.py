# database_utils.py
import pyodbc
import urllib.parse
from sqlalchemy import create_engine


def get_connection():
    database_config = {
        'driver': 'SQL Server',
        'server': 'BI-SQL001',
        'database': 'CRPAF'
    }
    if 'user' in database_config and 'password' in database_config:
        connection_string = f"DRIVER={database_config['driver']};SERVER={database_config['server']};DATABASE={database_config['database']};UID={database_config['user']};PWD={database_config['password']}"
    else:
        # For trusted connection
        connection_string = f"DRIVER={database_config['driver']};SERVER={database_config['server']};DATABASE={database_config['database']};Trusted_Connection=yes"

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
        "Trusted_Connection=yes;")
    engine = create_engine("mssql+pyodbc:///?odbc_connect=%s" % params)
    return engine

def get_sqlalchemy_engine_PLM():
    params = urllib.parse.quote_plus(
        "DRIVER={ODBC Driver 17 for SQL Server};"
        "SERVER=BI-SQL001;"
        "DATABASE=PLMReports;"
        "Trusted_Connection=yes;")
    engine = create_engine("mssql+pyodbc:///?odbc_connect=%s" % params)
    return engine

def get_sqlalchemy_engine_BIWarehouse():
    params = urllib.parse.quote_plus(
        "DRIVER={ODBC Driver 17 for SQL Server};"
        "SERVER=BI-SQL001;"
        "DATABASE=BIWarehouse;"
        "Trusted_Connection=yes;")
    engineBIWarehouse = create_engine("mssql+pyodbc:///?odbc_connect=%s" % params)
    return engineBIWarehouse

def get_sqlalchemy_engine_Pricebooks():
    params = urllib.parse.quote_plus(
        "DRIVER={ODBC Driver 17 for SQL Server};"
        "SERVER=BI-SQL001;"
        "DATABASE=Pricebooks;"
        "Trusted_Connection=yes;")
    enginePricebooks = create_engine("mssql+pyodbc:///?odbc_connect=%s" % params)
    return enginePricebooks
    
def get_sqlalchemy_engine_CRPREPORTSPricebooks():
    params = urllib.parse.quote_plus(
        "DRIVER={ODBC Driver 17 for SQL Server};"
        "SERVER=CRPREPORTS\BI;"
        "DATABASE=Pricebooks;"
        "Trusted_Connection=yes;")
    enginePricebooks = create_engine("mssql+pyodbc:///?odbc_connect=%s" % params)
    return enginePricebooks    