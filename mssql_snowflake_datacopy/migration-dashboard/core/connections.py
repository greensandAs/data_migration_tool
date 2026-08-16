"""connections.py — MSSQL and Snowflake connection management."""
from __future__ import annotations

import os

import pandas as pd
import pyodbc
import snowflake.connector


def get_sf_conn():
    """Create a Snowflake connection using environment variables."""
    return snowflake.connector.connect(
        account=os.getenv("SF_ACCOUNT", ""),
        user=os.getenv("SF_USER", ""),
        password=os.getenv("SF_PASSWORD", ""),
        role=os.getenv("SF_ROLE", "ACCOUNTADMIN"),
        warehouse=os.getenv("SF_WAREHOUSE", "COMPUTE_WH"),
        database=os.getenv("SF_DATABASE", "DATA_MIGRATION"),
        schema=os.getenv("SF_SCHEMA", "CONTROL"),
    )


def sf_query(sql: str, params=None) -> pd.DataFrame:
    """Execute a Snowflake query and return results as DataFrame."""
    con = get_sf_conn()
    try:
        cur = con.cursor()
        cur.execute(sql, params or [])
        cols = [d[0] for d in cur.description] if cur.description else []
        rows = cur.fetchall()
        return pd.DataFrame(rows, columns=cols) if cols else pd.DataFrame()
    finally:
        con.close()


def sf_execute(sql: str, params=None):
    """Execute a Snowflake statement (INSERT/UPDATE/DDL)."""
    con = get_sf_conn()
    try:
        cur = con.cursor()
        cur.execute(sql, params or [])
        con.commit()
    finally:
        con.close()


def get_mssql_conn(database: str = None):
    """Create an MSSQL connection using environment variables."""
    server = os.getenv("MSSQL_SERVER", "")
    user = os.getenv("MSSQL_USER", "")
    password = os.getenv("MSSQL_PASSWORD", "")
    driver = os.getenv("MSSQL_DRIVER", "ODBC Driver 17 for SQL Server")
    db_part = f"DATABASE={database};" if database else ""
    conn_str = (
        f"DRIVER={{{driver}}};SERVER={server};{db_part}"
        f"UID={user};PWD={password};Encrypt=yes;TrustServerCertificate=yes;"
    )
    return pyodbc.connect(conn_str)


def mssql_query(database: str, sql: str, params=None) -> list:
    """Execute a query against MSSQL, return list of rows."""
    con = get_mssql_conn(database)
    try:
        cur = con.cursor()
        if params:
            cur.execute(sql, params)
        else:
            cur.execute(sql)
        return cur.fetchall()
    finally:
        con.close()


def mssql_count(database: str, schema: str, table: str, condition: str = "1=1") -> int:
    """Get row count from an MSSQL table with optional WHERE condition."""
    sql = f"SELECT COUNT(*) FROM [{schema}].[{table}] WHERE {condition}"
    rows = mssql_query(database, sql)
    return rows[0][0] if rows else 0
