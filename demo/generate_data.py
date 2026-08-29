#!/usr/bin/env python3
"""generate_data.py -- Generate demo data SQL scripts for all 4 source databases.

Creates INSERT statements for ~1M+ rows per source with realistic e-commerce data.
Run: python demo/generate_data.py

Output:
  demo/mysql_setup.sql
  demo/mssql_setup.sql
  demo/oracle_setup.sql
  demo/teradata_setup.sql
  demo/incremental_inserts.sql
  demo/s3_test_files/orders_20260828.csv
  demo/s3_test_files/orders_20260828.parquet
"""
from __future__ import annotations

import csv
import io
import os
import random
import sys
from datetime import datetime, timedelta
from pathlib import Path

# Seed for reproducibility
random.seed(2908)

DEMO_DIR = Path(__file__).parent
BATCH = 5000  # rows per INSERT batch

# -- Fake data pools ----------------------------------------------------------
FIRST_NAMES = [
    "James", "Mary", "John", "Patricia", "Robert", "Jennifer", "Michael", "Linda",
    "David", "Elizabeth", "William", "Barbara", "Richard", "Susan", "Joseph", "Jessica",
    "Thomas", "Sarah", "Charles", "Karen", "Daniel", "Lisa", "Matthew", "Nancy",
    "Anthony", "Betty", "Mark", "Margaret", "Donald", "Sandra", "Steven", "Ashley",
    "Paul", "Kimberly", "Andrew", "Emily", "Joshua", "Donna", "Kenneth", "Michelle",
    "Kevin", "Carol", "Brian", "Amanda", "George", "Dorothy", "Timothy", "Melissa",
    "Ronald", "Deborah", "Edward", "Stephanie", "Jason", "Rebecca", "Jeffrey", "Sharon",
]
LAST_NAMES = [
    "Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller", "Davis",
    "Rodriguez", "Martinez", "Hernandez", "Lopez", "Gonzalez", "Wilson", "Anderson",
    "Thomas", "Taylor", "Moore", "Jackson", "Martin", "Lee", "Perez", "Thompson",
    "White", "Harris", "Sanchez", "Clark", "Ramirez", "Lewis", "Robinson", "Walker",
    "Young", "Allen", "King", "Wright", "Scott", "Torres", "Nguyen", "Hill", "Flores",
]
CITIES = [
    "New York", "Los Angeles", "Chicago", "Houston", "Phoenix", "Philadelphia",
    "San Antonio", "San Diego", "Dallas", "San Jose", "Austin", "Jacksonville",
    "Fort Worth", "Columbus", "Charlotte", "Indianapolis", "San Francisco",
    "Seattle", "Denver", "Nashville", "Portland", "Memphis", "Louisville",
]
STATES = [
    "NY", "CA", "IL", "TX", "AZ", "PA", "TX", "CA", "TX", "CA", "TX", "FL",
    "TX", "OH", "NC", "IN", "CA", "WA", "CO", "TN", "OR", "TN", "KY",
]
COUNTRIES = ["US", "US", "US", "US", "CA", "UK", "DE", "AU", "IN", "JP"]
CATEGORIES = ["Electronics", "Clothing", "Home", "Sports", "Books", "Toys", "Food", "Auto"]
PRODUCT_ADJS = ["Premium", "Classic", "Ultra", "Pro", "Lite", "Max", "Mini", "Eco"]
PRODUCT_NOUNS = [
    "Widget", "Gadget", "Sensor", "Module", "Kit", "Pack", "Set", "Bundle",
    "Cable", "Adapter", "Mount", "Cover", "Case", "Stand", "Holder", "Charger",
]
STATUSES = ["completed", "completed", "completed", "shipped", "processing", "cancelled"]


def rand_ts(start: datetime, end: datetime) -> datetime:
    delta = (end - start).total_seconds()
    return start + timedelta(seconds=random.random() * delta)


def fmt_ts(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%d %H:%M:%S")


def fmt_ts_oracle(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%d %H:%M:%S.%f")


# -- Generate raw data in memory ----------------------------------------------
print("Generating customers...")
DATE_START = datetime(2024, 1, 1)
DATE_END = datetime(2026, 8, 28)

customers = []
for i in range(1, 500_001):
    fn = random.choice(FIRST_NAMES)
    ln = random.choice(LAST_NAMES)
    email = f"{fn.lower()}.{ln.lower()}{i}@example.com"
    city = random.choice(CITIES)
    idx = CITIES.index(city)
    state = STATES[idx % len(STATES)]
    country = random.choice(COUNTRIES)
    created = rand_ts(DATE_START, DATE_END)
    updated = rand_ts(created, DATE_END)
    customers.append((i, fn, ln, email, city, state, country, created, updated))

print("Generating products...")
products = []
for i in range(1, 10_001):
    name = f"{random.choice(PRODUCT_ADJS)} {random.choice(PRODUCT_NOUNS)} {i}"
    cat = random.choice(CATEGORIES)
    price = round(random.uniform(5.0, 999.99), 2)
    stock = random.randint(0, 10000)
    created = rand_ts(DATE_START, DATE_END)
    updated = rand_ts(created, DATE_END)
    products.append((i, name, cat, price, stock, created, updated))

print("Generating orders (1M)...")
orders = []
for i in range(1, 1_000_001):
    cust_id = random.randint(1, 500_000)
    order_date = rand_ts(DATE_START, DATE_END)
    status = random.choice(STATUSES)
    total = round(random.uniform(10.0, 2500.0), 2)
    ship_city = random.choice(CITIES)
    updated = rand_ts(order_date, DATE_END)
    orders.append((i, cust_id, order_date, status, total, ship_city, updated))

print("Generating order_items (2M)...")
order_items = []
item_id = 0
for order_id in range(1, 1_000_001):
    n_items = random.choices([1, 2, 3, 4], weights=[40, 35, 15, 10])[0]
    for _ in range(n_items):
        item_id += 1
        prod_id = random.randint(1, 10_000)
        qty = random.randint(1, 10)
        price = round(random.uniform(5.0, 500.0), 2)
        created = orders[order_id - 1][2]
        order_items.append((item_id, order_id, prod_id, qty, price, created))
        if item_id >= 2_000_000:
            break
    if item_id >= 2_000_000:
        break

print(f"  Generated: {len(customers)} customers, {len(products)} products, "
      f"{len(orders)} orders, {len(order_items)} order_items")


# -- Helper: batch INSERT writer ----------------------------------------------
def escape_sql(val) -> str:
    if val is None:
        return "NULL"
    if isinstance(val, (int, float)):
        return str(val)
    if isinstance(val, datetime):
        return f"'{fmt_ts(val)}'"
    s = str(val).replace("'", "''")
    return f"'{s}'"


def write_inserts(f, table: str, columns: list[str], rows: list[tuple],
                  batch_size: int = BATCH, ts_formatter=None):
    """Write batched INSERT statements."""
    col_list = ", ".join(columns)
    total = len(rows)
    for start in range(0, total, batch_size):
        batch = rows[start:start + batch_size]
        f.write(f"INSERT INTO {table} ({col_list}) VALUES\n")
        for i, row in enumerate(batch):
            vals = []
            for v in row:
                if isinstance(v, datetime) and ts_formatter:
                    vals.append(ts_formatter(v))
                else:
                    vals.append(escape_sql(v))
            sep = ",\n" if i < len(batch) - 1 else ";\n"
            f.write(f"  ({', '.join(vals)}){sep}")
        f.write("\n")


# -- MySQL --------------------------------------------------------------------
print("Writing demo/mysql_setup.sql...")
with open(DEMO_DIR / "mysql_setup.sql", "w", encoding="utf-8") as f:
    f.write("-- MigrateX Demo: MySQL Source Data\n")
    f.write("-- Database: MYSQL_DEMO_2908\n")
    f.write("-- Run: mysql -u root < demo/mysql_setup.sql\n\n")
    f.write("CREATE DATABASE IF NOT EXISTS MYSQL_DEMO_2908;\n")
    f.write("USE MYSQL_DEMO_2908;\n\n")

    f.write("""CREATE TABLE IF NOT EXISTS CUSTOMERS (
    CUSTOMER_ID INT PRIMARY KEY,
    FIRST_NAME VARCHAR(50) NOT NULL,
    LAST_NAME VARCHAR(50) NOT NULL,
    EMAIL VARCHAR(120),
    CITY VARCHAR(60),
    STATE VARCHAR(10),
    COUNTRY VARCHAR(10),
    CREATED_AT DATETIME NOT NULL,
    UPDATED_AT DATETIME NOT NULL
);\n\n""")

    f.write("""CREATE TABLE IF NOT EXISTS PRODUCTS (
    PRODUCT_ID INT PRIMARY KEY,
    PRODUCT_NAME VARCHAR(100) NOT NULL,
    CATEGORY VARCHAR(30),
    PRICE DECIMAL(10,2),
    STOCK_QTY INT,
    CREATED_AT DATETIME NOT NULL,
    UPDATED_AT DATETIME NOT NULL
);\n\n""")

    f.write("""CREATE TABLE IF NOT EXISTS ORDERS (
    ORDER_ID INT PRIMARY KEY,
    CUSTOMER_ID INT NOT NULL,
    ORDER_DATE DATETIME NOT NULL,
    STATUS VARCHAR(20),
    TOTAL_AMOUNT DECIMAL(12,2),
    SHIP_CITY VARCHAR(60),
    UPDATED_AT DATETIME NOT NULL,
    INDEX idx_order_date (ORDER_DATE),
    INDEX idx_updated (UPDATED_AT)
);\n\n""")

    f.write("""CREATE TABLE IF NOT EXISTS ORDER_ITEMS (
    ITEM_ID BIGINT PRIMARY KEY,
    ORDER_ID INT NOT NULL,
    PRODUCT_ID INT NOT NULL,
    QUANTITY INT,
    UNIT_PRICE DECIMAL(10,2),
    CREATED_AT DATETIME NOT NULL,
    INDEX idx_order_id (ORDER_ID),
    INDEX idx_created (CREATED_AT)
);\n\n""")

    f.write("-- Customers (500K)\n")
    write_inserts(f, "CUSTOMERS",
                  ["CUSTOMER_ID", "FIRST_NAME", "LAST_NAME", "EMAIL", "CITY", "STATE", "COUNTRY", "CREATED_AT", "UPDATED_AT"],
                  customers)

    f.write("-- Products (10K)\n")
    write_inserts(f, "PRODUCTS",
                  ["PRODUCT_ID", "PRODUCT_NAME", "CATEGORY", "PRICE", "STOCK_QTY", "CREATED_AT", "UPDATED_AT"],
                  products)

    f.write("-- Orders (1M)\n")
    write_inserts(f, "ORDERS",
                  ["ORDER_ID", "CUSTOMER_ID", "ORDER_DATE", "STATUS", "TOTAL_AMOUNT", "SHIP_CITY", "UPDATED_AT"],
                  orders)

    f.write("-- Order Items (2M)\n")
    write_inserts(f, "ORDER_ITEMS",
                  ["ITEM_ID", "ORDER_ID", "PRODUCT_ID", "QUANTITY", "UNIT_PRICE", "CREATED_AT"],
                  order_items)

print(f"  mysql_setup.sql written")


# -- MSSQL --------------------------------------------------------------------
print("Writing demo/mssql_setup.sql...")
with open(DEMO_DIR / "mssql_setup.sql", "w", encoding="utf-8") as f:
    f.write("-- MigrateX Demo: MSSQL Source Data\n")
    f.write("-- Database: MSSQL_DEMO_2908\n")
    f.write("-- Run: sqlcmd -S server -U sa -i demo/mssql_setup.sql\n\n")
    f.write("IF NOT EXISTS (SELECT * FROM sys.databases WHERE name = 'MSSQL_DEMO_2908')\n")
    f.write("    CREATE DATABASE MSSQL_DEMO_2908;\nGO\n\n")
    f.write("USE MSSQL_DEMO_2908;\nGO\n\n")

    f.write("""IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'CUSTOMERS')
CREATE TABLE dbo.CUSTOMERS (
    CUSTOMER_ID INT PRIMARY KEY,
    FIRST_NAME NVARCHAR(50) NOT NULL,
    LAST_NAME NVARCHAR(50) NOT NULL,
    EMAIL NVARCHAR(120),
    CITY NVARCHAR(60),
    STATE NVARCHAR(10),
    COUNTRY NVARCHAR(10),
    CREATED_AT DATETIME2 NOT NULL,
    UPDATED_AT DATETIME2 NOT NULL
);\nGO\n\n""")

    f.write("""IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'PRODUCTS')
CREATE TABLE dbo.PRODUCTS (
    PRODUCT_ID INT PRIMARY KEY,
    PRODUCT_NAME NVARCHAR(100) NOT NULL,
    CATEGORY NVARCHAR(30),
    PRICE MONEY,
    STOCK_QTY INT,
    CREATED_AT DATETIME2 NOT NULL,
    UPDATED_AT DATETIME2 NOT NULL
);\nGO\n\n""")

    f.write("""IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'ORDERS')
CREATE TABLE dbo.ORDERS (
    ORDER_ID INT PRIMARY KEY,
    CUSTOMER_ID INT NOT NULL,
    ORDER_DATE DATETIME2 NOT NULL,
    STATUS NVARCHAR(20),
    TOTAL_AMOUNT MONEY,
    SHIP_CITY NVARCHAR(60),
    UPDATED_AT DATETIME2 NOT NULL
);\nGO\n\n""")

    f.write("""IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'ORDER_ITEMS')
CREATE TABLE dbo.ORDER_ITEMS (
    ITEM_ID BIGINT PRIMARY KEY,
    ORDER_ID INT NOT NULL,
    PRODUCT_ID INT NOT NULL,
    QUANTITY INT,
    UNIT_PRICE MONEY,
    CREATED_AT DATETIME2 NOT NULL
);\nGO\n\n""")

    f.write("-- Customers (500K)\n")
    write_inserts(f, "dbo.CUSTOMERS",
                  ["CUSTOMER_ID", "FIRST_NAME", "LAST_NAME", "EMAIL", "CITY", "STATE", "COUNTRY", "CREATED_AT", "UPDATED_AT"],
                  customers)

    f.write("-- Products (10K)\n")
    write_inserts(f, "dbo.PRODUCTS",
                  ["PRODUCT_ID", "PRODUCT_NAME", "CATEGORY", "PRICE", "STOCK_QTY", "CREATED_AT", "UPDATED_AT"],
                  products)

    f.write("-- Orders (1M)\n")
    write_inserts(f, "dbo.ORDERS",
                  ["ORDER_ID", "CUSTOMER_ID", "ORDER_DATE", "STATUS", "TOTAL_AMOUNT", "SHIP_CITY", "UPDATED_AT"],
                  orders)

    f.write("-- Order Items (2M)\n")
    write_inserts(f, "dbo.ORDER_ITEMS",
                  ["ITEM_ID", "ORDER_ID", "PRODUCT_ID", "QUANTITY", "UNIT_PRICE", "CREATED_AT"],
                  order_items)

print(f"  mssql_setup.sql written")


# -- Oracle -------------------------------------------------------------------
print("Writing demo/oracle_setup.sql...")

def oracle_ts(dt: datetime) -> str:
    return f"TO_TIMESTAMP('{fmt_ts_oracle(dt)}', 'YYYY-MM-DD HH24:MI:SS.FF6')"

with open(DEMO_DIR / "oracle_setup.sql", "w", encoding="utf-8") as f:
    f.write("-- MigrateX Demo: Oracle Source Data\n")
    f.write("-- Schema: ORACLE_DEMO_2908\n")
    f.write("-- Run: sqlplus sys/password@//host:1521/ORCL as sysdba @demo/oracle_setup.sql\n\n")
    f.write("-- Create user/schema\n")
    f.write("CREATE USER ORACLE_DEMO_2908 IDENTIFIED BY demo2908pwd\n")
    f.write("  DEFAULT TABLESPACE USERS QUOTA UNLIMITED ON USERS;\n")
    f.write("GRANT CONNECT, RESOURCE, CREATE SESSION TO ORACLE_DEMO_2908;\n\n")
    f.write("ALTER SESSION SET CURRENT_SCHEMA = ORACLE_DEMO_2908;\n\n")

    f.write("""CREATE TABLE CUSTOMERS (
    CUSTOMER_ID NUMBER(10) PRIMARY KEY,
    FIRST_NAME VARCHAR2(50) NOT NULL,
    LAST_NAME VARCHAR2(50) NOT NULL,
    EMAIL VARCHAR2(120),
    CITY VARCHAR2(60),
    STATE VARCHAR2(10),
    COUNTRY VARCHAR2(10),
    CREATED_AT TIMESTAMP NOT NULL,
    UPDATED_AT TIMESTAMP NOT NULL
);\n\n""")

    f.write("""CREATE TABLE PRODUCTS (
    PRODUCT_ID NUMBER(10) PRIMARY KEY,
    PRODUCT_NAME VARCHAR2(100) NOT NULL,
    CATEGORY VARCHAR2(30),
    PRICE NUMBER(10,2),
    STOCK_QTY NUMBER(10),
    CREATED_AT TIMESTAMP NOT NULL,
    UPDATED_AT TIMESTAMP NOT NULL
);\n\n""")

    f.write("""CREATE TABLE ORDERS (
    ORDER_ID NUMBER(10) PRIMARY KEY,
    CUSTOMER_ID NUMBER(10) NOT NULL,
    ORDER_DATE TIMESTAMP NOT NULL,
    STATUS VARCHAR2(20),
    TOTAL_AMOUNT NUMBER(12,2),
    SHIP_CITY VARCHAR2(60),
    UPDATED_AT TIMESTAMP NOT NULL
);\n\n""")

    f.write("""CREATE TABLE ORDER_ITEMS (
    ITEM_ID NUMBER(18) PRIMARY KEY,
    ORDER_ID NUMBER(10) NOT NULL,
    PRODUCT_ID NUMBER(10) NOT NULL,
    QUANTITY NUMBER(5),
    UNIT_PRICE NUMBER(10,2),
    CREATED_AT TIMESTAMP NOT NULL
);\n\n""")

    f.write("-- Customers (500K)\n")
    write_inserts(f, "CUSTOMERS",
                  ["CUSTOMER_ID", "FIRST_NAME", "LAST_NAME", "EMAIL", "CITY", "STATE", "COUNTRY", "CREATED_AT", "UPDATED_AT"],
                  customers, ts_formatter=oracle_ts)

    f.write("-- Products (10K)\n")
    write_inserts(f, "PRODUCTS",
                  ["PRODUCT_ID", "PRODUCT_NAME", "CATEGORY", "PRICE", "STOCK_QTY", "CREATED_AT", "UPDATED_AT"],
                  products, ts_formatter=oracle_ts)

    f.write("-- Orders (1M)\n")
    write_inserts(f, "ORDERS",
                  ["ORDER_ID", "CUSTOMER_ID", "ORDER_DATE", "STATUS", "TOTAL_AMOUNT", "SHIP_CITY", "UPDATED_AT"],
                  orders, ts_formatter=oracle_ts)

    f.write("-- Order Items (2M)\n")
    write_inserts(f, "ORDER_ITEMS",
                  ["ITEM_ID", "ORDER_ID", "PRODUCT_ID", "QUANTITY", "UNIT_PRICE", "CREATED_AT"],
                  order_items, ts_formatter=oracle_ts)

    f.write("\nCOMMIT;\n")

print(f"  oracle_setup.sql written")


# -- Teradata -----------------------------------------------------------------
print("Writing demo/teradata_setup.sql...")
with open(DEMO_DIR / "teradata_setup.sql", "w", encoding="utf-8") as f:
    f.write("-- MigrateX Demo: Teradata Source Data\n")
    f.write("-- Database: TD_DEMO_2908\n")
    f.write("-- Run via BTEQ or Teradata Studio\n\n")
    f.write("CREATE DATABASE TD_DEMO_2908 AS PERM = 2e9;\n\n")
    f.write("DATABASE TD_DEMO_2908;\n\n")

    f.write("""CREATE TABLE CUSTOMERS (
    CUSTOMER_ID INTEGER NOT NULL,
    FIRST_NAME VARCHAR(50) NOT NULL,
    LAST_NAME VARCHAR(50) NOT NULL,
    EMAIL VARCHAR(120),
    CITY VARCHAR(60),
    STATE VARCHAR(10),
    COUNTRY VARCHAR(10),
    CREATED_AT TIMESTAMP(6) NOT NULL,
    UPDATED_AT TIMESTAMP(6) NOT NULL
) PRIMARY INDEX (CUSTOMER_ID);\n\n""")

    f.write("""CREATE TABLE PRODUCTS (
    PRODUCT_ID INTEGER NOT NULL,
    PRODUCT_NAME VARCHAR(100) NOT NULL,
    CATEGORY VARCHAR(30),
    PRICE DECIMAL(10,2),
    STOCK_QTY INTEGER,
    CREATED_AT TIMESTAMP(6) NOT NULL,
    UPDATED_AT TIMESTAMP(6) NOT NULL
) PRIMARY INDEX (PRODUCT_ID);\n\n""")

    f.write("""CREATE TABLE ORDERS (
    ORDER_ID INTEGER NOT NULL,
    CUSTOMER_ID INTEGER NOT NULL,
    ORDER_DATE TIMESTAMP(6) NOT NULL,
    STATUS VARCHAR(20),
    TOTAL_AMOUNT DECIMAL(12,2),
    SHIP_CITY VARCHAR(60),
    UPDATED_AT TIMESTAMP(6) NOT NULL
) PRIMARY INDEX (ORDER_ID);\n\n""")

    f.write("""CREATE TABLE ORDER_ITEMS (
    ITEM_ID BIGINT NOT NULL,
    ORDER_ID INTEGER NOT NULL,
    PRODUCT_ID INTEGER NOT NULL,
    QUANTITY INTEGER,
    UNIT_PRICE DECIMAL(10,2),
    CREATED_AT TIMESTAMP(6) NOT NULL
) PRIMARY INDEX (ITEM_ID);\n\n""")

    # Teradata uses INSERT INTO ... VALUES one row at a time (or USING)
    # For bulk, we write multi-statement INSERTs
    f.write("-- Customers (500K)\n")
    write_inserts(f, "CUSTOMERS",
                  ["CUSTOMER_ID", "FIRST_NAME", "LAST_NAME", "EMAIL", "CITY", "STATE", "COUNTRY", "CREATED_AT", "UPDATED_AT"],
                  customers)

    f.write("-- Products (10K)\n")
    write_inserts(f, "PRODUCTS",
                  ["PRODUCT_ID", "PRODUCT_NAME", "CATEGORY", "PRICE", "STOCK_QTY", "CREATED_AT", "UPDATED_AT"],
                  products)

    f.write("-- Orders (1M)\n")
    write_inserts(f, "ORDERS",
                  ["ORDER_ID", "CUSTOMER_ID", "ORDER_DATE", "STATUS", "TOTAL_AMOUNT", "SHIP_CITY", "UPDATED_AT"],
                  orders)

    f.write("-- Order Items (2M)\n")
    write_inserts(f, "ORDER_ITEMS",
                  ["ITEM_ID", "ORDER_ID", "PRODUCT_ID", "QUANTITY", "UNIT_PRICE", "CREATED_AT"],
                  order_items)

print(f"  teradata_setup.sql written")


# -- Incremental inserts (mid-demo) -------------------------------------------
print("Writing demo/incremental_inserts.sql...")
INCR_START = datetime(2026, 8, 28, 15, 0, 0)
INCR_END = datetime(2026, 8, 28, 18, 0, 0)

incr_orders = []
for i in range(1_000_001, 1_005_001):
    cust_id = random.randint(1, 500_000)
    order_date = rand_ts(INCR_START, INCR_END)
    status = random.choice(["processing", "shipped"])
    total = round(random.uniform(10.0, 2500.0), 2)
    ship_city = random.choice(CITIES)
    incr_orders.append((i, cust_id, order_date, status, total, ship_city, order_date))

incr_items = []
iid = 2_000_001
for o in incr_orders:
    for _ in range(random.randint(1, 3)):
        incr_items.append((iid, o[0], random.randint(1, 10_000),
                           random.randint(1, 5), round(random.uniform(5, 500), 2), o[2]))
        iid += 1

# Updated customers (SCD2 demo)
incr_customers = []
for i in random.sample(range(1, 500_001), 500):
    fn = random.choice(FIRST_NAMES)
    ln = random.choice(LAST_NAMES)
    email = f"{fn.lower()}.{ln.lower()}{i}@newdomain.com"
    city = random.choice(CITIES)
    idx = CITIES.index(city)
    state = STATES[idx % len(STATES)]
    country = random.choice(COUNTRIES)
    created = customers[i - 1][7]
    updated = rand_ts(INCR_START, INCR_END)
    incr_customers.append((i, fn, ln, email, city, state, country, created, updated))

with open(DEMO_DIR / "incremental_inserts.sql", "w", encoding="utf-8") as f:
    f.write("-- MigrateX Demo: Incremental data (run mid-demo)\n")
    f.write("-- Insert these AFTER the first full load to demo incremental pickup\n\n")

    for label, db, prefix in [
        ("MySQL", "MYSQL_DEMO_2908", "USE MYSQL_DEMO_2908;\n"),
        ("MSSQL", "MSSQL_DEMO_2908", "USE MSSQL_DEMO_2908;\nGO\n"),
    ]:
        f.write(f"-- ========================================\n")
        f.write(f"-- {label}: New orders + updated customers\n")
        f.write(f"-- ========================================\n")
        f.write(prefix + "\n")

        f.write(f"-- 5,000 new orders\n")
        write_inserts(f, "ORDERS",
                      ["ORDER_ID", "CUSTOMER_ID", "ORDER_DATE", "STATUS", "TOTAL_AMOUNT", "SHIP_CITY", "UPDATED_AT"],
                      incr_orders)

        f.write(f"-- ~10K new order items\n")
        write_inserts(f, "ORDER_ITEMS",
                      ["ITEM_ID", "ORDER_ID", "PRODUCT_ID", "QUANTITY", "UNIT_PRICE", "CREATED_AT"],
                      incr_items)

        f.write(f"-- 500 updated customers (SCD2 demo)\n")
        for c in incr_customers:
            cid = c[0]
            f.write(f"UPDATE CUSTOMERS SET FIRST_NAME='{c[1]}', LAST_NAME='{c[2]}', "
                    f"EMAIL='{c[3]}', CITY='{c[4]}', STATE='{c[5]}', "
                    f"UPDATED_AT='{fmt_ts(c[8])}' WHERE CUSTOMER_ID={cid};\n")
        f.write("\n")

    # Oracle
    f.write("-- ========================================\n")
    f.write("-- Oracle: New orders + updated customers\n")
    f.write("-- ========================================\n")
    f.write("ALTER SESSION SET CURRENT_SCHEMA = ORACLE_DEMO_2908;\n\n")

    write_inserts(f, "ORDERS",
                  ["ORDER_ID", "CUSTOMER_ID", "ORDER_DATE", "STATUS", "TOTAL_AMOUNT", "SHIP_CITY", "UPDATED_AT"],
                  incr_orders, ts_formatter=oracle_ts)
    write_inserts(f, "ORDER_ITEMS",
                  ["ITEM_ID", "ORDER_ID", "PRODUCT_ID", "QUANTITY", "UNIT_PRICE", "CREATED_AT"],
                  incr_items, ts_formatter=oracle_ts)

    for c in incr_customers:
        cid = c[0]
        f.write(f"UPDATE CUSTOMERS SET FIRST_NAME='{c[1]}', LAST_NAME='{c[2]}', "
                f"EMAIL='{c[3]}', CITY='{c[4]}', STATE='{c[5]}', "
                f"UPDATED_AT={oracle_ts(c[8])} WHERE CUSTOMER_ID={cid};\n")
    f.write("COMMIT;\n\n")

    # Teradata
    f.write("-- ========================================\n")
    f.write("-- Teradata: New orders + updated customers\n")
    f.write("-- ========================================\n")
    f.write("DATABASE TD_DEMO_2908;\n\n")

    write_inserts(f, "ORDERS",
                  ["ORDER_ID", "CUSTOMER_ID", "ORDER_DATE", "STATUS", "TOTAL_AMOUNT", "SHIP_CITY", "UPDATED_AT"],
                  incr_orders)
    write_inserts(f, "ORDER_ITEMS",
                  ["ITEM_ID", "ORDER_ID", "PRODUCT_ID", "QUANTITY", "UNIT_PRICE", "CREATED_AT"],
                  incr_items)

    for c in incr_customers:
        cid = c[0]
        f.write(f"UPDATE CUSTOMERS SET FIRST_NAME='{c[1]}', LAST_NAME='{c[2]}', "
                f"EMAIL='{c[3]}', CITY='{c[4]}', STATE='{c[5]}', "
                f"UPDATED_AT='{fmt_ts(c[8])}' WHERE CUSTOMER_ID={cid};\n")
    f.write("\n")

print(f"  incremental_inserts.sql written ({len(incr_orders)} orders, {len(incr_items)} items, {len(incr_customers)} customer updates)")


# -- S3 test files (CSV + Parquet) --------------------------------------------
print("Writing demo/s3_test_files/...")

# CSV -- 5K orders for file ingestion demo
csv_path = DEMO_DIR / "s3_test_files" / "orders_20260828.csv"
with open(csv_path, "w", newline="") as f:
    writer = csv.writer(f)
    writer.writerow(["ORDER_ID", "CUSTOMER_ID", "ORDER_DATE", "STATUS", "TOTAL_AMOUNT"])
    for o in incr_orders:
        writer.writerow([o[0], o[1], fmt_ts(o[2]), o[3], o[4]])
print(f"  orders_20260828.csv ({len(incr_orders)} rows)")

# Parquet
try:
    import pyarrow as pa
    import pyarrow.parquet as pq

    table = pa.table({
        "ORDER_ID": [o[0] for o in incr_orders],
        "CUSTOMER_ID": [o[1] for o in incr_orders],
        "ORDER_DATE": [fmt_ts(o[2]) for o in incr_orders],
        "STATUS": [o[3] for o in incr_orders],
        "TOTAL_AMOUNT": [o[4] for o in incr_orders],
    })
    pq_path = DEMO_DIR / "s3_test_files" / "orders_20260828.parquet"
    pq.write_table(table, pq_path)
    print(f"  orders_20260828.parquet ({len(incr_orders)} rows)")
except ImportError:
    print("  [WARN] pyarrow not installed -- skipping Parquet file")

print("\n[OK] All demo data generated successfully!")
print(f"   Total: ~{len(customers) + len(products) + len(orders) + len(order_items):,} rows per source")
