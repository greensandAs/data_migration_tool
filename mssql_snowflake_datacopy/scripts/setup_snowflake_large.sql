-- ============================================================================
-- Snowflake Target Tables — Large Volume Schema
-- ============================================================================
-- Run this ONCE in Snowflake before the large-volume demo.
-- Matches the expanded MSSQL schema from seed_large_volume.sql
-- ============================================================================

USE ROLE ACCOUNTADMIN;
USE WAREHOUSE COMPUTE_WH;

-- ╔══════════════════════════════════════════════════════════════════════════════╗
-- ║  TARGET TABLES (ANALYTICS.PUBLIC)                                            ║
-- ╚══════════════════════════════════════════════════════════════════════════════╝

CREATE OR REPLACE TABLE ANALYTICS.PUBLIC.CUSTOMERS (
    CUSTOMERID      INT,
    FIRSTNAME       VARCHAR(50),
    LASTNAME        VARCHAR(50),
    EMAIL           VARCHAR(100),
    PHONE           VARCHAR(20),
    CITY            VARCHAR(50),
    STATE           VARCHAR(30),
    COUNTRY         VARCHAR(30),
    ZIPCODE         VARCHAR(10),
    CUSTOMERTYPE    VARCHAR(20),
    CREDITLIMIT     NUMBER(12,2),
    ISACTIVE        BOOLEAN,
    CREATEDAT       TIMESTAMP_NTZ,
    UPDATEDAT       TIMESTAMP_NTZ
);

CREATE OR REPLACE TABLE ANALYTICS.PUBLIC.ORDERS (
    ORDERID         INT,
    CUSTOMERID      INT,
    PRODUCTID       INT,
    ORDERDATE       DATE,
    SHIPDATE        DATE,
    QUANTITY        INT,
    UNITPRICE       NUMBER(10,2),
    DISCOUNT        NUMBER(5,2),
    TOTALAMOUNT     NUMBER(12,2),
    ORDERSTATUS     VARCHAR(20),
    PAYMENTMETHOD   VARCHAR(20),
    SHIPPINGMETHOD  VARCHAR(20),
    REGION          VARCHAR(30),
    CREATEDAT       TIMESTAMP_NTZ,
    UPDATEDAT       TIMESTAMP_NTZ
);

CREATE OR REPLACE TABLE ANALYTICS.PUBLIC.PRODUCTS (
    PRODUCTID       INT,
    PRODUCTNAME     VARCHAR(100),
    CATEGORY        VARCHAR(50),
    SUBCATEGORY     VARCHAR(50),
    BRAND           VARCHAR(50),
    SKU             VARCHAR(30),
    UNITPRICE       NUMBER(10,2),
    COSTPRICE       NUMBER(10,2),
    WEIGHT          NUMBER(8,2),
    ISACTIVE        BOOLEAN,
    CREATEDAT       TIMESTAMP_NTZ,
    UPDATEDAT       TIMESTAMP_NTZ
);


-- ╔══════════════════════════════════════════════════════════════════════════════╗
-- ║  WORK SCHEMA                                                                ║
-- ╚══════════════════════════════════════════════════════════════════════════════╝

CREATE SCHEMA IF NOT EXISTS ANALYTICS.PUBLIC_WRK;


-- ╔══════════════════════════════════════════════════════════════════════════════╗
-- ║  VERIFICATION                                                               ║
-- ╚══════════════════════════════════════════════════════════════════════════════╝

-- Confirm tables exist and are empty
SELECT 'CUSTOMERS' AS TBL, COUNT(*) AS ROW_S FROM ANALYTICS.PUBLIC.CUSTOMERS
UNION ALL SELECT 'ORDERS', COUNT(*) FROM ANALYTICS.PUBLIC.ORDERS
UNION ALL SELECT 'PRODUCTS', COUNT(*) FROM ANALYTICS.PUBLIC.PRODUCTS;

-- Show column structure
DESCRIBE TABLE ANALYTICS.PUBLIC.CUSTOMERS;
DESCRIBE TABLE ANALYTICS.PUBLIC.ORDERS;
DESCRIBE TABLE ANALYTICS.PUBLIC.PRODUCTS;
