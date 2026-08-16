-- ============================================================================
-- MSSQL → Snowflake Migration Tool — Demo Queries
-- ============================================================================
-- Use these queries during the live demo to verify and showcase the pipeline.
-- Run in order: Pre-Demo Reset → During Demo → Post-Demo Verification
-- ============================================================================


-- ╔══════════════════════════════════════════════════════════════════════════════╗
-- ║  SECTION 1: PRE-DEMO RESET (Run before every demo)                         ║
-- ╚══════════════════════════════════════════════════════════════════════════════╝

-- 1.1 Truncate control tables
TRUNCATE TABLE DATA_MIGRATION.CONTROL.CONFIG_TABLE;
TRUNCATE TABLE DATA_MIGRATION.CONTROL.LOG_TABLE;

-- 1.2 Truncate target tables
TRUNCATE TABLE ANALYTICS.PUBLIC.CUSTOMERS;
TRUNCATE TABLE ANALYTICS.PUBLIC.ORDERS;
TRUNCATE TABLE ANALYTICS.PUBLIC.PRODUCTS;

-- 1.3 Drop work tables
DROP TABLE IF EXISTS ANALYTICS.PUBLIC_WRK.CUSTOMERS;
DROP TABLE IF EXISTS ANALYTICS.PUBLIC_WRK.ORDERS;
DROP TABLE IF EXISTS ANALYTICS.PUBLIC_WRK.PRODUCTS;

-- 1.4 Clean stage (Azure Blob files)
REMOVE @DATA_MIGRATION.CONTROL.MIGRATION_STAGE/CUSTOMERS/;
REMOVE @DATA_MIGRATION.CONTROL.MIGRATION_STAGE/ORDERS/;
REMOVE @DATA_MIGRATION.CONTROL.MIGRATION_STAGE/PRODUCTS/;
REMOVE @DATA_MIGRATION.CONTROL.MIGRATION_STAGE/processed/CUSTOMERS/;
REMOVE @DATA_MIGRATION.CONTROL.MIGRATION_STAGE/processed/ORDERS/;
REMOVE @DATA_MIGRATION.CONTROL.MIGRATION_STAGE/processed/PRODUCTS/;

-- 1.5 Verify everything is clean
SELECT 'CONFIG_TABLE' AS OBJECT, COUNT(*) AS ROW_COUNT FROM DATA_MIGRATION.CONTROL.CONFIG_TABLE
UNION ALL SELECT 'LOG_TABLE', COUNT(*) FROM DATA_MIGRATION.CONTROL.LOG_TABLE
UNION ALL SELECT 'CUSTOMERS', COUNT(*) FROM ANALYTICS.PUBLIC.CUSTOMERS
UNION ALL SELECT 'ORDERS', COUNT(*) FROM ANALYTICS.PUBLIC.ORDERS
UNION ALL SELECT 'PRODUCTS', COUNT(*) FROM ANALYTICS.PUBLIC.PRODUCTS;

-- Expected: All rows = 0


-- ╔══════════════════════════════════════════════════════════════════════════════╗
-- ║  SECTION 2: DURING DEMO — Config Verification                              ║
-- ╚══════════════════════════════════════════════════════════════════════════════╝

-- 2.1 Show config auto-synced from the app
SELECT
    MSSQL_DATABASE_NAME AS SOURCE_DB,
    MSSQL_SCHEMA_NAME   AS SOURCE_SCHEMA,
    MSSQL_TABLE_NAME    AS SOURCE_TABLE,
    SF_DATABASE_NAME    AS TARGET_DB,
    SF_TABLE_NAME       AS TARGET_TABLE,
    LOAD_TYPE,
    SCD_TYPE,
    PRIMARY_KEY,
    CDC_COLUMNS,
    ENABLED
FROM DATA_MIGRATION.CONTROL.CONFIG_TABLE
ORDER BY JOB_ID;


-- ╔══════════════════════════════════════════════════════════════════════════════╗
-- ║  SECTION 3: DURING DEMO — Stage & Load Verification                        ║
-- ╚══════════════════════════════════════════════════════════════════════════════╝

-- 3.1 Check files landed in Azure Blob (after EXPORT phase)
LIST @DATA_MIGRATION.CONTROL.MIGRATION_STAGE/CUSTOMERS/;
LIST @DATA_MIGRATION.CONTROL.MIGRATION_STAGE/ORDERS/;
LIST @DATA_MIGRATION.CONTROL.MIGRATION_STAGE/PRODUCTS/;

-- 3.2 Check files moved to processed (after LOAD phase)
LIST @DATA_MIGRATION.CONTROL.MIGRATION_STAGE/processed/;


-- ╔══════════════════════════════════════════════════════════════════════════════╗
-- ║  SECTION 4: POST-MIGRATION — Row Count Verification                        ║
-- ╚══════════════════════════════════════════════════════════════════════════════╝

-- 4.1 Target table counts (expected: 100 / 500 / 50)
SELECT 'CUSTOMERS' AS TABLE_NAME, COUNT(*) AS ROW_COUNT FROM ANALYTICS.PUBLIC.CUSTOMERS
UNION ALL SELECT 'ORDERS', COUNT(*) FROM ANALYTICS.PUBLIC.ORDERS
UNION ALL SELECT 'PRODUCTS', COUNT(*) FROM ANALYTICS.PUBLIC.PRODUCTS;

-- 4.2 Sample data — Customers
SELECT * FROM ANALYTICS.PUBLIC.CUSTOMERS LIMIT 10;

-- 4.3 Sample data — Orders
SELECT * FROM ANALYTICS.PUBLIC.ORDERS LIMIT 10;

-- 4.4 Sample data — Products
SELECT * FROM ANALYTICS.PUBLIC.PRODUCTS LIMIT 10;


-- ╔══════════════════════════════════════════════════════════════════════════════╗
-- ║  SECTION 5: AUDIT TRAIL — Full Job History                                  ║
-- ╚══════════════════════════════════════════════════════════════════════════════╝

-- 5.1 Job summary (the money shot for compliance)
SELECT
    BATCH_ID,
    JOB_ID,
    MSSQL_TABLE_NAME    AS SOURCE_TABLE,
    SF_TABLE_NAME       AS TARGET_TABLE,
    EXECUTION_MODE      AS MODE,
    LOAD_TYPE,
    MSSQL_TABLE_COUNT   AS SOURCE_ROWS,
    SF_TABLE_COUNT      AS TARGET_ROWS,
    FINAL_STATUS        AS STATUS,
    JOB_DURATION        AS DURATION_SEC,
    JOB_START_TIME,
    JOB_END_TIME
FROM DATA_MIGRATION.CONTROL.LOG_TABLE
ORDER BY BATCH_ID, JOB_ID;

-- 5.2 Detailed step-by-step status
SELECT
    BATCH_ID,
    MSSQL_TABLE_NAME,
    BCP_EXPORT_STATUS   AS EXTRACT,
    S3_UPLOAD_STATUS    AS UPLOAD,
    COPY_COMMAND_STATUS AS COPY_INTO,
    MERGE_STATEMENT_STATUS AS MERGE,
    FINAL_STATUS
FROM DATA_MIGRATION.CONTROL.LOG_TABLE
ORDER BY BATCH_ID, JOB_ID;

-- DELETE FROM DATA_MIGRATION.CONTROL.LOG_TABLE WHERE BATCH_ID = 10001;

-- 5.3 View the actual MERGE SQL used (transparency)
SELECT
    MSSQL_TABLE_NAME,
    MERGE_STATEMENT
FROM DATA_MIGRATION.CONTROL.LOG_TABLE
WHERE MERGE_STATEMENT IS NOT NULL
ORDER BY BATCH_ID DESC
LIMIT 3;


-- ╔══════════════════════════════════════════════════════════════════════════════╗
-- ║  SECTION 6: INCREMENTAL MODE DEMO (The Power of CDC)                        ║
-- ║                                                                              ║
-- ║  FLOW: Full Load → Change Source Data → Incremental Run → Verify Delta      ║
-- ╚══════════════════════════════════════════════════════════════════════════════╝

-- ─────────────────────────────────────────────────────────────────────────────
-- STEP 1: Confirm baseline after FULL load (should already be done)
-- ─────────────────────────────────────────────────────────────────────────────

-- Run in SNOWFLAKE: Verify initial counts
SELECT 'CUSTOMERS' AS TABLE_NAME, COUNT(*) AS ROW_COUNT FROM ANALYTICS.PUBLIC.CUSTOMERS
UNION ALL SELECT 'ORDERS', COUNT(*) FROM ANALYTICS.PUBLIC.ORDERS;
-- Expected: CUSTOMERS=100, ORDERS=500


-- ─────────────────────────────────────────────────────────────────────────────
-- STEP 2: Make changes in MSSQL source (Run in Azure SQL / SSMS / Azure Data Studio)
-- ─────────────────────────────────────────────────────────────────────────────

-- ┌─────────────────────────────────────────────────────────────────────────┐
-- │  RUN THESE IN MSSQL (Azure SQL / SSMS / Azure Data Studio)              │
-- └─────────────────────────────────────────────────────────────────────────┘

-- 2a. UPDATE 5 existing customers (simulate email/phone change)
/*
USE TestDB;

UPDATE dbo.Customers SET
    Email = 'updated_' + CAST(CustomerID AS VARCHAR) + '@newdomain.com',
    Phone = '999-000-' + RIGHT('0000' + CAST(CustomerID AS VARCHAR), 4),
    ModifiedDate = GETDATE()
WHERE CustomerID IN (1, 2, 3, 4, 5);

-- 2b. INSERT 3 new customers (simulate new sign-ups)
INSERT INTO dbo.Customers (FirstName, LastName, Email, Phone, City, State, Country, CreatedDate, ModifiedDate)
VALUES
    ('Demo', 'NewUser1', 'demo.user1@test.com', '555-0001', 'New York', 'NY', 'USA', GETDATE(), GETDATE()),
    ('Demo', 'NewUser2', 'demo.user2@test.com', '555-0002', 'London', NULL, 'UK', GETDATE(), GETDATE()),
    ('Demo', 'NewUser3', 'demo.user3@test.com', '555-0003', 'Mumbai', 'MH', 'India', GETDATE(), GETDATE());

-- 2c. INSERT 10 new orders (simulate recent transactions)
INSERT INTO dbo.Orders (CustomerID, OrderDate, TotalAmount, Status, ShippingAddress, CreatedDate, ModifiedDate)
VALUES
    (1, GETDATE(), 299.99, 'Pending',   '123 Demo St, NY', GETDATE(), GETDATE()),
    (2, GETDATE(), 149.50, 'Pending',   '456 Test Ave, CA', GETDATE(), GETDATE()),
    (3, GETDATE(), 599.00, 'Confirmed', '789 Live Rd, TX', GETDATE(), GETDATE()),
    (5, GETDATE(), 89.99,  'Pending',   '321 Show Ln, WA', GETDATE(), GETDATE()),
    (10, GETDATE(), 450.00, 'Shipped',  '654 Prod Blvd, FL', GETDATE(), GETDATE()),
    (15, GETDATE(), 75.25,  'Pending',  '987 Run Way, IL', GETDATE(), GETDATE()),
    (20, GETDATE(), 1250.00, 'Confirmed', '147 Scale Dr, CO', GETDATE(), GETDATE()),
    (25, GETDATE(), 34.99,  'Pending',   '258 Fast Ct, GA', GETDATE(), GETDATE()),
    (30, GETDATE(), 675.00, 'Shipped',   '369 Bulk Pl, AZ', GETDATE(), GETDATE()),
    (50, GETDATE(), 199.99, 'Pending',   '741 Batch Rd, OR', GETDATE(), GETDATE());

-- 2d. Verify changes in MSSQL
SELECT COUNT(*) AS TotalCustomers FROM dbo.Customers;       -- Expected: 103
SELECT COUNT(*) AS TotalOrders FROM dbo.Orders;             -- Expected: 510
SELECT TOP 5 * FROM dbo.Customers ORDER BY ModifiedDate DESC;  -- Shows updated rows
SELECT TOP 10 * FROM dbo.Orders ORDER BY CreatedDate DESC;     -- Shows new orders
*/


-- ─────────────────────────────────────────────────────────────────────────────
-- STEP 3: Update app config to INCREMENTAL mode
-- ─────────────────────────────────────────────────────────────────────────────

-- Option A: Do this in the Streamlit UI (Configuration tab → Edit → load_type=incremental)
-- Option B: Verify config shows incremental after UI change:
SELECT
    MSSQL_TABLE_NAME,
    LOAD_TYPE,
    CDC_COLUMNS,
    CDC_TYPE,
    SCD_TYPE,
    PRIMARY_KEY
FROM DATA_MIGRATION.CONTROL.CONFIG_TABLE;
-- CUSTOMERS should show: LOAD_TYPE=INCREMENTAL, CDC_COLUMNS=ModifiedDate, CDC_TYPE=TIMESTAMP
-- ORDERS should show: LOAD_TYPE=INCREMENTAL, CDC_COLUMNS=ModifiedDate, CDC_TYPE=TIMESTAMP


-- ─────────────────────────────────────────────────────────────────────────────
-- STEP 4: Run INCREMENTAL migration from the Streamlit app
-- ─────────────────────────────────────────────────────────────────────────────

-- In the app: Select CUSTOMERS + ORDERS → Mode: FULL → Click Start
-- The pipeline will:
--   1. Read last successful run timestamp from LOG_TABLE
--   2. Build WHERE clause: ModifiedDate >= last_run AND ModifiedDate < now
--   3. BCP exports ONLY changed/new rows (8 customers, 10 orders)
--   4. Upload → COPY INTO _WRK → MERGE into target


-- ─────────────────────────────────────────────────────────────────────────────
-- STEP 5: Verify in Snowflake — Only deltas were processed
-- ─────────────────────────────────────────────────────────────────────────────

-- 5a. Check row counts (should reflect new totals)
SELECT 'CUSTOMERS' AS TABLE_NAME, COUNT(*) AS ROW_COUNT FROM ANALYTICS.PUBLIC.CUSTOMERS
UNION ALL SELECT 'ORDERS', COUNT(*) FROM ANALYTICS.PUBLIC.ORDERS;
-- Expected: CUSTOMERS=103 (100 + 3 new), ORDERS=510 (500 + 10 new)

-- 5b. Verify the updated customers have new email/phone
SELECT CustomerID, Email, Phone
FROM ANALYTICS.PUBLIC.CUSTOMERS
WHERE CustomerID IN (1, 2, 3, 4, 5)
ORDER BY CustomerID;
-- Expected: Email = 'updated_X@newdomain.com', Phone = '999-000-XXXX'

-- 5c. Verify new customers were inserted
SELECT *
FROM ANALYTICS.PUBLIC.CUSTOMERS
WHERE FirstName = 'Demo'
ORDER BY CustomerID;
-- Expected: 3 rows (NewUser1, NewUser2, NewUser3)

-- 5d. Verify new orders appeared
SELECT *
FROM ANALYTICS.PUBLIC.ORDERS
WHERE TotalAmount IN (299.99, 149.50, 599.00, 89.99, 450.00, 75.25, 1250.00, 34.99, 675.00, 199.99)
ORDER BY OrderDate DESC;
-- Expected: 10 new orders

-- 5e. Check LOG_TABLE — should show delta counts (not full table)
SELECT
    BATCH_ID,
    MSSQL_TABLE_NAME,
    EXECUTION_MODE,
    LOAD_TYPE,
    MSSQL_TABLE_COUNT AS ROWS_EXTRACTED,
    SF_TABLE_COUNT    AS TARGET_TOTAL,
    FINAL_STATUS,
    JOB_DURATION
FROM DATA_MIGRATION.CONTROL.LOG_TABLE
ORDER BY BATCH_ID DESC, JOB_ID
LIMIT 5;
-- Expected: CUSTOMERS extracted=8 (5 updated + 3 new), ORDERS extracted=10

-- 5f. Compare FULL vs INCREMENTAL runs side by side
SELECT
    BATCH_ID,
    MSSQL_TABLE_NAME,
    LOAD_TYPE,
    MSSQL_TABLE_COUNT AS ROWS_MOVED,
    JOB_DURATION      AS SECONDS,
    JOB_START_TIME
FROM DATA_MIGRATION.CONTROL.LOG_TABLE
WHERE MSSQL_TABLE_NAME IN ('Customers', 'Orders')
ORDER BY JOB_START_TIME;
-- Shows: Full=100/500 rows, Incremental=8/10 rows — dramatic reduction


-- ─────────────────────────────────────────────────────────────────────────────
-- STEP 6: Narration / Talking Points
-- ─────────────────────────────────────────────────────────────────────────────

-- "In the first run, we moved 100 customers and 500 orders — full table scan.
--  In the incremental run, we moved only 8 customers and 10 orders.
--  That's a 92% reduction in data movement.
--  At enterprise scale (10M rows), this means moving 800 rows instead of 10 million.
--  Faster. Cheaper. Less load on the source system."


-- ╔══════════════════════════════════════════════════════════════════════════════╗
-- ║  SECTION 6B: SCD TYPE 2 — History Tracking Demo                             ║
-- ║                                                                              ║
-- ║  Requires: IS_CURRENT, EFF_START_DATE, EFF_END_DATE columns on target       ║
-- ╚══════════════════════════════════════════════════════════════════════════════╝

-- ─────────────────────────────────────────────────────────────────────────────
-- PREREQUISITE: Add SCD2 columns to CUSTOMERS (one-time setup)
-- ─────────────────────────────────────────────────────────────────────────────

/*
-- Run this ONLY if demoing SCD Type 2:
ALTER TABLE ANALYTICS.PUBLIC.CUSTOMERS ADD COLUMN IS_CURRENT BOOLEAN DEFAULT TRUE;
ALTER TABLE ANALYTICS.PUBLIC.CUSTOMERS ADD COLUMN EFF_START_DATE TIMESTAMP DEFAULT CURRENT_TIMESTAMP();
ALTER TABLE ANALYTICS.PUBLIC.CUSTOMERS ADD COLUMN EFF_END_DATE TIMESTAMP DEFAULT NULL;

-- Initialize existing rows
UPDATE ANALYTICS.PUBLIC.CUSTOMERS SET IS_CURRENT = TRUE, EFF_START_DATE = CURRENT_TIMESTAMP();
*/

-- ─────────────────────────────────────────────────────────────────────────────
-- After running SCD2 incremental migration:
-- ─────────────────────────────────────────────────────────────────────────────

-- Show history for a changed customer
/*
SELECT
    CustomerID,
    Email,
    Phone,
    IS_CURRENT,
    EFF_START_DATE,
    EFF_END_DATE
FROM ANALYTICS.PUBLIC.CUSTOMERS
WHERE CustomerID = 1
ORDER BY EFF_START_DATE DESC;
-- Expected: 2 rows
--   Row 1: IS_CURRENT=TRUE,  new email, EFF_END_DATE=NULL (current version)
--   Row 2: IS_CURRENT=FALSE, old email, EFF_END_DATE=<timestamp> (expired version)

-- Total rows should be: 100 original + 5 expired duplicates + 3 new = 108
SELECT COUNT(*) FROM ANALYTICS.PUBLIC.CUSTOMERS;

-- Only current records
SELECT COUNT(*) FROM ANALYTICS.PUBLIC.CUSTOMERS WHERE IS_CURRENT = TRUE;
-- Expected: 103 (100 original - 5 expired + 5 new versions + 3 new customers)
*/


-- ╔══════════════════════════════════════════════════════════════════════════════╗
-- ║  SECTION 7: PERFORMANCE & COST METRICS (Impress the CTO)                   ║
-- ╚══════════════════════════════════════════════════════════════════════════════╝

-- 7.1 Average duration per table
SELECT
    MSSQL_TABLE_NAME,
    COUNT(*)                AS TOTAL_RUNS,
    AVG(JOB_DURATION)      AS AVG_DURATION_SEC,
    SUM(MSSQL_TABLE_COUNT) AS TOTAL_ROWS_MOVED,
    MIN(JOB_START_TIME)    AS FIRST_RUN,
    MAX(JOB_END_TIME)      AS LAST_RUN
FROM DATA_MIGRATION.CONTROL.LOG_TABLE
WHERE FINAL_STATUS = 'SUCCESS'
GROUP BY MSSQL_TABLE_NAME;

-- 7.2 Success rate
SELECT
    COUNT(*) AS TOTAL_JOBS,
    SUM(CASE WHEN FINAL_STATUS = 'SUCCESS' THEN 1 ELSE 0 END) AS SUCCEEDED,
    SUM(CASE WHEN FINAL_STATUS = 'FAILED' THEN 1 ELSE 0 END)  AS FAILED,
    ROUND(SUM(CASE WHEN FINAL_STATUS = 'SUCCESS' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1) AS SUCCESS_RATE_PCT
FROM DATA_MIGRATION.CONTROL.LOG_TABLE;

-- 7.3 Total data moved today
SELECT
    TO_DATE(JOB_START_TIME) AS RUN_DATE,
    COUNT(*)                AS TABLES_PROCESSED,
    SUM(MSSQL_TABLE_COUNT)  AS TOTAL_ROWS_EXTRACTED,
    SUM(SF_TABLE_COUNT)     AS TOTAL_ROWS_LOADED,
    SUM(JOB_DURATION)       AS TOTAL_DURATION_SEC
FROM DATA_MIGRATION.CONTROL.LOG_TABLE
WHERE FINAL_STATUS = 'SUCCESS'
GROUP BY TO_DATE(JOB_START_TIME)
ORDER BY RUN_DATE DESC;


-- ╔══════════════════════════════════════════════════════════════════════════════╗
-- ║  SECTION 8: SNOWFLAKE OBJECTS OVERVIEW (Architecture slide backup)          ║
-- ╚══════════════════════════════════════════════════════════════════════════════╝

-- 8.1 External stage definition
DESCRIBE STAGE DATA_MIGRATION.CONTROL.MIGRATION_STAGE;

-- 8.2 Target table DDL
SELECT GET_DDL('TABLE', 'ANALYTICS.PUBLIC.CUSTOMERS');
SELECT GET_DDL('TABLE', 'ANALYTICS.PUBLIC.ORDERS');
SELECT GET_DDL('TABLE', 'ANALYTICS.PUBLIC.PRODUCTS');

-- 8.3 All schemas in play
SHOW SCHEMAS IN DATABASE ANALYTICS;
SHOW SCHEMAS IN DATABASE DATA_MIGRATION;
