-- MigrateX Demo: MySQL Source Data (Stored Procedure)
-- Database: MYSQL_DEMO_2908
-- Generates ~3.5M rows directly inside MySQL (no file transfer)
-- Run: mysql -u root -p < demo/mysql_setup.sql

CREATE DATABASE IF NOT EXISTS MYSQL_DEMO_2908;
USE MYSQL_DEMO_2908;

-- Drop existing tables
DROP TABLE IF EXISTS ORDER_ITEMS;
DROP TABLE IF EXISTS ORDERS;
DROP TABLE IF EXISTS PRODUCTS;
DROP TABLE IF EXISTS CUSTOMERS;

CREATE TABLE CUSTOMERS (
    CUSTOMER_ID INT PRIMARY KEY,
    FIRST_NAME VARCHAR(50) NOT NULL,
    LAST_NAME VARCHAR(50) NOT NULL,
    EMAIL VARCHAR(120),
    CITY VARCHAR(60),
    STATE VARCHAR(10),
    COUNTRY VARCHAR(10),
    CREATED_AT DATETIME NOT NULL,
    UPDATED_AT DATETIME NOT NULL
);

CREATE TABLE PRODUCTS (
    PRODUCT_ID INT PRIMARY KEY,
    PRODUCT_NAME VARCHAR(100) NOT NULL,
    CATEGORY VARCHAR(30),
    PRICE DECIMAL(10,2),
    STOCK_QTY INT,
    CREATED_AT DATETIME NOT NULL,
    UPDATED_AT DATETIME NOT NULL
);

CREATE TABLE ORDERS (
    ORDER_ID INT PRIMARY KEY,
    CUSTOMER_ID INT NOT NULL,
    ORDER_DATE DATETIME NOT NULL,
    STATUS VARCHAR(20),
    TOTAL_AMOUNT DECIMAL(12,2),
    SHIP_CITY VARCHAR(60),
    UPDATED_AT DATETIME NOT NULL,
    INDEX idx_order_date (ORDER_DATE),
    INDEX idx_updated (UPDATED_AT)
);

CREATE TABLE ORDER_ITEMS (
    ITEM_ID BIGINT PRIMARY KEY,
    ORDER_ID INT NOT NULL,
    PRODUCT_ID INT NOT NULL,
    QUANTITY INT,
    UNIT_PRICE DECIMAL(10,2),
    CREATED_AT DATETIME NOT NULL,
    INDEX idx_order_id (ORDER_ID),
    INDEX idx_created (CREATED_AT)
);

-- Helper: random datetime between two dates
DELIMITER //

DROP PROCEDURE IF EXISTS generate_demo_data//

CREATE PROCEDURE generate_demo_data()
BEGIN
    DECLARE i INT DEFAULT 1;
    DECLARE j INT;
    DECLARE item_id BIGINT DEFAULT 1;
    DECLARE n_items INT;
    DECLARE batch_size INT DEFAULT 5000;

    -- Lookup arrays stored as comma-separated (MySQL doesn't have arrays)
    -- We use ELT() + random index instead

    SET @date_start = '2024-01-01 00:00:00';
    SET @date_end = '2026-08-28 23:59:59';
    SET @date_range_sec = TIMESTAMPDIFF(SECOND, @date_start, @date_end);

    -- ======================================
    -- PRODUCTS (10,000)
    -- ======================================
    SET i = 1;
    WHILE i <= 10000 DO
        INSERT INTO PRODUCTS (PRODUCT_ID, PRODUCT_NAME, CATEGORY, PRICE, STOCK_QTY, CREATED_AT, UPDATED_AT)
        VALUES (
            i,
            CONCAT(
                ELT(1 + FLOOR(RAND() * 8), 'Premium','Classic','Ultra','Pro','Lite','Max','Mini','Eco'),
                ' ',
                ELT(1 + FLOOR(RAND() * 8), 'Widget','Gadget','Sensor','Module','Kit','Pack','Set','Cable'),
                ' ', i
            ),
            ELT(1 + FLOOR(RAND() * 8), 'Electronics','Clothing','Home','Sports','Books','Toys','Food','Auto'),
            ROUND(5 + RAND() * 995, 2),
            FLOOR(RAND() * 10000),
            DATE_ADD(@date_start, INTERVAL FLOOR(RAND() * @date_range_sec) SECOND),
            DATE_ADD(@date_start, INTERVAL FLOOR(RAND() * @date_range_sec) SECOND)
        );
        SET i = i + 1;
    END WHILE;

    SELECT 'Products: 10,000 rows inserted' AS progress;

    -- ======================================
    -- CUSTOMERS (500,000) - batch insert
    -- ======================================
    SET i = 1;
    START TRANSACTION;
    WHILE i <= 500000 DO
        INSERT INTO CUSTOMERS (CUSTOMER_ID, FIRST_NAME, LAST_NAME, EMAIL, CITY, STATE, COUNTRY, CREATED_AT, UPDATED_AT)
        VALUES (
            i,
            ELT(1 + FLOOR(RAND() * 20), 'James','Mary','John','Patricia','Robert','Jennifer','Michael','Linda',
                'David','Elizabeth','William','Barbara','Richard','Susan','Joseph','Jessica','Thomas','Sarah','Charles','Karen'),
            ELT(1 + FLOOR(RAND() * 20), 'Smith','Johnson','Williams','Brown','Jones','Garcia','Miller','Davis',
                'Rodriguez','Martinez','Hernandez','Lopez','Gonzalez','Wilson','Anderson','Thomas','Taylor','Moore','Jackson','Martin'),
            CONCAT(LOWER(ELT(1 + FLOOR(RAND() * 5), 'james','mary','john','robert','david')),
                   '.', LOWER(ELT(1 + FLOOR(RAND() * 5), 'smith','jones','garcia','wilson','lee')),
                   i, '@example.com'),
            ELT(1 + FLOOR(RAND() * 10), 'New York','Los Angeles','Chicago','Houston','Phoenix',
                'Philadelphia','San Antonio','San Diego','Dallas','San Jose'),
            ELT(1 + FLOOR(RAND() * 10), 'NY','CA','IL','TX','AZ','PA','TX','CA','TX','CA'),
            ELT(1 + FLOOR(RAND() * 5), 'US','US','US','CA','UK'),
            DATE_ADD(@date_start, INTERVAL FLOOR(RAND() * @date_range_sec) SECOND),
            DATE_ADD(@date_start, INTERVAL FLOOR(RAND() * @date_range_sec) SECOND)
        );
        IF i % batch_size = 0 THEN
            COMMIT;
            START TRANSACTION;
        END IF;
        SET i = i + 1;
    END WHILE;
    COMMIT;

    SELECT 'Customers: 500,000 rows inserted' AS progress;

    -- ======================================
    -- ORDERS (1,000,000) - batch insert
    -- ======================================
    SET i = 1;
    START TRANSACTION;
    WHILE i <= 1000000 DO
        INSERT INTO ORDERS (ORDER_ID, CUSTOMER_ID, ORDER_DATE, STATUS, TOTAL_AMOUNT, SHIP_CITY, UPDATED_AT)
        VALUES (
            i,
            1 + FLOOR(RAND() * 500000),
            DATE_ADD(@date_start, INTERVAL FLOOR(RAND() * @date_range_sec) SECOND),
            ELT(1 + FLOOR(RAND() * 6), 'completed','completed','completed','shipped','processing','cancelled'),
            ROUND(10 + RAND() * 2490, 2),
            ELT(1 + FLOOR(RAND() * 10), 'New York','Los Angeles','Chicago','Houston','Phoenix',
                'Philadelphia','San Antonio','San Diego','Dallas','San Jose'),
            DATE_ADD(@date_start, INTERVAL FLOOR(RAND() * @date_range_sec) SECOND)
        );
        IF i % batch_size = 0 THEN
            COMMIT;
            START TRANSACTION;
        END IF;
        SET i = i + 1;
    END WHILE;
    COMMIT;

    SELECT 'Orders: 1,000,000 rows inserted' AS progress;

    -- ======================================
    -- ORDER_ITEMS (~2,000,000) - batch insert
    -- ======================================
    SET i = 1;
    SET item_id = 1;
    START TRANSACTION;
    WHILE i <= 1000000 AND item_id <= 2000000 DO
        SET n_items = 1 + FLOOR(RAND() * 3);
        SET j = 1;
        WHILE j <= n_items AND item_id <= 2000000 DO
            INSERT INTO ORDER_ITEMS (ITEM_ID, ORDER_ID, PRODUCT_ID, QUANTITY, UNIT_PRICE, CREATED_AT)
            VALUES (
                item_id,
                i,
                1 + FLOOR(RAND() * 10000),
                1 + FLOOR(RAND() * 10),
                ROUND(5 + RAND() * 495, 2),
                DATE_ADD(@date_start, INTERVAL FLOOR(RAND() * @date_range_sec) SECOND)
            );
            SET item_id = item_id + 1;
            SET j = j + 1;
        END WHILE;
        IF i % batch_size = 0 THEN
            COMMIT;
            START TRANSACTION;
        END IF;
        SET i = i + 1;
    END WHILE;
    COMMIT;

    SELECT CONCAT('Order Items: ', item_id - 1, ' rows inserted') AS progress;
    SELECT 'DEMO DATA GENERATION COMPLETE' AS status;
END//

DELIMITER ;

-- Run it
CALL generate_demo_data();
