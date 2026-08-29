-- MigrateX Demo: Oracle Source Data (PL/SQL Procedure)
-- Schema: ORACLE_DEMO_2908
-- Generates ~3.5M rows directly inside Oracle
-- Run: sqlplus sys/password@//host:1521/ORCL as sysdba @demo/oracle_setup.sql

-- Create user/schema
CREATE USER ORACLE_DEMO_2908 IDENTIFIED BY demo2908pwd
  DEFAULT TABLESPACE USERS QUOTA UNLIMITED ON USERS;
GRANT CONNECT, RESOURCE, CREATE SESSION TO ORACLE_DEMO_2908;

ALTER SESSION SET CURRENT_SCHEMA = ORACLE_DEMO_2908;

-- Drop existing
BEGIN EXECUTE IMMEDIATE 'DROP TABLE ORDER_ITEMS CASCADE CONSTRAINTS'; EXCEPTION WHEN OTHERS THEN NULL; END;
/
BEGIN EXECUTE IMMEDIATE 'DROP TABLE ORDERS CASCADE CONSTRAINTS'; EXCEPTION WHEN OTHERS THEN NULL; END;
/
BEGIN EXECUTE IMMEDIATE 'DROP TABLE PRODUCTS CASCADE CONSTRAINTS'; EXCEPTION WHEN OTHERS THEN NULL; END;
/
BEGIN EXECUTE IMMEDIATE 'DROP TABLE CUSTOMERS CASCADE CONSTRAINTS'; EXCEPTION WHEN OTHERS THEN NULL; END;
/

CREATE TABLE CUSTOMERS (
    CUSTOMER_ID NUMBER(10) PRIMARY KEY,
    FIRST_NAME VARCHAR2(50) NOT NULL,
    LAST_NAME VARCHAR2(50) NOT NULL,
    EMAIL VARCHAR2(120),
    CITY VARCHAR2(60),
    STATE VARCHAR2(10),
    COUNTRY VARCHAR2(10),
    CREATED_AT TIMESTAMP NOT NULL,
    UPDATED_AT TIMESTAMP NOT NULL
);

CREATE TABLE PRODUCTS (
    PRODUCT_ID NUMBER(10) PRIMARY KEY,
    PRODUCT_NAME VARCHAR2(100) NOT NULL,
    CATEGORY VARCHAR2(30),
    PRICE NUMBER(10,2),
    STOCK_QTY NUMBER(10),
    CREATED_AT TIMESTAMP NOT NULL,
    UPDATED_AT TIMESTAMP NOT NULL
);

CREATE TABLE ORDERS (
    ORDER_ID NUMBER(10) PRIMARY KEY,
    CUSTOMER_ID NUMBER(10) NOT NULL,
    ORDER_DATE TIMESTAMP NOT NULL,
    STATUS VARCHAR2(20),
    TOTAL_AMOUNT NUMBER(12,2),
    SHIP_CITY VARCHAR2(60),
    UPDATED_AT TIMESTAMP NOT NULL
);

CREATE TABLE ORDER_ITEMS (
    ITEM_ID NUMBER(18) PRIMARY KEY,
    ORDER_ID NUMBER(10) NOT NULL,
    PRODUCT_ID NUMBER(10) NOT NULL,
    QUANTITY NUMBER(5),
    UNIT_PRICE NUMBER(10,2),
    CREATED_AT TIMESTAMP NOT NULL
);

-- Helper: random timestamp
CREATE OR REPLACE FUNCTION rand_ts RETURN TIMESTAMP IS
    v_secs NUMBER := TRUNC(DBMS_RANDOM.VALUE(0, 83980800));
BEGIN
    RETURN TIMESTAMP '2024-01-01 00:00:00' + NUMTODSINTERVAL(v_secs, 'SECOND');
END;
/

-- Data generation procedure
CREATE OR REPLACE PROCEDURE generate_demo_data IS
    TYPE t_names IS TABLE OF VARCHAR2(20);
    v_first t_names := t_names('James','Mary','John','Patricia','Robert','Jennifer','Michael','Linda',
                               'David','Elizabeth','William','Barbara','Richard','Susan','Joseph',
                               'Jessica','Thomas','Sarah','Charles','Karen');
    v_last  t_names := t_names('Smith','Johnson','Williams','Brown','Jones','Garcia','Miller','Davis',
                               'Rodriguez','Martinez','Hernandez','Lopez','Gonzalez','Wilson','Anderson',
                               'Thomas','Taylor','Moore','Jackson','Martin');
    v_cities t_names := t_names('New York','Los Angeles','Chicago','Houston','Phoenix',
                                'Philadelphia','San Antonio','San Diego','Dallas','San Jose');
    v_states t_names := t_names('NY','CA','IL','TX','AZ','PA','TX','CA','TX','CA');
    v_cats   t_names := t_names('Electronics','Clothing','Home','Sports','Books','Toys','Food','Auto');
    v_adj    t_names := t_names('Premium','Classic','Ultra','Pro','Lite','Max','Mini','Eco');
    v_noun   t_names := t_names('Widget','Gadget','Sensor','Module','Kit','Pack','Set','Cable');
    v_status t_names := t_names('completed','completed','completed','shipped','processing','cancelled');

    v_item_id NUMBER := 1;
    v_n_items NUMBER;
    v_city_idx NUMBER;
BEGIN
    -- Products (10K)
    DBMS_OUTPUT.PUT_LINE('Generating Products...');
    FOR i IN 1..10000 LOOP
        INSERT INTO PRODUCTS VALUES (
            i,
            v_adj(TRUNC(DBMS_RANDOM.VALUE(1, 9))) || ' ' || v_noun(TRUNC(DBMS_RANDOM.VALUE(1, 9))) || ' ' || i,
            v_cats(TRUNC(DBMS_RANDOM.VALUE(1, 9))),
            ROUND(5 + DBMS_RANDOM.VALUE * 995, 2),
            TRUNC(DBMS_RANDOM.VALUE(0, 10000)),
            rand_ts, rand_ts
        );
    END LOOP;
    COMMIT;
    DBMS_OUTPUT.PUT_LINE('Products: 10,000 done');

    -- Customers (500K)
    DBMS_OUTPUT.PUT_LINE('Generating Customers...');
    FOR i IN 1..500000 LOOP
        v_city_idx := TRUNC(DBMS_RANDOM.VALUE(1, 11));
        INSERT INTO CUSTOMERS VALUES (
            i,
            v_first(TRUNC(DBMS_RANDOM.VALUE(1, 21))),
            v_last(TRUNC(DBMS_RANDOM.VALUE(1, 21))),
            LOWER(v_first(TRUNC(DBMS_RANDOM.VALUE(1, 6)))) || '.' ||
                LOWER(v_last(TRUNC(DBMS_RANDOM.VALUE(1, 6)))) || i || '@example.com',
            v_cities(v_city_idx),
            v_states(v_city_idx),
            CASE WHEN DBMS_RANDOM.VALUE < 0.8 THEN 'US' WHEN DBMS_RANDOM.VALUE < 0.9 THEN 'CA' ELSE 'UK' END,
            rand_ts, rand_ts
        );
        IF MOD(i, 10000) = 0 THEN
            COMMIT;
            DBMS_OUTPUT.PUT_LINE('Customers: ' || i || '/500000');
        END IF;
    END LOOP;
    COMMIT;

    -- Orders (1M)
    DBMS_OUTPUT.PUT_LINE('Generating Orders...');
    FOR i IN 1..1000000 LOOP
        INSERT INTO ORDERS VALUES (
            i,
            TRUNC(DBMS_RANDOM.VALUE(1, 500001)),
            rand_ts,
            v_status(TRUNC(DBMS_RANDOM.VALUE(1, 7))),
            ROUND(10 + DBMS_RANDOM.VALUE * 2490, 2),
            v_cities(TRUNC(DBMS_RANDOM.VALUE(1, 11))),
            rand_ts
        );
        IF MOD(i, 10000) = 0 THEN
            COMMIT;
            DBMS_OUTPUT.PUT_LINE('Orders: ' || i || '/1000000');
        END IF;
    END LOOP;
    COMMIT;

    -- Order Items (2M)
    DBMS_OUTPUT.PUT_LINE('Generating Order Items...');
    FOR i IN 1..1000000 LOOP
        v_n_items := TRUNC(DBMS_RANDOM.VALUE(1, 4));
        FOR j IN 1..v_n_items LOOP
            EXIT WHEN v_item_id > 2000000;
            INSERT INTO ORDER_ITEMS VALUES (
                v_item_id,
                i,
                TRUNC(DBMS_RANDOM.VALUE(1, 10001)),
                TRUNC(DBMS_RANDOM.VALUE(1, 11)),
                ROUND(5 + DBMS_RANDOM.VALUE * 495, 2),
                rand_ts
            );
            v_item_id := v_item_id + 1;
        END LOOP;
        EXIT WHEN v_item_id > 2000000;
        IF MOD(i, 10000) = 0 THEN
            COMMIT;
            DBMS_OUTPUT.PUT_LINE('Order Items: ' || (v_item_id - 1));
        END IF;
    END LOOP;
    COMMIT;

    DBMS_OUTPUT.PUT_LINE('ORACLE DEMO DATA GENERATION COMPLETE');
    DBMS_OUTPUT.PUT_LINE('Total items: ' || (v_item_id - 1));
END;
/

-- Run it
SET SERVEROUTPUT ON SIZE UNLIMITED;
EXEC generate_demo_data;
