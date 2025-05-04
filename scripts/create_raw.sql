/*------------------------------------------------------------------------------
  Nubank Case — Bootstrap completo
  - Creates dedicated XS warehouse
  - Creates DB, RAW schema and file-format and stage
  - Create RAW tables
------------------------------------------------------------------------------*/

-----------------------------------------------------------------
-- 0. Parameters
-----------------------------------------------------------------
USE ROLE SYSADMIN;
SET WH_NAME         = 'NUBANK_CASE_WH_XS';
SET WH_DBT_NAME     = 'NUBANK_CASE_DBT_WH_XS';
SET DB_NAME         = 'NUBANK_CASE';
SET RAW_SCHEMA      = 'RAW';
SET STAGE_NAME      = 'NUBANK_STAGE';
SET FILE_FORMAT     = 'CSV_RAW';
SET DBT_ROLE        = 'ANALYTICS'; -- dbt rol

-----------------------------------------------------------------
-- 1. Compute warehouse dedicated (XS, autosuspend 60 s)
-----------------------------------------------------------------
CREATE OR REPLACE WAREHOUSE IDENTIFIER($WH_NAME)
  WAREHOUSE_SIZE      = 'XSMALL'
  AUTO_SUSPEND        = 60
  AUTO_RESUME         = TRUE
  INITIALLY_SUSPENDED = TRUE
  COMMENT = 'Dedicated warehouse for exclusive use for the Nubank case (minimum cost)';

CREATE OR REPLACE WAREHOUSE IDENTIFIER($WH_DBT_NAME)
  WAREHOUSE_SIZE      = 'XSMALL'
  AUTO_SUSPEND        = 60
  AUTO_RESUME         = TRUE
  INITIALLY_SUSPENDED = TRUE
  COMMENT = 'Dedicated warehouse for Nubank dbt (minimum cost)';

-----------------------------------------------------------------
-- 2. Database creation and raw schema
-----------------------------------------------------------------
CREATE OR REPLACE TRANSIENT DATABASE IDENTIFIER($DB_NAME)
  COMMENT = 'Case study database for Nubank interview';

SET FULL_RAW_SCHEMA = $DB_NAME || '.' || $RAW_SCHEMA;

CREATE OR REPLACE TRANSIENT SCHEMA IDENTIFIER($FULL_RAW_SCHEMA)
  COMMENT = 'Landing layer – raw CSV load';

-----------------------------------------------------------------
-- 3. File format and internal stage for CSV
-----------------------------------------------------------------

SET FULL_FILE_FORMAT_NAME = $DB_NAME || '.' || $RAW_SCHEMA || '.' || $FILE_FORMAT;

CREATE OR REPLACE FILE FORMAT IDENTIFIER($FULL_FILE_FORMAT_NAME)
  TYPE                       = 'CSV'
  FIELD_DELIMITER            = ','
  SKIP_HEADER                = 1
  FIELD_OPTIONALLY_ENCLOSED_BY = '"'
  EMPTY_FIELD_AS_NULL        = TRUE
  COMMENT = 'Standard CSV format for Nubank case';

SET FULL_INTERNAL_STAGE_NAME = $DB_NAME || '.' || $RAW_SCHEMA || '.' || $STAGE_NAME;

CREATE OR REPLACE STAGE IDENTIFIER($FULL_INTERNAL_STAGE_NAME)
  FILE_FORMAT = (FORMAT_NAME = $FULL_FILE_FORMAT_NAME)
  COMMENT     = 'Internal stage holding raw CSV files for the case';

-----------------------------------------------------------------
-- 4. RAW Tables
-----------------------------------------------------------------
create or replace TRANSIENT TABLE NUBANK_CASE.RAW.ACCOUNTS (
    ACCOUNT_ID NUMBER(38,0),
    CUSTOMER_ID NUMBER(38,0),
    CREATED_AT TIMESTAMP_NTZ(9),
    STATUS VARCHAR(16777216),
    ACCOUNT_BRANCH NUMBER(38,0),
    ACCOUNT_CHECK_DIGIT NUMBER(38,0),
    ACCOUNT_NUMBER NUMBER(38,0)
);

create or replace TRANSIENT TABLE NUBANK_CASE.RAW.CITY (
    CITY VARCHAR(16777216),
    STATE_ID NUMBER(38,0),
    CITY_ID NUMBER(38,0)
);

create or replace TRANSIENT TABLE NUBANK_CASE.RAW.COUNTRY (
    COUNTRY VARCHAR(16777216),
    COUNTRY_ID NUMBER(38,0)
);

create or replace TRANSIENT TABLE NUBANK_CASE.RAW.CUSTOMERS (
    CUSTOMER_ID NUMBER(38,0),
    FIRST_NAME VARCHAR(16777216),
    LAST_NAME VARCHAR(16777216),
    CUSTOMER_CITY NUMBER(38,0),
    CPF NUMBER(38,0),
    COUNTRY_NAME VARCHAR(16777216)
);

create or replace TRANSIENT TABLE NUBANK_CASE.RAW.D_MONTH (
    MONTH_ID NUMBER(38,0),
    ACTION_MONTH NUMBER(38,0)
);

create or replace TRANSIENT TABLE NUBANK_CASE.RAW.D_TIME (
    TIME_ID NUMBER(38,0),
    ACTION_TIMESTAMP TIMESTAMP_NTZ(9),
    WEEK_ID NUMBER(38,0),
    MONTH_ID NUMBER(38,0),
    YEAR_ID NUMBER(38,0),
    WEEKDAY_ID NUMBER(38,0)
);

create or replace TRANSIENT TABLE NUBANK_CASE.RAW.D_WEEK (
    WEEK_ID NUMBER(38,0),
    ACTION_WEEK NUMBER(38,0)
);

create or replace TRANSIENT TABLE NUBANK_CASE.RAW.D_WEEKDAY (
    WEEKDAY_ID NUMBER(38,0),
    ACTION_WEEKDAY NUMBER(38,0)
);

create or replace TRANSIENT TABLE NUBANK_CASE.RAW.D_YEAR (
    YEAR_ID NUMBER(38,0),
    ACTION_YEAR NUMBER(38,0)
);

create or replace TRANSIENT TABLE NUBANK_CASE.RAW.PIX_MOVEMENTS (
    ID NUMBER(38,0),
    ACCOUNT_ID NUMBER(38,0),
    PIX_AMOUNT NUMBER(38,2),
    PIX_REQUESTED_AT NUMBER(38,0),
    PIX_COMPLETED_AT VARCHAR(16777216),
    STATUS VARCHAR(16777216),
    IN_OR_OUT VARCHAR(16777216)
);

create or replace TRANSIENT TABLE NUBANK_CASE.RAW.STATE (
    STATE VARCHAR(16777216),
    COUNTRY_ID NUMBER(38,0),
    STATE_ID NUMBER(38,0)
);

create or replace TRANSIENT TABLE NUBANK_CASE.RAW.TRANSFER_INS (
    ID NUMBER(38,0),
    ACCOUNT_ID NUMBER(38,0),
    AMOUNT NUMBER(38,2),
    TRANSACTION_REQUESTED_AT NUMBER(38,0),
    TRANSACTION_COMPLETED_AT VARCHAR(16777216),
    STATUS VARCHAR(16777216)
);

create or replace TRANSIENT TABLE NUBANK_CASE.RAW.TRANSFER_OUTS (
    ID NUMBER(38,0),
    ACCOUNT_ID NUMBER(38,0),
    AMOUNT NUMBER(38,2),
    TRANSACTION_REQUESTED_AT NUMBER(38,0),
    TRANSACTION_COMPLETED_AT VARCHAR(16777216),
    STATUS VARCHAR(16777216)
);

-----------------------------------------------------------------
-- 5. (Optional) Truncate and insert using COPY into
-----------------------------------------------------------------
TRUNCATE TABLE IDENTIFIER($DB_NAME || '.' || $RAW_SCHEMA || '.ACCOUNTS');
COPY INTO IDENTIFIER($DB_NAME || '.' || $RAW_SCHEMA || '.ACCOUNTS')
  FROM @IDENTIFIER($DB_NAME || '.' || $RAW_SCHEMA || '.' || $STAGE_NAME)/accounts.csv
  FILE_FORMAT = (FORMAT_NAME = IDENTIFIER($DB_NAME || '.' || $RAW_SCHEMA || '.' || $FILE_FORMAT))
  ON_ERROR = 'ABORT_STATEMENT';

-- Repeat for other tables

-----------------------------------------------------------------
-- 6. Minimum grants
-----------------------------------------------------------------
USE ROLE SECURITYADMIN;
CREATE OR REPLACE ROLE IDENTIFIER($DBT_ROLE);

GRANT USAGE ON WAREHOUSE IDENTIFIER($WH_DBT_NAME) TO ROLE IDENTIFIER($DBT_ROLE);
GRANT OPERATE ON WAREHOUSE IDENTIFIER($WH_DBT_NAME) TO ROLE IDENTIFIER($DBT_ROLE);

GRANT USAGE ON DATABASE IDENTIFIER($DB_NAME) TO ROLE IDENTIFIER($DBT_ROLE);

SET FULL_SCHEMA_NAME = $DB_NAME||'.'||$RAW_SCHEMA;
GRANT USAGE ON SCHEMA IDENTIFIER($FULL_SCHEMA_NAME) TO ROLE IDENTIFIER($DBT_ROLE);

GRANT SELECT ON ALL TABLES IN SCHEMA IDENTIFIER($FULL_SCHEMA_NAME) 
  TO ROLE IDENTIFIER($DBT_ROLE);

GRANT SELECT ON FUTURE TABLES IN SCHEMA IDENTIFIER($FULL_SCHEMA_NAME) 
  TO ROLE IDENTIFIER($DBT_ROLE);

-----------------------------------------------------------------
-- 7. Confirmation
-----------------------------------------------------------------
SELECT '✅  Warehouse, DB y esquema RAW listos' AS STATUS;
