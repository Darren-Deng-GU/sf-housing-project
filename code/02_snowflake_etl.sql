-- ============================================================
-- 02_snowflake_etl.sql
-- ============================================================
-- Snowflake ETL pipeline for SF Bay Area Housing Project
--
-- This script:
--   1. Creates database, schema, warehouse, and file format
--   2. Creates an external stage pointing to S3
--   3. Loads raw data via COPY INTO
--   4. Cleans and transforms data into analytical tables
--   5. Creates aggregation views for visualization
--
-- Prerequisites:
--   - Raw CSV files uploaded to S3 (run 01_data_ingestion.py first)
--   - Snowflake account with ACCOUNTADMIN or equivalent role
--   - S3 bucket access configured (storage integration or keys)
--
-- Usage:
--   Run each section sequentially in Snowflake Worksheet or SnowSQL
-- ============================================================


-- ============================================================
-- SECTION 1: INFRASTRUCTURE SETUP
-- ============================================================

-- Create dedicated database and schema
CREATE DATABASE IF NOT EXISTS SF_HOUSING;
USE DATABASE SF_HOUSING;

CREATE SCHEMA IF NOT EXISTS RAW;        -- raw/staging layer
CREATE SCHEMA IF NOT EXISTS ANALYTICS;  -- transformed/clean layer

-- Create warehouse (XS is sufficient for this data volume)
-- Auto-suspend after 60 seconds to minimize cost
CREATE WAREHOUSE IF NOT EXISTS HOUSING_WH
  WAREHOUSE_SIZE = 'X-SMALL'
  AUTO_SUSPEND = 60
  AUTO_RESUME = TRUE
  INITIALLY_SUSPENDED = TRUE;

USE WAREHOUSE HOUSING_WH;

-- CSV file format (handles quoted fields, headers, etc.)
CREATE OR REPLACE FILE FORMAT RAW.CSV_FORMAT
  TYPE = 'CSV'
  FIELD_OPTIONALLY_ENCLOSED_BY = '"'
  SKIP_HEADER = 1
  NULL_IF = ('', 'NULL', 'null', 'NA')
  FIELD_DELIMITER = ','
  RECORD_DELIMITER = '\n'
  ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE;


-- ============================================================
-- SECTION 2: EXTERNAL STAGE (S3)
-- ============================================================

-- Option A: Using storage integration (recommended for production)
-- You would first create a storage integration:
--
-- CREATE OR REPLACE STORAGE INTEGRATION S3_HOUSING_INT
--   TYPE = EXTERNAL_STAGE
--   STORAGE_PROVIDER = 'S3'
--   ENABLED = TRUE
--   STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::<account-id>:role/<role-name>'
--   STORAGE_ALLOWED_LOCATIONS = ('s3://sf-housing-project/');
--
-- CREATE OR REPLACE STAGE RAW.S3_STAGE
--   STORAGE_INTEGRATION = S3_HOUSING_INT
--   URL = 's3://sf-housing-project/raw/'
--   FILE_FORMAT = RAW.CSV_FORMAT;

-- Option B: Using AWS keys directly (simpler for coursework)
CREATE OR REPLACE STAGE RAW.S3_STAGE
  URL = 's3://sf-housing-project/raw/'
  CREDENTIALS = (
    AWS_KEY_ID = '***'          -- ← replace with your key
    AWS_SECRET_KEY = '***'      -- ← replace with your secret
  )
  FILE_FORMAT = RAW.CSV_FORMAT;

-- Verify stage is accessible
LIST @RAW.S3_STAGE;


-- ============================================================
-- SECTION 3: RAW TABLES + DATA LOADING
-- ============================================================

USE SCHEMA RAW;

-- 3A. Building Permits
CREATE OR REPLACE TABLE RAW.BUILDING_PERMITS (
    permit_number           VARCHAR,
    permit_type             NUMBER,
    permit_type_definition  VARCHAR,
    permit_creation_date    VARCHAR,
    block                   VARCHAR,
    lot                     VARCHAR,
    street_number           VARCHAR,
    street_number_suffix    VARCHAR,
    street_name             VARCHAR,
    street_suffix           VARCHAR,
    unit                    VARCHAR,
    unit_suffix             VARCHAR,
    description             VARCHAR,
    current_status          VARCHAR,
    current_status_date     VARCHAR,
    filed_date              VARCHAR,
    issued_date             VARCHAR,
    completed_date          VARCHAR,
    first_construction_document_date VARCHAR,
    structural_notification VARCHAR,
    number_of_existing_stories NUMBER,
    number_of_proposed_stories NUMBER,
    voluntary_soft_story_retrofit VARCHAR,
    fire_only_permit        VARCHAR,
    permit_expiration_date  VARCHAR,
    estimated_cost          NUMBER,
    revised_cost            NUMBER,
    existing_use            VARCHAR,
    existing_units          NUMBER,
    proposed_use            VARCHAR,
    proposed_units          NUMBER,
    plansets                NUMBER,
    tidf_compliance         VARCHAR,
    existing_construction_type VARCHAR,
    existing_construction_type_description VARCHAR,
    proposed_construction_type VARCHAR,
    proposed_construction_type_description VARCHAR,
    site_permit             VARCHAR,
    supervisor_district     VARCHAR,
    neighborhoods_analysis_boundaries VARCHAR,
    zipcode                 VARCHAR,
    location                VARCHAR,
    record_id               VARCHAR
);

COPY INTO RAW.BUILDING_PERMITS
  FROM @RAW.S3_STAGE/Building_Permits.csv
  FILE_FORMAT = RAW.CSV_FORMAT
  ON_ERROR = 'CONTINUE';

-- Verify load
SELECT COUNT(*) AS permit_count FROM RAW.BUILDING_PERMITS;
-- Expected: ~200,000+ rows


-- 3B. Land Use
CREATE OR REPLACE TABLE RAW.LAND_USE (
    the_geom        VARCHAR,
    objectid        NUMBER,
    blklot          VARCHAR,
    block           VARCHAR,
    lot             VARCHAR,
    from_st         VARCHAR,
    to_st           VARCHAR,
    st_type         VARCHAR,
    street          VARCHAR,
    odd_even        VARCHAR,
    mapblklot       VARCHAR,
    landuse         VARCHAR,
    restype         VARCHAR,
    bldgsqft        NUMBER,
    yrbuilt         NUMBER,
    units           NUMBER,
    mips_units      NUMBER,
    pdr_units       NUMBER,
    cie_units       NUMBER,
    med_units       NUMBER,
    ret_units       NUMBER,
    visitor_units   NUMBER,
    cultural_units  NUMBER
);

COPY INTO RAW.LAND_USE
  FROM @RAW.S3_STAGE/SF_Land_Use_2023.csv
  FILE_FORMAT = RAW.CSV_FORMAT
  ON_ERROR = 'CONTINUE';

SELECT COUNT(*) AS landuse_count FROM RAW.LAND_USE;


-- 3C. House Price Index
CREATE OR REPLACE TABLE RAW.HOUSE_PRICE_INDEX (
    observation_date VARCHAR,
    ATNHPIUS41884Q   FLOAT
);

COPY INTO RAW.HOUSE_PRICE_INDEX
  FROM @RAW.S3_STAGE/ATNHPIUS41884Q.csv
  FILE_FORMAT = RAW.CSV_FORMAT
  ON_ERROR = 'CONTINUE';

SELECT COUNT(*) AS hpi_count FROM RAW.HOUSE_PRICE_INDEX;


-- 3D. Census ACS
CREATE OR REPLACE TABLE RAW.CENSUS_ACS (
    name           VARCHAR,
    median_rent    NUMBER,
    median_income  NUMBER,
    pop_total      NUMBER,
    pop_white      NUMBER,
    state          VARCHAR,
    county         VARCHAR,
    tract          VARCHAR,
    geoid          VARCHAR
);

COPY INTO RAW.CENSUS_ACS
  FROM @RAW.S3_STAGE/census_acs_bayarea.csv
  FILE_FORMAT = RAW.CSV_FORMAT
  ON_ERROR = 'CONTINUE';

SELECT COUNT(*) AS census_count FROM RAW.CENSUS_ACS;


-- ============================================================
-- SECTION 4: DATA TRANSFORMATION (RAW → ANALYTICS)
-- ============================================================

USE SCHEMA ANALYTICS;

-- ────────────────────────────────────────
-- 4A. Clean Permits Table
-- ────────────────────────────────────────
CREATE OR REPLACE TABLE ANALYTICS.PERMITS_CLEAN AS
SELECT
    permit_number,
    INITCAP(permit_type_definition)         AS permit_type,
    TRY_TO_TIMESTAMP(filed_date)            AS filed_date,
    TRY_TO_TIMESTAMP(issued_date)           AS issued_date,
    TRY_TO_TIMESTAMP(completed_date)        AS completed_date,
    YEAR(TRY_TO_TIMESTAMP(issued_date))     AS issued_year,
    existing_use,
    proposed_use,
    estimated_cost,
    existing_units,
    proposed_units,
    neighborhoods_analysis_boundaries       AS neighborhood,
    zipcode,
    supervisor_district,
    -- Calculate processing time in days
    DATEDIFF('day',
        TRY_TO_TIMESTAMP(filed_date),
        TRY_TO_TIMESTAMP(issued_date)
    ) AS processing_days
FROM RAW.BUILDING_PERMITS
WHERE TRY_TO_TIMESTAMP(issued_date) IS NOT NULL;

-- Verify
SELECT
    COUNT(*)                           AS total_rows,
    MIN(issued_year)                   AS min_year,
    MAX(issued_year)                   AS max_year,
    AVG(processing_days)               AS avg_processing_days,
    COUNT(DISTINCT neighborhood)       AS distinct_neighborhoods
FROM ANALYTICS.PERMITS_CLEAN;


-- ────────────────────────────────────────
-- 4B. Clean Land Use Table
-- ────────────────────────────────────────
CREATE OR REPLACE TABLE ANALYTICS.LAND_USE_CLEAN AS
SELECT
    blklot,
    block,
    lot,
    street,
    landuse,
    TRIM(restype)                      AS restype,
    bldgsqft,
    yrbuilt,
    units,
    the_geom
FROM RAW.LAND_USE
WHERE landuse IS NOT NULL;


-- ────────────────────────────────────────
-- 4C. Clean HPI Table
-- ────────────────────────────────────────
CREATE OR REPLACE TABLE ANALYTICS.HPI_CLEAN AS
SELECT
    TRY_TO_DATE(observation_date)      AS observation_date,
    ATNHPIUS41884Q                     AS hpi_value
FROM RAW.HOUSE_PRICE_INDEX
WHERE TRY_TO_DATE(observation_date) IS NOT NULL
ORDER BY observation_date;


-- ────────────────────────────────────────
-- 4D. Clean Census Table
-- ────────────────────────────────────────
CREATE OR REPLACE TABLE ANALYTICS.CENSUS_CLEAN AS
SELECT
    name                               AS tract_name,
    geoid,
    state,
    county,
    tract,
    NULLIF(median_rent, -666666666)    AS median_rent,    -- Census uses negative sentinel for missing
    NULLIF(median_income, -666666666)  AS median_income,
    pop_total,
    pop_white,
    CASE
        WHEN pop_total > 0 THEN ROUND(pop_white / pop_total * 100, 1)
        ELSE NULL
    END AS pct_white
FROM RAW.CENSUS_ACS;


-- ============================================================
-- SECTION 5: ANALYTICAL VIEWS (for visualization export)
-- ============================================================

-- ────────────────────────────────────────
-- View 1: Permit counts by year and type
-- (→ Permits Trend line chart)
-- ────────────────────────────────────────
CREATE OR REPLACE VIEW ANALYTICS.VW_PERMITS_TREND AS
SELECT
    issued_year,
    permit_type,
    COUNT(*)                           AS permit_count
FROM ANALYTICS.PERMITS_CLEAN
WHERE issued_year BETWEEN 2000 AND 2024
GROUP BY issued_year, permit_type
ORDER BY issued_year, permit_type;


-- ────────────────────────────────────────
-- View 2: Processing time by permit type
-- (→ Violin / Box plot)
-- ────────────────────────────────────────
CREATE OR REPLACE VIEW ANALYTICS.VW_PROCESSING_TIME AS
SELECT
    permit_type,
    processing_days
FROM ANALYTICS.PERMITS_CLEAN
WHERE processing_days IS NOT NULL
  AND processing_days BETWEEN 0 AND 1000;   -- exclude outliers


-- ────────────────────────────────────────
-- View 3: Top neighborhoods by permit count
-- (→ Lollipop chart)
-- ────────────────────────────────────────
CREATE OR REPLACE VIEW ANALYTICS.VW_NEIGHBORHOOD_ACTIVITY AS
SELECT
    neighborhood,
    COUNT(*)                           AS total_permits,
    AVG(processing_days)               AS avg_processing_days,
    SUM(estimated_cost)                AS total_estimated_cost
FROM ANALYTICS.PERMITS_CLEAN
WHERE neighborhood IS NOT NULL
GROUP BY neighborhood
ORDER BY total_permits DESC
LIMIT 20;


-- ────────────────────────────────────────
-- View 4: Change of Use (Existing → Proposed)
-- (→ Sankey diagram)
-- ────────────────────────────────────────
CREATE OR REPLACE VIEW ANALYTICS.VW_CHANGE_OF_USE AS
SELECT
    INITCAP(existing_use)              AS existing_use,
    INITCAP(proposed_use)              AS proposed_use,
    COUNT(*)                           AS flow_count
FROM ANALYTICS.PERMITS_CLEAN
WHERE existing_use IS NOT NULL
  AND proposed_use IS NOT NULL
  AND existing_use != proposed_use
GROUP BY existing_use, proposed_use
ORDER BY flow_count DESC
LIMIT 15;


-- ────────────────────────────────────────
-- View 5: Residential type distribution
-- (→ Bar chart)
-- ────────────────────────────────────────
CREATE OR REPLACE VIEW ANALYTICS.VW_HOUSING_TYPES AS
SELECT
    restype,
    COUNT(*) AS parcel_count
FROM ANALYTICS.LAND_USE_CLEAN
WHERE restype IS NOT NULL
  AND TRIM(restype) != ''
GROUP BY restype
ORDER BY parcel_count DESC;


-- ────────────────────────────────────────
-- View 6: Cross-analysis — Neighborhood income vs processing time
-- (→ Scatter plot — YOUR unique addition)
-- ────────────────────────────────────────
CREATE OR REPLACE VIEW ANALYTICS.VW_NEIGHBORHOOD_INCOME_VS_WAIT AS
SELECT
    p.neighborhood,
    AVG(p.processing_days)                     AS avg_wait_days,
    COUNT(*)                                   AS permit_count,
    ROUND(AVG(c.median_income), 0)             AS avg_median_income,
    ROUND(AVG(c.median_rent), 0)               AS avg_median_rent
FROM ANALYTICS.PERMITS_CLEAN p
LEFT JOIN ANALYTICS.CENSUS_CLEAN c
    ON p.zipcode = c.tract                     -- approximate join; see note below
WHERE p.neighborhood IS NOT NULL
  AND p.processing_days BETWEEN 0 AND 1000
  AND c.median_income > 0
GROUP BY p.neighborhood
HAVING COUNT(*) >= 50
ORDER BY avg_wait_days DESC;

-- NOTE: The join between permits (by neighborhood/zipcode) and census (by tract)
-- is approximate. In production, you would use a spatial join via lat/lon.
-- This is noted as a limitation in the paper.


-- ============================================================
-- SECTION 6: EXPORT QUERIES
-- ============================================================
-- Run these queries and export results as CSV for visualization.
-- In Snowflake Worksheet: click the download button after running.
-- Or use SnowSQL: snowsql -q "SELECT * FROM ..." -o output.csv

-- Export 1: Permits Trend
SELECT * FROM ANALYTICS.VW_PERMITS_TREND;

-- Export 2: Processing Time (sample for visualization)
SELECT * FROM ANALYTICS.VW_PROCESSING_TIME
SAMPLE (10000 ROWS);  -- sample for visualization performance

-- Export 3: Neighborhood Activity
SELECT * FROM ANALYTICS.VW_NEIGHBORHOOD_ACTIVITY;

-- Export 4: Change of Use
SELECT * FROM ANALYTICS.VW_CHANGE_OF_USE;

-- Export 5: Housing Types
SELECT * FROM ANALYTICS.VW_HOUSING_TYPES;

-- Export 6: HPI
SELECT * FROM ANALYTICS.HPI_CLEAN;

-- Export 7: Census
SELECT * FROM ANALYTICS.CENSUS_CLEAN;

-- Export 8: Income vs Wait (Cross-analysis)
SELECT * FROM ANALYTICS.VW_NEIGHBORHOOD_INCOME_VS_WAIT;


-- ============================================================
-- SECTION 7: CLEANUP (run when done to save costs)
-- ============================================================
-- ALTER WAREHOUSE HOUSING_WH SUSPEND;
