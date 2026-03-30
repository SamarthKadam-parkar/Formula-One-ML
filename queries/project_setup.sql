-- Using sysadmin role
USE ROLE SYSADMIN;

-- Creating DATABASE
CREATE DATABASE F1_ANALYTICS;
USE F1_ANALYTICS;

-- Creating Raw schema to stage data
CREATE SCHEMA F1_ANALYTICS.RAW;
CREATE SCHEMA F1_ANALYTICS.BRONZE;
CREATE SCHEMA F1_ANALYTICS.SILVER;
CREATE SCHEMA F1_ANALYTICS.GOLD

-- Creating Stage
CREATE STAGE F1_ANALYTICS.RAW.F1_ANALYTICS_DATA_STAGE
    DIRECTORY = ( ENABLE = TRUE )
    ENCRYPTION = ( TYPE = 'SNOWFLAKE_SSE' );

-- Creating a File Format to read JSON File
CREATE OR REPLACE FILE FORMAT F1_ANALYTICS.RAW.FF_JSON 
TYPE = 'JSON'
COMPRESSION = 'AUTO'
enable_octal = true
allow_duplicate = false 
strip_outer_array = true
strip_null_values = false 
ignore_utf8_errors = false;
