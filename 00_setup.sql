-- =====================================================================
-- Streams & Tasks vs Dynamic Tables: 1 pipeline, 2 ways
-- 00_setup.sql  |  runs on a Snowflake free trial (Standard edition is enough)
-- =====================================================================

USE ROLE ACCOUNTADMIN;

-- SYSADMIN owns everything we build. It needs EXECUTE TASK to resume tasks.
GRANT EXECUTE TASK ON ACCOUNT TO ROLE SYSADMIN;

USE ROLE SYSADMIN;

CREATE WAREHOUSE IF NOT EXISTS RIDES_WH
  WAREHOUSE_SIZE = XSMALL
  AUTO_SUSPEND = 60          -- seconds; idle warehouse goes to sleep
  AUTO_RESUME = TRUE
  INITIALLY_SUSPENDED = TRUE;

CREATE DATABASE IF NOT EXISTS RIDES_DB;
CREATE SCHEMA IF NOT EXISTS RIDES_DB.RAW;          -- events land here
CREATE SCHEMA IF NOT EXISTS RIDES_DB.STREAMS_WAY;  -- pipeline #1: stream + MERGE + tasks
CREATE SCHEMA IF NOT EXISTS RIDES_DB.DT_WAY;       -- pipeline #2: dynamic tables

USE WAREHOUSE RIDES_WH;
USE SCHEMA RIDES_DB.RAW;
