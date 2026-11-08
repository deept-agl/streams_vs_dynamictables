-- =====================================================================
-- 03_dynamic_tables.sql   PIPELINE #2: Dynamic Tables
-- You tell Snowflake WHAT the result is. It works out the HOW and the WHEN.
-- =====================================================================

USE ROLE SYSADMIN;
USE WAREHOUSE RIDES_WH;
USE SCHEMA RIDES_DB.DT_WAY;

-- 1. Latest status per trip. Same QUALIFY as the fix in pipeline #1,
--    but no stream, no MERGE, no late-event guard: it's just a SELECT.
--    TARGET_LAG = DOWNSTREAM: refresh only when something downstream needs it.
CREATE OR REPLACE DYNAMIC TABLE TRIPS_CURRENT
  TARGET_LAG = DOWNSTREAM
  WAREHOUSE = RIDES_WH
  REFRESH_MODE = INCREMENTAL
AS
SELECT TRIP_ID, DRIVER_ID, CITY, STATUS, FARE, EVENT_TS AS LAST_EVENT_TS
FROM RIDES_DB.RAW.TRIP_EVENTS
QUALIFY ROW_NUMBER() OVER (PARTITION BY TRIP_ID ORDER BY EVENT_TS DESC) = 1;

-- 2. Earnings, built on top of the first dynamic table.
--    TARGET_LAG = '1 minute': data is at most ~1 minute behind raw.
CREATE OR REPLACE DYNAMIC TABLE DRIVER_DAILY_EARNINGS
  TARGET_LAG = '1 minute'
  WAREHOUSE = RIDES_WH
  REFRESH_MODE = INCREMENTAL
AS
SELECT DRIVER_ID,
       LAST_EVENT_TS::DATE AS TRIP_DATE,
       COUNT(*)            AS COMPLETED_TRIPS,
       SUM(FARE)           AS TOTAL_FARE
FROM TRIPS_CURRENT
WHERE STATUS = 'COMPLETED'
GROUP BY 1, 2;

-- That's the whole pipeline. Two statements.

-- See the chain (Snowsight: Monitoring > Dynamic Tables > graph view also shows it)
SHOW DYNAMIC TABLES IN SCHEMA RIDES_DB.DT_WAY
  ->> SELECT "name", "target_lag", "refresh_mode", "refresh_mode_reason", "scheduling_state" FROM $1;

-- =====================================================================
-- 3. THE DYNAMIC TABLE TRAP: AUTO mode can quietly pick FULL refresh.
--    Add one innocent "loaded at" column...
-- =====================================================================
CREATE OR REPLACE DYNAMIC TABLE TRIPS_CURRENT_AUTO
  TARGET_LAG = DOWNSTREAM
  WAREHOUSE = RIDES_WH
  -- no REFRESH_MODE  -> AUTO
AS
SELECT TRIP_ID, DRIVER_ID, CITY, STATUS, FARE, EVENT_TS AS LAST_EVENT_TS,
       CURRENT_TIMESTAMP() AS LOADED_AT          -- non-deterministic in SELECT
FROM RIDES_DB.RAW.TRIP_EVENTS
QUALIFY ROW_NUMBER() OVER (PARTITION BY TRIP_ID ORDER BY EVENT_TS DESC) = 1;

SHOW DYNAMIC TABLES LIKE 'TRIPS_CURRENT_AUTO' IN SCHEMA RIDES_DB.DT_WAY
  ->> SELECT "name", "refresh_mode", "refresh_mode_reason" FROM $1;
-- refresh_mode = FULL. Every refresh recomputes ALL trips. No error, no warning in your face.
-- AUTO is decided once, at creation, and can't be changed with ALTER.
-- Fix: drop the column (or use METADATA$ROW_LAST_COMMIT_TIME), and set REFRESH_MODE = INCREMENTAL
-- so creation FAILS loudly instead of silently going full.

DROP DYNAMIC TABLE TRIPS_CURRENT_AUTO;
