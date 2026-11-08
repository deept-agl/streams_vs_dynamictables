-- =====================================================================
-- 01_raw_events.sql
-- A ride-hailing app writes one EVENT every time a trip changes status:
--   REQUESTED -> STARTED -> COMPLETED   (or REQUESTED -> CANCELLED)
-- The raw table is append-only. Nobody updates it. Both pipelines read it.
-- =====================================================================

USE ROLE SYSADMIN;
USE WAREHOUSE RIDES_WH;
USE SCHEMA RIDES_DB.RAW;

CREATE OR REPLACE TABLE TRIP_EVENTS (
  EVENT_ID   STRING        DEFAULT UUID_STRING(),
  TRIP_ID    NUMBER        NOT NULL,
  DRIVER_ID  NUMBER        NOT NULL,
  CITY       STRING,
  STATUS     STRING        NOT NULL,   -- REQUESTED | STARTED | COMPLETED | CANCELLED
  FARE       NUMBER(10,2),             -- only filled on COMPLETED
  EVENT_TS   TIMESTAMP_NTZ NOT NULL
);

-- ---------------------------------------------------------------------
-- Batch 1: 300 trips. Each trip gets 1-3 events IN THE SAME BATCH.
-- (This is what real event feeds look like, and it's what breaks a naive MERGE.)
-- Step A: build the trips once in a temp table so RANDOM() is fixed per trip.
-- ---------------------------------------------------------------------
SET START_ID = (SELECT COALESCE(MAX(TRIP_ID), 1000) FROM TRIP_EVENTS);

CREATE OR REPLACE TEMPORARY TABLE TMP_TRIPS AS
SELECT
  $START_ID + ROW_NUMBER() OVER (ORDER BY SEQ4())                       AS TRIP_ID,
  UNIFORM(1, 20, RANDOM())                                               AS DRIVER_ID,
  ARRAY_CONSTRUCT('Bengaluru','Mumbai','Delhi','Pune')[UNIFORM(0, 3, RANDOM())]::STRING AS CITY,
  UNIFORM(150, 900, RANDOM())::NUMBER(10,2)                              AS TRIP_FARE,
  UNIFORM(1, 10, RANDOM())                                               AS OUTCOME_ROLL, -- 1 = cancelled
  UNIFORM(1, 3, RANDOM())                                                AS STAGE_REACHED,
  DATEADD(MINUTE, -UNIFORM(60, 4320, RANDOM()), CURRENT_TIMESTAMP())::TIMESTAMP_NTZ AS REQUESTED_TS
FROM TABLE(GENERATOR(ROWCOUNT => 300));

-- Step B: explode each trip into its events.
INSERT INTO TRIP_EVENTS (TRIP_ID, DRIVER_ID, CITY, STATUS, FARE, EVENT_TS)
SELECT
  t.TRIP_ID, t.DRIVER_ID, t.CITY,
  s.STATUS,
  IFF(s.STATUS = 'COMPLETED', t.TRIP_FARE, NULL),
  DATEADD(MINUTE, s.OFFSET_MIN, t.REQUESTED_TS)
FROM TMP_TRIPS t
JOIN (
  SELECT 'REQUESTED' AS STATUS, 1 AS STEP, 0  AS OFFSET_MIN UNION ALL
  SELECT 'STARTED',             2,         6               UNION ALL
  SELECT 'COMPLETED',           3,         32
) s
  ON s.STEP <= t.STAGE_REACHED
 AND t.OUTCOME_ROLL > 1                     -- normal trips
UNION ALL
SELECT t.TRIP_ID, t.DRIVER_ID, t.CITY, c.STATUS, NULL, DATEADD(MINUTE, c.OFFSET_MIN, t.REQUESTED_TS)
FROM TMP_TRIPS t
JOIN (SELECT 'REQUESTED' AS STATUS, 0 AS OFFSET_MIN UNION ALL SELECT 'CANCELLED', 3) c
  ON t.OUTCOME_ROLL = 1;                    -- ~10% cancelled

-- Look at the data: one trip, several rows.
SELECT TRIP_ID, STATUS, FARE, EVENT_TS
FROM TRIP_EVENTS
WHERE TRIP_ID IN (SELECT TRIP_ID FROM TRIP_EVENTS GROUP BY TRIP_ID HAVING COUNT(*) = 3 LIMIT 1)
ORDER BY EVENT_TS;

SELECT COUNT(*) AS EVENTS, COUNT(DISTINCT TRIP_ID) AS TRIPS FROM TRIP_EVENTS;

-- What the business wants (both pipelines must produce these two tables):
--   1. TRIPS_CURRENT          : one row per trip, its LATEST status
--   2. DRIVER_DAILY_EARNINGS  : per driver per day, completed trips and total fare
