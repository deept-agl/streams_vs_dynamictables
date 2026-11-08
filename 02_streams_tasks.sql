-- =====================================================================
-- 02_streams_tasks.sql   PIPELINE #1: Stream + MERGE + Tasks
-- You tell Snowflake HOW: track changes, merge them, schedule it, chain it.
-- =====================================================================

USE ROLE SYSADMIN;
USE WAREHOUSE RIDES_WH;
USE SCHEMA RIDES_DB.STREAMS_WAY;

-- 1. A stream = a bookmark on the raw table. It returns rows added since
--    the last time a DML statement consumed it.
--    SHOW_INITIAL_ROWS = TRUE so batch 1 (already loaded) counts as "new".
CREATE OR REPLACE STREAM TRIP_EVENTS_STREAM
  ON TABLE RIDES_DB.RAW.TRIP_EVENTS
  APPEND_ONLY = TRUE
  SHOW_INITIAL_ROWS = TRUE;

SELECT COUNT(*) FROM TRIP_EVENTS_STREAM;   -- reading a stream does NOT move the bookmark
SELECT COUNT(*) FROM TRIP_EVENTS_STREAM;   -- same number again

-- 2. Target tables
CREATE OR REPLACE TABLE TRIPS_CURRENT (
  TRIP_ID       NUMBER,
  DRIVER_ID     NUMBER,
  CITY          STRING,
  STATUS        STRING,
  FARE          NUMBER(10,2),
  LAST_EVENT_TS TIMESTAMP_NTZ
);

CREATE OR REPLACE TABLE DRIVER_DAILY_EARNINGS (
  DRIVER_ID       NUMBER,
  TRIP_DATE       DATE,
  COMPLETED_TRIPS NUMBER,
  TOTAL_FARE      NUMBER(12,2)
);

-- =====================================================================
-- 3. The MERGE, in a procedure so a task can call it.
--    A batch can hold several events for the same trip, so first keep
--    one row per trip (newest event wins), and never let an older (late)
--    event overwrite a newer one that's already in the target.
-- =====================================================================
CREATE OR REPLACE PROCEDURE MERGE_TRIPS()
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN
  MERGE INTO TRIPS_CURRENT t
  USING (
    SELECT TRIP_ID, DRIVER_ID, CITY, STATUS, FARE, EVENT_TS
    FROM TRIP_EVENTS_STREAM
    QUALIFY ROW_NUMBER() OVER (PARTITION BY TRIP_ID ORDER BY EVENT_TS DESC) = 1
  ) s
    ON t.TRIP_ID = s.TRIP_ID
  WHEN MATCHED AND s.EVENT_TS > t.LAST_EVENT_TS THEN UPDATE SET   -- late events can't go backwards
    t.STATUS = s.STATUS, t.FARE = s.FARE, t.LAST_EVENT_TS = s.EVENT_TS
  WHEN NOT MATCHED THEN INSERT (TRIP_ID, DRIVER_ID, CITY, STATUS, FARE, LAST_EVENT_TS)
    VALUES (s.TRIP_ID, s.DRIVER_ID, s.CITY, s.STATUS, s.FARE, s.EVENT_TS);
  RETURN 'merged ' || SQLROWCOUNT || ' rows';
END;
$$;

CALL MERGE_TRIPS();
SELECT COUNT(*) AS ROWS_IN_TARGET, COUNT(DISTINCT TRIP_ID) AS TRIPS FROM TRIPS_CURRENT;  -- equal now
SELECT COUNT(*) FROM TRIP_EVENTS_STREAM;   -- 0: the MERGE consumed the stream

-- =====================================================================
-- 4. Earnings. With streams, every downstream table is more code.
--    Simplest honest version: rebuild the aggregate after each merge.
--    (Doing this incrementally = another stream + another MERGE.)
-- =====================================================================
INSERT OVERWRITE INTO DRIVER_DAILY_EARNINGS
SELECT DRIVER_ID, LAST_EVENT_TS::DATE, COUNT(*), SUM(FARE)
FROM TRIPS_CURRENT
WHERE STATUS = 'COMPLETED'
GROUP BY 1, 2;

-- =====================================================================
-- 5. Schedule it: a root task (only runs if the stream has data)
--    and a child task that runs after it.
-- =====================================================================
CREATE OR REPLACE TASK T_MERGE_TRIPS
  WAREHOUSE = RIDES_WH
  SCHEDULE = '1 MINUTE'
  WHEN SYSTEM$STREAM_HAS_DATA('RIDES_DB.STREAMS_WAY.TRIP_EVENTS_STREAM')
AS
  CALL MERGE_TRIPS();

CREATE OR REPLACE TASK T_REFRESH_EARNINGS
  WAREHOUSE = RIDES_WH
  AFTER T_MERGE_TRIPS
AS
  INSERT OVERWRITE INTO DRIVER_DAILY_EARNINGS
  SELECT DRIVER_ID, LAST_EVENT_TS::DATE, COUNT(*), SUM(FARE)
  FROM TRIPS_CURRENT
  WHERE STATUS = 'COMPLETED'
  GROUP BY 1, 2;

-- Tasks are created SUSPENDED. Resume children first, root last.
ALTER TASK T_REFRESH_EARNINGS RESUME;
ALTER TASK T_MERGE_TRIPS RESUME;

SHOW TASKS IN SCHEMA RIDES_DB.STREAMS_WAY;

-- Count what we wrote for pipeline #1:
--   1 stream, 2 tables, 1 procedure (MERGE + dedup + late-event guard), 2 tasks, a resume order.
