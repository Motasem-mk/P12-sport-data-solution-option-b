-- ============================================================
-- Demo: simulate an activity source change in the OLTP database
-- ============================================================
-- In production, this change would come from Strava, Garmin,
-- an internal API, or another sport-tracking provider.
--
-- In this POC, we simulate the same behavior by inserting or
-- updating rows in public.activities.
--
-- Debezium captures these changes and sends them to Redpanda.
-- Then Spark writes them to Bronze, Bronze is processed to Silver,
-- Gold/OLAP is recalculated, and the dashboard is updated.
-- ============================================================


-- ------------------------------------------------------------
-- Option 1: Insert a new activity
-- ------------------------------------------------------------
INSERT INTO public.activities (
    employee_id,
    start_time,
    sport_type,
    distance_m,
    elapsed_time_s,
    comment
)
VALUES (
    1005,
    NOW(),
    'running',
    5200,
    1800,
    'Demo activity inserted from source system'
);


-- ------------------------------------------------------------
-- Option 2: Correct an existing activity
-- Replace 205 with an existing activity_id from your database.
-- ------------------------------------------------------------
UPDATE public.activities
SET distance_m = 8000,
    elapsed_time_s = 2500,
    comment = 'Corrected activity distance from source system'
WHERE activity_id = 205;
