-- ============================================================
-- 01_create_oltp_activities_table.sql
-- Purpose:
--   Create the OLTP activities table used as the operational
--   source table for employee sport activities.
--
-- Context:
--   This table is the PostgreSQL source watched by Debezium CDC.
-- ============================================================

CREATE TABLE IF NOT EXISTS public.activities (
  activity_id      BIGSERIAL PRIMARY KEY,
  employee_id      BIGINT       NOT NULL,
  start_time       TIMESTAMP    NOT NULL,
  sport_type       VARCHAR(50)  NOT NULL,
  distance_m       INTEGER,
  elapsed_time_s   INTEGER      NOT NULL,
  comment          TEXT
);

CREATE INDEX IF NOT EXISTS idx_activities_employee_id
ON public.activities(employee_id);

CREATE INDEX IF NOT EXISTS idx_activities_start_time
ON public.activities(start_time);

