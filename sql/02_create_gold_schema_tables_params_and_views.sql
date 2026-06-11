-- 02_create_gold_schema_tables_params_and_views.sql
-- Clean bootstrap script for the PostgreSQL OLAP Gold layer.
-- Credentials are handled outside this file by the DAG / psql command.

CREATE SCHEMA IF NOT EXISTS gold;
CREATE SCHEMA IF NOT EXISTS gold_staging;

-- ============================================================
-- 1. GOLD TABLES
-- ============================================================

CREATE TABLE IF NOT EXISTS gold.dim_employee (
  employee_id             BIGINT PRIMARY KEY,
  last_name               TEXT,
  first_name              TEXT,
  birth_date              DATE,
  business_unit           TEXT,
  hire_date               DATE,
  gross_salary_eur        DOUBLE PRECISION,
  contract_type           TEXT,
  annual_leave_days       INTEGER,
  home_address            TEXT,
  commute_mode            TEXT,
  sport_practice          TEXT,
  has_sport_practice      BOOLEAN,
  distance_km             DOUBLE PRECISION,
  commute_valid_for_bonus BOOLEAN,
  business_hash           TEXT,
  created_at              TIMESTAMP,
  updated_at              TIMESTAMP,
  commute_checked_at      TIMESTAMP
);

CREATE TABLE IF NOT EXISTS gold.dim_date (
  date_key      INTEGER PRIMARY KEY,
  date_day      DATE NOT NULL UNIQUE,
  year          INTEGER,
  quarter       INTEGER,
  month         INTEGER,
  month_name    TEXT,
  day_of_month  INTEGER,
  day_of_week   INTEGER,
  day_name      TEXT,
  week_of_year  INTEGER,
  is_weekend    BOOLEAN
);

CREATE TABLE IF NOT EXISTS gold.fact_activity (
  activity_id     BIGINT PRIMARY KEY,
  employee_id     BIGINT NOT NULL,
  date_key        INTEGER,
  activity_date   DATE,
  start_time      TIMESTAMP,
  sport_type      TEXT,
  distance_m      INTEGER,
  elapsed_time_s  INTEGER,
  comment         TEXT
);

-- ============================================================
-- 2. STAGING TABLES
-- ============================================================

CREATE TABLE IF NOT EXISTS gold_staging.dim_employee_stage (
  employee_id             BIGINT,
  last_name               TEXT,
  first_name              TEXT,
  birth_date              DATE,
  business_unit           TEXT,
  hire_date               DATE,
  gross_salary_eur        DOUBLE PRECISION,
  contract_type           TEXT,
  annual_leave_days       INTEGER,
  home_address            TEXT,
  commute_mode            TEXT,
  sport_practice          TEXT,
  has_sport_practice      BOOLEAN,
  distance_km             DOUBLE PRECISION,
  commute_valid_for_bonus BOOLEAN,
  business_hash           TEXT,
  created_at              TIMESTAMP,
  updated_at              TIMESTAMP,
  commute_checked_at      TIMESTAMP
);

CREATE TABLE IF NOT EXISTS gold_staging.dim_date_stage (
  date_key      INTEGER,
  date_day      DATE,
  year          INTEGER,
  quarter       INTEGER,
  month         INTEGER,
  month_name    TEXT,
  day_of_month  INTEGER,
  day_of_week   INTEGER,
  day_name      TEXT,
  week_of_year  INTEGER,
  is_weekend    BOOLEAN
);

CREATE TABLE IF NOT EXISTS gold_staging.fact_activity_stage (
  activity_id     BIGINT,
  employee_id     BIGINT,
  date_key        INTEGER,
  activity_date   DATE,
  start_time      TIMESTAMP,
  sport_type      TEXT,
  distance_m      INTEGER,
  elapsed_time_s  INTEGER,
  comment         TEXT
);

-- ============================================================
-- 3. BUSINESS PARAMETERS
-- ============================================================

CREATE TABLE IF NOT EXISTS gold.params (
  param_key   TEXT PRIMARY KEY,
  param_value DOUBLE PRECISION NOT NULL,
  updated_at  TIMESTAMP NOT NULL DEFAULT NOW()
);

INSERT INTO gold.params (param_key, param_value)
VALUES
  ('bonus_rate', 0.05),
  ('wellbeing_days', 5),
  ('wellbeing_min_activities_12m', 15),
  ('walk_run_max_km', 15),
  ('bike_scooter_other_max_km', 25)
ON CONFLICT (param_key) DO NOTHING;

-- ============================================================
-- 4. INDEXES
-- ============================================================

CREATE INDEX IF NOT EXISTS idx_dim_employee_bu
ON gold.dim_employee(business_unit);

CREATE INDEX IF NOT EXISTS idx_fact_activity_date
ON gold.fact_activity(activity_date);

CREATE INDEX IF NOT EXISTS idx_fact_activity_date_key
ON gold.fact_activity(date_key);

CREATE INDEX IF NOT EXISTS idx_fact_activity_emp
ON gold.fact_activity(employee_id);

CREATE INDEX IF NOT EXISTS idx_fact_activity_start
ON gold.fact_activity(start_time);

CREATE INDEX IF NOT EXISTS idx_fact_activity_sport
ON gold.fact_activity(sport_type);

CREATE INDEX IF NOT EXISTS idx_dim_date_day
ON gold.dim_date(date_day);

CREATE INDEX IF NOT EXISTS idx_dim_date_year_month
ON gold.dim_date(year, month);

-- ============================================================
-- 5. REPORTING VIEWS
-- ============================================================

CREATE OR REPLACE VIEW gold.v_fact_activity_enriched AS
SELECT
  fa.activity_id,
  fa.employee_id,
  CONCAT_WS(' ', e.first_name, e.last_name) AS employee_name,
  e.business_unit,
  fa.date_key,
  fa.activity_date,
  d.year,
  d.quarter,
  d.month,
  d.month_name,
  d.week_of_year,
  d.day_name,
  d.is_weekend,
  fa.start_time,
  (
    fa.start_time
    + (COALESCE(fa.elapsed_time_s, 0)::text || ' seconds')::interval
  ) AS end_time,
  fa.sport_type,
  fa.distance_m,
  ROUND(COALESCE(fa.distance_m, 0) / 1000.0, 2) AS distance_km,
  fa.elapsed_time_s,
  ROUND(COALESCE(fa.elapsed_time_s, 0) / 60.0, 2) AS duration_min,
  fa.comment
FROM gold.fact_activity fa
LEFT JOIN gold.dim_employee e
  ON e.employee_id = fa.employee_id
LEFT JOIN gold.dim_date d
  ON d.date_key = fa.date_key;

CREATE OR REPLACE VIEW gold.v_employee_benefits AS
WITH p AS (
  SELECT
    MAX(CASE WHEN param_key = 'bonus_rate' THEN param_value END) AS bonus_rate,
    MAX(CASE WHEN param_key = 'wellbeing_days' THEN param_value END) AS wellbeing_days,
    MAX(CASE WHEN param_key = 'wellbeing_min_activities_12m' THEN param_value END) AS wellbeing_min_activities_12m
  FROM gold.params
),
a12 AS (
  SELECT
    employee_id,
    COUNT(*)::bigint AS activities_12m
  FROM gold.fact_activity
  WHERE activity_date >= (CURRENT_DATE - INTERVAL '12 months')
  GROUP BY employee_id
)
SELECT
  CURRENT_DATE AS snapshot_date,
  e.employee_id,
  CONCAT_WS(' ', e.first_name, e.last_name) AS employee_name,
  e.first_name,
  e.last_name,
  e.business_unit,
  e.gross_salary_eur,
  e.commute_mode,
  e.sport_practice,
  e.has_sport_practice,
  e.distance_km,
  e.commute_valid_for_bonus,
  COALESCE(a12.activities_12m, 0) AS activities_12m,
  (
    COALESCE(e.has_sport_practice, FALSE)
    AND COALESCE(e.commute_valid_for_bonus, FALSE)
  ) AS bonus_eligible,
  CASE
    WHEN COALESCE(e.has_sport_practice, FALSE)
         AND COALESCE(e.commute_valid_for_bonus, FALSE)
      THEN COALESCE(e.gross_salary_eur, 0) * COALESCE(p.bonus_rate, 0.05)
    ELSE 0
  END AS bonus_amount_eur,
  (
    COALESCE(a12.activities_12m, 0) >= COALESCE(p.wellbeing_min_activities_12m, 15)
  ) AS wellbeing_eligible,
  CASE
    WHEN COALESCE(a12.activities_12m, 0) >= COALESCE(p.wellbeing_min_activities_12m, 15)
      THEN COALESCE(p.wellbeing_days, 5)
    ELSE 0
  END AS wellbeing_days_granted,
  CASE
    WHEN COALESCE(a12.activities_12m, 0) >= COALESCE(p.wellbeing_min_activities_12m, 15)
      THEN 'Eligible'
    WHEN COALESCE(a12.activities_12m, 0) BETWEEN 10 AND COALESCE(p.wellbeing_min_activities_12m, 15) - 1
      THEN 'Close to eligibility'
    ELSE 'Not eligible'
  END AS eligibility_status
FROM gold.dim_employee e
CROSS JOIN p
LEFT JOIN a12
  ON a12.employee_id = e.employee_id;

CREATE OR REPLACE VIEW gold.v_commute_declaration_issues AS
WITH p AS (
  SELECT
    MAX(CASE WHEN param_key = 'walk_run_max_km' THEN param_value END) AS walk_run_max_km,
    MAX(CASE WHEN param_key = 'bike_scooter_other_max_km' THEN param_value END) AS bike_scooter_other_max_km
  FROM gold.params
)
SELECT
  CURRENT_DATE AS check_date,
  e.employee_id,
  CONCAT_WS(' ', e.first_name, e.last_name) AS employee_name,
  e.business_unit,
  e.home_address,
  e.commute_mode,
  e.distance_km,
  e.commute_checked_at,
  CASE
    WHEN e.commute_mode IS NULL
      THEN 'missing_commute_mode'

    WHEN LOWER(e.commute_mode) IN ('walk_run', 'walk', 'walking', 'run', 'running')
         AND e.distance_km IS NULL
      THEN 'missing_distance_for_walk_run'

    WHEN LOWER(e.commute_mode) IN ('bike_scooter', 'bike', 'bicycle', 'cycling', 'scooter', 'other')
         AND e.distance_km IS NULL
      THEN 'missing_distance_for_bike_scooter_other'

    WHEN LOWER(e.commute_mode) IN ('walk_run', 'walk', 'walking', 'run', 'running')
         AND e.distance_km > COALESCE((SELECT walk_run_max_km FROM p), 15)
      THEN 'distance_too_high_walk_run'

    WHEN LOWER(e.commute_mode) IN ('bike_scooter', 'bike', 'bicycle', 'cycling', 'scooter', 'other')
         AND e.distance_km > COALESCE((SELECT bike_scooter_other_max_km FROM p), 25)
      THEN 'distance_too_high_bike_scooter_other'

    ELSE NULL
  END AS issue_reason,
  CASE
    WHEN LOWER(e.commute_mode) IN ('walk_run', 'walk', 'walking', 'run', 'running')
      THEN 'walking/running <= walk_run_max_km'
    WHEN LOWER(e.commute_mode) IN ('bike_scooter', 'bike', 'bicycle', 'cycling', 'scooter', 'other')
      THEN 'bike/scooter/other <= bike_scooter_other_max_km'
    ELSE 'commute_mode and distance must be declared'
  END AS validation_rule,
  FALSE AS is_valid_for_bonus
FROM gold.dim_employee e
WHERE
  e.commute_mode IS NULL
  OR (
    LOWER(e.commute_mode) IN (
      'walk_run', 'walk', 'walking', 'run', 'running',
      'bike_scooter', 'bike', 'bicycle', 'cycling', 'scooter', 'other'
    )
    AND e.distance_km IS NULL
  )
  OR (
    LOWER(e.commute_mode) IN ('walk_run', 'walk', 'walking', 'run', 'running')
    AND e.distance_km > COALESCE((SELECT walk_run_max_km FROM p), 15)
  )
  OR (
    LOWER(e.commute_mode) IN ('bike_scooter', 'bike', 'bicycle', 'cycling', 'scooter', 'other')
    AND e.distance_km > COALESCE((SELECT bike_scooter_other_max_km FROM p), 25)
  );

CREATE OR REPLACE VIEW gold.v_kpi_summary AS
SELECT
  CURRENT_DATE AS snapshot_date,
  COUNT(*)::bigint AS total_employees,
  SUM(CASE WHEN bonus_eligible THEN 1 ELSE 0 END)::bigint AS bonus_eligible_employees,
  ROUND(SUM(bonus_amount_eur)::numeric, 2)::double precision AS total_annual_bonus_cost_eur,
  SUM(CASE WHEN wellbeing_eligible THEN 1 ELSE 0 END)::bigint AS wellbeing_eligible_employees,
  SUM(wellbeing_days_granted)::double precision AS total_wellbeing_days_granted,
  (SELECT COUNT(*)::bigint FROM gold.fact_activity) AS total_activities,
  (SELECT COUNT(*)::bigint FROM gold.v_commute_declaration_issues WHERE issue_reason IS NOT NULL) AS invalid_commute_declarations,
  ROUND(
    AVG(
      CASE
        WHEN bonus_eligible OR wellbeing_eligible THEN 1.0
        ELSE 0.0
      END
    )::numeric,
    4
  )::double precision AS participation_rate,
  (SELECT param_value FROM gold.params WHERE param_key = 'bonus_rate') AS bonus_rate_used,
  NOW() AS refreshed_at
FROM gold.v_employee_benefits;

CREATE OR REPLACE VIEW gold.v_financial_impact AS
SELECT
  snapshot_date,
  COALESCE(business_unit, 'Unknown') AS business_unit,
  COUNT(*)::bigint AS total_employees,
  SUM(CASE WHEN bonus_eligible THEN 1 ELSE 0 END)::bigint AS bonus_eligible_employees,
  ROUND(AVG(gross_salary_eur)::numeric, 2)::double precision AS average_salary_eur,
  (SELECT param_value FROM gold.params WHERE param_key = 'bonus_rate') AS bonus_rate,
  ROUND(SUM(bonus_amount_eur)::numeric, 2)::double precision AS annual_bonus_cost_eur,
  ROUND(
    (
      SUM(bonus_amount_eur)
      / NULLIF(SUM(CASE WHEN bonus_eligible THEN 1 ELSE 0 END), 0)
    )::numeric,
    2
  )::double precision AS average_bonus_per_eligible_employee_eur
FROM gold.v_employee_benefits
GROUP BY snapshot_date, COALESCE(business_unit, 'Unknown');

CREATE OR REPLACE VIEW gold.v_wellbeing_days AS
SELECT
  snapshot_date,
  employee_id,
  employee_name,
  business_unit,
  activities_12m AS activity_count_12m,
  wellbeing_eligible,
  wellbeing_days_granted,
  eligibility_status
FROM gold.v_employee_benefits;

CREATE OR REPLACE VIEW gold.v_kpi_monthly AS
SELECT
  DATE_TRUNC('month', fa.activity_date)::date AS activity_month,
  d.year,
  d.month,
  d.month_name,
  COUNT(*)::bigint AS activities_count,
  COUNT(DISTINCT fa.employee_id)::bigint AS active_employees_count,
  SUM(COALESCE(fa.distance_m, 0))::bigint AS total_distance_m,
  ROUND(SUM(COALESCE(fa.distance_m, 0)) / 1000.0, 2) AS total_distance_km,
  SUM(COALESCE(fa.elapsed_time_s, 0))::bigint AS total_elapsed_time_s,
  ROUND(SUM(COALESCE(fa.elapsed_time_s, 0)) / 3600.0, 2) AS total_elapsed_time_hours
FROM gold.fact_activity fa
LEFT JOIN gold.dim_date d
  ON d.date_key = fa.date_key
GROUP BY
  DATE_TRUNC('month', fa.activity_date)::date,
  d.year,
  d.month,
  d.month_name;

CREATE OR REPLACE VIEW gold.v_sports_activity AS
SELECT
  DATE_TRUNC('month', fa.activity_date)::date AS activity_month,
  COALESCE(fa.sport_type, 'Unknown') AS sport_type,
  COUNT(*)::bigint AS activity_count,
  COUNT(DISTINCT fa.employee_id)::bigint AS active_employees,
  ROUND(SUM(COALESCE(fa.distance_m, 0)) / 1000.0, 2) AS total_distance_km,
  ROUND(AVG(COALESCE(fa.distance_m, 0)) / 1000.0, 2) AS average_distance_km,
  ROUND(AVG(COALESCE(fa.elapsed_time_s, 0)) / 60.0, 2) AS average_duration_min
FROM gold.fact_activity fa
GROUP BY
  DATE_TRUNC('month', fa.activity_date)::date,
  COALESCE(fa.sport_type, 'Unknown');

CREATE OR REPLACE VIEW gold.v_data_quality_summary AS
SELECT
  CURRENT_DATE AS check_date,
  'Invalid commute declarations' AS dq_check_name,
  COUNT(*)::bigint AS invalid_count,
  'High' AS severity,
  'May wrongly grant or reject bonus eligibility' AS business_impact,
  CASE WHEN COUNT(*) > 0 THEN 'Failed' ELSE 'Passed' END AS status
FROM gold.v_commute_declaration_issues
WHERE issue_reason IS NOT NULL

UNION ALL

SELECT
  CURRENT_DATE AS check_date,
  'Activity with negative distance' AS dq_check_name,
  COUNT(*)::bigint AS invalid_count,
  'High' AS severity,
  'Invalid activity metric' AS business_impact,
  CASE WHEN COUNT(*) > 0 THEN 'Failed' ELSE 'Passed' END AS status
FROM gold.fact_activity
WHERE distance_m < 0

UNION ALL

SELECT
  CURRENT_DATE AS check_date,
  'Activity with invalid dates' AS dq_check_name,
  COUNT(*)::bigint AS invalid_count,
  'High' AS severity,
  'Invalid activity chronology' AS business_impact,
  CASE WHEN COUNT(*) > 0 THEN 'Failed' ELSE 'Passed' END AS status
FROM gold.fact_activity
WHERE activity_date IS NULL OR start_time IS NULL

UNION ALL

SELECT
  CURRENT_DATE AS check_date,
  'Missing employee reference' AS dq_check_name,
  COUNT(*)::bigint AS invalid_count,
  'Medium' AS severity,
  'Activity cannot be linked to HR reference' AS business_impact,
  CASE WHEN COUNT(*) > 0 THEN 'Failed' ELSE 'Passed' END AS status
FROM gold.fact_activity fa
LEFT JOIN gold.dim_employee e
  ON e.employee_id = fa.employee_id
WHERE e.employee_id IS NULL;


-- -- ============================================================
-- -- 02_create_gold_schema_tables_params_and_views.sql
-- --
-- -- Purpose:
-- --   Create the PostgreSQL OLAP serving layer for the Gold Delta star schema.
-- --
-- -- Final simplified approach:
-- --   Silver Delta
-- --      ↓
-- --   Gold Delta local star schema
-- --      - dim_employee
-- --      - dim_date
-- --      - fact_activity
-- --      ↓
-- --   PostgreSQL gold_staging
-- --      ↓
-- --   PostgreSQL gold OLAP star schema
-- --      - gold.dim_employee
-- --      - gold.dim_date
-- --      - gold.fact_activity
-- --      - gold.params
-- --      - KPI/business views
-- --      ↓
-- --   Metabase
-- --
-- -- Important simplification:
-- --   KPI tables are removed.
-- --   KPI logic is exposed through PostgreSQL views.
-- --   Views update automatically when dim/fact/date tables are upserted.
-- -- ============================================================

-- CREATE SCHEMA IF NOT EXISTS gold;
-- CREATE SCHEMA IF NOT EXISTS gold_staging;

-- -- ============================================================
-- -- 0. CLEAN OLD KPI TABLE APPROACH OBJECTS
-- -- ============================================================
-- -- These were used in the previous approach where KPIs were physical tables.
-- -- In the new simplified star-schema approach, KPI outputs are views.

-- DROP VIEW IF EXISTS gold.v_data_quality_summary CASCADE;
-- DROP VIEW IF EXISTS gold.v_commute_declaration_issues CASCADE;
-- DROP VIEW IF EXISTS gold.v_sports_activity CASCADE;
-- DROP VIEW IF EXISTS gold.v_wellbeing_days CASCADE;
-- DROP VIEW IF EXISTS gold.v_financial_impact CASCADE;
-- DROP VIEW IF EXISTS gold.v_kpi_monthly CASCADE;
-- DROP VIEW IF EXISTS gold.v_kpi_summary CASCADE;
-- DROP VIEW IF EXISTS gold.v_employee_benefits CASCADE;
-- DROP VIEW IF EXISTS gold.v_fact_activity_enriched CASCADE;

-- DROP TABLE IF EXISTS gold.kpi_executive_summary CASCADE;
-- DROP TABLE IF EXISTS gold.kpi_financial_impact CASCADE;
-- DROP TABLE IF EXISTS gold.kpi_wellbeing_days CASCADE;
-- DROP TABLE IF EXISTS gold.kpi_sports_activity CASCADE;
-- DROP TABLE IF EXISTS gold.kpi_data_quality CASCADE;
-- DROP TABLE IF EXISTS gold.invalid_commute_declarations CASCADE;
-- DROP TABLE IF EXISTS gold.latest_activity_demo CASCADE;

-- DROP TABLE IF EXISTS gold_staging.kpi_executive_summary_stage CASCADE;
-- DROP TABLE IF EXISTS gold_staging.kpi_financial_impact_stage CASCADE;
-- DROP TABLE IF EXISTS gold_staging.kpi_wellbeing_days_stage CASCADE;
-- DROP TABLE IF EXISTS gold_staging.kpi_sports_activity_stage CASCADE;
-- DROP TABLE IF EXISTS gold_staging.kpi_data_quality_stage CASCADE;
-- DROP TABLE IF EXISTS gold_staging.invalid_commute_declarations_stage CASCADE;
-- DROP TABLE IF EXISTS gold_staging.latest_activity_demo_stage CASCADE;

-- -- ============================================================
-- -- 1. FINAL OLAP STAR-SCHEMA TABLES
-- -- ============================================================

-- -- ------------------------------------------------------------
-- -- gold.dim_employee
-- -- Purpose:
-- --   Employee dimension used for HR, department, salary, commute,
-- --   bonus eligibility, and wellbeing analysis.
-- -- ------------------------------------------------------------
-- CREATE TABLE IF NOT EXISTS gold.dim_employee (
--   employee_id             BIGINT PRIMARY KEY,
--   last_name               TEXT,
--   first_name              TEXT,
--   birth_date              DATE,
--   business_unit           TEXT,
--   hire_date               DATE,
--   gross_salary_eur        DOUBLE PRECISION,
--   contract_type           TEXT,
--   annual_leave_days       INTEGER,
--   home_address            TEXT,
--   commute_mode            TEXT,
--   sport_practice          TEXT,
--   has_sport_practice      BOOLEAN,
--   distance_km             DOUBLE PRECISION,
--   commute_valid_for_bonus BOOLEAN,
--   business_hash           TEXT,
--   created_at              TIMESTAMP,
--   updated_at              TIMESTAMP,
--   commute_checked_at      TIMESTAMP
-- );

-- -- ------------------------------------------------------------
-- -- gold.dim_date
-- -- Purpose:
-- --   Calendar dimension used for month, year, weekday and trend analysis.
-- -- ------------------------------------------------------------
-- CREATE TABLE IF NOT EXISTS gold.dim_date (
--   date_key      INTEGER PRIMARY KEY,
--   date_day      DATE NOT NULL UNIQUE,
--   year          INTEGER,
--   quarter       INTEGER,
--   month         INTEGER,
--   month_name    TEXT,
--   day_of_month  INTEGER,
--   day_of_week   INTEGER,
--   day_name      TEXT,
--   week_of_year  INTEGER,
--   is_weekend    BOOLEAN
-- );

-- -- ------------------------------------------------------------
-- -- gold.fact_activity
-- -- Purpose:
-- --   Sport activity fact table used for activity counts, distance,
-- --   duration, sport practice trends and wellbeing eligibility.
-- -- ------------------------------------------------------------
-- CREATE TABLE IF NOT EXISTS gold.fact_activity (
--   activity_id     BIGINT PRIMARY KEY,
--   employee_id     BIGINT NOT NULL,
--   date_key        INTEGER,
--   activity_date   DATE,
--   start_time      TIMESTAMP,
--   sport_type      TEXT,
--   distance_m      INTEGER,
--   elapsed_time_s  INTEGER,
--   comment         TEXT
-- );

-- -- Compatibility if the table existed before without date_key.
-- ALTER TABLE gold.fact_activity
-- ADD COLUMN IF NOT EXISTS date_key INTEGER;

-- -- ============================================================
-- -- 2. STAGING TABLES
-- -- ============================================================

-- -- Temporary loading table for dim_employee.
-- CREATE TABLE IF NOT EXISTS gold_staging.dim_employee_stage (
--   employee_id             BIGINT,
--   last_name               TEXT,
--   first_name              TEXT,
--   birth_date              DATE,
--   business_unit           TEXT,
--   hire_date               DATE,
--   gross_salary_eur        DOUBLE PRECISION,
--   contract_type           TEXT,
--   annual_leave_days       INTEGER,
--   home_address            TEXT,
--   commute_mode            TEXT,
--   sport_practice          TEXT,
--   has_sport_practice      BOOLEAN,
--   distance_km             DOUBLE PRECISION,
--   commute_valid_for_bonus BOOLEAN,
--   business_hash           TEXT,
--   created_at              TIMESTAMP,
--   updated_at              TIMESTAMP,
--   commute_checked_at      TIMESTAMP
-- );

-- -- Temporary loading table for dim_date.
-- CREATE TABLE IF NOT EXISTS gold_staging.dim_date_stage (
--   date_key      INTEGER,
--   date_day      DATE,
--   year          INTEGER,
--   quarter       INTEGER,
--   month         INTEGER,
--   month_name    TEXT,
--   day_of_month  INTEGER,
--   day_of_week   INTEGER,
--   day_name      TEXT,
--   week_of_year  INTEGER,
--   is_weekend    BOOLEAN
-- );

-- -- Temporary loading table for fact_activity.
-- CREATE TABLE IF NOT EXISTS gold_staging.fact_activity_stage (
--   activity_id      BIGINT,
--   employee_id      BIGINT,
--   date_key         INTEGER,
--   activity_date    DATE,
--   start_time       TIMESTAMP,
--   sport_type       TEXT,
--   distance_m       INTEGER,
--   elapsed_time_s   INTEGER,
--   comment          TEXT
-- );

-- -- Compatibility if the stage table existed before without date_key.
-- ALTER TABLE gold_staging.fact_activity_stage
-- ADD COLUMN IF NOT EXISTS date_key INTEGER;

-- -- ============================================================
-- -- 3. BUSINESS PARAMETERS
-- -- ============================================================
-- -- These parameters are used by the KPI views.
-- -- During the live demo, bonus_rate can be changed from 5% to 3%, 7%, 10%, etc.

-- CREATE TABLE IF NOT EXISTS gold.params (
--   param_key   TEXT PRIMARY KEY,
--   param_value DOUBLE PRECISION NOT NULL,
--   updated_at  TIMESTAMP NOT NULL DEFAULT NOW()
-- );

-- INSERT INTO gold.params (param_key, param_value)
-- VALUES
--   ('bonus_rate', 0.05),
--   ('wellbeing_days', 5),
--   ('wellbeing_min_activities_12m', 15),
--   ('walk_run_max_km', 15),
--   ('bike_scooter_other_max_km', 25)
-- ON CONFLICT (param_key) DO NOTHING;

-- -- ============================================================
-- -- 4. INDEXES FOR REPORTING PERFORMANCE
-- -- ============================================================

-- CREATE INDEX IF NOT EXISTS idx_dim_employee_bu
-- ON gold.dim_employee(business_unit);

-- CREATE INDEX IF NOT EXISTS idx_fact_activity_date
-- ON gold.fact_activity(activity_date);

-- CREATE INDEX IF NOT EXISTS idx_fact_activity_date_key
-- ON gold.fact_activity(date_key);

-- CREATE INDEX IF NOT EXISTS idx_fact_activity_emp
-- ON gold.fact_activity(employee_id);

-- CREATE INDEX IF NOT EXISTS idx_fact_activity_start
-- ON gold.fact_activity(start_time);

-- CREATE INDEX IF NOT EXISTS idx_fact_activity_sport
-- ON gold.fact_activity(sport_type);

-- CREATE INDEX IF NOT EXISTS idx_dim_date_day
-- ON gold.dim_date(date_day);

-- CREATE INDEX IF NOT EXISTS idx_dim_date_year_month
-- ON gold.dim_date(year, month);

-- -- ============================================================
-- -- 5. BUSINESS KPI VIEWS
-- -- ============================================================

-- -- ------------------------------------------------------------
-- -- gold.v_fact_activity_enriched
-- -- Purpose:
-- --   Activity fact enriched with employee name, business unit,
-- --   calendar attributes and calculated end_time.
-- -- ------------------------------------------------------------
-- CREATE OR REPLACE VIEW gold.v_fact_activity_enriched AS
-- SELECT
--   fa.activity_id,
--   fa.employee_id,
--   CONCAT_WS(' ', e.first_name, e.last_name) AS employee_name,
--   e.business_unit,
--   fa.date_key,
--   fa.activity_date,
--   d.year,
--   d.quarter,
--   d.month,
--   d.month_name,
--   d.week_of_year,
--   d.day_name,
--   d.is_weekend,
--   fa.start_time,
--   (
--     fa.start_time
--     + (COALESCE(fa.elapsed_time_s, 0)::text || ' seconds')::interval
--   ) AS end_time,
--   fa.sport_type,
--   fa.distance_m,
--   ROUND(COALESCE(fa.distance_m, 0) / 1000.0, 2) AS distance_km,
--   fa.elapsed_time_s,
--   ROUND(COALESCE(fa.elapsed_time_s, 0) / 60.0, 2) AS duration_min,
--   fa.comment
-- FROM gold.fact_activity fa
-- LEFT JOIN gold.dim_employee e
--   ON e.employee_id = fa.employee_id
-- LEFT JOIN gold.dim_date d
--   ON d.date_key = fa.date_key;

-- -- ------------------------------------------------------------
-- -- gold.v_employee_benefits
-- -- Purpose:
-- --   Employee-level eligibility, bonus amount and wellbeing days.
-- -- ------------------------------------------------------------
-- CREATE OR REPLACE VIEW gold.v_employee_benefits AS
-- WITH p AS (
--   SELECT
--     MAX(CASE WHEN param_key = 'bonus_rate' THEN param_value END) AS bonus_rate,
--     MAX(CASE WHEN param_key = 'wellbeing_days' THEN param_value END) AS wellbeing_days,
--     MAX(CASE WHEN param_key = 'wellbeing_min_activities_12m' THEN param_value END) AS wellbeing_min_activities_12m
--   FROM gold.params
-- ),
-- a12 AS (
--   SELECT
--     employee_id,
--     COUNT(*)::bigint AS activities_12m
--   FROM gold.fact_activity
--   WHERE activity_date >= (CURRENT_DATE - INTERVAL '12 months')
--   GROUP BY employee_id
-- )
-- SELECT
--   CURRENT_DATE AS snapshot_date,
--   e.employee_id,
--   CONCAT_WS(' ', e.first_name, e.last_name) AS employee_name,
--   e.first_name,
--   e.last_name,
--   e.business_unit,
--   e.gross_salary_eur,
--   e.commute_mode,
--   e.sport_practice,
--   e.has_sport_practice,
--   e.distance_km,
--   e.commute_valid_for_bonus,
--   COALESCE(a12.activities_12m, 0) AS activities_12m,
--   (
--     COALESCE(e.has_sport_practice, FALSE)
--     AND COALESCE(e.commute_valid_for_bonus, FALSE)
--   ) AS bonus_eligible,
--   CASE
--     WHEN COALESCE(e.has_sport_practice, FALSE)
--          AND COALESCE(e.commute_valid_for_bonus, FALSE)
--       THEN COALESCE(e.gross_salary_eur, 0) * COALESCE(p.bonus_rate, 0.05)
--     ELSE 0
--   END AS bonus_amount_eur,
--   (
--     COALESCE(a12.activities_12m, 0) >= COALESCE(p.wellbeing_min_activities_12m, 15)
--   ) AS wellbeing_eligible,
--   CASE
--     WHEN COALESCE(a12.activities_12m, 0) >= COALESCE(p.wellbeing_min_activities_12m, 15)
--       THEN COALESCE(p.wellbeing_days, 5)
--     ELSE 0
--   END AS wellbeing_days_granted,
--   CASE
--     WHEN COALESCE(a12.activities_12m, 0) >= COALESCE(p.wellbeing_min_activities_12m, 15)
--       THEN 'Eligible'
--     WHEN COALESCE(a12.activities_12m, 0) BETWEEN 10 AND COALESCE(p.wellbeing_min_activities_12m, 15) - 1
--       THEN 'Close to eligibility'
--     ELSE 'Not eligible'
--   END AS eligibility_status
-- FROM gold.dim_employee e
-- CROSS JOIN p
-- LEFT JOIN a12
--   ON a12.employee_id = e.employee_id;

-- -- ------------------------------------------------------------
-- -- gold.v_commute_declaration_issues
-- -- Purpose:
-- --   Detailed suspicious or incomplete commute declarations.
-- -- ------------------------------------------------------------
-- CREATE OR REPLACE VIEW gold.v_commute_declaration_issues AS
-- WITH p AS (
--   SELECT
--     MAX(CASE WHEN param_key = 'walk_run_max_km' THEN param_value END) AS walk_run_max_km,
--     MAX(CASE WHEN param_key = 'bike_scooter_other_max_km' THEN param_value END) AS bike_scooter_other_max_km
--   FROM gold.params
-- )
-- SELECT
--   CURRENT_DATE AS check_date,
--   e.employee_id,
--   CONCAT_WS(' ', e.first_name, e.last_name) AS employee_name,
--   e.business_unit,
--   e.home_address,
--   e.commute_mode,
--   e.distance_km,
--   e.commute_checked_at,
--   CASE
--     WHEN e.commute_mode IS NULL
--       THEN 'missing_commute_mode'

--     WHEN LOWER(e.commute_mode) IN ('walk_run', 'walk', 'walking', 'run', 'running')
--          AND e.distance_km IS NULL
--       THEN 'missing_distance_for_walk_run'

--     WHEN LOWER(e.commute_mode) IN ('bike_scooter', 'bike', 'bicycle', 'cycling', 'scooter', 'other')
--          AND e.distance_km IS NULL
--       THEN 'missing_distance_for_bike_scooter_other'

--     WHEN LOWER(e.commute_mode) IN ('walk_run', 'walk', 'walking', 'run', 'running')
--          AND e.distance_km > COALESCE((SELECT walk_run_max_km FROM p), 15)
--       THEN 'distance_too_high_walk_run'

--     WHEN LOWER(e.commute_mode) IN ('bike_scooter', 'bike', 'bicycle', 'cycling', 'scooter', 'other')
--          AND e.distance_km > COALESCE((SELECT bike_scooter_other_max_km FROM p), 25)
--       THEN 'distance_too_high_bike_scooter_other'

--     ELSE NULL
--   END AS issue_reason,
--   CASE
--     WHEN LOWER(e.commute_mode) IN ('walk_run', 'walk', 'walking', 'run', 'running')
--       THEN 'walking/running <= walk_run_max_km'
--     WHEN LOWER(e.commute_mode) IN ('bike_scooter', 'bike', 'bicycle', 'cycling', 'scooter', 'other')
--       THEN 'bike/scooter/other <= bike_scooter_other_max_km'
--     ELSE 'commute_mode and distance must be declared'
--   END AS validation_rule,
--   FALSE AS is_valid_for_bonus
-- FROM gold.dim_employee e
-- WHERE
--   e.commute_mode IS NULL
--   OR (
--     LOWER(e.commute_mode) IN ('walk_run', 'walk', 'walking', 'run', 'running',
--                               'bike_scooter', 'bike', 'bicycle', 'cycling', 'scooter', 'other')
--     AND e.distance_km IS NULL
--   )
--   OR (
--     LOWER(e.commute_mode) IN ('walk_run', 'walk', 'walking', 'run', 'running')
--     AND e.distance_km > COALESCE((SELECT walk_run_max_km FROM p), 15)
--   )
--   OR (
--     LOWER(e.commute_mode) IN ('bike_scooter', 'bike', 'bicycle', 'cycling', 'scooter', 'other')
--     AND e.distance_km > COALESCE((SELECT bike_scooter_other_max_km FROM p), 25)
--   );

-- -- ------------------------------------------------------------
-- -- gold.v_kpi_summary
-- -- Purpose:
-- --   Executive summary KPI cards for the dashboard.
-- -- ------------------------------------------------------------
-- CREATE OR REPLACE VIEW gold.v_kpi_summary AS
-- SELECT
--   CURRENT_DATE AS snapshot_date,
--   COUNT(*)::bigint AS total_employees,
--   SUM(CASE WHEN bonus_eligible THEN 1 ELSE 0 END)::bigint AS bonus_eligible_employees,
--   ROUND(SUM(bonus_amount_eur)::numeric, 2)::double precision AS total_annual_bonus_cost_eur,
--   SUM(CASE WHEN wellbeing_eligible THEN 1 ELSE 0 END)::bigint AS wellbeing_eligible_employees,
--   SUM(wellbeing_days_granted)::double precision AS total_wellbeing_days_granted,
--   (SELECT COUNT(*)::bigint FROM gold.fact_activity) AS total_activities,
--   (SELECT COUNT(*)::bigint FROM gold.v_commute_declaration_issues WHERE issue_reason IS NOT NULL) AS invalid_commute_declarations,
--   ROUND(
--     AVG(
--       CASE
--         WHEN bonus_eligible OR wellbeing_eligible THEN 1.0
--         ELSE 0.0
--       END
--     )::numeric,
--     4
--   )::double precision AS participation_rate,
--   (SELECT param_value FROM gold.params WHERE param_key = 'bonus_rate') AS bonus_rate_used,
--   NOW() AS refreshed_at
-- FROM gold.v_employee_benefits;

-- -- ------------------------------------------------------------
-- -- gold.v_financial_impact
-- -- Purpose:
-- --   Bonus cost and financial impact by business unit.
-- -- ------------------------------------------------------------
-- CREATE OR REPLACE VIEW gold.v_financial_impact AS
-- SELECT
--   snapshot_date,
--   COALESCE(business_unit, 'Unknown') AS business_unit,
--   COUNT(*)::bigint AS total_employees,
--   SUM(CASE WHEN bonus_eligible THEN 1 ELSE 0 END)::bigint AS bonus_eligible_employees,
--   ROUND(AVG(gross_salary_eur)::numeric, 2)::double precision AS average_salary_eur,
--   (SELECT param_value FROM gold.params WHERE param_key = 'bonus_rate') AS bonus_rate,
--   ROUND(SUM(bonus_amount_eur)::numeric, 2)::double precision AS annual_bonus_cost_eur,
--   ROUND(
--     (
--       SUM(bonus_amount_eur)
--       / NULLIF(SUM(CASE WHEN bonus_eligible THEN 1 ELSE 0 END), 0)
--     )::numeric,
--     2
--   )::double precision AS average_bonus_per_eligible_employee_eur
-- FROM gold.v_employee_benefits
-- GROUP BY snapshot_date, COALESCE(business_unit, 'Unknown');

-- -- ------------------------------------------------------------
-- -- gold.v_wellbeing_days
-- -- Purpose:
-- --   Wellbeing-day eligibility by employee.
-- -- ------------------------------------------------------------
-- CREATE OR REPLACE VIEW gold.v_wellbeing_days AS
-- SELECT
--   snapshot_date,
--   employee_id,
--   employee_name,
--   business_unit,
--   activities_12m AS activity_count_12m,
--   wellbeing_eligible,
--   wellbeing_days_granted,
--   eligibility_status
-- FROM gold.v_employee_benefits;

-- -- ------------------------------------------------------------
-- -- gold.v_kpi_monthly
-- -- Purpose:
-- --   Monthly historical activity indicators.
-- -- ------------------------------------------------------------
-- CREATE OR REPLACE VIEW gold.v_kpi_monthly AS
-- SELECT
--   DATE_TRUNC('month', fa.activity_date)::date AS activity_month,
--   d.year,
--   d.month,
--   d.month_name,
--   COUNT(*)::bigint AS activities_count,
--   COUNT(DISTINCT fa.employee_id)::bigint AS active_employees_count,
--   SUM(COALESCE(fa.distance_m, 0))::bigint AS total_distance_m,
--   ROUND(SUM(COALESCE(fa.distance_m, 0)) / 1000.0, 2) AS total_distance_km,
--   SUM(COALESCE(fa.elapsed_time_s, 0))::bigint AS total_elapsed_time_s,
--   ROUND(SUM(COALESCE(fa.elapsed_time_s, 0)) / 3600.0, 2) AS total_elapsed_time_hours
-- FROM gold.fact_activity fa
-- LEFT JOIN gold.dim_date d
--   ON d.date_key = fa.date_key
-- GROUP BY
--   DATE_TRUNC('month', fa.activity_date)::date,
--   d.year,
--   d.month,
--   d.month_name;

-- -- ------------------------------------------------------------
-- -- gold.v_sports_activity
-- -- Purpose:
-- --   Sports practice by month and sport type.
-- -- ------------------------------------------------------------
-- CREATE OR REPLACE VIEW gold.v_sports_activity AS
-- SELECT
--   DATE_TRUNC('month', fa.activity_date)::date AS activity_month,
--   COALESCE(fa.sport_type, 'Unknown') AS sport_type,
--   COUNT(*)::bigint AS activity_count,
--   COUNT(DISTINCT fa.employee_id)::bigint AS active_employees,
--   ROUND(SUM(COALESCE(fa.distance_m, 0)) / 1000.0, 2) AS total_distance_km,
--   ROUND(AVG(COALESCE(fa.distance_m, 0)) / 1000.0, 2) AS average_distance_km,
--   ROUND(AVG(COALESCE(fa.elapsed_time_s, 0)) / 60.0, 2) AS average_duration_min
-- FROM gold.fact_activity fa
-- GROUP BY
--   DATE_TRUNC('month', fa.activity_date)::date,
--   COALESCE(fa.sport_type, 'Unknown');

-- -- ------------------------------------------------------------
-- -- gold.v_data_quality_summary
-- -- Purpose:
-- --   Lightweight data quality summary for the dashboard.
-- -- ------------------------------------------------------------
-- CREATE OR REPLACE VIEW gold.v_data_quality_summary AS
-- SELECT
--   CURRENT_DATE AS check_date,
--   'Invalid commute declarations' AS dq_check_name,
--   COUNT(*)::bigint AS invalid_count,
--   'High' AS severity,
--   'May wrongly grant or reject bonus eligibility' AS business_impact,
--   CASE WHEN COUNT(*) > 0 THEN 'Failed' ELSE 'Passed' END AS status
-- FROM gold.v_commute_declaration_issues
-- WHERE issue_reason IS NOT NULL

-- UNION ALL

-- SELECT
--   CURRENT_DATE AS check_date,
--   'Activity with negative distance' AS dq_check_name,
--   COUNT(*)::bigint AS invalid_count,
--   'High' AS severity,
--   'Invalid activity metric' AS business_impact,
--   CASE WHEN COUNT(*) > 0 THEN 'Failed' ELSE 'Passed' END AS status
-- FROM gold.fact_activity
-- WHERE distance_m < 0

-- UNION ALL

-- SELECT
--   CURRENT_DATE AS check_date,
--   'Activity with invalid dates' AS dq_check_name,
--   COUNT(*)::bigint AS invalid_count,
--   'High' AS severity,
--   'Invalid activity chronology' AS business_impact,
--   CASE WHEN COUNT(*) > 0 THEN 'Failed' ELSE 'Passed' END AS status
-- FROM gold.fact_activity
-- WHERE activity_date IS NULL OR start_time IS NULL

-- UNION ALL

-- SELECT
--   CURRENT_DATE AS check_date,
--   'Missing employee reference' AS dq_check_name,
--   COUNT(*)::bigint AS invalid_count,
--   'Medium' AS severity,
--   'Activity cannot be linked to HR reference' AS business_impact,
--   CASE WHEN COUNT(*) > 0 THEN 'Failed' ELSE 'Passed' END AS status
-- FROM gold.fact_activity fa
-- LEFT JOIN gold.dim_employee e
--   ON e.employee_id = fa.employee_id
-- WHERE e.employee_id IS NULL;

-- -- ============================================================
-- -- 6. GRANTS FOR LOCAL POC REPORTING
-- -- ============================================================

-- GRANT USAGE ON SCHEMA gold TO PUBLIC;
-- GRANT USAGE ON SCHEMA gold_staging TO PUBLIC;

-- GRANT SELECT ON ALL TABLES IN SCHEMA gold TO PUBLIC;
-- GRANT SELECT ON ALL TABLES IN SCHEMA gold_staging TO PUBLIC;

-- ALTER DEFAULT PRIVILEGES IN SCHEMA gold GRANT SELECT ON TABLES TO PUBLIC;
-- ALTER DEFAULT PRIVILEGES IN SCHEMA gold_staging GRANT SELECT ON TABLES TO PUBLIC;
