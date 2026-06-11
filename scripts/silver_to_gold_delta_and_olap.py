# scripts/silver_to_gold_delta_and_olap.py

"""
Silver Delta -> Gold Delta star schema -> PostgreSQL OLAP

Purpose
-------
This script implements the simplified Gold star-schema approach.

1. Read Silver Delta tables:
   - silver.employee_ref
   - silver.activities

2. Build only three Gold Delta tables:
   - dim_employee
   - dim_date
   - fact_activity

3. Persist those three tables locally as Gold Delta:
   - /opt/workspace/data/delta/gold/dim_employee
   - /opt/workspace/data/delta/gold/dim_date
   - /opt/workspace/data/delta/gold/fact_activity

4. Publish the same three tables to PostgreSQL:
   - TRUNCATE gold_staging.*
   - APPEND current Gold tables into staging
   - UPSERT staging into final gold.* OLAP tables

5. PostgreSQL KPI views calculate business KPIs dynamically:
   - gold.v_kpi_summary
   - gold.v_financial_impact
   - gold.v_wellbeing_days
   - gold.v_kpi_monthly
   - gold.v_sports_activity
   - gold.v_commute_declaration_issues
   - gold.v_data_quality_summary

Metabase should read from PostgreSQL gold tables/views, not staging.
"""

import os
from datetime import date

import psycopg2
from pyspark.sql import SparkSession, functions as F


# ============================================================
# Config
# ============================================================

SILVER_EMPLOYEE_REF_PATH = os.getenv(
    "SILVER_EMPLOYEE_REF_PATH",
    "/opt/workspace/data/delta/silver/employee_ref",
)

SILVER_ACTIVITIES_PATH = os.getenv(
    "SILVER_ACTIVITIES_PATH",
    "/opt/workspace/data/delta/silver/activities",
)

GOLD_BASE_PATH = os.getenv(
    "GOLD_BASE_PATH",
    "/opt/workspace/data/delta/gold",
)

GOLD_PATHS = {
    "dim_employee": f"{GOLD_BASE_PATH}/dim_employee",
    "dim_date": f"{GOLD_BASE_PATH}/dim_date",
    "fact_activity": f"{GOLD_BASE_PATH}/fact_activity",
}

PG_HOST = os.getenv("POSTGRES_HOST", "postgres")
PG_PORT = int(os.getenv("POSTGRES_PORT", "5432"))

PG_GOLD_DB = os.getenv("SPORT_OLAP_DB") or os.getenv("POSTGRES_GOLD_DB", "sportdw")
PG_USER = os.getenv("SPORT_OLAP_USER") or os.getenv("POSTGRES_USER", "sport")
PG_PASSWORD = os.getenv("SPORT_OLAP_PASSWORD") or os.getenv("POSTGRES_PASSWORD", "sport")

GOLD_SCHEMA = "gold"
STAGE_SCHEMA = "gold_staging"


# ============================================================
# Helpers
# ============================================================

def log(msg: str) -> None:
    print(f"[silver_to_gold_delta_and_olap] {msg}", flush=True)


def build_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("Silver_To_Gold_Star_Schema_And_OLAP")
        .getOrCreate()
    )


def pg_connect():
    return psycopg2.connect(
        host=PG_HOST,
        port=PG_PORT,
        dbname=PG_GOLD_DB,
        user=PG_USER,
        password=PG_PASSWORD,
    )


def pick_col(df, candidates, cast_type=None, alias=None):
    """
    Pick the first available column from a list of possible names.
    If none exists, return NULL with optional cast and alias.
    """
    for c in candidates:
        if c in df.columns:
            col = F.col(c)
            if cast_type is not None:
                col = col.cast(cast_type)
            if alias is not None:
                col = col.alias(alias)
            return col

    col = F.lit(None)
    if cast_type is not None:
        col = col.cast(cast_type)
    if alias is not None:
        col = col.alias(alias)
    return col


def assert_target_objects_exist() -> None:
    """
    Fail early if the SQL bootstrap script has not created the required objects.
    """
    required = [
        # Final Gold OLAP tables
        (GOLD_SCHEMA, "dim_employee"),
        (GOLD_SCHEMA, "dim_date"),
        (GOLD_SCHEMA, "fact_activity"),
        (GOLD_SCHEMA, "params"),

        # Staging tables
        (STAGE_SCHEMA, "dim_employee_stage"),
        (STAGE_SCHEMA, "dim_date_stage"),
        (STAGE_SCHEMA, "fact_activity_stage"),
    ]

    schemas = list({s for s, _ in required})
    names = list({t for _, t in required})

    sql = """
    SELECT table_schema, table_name
    FROM information_schema.tables
    WHERE table_schema = ANY(%s)
      AND table_name = ANY(%s);
    """

    conn = pg_connect()
    try:
        with conn.cursor() as cur:
            cur.execute(sql, (schemas, names))
            found = {(r[0], r[1]) for r in cur.fetchall()}
    finally:
        conn.close()

    missing = [x for x in required if x not in found]

    if missing:
        raise RuntimeError(
            "Missing PostgreSQL Gold objects. Run the bootstrap SQL first.\n"
            f"Missing: {missing}\n"
            f"Connected to DB='{PG_GOLD_DB}' user='{PG_USER}' host='{PG_HOST}:{PG_PORT}'"
        )


def truncate_staging() -> None:
    """
    Staging is technical and temporary.
    It receives the current Gold publication batch.
    """
    sql = f"""
    TRUNCATE TABLE
      {STAGE_SCHEMA}.fact_activity_stage,
      {STAGE_SCHEMA}.dim_date_stage,
      {STAGE_SCHEMA}.dim_employee_stage;
    """

    log("Truncating PostgreSQL staging tables...")

    conn = pg_connect()
    try:
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute(sql)
    finally:
        conn.close()

    log("Staging tables truncated.")


def write_df_to_postgres(df, table_full_name: str) -> None:
    """
    Append a Spark DataFrame into a PostgreSQL staging table.
    """
    jdbc_url = f"jdbc:postgresql://{PG_HOST}:{PG_PORT}/{PG_GOLD_DB}"

    log(f"Appending DataFrame to PostgreSQL staging table: {table_full_name}")

    (
        df.write
        .format("jdbc")
        .option("url", jdbc_url)
        .option("dbtable", table_full_name)
        .option("user", PG_USER)
        .option("password", PG_PASSWORD)
        .option("driver", "org.postgresql.Driver")
        .mode("append")
        .save()
    )


def write_delta(df, path: str, partition_cols=None) -> None:
    """
    Write a Gold table locally as Delta.

    For this POC, Gold star-schema tables are overwritten with the latest
    trusted analytical state. Delta keeps table versions in _delta_log.
    """
    log(f"Writing Gold Delta table: {path}")

    writer = (
        df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
    )

    if partition_cols:
        writer = writer.partitionBy(*partition_cols)

    writer.save(path)


def validate_staging() -> None:
    """
    Minimal staging validation before publishing to final OLAP.
    """
    sql = f"""
    SELECT
      (SELECT COUNT(*) FROM {STAGE_SCHEMA}.dim_employee_stage) AS dim_employee_count,
      (SELECT COUNT(*) FROM {STAGE_SCHEMA}.dim_date_stage) AS dim_date_count,
      (SELECT COUNT(*) FROM {STAGE_SCHEMA}.fact_activity_stage) AS fact_activity_count;
    """

    conn = pg_connect()
    try:
        with conn.cursor() as cur:
            cur.execute(sql)
            dim_employee_count, dim_date_count, fact_activity_count = cur.fetchone()
    finally:
        conn.close()

    log(
        "Staging validation counts: "
        f"dim_employee={dim_employee_count}, "
        f"dim_date={dim_date_count}, "
        f"fact_activity={fact_activity_count}"
    )

    if dim_employee_count == 0:
        raise RuntimeError("Staging validation failed: dim_employee_stage is empty.")

    if dim_date_count == 0:
        raise RuntimeError("Staging validation failed: dim_date_stage is empty.")

    if fact_activity_count == 0:
        raise RuntimeError("Staging validation failed: fact_activity_stage is empty.")


def publish_staging_to_gold() -> None:
    """
    Publish PostgreSQL staging tables into final PostgreSQL OLAP Gold tables.

    Technique:
    - UPSERT dim_employee by employee_id
    - UPSERT dim_date by date_key
    - UPSERT fact_activity by activity_id

    PostgreSQL views then calculate KPIs dynamically from these final tables.
    """

    sql = f"""
    -- ============================================================
    -- 1. UPSERT dim_employee
    -- Business key: employee_id
    -- ============================================================

    INSERT INTO {GOLD_SCHEMA}.dim_employee (
      employee_id,
      last_name,
      first_name,
      birth_date,
      business_unit,
      hire_date,
      gross_salary_eur,
      contract_type,
      annual_leave_days,
      home_address,
      commute_mode,
      sport_practice,
      has_sport_practice,
      distance_km,
      commute_valid_for_bonus,
      business_hash,
      created_at,
      updated_at,
      commute_checked_at
    )
    SELECT
      employee_id,
      last_name,
      first_name,
      birth_date,
      business_unit,
      hire_date,
      gross_salary_eur,
      contract_type,
      annual_leave_days,
      home_address,
      commute_mode,
      sport_practice,
      has_sport_practice,
      distance_km,
      commute_valid_for_bonus,
      business_hash,
      created_at,
      updated_at,
      commute_checked_at
    FROM {STAGE_SCHEMA}.dim_employee_stage
    ON CONFLICT (employee_id) DO UPDATE SET
      last_name               = EXCLUDED.last_name,
      first_name              = EXCLUDED.first_name,
      birth_date              = EXCLUDED.birth_date,
      business_unit           = EXCLUDED.business_unit,
      hire_date               = EXCLUDED.hire_date,
      gross_salary_eur        = EXCLUDED.gross_salary_eur,
      contract_type           = EXCLUDED.contract_type,
      annual_leave_days       = EXCLUDED.annual_leave_days,
      home_address            = EXCLUDED.home_address,
      commute_mode            = EXCLUDED.commute_mode,
      sport_practice          = EXCLUDED.sport_practice,
      has_sport_practice      = EXCLUDED.has_sport_practice,
      distance_km             = EXCLUDED.distance_km,
      commute_valid_for_bonus = EXCLUDED.commute_valid_for_bonus,
      business_hash           = EXCLUDED.business_hash,
      created_at              = EXCLUDED.created_at,
      updated_at              = EXCLUDED.updated_at,
      commute_checked_at      = EXCLUDED.commute_checked_at;

    -- ============================================================
    -- 2. UPSERT dim_date
    -- Business key: date_key
    -- ============================================================

    INSERT INTO {GOLD_SCHEMA}.dim_date (
      date_key,
      date_day,
      year,
      quarter,
      month,
      month_name,
      day_of_month,
      day_of_week,
      day_name,
      week_of_year,
      is_weekend
    )
    SELECT
      date_key,
      date_day,
      year,
      quarter,
      month,
      month_name,
      day_of_month,
      day_of_week,
      day_name,
      week_of_year,
      is_weekend
    FROM {STAGE_SCHEMA}.dim_date_stage
    ON CONFLICT (date_key) DO UPDATE SET
      date_day      = EXCLUDED.date_day,
      year          = EXCLUDED.year,
      quarter       = EXCLUDED.quarter,
      month         = EXCLUDED.month,
      month_name    = EXCLUDED.month_name,
      day_of_month  = EXCLUDED.day_of_month,
      day_of_week   = EXCLUDED.day_of_week,
      day_name      = EXCLUDED.day_name,
      week_of_year  = EXCLUDED.week_of_year,
      is_weekend    = EXCLUDED.is_weekend;

    -- ============================================================
    -- 3. UPSERT fact_activity
    -- Business key: activity_id
    -- ============================================================

    INSERT INTO {GOLD_SCHEMA}.fact_activity (
      activity_id,
      employee_id,
      date_key,
      activity_date,
      start_time,
      sport_type,
      distance_m,
      elapsed_time_s,
      comment
    )
    SELECT
      activity_id,
      employee_id,
      date_key,
      activity_date,
      start_time,
      sport_type,
      distance_m,
      elapsed_time_s,
      comment
    FROM {STAGE_SCHEMA}.fact_activity_stage
    ON CONFLICT (activity_id) DO UPDATE SET
      employee_id      = EXCLUDED.employee_id,
      date_key         = EXCLUDED.date_key,
      activity_date    = EXCLUDED.activity_date,
      start_time       = EXCLUDED.start_time,
      sport_type       = EXCLUDED.sport_type,
      distance_m       = EXCLUDED.distance_m,
      elapsed_time_s   = EXCLUDED.elapsed_time_s,
      comment          = EXCLUDED.comment;
    """

    log("Publishing staging -> PostgreSQL OLAP Gold using UPSERT logic...")

    conn = pg_connect()
    try:
        conn.autocommit = False
        with conn.cursor() as cur:
            cur.execute(sql)
        conn.commit()
    except Exception as e:
        conn.rollback()
        log(f"ERROR during PostgreSQL OLAP publication: {e}")
        raise
    finally:
        conn.close()

    log("PostgreSQL OLAP Gold publication completed.")


def build_dim_date(spark: SparkSession, fact_activity):
    """
    Build a date dimension from the min/max activity_date in fact_activity.
    If the fact table is empty, create a one-day date dimension for today.
    """
    bounds = (
        fact_activity
        .agg(
            F.min("activity_date").alias("min_date"),
            F.max("activity_date").alias("max_date"),
        )
        .collect()[0]
    )

    min_date = bounds["min_date"] or date.today()
    max_date = bounds["max_date"] or date.today()

    log(f"Building dim_date from {min_date} to {max_date}")

    dim_date = (
        spark.sql(
            f"""
            SELECT explode(
              sequence(
                to_date('{min_date}'),
                to_date('{max_date}'),
                interval 1 day
              )
            ) AS date_day
            """
        )
        .select(
            F.date_format("date_day", "yyyyMMdd").cast("int").alias("date_key"),
            F.col("date_day").cast("date").alias("date_day"),
            F.year("date_day").cast("int").alias("year"),
            F.quarter("date_day").cast("int").alias("quarter"),
            F.month("date_day").cast("int").alias("month"),
            F.date_format("date_day", "MMMM").alias("month_name"),
            F.dayofmonth("date_day").cast("int").alias("day_of_month"),
            F.dayofweek("date_day").cast("int").alias("day_of_week"),
            F.date_format("date_day", "EEEE").alias("day_name"),
            F.weekofyear("date_day").cast("int").alias("week_of_year"),
            F.when(F.dayofweek("date_day").isin([1, 7]), F.lit(True))
             .otherwise(F.lit(False))
             .alias("is_weekend"),
        )
        .dropDuplicates(["date_key"])
    )

    return dim_date


# ============================================================
# Main logic
# ============================================================

def main():
    spark = build_spark()

    try:
        assert_target_objects_exist()

        # ------------------------------------------------------------
        # Read Silver Delta tables
        # ------------------------------------------------------------
        log(f"Reading Silver employee reference: {SILVER_EMPLOYEE_REF_PATH}")
        df_emp = spark.read.format("delta").load(SILVER_EMPLOYEE_REF_PATH)

        log(f"Reading Silver activities: {SILVER_ACTIVITIES_PATH}")
        df_act = spark.read.format("delta").load(SILVER_ACTIVITIES_PATH)

        # ------------------------------------------------------------
        # Build Gold dim_employee
        # ------------------------------------------------------------
        dim_employee = (
            df_emp.select(
                F.col("employee_id").cast("long").alias("employee_id"),
                pick_col(df_emp, ["last_name"], alias="last_name"),
                pick_col(df_emp, ["first_name"], alias="first_name"),
                pick_col(df_emp, ["birth_date"], cast_type="date", alias="birth_date"),
                pick_col(df_emp, ["business_unit"], alias="business_unit"),
                pick_col(df_emp, ["hire_date"], cast_type="date", alias="hire_date"),
                pick_col(df_emp, ["gross_salary_eur"], cast_type="double", alias="gross_salary_eur"),
                pick_col(df_emp, ["contract_type"], alias="contract_type"),
                pick_col(df_emp, ["annual_leave_days"], cast_type="int", alias="annual_leave_days"),
                pick_col(df_emp, ["home_address"], alias="home_address"),
                pick_col(df_emp, ["commute_mode"], alias="commute_mode"),
                pick_col(df_emp, ["sport_practice"], alias="sport_practice"),
                pick_col(df_emp, ["has_sport_practice"], cast_type="boolean", alias="has_sport_practice"),
                pick_col(df_emp, ["distance_km"], cast_type="double", alias="distance_km"),
                pick_col(df_emp, ["commute_valid_for_bonus"], cast_type="boolean", alias="commute_valid_for_bonus"),
                pick_col(df_emp, ["business_hash"], alias="business_hash"),
                pick_col(
                    df_emp,
                    ["created_at", "created_at_utc", "created_at (UTC)"],
                    cast_type="timestamp",
                    alias="created_at",
                ),
                pick_col(
                    df_emp,
                    ["updated_at", "updated_at_utc", "updated_at (UTC)"],
                    cast_type="timestamp",
                    alias="updated_at",
                ),
                pick_col(
                    df_emp,
                    ["commute_checked_at", "commute_checked_at_utc", "commute_checked_at (UTC)"],
                    cast_type="timestamp",
                    alias="commute_checked_at",
                ),
            )
            .dropDuplicates(["employee_id"])
            .cache()
        )

        # ------------------------------------------------------------
        # Build Gold fact_activity
        # ------------------------------------------------------------
        fact_activity_raw = (
            df_act.select(
                F.col("activity_id").cast("long").alias("activity_id"),
                F.col("employee_id").cast("long").alias("employee_id"),
                pick_col(df_act, ["activity_date"], cast_type="date", alias="activity_date"),
                pick_col(df_act, ["start_time"], cast_type="timestamp", alias="start_time"),
                pick_col(df_act, ["sport_type"], alias="sport_type"),
                pick_col(df_act, ["distance_m"], cast_type="int", alias="distance_m"),
                pick_col(df_act, ["elapsed_time_s"], cast_type="int", alias="elapsed_time_s"),
                pick_col(df_act, ["comment"], alias="comment"),
            )
            .dropDuplicates(["activity_id"])
            .cache()
        )

        # Keep only activities linked to known employees.
        # Unknown employee references should be handled by DQ checks upstream.
        fact_activity = (
            fact_activity_raw
            .join(
                dim_employee.select("employee_id"),
                on="employee_id",
                how="inner",
            )
            .withColumn(
                "date_key",
                F.date_format(F.col("activity_date"), "yyyyMMdd").cast("int"),
            )
            .select(
                "activity_id",
                "employee_id",
                "date_key",
                "activity_date",
                "start_time",
                "sport_type",
                "distance_m",
                "elapsed_time_s",
                "comment",
            )
            .cache()
        )

        # ------------------------------------------------------------
        # Build Gold dim_date
        # ------------------------------------------------------------
        dim_date = build_dim_date(spark, fact_activity).cache()

        # ------------------------------------------------------------
        # Write Gold Delta locally
        # ------------------------------------------------------------
        write_delta(dim_employee, GOLD_PATHS["dim_employee"])
        write_delta(dim_date, GOLD_PATHS["dim_date"])
        write_delta(fact_activity, GOLD_PATHS["fact_activity"], partition_cols=["activity_date"])

        # ------------------------------------------------------------
        # Publish Gold tables to PostgreSQL staging
        # ------------------------------------------------------------
        truncate_staging()

        write_df_to_postgres(dim_employee, f"{STAGE_SCHEMA}.dim_employee_stage")
        write_df_to_postgres(dim_date, f"{STAGE_SCHEMA}.dim_date_stage")
        write_df_to_postgres(fact_activity, f"{STAGE_SCHEMA}.fact_activity_stage")

        validate_staging()

        # ------------------------------------------------------------
        # UPSERT staging into final PostgreSQL OLAP Gold
        # ------------------------------------------------------------
        publish_staging_to_gold()

        log("Gold Delta star schema + PostgreSQL OLAP refresh completed successfully.")

        log(
            "Counts: "
            f"dim_employee={dim_employee.count()}, "
            f"dim_date={dim_date.count()}, "
            f"fact_activity={fact_activity.count()}"
        )

    finally:
        spark.stop()
        log("Spark stopped.")


if __name__ == "__main__":
    main()



#############################################################################
# # scripts/silver_to_gold_delta_and_olap.py
# KPI tables .. without date table 

# """
# Silver Delta -> Gold Delta -> PostgreSQL OLAP

# Purpose
# -------
# This script implements the Gold Delta approach:

# 1. Read Silver Delta tables:
#    - silver.employee_ref
#    - silver.activities

# 2. Build Gold business tables:
#    - dim_employee
#    - fact_activity
#    - kpi_executive_summary
#    - kpi_financial_impact
#    - kpi_wellbeing_days
#    - kpi_sports_activity
#    - kpi_data_quality
#    - invalid_commute_declarations

# 3. Persist those tables locally as Gold Delta:
#    - /opt/workspace/data/delta/gold/...

# 4. Publish the same Gold tables to PostgreSQL:
#    - TRUNCATE gold_staging.*
#    - APPEND current Gold tables into staging
#    - UPSERT staging into final gold.* OLAP tables

# Metabase should read from PostgreSQL gold schema, not staging.
# """

# import os
# from datetime import date

# import psycopg2
# from pyspark.sql import SparkSession, functions as F, types as T


# # ============================================================
# # Config
# # ============================================================

# SILVER_EMPLOYEE_REF_PATH = os.getenv(
#     "SILVER_EMPLOYEE_REF_PATH",
#     "/opt/workspace/data/delta/silver/employee_ref",
# )

# SILVER_ACTIVITIES_PATH = os.getenv(
#     "SILVER_ACTIVITIES_PATH",
#     "/opt/workspace/data/delta/silver/activities",
# )

# GOLD_BASE_PATH = os.getenv(
#     "GOLD_BASE_PATH",
#     "/opt/workspace/data/delta/gold",
# )

# GOLD_PATHS = {
#     "dim_employee": f"{GOLD_BASE_PATH}/dim_employee",
#     "fact_activity": f"{GOLD_BASE_PATH}/fact_activity",
#     "kpi_executive_summary": f"{GOLD_BASE_PATH}/kpi_executive_summary",
#     "kpi_financial_impact": f"{GOLD_BASE_PATH}/kpi_financial_impact",
#     "kpi_wellbeing_days": f"{GOLD_BASE_PATH}/kpi_wellbeing_days",
#     "kpi_sports_activity": f"{GOLD_BASE_PATH}/kpi_sports_activity",
#     "kpi_data_quality": f"{GOLD_BASE_PATH}/kpi_data_quality",
#     "invalid_commute_declarations": f"{GOLD_BASE_PATH}/invalid_commute_declarations",
# }

# PG_HOST = os.getenv("POSTGRES_HOST", "postgres")
# PG_PORT = int(os.getenv("POSTGRES_PORT", "5432"))

# PG_GOLD_DB = os.getenv("SPORT_OLAP_DB") or os.getenv("POSTGRES_GOLD_DB", "sportdw")
# PG_USER = os.getenv("SPORT_OLAP_USER") or os.getenv("POSTGRES_USER", "sport")
# PG_PASSWORD = os.getenv("SPORT_OLAP_PASSWORD") or os.getenv("POSTGRES_PASSWORD", "sport")

# GOLD_SCHEMA = "gold"
# STAGE_SCHEMA = "gold_staging"


# # ============================================================
# # Helpers
# # ============================================================

# def log(msg: str) -> None:
#     print(f"[silver_to_gold_delta_and_olap] {msg}", flush=True)


# def build_spark() -> SparkSession:
#     return (
#         SparkSession.builder
#         .appName("Silver_To_Gold_Delta_And_OLAP")
#         .getOrCreate()
#     )


# def pg_connect():
#     return psycopg2.connect(
#         host=PG_HOST,
#         port=PG_PORT,
#         dbname=PG_GOLD_DB,
#         user=PG_USER,
#         password=PG_PASSWORD,
#     )


# def pick_col(df, candidates, cast_type=None, alias=None):
#     """
#     Pick the first available column from a list of possible names.
#     If none exists, return NULL with optional cast and alias.
#     This makes the script resilient to small column naming differences.
#     """
#     for c in candidates:
#         if c in df.columns:
#             col = F.col(c)
#             if cast_type is not None:
#                 col = col.cast(cast_type)
#             if alias is not None:
#                 col = col.alias(alias)
#             return col

#     col = F.lit(None)
#     if cast_type is not None:
#         col = col.cast(cast_type)
#     if alias is not None:
#         col = col.alias(alias)
#     return col


# def get_params() -> dict:
#     """
#     Read business parameters from PostgreSQL gold.params.
#     Defaults are used if a parameter is missing.
#     """
#     params = {
#         "bonus_rate": 0.05,
#         "wellbeing_days": 5.0,
#         "wellbeing_min_activities_12m": 15.0,
#         "walk_run_max_km": 15.0,
#         "bike_scooter_other_max_km": 25.0,
#     }

#     sql = f"SELECT param_key, param_value FROM {GOLD_SCHEMA}.params;"

#     conn = pg_connect()
#     try:
#         with conn.cursor() as cur:
#             cur.execute(sql)
#             for key, value in cur.fetchall():
#                 params[key] = float(value)
#     finally:
#         conn.close()

#     return params


# def assert_target_objects_exist() -> None:
#     """
#     Fail early if the SQL bootstrap script has not created the required objects.
#     """
#     required = [
#         # Final Gold OLAP tables
#         (GOLD_SCHEMA, "dim_employee"),
#         (GOLD_SCHEMA, "fact_activity"),
#         (GOLD_SCHEMA, "kpi_executive_summary"),
#         (GOLD_SCHEMA, "kpi_financial_impact"),
#         (GOLD_SCHEMA, "kpi_wellbeing_days"),
#         (GOLD_SCHEMA, "kpi_sports_activity"),
#         (GOLD_SCHEMA, "kpi_data_quality"),
#         (GOLD_SCHEMA, "invalid_commute_declarations"),
#         (GOLD_SCHEMA, "params"),

#         # Staging tables
#         (STAGE_SCHEMA, "dim_employee_stage"),
#         (STAGE_SCHEMA, "fact_activity_stage"),
#         (STAGE_SCHEMA, "kpi_executive_summary_stage"),
#         (STAGE_SCHEMA, "kpi_financial_impact_stage"),
#         (STAGE_SCHEMA, "kpi_wellbeing_days_stage"),
#         (STAGE_SCHEMA, "kpi_sports_activity_stage"),
#         (STAGE_SCHEMA, "kpi_data_quality_stage"),
#         (STAGE_SCHEMA, "invalid_commute_declarations_stage"),
#     ]

#     schemas = list({s for s, _ in required})
#     names = list({t for _, t in required})

#     sql = """
#     SELECT table_schema, table_name
#     FROM information_schema.tables
#     WHERE table_schema = ANY(%s)
#       AND table_name = ANY(%s);
#     """

#     conn = pg_connect()
#     try:
#         with conn.cursor() as cur:
#             cur.execute(sql, (schemas, names))
#             found = {(r[0], r[1]) for r in cur.fetchall()}
#     finally:
#         conn.close()

#     missing = [x for x in required if x not in found]
#     if missing:
#         raise RuntimeError(
#             "Missing PostgreSQL Gold objects. Run the bootstrap SQL first.\n"
#             f"Missing: {missing}\n"
#             f"Connected to DB='{PG_GOLD_DB}' user='{PG_USER}' host='{PG_HOST}:{PG_PORT}'"
#         )


# def truncate_staging() -> None:
#     """
#     Staging is technical and temporary.
#     We truncate it before loading the current Gold publication batch.
#     """
#     sql = f"""
#     TRUNCATE TABLE
#       {STAGE_SCHEMA}.invalid_commute_declarations_stage,
#       {STAGE_SCHEMA}.kpi_data_quality_stage,
#       {STAGE_SCHEMA}.kpi_sports_activity_stage,
#       {STAGE_SCHEMA}.kpi_wellbeing_days_stage,
#       {STAGE_SCHEMA}.kpi_financial_impact_stage,
#       {STAGE_SCHEMA}.kpi_executive_summary_stage,
#       {STAGE_SCHEMA}.fact_activity_stage,
#       {STAGE_SCHEMA}.dim_employee_stage;
#     """

#     log("Truncating PostgreSQL staging tables...")
#     conn = pg_connect()
#     try:
#         conn.autocommit = True
#         with conn.cursor() as cur:
#             cur.execute(sql)
#     finally:
#         conn.close()

#     log("Staging tables truncated.")


# def write_df_to_postgres(df, table_full_name: str) -> None:
#     jdbc_url = f"jdbc:postgresql://{PG_HOST}:{PG_PORT}/{PG_GOLD_DB}"

#     log(f"Appending DataFrame to PostgreSQL staging table: {table_full_name}")

#     (
#         df.write
#         .format("jdbc")
#         .option("url", jdbc_url)
#         .option("dbtable", table_full_name)
#         .option("user", PG_USER)
#         .option("password", PG_PASSWORD)
#         .option("driver", "org.postgresql.Driver")
#         .mode("append")
#         .save()
#     )


# def write_delta(df, path: str, partition_cols=None) -> None:
#     """
#     Write a Gold table locally as Delta.
#     For this POC, Gold tables are overwritten with the latest trusted business state.
#     Delta still keeps table versions in _delta_log.
#     """
#     log(f"Writing Gold Delta table: {path}")

#     writer = (
#         df.write
#         .format("delta")
#         .mode("overwrite")
#         .option("overwriteSchema", "true")
#     )

#     if partition_cols:
#         writer = writer.partitionBy(*partition_cols)

#     writer.save(path)


# def validate_staging() -> None:
#     """
#     Minimal staging validation before publishing to final OLAP.
#     This is intentionally simple for the POC.
#     """
#     sql = f"""
#     SELECT
#       (SELECT COUNT(*) FROM {STAGE_SCHEMA}.dim_employee_stage) AS dim_employee_count,
#       (SELECT COUNT(*) FROM {STAGE_SCHEMA}.fact_activity_stage) AS fact_activity_count,
#       (SELECT COUNT(*) FROM {STAGE_SCHEMA}.kpi_executive_summary_stage) AS executive_summary_count;
#     """

#     conn = pg_connect()
#     try:
#         with conn.cursor() as cur:
#             cur.execute(sql)
#             dim_count, fact_count, summary_count = cur.fetchone()
#     finally:
#         conn.close()

#     log(
#         "Staging validation counts: "
#         f"dim_employee={dim_count}, "
#         f"fact_activity={fact_count}, "
#         f"kpi_executive_summary={summary_count}"
#     )

#     if dim_count == 0:
#         raise RuntimeError("Staging validation failed: dim_employee_stage is empty.")

#     if summary_count == 0:
#         raise RuntimeError("Staging validation failed: kpi_executive_summary_stage is empty.")


# def publish_staging_to_gold() -> None:
#     """
#     Publish PostgreSQL staging tables into final PostgreSQL OLAP Gold tables.

#     Main technique:
#     - UPSERT final OLAP tables from staging using PostgreSQL
#       INSERT ... ON CONFLICT (...) DO UPDATE.

#     Why UPSERT?
#     - If the business key already exists, update the existing row.
#     - If the business key does not exist, insert a new row.
#     - This makes the load idempotent.

#     Stale KPI cleanup:
#     - Since KPI tables are computed as current business outputs, we remove
#       stale KPI rows for the current snapshot/month/check date when they are
#       no longer present in staging.
#     """

#     sql = f"""
#     -- ============================================================
#     -- 1. UPSERT dim_employee
#     -- Business key: employee_id
#     -- ============================================================

#     INSERT INTO {GOLD_SCHEMA}.dim_employee (
#       employee_id,
#       last_name, first_name, birth_date, business_unit, hire_date,
#       gross_salary_eur, contract_type, annual_leave_days, home_address,
#       commute_mode, sport_practice, has_sport_practice,
#       distance_km, commute_valid_for_bonus,
#       business_hash, created_at, updated_at, commute_checked_at
#     )
#     SELECT
#       employee_id,
#       last_name, first_name, birth_date, business_unit, hire_date,
#       gross_salary_eur, contract_type, annual_leave_days, home_address,
#       commute_mode, sport_practice, has_sport_practice,
#       distance_km, commute_valid_for_bonus,
#       business_hash, created_at, updated_at, commute_checked_at
#     FROM {STAGE_SCHEMA}.dim_employee_stage
#     ON CONFLICT (employee_id) DO UPDATE SET
#       last_name               = EXCLUDED.last_name,
#       first_name              = EXCLUDED.first_name,
#       birth_date              = EXCLUDED.birth_date,
#       business_unit           = EXCLUDED.business_unit,
#       hire_date               = EXCLUDED.hire_date,
#       gross_salary_eur        = EXCLUDED.gross_salary_eur,
#       contract_type           = EXCLUDED.contract_type,
#       annual_leave_days       = EXCLUDED.annual_leave_days,
#       home_address            = EXCLUDED.home_address,
#       commute_mode            = EXCLUDED.commute_mode,
#       sport_practice          = EXCLUDED.sport_practice,
#       has_sport_practice      = EXCLUDED.has_sport_practice,
#       distance_km             = EXCLUDED.distance_km,
#       commute_valid_for_bonus = EXCLUDED.commute_valid_for_bonus,
#       business_hash           = EXCLUDED.business_hash,
#       created_at              = EXCLUDED.created_at,
#       updated_at              = EXCLUDED.updated_at,
#       commute_checked_at      = EXCLUDED.commute_checked_at;

#     -- ============================================================
#     -- 2. UPSERT fact_activity
#     -- Business key: activity_id
#     -- ============================================================

#     INSERT INTO {GOLD_SCHEMA}.fact_activity (
#       activity_id, employee_id, activity_date, start_time, sport_type,
#       distance_m, elapsed_time_s, comment
#     )
#     SELECT
#       activity_id, employee_id, activity_date, start_time, sport_type,
#       distance_m, elapsed_time_s, comment
#     FROM {STAGE_SCHEMA}.fact_activity_stage
#     ON CONFLICT (activity_id) DO UPDATE SET
#       employee_id      = EXCLUDED.employee_id,
#       activity_date    = EXCLUDED.activity_date,
#       start_time       = EXCLUDED.start_time,
#       sport_type       = EXCLUDED.sport_type,
#       distance_m       = EXCLUDED.distance_m,
#       elapsed_time_s   = EXCLUDED.elapsed_time_s,
#       comment          = EXCLUDED.comment;

#     -- ============================================================
#     -- 3. UPSERT kpi_executive_summary
#     -- Business key: snapshot_date
#     -- ============================================================

#     INSERT INTO {GOLD_SCHEMA}.kpi_executive_summary (
#       snapshot_date,
#       total_employees,
#       bonus_eligible_employees,
#       total_annual_bonus_cost_eur,
#       wellbeing_eligible_employees,
#       total_wellbeing_days_granted,
#       total_activities,
#       invalid_commute_declarations,
#       bonus_rate_used,
#       refreshed_at
#     )
#     SELECT
#       snapshot_date,
#       total_employees,
#       bonus_eligible_employees,
#       total_annual_bonus_cost_eur,
#       wellbeing_eligible_employees,
#       total_wellbeing_days_granted,
#       total_activities,
#       invalid_commute_declarations,
#       bonus_rate_used,
#       refreshed_at
#     FROM {STAGE_SCHEMA}.kpi_executive_summary_stage
#     ON CONFLICT (snapshot_date) DO UPDATE SET
#       total_employees                 = EXCLUDED.total_employees,
#       bonus_eligible_employees         = EXCLUDED.bonus_eligible_employees,
#       total_annual_bonus_cost_eur      = EXCLUDED.total_annual_bonus_cost_eur,
#       wellbeing_eligible_employees     = EXCLUDED.wellbeing_eligible_employees,
#       total_wellbeing_days_granted     = EXCLUDED.total_wellbeing_days_granted,
#       total_activities                 = EXCLUDED.total_activities,
#       invalid_commute_declarations     = EXCLUDED.invalid_commute_declarations,
#       bonus_rate_used                  = EXCLUDED.bonus_rate_used,
#       refreshed_at                     = EXCLUDED.refreshed_at;

#     -- ============================================================
#     -- 4. UPSERT kpi_financial_impact
#     -- Business key: snapshot_date + business_unit
#     -- ============================================================

#     INSERT INTO {GOLD_SCHEMA}.kpi_financial_impact (
#       snapshot_date,
#       business_unit,
#       total_employees,
#       bonus_eligible_employees,
#       average_salary_eur,
#       bonus_rate,
#       annual_bonus_cost_eur,
#       average_bonus_per_eligible_employee_eur,
#       refreshed_at
#     )
#     SELECT
#       snapshot_date,
#       business_unit,
#       total_employees,
#       bonus_eligible_employees,
#       average_salary_eur,
#       bonus_rate,
#       annual_bonus_cost_eur,
#       average_bonus_per_eligible_employee_eur,
#       refreshed_at
#     FROM {STAGE_SCHEMA}.kpi_financial_impact_stage
#     ON CONFLICT (snapshot_date, business_unit) DO UPDATE SET
#       total_employees                         = EXCLUDED.total_employees,
#       bonus_eligible_employees                 = EXCLUDED.bonus_eligible_employees,
#       average_salary_eur                       = EXCLUDED.average_salary_eur,
#       bonus_rate                               = EXCLUDED.bonus_rate,
#       annual_bonus_cost_eur                    = EXCLUDED.annual_bonus_cost_eur,
#       average_bonus_per_eligible_employee_eur  = EXCLUDED.average_bonus_per_eligible_employee_eur,
#       refreshed_at                             = EXCLUDED.refreshed_at;

#     DELETE FROM {GOLD_SCHEMA}.kpi_financial_impact g
#     WHERE g.snapshot_date IN (
#       SELECT DISTINCT snapshot_date
#       FROM {STAGE_SCHEMA}.kpi_financial_impact_stage
#     )
#     AND NOT EXISTS (
#       SELECT 1
#       FROM {STAGE_SCHEMA}.kpi_financial_impact_stage s
#       WHERE s.snapshot_date = g.snapshot_date
#         AND s.business_unit = g.business_unit
#     );

#     -- ============================================================
#     -- 5. UPSERT kpi_wellbeing_days
#     -- Business key: snapshot_date + employee_id
#     -- ============================================================

#     INSERT INTO {GOLD_SCHEMA}.kpi_wellbeing_days (
#       snapshot_date,
#       employee_id,
#       employee_name,
#       business_unit,
#       activity_count_12m,
#       wellbeing_eligible,
#       wellbeing_days_granted,
#       eligibility_status,
#       refreshed_at
#     )
#     SELECT
#       snapshot_date,
#       employee_id,
#       employee_name,
#       business_unit,
#       activity_count_12m,
#       wellbeing_eligible,
#       wellbeing_days_granted,
#       eligibility_status,
#       refreshed_at
#     FROM {STAGE_SCHEMA}.kpi_wellbeing_days_stage
#     ON CONFLICT (snapshot_date, employee_id) DO UPDATE SET
#       employee_name             = EXCLUDED.employee_name,
#       business_unit             = EXCLUDED.business_unit,
#       activity_count_12m        = EXCLUDED.activity_count_12m,
#       wellbeing_eligible        = EXCLUDED.wellbeing_eligible,
#       wellbeing_days_granted    = EXCLUDED.wellbeing_days_granted,
#       eligibility_status        = EXCLUDED.eligibility_status,
#       refreshed_at              = EXCLUDED.refreshed_at;

#     DELETE FROM {GOLD_SCHEMA}.kpi_wellbeing_days g
#     WHERE g.snapshot_date IN (
#       SELECT DISTINCT snapshot_date
#       FROM {STAGE_SCHEMA}.kpi_wellbeing_days_stage
#     )
#     AND NOT EXISTS (
#       SELECT 1
#       FROM {STAGE_SCHEMA}.kpi_wellbeing_days_stage s
#       WHERE s.snapshot_date = g.snapshot_date
#         AND s.employee_id = g.employee_id
#     );

#     -- ============================================================
#     -- 6. UPSERT kpi_sports_activity
#     -- Business key: activity_month + sport_type
#     -- ============================================================

#     INSERT INTO {GOLD_SCHEMA}.kpi_sports_activity (
#       activity_month,
#       sport_type,
#       activity_count,
#       active_employees,
#       total_distance_km,
#       average_distance_km,
#       average_duration_min,
#       refreshed_at
#     )
#     SELECT
#       activity_month,
#       sport_type,
#       activity_count,
#       active_employees,
#       total_distance_km,
#       average_distance_km,
#       average_duration_min,
#       refreshed_at
#     FROM {STAGE_SCHEMA}.kpi_sports_activity_stage
#     ON CONFLICT (activity_month, sport_type) DO UPDATE SET
#       activity_count        = EXCLUDED.activity_count,
#       active_employees      = EXCLUDED.active_employees,
#       total_distance_km     = EXCLUDED.total_distance_km,
#       average_distance_km   = EXCLUDED.average_distance_km,
#       average_duration_min  = EXCLUDED.average_duration_min,
#       refreshed_at          = EXCLUDED.refreshed_at;

#     DELETE FROM {GOLD_SCHEMA}.kpi_sports_activity g
#     WHERE g.activity_month IN (
#       SELECT DISTINCT activity_month
#       FROM {STAGE_SCHEMA}.kpi_sports_activity_stage
#     )
#     AND NOT EXISTS (
#       SELECT 1
#       FROM {STAGE_SCHEMA}.kpi_sports_activity_stage s
#       WHERE s.activity_month = g.activity_month
#         AND s.sport_type = g.sport_type
#     );

#     -- ============================================================
#     -- 7. UPSERT kpi_data_quality
#     -- Business key: check_date + dq_check_name
#     -- ============================================================

#     INSERT INTO {GOLD_SCHEMA}.kpi_data_quality (
#       check_date,
#       dq_check_name,
#       invalid_count,
#       severity,
#       business_impact,
#       status,
#       refreshed_at
#     )
#     SELECT
#       check_date,
#       dq_check_name,
#       invalid_count,
#       severity,
#       business_impact,
#       status,
#       refreshed_at
#     FROM {STAGE_SCHEMA}.kpi_data_quality_stage
#     ON CONFLICT (check_date, dq_check_name) DO UPDATE SET
#       invalid_count     = EXCLUDED.invalid_count,
#       severity          = EXCLUDED.severity,
#       business_impact   = EXCLUDED.business_impact,
#       status            = EXCLUDED.status,
#       refreshed_at      = EXCLUDED.refreshed_at;

#     DELETE FROM {GOLD_SCHEMA}.kpi_data_quality g
#     WHERE g.check_date IN (
#       SELECT DISTINCT check_date
#       FROM {STAGE_SCHEMA}.kpi_data_quality_stage
#     )
#     AND NOT EXISTS (
#       SELECT 1
#       FROM {STAGE_SCHEMA}.kpi_data_quality_stage s
#       WHERE s.check_date = g.check_date
#         AND s.dq_check_name = g.dq_check_name
#     );

#     -- ============================================================
#     -- 8. UPSERT invalid_commute_declarations
#     -- Business key: check_date + employee_id + issue_reason
#     -- ============================================================

#     INSERT INTO {GOLD_SCHEMA}.invalid_commute_declarations (
#       check_date,
#       employee_id,
#       employee_name,
#       business_unit,
#       commute_mode,
#       distance_km,
#       validation_rule,
#       issue_reason,
#       is_valid_for_bonus,
#       refreshed_at
#     )
#     SELECT
#       check_date,
#       employee_id,
#       employee_name,
#       business_unit,
#       commute_mode,
#       distance_km,
#       validation_rule,
#       issue_reason,
#       is_valid_for_bonus,
#       refreshed_at
#     FROM {STAGE_SCHEMA}.invalid_commute_declarations_stage
#     ON CONFLICT (check_date, employee_id, issue_reason) DO UPDATE SET
#       employee_name       = EXCLUDED.employee_name,
#       business_unit       = EXCLUDED.business_unit,
#       commute_mode        = EXCLUDED.commute_mode,
#       distance_km         = EXCLUDED.distance_km,
#       validation_rule     = EXCLUDED.validation_rule,
#       is_valid_for_bonus  = EXCLUDED.is_valid_for_bonus,
#       refreshed_at        = EXCLUDED.refreshed_at;

#     DELETE FROM {GOLD_SCHEMA}.invalid_commute_declarations g
#     WHERE g.check_date IN (
#       SELECT DISTINCT check_date
#       FROM {STAGE_SCHEMA}.kpi_data_quality_stage
#     )
#     AND NOT EXISTS (
#       SELECT 1
#       FROM {STAGE_SCHEMA}.invalid_commute_declarations_stage s
#       WHERE s.check_date = g.check_date
#         AND s.employee_id = g.employee_id
#         AND s.issue_reason = g.issue_reason
#     );
#     """

#     log("Publishing staging -> PostgreSQL OLAP Gold using UPSERT logic...")

#     conn = pg_connect()
#     try:
#         conn.autocommit = False
#         with conn.cursor() as cur:
#             cur.execute(sql)
#         conn.commit()
#     except Exception as e:
#         conn.rollback()
#         log(f"ERROR during PostgreSQL OLAP publication: {e}")
#         raise
#     finally:
#         conn.close()

#     log("PostgreSQL OLAP Gold publication completed.")


# # ============================================================
# # Main logic
# # ============================================================

# def main():
#     spark = build_spark()

#     try:
#         assert_target_objects_exist()
#         params = get_params()

#         bonus_rate = params["bonus_rate"]
#         wellbeing_days = params["wellbeing_days"]
#         wellbeing_min_activities = int(params["wellbeing_min_activities_12m"])
#         walk_run_max_km = params["walk_run_max_km"]
#         bike_scooter_other_max_km = params["bike_scooter_other_max_km"]

#         log(f"Using business params: {params}")

#         # ------------------------------------------------------------
#         # Read Silver Delta tables
#         # ------------------------------------------------------------
#         log(f"Reading Silver employee reference: {SILVER_EMPLOYEE_REF_PATH}")
#         df_emp = spark.read.format("delta").load(SILVER_EMPLOYEE_REF_PATH)

#         log(f"Reading Silver activities: {SILVER_ACTIVITIES_PATH}")
#         df_act = spark.read.format("delta").load(SILVER_ACTIVITIES_PATH)

#         # ------------------------------------------------------------
#         # Build Gold dim_employee
#         # ------------------------------------------------------------
#         dim_employee = (
#             df_emp.select(
#                 F.col("employee_id").cast("long").alias("employee_id"),
#                 pick_col(df_emp, ["last_name"], alias="last_name"),
#                 pick_col(df_emp, ["first_name"], alias="first_name"),
#                 pick_col(df_emp, ["birth_date"], cast_type="date", alias="birth_date"),
#                 pick_col(df_emp, ["business_unit"], alias="business_unit"),
#                 pick_col(df_emp, ["hire_date"], cast_type="date", alias="hire_date"),
#                 pick_col(df_emp, ["gross_salary_eur"], cast_type="double", alias="gross_salary_eur"),
#                 pick_col(df_emp, ["contract_type"], alias="contract_type"),
#                 pick_col(df_emp, ["annual_leave_days"], cast_type="int", alias="annual_leave_days"),
#                 pick_col(df_emp, ["home_address"], alias="home_address"),
#                 pick_col(df_emp, ["commute_mode"], alias="commute_mode"),
#                 pick_col(df_emp, ["sport_practice"], alias="sport_practice"),
#                 pick_col(df_emp, ["has_sport_practice"], cast_type="boolean", alias="has_sport_practice"),
#                 pick_col(df_emp, ["distance_km"], cast_type="double", alias="distance_km"),
#                 pick_col(df_emp, ["commute_valid_for_bonus"], cast_type="boolean", alias="commute_valid_for_bonus"),
#                 pick_col(df_emp, ["business_hash"], alias="business_hash"),
#                 pick_col(df_emp, ["created_at", "created_at_utc", "created_at (UTC)"], cast_type="timestamp", alias="created_at"),
#                 pick_col(df_emp, ["updated_at", "updated_at_utc", "updated_at (UTC)"], cast_type="timestamp", alias="updated_at"),
#                 pick_col(
#                     df_emp,
#                     ["commute_checked_at", "commute_checked_at_utc", "commute_checked_at (UTC)"],
#                     cast_type="timestamp",
#                     alias="commute_checked_at",
#                 ),
#             )
#             .dropDuplicates(["employee_id"])
#             .cache()
#         )

#         # ------------------------------------------------------------
#         # Build Gold fact_activity
#         # ------------------------------------------------------------
#         fact_activity_raw = (
#             df_act.select(
#                 F.col("activity_id").cast("long").alias("activity_id"),
#                 F.col("employee_id").cast("long").alias("employee_id"),
#                 pick_col(df_act, ["activity_date"], cast_type="date", alias="activity_date"),
#                 pick_col(df_act, ["start_time"], cast_type="timestamp", alias="start_time"),
#                 pick_col(df_act, ["sport_type"], alias="sport_type"),
#                 pick_col(df_act, ["distance_m"], cast_type="int", alias="distance_m"),
#                 pick_col(df_act, ["elapsed_time_s"], cast_type="int", alias="elapsed_time_s"),
#                 pick_col(df_act, ["comment"], alias="comment"),
#             )
#             .dropDuplicates(["activity_id"])
#             .cache()
#         )

#         # DQ: activities linked to unknown employees.
#         missing_employee_refs_count = fact_activity_raw.join(
#             dim_employee.select("employee_id"),
#             on="employee_id",
#             how="left_anti",
#         ).count()

#         # Keep only activities where employee exists.
#         fact_activity = (
#             fact_activity_raw.join(
#                 dim_employee.select("employee_id"),
#                 on="employee_id",
#                 how="inner",
#             )
#             .cache()
#         )

#         # ------------------------------------------------------------
#         # Common business calculations
#         # ------------------------------------------------------------
#         employee_name = F.trim(
#             F.concat_ws(
#                 " ",
#                 F.coalesce(F.col("first_name"), F.lit("")),
#                 F.coalesce(F.col("last_name"), F.lit("")),
#             )
#         )

#         dim_bonus = (
#             dim_employee
#             .withColumn(
#                 "business_unit_clean",
#                 F.coalesce(F.col("business_unit"), F.lit("Unknown")),
#             )
#             .withColumn(
#                 "bonus_eligible",
#                 F.coalesce(F.col("has_sport_practice"), F.lit(False))
#                 & F.coalesce(F.col("commute_valid_for_bonus"), F.lit(False)),
#             )
#             .withColumn(
#                 "bonus_amount_eur",
#                 F.when(
#                     F.col("bonus_eligible"),
#                     F.coalesce(F.col("gross_salary_eur"), F.lit(0.0)) * F.lit(float(bonus_rate)),
#                 ).otherwise(F.lit(0.0)),
#             )
#             .cache()
#         )

#         activities_12m = (
#             fact_activity
#             .filter(F.col("activity_date") >= F.add_months(F.current_date(), -12))
#             .groupBy("employee_id")
#             .agg(F.count("*").cast("long").alias("activity_count_12m"))
#         )

#         employee_benefits = (
#             dim_bonus
#             .join(activities_12m, on="employee_id", how="left")
#             .fillna({"activity_count_12m": 0})
#             .withColumn(
#                 "wellbeing_eligible",
#                 F.col("activity_count_12m") >= F.lit(wellbeing_min_activities),
#             )
#             .withColumn(
#                 "wellbeing_days_granted",
#                 F.when(
#                     F.col("wellbeing_eligible"),
#                     F.lit(float(wellbeing_days)),
#                 ).otherwise(F.lit(0.0)),
#             )
#             .cache()
#         )

#         # ------------------------------------------------------------
#         # Gold KPI: wellbeing days
#         # ------------------------------------------------------------
#         kpi_wellbeing_days = (
#             employee_benefits
#             .select(
#                 F.current_date().alias("snapshot_date"),
#                 F.col("employee_id"),
#                 employee_name.alias("employee_name"),
#                 F.col("business_unit_clean").alias("business_unit"),
#                 F.col("activity_count_12m"),
#                 F.col("wellbeing_eligible"),
#                 F.col("wellbeing_days_granted"),
#                 F.when(F.col("wellbeing_eligible"), F.lit("Eligible"))
#                  .when(
#                      F.col("activity_count_12m").between(10, wellbeing_min_activities - 1),
#                      F.lit("Close to eligibility"),
#                  )
#                  .otherwise(F.lit("Not eligible"))
#                  .alias("eligibility_status"),
#                 F.current_timestamp().alias("refreshed_at"),
#             )
#             .cache()
#         )

#         # ------------------------------------------------------------
#         # Gold KPI: invalid commute declarations
#         # ------------------------------------------------------------
#         commute_mode_lower = F.lower(F.coalesce(F.col("commute_mode"), F.lit("")))

#         walk_run_modes = ["walk_run", "walk", "walking", "run", "running"]
#         bike_other_modes = ["bike_scooter", "bike", "bicycle", "cycling", "scooter", "other"]

#         issue_reason = (
#             F.when(F.col("commute_mode").isNull(), F.lit("missing_commute_mode"))
#             .when(
#                 commute_mode_lower.isin(walk_run_modes) & F.col("distance_km").isNull(),
#                 F.lit("missing_distance_for_walk_run"),
#             )
#             .when(
#                 commute_mode_lower.isin(bike_other_modes) & F.col("distance_km").isNull(),
#                 F.lit("missing_distance_for_bike_scooter_other"),
#             )
#             .when(
#                 commute_mode_lower.isin(walk_run_modes) & (F.col("distance_km") > F.lit(float(walk_run_max_km))),
#                 F.lit("distance_too_high_walk_run"),
#             )
#             .when(
#                 commute_mode_lower.isin(bike_other_modes) & (F.col("distance_km") > F.lit(float(bike_scooter_other_max_km))),
#                 F.lit("distance_too_high_bike_scooter_other"),
#             )
#             .otherwise(F.lit(None))
#         )

#         validation_rule = (
#             F.when(
#                 commute_mode_lower.isin(walk_run_modes),
#                 F.lit(f"walking/running <= {walk_run_max_km} km"),
#             )
#             .when(
#                 commute_mode_lower.isin(bike_other_modes),
#                 F.lit(f"bike/scooter/other <= {bike_scooter_other_max_km} km"),
#             )
#             .otherwise(F.lit("commute_mode and distance must be declared"))
#         )

#         invalid_commute_declarations = (
#             dim_employee
#             .withColumn("issue_reason", issue_reason)
#             .filter(F.col("issue_reason").isNotNull())
#             .select(
#                 F.current_date().alias("check_date"),
#                 F.col("employee_id"),
#                 employee_name.alias("employee_name"),
#                 F.coalesce(F.col("business_unit"), F.lit("Unknown")).alias("business_unit"),
#                 F.col("commute_mode"),
#                 F.col("distance_km"),
#                 validation_rule.alias("validation_rule"),
#                 F.col("issue_reason"),
#                 F.lit(False).alias("is_valid_for_bonus"),
#                 F.current_timestamp().alias("refreshed_at"),
#             )
#             .cache()
#         )

#         invalid_commute_count = invalid_commute_declarations.count()
#         total_activities_count = fact_activity.count()

#         negative_distance_count = fact_activity_raw.filter(F.col("distance_m") < 0).count()

#         invalid_dates_count = fact_activity_raw.filter(
#             F.col("activity_date").isNull() | F.col("start_time").isNull()
#         ).count()

#         # ------------------------------------------------------------
#         # Gold KPI: executive summary
#         # ------------------------------------------------------------
#         kpi_executive_summary = (
#             employee_benefits
#             .agg(
#                 F.count("*").cast("long").alias("total_employees"),
#                 F.sum(F.when(F.col("bonus_eligible"), F.lit(1)).otherwise(F.lit(0))).cast("long").alias("bonus_eligible_employees"),
#                 F.sum("bonus_amount_eur").cast("double").alias("total_annual_bonus_cost_eur"),
#                 F.sum(F.when(F.col("wellbeing_eligible"), F.lit(1)).otherwise(F.lit(0))).cast("long").alias("wellbeing_eligible_employees"),
#                 F.sum("wellbeing_days_granted").cast("double").alias("total_wellbeing_days_granted"),
#             )
#             .withColumn("snapshot_date", F.current_date())
#             .withColumn("total_activities", F.lit(int(total_activities_count)).cast("long"))
#             .withColumn("invalid_commute_declarations", F.lit(int(invalid_commute_count)).cast("long"))
#             .withColumn("bonus_rate_used", F.lit(float(bonus_rate)).cast("double"))
#             .withColumn("refreshed_at", F.current_timestamp())
#             .select(
#                 "snapshot_date",
#                 "total_employees",
#                 "bonus_eligible_employees",
#                 "total_annual_bonus_cost_eur",
#                 "wellbeing_eligible_employees",
#                 "total_wellbeing_days_granted",
#                 "total_activities",
#                 "invalid_commute_declarations",
#                 "bonus_rate_used",
#                 "refreshed_at",
#             )
#             .cache()
#         )

#         # ------------------------------------------------------------
#         # Gold KPI: financial impact
#         # ------------------------------------------------------------
#         kpi_financial_impact = (
#             dim_bonus
#             .groupBy(F.col("business_unit_clean").alias("business_unit"))
#             .agg(
#                 F.count("*").cast("long").alias("total_employees"),
#                 F.sum(F.when(F.col("bonus_eligible"), F.lit(1)).otherwise(F.lit(0))).cast("long").alias("bonus_eligible_employees"),
#                 F.round(F.avg("gross_salary_eur"), 2).cast("double").alias("average_salary_eur"),
#                 F.sum("bonus_amount_eur").cast("double").alias("annual_bonus_cost_eur"),
#             )
#             .withColumn("snapshot_date", F.current_date())
#             .withColumn("bonus_rate", F.lit(float(bonus_rate)).cast("double"))
#             .withColumn(
#                 "average_bonus_per_eligible_employee_eur",
#                 F.when(
#                     F.col("bonus_eligible_employees") > 0,
#                     F.round(F.col("annual_bonus_cost_eur") / F.col("bonus_eligible_employees"), 2),
#                 ).otherwise(F.lit(0.0)),
#             )
#             .withColumn("refreshed_at", F.current_timestamp())
#             .select(
#                 "snapshot_date",
#                 "business_unit",
#                 "total_employees",
#                 "bonus_eligible_employees",
#                 "average_salary_eur",
#                 "bonus_rate",
#                 "annual_bonus_cost_eur",
#                 "average_bonus_per_eligible_employee_eur",
#                 "refreshed_at",
#             )
#             .cache()
#         )

#         # ------------------------------------------------------------
#         # Gold KPI: sports activity
#         # ------------------------------------------------------------
#         kpi_sports_activity = (
#             fact_activity
#             .withColumn("activity_month", F.trunc("activity_date", "MM"))
#             .withColumn("sport_type_clean", F.coalesce(F.col("sport_type"), F.lit("Unknown")))
#             .groupBy("activity_month", "sport_type_clean")
#             .agg(
#                 F.count("*").cast("long").alias("activity_count"),
#                 F.countDistinct("employee_id").cast("long").alias("active_employees"),
#                 F.round(F.sum(F.coalesce(F.col("distance_m"), F.lit(0))) / 1000.0, 2).cast("double").alias("total_distance_km"),
#                 F.round(F.avg(F.coalesce(F.col("distance_m"), F.lit(0))) / 1000.0, 2).cast("double").alias("average_distance_km"),
#                 F.round(F.avg(F.coalesce(F.col("elapsed_time_s"), F.lit(0))) / 60.0, 2).cast("double").alias("average_duration_min"),
#             )
#             .withColumnRenamed("sport_type_clean", "sport_type")
#             .withColumn("refreshed_at", F.current_timestamp())
#             .select(
#                 "activity_month",
#                 "sport_type",
#                 "activity_count",
#                 "active_employees",
#                 "total_distance_km",
#                 "average_distance_km",
#                 "average_duration_min",
#                 "refreshed_at",
#             )
#             .cache()
#         )

#         # ------------------------------------------------------------
#         # Gold KPI: data quality
#         # ------------------------------------------------------------
#         today = date.today()

#         dq_rows = [
#             (
#                 today,
#                 "Invalid commute declarations",
#                 int(invalid_commute_count),
#                 "High",
#                 "May wrongly grant or reject bonus eligibility",
#                 "Failed" if invalid_commute_count > 0 else "Passed",
#             ),
#             (
#                 today,
#                 "Activity with negative distance",
#                 int(negative_distance_count),
#                 "High",
#                 "Invalid activity metric",
#                 "Failed" if negative_distance_count > 0 else "Passed",
#             ),
#             (
#                 today,
#                 "Activity with invalid dates",
#                 int(invalid_dates_count),
#                 "High",
#                 "Invalid activity chronology",
#                 "Failed" if invalid_dates_count > 0 else "Passed",
#             ),
#             (
#                 today,
#                 "Missing employee reference",
#                 int(missing_employee_refs_count),
#                 "Medium",
#                 "Activity cannot be linked to HR reference",
#                 "Failed" if missing_employee_refs_count > 0 else "Passed",
#             ),
#         ]

#         dq_schema = T.StructType([
#             T.StructField("check_date", T.DateType(), False),
#             T.StructField("dq_check_name", T.StringType(), False),
#             T.StructField("invalid_count", T.LongType(), False),
#             T.StructField("severity", T.StringType(), False),
#             T.StructField("business_impact", T.StringType(), False),
#             T.StructField("status", T.StringType(), False),
#         ])

#         kpi_data_quality = (
#             spark.createDataFrame(dq_rows, dq_schema)
#             .withColumn("refreshed_at", F.current_timestamp())
#             .cache()
#         )

#         # ------------------------------------------------------------
#         # Write Gold Delta locally
#         # ------------------------------------------------------------
#         write_delta(dim_employee, GOLD_PATHS["dim_employee"])
#         write_delta(fact_activity, GOLD_PATHS["fact_activity"], partition_cols=["activity_date"])
#         write_delta(kpi_executive_summary, GOLD_PATHS["kpi_executive_summary"])
#         write_delta(kpi_financial_impact, GOLD_PATHS["kpi_financial_impact"], partition_cols=["snapshot_date"])
#         write_delta(kpi_wellbeing_days, GOLD_PATHS["kpi_wellbeing_days"], partition_cols=["snapshot_date"])
#         write_delta(kpi_sports_activity, GOLD_PATHS["kpi_sports_activity"], partition_cols=["activity_month"])
#         write_delta(kpi_data_quality, GOLD_PATHS["kpi_data_quality"], partition_cols=["check_date"])
#         write_delta(invalid_commute_declarations, GOLD_PATHS["invalid_commute_declarations"], partition_cols=["check_date"])

#         # ------------------------------------------------------------
#         # Publish Gold tables to PostgreSQL staging
#         # ------------------------------------------------------------
#         truncate_staging()

#         write_df_to_postgres(dim_employee, f"{STAGE_SCHEMA}.dim_employee_stage")
#         write_df_to_postgres(fact_activity, f"{STAGE_SCHEMA}.fact_activity_stage")
#         write_df_to_postgres(kpi_executive_summary, f"{STAGE_SCHEMA}.kpi_executive_summary_stage")
#         write_df_to_postgres(kpi_financial_impact, f"{STAGE_SCHEMA}.kpi_financial_impact_stage")
#         write_df_to_postgres(kpi_wellbeing_days, f"{STAGE_SCHEMA}.kpi_wellbeing_days_stage")
#         write_df_to_postgres(kpi_sports_activity, f"{STAGE_SCHEMA}.kpi_sports_activity_stage")
#         write_df_to_postgres(kpi_data_quality, f"{STAGE_SCHEMA}.kpi_data_quality_stage")
#         write_df_to_postgres(invalid_commute_declarations, f"{STAGE_SCHEMA}.invalid_commute_declarations_stage")

#         validate_staging()

#         # ------------------------------------------------------------
#         # UPSERT staging into final PostgreSQL OLAP Gold
#         # ------------------------------------------------------------
#         publish_staging_to_gold()

#         log("Gold Delta + PostgreSQL OLAP refresh completed successfully.")

#         log(
#             "Counts: "
#             f"dim_employee={dim_employee.count()}, "
#             f"fact_activity={fact_activity.count()}, "
#             f"kpi_executive_summary={kpi_executive_summary.count()}, "
#             f"kpi_financial_impact={kpi_financial_impact.count()}, "
#             f"kpi_wellbeing_days={kpi_wellbeing_days.count()}, "
#             f"kpi_sports_activity={kpi_sports_activity.count()}, "
#             f"kpi_data_quality={kpi_data_quality.count()}, "
#             f"invalid_commute_declarations={invalid_commute_declarations.count()}"
#         )

#     finally:
#         spark.stop()
#         log("Spark stopped.")


# if __name__ == "__main__":
#     main()


###############################################################
# without date table #

# # scripts/silver_to_gold_postgres.py
# """
# Option B (Robust, evaluator-friendly): Silver (Delta) -> Gold (Postgres sportdw) with UPSERT

# LOAD-ONLY version (recommended):
# - Assumes sport_flow1_bootstrap already created:
#   * DB + schemas gold/gold_staging
#   * tables (dim_employee, fact_activity, staging tables)
#   * params + business views
# - This script only:
#   1) truncates staging
#   2) writes Silver data into staging
#   3) UPSERTs staging -> gold

# Updated to include new Silver employee_ref columns:
# - business_hash
# - created_at
# - updated_at
# - commute_checked_at
# """

# import os
# import psycopg2
# from pyspark.sql import SparkSession, functions as F


# # -----------------------------
# # Config
# # -----------------------------
# SILVER_EMPLOYEE_REF_PATH = os.getenv(
#     "SILVER_EMPLOYEE_REF_PATH",
#     "/opt/workspace/data/delta/silver/employee_ref",
# )

# SILVER_ACTIVITIES_PATH = os.getenv(
#     "SILVER_ACTIVITIES_PATH",
#     "/opt/workspace/data/delta/silver/activities",
# )

# PG_HOST = os.getenv("POSTGRES_HOST", "postgres")
# PG_PORT = int(os.getenv("POSTGRES_PORT", "5432"))

# PG_GOLD_DB = os.getenv("SPORT_OLAP_DB") or os.getenv("POSTGRES_GOLD_DB", "sportdw")
# PG_USER = os.getenv("SPORT_OLAP_USER") or os.getenv("POSTGRES_USER", "sport")
# PG_PASSWORD = os.getenv("SPORT_OLAP_PASSWORD") or os.getenv("POSTGRES_PASSWORD", "sport")

# GOLD_SCHEMA = "gold"
# STAGE_SCHEMA = "gold_staging"


# def log(msg: str) -> None:
#     print(f"[silver_to_gold_upsert] {msg}", flush=True)


# def build_spark() -> SparkSession:
#     return SparkSession.builder.appName("Silver_To_Gold_Postgres_UPSERT").getOrCreate()


# def pg_connect():
#     return psycopg2.connect(
#         host=PG_HOST,
#         port=PG_PORT,
#         dbname=PG_GOLD_DB,
#         user=PG_USER,
#         password=PG_PASSWORD,
#     )


# # -----------------------------
# # Helpers
# # -----------------------------
# def pick_col(df, candidates, cast_type=None, alias=None):
#     """
#     Returns the first existing column from candidates, optionally casted and aliased.
#     If none exist, returns NULL of cast_type (or NULL as-is).
#     This makes the script resilient if column naming differs slightly.
#     """
#     for c in candidates:
#         if c in df.columns:
#             col = F.col(c)
#             if cast_type is not None:
#                 col = col.cast(cast_type)
#             if alias is not None:
#                 col = col.alias(alias)
#             return col

#     # column not found -> NULL
#     col = F.lit(None)
#     if cast_type is not None:
#         col = col.cast(cast_type)
#     if alias is not None:
#         col = col.alias(alias)
#     return col


# def assert_target_objects_exist() -> None:
#     """
#     Fail fast with a clear message if Flow1 bootstrap has not created the targets.
#     """
#     required = [
#         (GOLD_SCHEMA, "dim_employee"),
#         (GOLD_SCHEMA, "fact_activity"),
#         (STAGE_SCHEMA, "dim_employee_stage"),
#         (STAGE_SCHEMA, "fact_activity_stage"),
#         (GOLD_SCHEMA, "params"),
#     ]

#     sql = """
#     SELECT table_schema, table_name
#     FROM information_schema.tables
#     WHERE table_schema = ANY(%s)
#       AND table_name = ANY(%s);
#     """

#     schemas = list({s for s, _ in required})
#     names = list({t for _, t in required})

#     conn = pg_connect()
#     try:
#         with conn.cursor() as cur:
#             cur.execute(sql, (schemas, names))
#             found = {(r[0], r[1]) for r in cur.fetchall()}
#     finally:
#         conn.close()

#     missing = [x for x in required if x not in found]
#     if missing:
#         msg = (
#             "Missing Gold objects in Postgres. Run sport_flow1_bootstrap first.\n"
#             f"Missing: {missing}\n"
#             f"Connected to DB='{PG_GOLD_DB}' user='{PG_USER}' host='{PG_HOST}:{PG_PORT}'"
#         )
#         raise RuntimeError(msg)


# def truncate_staging() -> None:
#     sql = f"""
#     TRUNCATE TABLE {STAGE_SCHEMA}.fact_activity_stage;
#     TRUNCATE TABLE {STAGE_SCHEMA}.dim_employee_stage;
#     """
#     log("Truncating staging tables ...")
#     conn = pg_connect()
#     try:
#         conn.autocommit = True
#         with conn.cursor() as cur:
#             cur.execute(sql)
#     finally:
#         conn.close()
#     log("Staging tables truncated.")


# def write_df_to_postgres(df, table_full_name: str) -> None:
#     jdbc_url = f"jdbc:postgresql://{PG_HOST}:{PG_PORT}/{PG_GOLD_DB}"
#     log(f"Writing -> {table_full_name} via JDBC {jdbc_url}")
#     (
#         df.write.format("jdbc")
#         .option("url", jdbc_url)
#         .option("dbtable", table_full_name)
#         .option("user", PG_USER)
#         .option("password", PG_PASSWORD)
#         .option("driver", "org.postgresql.Driver")
#         .mode("append")
#         .save()
#     )


# def upsert_gold_from_staging() -> None:
#     """
#     UPSERT staging -> gold in a single transaction.
#     Includes NEW employee columns (business_hash, created_at, updated_at, commute_checked_at).
#     """
#     sql_dim = f"""
#     INSERT INTO {GOLD_SCHEMA}.dim_employee (
#       employee_id,
#       last_name, first_name, birth_date, business_unit, hire_date,
#       gross_salary_eur, contract_type, annual_leave_days, home_address,
#       commute_mode, sport_practice, has_sport_practice,
#       distance_km, commute_valid_for_bonus,
#       business_hash, created_at, updated_at, commute_checked_at
#     )
#     SELECT
#       employee_id,
#       last_name, first_name, birth_date, business_unit, hire_date,
#       gross_salary_eur, contract_type, annual_leave_days, home_address,
#       commute_mode, sport_practice, has_sport_practice,
#       distance_km, commute_valid_for_bonus,
#       business_hash, created_at, updated_at, commute_checked_at
#     FROM {STAGE_SCHEMA}.dim_employee_stage
#     ON CONFLICT (employee_id) DO UPDATE SET
#       last_name               = EXCLUDED.last_name,
#       first_name              = EXCLUDED.first_name,
#       birth_date              = EXCLUDED.birth_date,
#       business_unit           = EXCLUDED.business_unit,
#       hire_date               = EXCLUDED.hire_date,
#       gross_salary_eur        = EXCLUDED.gross_salary_eur,
#       contract_type           = EXCLUDED.contract_type,
#       annual_leave_days       = EXCLUDED.annual_leave_days,
#       home_address            = EXCLUDED.home_address,
#       commute_mode            = EXCLUDED.commute_mode,
#       sport_practice          = EXCLUDED.sport_practice,
#       has_sport_practice      = EXCLUDED.has_sport_practice,
#       distance_km             = EXCLUDED.distance_km,
#       commute_valid_for_bonus = EXCLUDED.commute_valid_for_bonus,
#       business_hash           = EXCLUDED.business_hash,
#       created_at              = EXCLUDED.created_at,
#       updated_at              = EXCLUDED.updated_at,
#       commute_checked_at      = EXCLUDED.commute_checked_at;
#     """

#     sql_fact = f"""
#     INSERT INTO {GOLD_SCHEMA}.fact_activity (
#       activity_id, employee_id, activity_date, start_time, sport_type,
#       distance_m, elapsed_time_s, comment
#     )
#     SELECT
#       activity_id, employee_id, activity_date, start_time, sport_type,
#       distance_m, elapsed_time_s, comment
#     FROM {STAGE_SCHEMA}.fact_activity_stage
#     ON CONFLICT (activity_id) DO UPDATE SET
#       employee_id      = EXCLUDED.employee_id,
#       activity_date    = EXCLUDED.activity_date,
#       start_time       = EXCLUDED.start_time,
#       sport_type       = EXCLUDED.sport_type,
#       distance_m       = EXCLUDED.distance_m,
#       elapsed_time_s   = EXCLUDED.elapsed_time_s,
#       comment          = EXCLUDED.comment;
#     """

#     log("Upserting staging -> gold (single transaction) ...")
#     conn = pg_connect()
#     try:
#         conn.autocommit = False
#         with conn.cursor() as cur:
#             cur.execute(sql_dim)
#             cur.execute(sql_fact)
#         conn.commit()
#     except Exception as e:
#         conn.rollback()
#         log(f"ERROR during UPSERT: {e}")
#         raise
#     finally:
#         conn.close()
#     log("UPSERT completed.")


# def main():
#     spark = build_spark()
#     try:
#         # Fail fast if Flow1 was not applied
#         assert_target_objects_exist()

#         truncate_staging()

#         log(f"Reading Delta: {SILVER_EMPLOYEE_REF_PATH}")
#         df_emp = spark.read.format("delta").load(SILVER_EMPLOYEE_REF_PATH)

#         log(f"Reading Delta: {SILVER_ACTIVITIES_PATH}")
#         df_act = spark.read.format("delta").load(SILVER_ACTIVITIES_PATH)

#         # ---- DIM EMPLOYEE (with NEW columns) ----
#         dim_employee = (
#             df_emp.select(
#                 F.col("employee_id").cast("long").alias("employee_id"),
#                 pick_col(df_emp, ["last_name"], alias="last_name"),
#                 pick_col(df_emp, ["first_name"], alias="first_name"),
#                 pick_col(df_emp, ["birth_date"], cast_type="date", alias="birth_date"),
#                 pick_col(df_emp, ["business_unit"], alias="business_unit"),
#                 pick_col(df_emp, ["hire_date"], cast_type="date", alias="hire_date"),
#                 pick_col(df_emp, ["gross_salary_eur"], cast_type="double", alias="gross_salary_eur"),
#                 pick_col(df_emp, ["contract_type"], alias="contract_type"),
#                 pick_col(df_emp, ["annual_leave_days"], cast_type="int", alias="annual_leave_days"),
#                 pick_col(df_emp, ["home_address"], alias="home_address"),
#                 pick_col(df_emp, ["commute_mode"], alias="commute_mode"),
#                 pick_col(df_emp, ["sport_practice"], alias="sport_practice"),
#                 pick_col(df_emp, ["has_sport_practice"], cast_type="boolean", alias="has_sport_practice"),
#                 pick_col(df_emp, ["distance_km"], cast_type="double", alias="distance_km"),
#                 pick_col(df_emp, ["commute_valid_for_bonus"], cast_type="boolean", alias="commute_valid_for_bonus"),

#                 # NEW Silver columns (name-robust)
#                 pick_col(df_emp, ["business_hash"], alias="business_hash"),
#                 pick_col(df_emp, ["created_at", "created_at_utc", "created_at (UTC)"], cast_type="timestamp", alias="created_at"),
#                 pick_col(df_emp, ["updated_at", "updated_at_utc", "updated_at (UTC)"], cast_type="timestamp", alias="updated_at"),
#                 pick_col(df_emp, ["commute_checked_at", "commute_checked_at_utc", "commute_checked_at (UTC)"],
#                          cast_type="timestamp", alias="commute_checked_at"),
#             )
#             .dropDuplicates(["employee_id"])
#         )

#         # ---- FACT ACTIVITY ----
#         fact_activity = (
#             df_act.select(
#                 F.col("activity_id").cast("long").alias("activity_id"),
#                 F.col("employee_id").cast("long").alias("employee_id"),
#                 pick_col(df_act, ["activity_date"], cast_type="date", alias="activity_date"),
#                 pick_col(df_act, ["start_time"], cast_type="timestamp", alias="start_time"),
#                 pick_col(df_act, ["sport_type"], alias="sport_type"),
#                 pick_col(df_act, ["distance_m"], cast_type="int", alias="distance_m"),
#                 pick_col(df_act, ["elapsed_time_s"], cast_type="int", alias="elapsed_time_s"),
#                 pick_col(df_act, ["comment"], alias="comment"),
#             )
#             .dropDuplicates(["activity_id"])
#         )

#         # Prevent FK problems: keep only activities where employee exists
#         fact_activity = fact_activity.join(
#             dim_employee.select("employee_id"),
#             on="employee_id",
#             how="inner"
#         )

#         write_df_to_postgres(dim_employee, f"{STAGE_SCHEMA}.dim_employee_stage")
#         write_df_to_postgres(fact_activity, f"{STAGE_SCHEMA}.fact_activity_stage")

#         upsert_gold_from_staging()

#         # Optional logs (costly, but useful in demo)
#         log(f"Loaded counts: dim_employee={dim_employee.count()} fact_activity={fact_activity.count()}")
#         log("Done.")
#     finally:
#         spark.stop()
#         log("Spark stopped.")


# if __name__ == "__main__":
#     main()

