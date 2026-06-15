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

