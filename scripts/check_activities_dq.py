# scripts/check_activities_dq.py
"""
Data quality checks for the silver.activities Delta table.

Goal
----
Run a small set of sanity checks on the curated activities dataset in Silver
before we aggregate to Gold and expose to Metabase.

This script is meant to be run INSIDE the Spark container, for example:

    docker compose exec spark-master \
      spark-submit /opt/workspace/scripts/check_activities_dq.py

High-level steps
----------------
1. Start a SparkSession.
2. Read the Delta table from /opt/workspace/data/delta/silver/activities.
3. Wrap the DataFrame with Great Expectations (SparkDFDataset).
4. Run a few expectations:
   - employee_id / start_time / sport_type / elapsed_time_s not null
   - elapsed_time_s >= 60 seconds
   - table row count in a reasonable range
5. Run a couple of manual Spark checks:
   - distance_m < 0
   - extremely long durations (elapsed_time_s > 4 hours)
6. Print a small summary and exit 0 if all checks pass, 1 otherwise.

You will later be able to call this script from Kestra as a Python task:
- If exit code != 0 → fail the Kestra execution.
"""

import os
import sys

from pyspark.sql import SparkSession, functions as F
from great_expectations.dataset import SparkDFDataset


# --------------------------------------------------------------------
# Config
# --------------------------------------------------------------------

SILVER_ACTIVITIES_PATH = os.getenv(
    "SILVER_ACTIVITIES_PATH",
    "/opt/workspace/data/delta/silver/activities",
)


def build_spark_session() -> SparkSession:
    """Create a simple SparkSession for batch DQ."""
    spark = (
        SparkSession.builder
        .appName("DQ_Silver_Activities")
        .getOrCreate()
    )
    print(f"[DQ] Spark version: {spark.version}", flush=True)
    return spark


def print_expectation_result(name: str, result: dict) -> bool:
    """
    Pretty-print a Great Expectations result and return success boolean.
    """
    success = bool(result.get("success"))
    details = result.get("result", {})
    unexpected_count = details.get("unexpected_count")
    observed_value = details.get("observed_value")

    line = f"[DQ] {name}: success={success}"
    if unexpected_count is not None:
        line += f", unexpected_count={unexpected_count}"
    if observed_value is not None:
        line += f", observed_value={observed_value}"

    print(line, flush=True)
    return success


def main() -> None:
    spark = build_spark_session()

    # 1) Load silver.activities
    print(f"[DQ] Reading Delta table from: {SILVER_ACTIVITIES_PATH}", flush=True)
    df = (
        spark.read
        .format("delta")
        .load(SILVER_ACTIVITIES_PATH)
    )

    print("[DQ] silver.activities schema:", flush=True)
    df.printSchema()

    # Optional: quick row count
    total_rows = df.count()
    print(f"[DQ] Row count = {total_rows}", flush=True)

    # 2) Wrap with Great Expectations
    ge_df = SparkDFDataset(df)

    all_ok = True
    checks = []

    # ---------------- GE expectations ----------------

    # Basic "not null" constraints
    checks.append((
        "employee_id_not_null",
        ge_df.expect_column_values_to_not_be_null("employee_id"),
    ))
    checks.append((
        "start_time_not_null",
        ge_df.expect_column_values_to_not_be_null("start_time"),
    ))
    checks.append((
        "sport_type_not_null",
        ge_df.expect_column_values_to_not_be_null("sport_type"),
    ))
    checks.append((
        "elapsed_time_not_null",
        ge_df.expect_column_values_to_not_be_null("elapsed_time_s"),
    ))

    # elapsed_time_s should be at least 60 seconds (1 minute)
    checks.append((
        "elapsed_time_min_60s",
        ge_df.expect_column_values_to_be_between(
            "elapsed_time_s",
            min_value=60,
            max_value=None,
        ),
    ))

    # Table row count should not be "too small" or "insanely large"
    # (you can adjust these bounds later if needed)
    checks.append((
        "row_count_reasonable",
        ge_df.expect_table_row_count_to_be_between(
            min_value=10,          # at least 10 rows
            max_value=10_000_000,  # arbitrary large upper bound
        ),
    ))

    print("[DQ] Running Great Expectations checks...", flush=True)
    for name, result in checks:
        ok = print_expectation_result(name, result)
        if not ok:
            all_ok = False

    # ---------------- Manual Spark checks ----------------

    print("[DQ] Running manual Spark checks...", flush=True)

    # 1) distance_m should not be negative (NULL is allowed for some sports)
    negative_distance_cnt = df.filter(F.col("distance_m") < 0).count()
    if negative_distance_cnt > 0:
        print(
            f"[DQ] distance_m_negative: FOUND {negative_distance_cnt} rows with distance_m < 0",
            flush=True,
        )
        all_ok = False
    else:
        print("[DQ] distance_m_negative: OK (no negative distances)", flush=True)

    # 2) elapsed_time_s should not be absurdly large (e.g. > 4 hours = 14400s)
    too_long_cnt = df.filter(F.col("elapsed_time_s") > 14400).count()
    if too_long_cnt > 0:
        print(
            f"[DQ] elapsed_time_too_long: FOUND {too_long_cnt} rows with elapsed_time_s > 14400",
            flush=True,
        )
        all_ok = False
    else:
        print("[DQ] elapsed_time_too_long: OK (no durations > 4h)", flush=True)

    # 3) Optional: quick min/max for sanity (no expectation, just info)
    minmax = df.select(
        F.min("start_time").alias("min_start_time"),
        F.max("start_time").alias("max_start_time"),
    ).collect()[0]

    print(
        f"[DQ] start_time range: {minmax['min_start_time']} -> {minmax['max_start_time']}",
        flush=True,
    )

    # ---------------- Final status ----------------

    if all_ok:
        print("[DQ] ✅ All data quality checks passed.", flush=True)
        spark.stop()
        sys.exit(0)
    else:
        print("[DQ] ❌ Some data quality checks FAILED. See logs above.", flush=True)
        spark.stop()
        # Non-zero exit code so Kestra / CI / shell can detect failure
        sys.exit(1)


if __name__ == "__main__":
    main()
