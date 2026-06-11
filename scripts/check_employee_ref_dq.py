# scripts/check_employee_ref_dq.py
"""
Data quality checks for silver.employee_ref using Great Expectations + Spark rules.

Aligned with updated pipeline scripts:
- hr_sportive_to_silver_employee_ref.py (MERGE + created_at/updated_at/business_hash + preserves enrichment cols)
- enrich_commute_gmaps.py (incremental enrichment + MERGE + commute_checked_at)

Expected columns (core):
- employee_id (PK)
- commute_mode (controlled vocab)
- sport_practice + has_sport_practice (consistency)
- birth_date, hire_date, gross_salary_eur (sanity)

Expected operational columns:
- created_at (timestamp, not null)
- updated_at (timestamp, not null)
- business_hash (string, not null)

Expected enrichment columns:
- distance_km (double, nullable)
- commute_valid_for_bonus (boolean, nullable but should be present when checked)
- commute_checked_at (timestamp, nullable)

Rules (note de cadrage defaults):
- walk_run => max 15 km
- bike_scooter / other => max 25 km
- car/public_transport => not eligible (bonus=False)
"""

import os
import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from great_expectations.dataset import SparkDFDataset


# -------------------------------------------------------------------
# Config
# -------------------------------------------------------------------
COMMUTE_MAX_KM_WALK_RUN = float(os.getenv("COMMUTE_MAX_KM_WALK_RUN", "15.0"))
COMMUTE_MAX_KM_OTHER = float(os.getenv("COMMUTE_MAX_KM_OTHER", "25.0"))

# Backward-compat: if COMMUTE_MAX_KM is set, treat it as walk_run threshold
if os.getenv("COMMUTE_MAX_KM") is not None:
    COMMUTE_MAX_KM_WALK_RUN = float(os.getenv("COMMUTE_MAX_KM", str(COMMUTE_MAX_KM_WALK_RUN)))

EMPLOYEE_REF_PATH = os.getenv("EMPLOYEE_REF_PATH", "/opt/workspace/data/delta/silver/employee_ref")

# If set to "1", print a tiny sample of violating employee_id only (still safe)
DEBUG_VIOLATION_IDS = os.getenv("DQ_DEBUG_VIOLATION_IDS", "0") == "1"
MAX_DEBUG_IDS = int(os.getenv("DQ_MAX_DEBUG_IDS", "10"))


def build_spark_session() -> SparkSession:
    spark = (
        SparkSession.builder
        .appName("DQ_Silver_EmployeeRef")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel(os.getenv("SPARK_LOG_LEVEL", "WARN"))
    print("[DQ employee_ref] Spark version:", spark.version, flush=True)
    return spark


def load_employee_ref(spark: SparkSession):
    print(f"[DQ employee_ref] Reading Delta table from: {EMPLOYEE_REF_PATH}", flush=True)
    df = spark.read.format("delta").load(EMPLOYEE_REF_PATH)

    print("[DQ employee_ref] Schema:", flush=True)
    df.printSchema()

    # No df.show() here to avoid PII leakage (names/addresses)
    row_count = df.count()
    print(f"[DQ employee_ref] Row count: {row_count}", flush=True)
    return df


def _print_violation_ids(df, condition_col, label: str):
    """
    Print only employee_id for a small sample when debugging.
    This avoids leaking PII like names/addresses.
    """
    if not DEBUG_VIOLATION_IDS:
        return
    ids = (
        df.filter(condition_col)
          .select("employee_id")
          .limit(MAX_DEBUG_IDS)
          .collect()
    )
    ids = [r["employee_id"] for r in ids]
    if ids:
        print(f"     sample employee_id violating {label}: {ids}", flush=True)


def require_columns(df, cols, group_name: str) -> bool:
    missing = [c for c in cols if c not in df.columns]
    if missing:
        print(f"[DQ employee_ref] ❌ Missing {group_name} columns: {missing}", flush=True)
        return False
    print(f"[DQ employee_ref] ✅ Required {group_name} columns present.", flush=True)
    return True


def run_ge_expectations(df) -> bool:
    """
    Great Expectations checks for core constraints.
    Keep this minimal + stable; use Spark checks for more complex logic.
    """
    ge_df = SparkDFDataset(df)
    print("[DQ employee_ref] Running Great Expectations checks...", flush=True)

    results = []

    # PK existence + uniqueness
    results.append(("employee_id_not_null", ge_df.expect_column_values_to_not_be_null("employee_id")))
    results.append(("employee_id_unique", ge_df.expect_column_values_to_be_unique("employee_id")))

    # Controlled vocab
    allowed_commute = ["walk_run", "bike_scooter", "car", "public_transport", "other"]
    results.append(("commute_mode_in_set", ge_df.expect_column_values_to_be_in_set("commute_mode", allowed_commute)))

    # Boolean checks
    results.append(("has_sport_practice_boolean", ge_df.expect_column_values_to_be_in_set("has_sport_practice", [True, False])))

    # Salary sanity
    results.append(("gross_salary_not_null", ge_df.expect_column_values_to_not_be_null("gross_salary_eur")))
    results.append(("gross_salary_non_negative", ge_df.expect_column_min_to_be_between("gross_salary_eur", min_value=0, max_value=None)))

    # Parsing sanity
    results.append(("birth_date_not_null", ge_df.expect_column_values_to_not_be_null("birth_date")))
    results.append(("hire_date_not_null", ge_df.expect_column_values_to_not_be_null("hire_date")))

    # Operational columns (new)
    results.append(("created_at_not_null", ge_df.expect_column_values_to_not_be_null("created_at")))
    results.append(("updated_at_not_null", ge_df.expect_column_values_to_not_be_null("updated_at")))
    results.append(("business_hash_not_null", ge_df.expect_column_values_to_not_be_null("business_hash")))

    # Enrichment columns (existence + basic type checks)
    results.append(("commute_checked_at_column_exists", {"success": ("commute_checked_at" in df.columns)}))
    results.append(("distance_km_column_exists", {"success": ("distance_km" in df.columns)}))
    results.append(("commute_valid_for_bonus_column_exists", {"success": ("commute_valid_for_bonus" in df.columns)}))

    all_ok = True
    print("[DQ employee_ref] GE expectation results:", flush=True)

    for name, res in results:
        success = res.get("success", False)
        if success:
            print(f"  ✅ {name}", flush=True)
        else:
            all_ok = False
            print(f"  ❌ {name}", flush=True)
            unexpected = res.get("result", {}).get("unexpected_count")
            if unexpected is not None:
                print(f"     unexpected_count = {unexpected}", flush=True)

    return all_ok


def run_cross_column_checks(df) -> bool:
    """
    Cross-column checks:

    Sport consistency:
      A) has_sport_practice=True  -> sport_practice NOT NULL
      B) has_sport_practice=False -> sport_practice NULL

    Operational sanity:
      E) created_at <= updated_at (no future reversal)

    Enrichment consistency:
      C) If commute_valid_for_bonus=True:
         - commute_mode must be in (walk_run, bike_scooter, other)
         - distance_km NOT NULL
         - distance_km <= threshold (15 for walk_run, 25 for others)
         - commute_checked_at NOT NULL
      D) If commute_mode in (car, public_transport) -> commute_valid_for_bonus must be False (or NULL only if never checked)
      F) If commute_checked_at is NOT NULL -> commute_valid_for_bonus must NOT be NULL
      G) If distance_km is NOT NULL -> distance_km >= 0
    """
    print("[DQ employee_ref] Running cross-column consistency checks...", flush=True)
    ok = True

    # Required columns for these checks
    required = {
        "employee_id",
        "has_sport_practice",
        "sport_practice",
        "commute_mode",
        "created_at",
        "updated_at",
        "distance_km",
        "commute_valid_for_bonus",
        "commute_checked_at",
    }
    if not require_columns(df, required, "cross-check"):
        return False

    # A) has_sport_practice=True -> sport_practice NOT NULL
    bad_a = df.filter((F.col("has_sport_practice") == True) & F.col("sport_practice").isNull()).count()
    if bad_a == 0:
        print("  ✅ Rule A: has_sport_practice=True -> sport_practice NOT NULL", flush=True)
    else:
        print("  ❌ Rule A violation count:", bad_a, flush=True)
        _print_violation_ids(df, (F.col("has_sport_practice") == True) & F.col("sport_practice").isNull(), "Rule A")
        ok = False

    # B) has_sport_practice=False -> sport_practice NULL
    bad_b = df.filter((F.col("has_sport_practice") == False) & F.col("sport_practice").isNotNull()).count()
    if bad_b == 0:
        print("  ✅ Rule B: has_sport_practice=False -> sport_practice NULL", flush=True)
    else:
        print("  ❌ Rule B violation count:", bad_b, flush=True)
        _print_violation_ids(df, (F.col("has_sport_practice") == False) & F.col("sport_practice").isNotNull(), "Rule B")
        ok = False

    # E) created_at <= updated_at (basic operational sanity)
    bad_e = df.filter(F.col("created_at") > F.col("updated_at")).count()
    if bad_e == 0:
        print("  ✅ Rule E: created_at <= updated_at", flush=True)
    else:
        print("  ❌ Rule E violation count:", bad_e, flush=True)
        _print_violation_ids(df, F.col("created_at") > F.col("updated_at"), "Rule E")
        ok = False

    # Threshold per mode: 15km for walk_run, else 25km
    thr = F.when(F.col("commute_mode") == "walk_run", F.lit(COMMUTE_MAX_KM_WALK_RUN)).otherwise(F.lit(COMMUTE_MAX_KM_OTHER))

    # C) bonus True => must satisfy mode+distance+checked_at+threshold
    bad_c = df.filter(
        (F.col("commute_valid_for_bonus") == True) &
        (
            (~F.col("commute_mode").isin(["walk_run", "bike_scooter", "other"])) |
            F.col("distance_km").isNull() |
            F.col("commute_checked_at").isNull() |
            (F.col("distance_km") > thr)
        )
    ).count()
    if bad_c == 0:
        print("  ✅ Rule C: bonus=True -> sport mode + distance present + checked_at present + <= threshold (15/25)", flush=True)
    else:
        print("  ❌ Rule C violation count:", bad_c, flush=True)
        _print_violation_ids(
            df,
            (F.col("commute_valid_for_bonus") == True) &
            (
                (~F.col("commute_mode").isin(["walk_run", "bike_scooter", "other"])) |
                F.col("distance_km").isNull() |
                F.col("commute_checked_at").isNull() |
                (F.col("distance_km") > thr)
            ),
            "Rule C"
        )
        ok = False

    # D) car/public_transport => bonus must be False (or NULL only if never checked)
    # If commute_checked_at is NOT NULL, then bonus cannot be NULL; it must be False.
    bad_d = df.filter(
        (F.col("commute_mode").isin(["car", "public_transport"])) &
        (
            (F.col("commute_checked_at").isNotNull() & (F.col("commute_valid_for_bonus") != F.lit(False))) |
            (F.col("commute_checked_at").isNull() & (F.col("commute_valid_for_bonus") == F.lit(True)))
        )
    ).count()
    if bad_d == 0:
        print("  ✅ Rule D: car/public_transport -> bonus=False (when checked), never True", flush=True)
    else:
        print("  ❌ Rule D violation count:", bad_d, flush=True)
        _print_violation_ids(
            df,
            (F.col("commute_mode").isin(["car", "public_transport"])) &
            (
                (F.col("commute_checked_at").isNotNull() & (F.col("commute_valid_for_bonus") != F.lit(False))) |
                (F.col("commute_checked_at").isNull() & (F.col("commute_valid_for_bonus") == F.lit(True)))
            ),
            "Rule D"
        )
        ok = False

    # F) If checked_at is present -> commute_valid_for_bonus must not be NULL
    bad_f = df.filter(F.col("commute_checked_at").isNotNull() & F.col("commute_valid_for_bonus").isNull()).count()
    if bad_f == 0:
        print("  ✅ Rule F: checked_at present -> commute_valid_for_bonus NOT NULL", flush=True)
    else:
        print("  ❌ Rule F violation count:", bad_f, flush=True)
        _print_violation_ids(df, F.col("commute_checked_at").isNotNull() & F.col("commute_valid_for_bonus").isNull(), "Rule F")
        ok = False

    # G) If distance_km present -> non-negative
    bad_g = df.filter(F.col("distance_km").isNotNull() & (F.col("distance_km") < F.lit(0))).count()
    if bad_g == 0:
        print("  ✅ Rule G: distance_km >= 0 when present", flush=True)
    else:
        print("  ❌ Rule G violation count:", bad_g, flush=True)
        _print_violation_ids(df, F.col("distance_km").isNotNull() & (F.col("distance_km") < F.lit(0)), "Rule G")
        ok = False

    return ok


def main():
    spark = build_spark_session()
    df = load_employee_ref(spark)

    # Presence checks (quick fail-fast)
    core_ok = require_columns(df, {
        "employee_id", "commute_mode", "has_sport_practice", "sport_practice",
        "birth_date", "hire_date", "gross_salary_eur",
        "created_at", "updated_at", "business_hash",
        "distance_km", "commute_valid_for_bonus", "commute_checked_at"
    }, "core")

    if not core_ok:
        spark.stop()
        print("[DQ employee_ref] ❌ Missing core columns -> FAIL", flush=True)
        sys.exit(1)

    ge_ok = run_ge_expectations(df)
    cross_ok = run_cross_column_checks(df)

    all_ok = ge_ok and cross_ok

    spark.stop()

    if all_ok:
        print("[DQ employee_ref] ✅ All data quality checks passed.", flush=True)
        sys.exit(0)
    else:
        print("[DQ employee_ref] ❌ Some data quality checks FAILED.", flush=True)
        sys.exit(1)


if __name__ == "__main__":
    main()

#######################################################################################
# # scripts/check_employee_ref_dq.py
# """
# Data quality checks for silver.employee_ref using Great Expectations + Spark rules.

# Enrichment columns expected (from enrich_commute_gmaps.py):
#     distance_km              (double, nullable)
#     commute_valid_for_bonus  (boolean)

# Rules (Note de cadrage defaults):
# - walk_run => max 15 km
# - bike_scooter / other => max 25 km
# """

# import os
# import sys

# from pyspark.sql import SparkSession
# from pyspark.sql import functions as F
# from great_expectations.dataset import SparkDFDataset


# # -------------------------------------------------------------------
# # Config
# # -------------------------------------------------------------------
# COMMUTE_MAX_KM_WALK_RUN = float(os.getenv("COMMUTE_MAX_KM_WALK_RUN", "15.0"))
# COMMUTE_MAX_KM_OTHER = float(os.getenv("COMMUTE_MAX_KM_OTHER", "25.0"))

# # Backward-compat: if COMMUTE_MAX_KM is set, treat it as walk_run threshold
# if os.getenv("COMMUTE_MAX_KM") is not None:
#     COMMUTE_MAX_KM_WALK_RUN = float(os.getenv("COMMUTE_MAX_KM", str(COMMUTE_MAX_KM_WALK_RUN)))

# EMPLOYEE_REF_PATH = os.getenv("EMPLOYEE_REF_PATH", "/opt/workspace/data/delta/silver/employee_ref")


# def build_spark_session() -> SparkSession:
#     spark = (
#         SparkSession.builder
#         .appName("DQ_Silver_EmployeeRef")
#         .getOrCreate()
#     )
#     print("[DQ employee_ref] Spark version:", spark.version, flush=True)
#     return spark


# def load_employee_ref(spark: SparkSession):
#     print(f"[DQ employee_ref] Reading Delta table from: {EMPLOYEE_REF_PATH}", flush=True)
#     df = spark.read.format("delta").load(EMPLOYEE_REF_PATH)

#     print("[DQ employee_ref] Schema:", flush=True)
#     df.printSchema()

#     print("[DQ employee_ref] Sample rows:", flush=True)
#     df.show(10, truncate=False)

#     print(f"[DQ employee_ref] Row count: {df.count()}", flush=True)
#     return df


# def run_ge_expectations(df) -> bool:
#     ge_df = SparkDFDataset(df)
#     print("[DQ employee_ref] Running Great Expectations checks...", flush=True)

#     results = []

#     # PK existence + uniqueness
#     results.append(("employee_id_not_null", ge_df.expect_column_values_to_not_be_null("employee_id")))
#     results.append(("employee_id_unique", ge_df.expect_column_values_to_be_unique("employee_id")))

#     # commute_mode controlled vocab (UPDATED to include bike_scooter)
#     allowed_commute = ["walk_run", "bike_scooter", "car", "public_transport", "other"]
#     results.append(("commute_mode_in_set", ge_df.expect_column_values_to_be_in_set("commute_mode", allowed_commute)))

#     # booleans
#     results.append(("has_sport_practice_boolean", ge_df.expect_column_values_to_be_in_set("has_sport_practice", [True, False])))

#     # salary sanity
#     results.append(("gross_salary_non_negative", ge_df.expect_column_min_to_be_between("gross_salary_eur", min_value=0, max_value=None)))

#     # parsing sanity
#     results.append(("birth_date_not_null", ge_df.expect_column_values_to_not_be_null("birth_date")))
#     results.append(("hire_date_not_null", ge_df.expect_column_values_to_not_be_null("hire_date")))
#     results.append(("gross_salary_not_null", ge_df.expect_column_values_to_not_be_null("gross_salary_eur")))

#     # enrichment columns existence + checks
#     if "distance_km" in df.columns:
#         results.append(("distance_km_non_negative", ge_df.expect_column_min_to_be_between("distance_km", min_value=0, max_value=None)))
#     else:
#         results.append(("distance_km_column_exists", {"success": False, "result": {"unexpected_count": None}}))

#     if "commute_valid_for_bonus" in df.columns:
#         results.append(("commute_valid_for_bonus_boolean", ge_df.expect_column_values_to_be_in_set("commute_valid_for_bonus", [True, False])))
#     else:
#         results.append(("commute_valid_for_bonus_column_exists", {"success": False, "result": {"unexpected_count": None}}))

#     all_ok = True
#     print("[DQ employee_ref] GE expectation results:", flush=True)
#     for name, res in results:
#         success = res.get("success", False)
#         if success:
#             print(f"  ✅ {name}", flush=True)
#         else:
#             all_ok = False
#             print(f"  ❌ {name}", flush=True)
#             unexpected = res.get("result", {}).get("unexpected_count")
#             if unexpected is not None:
#                 print(f"     unexpected_count = {unexpected}", flush=True)

#     return all_ok


# def run_cross_column_checks(df) -> bool:
#     """
#     Cross-column checks:

#     Sport consistency:
#       A) has_sport_practice=True  -> sport_practice NOT NULL
#       B) has_sport_practice=False -> sport_practice NULL

#     Commute consistency (UPDATED):
#       C) If commute_valid_for_bonus=True:
#          - commute_mode must be in (walk_run, bike_scooter, other)
#          - distance_km NOT NULL
#          - distance_km <= threshold (15 for walk_run, 25 for others)
#       D) If commute_mode in (car, public_transport) -> commute_valid_for_bonus must be False
#     """
#     print("[DQ employee_ref] Running cross-column consistency checks...", flush=True)
#     ok = True

#     # A/B sport checks
#     bad_true = df.filter((F.col("has_sport_practice") == True) & F.col("sport_practice").isNull()).count()
#     bad_false = df.filter((F.col("has_sport_practice") == False) & F.col("sport_practice").isNotNull()).count()

#     if bad_true == 0:
#         print("  ✅ Rule A: has_sport_practice=True -> sport_practice NOT NULL", flush=True)
#     else:
#         print("  ❌ Rule A violation count:", bad_true, flush=True)
#         ok = False

#     if bad_false == 0:
#         print("  ✅ Rule B: has_sport_practice=False -> sport_practice NULL", flush=True)
#     else:
#         print("  ❌ Rule B violation count:", bad_false, flush=True)
#         ok = False

#     # Enrichment cols exist?
#     required_cols = {"distance_km", "commute_valid_for_bonus", "commute_mode"}
#     missing = [c for c in required_cols if c not in df.columns]
#     if missing:
#         print(f"  ❌ Missing required columns: {missing}", flush=True)
#         return False

#     # Threshold per mode
#     thr = F.when(F.col("commute_mode") == "walk_run", F.lit(COMMUTE_MAX_KM_WALK_RUN)).otherwise(F.lit(COMMUTE_MAX_KM_OTHER))

#     # C) bonus True => must satisfy mode+distance+threshold
#     bad_bonus_true = df.filter(
#         (F.col("commute_valid_for_bonus") == True) &
#         (
#             (~F.col("commute_mode").isin(["walk_run", "bike_scooter", "other"])) |
#             F.col("distance_km").isNull() |
#             (F.col("distance_km") > thr)
#         )
#     ).count()

#     if bad_bonus_true == 0:
#         print("  ✅ Rule C: bonus=True -> sport mode + distance present + <= threshold (15/25)", flush=True)
#     else:
#         print("  ❌ Rule C violation count:", bad_bonus_true, flush=True)
#         ok = False

#     # D) car/public_transport => bonus must be False
#     bad_non_sport_bonus = df.filter(
#         (F.col("commute_mode").isin(["car", "public_transport"])) &
#         (F.col("commute_valid_for_bonus") != F.lit(False))
#     ).count()

#     if bad_non_sport_bonus == 0:
#         print("  ✅ Rule D: car/public_transport -> bonus=False", flush=True)
#     else:
#         print("  ❌ Rule D violation count:", bad_non_sport_bonus, flush=True)
#         ok = False

#     return ok


# def main():
#     spark = build_spark_session()
#     df = load_employee_ref(spark)

#     ge_ok = run_ge_expectations(df)
#     cross_ok = run_cross_column_checks(df)

#     all_ok = ge_ok and cross_ok

#     spark.stop()

#     if all_ok:
#         print("[DQ employee_ref] ✅ All data quality checks passed.", flush=True)
#         sys.exit(0)
#     else:
#         print("[DQ employee_ref] ❌ Some data quality checks FAILED.", flush=True)
#         sys.exit(1)


# if __name__ == "__main__":
#     main()


#######################################################################################
# # scripts/check_employee_ref_dq.py
# """
# Data quality checks for silver.employee_ref using Great Expectations + Spark rules.

# This table is built in two steps:
#   1) HR + sportive -> silver.employee_ref (base columns)
#   2) Commute enrichment -> adds:
#        - distance_km
#        - commute_valid_for_bonus

# Delta input (inside Spark container)
# ------------------------------------
#     /opt/workspace/data/delta/silver/employee_ref

# Expected base schema (from HR_Sportive_To_Silver_EmployeeRef job)
# -----------------------------------------------------------------
#     employee_id        (bigint)
#     last_name          (string)
#     first_name         (string)
#     birth_date         (date)
#     business_unit      (string)
#     hire_date          (date)
#     gross_salary_eur   (double)
#     contract_type      (string)
#     annual_leave_days  (int)
#     home_address       (string)
#     commute_mode       (string)   # walk_run, car, public_transport, other
#     sport_practice     (string)   # tennis, running, hiking, etc. or null
#     has_sport_practice (boolean)

# Expected enrichment columns (from enrich_commute_gmaps.py)
# ----------------------------------------------------------
#     distance_km              (double, nullable)   # can be null if API failed / no address
#     commute_valid_for_bonus  (boolean)            # True only for walk_run and <= threshold

# Exit code:
#   - 0 if all checks pass
#   - 1 if any check fails
# """

# import os
# import sys

# from pyspark.sql import SparkSession
# from pyspark.sql import functions as F

# from great_expectations.dataset import SparkDFDataset


# # -------------------------------------------------------------------
# # Config
# # -------------------------------------------------------------------

# # Same threshold used by your enrichment script (keep consistent)
# COMMUTE_MAX_KM = float(os.getenv("COMMUTE_MAX_KM", "15.0"))

# EMPLOYEE_REF_PATH = "/opt/workspace/data/delta/silver/employee_ref"


# # -------------------------------------------------------------------
# # Helpers
# # -------------------------------------------------------------------

# def build_spark_session() -> SparkSession:
#     """
#     Start Spark. Spark configs are expected to be provided by the container / spark-defaults.conf.
#     """
#     spark = (
#         SparkSession.builder
#         .appName("DQ_Silver_EmployeeRef")
#         .getOrCreate()
#     )
#     print("[DQ employee_ref] Spark version:", spark.version)
#     return spark


# def load_employee_ref(spark: SparkSession):
#     """
#     Load the Delta table under test.

#     Note: df.count() and df.show() trigger Spark actions (they scan data),
#     which is fine for DQ/testing but can be expensive on very large tables.
#     """
#     print(f"[DQ employee_ref] Reading Delta table from: {EMPLOYEE_REF_PATH}")

#     df = (
#         spark.read
#         .format("delta")
#         .load(EMPLOYEE_REF_PATH)
#     )

#     print("[DQ employee_ref] Schema:")
#     df.printSchema()

#     print("[DQ employee_ref] Sample rows:")
#     df.show(10, truncate=False)

#     print(f"[DQ employee_ref] Row count: {df.count()}")
#     return df


# # -------------------------------------------------------------------
# # GE expectations
# # -------------------------------------------------------------------

# def run_ge_expectations(df) -> bool:
#     """
#     Run Great Expectations checks on the DataFrame.

#     Returns:
#         True  if all expectations succeed
#         False otherwise
#     """
#     ge_df = SparkDFDataset(df)
#     print("[DQ employee_ref] Running Great Expectations checks...")

#     results = []

#     # -----------------------------
#     # Base table expectations
#     # -----------------------------

#     # 1) Primary key must exist
#     results.append(("employee_id_not_null", ge_df.expect_column_values_to_not_be_null("employee_id")))

#     # 2) Primary key must be unique
#     results.append(("employee_id_unique", ge_df.expect_column_values_to_be_unique("employee_id")))

#     # 3) commute_mode must be in standardized set
#     allowed_commute = ["walk_run", "car", "public_transport", "other"]
#     results.append((
#         "commute_mode_in_set",
#         ge_df.expect_column_values_to_be_in_set("commute_mode", allowed_commute)
#     ))

#     # 4) has_sport_practice must be boolean (True/False)
#     results.append((
#         "has_sport_practice_boolean",
#         ge_df.expect_column_values_to_be_in_set("has_sport_practice", [True, False])
#     ))

#     # 5) salary must be >= 0 (basic sanity)
#     results.append((
#         "gross_salary_non_negative",
#         ge_df.expect_column_min_to_be_between("gross_salary_eur", min_value=0, max_value=None)
#     ))

#     # 6) Ensure parsing succeeded (recommended)
#     results.append(("birth_date_not_null", ge_df.expect_column_values_to_not_be_null("birth_date")))
#     results.append(("hire_date_not_null", ge_df.expect_column_values_to_not_be_null("hire_date")))
#     results.append(("gross_salary_not_null", ge_df.expect_column_values_to_not_be_null("gross_salary_eur")))

#     # -----------------------------
#     # Enrichment expectations
#     # -----------------------------

#     # 7) distance_km should be >= 0 when present
#     # GE "min >= 0" ensures there are no negative distances (min ignores nulls).
#     if "distance_km" in df.columns:
#         results.append((
#             "distance_km_non_negative",
#             ge_df.expect_column_min_to_be_between("distance_km", min_value=0, max_value=None)
#         ))
#     else:
#         # If enrichment did not run yet, failing here might not be what you want.
#         # If enrichment is mandatory in your flow, keep it as a FAIL.
#         results.append(("distance_km_column_exists", {"success": False, "result": {"unexpected_count": None}}))

#     # 8) commute_valid_for_bonus must be boolean if present
#     if "commute_valid_for_bonus" in df.columns:
#         results.append((
#             "commute_valid_for_bonus_boolean",
#             ge_df.expect_column_values_to_be_in_set("commute_valid_for_bonus", [True, False])
#         ))
#     else:
#         results.append(("commute_valid_for_bonus_column_exists", {"success": False, "result": {"unexpected_count": None}}))

#     # -----------------------------
#     # Print GE results
#     # -----------------------------
#     all_ok = True
#     print("[DQ employee_ref] GE expectation results:")
#     for name, res in results:
#         success = res.get("success", False)
#         if success:
#             print(f"  ✅ {name}")
#         else:
#             all_ok = False
#             print(f"  ❌ {name}")
#             unexpected = res.get("result", {}).get("unexpected_count")
#             if unexpected is not None:
#                 print(f"     unexpected_count = {unexpected}")

#     return all_ok


# # -------------------------------------------------------------------
# # Manual cross-column rules (Spark)
# # -------------------------------------------------------------------

# def run_cross_column_checks(df) -> bool:
#     """
#     Cross-column checks that are easier/clearer to express with Spark filters.

#     Sport rules:
#       A) has_sport_practice = True  -> sport_practice NOT NULL
#       B) has_sport_practice = False -> sport_practice NULL

#     Commute enrichment rules:
#       C) If commute_valid_for_bonus = True:
#             - commute_mode must be "walk_run"
#             - distance_km must NOT be null
#             - distance_km <= COMMUTE_MAX_KM
#       D) If commute_mode != "walk_run":
#             - commute_valid_for_bonus must be False
#     """
#     print("[DQ employee_ref] Running cross-column consistency checks...")

#     ok = True

#     # -----------------------------
#     # Sport consistency (A/B)
#     # -----------------------------

#     bad_true = df.filter(
#         (F.col("has_sport_practice") == True) &
#         F.col("sport_practice").isNull()
#     ).count()

#     bad_false = df.filter(
#         (F.col("has_sport_practice") == False) &
#         F.col("sport_practice").isNotNull()
#     ).count()

#     if bad_true == 0:
#         print("  ✅ Rule A: has_sport_practice = True -> sport_practice NOT NULL")
#     else:
#         print("  ❌ Rule A violation count:", bad_true)
#         ok = False

#     if bad_false == 0:
#         print("  ✅ Rule B: has_sport_practice = False -> sport_practice NULL")
#     else:
#         print("  ❌ Rule B violation count:", bad_false)
#         ok = False

#     # -----------------------------
#     # Commute enrichment consistency (C/D)
#     # -----------------------------

#     # If enrichment columns are missing, fail (because you requested these checks).
#     required_enrichment_cols = {"distance_km", "commute_valid_for_bonus", "commute_mode"}
#     missing = [c for c in required_enrichment_cols if c not in df.columns]
#     if missing:
#         print(f"  ❌ Enrichment columns missing in employee_ref: {missing}")
#         return False

#     # C) commute_valid_for_bonus True => must satisfy all conditions
#     bad_bonus_true = df.filter(
#         (F.col("commute_valid_for_bonus") == True) &
#         (
#             (F.col("commute_mode") != F.lit("walk_run")) |
#             F.col("distance_km").isNull() |
#             (F.col("distance_km") > F.lit(COMMUTE_MAX_KM))
#         )
#     ).count()

#     if bad_bonus_true == 0:
#         print(
#             "  ✅ Rule C: commute_valid_for_bonus=True -> walk_run + distance_km not null + distance_km <= threshold"
#         )
#     else:
#         print("  ❌ Rule C violation count:", bad_bonus_true)
#         ok = False

#     # D) if commute_mode != walk_run => commute_valid_for_bonus must be False
#     bad_non_walk_bonus = df.filter(
#         (F.col("commute_mode") != F.lit("walk_run")) &
#         (F.col("commute_valid_for_bonus") != F.lit(False))
#     ).count()

#     if bad_non_walk_bonus == 0:
#         print("  ✅ Rule D: commute_mode != walk_run -> commute_valid_for_bonus = False")
#     else:
#         print("  ❌ Rule D violation count:", bad_non_walk_bonus)
#         ok = False

#     return ok


# # -------------------------------------------------------------------
# # Main
# # -------------------------------------------------------------------

# def main():
#     """
#     Pipeline-friendly entrypoint:
#       - exit 0 on success
#       - exit 1 on failure (so Kestra / CI can stop the flow)
#     """
#     spark = build_spark_session()

#     df = load_employee_ref(spark)

#     ge_ok = run_ge_expectations(df)
#     cross_ok = run_cross_column_checks(df)

#     all_ok = ge_ok and cross_ok

#     if all_ok:
#         print("[DQ employee_ref] ✅ All data quality checks passed.")
#         exit_code = 0
#     else:
#         print("[DQ employee_ref] ❌ Some data quality checks FAILED.")
#         exit_code = 1

#     spark.stop()
#     sys.exit(exit_code)


# if __name__ == "__main__":
#     main()
