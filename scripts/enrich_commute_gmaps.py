# scripts/enrich_commute_gmaps.py

"""
Enrich the Silver employee_ref Delta table with commute distance information.

Input:
    /opt/workspace/data/delta/silver/employee_ref

Output columns updated:
    distance_km
    commute_valid_for_bonus
    commute_checked_at

Main behavior:
    - Select only employees that need enrichment.
    - Call Google Distance Matrix for sport commute modes.
    - Skip car/public_transport by default to reduce API cost.
    - Merge the result back into the Delta table.
"""

import os
import sys
import time
from typing import Dict, List, Optional, Tuple

import requests

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

try:
    from delta.tables import DeltaTable
except Exception:
    print(
        "ERROR: delta.tables import failed. "
        "Make sure Delta Lake is available in the Spark image.",
        file=sys.stderr,
    )
    raise


# ============================================================
# 1. Configuration
# ============================================================

EMPLOYEE_REF_PATH = os.getenv(
    "EMPLOYEE_REF_PATH",
    "/opt/workspace/data/delta/silver/employee_ref",
)

COMPANY_ADDRESS = os.getenv(
    "COMPANY_ADDRESS",
    "1362 Av. des Platanes, 34970 Lattes, France",
)

GOOGLE_MAPS_API_KEY = os.getenv("GOOGLE_MAPS_API_KEY")

if not GOOGLE_MAPS_API_KEY:
    raise RuntimeError("GOOGLE_MAPS_API_KEY is not set.")

# Bonus distance thresholds.
COMMUTE_MAX_KM_WALK_RUN = float(os.getenv("COMMUTE_MAX_KM_WALK_RUN", "15.0"))
COMMUTE_MAX_KM_OTHER = float(os.getenv("COMMUTE_MAX_KM_OTHER", "25.0"))

# If true, recompute all eligible rows even if already checked.
FORCE_RECOMPUTE = os.getenv("FORCE_RECOMPUTE", "0") == "1"

# If true, only enrich walk/run commute rows.
ONLY_WALK_RUN = os.getenv("ONLY_WALK_RUN", "0") == "1"

# By default, car/public_transport are marked ineligible without API calls.
COMPUTE_DISTANCE_FOR_ALL = os.getenv("COMPUTE_DISTANCE_FOR_ALL", "0") == "1"

# API batching / retry settings.
BATCH_SIZE = int(os.getenv("GMAPS_BATCH_SIZE", "20"))
SLEEP_SECONDS = float(os.getenv("GMAPS_SLEEP_SECONDS", "0.2"))
MAX_RETRIES = int(os.getenv("GMAPS_MAX_RETRIES", "5"))
BACKOFF_BASE_SECONDS = float(os.getenv("GMAPS_BACKOFF_BASE_SECONDS", "1.0"))

SPORT_COMMUTE_MODES = {"walk_run", "bike_scooter", "other"}


# ============================================================
# 2. Spark
# ============================================================

def build_spark_session() -> SparkSession:
    """
    Create Spark session for Delta read/write.
    """
    spark = (
        SparkSession.builder
        .appName("Enrich_Commute_Google_Maps")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.databricks.delta.schema.autoMerge.enabled", "true")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel(os.getenv("SPARK_LOG_LEVEL", "WARN"))

    print(f"[INFO] Spark version: {spark.version}", flush=True)
    return spark


# ============================================================
# 3. Business rules
# ============================================================

def google_mode_for_commute(commute_mode: Optional[str]) -> str:
    """
    Map project commute modes to Google Distance Matrix modes.
    """
    if commute_mode == "walk_run":
        return "walking"

    if commute_mode == "bike_scooter":
        return "bicycling"

    if commute_mode == "public_transport":
        return "transit"

    return "driving"


def threshold_for_commute_mode(commute_mode: Optional[str]) -> float:
    """
    Return the maximum valid distance for the commute mode.
    """
    if commute_mode == "walk_run":
        return COMMUTE_MAX_KM_WALK_RUN

    return COMMUTE_MAX_KM_OTHER


def is_valid_for_bonus(
    commute_mode: Optional[str],
    distance_km: Optional[float],
) -> bool:
    """
    Decide whether the employee is eligible for the commute bonus.
    """
    if distance_km is None:
        return False

    if commute_mode not in SPORT_COMMUTE_MODES:
        return False

    return distance_km <= threshold_for_commute_mode(commute_mode)


# ============================================================
# 4. Google Maps API helpers
# ============================================================

def safe_error_type(error: Exception) -> str:
    """
    Return only the exception type.

    We do not print raw request errors because they may contain
    addresses or API parameters.
    """
    return type(error).__name__


def call_distance_matrix_batch(
    origins: List[str],
    google_mode: str,
) -> Dict:
    """
    Call Google Distance Matrix for a batch of origins.

    One destination is used: the company address.
    """
    url = "https://maps.googleapis.com/maps/api/distancematrix/json"

    params = {
        "origins": "|".join(origins),
        "destinations": COMPANY_ADDRESS,
        "mode": google_mode,
        "key": GOOGLE_MAPS_API_KEY,
    }

    last_error = None

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            response = requests.get(url, params=params, timeout=15)

            # Retry transient HTTP errors.
            if response.status_code in (429, 500, 502, 503, 504):
                sleep_before_retry(attempt)
                continue

            data = response.json()
            status = data.get("status")

            # Retry temporary Google API statuses.
            if status in ("OVER_QUERY_LIMIT", "UNKNOWN_ERROR"):
                sleep_before_retry(attempt)
                continue

            if status not in (None, "OK"):
                raise RuntimeError(f"Google Distance Matrix status={status}")

            return data

        except Exception as error:
            last_error = error
            sleep_before_retry(attempt)

    raise RuntimeError(
        "Distance Matrix request failed after "
        f"{MAX_RETRIES} retries. LastErrorType={type(last_error).__name__}"
    )


def sleep_before_retry(attempt: int) -> None:
    """
    Exponential backoff between retries.
    """
    sleep_seconds = BACKOFF_BASE_SECONDS * (2 ** (attempt - 1))
    time.sleep(sleep_seconds)


def chunk_list(items: List[Tuple], size: int) -> List[List[Tuple]]:
    """
    Split a list into smaller batches.
    """
    return [
        items[i:i + size]
        for i in range(0, len(items), size)
    ]


# ============================================================
# 5. Candidate selection
# ============================================================

def validate_required_columns(df) -> None:
    """
    Ensure employee_ref has the columns needed by this script.
    """
    required_columns = {
        "employee_id",
        "home_address",
        "commute_mode",
        "distance_km",
        "commute_checked_at",
    }

    missing = [
        col for col in required_columns
        if col not in df.columns
    ]

    if missing:
        raise RuntimeError(
            f"Missing required columns in employee_ref: {missing}. "
            "Run hr_sportive_to_silver_employee_ref.py first."
        )


def select_candidates(df):
    """
    Select employees that need commute enrichment.

    Recompute when:
        - FORCE_RECOMPUTE=1
        - commute_checked_at is NULL
        - distance is missing for a sport commute mode

    Note:
        The HR/sportive UPSERT resets commute_checked_at to NULL
        when home_address or commute_mode changes.
    """
    sport_modes = ["walk_run"] if ONLY_WALK_RUN else list(SPORT_COMMUTE_MODES)

    return (
        df.filter(
            (F.col("home_address").isNotNull())
            & (F.length(F.trim(F.col("home_address"))) > 0)
            & (
                F.lit(FORCE_RECOMPUTE)
                | F.col("commute_checked_at").isNull()
                | (
                    F.col("commute_mode").isin(sport_modes)
                    & F.col("distance_km").isNull()
                )
            )
        )
        .select("employee_id", "home_address", "commute_mode")
    )


# ============================================================
# 6. Enrichment logic
# ============================================================

def split_ineligible_rows(candidate_rows: List) -> Tuple[List[Tuple], List]:
    """
    Skip API calls for car/public_transport by default.

    These modes are not eligible for the sport commute bonus, so distance
    is not required for the business rule.
    """
    updates = []

    if COMPUTE_DISTANCE_FOR_ALL:
        return updates, candidate_rows

    rows_for_api = []

    for row in candidate_rows:
        commute_mode = row["commute_mode"]

        if commute_mode in ("car", "public_transport"):
            updates.append((row["employee_id"], None, False))
        else:
            rows_for_api.append(row)

    return updates, rows_for_api


def group_rows_by_google_mode(candidate_rows: List) -> Dict[str, List[Tuple]]:
    """
    Group employees by Google travel mode.

    Each item is:
        employee_id, home_address, commute_mode
    """
    groups: Dict[str, List[Tuple]] = {}

    for row in candidate_rows:
        employee_id = row["employee_id"]
        address = row["home_address"]
        commute_mode = row["commute_mode"]

        google_mode = google_mode_for_commute(commute_mode)

        groups.setdefault(google_mode, []).append(
            (employee_id, address, commute_mode)
        )

    return groups


def parse_distance_response(
    data: Dict,
    batch: List[Tuple],
) -> Tuple[List[Tuple], int]:
    """
    Parse Google API response.

    Returns:
        updates list:
            employee_id, distance_km, commute_valid_for_bonus
        computed_count:
            number of non-null distances
    """
    rows = data.get("rows", [])

    if len(rows) != len(batch):
        raise RuntimeError("Unexpected number of rows returned by Google API.")

    updates = []
    computed_count = 0

    for index, api_row in enumerate(rows):
        employee_id, _address, commute_mode = batch[index]

        elements = (api_row or {}).get("elements", [])
        distance_km = None

        if elements:
            element = elements[0]

            if element.get("status") == "OK":
                meters = element["distance"]["value"]
                distance_km = float(meters) / 1000.0
                computed_count += 1

        updates.append(
            (
                employee_id,
                distance_km,
                is_valid_for_bonus(commute_mode, distance_km),
            )
        )

    return updates, computed_count


def enrich_candidates(candidate_rows: List) -> List[Tuple]:
    """
    Enrich candidate rows and return updates for Delta MERGE.
    """
    updates, rows_for_api = split_ineligible_rows(candidate_rows)

    groups = group_rows_by_google_mode(rows_for_api)

    computed_count = 0

    for google_mode, rows in groups.items():
        for batch in chunk_list(rows, BATCH_SIZE):
            origins = [item[1] for item in batch]
            employee_ids = [item[0] for item in batch]

            try:
                data = call_distance_matrix_batch(
                    origins=origins,
                    google_mode=google_mode,
                )

                batch_updates, batch_computed = parse_distance_response(
                    data=data,
                    batch=batch,
                )

                updates.extend(batch_updates)
                computed_count += batch_computed

            except Exception as error:
                print(
                    "[WARN] Google Maps batch failed "
                    f"mode={google_mode}, employee_ids={employee_ids}, "
                    f"error_type={safe_error_type(error)}",
                    file=sys.stderr,
                    flush=True,
                )

                # Keep the pipeline running.
                # Distance is unknown, so the employee is not bonus eligible.
                for employee_id, _address, _commute_mode in batch:
                    updates.append((employee_id, None, False))

            time.sleep(SLEEP_SECONDS)

    print(
        f"[INFO] Non-null distances computed: {computed_count}/{len(updates)}",
        flush=True,
    )

    return updates


# ============================================================
# 7. Delta MERGE
# ============================================================

def merge_updates(
    spark: SparkSession,
    updates: List[Tuple],
    employee_id_dtype: T.DataType,
) -> None:
    """
    Merge enrichment results back into employee_ref.

    Only these columns are updated:
        distance_km
        commute_valid_for_bonus
        commute_checked_at
    """
    if not updates:
        print("[INFO] No updates to merge.", flush=True)
        return

    schema = T.StructType([
        T.StructField("employee_id", employee_id_dtype, nullable=False),
        T.StructField("distance_km", T.DoubleType(), nullable=True),
        T.StructField("commute_valid_for_bonus", T.BooleanType(), nullable=False),
    ])

    df_updates = (
        spark.createDataFrame(updates, schema=schema)
        .withColumn("commute_checked_at", F.current_timestamp())
    )

    target = DeltaTable.forPath(spark, EMPLOYEE_REF_PATH)

    (
        target.alias("t")
        .merge(
            df_updates.alias("s"),
            "t.employee_id = s.employee_id",
        )
        .whenMatchedUpdate(
            set={
                "distance_km": "s.distance_km",
                "commute_valid_for_bonus": "s.commute_valid_for_bonus",
                "commute_checked_at": "s.commute_checked_at",
            }
        )
        .execute()
    )

    print("[INFO] Commute enrichment MERGE completed.", flush=True)


# ============================================================
# 8. Main
# ============================================================

def main() -> None:
    """
    Main execution flow.
    """
    spark = build_spark_session()

    try:
        print(f"[INFO] Reading employee_ref from {EMPLOYEE_REF_PATH}", flush=True)

        df = spark.read.format("delta").load(EMPLOYEE_REF_PATH)

        validate_required_columns(df)

        candidates = select_candidates(df)
        candidate_count = candidates.count()

        print(f"[INFO] Candidates to enrich: {candidate_count}", flush=True)

        if candidate_count == 0:
            print("[INFO] Nothing to enrich.", flush=True)
            return

        employee_id_dtype = df.schema["employee_id"].dataType

        # HR dataset is small, so collecting candidates is acceptable here.
        candidate_rows = list(candidates.toLocalIterator())

        updates = enrich_candidates(candidate_rows)

        merge_updates(
            spark=spark,
            updates=updates,
            employee_id_dtype=employee_id_dtype,
        )

        print("[INFO] Commute enrichment completed successfully.", flush=True)

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
    
# # scripts/enrich_commute_gmaps.py
# """
# Incrementally enrich silver.employee_ref with Google Distance Matrix:
# - distance_km
# - commute_valid_for_bonus
# - commute_checked_at

# Real-world/OC requirements implemented:
# - Incremental: recompute only if (missing checked_at) OR (updated_at > checked_at) OR (distance missing for sport modes) OR FORCE_RECOMPUTE=1
# - Cost-aware: skip API calls for car/public_transport by default (always ineligible)
# - Privacy-safe: never prints addresses
# - Robust: retries with exponential backoff, batching
# - Safe write-back: Delta MERGE updates only enrichment columns (no overwrite)


# - Prevent accidental leaking of addresses/API key via exception strings (sanitize error logging).
# - Make incremental logic cost-aware: rely on commute_checked_at NULL (which the UPSERT now sets on address/mode change)
#   instead of recomputing on any updated_at change (salary/BU changes should not trigger paid API calls).
# """

# import os
# import sys
# import time
# import requests
# from typing import Optional, Dict, List, Tuple

# from pyspark.sql import SparkSession
# from pyspark.sql import functions as F
# from pyspark.sql import types as T

# try:
#     from delta.tables import DeltaTable
# except Exception as e:
#     print("ERROR: delta.tables import failed. Ensure Delta Lake is available in your Spark image.", file=sys.stderr)
#     raise


# # --------- CONFIG (env overrides) ---------
# EMPLOYEE_REF_PATH = os.getenv("EMPLOYEE_REF_PATH", "/opt/workspace/data/delta/silver/employee_ref")

# COMPANY_ADDRESS = os.getenv("COMPANY_ADDRESS", "1362 Av. des Platanes, 34970 Lattes, France")

# COMMUTE_MAX_KM_WALK_RUN = float(os.getenv("COMMUTE_MAX_KM_WALK_RUN", "15.0"))
# COMMUTE_MAX_KM_OTHER = float(os.getenv("COMMUTE_MAX_KM_OTHER", "25.0"))

# # Backward-compat
# if os.getenv("COMMUTE_MAX_KM") is not None:
#     COMMUTE_MAX_KM_WALK_RUN = float(os.getenv("COMMUTE_MAX_KM", str(COMMUTE_MAX_KM_WALK_RUN)))

# FORCE_RECOMPUTE = os.getenv("FORCE_RECOMPUTE", "0") == "1"

# # Optional: only compute for walk_run employees
# ONLY_WALK_RUN = os.getenv("ONLY_WALK_RUN", "0") == "1"

# # If 1, compute distance even for car/public_transport (default 0 to save cost)
# COMPUTE_DISTANCE_FOR_ALL = os.getenv("COMPUTE_DISTANCE_FOR_ALL", "0") == "1"

# # Throttle + batching
# SLEEP_SECONDS = float(os.getenv("GMAPS_SLEEP_SECONDS", "0.2"))
# BATCH_SIZE = int(os.getenv("GMAPS_BATCH_SIZE", "20"))

# # Retries
# MAX_RETRIES = int(os.getenv("GMAPS_MAX_RETRIES", "5"))
# BACKOFF_BASE = float(os.getenv("GMAPS_BACKOFF_BASE_SECONDS", "1.0"))

# API_KEY = os.getenv("GOOGLE_MAPS_API_KEY")
# if not API_KEY:
#     print("ERROR: GOOGLE_MAPS_API_KEY is not set.", file=sys.stderr)
#     sys.exit(1)


# SPORT_MODES = {"walk_run", "bike_scooter", "other"}


# def build_spark_session() -> SparkSession:
#     """
#     Create a SparkSession configured for Delta Lake operations.
#     """
#     spark = (
#         SparkSession.builder
#         .appName("enrich_commute_gmaps")
#         .config("spark.sql.session.timeZone", "UTC")
#         .config("spark.databricks.delta.schema.autoMerge.enabled", "true")
#         .getOrCreate()
#     )
#     spark.sparkContext.setLogLevel(os.getenv("SPARK_LOG_LEVEL", "WARN"))
#     return spark


# def gmaps_mode_for_commute(commute_mode: Optional[str]) -> Optional[str]:
#     """
#     Map internal commute_mode (controlled vocab) to Google Distance Matrix 'mode'.
#     """
#     if not commute_mode:
#         return None
#     if commute_mode == "walk_run":
#         return "walking"
#     if commute_mode == "bike_scooter":
#         return "bicycling"
#     if commute_mode == "public_transport":
#         return "transit"
#     return "driving"


# def threshold_for_mode_km(commute_mode: Optional[str]) -> float:
#     """
#     Thresholds from note de cadrage defaults:
#     - walk_run => 15 km
#     - other sport modes => 25 km
#     """
#     if commute_mode == "walk_run":
#         return COMMUTE_MAX_KM_WALK_RUN
#     return COMMUTE_MAX_KM_OTHER


# def compute_bonus(commute_mode: Optional[str], distance_km: Optional[float]) -> bool:
#     """
#     Determine if employee is eligible for bonus based on sport commute mode + distance threshold.
#     """
#     if distance_km is None:
#         return False
#     if commute_mode in SPORT_MODES:
#         return distance_km <= threshold_for_mode_km(commute_mode)
#     return False


# def _safe_err(e: Exception) -> str:
#     """
#     (privacy/security):
#     Return a sanitized error string that does NOT risk leaking:
#     - addresses (origins/destination)
#     - API key
#     Some requests exceptions include URLs/params; we avoid printing that.
#     """
#     return f"{type(e).__name__}"


# def call_distance_matrix_batch(origins: List[str], mode: Optional[str]) -> Dict:
#     """
#     Call Distance Matrix for multiple origins (single destination).
#     Returns parsed JSON dict (may raise after retries).
#     """
#     url = "https://maps.googleapis.com/maps/api/distancematrix/json"
#     params = {
#         "origins": "|".join(origins),
#         "destinations": COMPANY_ADDRESS,
#         "key": API_KEY,
#     }
#     if mode:
#         params["mode"] = mode

#     last_err = None
#     for attempt in range(1, MAX_RETRIES + 1):
#         try:
#             resp = requests.get(url, params=params, timeout=15)

#             # Retry on rate limiting / transient server issues
#             if resp.status_code in (429, 500, 502, 503, 504):
#                 sleep_s = BACKOFF_BASE * (2 ** (attempt - 1))
#                 time.sleep(sleep_s)
#                 continue

#             data = resp.json()

#             #  defensive top-level status check
#             # WHY: If the request is not OK at top-level, treat as retryable/failed.
#             top_status = data.get("status")
#             if top_status in ("OVER_QUERY_LIMIT", "UNKNOWN_ERROR"):
#                 sleep_s = BACKOFF_BASE * (2 ** (attempt - 1))
#                 time.sleep(sleep_s)
#                 continue
#             if top_status not in (None, "OK"):
#                 # Non-retryable statuses like REQUEST_DENIED/INVALID_REQUEST:
#                 raise RuntimeError(f"GMAPS top-level status={top_status}")

#             return data

#         except Exception as e:
#             last_err = e
#             sleep_s = BACKOFF_BASE * (2 ** (attempt - 1))
#             time.sleep(sleep_s)

#     # do not stringify exception (could leak params)
#     raise RuntimeError(f"Distance Matrix failed after {MAX_RETRIES} retries. LastErrorType={type(last_err).__name__}")


# def chunk_list(xs: List[Tuple], n: int) -> List[List[Tuple]]:
#     """
#     Split a list into chunks of size n.
#     """
#     return [xs[i:i+n] for i in range(0, len(xs), n)]


# def main():
#     spark = build_spark_session()

#     print(f"[INFO] Reading employee_ref from {EMPLOYEE_REF_PATH}", flush=True)
#     df = spark.read.format("delta").load(EMPLOYEE_REF_PATH)

#     required_cols = {"employee_id", "home_address", "commute_mode", "updated_at", "distance_km", "commute_checked_at"}
#     missing = [c for c in required_cols if c not in df.columns]
#     if missing:
#         raise RuntimeError(
#             f"Missing required columns in employee_ref: {missing}. "
#             f"Run the updated hr_sportive_to_silver_employee_ref.py first."
#         )

#     # Candidate selection (incremental + cost-aware):
#     # - Always recompute if FORCE_RECOMPUTE
#     # - Or checked_at missing
#     # - Or distance missing for sport modes (walk_run/bike_scooter/other)
#     #
#     #  removed (updated_at > checked_at) as a default trigger
#     # WHY: updated_at can change for salary/BU/etc. which should NOT trigger paid API calls.
#     #      Correct invalidation is handled upstream: UPSERT sets commute_checked_at=NULL when address/mode changes.
#     #
#     # Additionally, if ONLY_WALK_RUN, limit sport modes to walk_run.
#     sport_modes_for_run = ["walk_run"] if ONLY_WALK_RUN else list(SPORT_MODES)

#     candidates = df.filter(
#         (F.col("home_address").isNotNull()) &
#         (F.length(F.trim(F.col("home_address"))) > 0) &
#         (
#             F.lit(FORCE_RECOMPUTE) |
#             F.col("commute_checked_at").isNull() |
#             (
#                 F.col("commute_mode").isin(sport_modes_for_run) &
#                 F.col("distance_km").isNull()
#             )
#         )
#     ).select("employee_id", "home_address", "commute_mode")

#     cand_count = candidates.count()
#     print(f"[INFO] Candidates to enrich: {cand_count}", flush=True)
#     if cand_count == 0:
#         spark.stop()
#         print("[INFO] Nothing to do.", flush=True)
#         return

#     # Keep employee_id dtype stable
#     emp_id_dtype = df.schema["employee_id"].dataType

#     # Collect candidates (small HR dataset). Use toLocalIterator to reduce driver memory risk.
#     cand_rows = list(candidates.toLocalIterator())

#     # Prepare updates list: (employee_id, distance_km, commute_valid_for_bonus)
#     updates: List[Tuple] = []

#     # 1) Handle always-ineligible modes without API calls (default).
#     #    This is cost-aware; set COMPUTE_DISTANCE_FOR_ALL=1 if you want distances for all modes.
#     if not COMPUTE_DISTANCE_FOR_ALL:
#         ineligible = [r for r in cand_rows if r["commute_mode"] in ("car", "public_transport")]
#         for r in ineligible:
#             updates.append((r["employee_id"], None, False))

#         # Keep only those that need API calls
#         cand_rows = [r for r in cand_rows if r["commute_mode"] not in ("car", "public_transport")]

#     # 2) Group remaining candidates by Google mode (walking/bicycling/driving/transit)
#     groups: Dict[str, List[Tuple[int, str, str]]] = {}
#     for r in cand_rows:
#         emp_id = r["employee_id"]
#         addr = r["home_address"]
#         cmode = r["commute_mode"]
#         gmode = gmaps_mode_for_commute(cmode) or "driving"
#         groups.setdefault(gmode, []).append((emp_id, addr, cmode))

#     # 3) Batch-call per mode
#     computed = 0
#     for gmode, items in groups.items():
#         batches = chunk_list(items, BATCH_SIZE)
#         for batch in batches:
#             # origins list must align with rows in response
#             origins = [x[1] for x in batch]

#             try:
#                 data = call_distance_matrix_batch(origins, mode=gmode)
#             except Exception as e:
#                 # Privacy-safe: do NOT print addresses, and do NOT print raw exception string
#                 #  sanitized logging
#                 emp_ids = [x[0] for x in batch]
#                 print(
#                     f"[WARN] GMAPS batch failed for mode={gmode} employee_ids={emp_ids}. err_type={_safe_err(e)}",
#                     file=sys.stderr,
#                     flush=True
#                 )
#                 # Mark them as not eligible (distance unknown) but still set bonus False
#                 for emp_id, _addr, cmode in batch:
#                     updates.append((emp_id, None, False))
#                 time.sleep(SLEEP_SECONDS)
#                 continue

#             rows = data.get("rows", [])
#             # Defensive: if API returns unexpected shape
#             if len(rows) != len(batch):
#                 emp_ids = [x[0] for x in batch]
#                 print(
#                     f"[WARN] GMAPS unexpected rows count for mode={gmode}. employee_ids={emp_ids}",
#                     file=sys.stderr,
#                     flush=True
#                 )
#                 for emp_id, _addr, cmode in batch:
#                     updates.append((emp_id, None, False))
#                 time.sleep(SLEEP_SECONDS)
#                 continue

#             for i, row in enumerate(rows):
#                 emp_id, _addr, cmode = batch[i]
#                 elements = (row or {}).get("elements", [])
#                 if not elements:
#                     distance_km = None
#                 else:
#                     el0 = elements[0]
#                     if el0.get("status") == "OK":
#                         meters = el0["distance"]["value"]
#                         distance_km = float(meters) / 1000.0
#                     else:
#                         distance_km = None

#                 commute_valid = compute_bonus(cmode, distance_km)
#                 updates.append((emp_id, distance_km, bool(commute_valid)))
#                 if distance_km is not None:
#                     computed += 1

#             time.sleep(SLEEP_SECONDS)

#     print(f"[INFO] Distance computed (non-null): {computed}/{len(updates)}", flush=True)

#     # Build updates DF (include commute_checked_at)
#     updates_schema = T.StructType([
#         T.StructField("employee_id", emp_id_dtype, nullable=False),
#         T.StructField("distance_km", T.DoubleType(), nullable=True),
#         T.StructField("commute_valid_for_bonus", T.BooleanType(), nullable=False),
#     ])

#     df_updates = spark.createDataFrame(updates, schema=updates_schema).withColumn(
#         "commute_checked_at", F.current_timestamp()
#     )

#     # Safe write-back: MERGE only enrichment columns
#     t = DeltaTable.forPath(spark, EMPLOYEE_REF_PATH)
#     (t.alias("t").merge(df_updates.alias("s"), "t.employee_id = s.employee_id")
#       .whenMatchedUpdate(set={
#           "distance_km": "s.distance_km",
#           "commute_valid_for_bonus": "s.commute_valid_for_bonus",
#           "commute_checked_at": "s.commute_checked_at",
#       })
#       .execute()
#     )

#     spark.stop()
#     print("[INFO] ✅ Commute enrichment MERGE completed.", flush=True)


# if __name__ == "__main__":
#     main()

