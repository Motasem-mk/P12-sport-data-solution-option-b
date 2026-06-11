# scripts/generate_activities.py
"""
Generate realistic Strava-style activities for sportive employees and insert them into
the Postgres OLTP table public.activities.

This script is intended to be used as a ONE-TIME backfill (e.g., 365 days).
After the backfill, you should use generate_daily_activities.py for incremental daily inserts.

Key behaviors (OC + real-world):
- Reads sportive employees from Delta silver.employee_ref (only employee_id + sport_practice)
- Generates realistic distances/durations based on sport type
- Bulk inserts into Postgres using psycopg2.execute_values
- (A): ONE-TIME guard (idempotent by design) — if backfill window already has rows, abort
- (C): sport_type is ALWAYS canonical English values (no French labels)
- (D): privacy-clean Spark read (no first_name/last_name)
- (E): wider, more realistic ranges for distance & duration
"""

import os
import random
import datetime as dt
from typing import List, Tuple, Optional, Dict

import psycopg2
from psycopg2.extras import execute_values

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


# -------------------------------------------------------------------
# 0) Parameters & configuration
# -------------------------------------------------------------------

EMPLOYEE_REF_PATH = os.getenv(
    "EMPLOYEE_REF_PATH",
    "/opt/workspace/data/delta/silver/employee_ref",
)

PG_HOST = os.getenv("POSTGRES_HOST", "postgres")
PG_PORT = int(os.getenv("POSTGRES_PORT", "5432"))

#  Prefer OLTP vars (new setup), fallback to old POSTGRES_* vars
PG_DB = os.getenv("SPORT_OLTP_DB") or os.getenv("POSTGRES_DB", "sportdb")
PG_USER = os.getenv("SPORT_OLTP_USER") or os.getenv("POSTGRES_USER", "sport")
PG_PASSWORD = os.getenv("SPORT_OLTP_PASSWORD") or os.getenv("POSTGRES_PASSWORD", "sport")

# Backfill window (rolling)
BACKFILL_DAYS = int(os.getenv("BACKFILL_DAYS", "365"))
BACKFILL_END_DATE_STR = os.getenv("BACKFILL_END_DATE")  # optional YYYY-MM-DD

#  CHANGE (A): allow explicit override if you REALLY want to rerun backfill
ALLOW_BACKFILL_RERUN = os.getenv("ALLOW_BACKFILL_RERUN", "0") == "1"

# For deterministic demos (optional)
RANDOM_SEED = int(os.getenv("RANDOM_SEED", "42"))


def _parse_end_date_utc() -> dt.date:
    """
    Parse BACKFILL_END_DATE (YYYY-MM-DD) or default to today's UTC date.

     WHY: keeps the backfill window stable across machines/timezones.
    """
    if BACKFILL_END_DATE_STR:
        return dt.datetime.strptime(BACKFILL_END_DATE_STR, "%Y-%m-%d").date()
    return dt.datetime.utcnow().date()


_END_DATE = _parse_end_date_utc()

# We'll use naive UTC datetimes (acceptable for Postgres timestamp columns).
# If your column is timestamptz, psycopg2 will still handle naive datetimes as local;
# in that case, prefer timezone-aware datetimes.
YEAR_END = dt.datetime.combine(_END_DATE, dt.time(23, 59, 59))
YEAR_START = YEAR_END - dt.timedelta(days=BACKFILL_DAYS)

MIN_ACTIVITIES_PER_EMP = int(os.getenv("MIN_ACTIVITIES_PER_EMP", "10"))
MAX_ACTIVITIES_PER_EMP = int(os.getenv("MAX_ACTIVITIES_PER_EMP", "40"))

# Canonical sports from your employee_ref normalization
DISTANCE_SPORTS = {"running", "hiking", "triathlon"}
SESSION_SPORTS = {
    "tennis",
    "swimming",
    "football",
    "rugby",
    "badminton",
    "sailing",
    "judo",
    "boxing",
    "climbing",
    "horse_riding",
    "table_tennis",
    "basketball",
}

RUNNING_COMMENTS = [
    "Back to training 💪",
    "Nice run today ✅",
    "New personal best 🚀",
    "Goal achieved for today 🔥",
]
HIKING_COMMENTS = [
    "Great hike today 👏",
    "Beautiful scenery 🙌",
    "Another hike in the books 🎉",
]
SESSION_COMMENTS = [
    "Great session today 🏆",
    "Workout done — proud of myself 😎",
    "Solid effort 🤝",
]
GENERIC_COMMENTS = [
    "Workout completed 🚀",
    "Great activity today 💪",
    "Daily goal achieved ✅",
]


# -------------------------------------------------------------------
# 1) Small helpers
# -------------------------------------------------------------------

def maybe_pick_comment_for_sport(sport_type: str) -> Optional[str]:
    """
    Randomly return a short comment or None.

    WHY: realistic behavior (not everyone posts comments).
    """
    sport = (sport_type or "").lower()

    if sport == "running":
        base_pool = RUNNING_COMMENTS
    elif sport == "hiking":
        base_pool = HIKING_COMMENTS
    elif sport in SESSION_SPORTS:
        base_pool = SESSION_COMMENTS
    else:
        base_pool = GENERIC_COMMENTS

    pool: List[Optional[str]] = [None, None] + base_pool  # 2/ (len+2) chance of None
    return random.choice(pool)


def clamp_dt(x: dt.datetime) -> dt.datetime:
    """
    Ensure a datetime stays inside [YEAR_START, YEAR_END].
    """
    if x < YEAR_START:
        return YEAR_START
    if x > YEAR_END:
        return YEAR_END
    return x


def random_datetime_within_window() -> dt.datetime:
    """
    Pick a random datetime between YEAR_START and YEAR_END,
    then adjust to a realistic time slot in the day.
    """
    total_seconds = int((YEAR_END - YEAR_START).total_seconds())
    offset = random.randint(0, total_seconds)
    base = YEAR_START + dt.timedelta(seconds=offset)

    slot = random.choice(["morning", "noon", "evening"])
    if slot == "morning":
        hour = random.randint(6, 9)
    elif slot == "noon":
        hour = random.randint(12, 14)
    else:
        hour = random.randint(17, 21)

    minute = random.randint(0, 59)
    second = random.randint(0, 59)

    candidate = base.replace(hour=hour, minute=minute, second=second, microsecond=0)
    return clamp_dt(candidate)


def canonical_sport_type(sport: str) -> str:
    """
    CHANGE (C): enforce canonical English sport_type.

    WHY:
    - avoids messy values like "randonnée", "course à pied"
    - keeps OLTP consistent with Silver/Gold expectations
    """
    s = (sport or "").strip().lower()

    # You can extend this mapping if needed
    mapping: Dict[str, str] = {
        "runing": "running",     # if any typo sneaks in
        "running": "running",
        "hiking": "hiking",
        "triathlon": "triathlon",
        "tennis": "tennis",
        "swimming": "swimming",
        "football": "football",
        "rugby": "rugby",
        "badminton": "badminton",
        "sailing": "sailing",
        "judo": "judo",
        "boxing": "boxing",
        "climbing": "climbing",
        "horse_riding": "horse_riding",
        "table_tennis": "table_tennis",
        "basketball": "basketball",
    }
    return mapping.get(s, s)  # fallback: keep value as-is (but should already be canonical)


# -------------------------------------------------------------------
# 2) Spark helpers
# -------------------------------------------------------------------

def build_spark_session() -> SparkSession:
    """
    Create a Spark session for reading Delta employee_ref.

    WHY: we only use Spark here to read the Delta table.
    """
    spark = (
        SparkSession.builder
        .appName("Generate_Activities_To_Postgres")
        .getOrCreate()
    )
    print("[INFO] Spark version:", spark.version, flush=True)
    return spark


def load_sportive_employees(spark: SparkSession):
    """
    CHANGE (D): privacy-clean read:
    Only read employee_id and sport_practice (no names).

    Returns a list of dicts: {"employee_id": ..., "sport_practice": ...}
    """
    print(f"[INFO] Reading employee_ref from {EMPLOYEE_REF_PATH}", flush=True)
    df = spark.read.format("delta").load(EMPLOYEE_REF_PATH)

    df_sportive = (
        df.filter(F.col("has_sport_practice") == True)
          .select("employee_id", "sport_practice")
    )

    rows = df_sportive.collect()
    print(f"[INFO] Sportive employees found: {len(rows)}", flush=True)

    return [
        {
            "employee_id": r["employee_id"],
            "sport_practice": r["sport_practice"],
        }
        for r in rows
    ]


# -------------------------------------------------------------------
# 3) Activity generation core logic
# -------------------------------------------------------------------

def generate_distance_and_time(sport: str) -> Tuple[int, int]:
    """
    CHANGE (E): wider + more realistic ranges.

    Returns:
        (distance_m, elapsed_time_s)

    For distance sports:
    - running: ~2–12 km
    - hiking: ~3–25 km
    - triathlon: ~5–40 km (simplified)
    """
    sport = canonical_sport_type(sport)

    if sport == "running":
        distance_km = random.uniform(2.0, 12.0)
        pace_min_per_km = random.uniform(4.5, 7.5)  # 4:30–7:30 /km
    elif sport == "hiking":
        distance_km = random.uniform(3.0, 25.0)
        pace_min_per_km = random.uniform(10.0, 18.0)
    elif sport == "triathlon":
        distance_km = random.uniform(5.0, 40.0)
        pace_min_per_km = random.uniform(3.0, 6.5)  # simplified
    else:
        # fallback distance sport (should rarely happen)
        distance_km = random.uniform(2.0, 10.0)
        pace_min_per_km = random.uniform(5.0, 8.0)

    duration_min = distance_km * pace_min_per_km

    # Keep reasonable bounds
    duration_min = max(20.0, min(duration_min, 240.0))  # 20 min to 4 hours

    distance_m = int(round(distance_km * 1000))
    elapsed_time_s = int(round(duration_min * 60))
    return distance_m, elapsed_time_s


def generate_session_time(sport: str) -> int:
    """
    CHANGE (E): more realistic session duration.

    Returns duration in seconds for session sports (no distance).
    """
    sport = canonical_sport_type(sport)

    if sport in {"tennis", "football", "rugby", "basketball"}:
        duration_min = random.uniform(45.0, 130.0)
    elif sport in {"boxing", "judo", "climbing"}:
        duration_min = random.uniform(30.0, 120.0)
    elif sport in {"swimming"}:
        duration_min = random.uniform(25.0, 90.0)
    else:
        duration_min = random.uniform(30.0, 120.0)

    return int(round(duration_min * 60))


def generate_activities_for_employee(
    employee_id: int,
    sport_type: str,
) -> List[Tuple[int, dt.datetime, str, Optional[int], int, Optional[str]]]:
    """
    Generate activities for a single employee.

    Returns a list of tuples matching Postgres insert columns:
        (employee_id, start_time, sport_type, distance_m, elapsed_time_s, comment)
    """
    activities: List[Tuple[int, dt.datetime, str, Optional[int], int, Optional[str]]] = []

    sport_type = canonical_sport_type(sport_type)
    n_sessions = random.randint(MIN_ACTIVITIES_PER_EMP, MAX_ACTIVITIES_PER_EMP)

    for _ in range(n_sessions):
        start_time = random_datetime_within_window()

        if sport_type in DISTANCE_SPORTS:
            distance_m, elapsed_time_s = generate_distance_and_time(sport_type)
        else:
            distance_m = None
            elapsed_time_s = generate_session_time(sport_type)

        comment = maybe_pick_comment_for_sport(sport_type)
        activities.append((employee_id, start_time, sport_type, distance_m, elapsed_time_s, comment))

    return activities


# -------------------------------------------------------------------
# 4) Postgres helpers
# -------------------------------------------------------------------

def get_pg_connection():
    """
    Create a Postgres connection (transactional).

    WHY: for bulk inserts + safe commit/rollback behavior.
    """
    print(f"[INFO] Connecting to Postgres at {PG_HOST}:{PG_PORT}/{PG_DB} as {PG_USER}", flush=True)
    conn = psycopg2.connect(
        host=PG_HOST,
        port=PG_PORT,
        dbname=PG_DB,
        user=PG_USER,
        password=PG_PASSWORD,
    )
    conn.autocommit = False
    return conn


def backfill_already_done(conn) -> bool:
    """
    CHANGE (A): one-time backfill guard.

    Checks whether activities already exist in the backfill window.
    If yes -> abort (unless ALLOW_BACKFILL_RERUN=1).

    WHY:
    - prevents accidental duplicate backfills
    - makes the pipeline safe for reruns in OC demos
    """
    sql = """
        SELECT COUNT(1)
        FROM public.activities
        WHERE start_time >= %s AND start_time <= %s
    """
    with conn.cursor() as cur:
        cur.execute(sql, (YEAR_START, YEAR_END))
        count = int(cur.fetchone()[0])
    return count > 0


def insert_activities_into_postgres(
    conn,
    rows: List[Tuple[int, dt.datetime, str, Optional[int], int, Optional[str]]],
):
    """
    Bulk insert activities into public.activities.
    """
    if not rows:
        print("[WARN] No activities to insert.", flush=True)
        return

    sql = """
        INSERT INTO public.activities
            (employee_id, start_time, sport_type, distance_m, elapsed_time_s, comment)
        VALUES %s
    """

    with conn.cursor() as cur:
        execute_values(cur, sql, rows)

    conn.commit()
    print(f"[INFO] Inserted {len(rows)} activities into public.activities", flush=True)


# -------------------------------------------------------------------
# 5) Main
# -------------------------------------------------------------------

def main():
    # Deterministic demo generation (optional)
    random.seed(RANDOM_SEED)

    print(f"[INFO] Backfill window (UTC naive): {YEAR_START}  -->  {YEAR_END}", flush=True)
    print(f"[INFO] MIN/MAX activities per sportive employee: {MIN_ACTIVITIES_PER_EMP}/{MAX_ACTIVITIES_PER_EMP}", flush=True)

    conn = get_pg_connection()
    try:
        # CHANGE (A): prevent duplicates if already backfilled
        if backfill_already_done(conn) and not ALLOW_BACKFILL_RERUN:
            print(
                "[INFO] Backfill appears already done (rows exist in backfill window). "
                "Aborting to avoid duplicates. "
                "Set ALLOW_BACKFILL_RERUN=1 only if you intentionally want to rerun.",
                flush=True,
            )
            return

        spark = build_spark_session()
        employees = load_sportive_employees(spark)

        if not employees:
            print("[ERROR] No sportive employees found, aborting.", flush=True)
            spark.stop()
            return

        all_activities: List[Tuple[int, dt.datetime, str, Optional[int], int, Optional[str]]] = []

        # Generate for each sportive employee
        for emp in employees:
            emp_id = emp["employee_id"]
            sport = emp["sport_practice"]

            if not sport:
                print(
                    f"[WARN] Employee {emp_id} has has_sport_practice=true but sport_practice is null. Skipping.",
                    flush=True,
                )
                continue

            all_activities.extend(generate_activities_for_employee(emp_id, sport))

        print(f"[INFO] Total activities generated: {len(all_activities)}", flush=True)

        insert_activities_into_postgres(conn, all_activities)

        spark.stop()
        print("[INFO] ✅ Backfill activity generation finished.", flush=True)

    finally:
        conn.close()


if __name__ == "__main__":
    main()

