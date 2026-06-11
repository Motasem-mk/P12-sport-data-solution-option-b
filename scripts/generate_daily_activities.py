# scripts/generate_daily_activities.py
import os
import random
import datetime as dt
from typing import Tuple, Any
from decimal import Decimal

import psycopg2


def debug(msg: str) -> None:
    print(f"[generate_daily_activities] {msg}", flush=True)


def to_float(value: Any, default: float = 0.0) -> float:
    if value is None:
        return default
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, Decimal):
        return float(value)
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


DISTANCE_SPORTS = {
    "running",
    "course à pied",
    "hiking",
    "randonnée",
    "cycling",
    "vélo",
    "bike",
}

RACKET_SPORTS = {"tennis", "badminton", "squash", "ping pong", "table tennis"}
RELAX_SPORTS = {"yoga", "pilates"}


def compute_metrics(
    sport_type: str,
    avg_distance_m: Any,
    avg_elapsed_time_s: Any,
) -> Tuple[int, int]:
    sport = (sport_type or "").lower()

    avg_dist = to_float(avg_distance_m, default=3000.0)
    avg_time = to_float(avg_elapsed_time_s, default=1800.0)

    if sport in DISTANCE_SPORTS:
        dist_factor = random.uniform(0.7, 1.3)
        time_factor = random.uniform(0.8, 1.2)
        distance_m = max(500, int(avg_dist * dist_factor))
        elapsed_s = max(300, int(avg_time * time_factor))

    elif sport in RACKET_SPORTS:
        distance_m = 500
        base = 45 * 60
        elapsed_s = int(base * random.uniform(0.7, 1.3))

    elif sport in RELAX_SPORTS:
        distance_m = 0
        base = 40 * 60
        elapsed_s = int(base * random.uniform(0.8, 1.2))

    else:
        dist_factor = random.uniform(0.7, 1.3)
        time_factor = random.uniform(0.8, 1.2)
        distance_m = max(500, int(avg_dist * dist_factor))
        elapsed_s = max(300, int(avg_time * time_factor))

    return distance_m, elapsed_s


#  Comments are in English 
RUNNING_COMMENTS = [
    "Back to training 💪",
    "Nice run today ✅",
    "New personal best 🚀!",
    "Goal achieved for today 🔥!",
]

HIKING_COMMENTS = [
    "Great hike today 👏!",
    "Beautiful scenery 🙌!",
    "Hike completed — highly recommended 🎉!",
]

RACKET_COMMENTS = [
    "Intense match — great game 🏆!",
    "Session done — you can be proud 😎!",
    "Great performance today 🤝!",
]

RELAX_COMMENTS = [
    "Relaxing session done — mind feels light ✨!",
    "A calm moment well deserved 🫡",
    "Yoga done — feeling peaceful 🧘‍♂️",
]

GENERIC_COMMENTS = [
    "Session completed — you can be proud 🚀!",
    "Great activity today — keep going 💪!",
    "Daily activity goal achieved ✅",
]


def pick_comment(sport_type: str) -> str:
    sport = (sport_type or "").lower()

    if "randonnée" in sport or "hiking" in sport:
        pool = HIKING_COMMENTS
    elif "course" in sport or "run" in sport:
        pool = RUNNING_COMMENTS
    elif any(r in sport for r in RACKET_SPORTS):
        pool = RACKET_COMMENTS
    elif any(r in sport for r in RELAX_SPORTS):
        pool = RELAX_COMMENTS
    else:
        pool = GENERIC_COMMENTS

    return random.choice(pool)


def main() -> None:
    pg_host = os.getenv("POSTGRES_HOST", "postgres")
    pg_port = int(os.getenv("POSTGRES_PORT", "5432"))

    # Prefer OLTP vars (new setup), fallback to old POSTGRES_* vars
    pg_db = os.getenv("SPORT_OLTP_DB") or os.getenv("POSTGRES_DB", "sportdb")
    pg_user = os.getenv("SPORT_OLTP_USER") or os.getenv("POSTGRES_USER", "sport")
    pg_password = os.getenv("SPORT_OLTP_PASSWORD") or os.getenv("POSTGRES_PASSWORD", "sport")

    debug(f"Connecting to Postgres at {pg_host}:{pg_port}/{pg_db} as {pg_user}")

    conn = psycopg2.connect(
        host=pg_host,
        port=pg_port,
        dbname=pg_db,
        user=pg_user,
        password=pg_password,
    )
    conn.autocommit = False

    try:
        with conn.cursor() as cur:
            debug("Loading base stats from public.activities ...")
            cur.execute(
                """
                SELECT
                    employee_id,
                    sport_type,
                    AVG(distance_m) AS avg_distance_m,
                    AVG(elapsed_time_s) AS avg_elapsed_time_s
                FROM public.activities
                GROUP BY employee_id, sport_type
                """
            )
            rows = cur.fetchall()
            debug(f"Found {len(rows)} sportive employees (employee_id, sport_type pairs).")

            if not rows:
                debug("No base activities found, nothing to generate.")
                return

            now = dt.datetime.now().replace(second=0, microsecond=0)
            inserted = 0

            insert_sql = """
                INSERT INTO public.activities (
                    employee_id,
                    start_time,
                    sport_type,
                    distance_m,
                    elapsed_time_s,
                    comment
                )
                VALUES (%s, %s, %s, %s, %s, %s)
            """

            for employee_id, sport_type, avg_dist, avg_time in rows:
                distance_m, elapsed_s = compute_metrics(sport_type, avg_dist, avg_time)

                minutes_ago = random.randint(0, 180)
                start_time = now - dt.timedelta(minutes=minutes_ago)

                comment = pick_comment(sport_type)

                cur.execute(
                    insert_sql,
                    (employee_id, start_time, sport_type, distance_m, elapsed_s, comment),
                )
                inserted += 1

        conn.commit()
        debug(f"Inserted {inserted} daily activities into public.activities")
    finally:
        conn.close()
        debug("Done.")


if __name__ == "__main__":
    main()

