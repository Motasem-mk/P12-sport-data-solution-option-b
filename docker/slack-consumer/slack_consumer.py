# docker/slack-consumer/slack_consumer.py

import json
import os
import sys
import time
from typing import Any, Dict, Optional, Tuple

import requests
from kafka import KafkaConsumer


def log(message: str) -> None:
    """Print a clear log message and flush immediately."""
    print(f"[slack-consumer] {message}", flush=True)


def decode_json(raw_bytes: Optional[bytes]) -> Optional[Dict[str, Any]]:
    """
    Decode Kafka message bytes into a Python dictionary.
    Returns None for tombstone/null/invalid messages.
    """
    if raw_bytes is None:
        return None

    try:
        return json.loads(raw_bytes.decode("utf-8"))
    except Exception as exc:
        log(f"Could not decode Kafka message as JSON: {exc}")
        return None


def extract_activity_and_op(record: Dict[str, Any]) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
    """
    Extract the activity payload and Debezium operation code.

    This supports two common Debezium shapes:

    1) Unwrapped Debezium event:
       {
         "schema": {...},
         "payload": {
           "activity_id": 1,
           "employee_id": 123,
           "sport_type": "running",
           "__op": "c"
         }
       }

    2) Classic Debezium envelope:
       {
         "payload": {
           "op": "c",
           "after": {
             "activity_id": 1,
             "employee_id": 123,
             "sport_type": "running"
           }
         }
       }

    In our POC, the Redpanda messages use the unwrapped shape with "__op".
    """
    if not isinstance(record, dict):
        return None, None

    payload = record.get("payload")

    if not isinstance(payload, dict):
        return None, None

    # Case 1: classic Debezium envelope: payload.after + payload.op
    if isinstance(payload.get("after"), dict):
        activity = payload["after"]
        op = payload.get("op")
        return activity, op

    # Case 2: unwrapped Debezium event: payload contains the activity directly
    activity = payload
    op = payload.get("__op") or payload.get("op")

    return activity, op


def format_slack_message(activity: Dict[str, Any]) -> str:
    """
    Build a simple Slack message for a sport activity.
    """
    activity_id = activity.get("activity_id")
    employee_id = activity.get("employee_id")
    sport_type = activity.get("sport_type", "activity")
    distance_m = activity.get("distance_m")
    elapsed_time_s = activity.get("elapsed_time_s")
    comment = activity.get("comment")

    text = f"New activity from employee `{employee_id}` *{sport_type}*"

    if distance_m is not None:
        try:
            distance_km = float(distance_m) / 1000
            text += f" {distance_km:.1f} km"
        except Exception:
            text += f" {distance_m} m"

    if elapsed_time_s is not None:
        try:
            minutes = int(elapsed_time_s) // 60
            text += f" ({minutes} min)"
        except Exception:
            pass

    if comment:
        text += f"\n_{comment}_"

    if activity_id is not None:
        text += f"\n(activity_id = {activity_id})"

    return text


def main() -> None:
    brokers = os.getenv("KAFKA_BROKERS", "redpanda:9092")
    topic = os.getenv("KAFKA_TOPIC", "sportdb.public.activities")
    group_id = os.getenv("KAFKA_GROUP_ID", "slack-consumer-group")
    auto_offset_reset = os.getenv("KAFKA_AUTO_OFFSET_RESET", "latest")
    slack_webhook_url = os.getenv("SLACK_WEBHOOK_URL")

    if not slack_webhook_url:
        log("ERROR: SLACK_WEBHOOK_URL is missing.")
        sys.exit(1)

    log(
        f"Starting Slack consumer. "
        f"brokers={brokers}, topic={topic}, group_id={group_id}, "
        f"auto_offset_reset={auto_offset_reset}"
    )

    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=[b.strip() for b in brokers.split(",") if b.strip()],
        group_id=group_id,
        auto_offset_reset=auto_offset_reset,
        enable_auto_commit=True,
        value_deserializer=decode_json,
    )

    log("Connected to Redpanda. Waiting for activity events...")

    processed = 0
    sent = 0
    skipped = 0

    for message in consumer:
        processed += 1
        record = message.value

        if record is None:
            skipped += 1
            log("Skipping null/tombstone message.")
            continue

        activity, op = extract_activity_and_op(record)

        if activity is None:
            skipped += 1
            log("Skipping message: no activity payload found.")
            continue

        activity_id = activity.get("activity_id")
        log(f"Received activity_id={activity_id}, op={op}")

        # IMPORTANT:
        # r = Debezium snapshot/read event for historical rows
        # c = real create/insert event
        # For Slack, we only publish real live inserts.
        if op != "c":
            skipped += 1
            log(f"Skipping activity_id={activity_id} because op={op}. Only op='c' is sent to Slack.")
            continue

        text = format_slack_message(activity)

        log("Sending Slack message:")
        log(text)

        try:
            response = requests.post(
                slack_webhook_url,
                json={"text": text},
                timeout=10,
            )

            if response.status_code == 200:
                sent += 1
                log(f"Slack message sent successfully. total_sent={sent}")
            else:
                log(f"Slack error {response.status_code}: {response.text}")

        except Exception as exc:
            log(f"Error while sending Slack message: {exc}")

        # Small pause to avoid sending too quickly in demo mode.
        time.sleep(1)

        if processed % 100 == 0:
            log(f"Stats: processed={processed}, sent={sent}, skipped={skipped}")


if __name__ == "__main__":
    main()

###################################################################


# # docker/slack-consumer/slack_consumer.py

# import json
# import os
# import sys
# import time
# from typing import Any, Dict, Optional, Iterable

# from kafka import KafkaConsumer
# import requests


# # ---------------------- Small helpers ----------------------


# def debug_print(msg: str) -> None:
#     """Print logs with a prefix and flush immediately."""
#     print(f"[slack-consumer] {msg}", flush=True)


# def decode_value(raw_bytes: Optional[bytes]) -> Optional[Any]:
#     """
#     Kafka value_deserializer: bytes -> Python object (dict).

#     Returns:
#         - dict / list / whatever json.loads gives
#         - None if value is null or cannot be decoded
#     """
#     if raw_bytes is None:
#         return None

#     try:
#         text = raw_bytes.decode("utf-8")
#         return json.loads(text)
#     except Exception as e:
#         debug_print(f"ERROR decoding message value: {e}")
#         return None


# def get_int_env(name: str, default: Optional[int] = None) -> Optional[int]:
#     """Read an int from env or return default if missing/invalid."""
#     raw = os.getenv(name)
#     if raw is None or raw == "":
#         return default
#     try:
#         return int(raw)
#     except ValueError:
#         return default


# def get_float_env(name: str, default: Optional[float] = None) -> Optional[float]:
#     """Read a float from env or return default if missing/invalid."""
#     raw = os.getenv(name)
#     if raw is None or raw == "":
#         return default
#     try:
#         return float(raw)
#     except ValueError:
#         return default


# # ---------------------- Activity payload finder ----------------------


# def _is_activity_record(obj: Any) -> bool:
#     """
#     Heuristic: is this dict 'activity-like'?
#     We consider it an activity if it has:
#       - employee_id
#       - and (sport_type or sport)
#     """
#     if not isinstance(obj, dict):
#         return False

#     if "employee_id" in obj and ("sport_type" in obj or "sport" in obj):
#         return True

#     # fallback: some schema where we at least see activity_id + employee_id
#     if "activity_id" in obj and "employee_id" in obj:
#         return True

#     return False


# def _iter_nodes(obj: Any) -> Iterable[Any]:
#     """
#     Breadth-first iteration over all nested dicts and lists inside obj.
#     """
#     queue = [obj]
#     while queue:
#         node = queue.pop(0)
#         yield node

#         if isinstance(node, dict):
#             for v in node.values():
#                 if isinstance(v, (dict, list)):
#                     queue.append(v)
#         elif isinstance(node, list):
#             for v in node:
#                 if isinstance(v, (dict, list)):
#                     queue.append(v)


# def extract_activity_payload(record: Any) -> Optional[Dict[str, Any]]:
#     """
#     Try to find the 'real' activity payload inside any Debezium shape.
#     """
#     if record is None:
#         return None

#     if isinstance(record, dict):
#         # top-level 'after'
#         if "after" in record and isinstance(record["after"], dict) and _is_activity_record(record["after"]):
#             return record["after"]

#         # top-level payload.after
#         payload = record.get("payload")
#         if isinstance(payload, dict):
#             if "after" in payload and isinstance(payload["after"], dict) and _is_activity_record(payload["after"]):
#                 return payload["after"]
#             if _is_activity_record(payload):
#                 return payload

#         # already flat
#         if _is_activity_record(record):
#             return record

#     # Generic BFS over all nested dicts/lists
#     for node in _iter_nodes(record):
#         if _is_activity_record(node):
#             return node

#     return None


# def get_debezium_op(record: Any) -> Optional[str]:
#     """
#     Try to extract Debezium 'op' code from record:
#       - 'c' = create
#       - 'u' = update
#       - 'd' = delete
#       - 'r' = snapshot
#     """
#     if not isinstance(record, dict):
#         return None

#     # direct
#     op = record.get("op")
#     if isinstance(op, str):
#         return op

#     payload = record.get("payload")
#     if isinstance(payload, dict):
#         op = payload.get("op")
#         if isinstance(op, str):
#             return op

#     # fallback BFS search
#     for node in _iter_nodes(record):
#         if isinstance(node, dict):
#             op = node.get("op")
#             if isinstance(op, str):
#                 return op

#     return None


# # ---------------------- Slack formatting ----------------------


# def format_slack_message(activity: Dict[str, Any]) -> str:
#     """
#     Build a human-friendly Slack message from the activity dict.
#     """
#     activity_id = activity.get("activity_id")
#     employee_id = activity.get("employee_id")
#     sport_type = activity.get("sport_type") or activity.get("sport")
#     distance_m = activity.get("distance_m")
#     elapsed_time_s = activity.get("elapsed_time_s")
#     comment = activity.get("comment")

#     parts = []

#     if sport_type:
#         parts.append(f"*{sport_type}*")

#     # Duration in min/s
#     minutes_str = None
#     if elapsed_time_s is not None:
#         try:
#             seconds_total = int(elapsed_time_s)
#             minutes = seconds_total // 60
#             seconds = seconds_total % 60
#             if minutes > 0:
#                 minutes_str = f"{minutes} min"
#                 if seconds:
#                     minutes_str += f" {seconds} s"
#             else:
#                 minutes_str = f"{seconds} s"
#         except Exception:
#             minutes_str = f"{elapsed_time_s} s"

#     if distance_m is not None:
#         try:
#             km = float(distance_m) / 1000.0
#             parts.append(f"{km:.1f} km")
#         except Exception:
#             parts.append(f"{distance_m} m")

#     # First line
#     main_line = f"New activity from employee `{employee_id}`"
#     if parts:
#         main_line += " " + " ".join(parts)

#     # Optional comment
#     if comment:
#         main_line += f"\n_{comment}_"

#     # activity_id
#     if activity_id is not None:
#         main_line += f"\n(activity_id = {activity_id})"

#     return main_line


# # ---------------------- Main loop ----------------------


# def main():
#     brokers = os.getenv("KAFKA_BROKERS", "redpanda:9092")
#     topic = os.getenv("KAFKA_TOPIC", "sportdb.public.activities")
#     slack_webhook = os.getenv("SLACK_WEBHOOK_URL")
#     group_id = os.getenv("KAFKA_GROUP_ID", "slack-consumer-group")
#     auto_offset_reset = os.getenv("KAFKA_AUTO_OFFSET_RESET", "latest")

#     # Optional: limit how many Slack messages to send (for demos)
#     max_slack_messages = get_int_env("SLACK_MAX_MESSAGES", default=None)

#     # ✅ NEW: simple throttle between Slack posts (demo-friendly)
#     min_seconds_between_posts = get_float_env("SLACK_MIN_SECONDS_BETWEEN_POSTS", default=1.5)

#     # ✅ NEW: if Slack returns 429 WITHOUT Retry-After, wait this many seconds
#     cooldown_fallback_seconds = get_int_env("SLACK_COOLDOWN_FALLBACK_SECONDS", default=60)

#     debug_print(
#         f"Starting. BROKERS={brokers}, TOPIC={topic}, GROUP_ID={group_id}, "
#         f"AUTO_OFFSET_RESET={auto_offset_reset}, MAX_SLACK_MESSAGES={max_slack_messages}, "
#         f"MIN_GAP={min_seconds_between_posts}s, COOLDOWN_FALLBACK={cooldown_fallback_seconds}s"
#     )

#     if not slack_webhook:
#         debug_print("ERROR: SLACK_WEBHOOK_URL is not set!")
#         sys.exit(1)

#     consumer = KafkaConsumer(
#         topic,
#         bootstrap_servers=[b.strip() for b in brokers.split(",") if b.strip()],
#         group_id=group_id,
#         auto_offset_reset=auto_offset_reset,
#         enable_auto_commit=True,
#         value_deserializer=decode_value,
#     )

#     debug_print("Connected to Kafka. Waiting for messages…")

#     processed = 0
#     sent_to_slack = 0
#     skipped_non_activity = 0
#     skipped_op = 0

#     debug_preview_non_activity = 3

#     # ✅ NEW: tracking last post time + cooldown window
#     last_post_ts = 0.0
#     cooldown_until_ts = 0.0

#     for msg in consumer:
#         processed += 1
#         record = msg.value

#         if record is None:
#             debug_print("Skipping null/tombstone value")
#             skipped_non_activity += 1
#             continue

#         activity = extract_activity_payload(record)

#         if activity is None:
#             skipped_non_activity += 1
#             if debug_preview_non_activity > 0:
#                 debug_print("Skipping non-activity message. RAW preview:")
#                 try:
#                     debug_print(json.dumps(record, indent=2)[:1000])
#                 except Exception:
#                     debug_print(str(record)[:1000])
#                 debug_preview_non_activity -= 1
#             continue

#         op = get_debezium_op(record)
#         if op and op not in ("c", "u"):
#             skipped_op += 1
#             debug_print(f"Skipping activity with op='{op}' (snapshot/delete)")
#             continue

#         text = format_slack_message(activity)

#         # ✅ NEW: if we are in cooldown (previous 429), do NOT send
#         now = time.time()
#         if now < cooldown_until_ts:
#             debug_print(f"Cooldown active -> skipping send until {int(cooldown_until_ts)}")
#             continue

#         # ✅ NEW: throttle (sleep) to avoid rate limits
#         wait = (last_post_ts + float(min_seconds_between_posts)) - now
#         if wait > 0:
#             time.sleep(wait)

#         debug_print("Slack payload text:")
#         debug_print(text)

#         try:
#             resp = requests.post(slack_webhook, json={"text": text}, timeout=10)

#             if resp.status_code == 200:
#                 sent_to_slack += 1
#                 last_post_ts = time.time()
#                 debug_print(f"Slack message sent ✅ (total sent: {sent_to_slack})")

#             elif resp.status_code == 429:
#                 # ✅ NEW: rate limit handling
#                 retry_after = resp.headers.get("Retry-After")
#                 if retry_after:
#                     try:
#                         retry_seconds = int(retry_after)
#                     except Exception:
#                         retry_seconds = cooldown_fallback_seconds
#                 else:
#                     # Slack often returns "message_limit_exceeded" without Retry-After
#                     retry_seconds = cooldown_fallback_seconds

#                 cooldown_until_ts = time.time() + retry_seconds
#                 debug_print(
#                     f"Slack error 429: {resp.text} | cooling down for {retry_seconds}s "
#                     f"(until {int(cooldown_until_ts)})"
#                 )

#             else:
#                 debug_print(f"Slack error {resp.status_code}: {resp.text}")

#         except Exception as e:
#             debug_print(f"Exception when calling Slack: {e}")

#         if max_slack_messages is not None and sent_to_slack >= max_slack_messages:
#             debug_print("Reached SLACK_MAX_MESSAGES, exiting consumer loop.")
#             break

#         if processed % 100 == 0:
#             debug_print(
#                 f"Stats so far: processed={processed}, "
#                 f"sent_to_slack={sent_to_slack}, "
#                 f"skipped_non_activity={skipped_non_activity}, "
#                 f"skipped_op={skipped_op}"
#             )

#     debug_print(
#         f"Consumer stopped. Final stats: processed={processed}, "
#         f"sent_to_slack={sent_to_slack}, "
#         f"skipped_non_activity={skipped_non_activity}, "
#         f"skipped_op={skipped_op}"
#     )


# if __name__ == "__main__":
#     main()
