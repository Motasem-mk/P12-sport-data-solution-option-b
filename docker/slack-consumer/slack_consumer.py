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
