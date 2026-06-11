from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

from alerts.slack_alerts import send_slack_failure_alert


default_args = {
    "owner": "motasem",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
    "on_failure_callback": send_slack_failure_alert,
}


CONNECTOR_NAME = "pg-activities"
CONNECTOR_JSON_PATH = "/connectors/pg-activities.json"
SPARK_STREAM_SCRIPT = "/opt/workspace/scripts/cdc_to_bronze_stream.py"


with DAG(
    dag_id="02_cdc_streaming",
    description=(
        "Manual CDC ingestion flow: register/update Debezium connector "
        "and start Spark Structured Streaming from Redpanda to Bronze Delta"
    ),
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(minutes=20),
    tags=["sport-data-solution", "cdc", "streaming", "bronze", "flow2"],
) as dag:

    docker_sock_smoke_test = BashOperator(
        task_id="docker_sock_smoke_test",
        execution_timeout=timedelta(minutes=2),
        bash_command=r"""
set -euo pipefail

echo "Checking Docker access from Airflow..."
docker ps | sed -n '1,20p'
echo "Docker access OK."
""",
    )

    validate_required_files = BashOperator(
        task_id="validate_required_files",
        execution_timeout=timedelta(minutes=3),
        bash_command=f"""
set -euo pipefail

echo "Checking Debezium connector JSON in connect container..."
docker exec connect test -f "{CONNECTOR_JSON_PATH}"

echo "Validating connector JSON..."
docker exec connect sh -lc "cat '{CONNECTOR_JSON_PATH}'" | python -m json.tool >/dev/null

echo "Checking Spark CDC-to-Bronze stream script..."
docker exec spark-master test -f "{SPARK_STREAM_SCRIPT}"

echo "Required files are available and connector JSON is valid."
""",
    )

    wait_connect_ready = BashOperator(
        task_id="wait_connect_ready",
        execution_timeout=timedelta(minutes=5),
        bash_command=r"""
set -euo pipefail

echo "Waiting for Debezium Kafka Connect API..."

for i in $(seq 1 40); do
  if curl -fsS http://connect:8083/connectors >/dev/null 2>&1; then
    echo "Kafka Connect API is ready."
    exit 0
  fi

  echo "Kafka Connect not ready yet... ($i/40)"
  sleep 2
done

echo "ERROR: Kafka Connect API is not reachable."
exit 1
""",
    )

    register_or_update_connector = BashOperator(
        task_id="register_or_update_connector",
        execution_timeout=timedelta(minutes=5),
        bash_command=f"""
set -euo pipefail

NAME="{CONNECTOR_NAME}"
JSON_PATH="{CONNECTOR_JSON_PATH}"

echo "Reading connector JSON from connect container: $JSON_PATH"
CONNECTOR_JSON=$(docker exec connect sh -lc "cat '$JSON_PATH'")

echo "Checking whether connector already exists: $NAME"
HTTP_CODE=$(curl -s -o /dev/null -w "%{{http_code}}" "http://connect:8083/connectors/$NAME" || true)

if [ "$HTTP_CODE" = "200" ]; then
  echo "Connector exists. Updating connector config."

  UPDATE_CODE=$(
    printf '%s' "$CONNECTOR_JSON" | python -c '
import sys, json
payload = json.load(sys.stdin)
print(json.dumps(payload["config"]))
' | curl -sS -o /tmp/connect_update_response.txt -w "%{{http_code}}" \
      -X PUT "http://connect:8083/connectors/$NAME/config" \
      -H "Content-Type: application/json" \
      -d @-
  )

  echo "Update HTTP code: $UPDATE_CODE"
  cat /tmp/connect_update_response.txt || true
  echo

  if [ "$UPDATE_CODE" -lt 200 ] || [ "$UPDATE_CODE" -ge 300 ]; then
    echo "ERROR: Failed to update connector."
    exit 1
  fi

else
  echo "Connector does not exist. Creating connector."

  CREATE_CODE=$(
    printf '%s' "$CONNECTOR_JSON" | curl -sS -o /tmp/connect_create_response.txt -w "%{{http_code}}" \
      -X POST "http://connect:8083/connectors" \
      -H "Content-Type: application/json" \
      -d @-
  )

  echo "Create HTTP code: $CREATE_CODE"
  cat /tmp/connect_create_response.txt || true
  echo

  if [ "$CREATE_CODE" -lt 200 ] || [ "$CREATE_CODE" -ge 300 ]; then
    echo "ERROR: Failed to create connector."
    exit 1
  fi
fi

echo "Current connectors:"
curl -sS "http://connect:8083/connectors"
echo
""",
    )

    wait_connector_running = BashOperator(
        task_id="wait_connector_running",
        execution_timeout=timedelta(minutes=5),
        bash_command=f"""
set -euo pipefail

NAME="{CONNECTOR_NAME}"

echo "Waiting for connector to reach RUNNING state: $NAME"

for i in $(seq 1 40); do
  STATUS="$(curl -sS "http://connect:8083/connectors/$NAME/status" || true)"

  echo "$STATUS"

  if printf '%s' "$STATUS" | grep -q '"state":"RUNNING"'; then
    echo "Connector is RUNNING."
    exit 0
  fi

  echo "Connector not running yet... ($i/40)"
  sleep 2
done

echo "ERROR: connector did not reach RUNNING state."
curl -sS "http://connect:8083/connectors/$NAME/status" || true
exit 1
""",
    )

    start_cdc_to_bronze_stream_detached = BashOperator(
        task_id="start_cdc_to_bronze_stream_detached",
        execution_timeout=timedelta(minutes=5),
        bash_command=f"""
set -euo pipefail

docker exec spark-master bash -lc '
  set -euo pipefail

  mkdir -p /opt/workspace/logs

  if ps aux | grep -v grep | grep -q "cdc_to_bronze_stream.py"; then
    echo "CDC-to-Bronze Spark stream is already running."
    exit 0
  fi

  echo "Starting CDC-to-Bronze Spark stream in detached mode..."

  nohup /opt/bitnami/spark/bin/spark-submit "{SPARK_STREAM_SCRIPT}" \
    > /opt/workspace/logs/cdc_to_bronze_stream.log 2>&1 &

  echo $! > /opt/workspace/logs/cdc_to_bronze_stream.pid

  echo "Started CDC-to-Bronze stream."
  echo "PID=$(cat /opt/workspace/logs/cdc_to_bronze_stream.pid)"
'
""",
    )

    verify_cdc_stream_running = BashOperator(
        task_id="verify_cdc_stream_running",
        execution_timeout=timedelta(minutes=3),
        bash_command=r"""
set -euo pipefail

echo "Verifying CDC-to-Bronze Spark stream process..."

sleep 5

docker exec spark-master bash -lc '
  set -euo pipefail

  if ps aux | grep -v grep | grep -q "cdc_to_bronze_stream.py"; then
    echo "CDC-to-Bronze stream process is running."
    echo "Recent log output:"
    tail -n 40 /opt/workspace/logs/cdc_to_bronze_stream.log || true
    exit 0
  fi

  echo "ERROR: CDC-to-Bronze stream process is not running."
  echo "Recent log output:"
  tail -n 80 /opt/workspace/logs/cdc_to_bronze_stream.log || true
  exit 1
'
""",
    )

    (
        docker_sock_smoke_test
        >> validate_required_files
        >> wait_connect_ready
        >> register_or_update_connector
        >> wait_connector_running
        >> start_cdc_to_bronze_stream_detached
        >> verify_cdc_stream_running
    )
