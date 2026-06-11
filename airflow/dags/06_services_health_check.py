from datetime import timedelta

import pendulum

from airflow import DAG
from airflow.operators.bash import BashOperator

from alerts.slack_alerts import send_slack_failure_alert


local_tz = pendulum.timezone("Europe/Paris")


default_args = {
    "owner": "motasem",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
    "on_failure_callback": send_slack_failure_alert,
}


with DAG(
    dag_id="06_services_health_check",
    description=(
        "Scheduled monitoring DAG: checks Docker, Postgres, Redpanda, Kafka Connect, "
        "Debezium connector, Spark stream, Delta paths and PostgreSQL OLAP readability. "
        "No data processing is performed."
    ),
    default_args=default_args,
    start_date=pendulum.datetime(2026, 1, 1, tz=local_tz),
    schedule="*/10 * * * *",
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(minutes=20),
    tags=["sport-data-solution", "health-check", "monitoring", "slack"],
) as dag:

    docker_sock_check = BashOperator(
        task_id="docker_sock_check",
        execution_timeout=timedelta(minutes=2),
        bash_command=r"""
set -euo pipefail

echo "Checking Docker access..."
docker ps | sed -n '1,20p'
echo "Docker access OK."
""",
    )

    check_postgres_oltp = BashOperator(
        task_id="check_postgres_oltp",
        execution_timeout=timedelta(minutes=3),
        bash_command=r"""
set -euo pipefail

echo "Checking OLTP PostgreSQL..."

docker exec postgres sh -lc '
  set -eu

  pg_isready -U "$POSTGRES_USER" -d "$POSTGRES_DB" -h localhost

  PGPASSWORD="$POSTGRES_PASSWORD" psql \
    -h localhost \
    -U "$POSTGRES_USER" \
    -d "$POSTGRES_DB" \
    -tAc "SELECT COUNT(*) FROM public.activities;" >/tmp/oltp_activity_count.txt

  echo "OLTP activity count: $(cat /tmp/oltp_activity_count.txt)"
'

echo "OLTP PostgreSQL OK."
""",
    )

    check_postgres_olap = BashOperator(
        task_id="check_postgres_olap",
        execution_timeout=timedelta(minutes=3),
        bash_command=r"""
set -euo pipefail

echo "Checking PostgreSQL OLAP database..."

OLAP_DB="$(docker exec postgres sh -lc 'printf "%s" "${SPORT_OLAP_DB:-sportdw}"')"
OLAP_USER="$(docker exec postgres sh -lc 'printf "%s" "${SPORT_OLAP_USER:-sport}"')"
OLAP_PWD="$(docker exec postgres sh -lc 'printf "%s" "${SPORT_OLAP_PASSWORD:-sport}"')"

docker exec -e PGPASSWORD="$OLAP_PWD" postgres \
  psql \
    -h localhost \
    -U "$OLAP_USER" \
    -d "$OLAP_DB" \
    -v ON_ERROR_STOP=1 \
    -tAc "
      SELECT COUNT(*)
      FROM information_schema.tables
      WHERE table_schema = 'gold';
    "

echo "PostgreSQL OLAP OK."
""",
    )

    check_redpanda = BashOperator(
        task_id="check_redpanda",
        execution_timeout=timedelta(minutes=3),
        bash_command=r"""
set -euo pipefail

echo "Checking Redpanda admin API..."

if curl -fsS http://redpanda:9644/v1/status/ready >/dev/null 2>&1; then
  echo "Redpanda admin API is ready."
  exit 0
fi

echo "Redpanda admin API did not respond. Trying container process check..."

docker exec redpanda sh -lc '
  ps aux | grep -v grep | grep -q redpanda
'

echo "Redpanda process is running."
""",
    )

    check_kafka_connect_and_debezium = BashOperator(
        task_id="check_kafka_connect_and_debezium",
        execution_timeout=timedelta(minutes=5),
        bash_command=r"""
set -euo pipefail

CONNECTOR_NAME="pg-activities"

echo "Checking Kafka Connect API..."
curl -fsS http://connect:8083/connectors >/dev/null

echo "Checking Debezium connector status..."
STATUS="$(curl -fsS "http://connect:8083/connectors/${CONNECTOR_NAME}/status" || true)"

echo "$STATUS"

if printf '%s' "$STATUS" | grep -q '"state":"RUNNING"'; then
  echo "Debezium connector is RUNNING."
else
  echo "ERROR: Debezium connector is not RUNNING."
  exit 1
fi
""",
    )

    check_spark_master_and_stream = BashOperator(
        task_id="check_spark_master_and_stream",
        execution_timeout=timedelta(minutes=5),
        bash_command=r"""
set -euo pipefail

echo "Checking Spark master UI..."
curl -fsS http://spark-master:8080 >/dev/null

echo "Checking CDC-to-Bronze Spark stream process..."

docker exec spark-master bash -lc '
  set -euo pipefail

  if ps aux | grep -v grep | grep -q "cdc_to_bronze_stream.py"; then
    echo "CDC-to-Bronze stream is running."
    echo "Recent CDC stream logs:"
    tail -n 30 /opt/workspace/logs/cdc_to_bronze_stream.log || true
    exit 0
  fi

  echo "ERROR: CDC-to-Bronze stream process is not running."
  echo "Recent CDC stream logs:"
  tail -n 80 /opt/workspace/logs/cdc_to_bronze_stream.log || true
  exit 1
'
""",
    )

    check_delta_paths = BashOperator(
        task_id="check_delta_paths",
        execution_timeout=timedelta(minutes=3),
        bash_command=r"""
set -euo pipefail

echo "Checking local Delta Lake paths..."

docker exec spark-master bash -lc '
  set -euo pipefail

  test -d /opt/workspace/data/delta/bronze || { echo "Missing bronze directory"; exit 1; }
  test -d /opt/workspace/data/delta/silver || { echo "Missing silver directory"; exit 1; }
  test -d /opt/workspace/data/delta/gold || { echo "Missing gold directory"; exit 1; }

  echo "Delta base directories exist."

  if [ -d /opt/workspace/data/delta/bronze/activities_cdc/_delta_log ]; then
    echo "Bronze activities_cdc Delta table exists."
  else
    echo "WARNING: Bronze activities_cdc Delta table not found yet."
  fi

  if [ -d /opt/workspace/data/delta/silver/activities/_delta_log ]; then
    echo "Silver activities Delta table exists."
  else
    echo "WARNING: Silver activities Delta table not found yet."
  fi

  if [ -d /opt/workspace/data/delta/gold/dim_employee/_delta_log ]; then
    echo "Gold dim_employee Delta table exists."
  else
    echo "WARNING: Gold dim_employee Delta table not found yet."
  fi

  if [ -d /opt/workspace/data/delta/gold/dim_date/_delta_log ]; then
    echo "Gold dim_date Delta table exists."
  else
    echo "WARNING: Gold dim_date Delta table not found yet."
  fi

  if [ -d /opt/workspace/data/delta/gold/fact_activity/_delta_log ]; then
    echo "Gold fact_activity Delta table exists."
  else
    echo "WARNING: Gold fact_activity Delta table not found yet."
  fi
'
""",
    )

    check_olap_reporting_tables_and_views = BashOperator(
        task_id="check_olap_reporting_tables_and_views",
        execution_timeout=timedelta(minutes=5),
        bash_command=r"""
set -euo pipefail

echo "Checking final PostgreSQL OLAP star-schema tables and KPI views..."

OLAP_DB="$(docker exec postgres sh -lc 'printf "%s" "${SPORT_OLAP_DB:-sportdw}"')"
OLAP_USER="$(docker exec postgres sh -lc 'printf "%s" "${SPORT_OLAP_USER:-sport}"')"
OLAP_PWD="$(docker exec postgres sh -lc 'printf "%s" "${SPORT_OLAP_PASSWORD:-sport}"')"

docker exec -e PGPASSWORD="$OLAP_PWD" postgres \
  psql \
    -h localhost \
    -U "$OLAP_USER" \
    -d "$OLAP_DB" \
    -v ON_ERROR_STOP=1 \
    -c "
      SELECT 'gold.dim_employee' AS object_name, COUNT(*) AS row_count FROM gold.dim_employee
      UNION ALL
      SELECT 'gold.dim_date' AS object_name, COUNT(*) AS row_count FROM gold.dim_date
      UNION ALL
      SELECT 'gold.fact_activity' AS object_name, COUNT(*) AS row_count FROM gold.fact_activity
      UNION ALL
      SELECT 'gold.v_kpi_summary' AS object_name, COUNT(*) AS row_count FROM gold.v_kpi_summary
      UNION ALL
      SELECT 'gold.v_financial_impact' AS object_name, COUNT(*) AS row_count FROM gold.v_financial_impact
      UNION ALL
      SELECT 'gold.v_wellbeing_days' AS object_name, COUNT(*) AS row_count FROM gold.v_wellbeing_days
      UNION ALL
      SELECT 'gold.v_kpi_monthly' AS object_name, COUNT(*) AS row_count FROM gold.v_kpi_monthly
      UNION ALL
      SELECT 'gold.v_sports_activity' AS object_name, COUNT(*) AS row_count FROM gold.v_sports_activity
      UNION ALL
      SELECT 'gold.v_commute_declaration_issues' AS object_name, COUNT(*) AS row_count FROM gold.v_commute_declaration_issues
      UNION ALL
      SELECT 'gold.v_data_quality_summary' AS object_name, COUNT(*) AS row_count FROM gold.v_data_quality_summary;
    "

echo "PostgreSQL OLAP star-schema tables and views are readable."
""",
    )

    (
        docker_sock_check
        >> check_postgres_oltp
        >> check_postgres_olap
        >> check_redpanda
        >> check_kafka_connect_and_debezium
        >> check_spark_master_and_stream
        >> check_delta_paths
        >> check_olap_reporting_tables_and_views
    )
