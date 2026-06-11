# airflow/dags/01_bootstrap.py

from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator

from alerts.slack_alerts import send_slack_failure_alert


local_tz = pendulum.timezone("Europe/Paris")


default_args = {
    "owner": "motasem",
    "depends_on_past": False,

    # Airflow handles retry logic.
    # Total attempts = first try + 2 retries.
    "retries": 2,
    "retry_delay": timedelta(minutes=2),

    # Slack alert on task failure.
    "on_failure_callback": send_slack_failure_alert,
}


with DAG(
    dag_id="01_bootstrap",
    description=(
        "One-time bootstrap flow: build Silver employee reference, enrich commute data, "
        "create OLTP activities table, generate one-year activity backfill, "
        "create OLAP database, and initialize Gold schemas/tables/views."
    ),
    default_args=default_args,
    start_date=pendulum.datetime(2026, 1, 1, tz=local_tz),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(hours=2),
    tags=["sport-data-solution", "bootstrap", "one-time"],
) as dag:

    # ------------------------------------------------------------
    # 1. Build Silver employee reference from HR + sportive CSVs
    # ------------------------------------------------------------
    hr_sportive_to_silver_employee_ref = BashOperator(
        task_id="hr_sportive_to_silver_employee_ref",
        execution_timeout=timedelta(minutes=15),
        bash_command=r"""
set -euo pipefail

echo "Building Silver employee_ref Delta table..."

docker exec spark-master /opt/bitnami/spark/bin/spark-submit \
  /opt/workspace/scripts/hr_sportive_to_silver_employee_ref.py

echo "Silver employee_ref completed."
""",
    )

    # ------------------------------------------------------------
    # 2. Enrich employee_ref with Google Maps commute distances
    # ------------------------------------------------------------
    enrich_commute_gmaps = BashOperator(
        task_id="enrich_commute_gmaps",
        execution_timeout=timedelta(minutes=25),
        bash_command=r"""
set -euo pipefail

echo "Enriching commute distances with Google Maps..."

docker exec spark-master /opt/bitnami/spark/bin/spark-submit \
  /opt/workspace/scripts/enrich_commute_gmaps.py

echo "Commute enrichment completed."
""",
    )

    # ------------------------------------------------------------
    # 3. Create OLTP activities table
    # ------------------------------------------------------------
    create_oltp_activities_table = BashOperator(
        task_id="create_oltp_activities_table",
        execution_timeout=timedelta(minutes=5),
        bash_command=r"""
set -euo pipefail

echo "Creating OLTP activities table..."

docker exec postgres sh -lc '
  set -eu

  PGPASSWORD="$POSTGRES_PASSWORD" psql \
    -h localhost \
    -U "$POSTGRES_USER" \
    -d "$POSTGRES_DB" \
    -v ON_ERROR_STOP=1 \
    -f /opt/workspace/sql/01_create_oltp_activities_table.sql
'

echo "OLTP activities table created/verified."
""",
    )

    # ------------------------------------------------------------
    # 4. Generate one-year historical activity backfill
    # ------------------------------------------------------------
    generate_activities_backfill_1y = BashOperator(
        task_id="generate_activities_backfill_1y",
        execution_timeout=timedelta(minutes=35),
        bash_command=r"""
set -euo pipefail

echo "Generating one-year activities backfill..."

docker exec spark-master /opt/bitnami/spark/bin/spark-submit \
  /opt/workspace/scripts/generate_activities.py

echo "Activities backfill completed."
""",
    )

    # ------------------------------------------------------------
    # 5. Create OLAP role and database
    # ------------------------------------------------------------
    create_olap_role_and_db = BashOperator(
        task_id="create_olap_role_and_db",
        execution_timeout=timedelta(minutes=5),
        bash_command=r"""
set -euo pipefail

echo "Creating/verifying OLAP role and database..."

docker exec postgres sh /opt/workspace/scripts/create_olap_role_and_database.sh

echo "OLAP role/database completed."
""",
    )

    # ------------------------------------------------------------
    # 6. Create Gold schemas, staging tables, final tables and views
    # ------------------------------------------------------------
    create_gold_schema_tables_params_and_views = BashOperator(
        task_id="create_gold_schema_tables_params_and_views",
        execution_timeout=timedelta(minutes=10),
        bash_command=r"""
set -euo pipefail

echo "Creating Gold OLAP schemas, tables, params and views..."

docker exec postgres sh -lc '
  set -eu

  : "${SPORT_OLAP_DB:?SPORT_OLAP_DB is missing}"
  : "${SPORT_OLAP_USER:?SPORT_OLAP_USER is missing}"
  : "${SPORT_OLAP_PASSWORD:?SPORT_OLAP_PASSWORD is missing}"

  PGPASSWORD="$SPORT_OLAP_PASSWORD" psql \
    -h localhost \
    -U "$SPORT_OLAP_USER" \
    -d "$SPORT_OLAP_DB" \
    -v ON_ERROR_STOP=1 \
    -f /opt/workspace/sql/02_create_gold_schema_tables_params_and_views.sql
'

echo "Gold OLAP objects created/verified."
""",
    )

    # ------------------------------------------------------------
    # DAG dependency flow
    # ------------------------------------------------------------
    (
        hr_sportive_to_silver_employee_ref
        >> enrich_commute_gmaps
        >> create_oltp_activities_table
        >> generate_activities_backfill_1y
        >> create_olap_role_and_db
        >> create_gold_schema_tables_params_and_views
    )
