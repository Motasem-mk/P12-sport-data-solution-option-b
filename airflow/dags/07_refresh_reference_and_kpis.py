from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.models.param import Param
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


# Reusable Spark command
SPARK_SUBMIT = "docker exec spark-master /opt/bitnami/spark/bin/spark-submit"


with DAG(
    dag_id="07_refresh_sources_and_kpis",
    description=(
        "Manual refresh flow used when HR/sportive reference sources or historical "
        "activity data are modified. It refreshes employee_ref, reruns Google Maps "
        "enrichment, runs data quality checks, optionally rebuilds Silver activities "
        "from Bronze, and recalculates Gold/OLAP KPIs for the dashboard."
    ),
    default_args=default_args,
    start_date=pendulum.datetime(2026, 1, 1, tz=local_tz),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(minutes=90),
    tags=[
        "sport-data-solution",
        "source-refresh",
        "employee-ref",
        "gmaps",
        "dq",
        "gold",
        "olap",
    ],
    params={
        "full_rebuild_activities": Param(
            False,
            type="boolean",
            description=(
                "Set to true only when historical activity data was corrected "
                "and Silver activities must be fully rebuilt from Bronze."
            ),
        ),
    },
) as dag:

    check_spark_submit_exists = BashOperator(
        task_id="check_spark_submit_exists",
        execution_timeout=timedelta(minutes=2),
        bash_command="""
            set -euo pipefail

            echo "Checking spark-submit inside spark-master..."
            docker exec spark-master test -x /opt/bitnami/spark/bin/spark-submit
            echo "spark-submit is available."
        """,
    )

    refresh_silver_employee_ref = BashOperator(
        task_id="refresh_silver_employee_ref",
        execution_timeout=timedelta(minutes=20),
        bash_command=f"""
            set -euo pipefail

            echo "Refreshing Silver employee_ref from HR and sportive source files..."
            echo "This script is idempotent and uses Delta MERGE by employee_id."

            {SPARK_SUBMIT} /opt/workspace/scripts/hr_sportive_to_silver_employee_ref.py
        """,
    )

    enrich_commute_gmaps = BashOperator(
        task_id="enrich_commute_gmaps",
        execution_timeout=timedelta(minutes=30),
        bash_command=f"""
            set -euo pipefail

            echo "Running Google Maps commute enrichment..."
            echo "Only employees needing enrichment are recalculated."

            {SPARK_SUBMIT} /opt/workspace/scripts/enrich_commute_gmaps.py
        """,
    )

    dq_employee_ref = BashOperator(
        task_id="dq_employee_ref",
        execution_timeout=timedelta(minutes=20),
        bash_command=f"""
            set -euo pipefail

            echo "Running data quality checks on Silver employee_ref..."

            {SPARK_SUBMIT} /opt/workspace/scripts/check_employee_ref_dq.py
        """,
    )

    optional_full_rebuild_activities = BashOperator(
        task_id="optional_full_rebuild_activities",
        execution_timeout=timedelta(minutes=25),
        bash_command=f"""
            set -euo pipefail

            FULL_REBUILD_ACTIVITIES="{{{{ params.full_rebuild_activities }}}}"

            echo "full_rebuild_activities parameter value: $FULL_REBUILD_ACTIVITIES"

            if [ "$FULL_REBUILD_ACTIVITIES" = "True" ] || [ "$FULL_REBUILD_ACTIVITIES" = "true" ]; then
                echo "Historical activity data was modified."
                echo "Rebuilding Silver activities from all Bronze CDC history..."

                docker exec \
                    -e FULL_REBUILD=1 \
                    spark-master \
                    /opt/bitnami/spark/bin/spark-submit \
                    /opt/workspace/scripts/bronze_to_silver.py

            else
                echo "Skipping full activity rebuild."
                echo "This is normal when only HR/sportive reference data changed."
            fi
        """,
    )

    dq_activities_if_rebuilt = BashOperator(
        task_id="dq_activities_if_rebuilt",
        execution_timeout=timedelta(minutes=20),
        bash_command=f"""
            set -euo pipefail

            FULL_REBUILD_ACTIVITIES="{{{{ params.full_rebuild_activities }}}}"

            echo "full_rebuild_activities parameter value: $FULL_REBUILD_ACTIVITIES"

            if [ "$FULL_REBUILD_ACTIVITIES" = "True" ] || [ "$FULL_REBUILD_ACTIVITIES" = "true" ]; then
                echo "Running data quality checks on Silver activities after full rebuild..."

                {SPARK_SUBMIT} /opt/workspace/scripts/check_activities_dq.py

            else
                echo "Skipping activity DQ because activities were not rebuilt in this DAG run."
            fi
        """,
    )

    recalculate_gold_olap = BashOperator(
        task_id="recalculate_gold_olap",
        execution_timeout=timedelta(minutes=25),
        bash_command=f"""
            set -euo pipefail

            echo "Recalculating Gold Delta and PostgreSQL OLAP KPIs..."
            echo "The dashboard reads from the refreshed Gold/OLAP outputs."

            {SPARK_SUBMIT} /opt/workspace/scripts/silver_to_gold_delta_and_olap.py
        """,
    )

    (
        check_spark_submit_exists
        >> refresh_silver_employee_ref
        >> enrich_commute_gmaps
        >> dq_employee_ref
        >> optional_full_rebuild_activities
        >> dq_activities_if_rebuilt
        >> recalculate_gold_olap
    )