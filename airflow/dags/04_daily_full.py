from datetime import datetime, timedelta
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
    dag_id="04_daily_full",
    description="Daily Ingestion: Triggers daily transactional activity simulation data generation.",
    default_args=default_args,
    start_date=datetime(2026, 1, 1, tzinfo=local_tz),
    schedule="0 2 * * *",
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(minutes=20),
    tags=["sport-data-solution", "daily", "oltp"],
) as dag:

    # In production, this DAG has exactly one job. It triggers, it runs, it exits.
    generate_daily_activities = BashOperator(
        task_id="generate_daily_activities",
        execution_timeout=timedelta(minutes=15),
        bash_command="docker exec spark-master /opt/bitnami/spark/bin/spark-submit /opt/workspace/scripts/generate_daily_activities.py",
    )

