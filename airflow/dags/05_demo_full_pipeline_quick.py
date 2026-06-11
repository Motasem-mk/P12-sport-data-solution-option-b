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

# Reusable configuration paths
SPARK_SUBMIT = "docker exec spark-master /opt/bitnami/spark/bin/spark-submit"
SPARK_SUBMIT_DETACHED = "docker exec -d spark-master /opt/bitnami/spark/bin/spark-submit"

with DAG(
    dag_id="05_demo_full_pipeline_quick",
    description="Unified Flow: Generate Activities -> Manage Live CDC Stream -> Batch Bronze -> Silver -> Gold",
    default_args=default_args,
    start_date=pendulum.datetime(2026, 1, 1, tz=local_tz),
    schedule=None,  # Manually triggered or on a cron schedule
    catchup=False,
    max_active_runs=1,
    tags=["sport-data-solution", "demo", "quick-demo", "streaming", "batch"],
) as dag:

    # Step 1: Generate source activities (OLTP Data simulation)
    generate_activities = BashOperator(
        task_id="generate_daily_activities",
        execution_timeout=timedelta(minutes=10),
        bash_command=f"{SPARK_SUBMIT} /opt/workspace/scripts/generate_daily_activities.py",
    )

    # Step 2: Idempotent Live Stream Controller
    # Checks if running. If down, launches it in the background. If up, passes silently.
    manage_cdc_stream = BashOperator(
        task_id="ensure_cdc_streaming_is_alive",
        execution_timeout=timedelta(minutes=3),
        bash_command=f"""
            set -euo pipefail
            
            echo "Checking if CDC stream application is currently running on Spark Master..."
            if docker exec spark-master ps aux | grep -v grep | grep -q "cdc_to_bronze_stream.py"; then
                echo "SUCCESS: CDC Live Stream is already up and processing data. Skipping invocation."
            else
                echo "WARNING: CDC Live Stream is down or not found! Attempting restart..."
                
                # We launch using -d (detached mode) so Airflow doesn't block waiting for an infinite stream
                {SPARK_SUBMIT_DETACHED} /opt/workspace/scripts/cdc_to_bronze_stream.py
                
                echo "Waiting 10 seconds for the stream context to initialize..."
                sleep 10
                
                # Verify that it didn't instantly crash
                if docker exec spark-master ps aux | grep -v grep | grep -q "cdc_to_bronze_stream.py"; then
                    echo "SUCCESS: CDC Live Stream restarted successfully in the background!"
                else
                    echo "CRITICAL ERROR: Failed to recover the CDC stream application. Check spark logs."
                    exit 1
                fi
            fi
            
            echo "Allowing a brief window for new records to settle into Bronze Delta..."
            sleep 15
        """,
    )

    # Step 3: Batch Process from Bronze layer to Silver layer
    bronze_to_silver = BashOperator(
        task_id="process_bronze_to_silver",
        execution_timeout=timedelta(minutes=15),
        bash_command=f"{SPARK_SUBMIT} /opt/workspace/scripts/bronze_to_silver.py",
    )

    # Step 4: Batch Process from Silver layer to Gold star schema & Postgres OLAP DB
    silver_to_gold = BashOperator(
        task_id="process_silver_to_gold_olap",
        execution_timeout=timedelta(minutes=20),
        bash_command=f"{SPARK_SUBMIT} /opt/workspace/scripts/silver_to_gold_delta_and_olap.py",
    )

    # Visual Workflow Dependency Mapping
    generate_activities >> manage_cdc_stream >> bronze_to_silver >> silver_to_gold