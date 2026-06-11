from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator


default_args = {
    "owner": "motasem",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}


with DAG(
    dag_id="00_system_check",
    description="Basic infrastructure health check for Sport Data Solution",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["system", "healthcheck", "sport-data-solution"],
) as dag:

    check_docker_access = BashOperator(
        task_id="check_docker_access",
        bash_command="docker ps",
    )

    check_postgres = BashOperator(
        task_id="check_postgres",
        bash_command="docker exec postgres sh -c 'pg_isready -U \"$POSTGRES_USER\" -d \"$POSTGRES_DB\" -h localhost'",
    )

    check_redpanda = BashOperator(
        task_id="check_redpanda",
        bash_command="docker exec redpanda rpk cluster health --api-urls localhost:9644",
    )

    check_debezium_connect = BashOperator(
        task_id="check_debezium_connect",
        bash_command="curl -fsS http://connect:8083/connectors",
    )

    check_spark_master = BashOperator(
        task_id="check_spark_master",
        bash_command="docker exec spark-master bash -lc '/opt/bitnami/spark/bin/spark-submit --version'",
    )

    check_metabase = BashOperator(
        task_id="check_metabase",
        bash_command="curl -fsS http://metabase:3000/api/health || true",
    )

    (
        check_docker_access
        >> check_postgres
        >> check_redpanda
        >> check_debezium_connect
        >> check_spark_master
        >> check_metabase
    )