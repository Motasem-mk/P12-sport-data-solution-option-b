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


with DAG(
    dag_id="03_bronze_silver_gold",
    description=(
        "Scheduled micro-batch flow: check for new Bronze CDC data, "
        "then process Bronze to Silver, run data quality checks, "
        "and publish Silver to Gold Delta + PostgreSQL OLAP"
    ),
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule="*/10 * * * *",
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(minutes=45),
    tags=["sport-data-solution", "bronze", "silver", "gold", "dq", "flow3"],
) as dag:

    docker_sock_check = BashOperator(
        task_id="docker_sock_check",
        execution_timeout=timedelta(minutes=2),
        bash_command=r"""
set -euo pipefail

echo "Checking Docker access from Airflow..."
docker ps | sed -n '1,20p'
echo "Docker access OK."
""",
    )

    check_spark_submit_exists = BashOperator(
        task_id="check_spark_submit_exists",
        execution_timeout=timedelta(minutes=2),
        bash_command=r"""
set -euo pipefail

docker exec spark-master bash -lc '
  set -euo pipefail
  test -x /opt/bitnami/spark/bin/spark-submit
  echo "spark-submit OK"
'
""",
    )

    skip_if_no_new_bronze_data = BashOperator(
        task_id="skip_if_no_new_bronze_data",
        execution_timeout=timedelta(minutes=5),
        skip_on_exit_code=99,
        bash_command=r"""
set -euo pipefail

echo "Checking whether new Bronze CDC records exist..."

docker exec spark-master bash -lc "cat > /tmp/check_new_bronze_data.py <<'PY'
import os
import sys

from pyspark.sql import SparkSession, functions as F
from delta.tables import DeltaTable


BRONZE_PATH = os.getenv(
    'BRONZE_ACTIVITIES_PATH',
    '/opt/workspace/data/delta/bronze/activities_cdc',
)

OFFSETS_WATERMARK_PATH = os.getenv(
    'BRONZE_ACTIVITIES_OFFSETS_PATH',
    '/opt/workspace/data/delta/bronze/_meta/activities_offsets',
)


def log(msg: str) -> None:
    print(f'[check_new_bronze_data] {msg}', flush=True)


def path_exists(spark: SparkSession, path: str) -> bool:
    jvm = spark._jvm
    hconf = spark._jsc.hadoopConfiguration()
    p = jvm.org.apache.hadoop.fs.Path(path)
    fs = p.getFileSystem(hconf)
    return fs.exists(p)


spark = SparkSession.builder.appName('Check_New_Bronze_Data').getOrCreate()

try:
    log(f'Bronze path: {BRONZE_PATH}')
    log(f'Watermark path: {OFFSETS_WATERMARK_PATH}')

    if not path_exists(spark, BRONZE_PATH):
        log('Bronze path does not exist yet. Nothing to process.')
        sys.exit(99)

    if not DeltaTable.isDeltaTable(spark, BRONZE_PATH):
        log('Bronze path exists but is not a Delta table. This is an error.')
        sys.exit(1)

    df_bronze = spark.read.format('delta').load(BRONZE_PATH).select(
        'kafka_partition',
        'kafka_offset',
    )

    if len(df_bronze.take(1)) == 0:
        log('Bronze Delta table exists but has no rows. Nothing to process.')
        sys.exit(99)

    if (
        not path_exists(spark, OFFSETS_WATERMARK_PATH)
        or not DeltaTable.isDeltaTable(spark, OFFSETS_WATERMARK_PATH)
    ):
        log('No offsets watermark found. Bronze has rows, so data should be processed.')
        sys.exit(0)

    df_wm = spark.read.format('delta').load(OFFSETS_WATERMARK_PATH)

    df_new = (
        df_bronze.alias('b')
        .join(df_wm.alias('w'), on='kafka_partition', how='left')
        .withColumn('last_offset', F.coalesce(F.col('last_offset'), F.lit(-1)))
        .filter(F.col('kafka_offset') > F.col('last_offset'))
    )

    has_new_data = df_new.limit(1).count() > 0

    if has_new_data:
        log('New Bronze records found. Continue DAG processing.')
        sys.exit(0)

    log('No new Bronze records since last processed watermark. Skipping downstream tasks.')
    sys.exit(99)

finally:
    spark.stop()
    log('Spark stopped.')
PY

/opt/bitnami/spark/bin/spark-submit /tmp/check_new_bronze_data.py
"
""",
    )

    bronze_to_silver = BashOperator(
        task_id="bronze_to_silver",
        execution_timeout=timedelta(minutes=15),
        bash_command=r"""
set -euo pipefail

docker exec spark-master bash -lc '
  set -euo pipefail

  /opt/bitnami/spark/bin/spark-submit \
    /opt/workspace/scripts/bronze_to_silver.py
'
""",
    )

    dq_employee_ref = BashOperator(
        task_id="dq_employee_ref",
        execution_timeout=timedelta(minutes=10),
        bash_command=r"""
set -euo pipefail

docker exec spark-master bash -lc '
  set -euo pipefail

  /opt/bitnami/spark/bin/spark-submit \
    /opt/workspace/scripts/check_employee_ref_dq.py
'
""",
    )

    dq_activities = BashOperator(
        task_id="dq_activities",
        execution_timeout=timedelta(minutes=10),
        bash_command=r"""
set -euo pipefail

docker exec spark-master bash -lc '
  set -euo pipefail

  /opt/bitnami/spark/bin/spark-submit \
    /opt/workspace/scripts/check_activities_dq.py
'
""",
    )

    silver_to_gold_delta_and_olap = BashOperator(
        task_id="silver_to_gold_delta_and_olap",
        execution_timeout=timedelta(minutes=20),
        bash_command=r"""
set -euo pipefail

docker exec spark-master bash -lc '
  set -euo pipefail

  mkdir -p /opt/workspace/logs

  /opt/bitnami/spark/bin/spark-submit \
    /opt/workspace/scripts/silver_to_gold_delta_and_olap.py \
    2>&1 | tee /opt/workspace/logs/silver_to_gold_delta_and_olap.log
'
""",
    )

    (
        docker_sock_check
        >> check_spark_submit_exists
        >> skip_if_no_new_bronze_data
        >> bronze_to_silver
        >> dq_employee_ref
        >> dq_activities
        >> silver_to_gold_delta_and_olap
    )
