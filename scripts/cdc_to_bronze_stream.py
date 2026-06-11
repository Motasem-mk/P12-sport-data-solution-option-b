# scripts/cdc_to_bronze_stream.py
import os
from pyspark.sql import SparkSession, functions as F
from delta.tables import DeltaTable

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "redpanda:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "sportdb.public.activities")
STARTING_OFFSETS = os.getenv("CDC_STARTING_OFFSETS", "earliest")
FAIL_ON_DATA_LOSS = os.getenv("CDC_FAIL_ON_DATA_LOSS", "false").lower() == "true"

BRONZE_PATH = os.getenv("BRONZE_ACTIVITIES_PATH", "/opt/workspace/data/delta/bronze/activities_cdc")
CHECKPOINT_PATH = os.getenv("BRONZE_ACTIVITIES_CHECKPOINT", "/opt/workspace/data/delta/bronze/_checkpoints/activities_cdc")

AVAILABLE_NOW = os.getenv("CDC_AVAILABLE_NOW", "0") == "1"
TRIGGER_INTERVAL = os.getenv("CDC_TRIGGER_INTERVAL", "10 seconds")


def log(msg: str) -> None:
    print(f"[cdc_to_bronze] {msg}", flush=True)


def build_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("CDC_To_Bronze_Activities")
        .config("spark.sql.session.timeZone", "UTC")  # ✅ NEW: stable timestamps
        .getOrCreate()
    )


def _path_exists(spark: SparkSession, path: str) -> bool:
    jvm = spark._jvm
    hconf = spark._jsc.hadoopConfiguration()
    fs = jvm.org.apache.hadoop.fs.FileSystem.get(hconf)
    return fs.exists(jvm.org.apache.hadoop.fs.Path(path))


def _assert_delta_or_empty(spark: SparkSession, path: str) -> None:
    #  refuse to write into non-delta location
    if _path_exists(spark, path) and not DeltaTable.isDeltaTable(spark, path):
        raise RuntimeError(f"Refusing to write: path exists but is NOT a Delta table: {path}")


def main():
    spark = build_spark()

    log(f"Kafka brokers: {KAFKA_BOOTSTRAP_SERVERS}")
    log(f"Topic: {KAFKA_TOPIC}")
    log(f"Starting offsets: {STARTING_OFFSETS}")
    log(f"Bronze path: {BRONZE_PATH}")
    log(f"Checkpoint: {CHECKPOINT_PATH}")
    log(f"Mode: {'availableNow' if AVAILABLE_NOW else 'continuous'}")

    _assert_delta_or_empty(spark, BRONZE_PATH)

    df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", STARTING_OFFSETS)
        .option("failOnDataLoss", str(FAIL_ON_DATA_LOSS).lower())
        .load()
    )

    out = df.select(
        F.col("topic").cast("string").alias("kafka_topic"),
        F.col("partition").cast("int").alias("kafka_partition"),
        F.col("offset").cast("long").alias("kafka_offset"),
        F.col("timestamp").alias("kafka_timestamp"),
        F.col("key").cast("string").alias("kafka_key_str"),
        F.col("value").cast("string").alias("kafka_value_str"),
        # Optional debugging gold:
        # F.col("value").alias("kafka_value_bytes"),
    )

    writer = (
        out.writeStream
        .format("delta")
        .option("checkpointLocation", CHECKPOINT_PATH)
        .outputMode("append")
    )

    if AVAILABLE_NOW:
        query = writer.trigger(availableNow=True).start(BRONZE_PATH)
    else:
        query = writer.trigger(processingTime=TRIGGER_INTERVAL).start(BRONZE_PATH)

    log("Streaming started.")
    query.awaitTermination()


if __name__ == "__main__":
    main()


# # scripts/cdc_to_bronze_stream.py
# """
# Spark Structured Streaming job: Redpanda (Kafka) CDC topic -> Delta Bronze (append-only)

# What it does
# ------------
# - Reads Debezium CDC messages from a Redpanda/Kafka topic.
# - Writes them append-only into a Delta Lake Bronze table with Kafka metadata:
#   topic, partition, offset, timestamp, key_str, value_str

# Why append-only Bronze
# ----------------------
# - Very robust (no upserts in streaming).
# - Keeps an audit trail of raw CDC events.
# - Makes reprocessing easy: Silver can be rebuilt from Bronze.

# Run (continuous, recommended for real demo)
# -------------------------------------------
# docker compose exec spark-master \
#   spark-submit /opt/workspace/scripts/cdc_to_bronze_stream.py

# Run (test mode - finite)
# ------------------------
# docker compose exec -e CDC_AVAILABLE_NOW=1 spark-master \
#   spark-submit /opt/workspace/scripts/cdc_to_bronze_stream.py
# """

# import os
# from pyspark.sql import SparkSession, functions as F

# KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "redpanda:9092")
# KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "sportdb.public.activities")
# STARTING_OFFSETS = os.getenv("CDC_STARTING_OFFSETS", "earliest")  # earliest for initial backfill
# FAIL_ON_DATA_LOSS = os.getenv("CDC_FAIL_ON_DATA_LOSS", "false").lower() == "true"

# BRONZE_PATH = os.getenv(
#     "BRONZE_ACTIVITIES_PATH",
#     "/opt/workspace/data/delta/bronze/activities_cdc",
# )
# CHECKPOINT_PATH = os.getenv(
#     "BRONZE_ACTIVITIES_CHECKPOINT",
#     "/opt/workspace/data/delta/bronze/_checkpoints/activities_cdc",
# )

# AVAILABLE_NOW = os.getenv("CDC_AVAILABLE_NOW", "0") == "1"
# TRIGGER_INTERVAL = os.getenv("CDC_TRIGGER_INTERVAL", "10 seconds")


# def log(msg: str) -> None:
#     print(f"[cdc_to_bronze] {msg}", flush=True)


# def build_spark() -> SparkSession:
#     return (
#         SparkSession.builder
#         .appName("CDC_To_Bronze_Activities")
#         .getOrCreate()
#     )


# def main():
#     spark = build_spark()
#     log(f"Kafka brokers: {KAFKA_BOOTSTRAP_SERVERS}")
#     log(f"Topic: {KAFKA_TOPIC}")
#     log(f"Starting offsets: {STARTING_OFFSETS}")
#     log(f"Bronze path: {BRONZE_PATH}")
#     log(f"Checkpoint: {CHECKPOINT_PATH}")
#     log(f"Mode: {'availableNow' if AVAILABLE_NOW else 'continuous'}")

#     df = (
#         spark.readStream
#         .format("kafka")
#         .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
#         .option("subscribe", KAFKA_TOPIC)
#         .option("startingOffsets", STARTING_OFFSETS)
#         .option("failOnDataLoss", str(FAIL_ON_DATA_LOSS).lower())
#         .load()
#     )

#     out = (
#         df.select(
#             F.col("topic").cast("string").alias("kafka_topic"),
#             F.col("partition").cast("int").alias("kafka_partition"),
#             F.col("offset").cast("long").alias("kafka_offset"),
#             F.col("timestamp").alias("kafka_timestamp"),
#             F.col("key").cast("string").alias("kafka_key_str"),
#             F.col("value").cast("string").alias("kafka_value_str"),
#         )
#     )

#     writer = (
#         out.writeStream
#         .format("delta")
#         .option("checkpointLocation", CHECKPOINT_PATH)
#         .outputMode("append")
#     )

#     if AVAILABLE_NOW:
#         query = writer.trigger(availableNow=True).start(BRONZE_PATH)
#     else:
#         query = writer.trigger(processingTime=TRIGGER_INTERVAL).start(BRONZE_PATH)

#     log("Streaming started.")
#     query.awaitTermination()


# if __name__ == "__main__":
#     main()
