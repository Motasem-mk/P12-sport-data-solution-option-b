# scripts/bronze_to_silver.py
"""
Bronze -> Silver (activities)

Modes
-----
- FULL_REBUILD=1:
    Reads ALL Bronze, rebuilds Silver (overwrite), and writes offsets watermark.

- Default (incremental):
    Reads only Bronze rows where kafka_offset > last_offset per partition
    (watermark stored as Delta at BRONZE/_meta/activities_offsets),
    then MERGE(upsert) into Silver by activity_id, and updates watermark.

Safety
------
- We do NOT advance the watermark if we ended up with 0 curated rows
  (e.g., parsing bug / schema mismatch). This prevents skipping data forever.

Optional auto-repair
--------------------
- AUTO_FULL_REBUILD_ON_EMPTY_SILVER=1 (default: 0):
    If Silver is missing or empty while Bronze has rows, automatically rebuild.

CDC formats supported
---------------------
This script can parse THREE common CDC JSON formats found in Kafka:

A) Kafka Connect JSON with schemas enabled (YOUR CURRENT FORMAT):
   {
     "schema": {...},
     "payload": {
       "activity_id": ...,
       "employee_id": ...,
       "start_time": ... (Debezium MicroTimestamp = micros since epoch),
       ...
       "__op": "c/u/r/d",
       "__ts_ms": 1234567890
     }
   }

B) Debezium envelope (schemas enabled, not unwrapped):
   {
     "schema": {...},
     "payload": {
       "after": {...},
       "op": "c/u/r/d",
       "ts_ms": 1234567890
     }
   }

C) Unwrapped / flattened JSON (ExtractNewRecordState):
   {
     "activity_id": ...,
     ...
     "op": "c/u/r",
     "ts_ms": 1234567890
   }
"""

import os
from pyspark.sql import SparkSession, functions as F, types as T
from pyspark.sql.window import Window
from delta.tables import DeltaTable


BRONZE_PATH = os.getenv("BRONZE_ACTIVITIES_PATH", "/opt/workspace/data/delta/bronze/activities_cdc")
OFFSETS_WATERMARK_PATH = os.getenv(
    "BRONZE_ACTIVITIES_OFFSETS_PATH",
    "/opt/workspace/data/delta/bronze/_meta/activities_offsets",
)
SILVER_PATH = os.getenv("SILVER_ACTIVITIES_PATH", "/opt/workspace/data/delta/silver/activities")


def log(msg: str) -> None:
    print(f"[bronze_to_silver] {msg}", flush=True)


def build_spark() -> SparkSession:
    return SparkSession.builder.appName("Bronze_To_Silver_Activities").getOrCreate()


def path_exists(spark: SparkSession, path: str) -> bool:
    jvm = spark._jvm
    hconf = spark._jsc.hadoopConfiguration()
    p = jvm.org.apache.hadoop.fs.Path(path)
    fs = p.getFileSystem(hconf)
    return fs.exists(p)


def df_has_rows(df) -> bool:
    # Fast-ish existence check without full count
    return len(df.take(1)) > 0


def load_offsets_watermark(spark: SparkSession):
    schema = T.StructType([
        T.StructField("kafka_partition", T.IntegerType(), False),
        T.StructField("last_offset", T.LongType(), False),
    ])

    if not path_exists(spark, OFFSETS_WATERMARK_PATH):
        log("No offsets watermark found (first run).")
        return spark.createDataFrame([], schema)

    return spark.read.format("delta").load(OFFSETS_WATERMARK_PATH)


def save_offsets_watermark(df_offsets) -> None:
    (df_offsets.write.format("delta").mode("overwrite").save(OFFSETS_WATERMARK_PATH))


def parse_debezium(df):
    """
    Normalize CDC rows into columns:
      activity_id, employee_id, start_time, activity_date, sport_type, distance_m, elapsed_time_s, comment, op, ts_ms
    Supports:
      A) Kafka Connect (schema+payload direct fields)   <-- your format
      B) Debezium envelope (payload.after)
      C) Unwrapped JSON (flat)
    """

    # ---------------------------
    # A) Kafka Connect schema/payload wrapper (YOUR CURRENT FORMAT)
    # ---------------------------
    payload_str = F.get_json_object(F.col("kafka_value_str"), "$.payload")

    payload_schema = T.StructType([
        T.StructField("activity_id", T.LongType(), True),
        T.StructField("employee_id", T.LongType(), True),
        T.StructField("start_time", T.LongType(), True),   # MicroTimestamp (micros since epoch)
        T.StructField("sport_type", T.StringType(), True),
        T.StructField("distance_m", T.IntegerType(), True),
        T.StructField("elapsed_time_s", T.IntegerType(), True),
        T.StructField("comment", T.StringType(), True),

        # Kafka Connect SMT / Debezium-added metadata
        T.StructField("__deleted", T.StringType(), True),
        T.StructField("__op", T.StringType(), True),       # usually 'c'
        T.StructField("__ts_ms", T.LongType(), True),
    ])

    # ---------------------------
    # B) Debezium envelope (payload.after)
    # ---------------------------
    after_str = F.get_json_object(F.col("kafka_value_str"), "$.payload.after")
    op_env = F.get_json_object(F.col("kafka_value_str"), "$.payload.op")
    ts_ms_env = F.get_json_object(F.col("kafka_value_str"), "$.payload.ts_ms").cast("long")

    after_schema = T.StructType([
        T.StructField("activity_id", T.LongType(), True),
        T.StructField("employee_id", T.LongType(), True),
        T.StructField("start_time", T.LongType(), True),   # MicroTimestamp (micros since epoch)
        T.StructField("sport_type", T.StringType(), True),
        T.StructField("distance_m", T.IntegerType(), True),
        T.StructField("elapsed_time_s", T.IntegerType(), True),
        T.StructField("comment", T.StringType(), True),
    ])

    # ---------------------------
    # C) Unwrapped / flat JSON
    # ---------------------------
    unwrap_schema = T.StructType([
        T.StructField("activity_id", T.LongType(), True),
        T.StructField("employee_id", T.LongType(), True),
        T.StructField("start_time", T.StringType(), True),
        T.StructField("sport_type", T.StringType(), True),
        T.StructField("distance_m", T.LongType(), True),
        T.StructField("elapsed_time_s", T.LongType(), True),
        T.StructField("comment", T.StringType(), True),
        T.StructField("op", T.StringType(), True),
        T.StructField("ts_ms", T.LongType(), True),
    ])

    df2 = (
        df
        .withColumn("p", F.from_json(payload_str, payload_schema))
        .withColumn("after", F.from_json(after_str, after_schema))
        .withColumn("u", F.from_json(F.col("kafka_value_str"), unwrap_schema))
        .withColumn("op_env", op_env)
        .withColumn("ts_ms_env", ts_ms_env)
    )

    out = (
        df2
        .select(
            "kafka_topic", "kafka_partition", "kafka_offset", "kafka_timestamp",

            F.coalesce(F.col("p.activity_id"), F.col("after.activity_id"), F.col("u.activity_id")).alias("activity_id"),
            F.coalesce(F.col("p.employee_id"), F.col("after.employee_id"), F.col("u.employee_id")).alias("employee_id"),

            # micros timestamp (from payload direct OR envelope after)
            F.coalesce(F.col("p.start_time"), F.col("after.start_time")).alias("start_time_micros"),
            # string timestamp (from unwrapped)
            F.col("u.start_time").alias("start_time_str"),

            F.coalesce(F.col("p.sport_type"), F.col("after.sport_type"), F.col("u.sport_type")).alias("sport_type"),
            F.coalesce(
                F.col("p.distance_m").cast("int"),
                F.col("after.distance_m").cast("int"),
                F.col("u.distance_m").cast("int")
            ).alias("distance_m"),
            F.coalesce(
                F.col("p.elapsed_time_s").cast("int"),
                F.col("after.elapsed_time_s").cast("int"),
                F.col("u.elapsed_time_s").cast("int")
            ).alias("elapsed_time_s"),
            F.coalesce(F.col("p.comment"), F.col("after.comment"), F.col("u.comment")).alias("comment"),

            # op + ts_ms
            F.coalesce(F.col("p.__op"), F.col("op_env"), F.col("u.op")).alias("op"),
            F.coalesce(F.col("p.__ts_ms"), F.col("ts_ms_env"), F.col("u.ts_ms")).alias("ts_ms"),
        )
    )

    # Convert start_time to timestamp (micros preferred)
    start_ts = F.coalesce(
        F.expr("timestamp_micros(start_time_micros)"),
        F.to_timestamp("start_time_str"),
        F.to_timestamp("start_time_str", "yyyy-MM-dd'T'HH:mm:ss.SSSX"),
        F.to_timestamp("start_time_str", "yyyy-MM-dd'T'HH:mm:ssX"),
        F.to_timestamp("start_time_str", "yyyy-MM-dd HH:mm:ss"),
    )

    return (
        out
        .withColumn("start_time", start_ts)
        .withColumn("activity_date", F.to_date("start_time"))
    )


def upsert_to_silver(spark: SparkSession, df_curated):
    cols = ["activity_id", "employee_id", "activity_date", "start_time", "sport_type",
            "distance_m", "elapsed_time_s", "comment"]
    df_curated = df_curated.select(*cols)

    if not df_has_rows(df_curated):
        log("Curated DF is empty -> nothing to write to Silver.")
        return False

    if not path_exists(spark, SILVER_PATH):
        log("Silver does not exist -> creating (overwrite)")
        (df_curated.write.format("delta").mode("overwrite").partitionBy("activity_date").save(SILVER_PATH))
        return True

    log("Silver exists -> MERGE by activity_id")
    target = DeltaTable.forPath(spark, SILVER_PATH)
    (
        target.alias("t")
        .merge(df_curated.alias("s"), "t.activity_id = s.activity_id")
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )
    return True


def do_full_rebuild(spark: SparkSession, df_bronze):
    log("FULL_REBUILD -> rebuild Silver from ALL Bronze")
    df_parsed = parse_debezium(df_bronze)

    df_parsed = (
        df_parsed
        .filter(F.col("op").isin(["c", "u", "r"]))  # ignore deletes
        .filter(F.col("activity_id").isNotNull())
        .filter(F.col("start_time").isNotNull())
    )

    # Keep latest event per activity_id
    order_col = F.coalesce(
        F.col("ts_ms").cast("long"),
        (F.col("kafka_timestamp").cast("long") * 1000)  # timestamp->seconds, convert to ms
    )
    win = Window.partitionBy("activity_id").orderBy(order_col.desc())
    df_latest = df_parsed.withColumn("rn", F.row_number().over(win)).filter("rn = 1").drop("rn")

    if not df_has_rows(df_latest):
        log("FULL_REBUILD produced 0 curated rows -> NOT overwriting Silver, NOT writing watermark.")
        return False

    (
        df_latest.select(
            "activity_id", "employee_id", "activity_date", "start_time", "sport_type",
            "distance_m", "elapsed_time_s", "comment"
        )
        .write.format("delta")
        .mode("overwrite")
        .partitionBy("activity_date")
        .save(SILVER_PATH)
    )

    df_offsets = df_bronze.groupBy("kafka_partition").agg(F.max("kafka_offset").alias("last_offset"))
    log("Writing offsets watermark (FULL_REBUILD)…")
    save_offsets_watermark(df_offsets)
    log("FULL_REBUILD done.")
    return True


def main():
    spark = build_spark()
    try:
        full_rebuild = os.getenv("FULL_REBUILD", "0") == "1"
        auto_rebuild = os.getenv("AUTO_FULL_REBUILD_ON_EMPTY_SILVER", "0") == "1"

        log(f"BRONZE_PATH={BRONZE_PATH}")
        log(f"SILVER_PATH={SILVER_PATH}")
        log(f"OFFSETS_WATERMARK_PATH={OFFSETS_WATERMARK_PATH}")
        log(f"FULL_REBUILD={full_rebuild}")
        log(f"AUTO_FULL_REBUILD_ON_EMPTY_SILVER={auto_rebuild}")

        if not path_exists(spark, BRONZE_PATH):
            log("ERROR: Bronze path does not exist. Exiting.")
            return

        df_bronze = spark.read.format("delta").load(BRONZE_PATH)

        if full_rebuild:
            do_full_rebuild(spark, df_bronze)
            return

        # Optional auto-repair: if Silver missing/empty but Bronze has rows -> rebuild
        if auto_rebuild:
            bronze_has = df_has_rows(df_bronze)
            silver_has = False
            if path_exists(spark, SILVER_PATH):
                try:
                    silver_has = df_has_rows(spark.read.format("delta").load(SILVER_PATH))
                except Exception:
                    silver_has = False
            if bronze_has and not silver_has:
                log("AUTO_REBUILD triggered: Silver missing/empty while Bronze has rows.")
                do_full_rebuild(spark, df_bronze)
                return

        # incremental
        df_wm = load_offsets_watermark(spark)

        df_new = (
            df_bronze.alias("b")
            .join(df_wm.alias("w"), on="kafka_partition", how="left")
            .withColumn("last_offset", F.coalesce(F.col("last_offset"), F.lit(-1)))
            .filter(F.col("kafka_offset") > F.col("last_offset"))
            .drop("last_offset")
            .cache()
        )

        new_cnt = df_new.count()
        log(f"New Bronze rows since watermark: {new_cnt}")
        if new_cnt == 0:
            log("No new Bronze events. Exiting.")
            return

        df_parsed = parse_debezium(df_new)
        df_parsed = (
            df_parsed
            .filter(F.col("op").isin(["c", "u", "r"]))
            .filter(F.col("activity_id").isNotNull())
            .filter(F.col("start_time").isNotNull())
        )

        order_col = F.coalesce(
            F.col("ts_ms").cast("long"),
            (F.col("kafka_timestamp").cast("long") * 1000)
        )
        win = Window.partitionBy("activity_id").orderBy(order_col.desc())
        df_latest = df_parsed.withColumn("rn", F.row_number().over(win)).filter("rn = 1").drop("rn")

        wrote = upsert_to_silver(spark, df_latest)
        if not wrote:
            log("SAFETY: Not updating watermark because no curated rows were written.")
            return

        # Update watermark: merge old + new, keep max per partition
        df_new_offsets = df_new.groupBy("kafka_partition").agg(F.max("kafka_offset").alias("last_offset"))

        df_offsets_final = (
            df_wm.unionByName(df_new_offsets)
            .groupBy("kafka_partition")
            .agg(F.max("last_offset").alias("last_offset"))
        )

        log("Updating offsets watermark…")
        save_offsets_watermark(df_offsets_final)
        log("Done.")

    finally:
        spark.stop()
        log("Spark stopped.")


if __name__ == "__main__":
    main()
