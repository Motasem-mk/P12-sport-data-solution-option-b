# scripts/hr_sportive_to_silver_employee_ref.py

"""
Build the Silver Delta employee reference table from raw HR and sportive CSV files.

Inputs:
    /opt/workspace/data/raw/hr.csv
    /opt/workspace/data/raw/sportive.csv

Output:
    /opt/workspace/data/delta/silver/employee_ref

Main behavior:
    - Read CSV files with explicit schemas.
    - Standardize HR and sport fields.
    - Join HR with sportive data.
    - Create or update the Silver employee_ref Delta table.
    - Preserve commute enrichment columns when business data does not change.
"""

import os
import sys

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

try:
    from delta.tables import DeltaTable
except Exception:
    print(
        "ERROR: delta.tables import failed. "
        "Make sure Delta Lake is available in the Spark image.",
        file=sys.stderr,
    )
    raise


# ============================================================
# 1. Configuration
# ============================================================

BASE_DATA = os.getenv("BASE_DATA", "/opt/workspace/data")

HR_RAW_PATH = os.getenv(
    "HR_RAW_PATH",
    f"{BASE_DATA}/raw/hr.csv",
)

SPORTIVE_RAW_PATH = os.getenv(
    "SPORTIVE_RAW_PATH",
    f"{BASE_DATA}/raw/sportive.csv",
)

EMPLOYEE_REF_PATH = os.getenv(
    "EMPLOYEE_REF_PATH",
    f"{BASE_DATA}/delta/silver/employee_ref",
)

CSV_DELIMITER = os.getenv("CSV_DELIMITER", ",")


# ============================================================
# 2. Input schemas
# ============================================================
# Explicit schemas make the pipeline stable and avoid inferSchema surprises.

HR_SCHEMA = T.StructType([
    T.StructField("ID salarié", T.StringType(), True),
    T.StructField("Nom", T.StringType(), True),
    T.StructField("Prénom", T.StringType(), True),
    T.StructField("Date de naissance", T.StringType(), True),
    T.StructField("BU", T.StringType(), True),
    T.StructField("Date d'embauche", T.StringType(), True),
    T.StructField("Salaire brut", T.StringType(), True),
    T.StructField("Type de contrat", T.StringType(), True),
    T.StructField("Nombre de jours de CP", T.StringType(), True),
    T.StructField("Adresse du domicile", T.StringType(), True),
    T.StructField("Moyen de déplacement", T.StringType(), True),
])

SPORTIVE_SCHEMA = T.StructType([
    T.StructField("ID salarié", T.StringType(), True),
    T.StructField("Pratique d'un sport", T.StringType(), True),
])


# Business columns used to detect changes between runs.
BUSINESS_COLS = [
    "employee_id",
    "last_name",
    "first_name",
    "birth_date",
    "business_unit",
    "hire_date",
    "gross_salary_eur",
    "contract_type",
    "annual_leave_days",
    "home_address",
    "commute_mode",
    "sport_practice",
    "has_sport_practice",
]


# ============================================================
# 3. Spark and filesystem helpers
# ============================================================

def build_spark_session() -> SparkSession:
    """
    Create Spark session for Delta operations.
    """
    spark = (
        SparkSession.builder
        .appName("HR_Sportive_To_Silver_EmployeeRef")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.databricks.delta.schema.autoMerge.enabled", "true")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel(os.getenv("SPARK_LOG_LEVEL", "WARN"))

    print(f"[INFO] Spark version: {spark.version}", flush=True)
    return spark


def path_exists(spark: SparkSession, path: str) -> bool:
    """
    Check if a path exists using Hadoop FileSystem.
    """
    jvm = spark._jvm
    hconf = spark._jsc.hadoopConfiguration()
    fs = jvm.org.apache.hadoop.fs.FileSystem.get(hconf)
    return fs.exists(jvm.org.apache.hadoop.fs.Path(path))


def assert_delta_or_empty(spark: SparkSession, path: str) -> None:
    """
    Safety check:
        - if the path does not exist, we can create it;
        - if it exists, it must already be a Delta table.
    """
    if path_exists(spark, path) and not DeltaTable.isDeltaTable(spark, path):
        raise RuntimeError(
            f"Refusing to write: path exists but is not a Delta table: {path}"
        )


# ============================================================
# 4. Reading and parsing helpers
# ============================================================

def read_csv(
    spark: SparkSession,
    path: str,
    schema: T.StructType,
) -> DataFrame:
    """
    Read a CSV file using an explicit schema.
    """
    return (
        spark.read.format("csv")
        .option("header", "true")
        .option("delimiter", CSV_DELIMITER)
        .schema(schema)
        .load(path)
    )


def parse_date(col: F.Column) -> F.Column:
    """
    Parse dates from either yyyy-MM-dd or dd/MM/yyyy.
    """
    return F.coalesce(
        F.to_date(col, "yyyy-MM-dd"),
        F.to_date(col, "dd/MM/yyyy"),
        F.to_date(col),
    )


def parse_salary(col: F.Column) -> F.Column:
    """
    Convert salary text to double.

    Handles:
        - spaces
        - French decimal comma
    """
    cleaned = F.regexp_replace(F.trim(col.cast("string")), r"\s+", "")
    cleaned = F.regexp_replace(cleaned, ",", ".")
    return cleaned.cast("double")


def normalize_text(col: F.Column) -> F.Column:
    """
    Basic text normalization for matching French labels.
    """
    c = F.lower(F.trim(col.cast("string")))

    c = F.regexp_replace(c, "é|è|ê|ë", "e")
    c = F.regexp_replace(c, "à|â", "a")
    c = F.regexp_replace(c, "ù|û|ü", "u")
    c = F.regexp_replace(c, "î|ï", "i")
    c = F.regexp_replace(c, "ô|ö", "o")
    c = F.regexp_replace(c, r"\s+", " ")

    return c


# ============================================================
# 5. Standardization
# ============================================================

def standardize_commute_mode(df_hr: DataFrame) -> DataFrame:
    """
    Convert raw commute labels into controlled values:

        walk_run
        bike_scooter
        car
        public_transport
        other
    """
    raw = normalize_text(F.col("Moyen de déplacement"))

    return df_hr.withColumn(
        "commute_mode",
        F.when(raw.rlike(r"(marche|walking|run|running|course)"), F.lit("walk_run"))
         .when(raw.rlike(r"(velo|bicyc|bike|trottinette|scooter)"), F.lit("bike_scooter"))
         .when(raw.rlike(r"(vehicule|voiture|auto|thermique|electrique)"), F.lit("car"))
         .when(raw.rlike(r"(transport|metro|bus|tram|rer)"), F.lit("public_transport"))
         .otherwise(F.lit("other")),
    )


def standardize_sport_practice(df_sportive: DataFrame) -> DataFrame:
    """
    Convert raw sport labels into English analytical values.
    """
    sport_col = F.trim(F.col("Pratique d'un sport"))

    fallback = F.lower(
        F.regexp_replace(sport_col.cast("string"), r"\s+", "_")
    )

    return (
        df_sportive
        .withColumn("Pratique d'un sport", sport_col)
        .withColumn(
            "sport_practice",
            F.when(sport_col == "Runing", "running")
             .when(sport_col == "Randonnée", "hiking")
             .when(sport_col == "Tennis", "tennis")
             .when(sport_col == "Natation", "swimming")
             .when(sport_col == "Football", "football")
             .when(sport_col == "Rugby", "rugby")
             .when(sport_col == "Badminton", "badminton")
             .when(sport_col == "Voile", "sailing")
             .when(sport_col == "Judo", "judo")
             .when(sport_col == "Boxe", "boxing")
             .when(sport_col == "Escalade", "climbing")
             .when(sport_col == "Triathlon", "triathlon")
             .when(sport_col == "Équitation", "horse_riding")
             .when(sport_col == "Tennis de table", "table_tennis")
             .when(sport_col == "Basketball", "basketball")
             .otherwise(fallback),
        )
    )


# ============================================================
# 6. Transformation
# ============================================================

def business_hash_expr(cols: list) -> F.Column:
    """
    Build a hash of business columns.

    This allows the MERGE to update rows only when real business
    content changed.
    """
    return F.sha2(
        F.concat_ws(
            "||",
            *[
                F.coalesce(F.col(c).cast("string"), F.lit(""))
                for c in cols
            ],
        ),
        256,
    )


def transform_to_employee_ref(
    df_hr_raw: DataFrame,
    df_sportive_raw: DataFrame,
) -> DataFrame:
    """
    Build the final Silver employee reference DataFrame.
    """
    df_hr = standardize_commute_mode(df_hr_raw)
    df_sportive = standardize_sport_practice(df_sportive_raw)

    df_hr = (
        df_hr
        .withColumnRenamed("ID salarié", "employee_id")
        .withColumnRenamed("Nom", "last_name")
        .withColumnRenamed("Prénom", "first_name")
        .withColumnRenamed("Date de naissance", "birth_date")
        .withColumnRenamed("BU", "business_unit")
        .withColumnRenamed("Date d'embauche", "hire_date")
        .withColumnRenamed("Salaire brut", "gross_salary_eur")
        .withColumnRenamed("Type de contrat", "contract_type")
        .withColumnRenamed("Nombre de jours de CP", "annual_leave_days")
        .withColumnRenamed("Adresse du domicile", "home_address")
    )

    df_hr = (
        df_hr
        .withColumn("employee_id", F.col("employee_id").cast(T.LongType()))
        .withColumn("last_name", F.trim(F.col("last_name")))
        .withColumn("first_name", F.trim(F.col("first_name")))
        .withColumn("business_unit", F.trim(F.col("business_unit")))
        .withColumn("contract_type", F.trim(F.col("contract_type")))
        .withColumn("home_address", F.trim(F.col("home_address")))
        .withColumn("birth_date", parse_date(F.col("birth_date")))
        .withColumn("hire_date", parse_date(F.col("hire_date")))
        .withColumn("gross_salary_eur", parse_salary(F.col("gross_salary_eur")))
        .withColumn("annual_leave_days", F.col("annual_leave_days").cast(T.IntegerType()))
    )

    df_sportive = (
        df_sportive
        .withColumnRenamed("ID salarié", "employee_id")
        .withColumn("employee_id", F.col("employee_id").cast(T.LongType()))
        .select("employee_id", "sport_practice")
    )

    df_joined = df_hr.join(df_sportive, on="employee_id", how="left")

    df_employee_ref = df_joined.select(
        "employee_id",
        "last_name",
        "first_name",
        "birth_date",
        "business_unit",
        "hire_date",
        "gross_salary_eur",
        "contract_type",
        "annual_leave_days",
        "home_address",
        "commute_mode",
        "sport_practice",
        (F.col("sport_practice").isNotNull()).alias("has_sport_practice"),
    )

    return df_employee_ref.withColumn(
        "business_hash",
        business_hash_expr(BUSINESS_COLS),
    )


# ============================================================
# 7. Validation
# ============================================================

def validate_employee_ref(df: DataFrame) -> None:
    """
    Basic data quality guards before writing the Delta table.
    """
    null_id_count = df.filter(F.col("employee_id").isNull()).limit(1).count()

    if null_id_count > 0:
        raise RuntimeError("Found NULL employee_id in employee_ref.")

    duplicate_count = (
        df.groupBy("employee_id")
          .count()
          .filter(F.col("count") > 1)
          .limit(1)
          .count()
    )

    if duplicate_count > 0:
        raise RuntimeError("Found duplicate employee_id values in employee_ref.")


# ============================================================
# 8. Delta write / upsert
# ============================================================

def upsert_employee_ref(
    spark: SparkSession,
    df_new: DataFrame,
    path: str,
) -> None:
    """
    Create or update the Silver employee_ref Delta table.

    First run:
        create the Delta table.

    Next runs:
        MERGE on employee_id.

    Enrichment columns:
        distance_km,
        commute_valid_for_bonus,
        commute_checked_at

    These are preserved unless home_address or commute_mode changes,
    because a commute distance must then be recalculated.
    """
    assert_delta_or_empty(spark, path)

    df_new = (
        df_new
        .withColumn("created_at", F.current_timestamp())
        .withColumn("updated_at", F.current_timestamp())
        .withColumn("distance_km", F.lit(None).cast("double"))
        .withColumn("commute_valid_for_bonus", F.lit(None).cast("boolean"))
        .withColumn("commute_checked_at", F.lit(None).cast("timestamp"))
    )

    if not path_exists(spark, path):
        (
            df_new.write
            .format("delta")
            .mode("overwrite")
            .save(path)
        )
        print("[INFO] Created new Delta employee_ref table.", flush=True)
        return

    target = DeltaTable.forPath(spark, path)

    commute_key_changed = (
        "coalesce(t.home_address,'') <> coalesce(s.home_address,'') "
        "OR coalesce(t.commute_mode,'') <> coalesce(s.commute_mode,'')"
    )

    # Do not update employee_id. It is the merge key.
    business_cols_to_update = [
        col for col in BUSINESS_COLS
        if col != "employee_id"
    ]

    update_set = {
        col: f"s.{col}"
        for col in business_cols_to_update
    }

    update_set.update({
        "business_hash": "s.business_hash",
        "updated_at": "current_timestamp()",
        "created_at": "t.created_at",

        # Preserve Google Maps enrichment unless address/mode changed.
        "distance_km": (
            f"CASE WHEN {commute_key_changed} "
            "THEN NULL ELSE t.distance_km END"
        ),
        "commute_valid_for_bonus": (
            f"CASE WHEN {commute_key_changed} "
            "THEN NULL ELSE t.commute_valid_for_bonus END"
        ),
        "commute_checked_at": (
            f"CASE WHEN {commute_key_changed} "
            "THEN NULL ELSE t.commute_checked_at END"
        ),
    })

    insert_values = {
        col: f"s.{col}"
        for col in (
            BUSINESS_COLS
            + [
                "business_hash",
                "created_at",
                "updated_at",
                "distance_km",
                "commute_valid_for_bonus",
                "commute_checked_at",
            ]
        )
    }

    (
        target.alias("t")
        .merge(
            df_new.alias("s"),
            "t.employee_id = s.employee_id",
        )
        .whenMatchedUpdate(
            condition="t.business_hash <> s.business_hash",
            set=update_set,
        )
        .whenNotMatchedInsert(values=insert_values)
        .execute()
    )

    print("[INFO] Delta employee_ref MERGE completed.", flush=True)


# ============================================================
# 9. Main
# ============================================================

def main() -> None:
    """
    Main execution flow.
    """
    spark = build_spark_session()

    try:
        print(f"[INFO] HR CSV: {HR_RAW_PATH}", flush=True)
        print(f"[INFO] Sportive CSV: {SPORTIVE_RAW_PATH}", flush=True)
        print(f"[INFO] Silver output: {EMPLOYEE_REF_PATH}", flush=True)

        df_hr_raw = read_csv(spark, HR_RAW_PATH, HR_SCHEMA)
        df_sportive_raw = read_csv(spark, SPORTIVE_RAW_PATH, SPORTIVE_SCHEMA)

        df_employee_ref = transform_to_employee_ref(
            df_hr_raw,
            df_sportive_raw,
        )

        validate_employee_ref(df_employee_ref)

        upsert_employee_ref(
            spark=spark,
            df_new=df_employee_ref,
            path=EMPLOYEE_REF_PATH,
        )

        print("[INFO] Silver employee_ref build completed successfully.", flush=True)

    finally:
        spark.stop()


if __name__ == "__main__":
    main()


# # scripts/hr_sportive_to_silver_employee_ref.py
# """
# Build/UPSERT silver.employee_ref Delta table from raw HR & sportive CSV files.

# Inputs:
#     /opt/workspace/data/raw/hr.csv
#     /opt/workspace/data/raw/sportive.csv

# Output (Delta):
#     /opt/workspace/data/delta/silver/employee_ref

# Real-world/OC requirements implemented:
# - Explicit CSV schemas (no inferSchema)
# - Controlled vocab for commute_mode
# - Join sportive -> HR and compute has_sport_practice
# - Idempotent UPSERT (Delta MERGE) keyed by employee_id
# - created_at set once, updated_at updated only when business data changes
# - Preserve enrichment columns (distance_km, commute_valid_for_bonus, commute_checked_at)
# - Reset enrichment only when home_address changes

# (real-world correctness + safety):
# - Reset enrichment when *home_address OR commute_mode* changes (distance depends on mode too).
# - Do NOT update primary key employee_id in MERGE update_set (best practice; avoids edge issues).
# """

# import os
# import sys
# from pyspark.sql import SparkSession, DataFrame
# from pyspark.sql import functions as F
# from pyspark.sql import types as T

# try:
#     from delta.tables import DeltaTable
# except Exception as e:
#     print("ERROR: delta.tables import failed. Ensure Delta Lake is available in your Spark image.", file=sys.stderr)
#     raise


# # ----------------------------
# # Config
# # ----------------------------
# BASE_DATA = os.getenv("BASE_DATA", "/opt/workspace/data")
# HR_RAW_PATH = os.getenv("HR_RAW_PATH", f"{BASE_DATA}/raw/hr.csv")
# SPORTIVE_RAW_PATH = os.getenv("SPORTIVE_RAW_PATH", f"{BASE_DATA}/raw/sportive.csv")
# EMPLOYEE_REF_SILVER_PATH = os.getenv("EMPLOYEE_REF_PATH", f"{BASE_DATA}/delta/silver/employee_ref")

# CSV_DELIMITER = os.getenv("CSV_DELIMITER", ",")


# # ----------------------------
# # Explicit schemas (stable)
# # ----------------------------
# HR_SCHEMA = T.StructType([
#     T.StructField("ID salarié", T.StringType(), True),
#     T.StructField("Nom", T.StringType(), True),
#     T.StructField("Prénom", T.StringType(), True),
#     T.StructField("Date de naissance", T.StringType(), True),
#     T.StructField("BU", T.StringType(), True),
#     T.StructField("Date d'embauche", T.StringType(), True),
#     T.StructField("Salaire brut", T.StringType(), True),
#     T.StructField("Type de contrat", T.StringType(), True),
#     T.StructField("Nombre de jours de CP", T.StringType(), True),
#     T.StructField("Adresse du domicile", T.StringType(), True),
#     T.StructField("Moyen de déplacement", T.StringType(), True),
# ])

# SPORTIVE_SCHEMA = T.StructType([
#     T.StructField("ID salarié", T.StringType(), True),
#     T.StructField("Pratique d'un sport", T.StringType(), True),
# ])


# # Business columns used for change detection (hash)
# BUSINESS_COLS = [
#     "employee_id",
#     "last_name",
#     "first_name",
#     "birth_date",
#     "business_unit",
#     "hire_date",
#     "gross_salary_eur",
#     "contract_type",
#     "annual_leave_days",
#     "home_address",
#     "commute_mode",
#     "sport_practice",
#     "has_sport_practice",
# ]


# def build_spark_session() -> SparkSession:
#     """
#     Create a SparkSession configured for Delta Lake operations.
#     - UTC timezone for stable timestamps
#     - Delta schema auto-merge enabled (safe for adding columns over time)
#     """
#     spark = (
#         SparkSession.builder
#         .appName("HR_Sportive_To_Silver_EmployeeRef")
#         # Stable timestamps
#         .config("spark.sql.session.timeZone", "UTC")
#         # Allow schema evolution in Delta MERGE (adds new columns if missing)
#         .config("spark.databricks.delta.schema.autoMerge.enabled", "true")
#         .getOrCreate()
#     )
#     spark.sparkContext.setLogLevel(os.getenv("SPARK_LOG_LEVEL", "WARN"))
#     return spark


# def hadoop_path_exists(spark: SparkSession, path: str) -> bool:
#     """
#     Check if a path exists using Hadoop FS (works for local FS, HDFS, S3, etc. depending on Spark config).
#     """
#     jvm = spark._jvm
#     hconf = spark._jsc.hadoopConfiguration()
#     fs = jvm.org.apache.hadoop.fs.FileSystem.get(hconf)
#     return fs.exists(jvm.org.apache.hadoop.fs.Path(path))


# def assert_delta_or_empty(spark: SparkSession, path: str) -> None:
#     """
#     Safety guard:
#     - If path exists but is NOT a Delta table -> FAIL (prevents accidental overwrite of non-delta data)
#     - If path doesn't exist -> OK (we will create)
#     """
#     if hadoop_path_exists(spark, path):
#         if not DeltaTable.isDeltaTable(spark, path):
#             raise RuntimeError(
#                 f"Refusing to write: path exists but is NOT a Delta table: {path}"
#             )


# def read_csv_strict(spark: SparkSession, path: str, schema: T.StructType) -> DataFrame:
#     """
#     Read CSV with an explicit schema (no inferSchema) for stability and OC requirements.
#     """
#     return (
#         spark.read.format("csv")
#         .option("header", "true")
#         .option("delimiter", CSV_DELIMITER)
#         .schema(schema)
#         .load(path)
#     )


# def parse_date(col: F.Column) -> F.Column:
#     """Robust date parsing: tries ISO first, then French dd/MM/yyyy."""
#     return F.coalesce(
#         F.to_date(col, "yyyy-MM-dd"),
#         F.to_date(col, "dd/MM/yyyy"),
#         F.to_date(col)
#     )


# def parse_salary(col: F.Column) -> F.Column:
#     """
#     Robust salary parsing:
#     - removes spaces
#     - converts comma decimal to dot
#     """
#     cleaned = F.regexp_replace(F.trim(col.cast("string")), r"\s+", "")
#     cleaned = F.regexp_replace(cleaned, ",", ".")
#     return cleaned.cast("double")


# def normalize_text(col: F.Column) -> F.Column:
#     """
#     Lightweight normalization (lowercase + remove common accents + collapse spaces).
#     """
#     c = F.lower(F.trim(col.cast("string")))
#     c = F.regexp_replace(c, "é|è|ê|ë", "e")
#     c = F.regexp_replace(c, "à|â", "a")
#     c = F.regexp_replace(c, "ù|û|ü", "u")
#     c = F.regexp_replace(c, "î|ï", "i")
#     c = F.regexp_replace(c, "ô|ö", "o")
#     c = F.regexp_replace(c, r"\s+", " ")
#     return c


# def standardize_commute_mode(df_hr: DataFrame) -> DataFrame:
#     """
#     Standardize commute mode into a controlled vocabulary:
#     walk_run, bike_scooter, car, public_transport, other
#     """
#     raw_norm = normalize_text(F.col("Moyen de déplacement"))

#     return df_hr.withColumn(
#         "commute_mode",
#         F.when(raw_norm.rlike(r"(marche|walking|run|running|course)"), F.lit("walk_run"))
#          .when(raw_norm.rlike(r"(velo|bicyc|bike|trottinette|scooter)"), F.lit("bike_scooter"))
#          .when(raw_norm.rlike(r"(vehicule|voiture|auto|thermique|electrique)"), F.lit("car"))
#          .when(raw_norm.rlike(r"(transport|metro|bus|tram|rer)"), F.lit("public_transport"))
#          .otherwise(F.lit("other"))
#     )


# def standardize_sport_practice(df_sportive: DataFrame) -> DataFrame:
#     """
#     Standardize sport practice labels to consistent, analytics-friendly values.
#     """
#     df = df_sportive.withColumn("Pratique d'un sport", F.trim(F.col("Pratique d'un sport")))
#     sport_col = F.col("Pratique d'un sport")

#     fallback = F.lower(F.regexp_replace(sport_col.cast("string"), r"\s+", "_"))

#     return df.withColumn(
#         "sport_practice",
#         F.when(sport_col == "Runing", "running")  # fix typo
#          .when(sport_col == "Randonnée", "hiking")
#          .when(sport_col == "Tennis", "tennis")
#          .when(sport_col == "Natation", "swimming")
#          .when(sport_col == "Football", "football")
#          .when(sport_col == "Rugby", "rugby")
#          .when(sport_col == "Badminton", "badminton")
#          .when(sport_col == "Voile", "sailing")
#          .when(sport_col == "Judo", "judo")
#          .when(sport_col == "Boxe", "boxing")
#          .when(sport_col == "Escalade", "climbing")
#          .when(sport_col == "Triathlon", "triathlon")
#          .when(sport_col == "Équitation", "horse_riding")
#          .when(sport_col == "Tennis de table", "table_tennis")
#          .when(sport_col == "Basketball", "basketball")
#          .otherwise(fallback)
#     )


# def business_hash_expr(cols) -> F.Column:
#     """
#     Build a stable SHA-256 hash over selected business columns.
#     Used to detect changes and update only when business content changes.
#     """
#     return F.sha2(
#         F.concat_ws("||", *[F.coalesce(F.col(c).cast("string"), F.lit("")) for c in cols]),
#         256
#     )


# def transform_to_employee_ref(df_hr_raw: DataFrame, df_sportive_raw: DataFrame) -> DataFrame:
#     """
#     Transform raw HR & sportive data into the silver.employee_ref business shape.
#     """
#     df_hr = standardize_commute_mode(df_hr_raw)
#     df_sp = standardize_sport_practice(df_sportive_raw)

#     df_hr = (
#         df_hr
#         .withColumnRenamed("ID salarié", "employee_id")
#         .withColumnRenamed("Nom", "last_name")
#         .withColumnRenamed("Prénom", "first_name")
#         .withColumnRenamed("Date de naissance", "birth_date")
#         .withColumnRenamed("BU", "business_unit")
#         .withColumnRenamed("Date d'embauche", "hire_date")
#         .withColumnRenamed("Salaire brut", "gross_salary_eur")
#         .withColumnRenamed("Type de contrat", "contract_type")
#         .withColumnRenamed("Nombre de jours de CP", "annual_leave_days")
#         .withColumnRenamed("Adresse du domicile", "home_address")
#     )

#     df_hr = (
#         df_hr
#         .withColumn("employee_id", F.col("employee_id").cast(T.LongType()))
#         .withColumn("last_name", F.trim(F.col("last_name")))
#         .withColumn("first_name", F.trim(F.col("first_name")))
#         .withColumn("business_unit", F.trim(F.col("business_unit")))
#         .withColumn("contract_type", F.trim(F.col("contract_type")))
#         .withColumn("home_address", F.trim(F.col("home_address")))
#         .withColumn("birth_date", parse_date(F.col("birth_date")))
#         .withColumn("hire_date", parse_date(F.col("hire_date")))
#         .withColumn("gross_salary_eur", parse_salary(F.col("gross_salary_eur")))
#         .withColumn("annual_leave_days", F.col("annual_leave_days").cast(T.IntegerType()))
#     )

#     df_sp = (
#         df_sp
#         .withColumnRenamed("ID salarié", "employee_id")
#         .withColumn("employee_id", F.col("employee_id").cast(T.LongType()))
#         .select("employee_id", "sport_practice")
#     )

#     df_joined = df_hr.join(df_sp, on="employee_id", how="left")

#     df_out = df_joined.select(
#         "employee_id",
#         "last_name",
#         "first_name",
#         "birth_date",
#         "business_unit",
#         "hire_date",
#         "gross_salary_eur",
#         "contract_type",
#         "annual_leave_days",
#         "home_address",
#         "commute_mode",
#         "sport_practice",
#         (F.col("sport_practice").isNotNull()).alias("has_sport_practice"),
#     )

#     # Add change detection hash
#     df_out = df_out.withColumn("business_hash", business_hash_expr(BUSINESS_COLS))
#     return df_out


# def upsert_employee_ref(spark: SparkSession, df_new: DataFrame, path: str) -> None:
#     """
#     Delta MERGE (UPSERT):
#     - Insert new employees with created_at/updated_at set, enrichment cols NULL
#     - Update matched employees only if business_hash changed
#     - Preserve enrichment cols unless home_address changes -> reset enrichment to NULL

    
#     - Reset enrichment when home_address OR commute_mode changes (distance depends on mode too).
#     - Avoid updating employee_id in update_set (PK should not be updated).
#     """
#     assert_delta_or_empty(spark, path)

#     # Ensure required columns exist in incoming DF
#     df_new = (
#         df_new
#         .withColumn("created_at", F.current_timestamp())
#         .withColumn("updated_at", F.current_timestamp())
#         .withColumn("distance_km", F.lit(None).cast("double"))
#         .withColumn("commute_valid_for_bonus", F.lit(None).cast("boolean"))
#         .withColumn("commute_checked_at", F.lit(None).cast("timestamp"))
#     )

#     if not hadoop_path_exists(spark, path):
#         (df_new.write.format("delta").mode("overwrite").save(path))
#         return

#     t = DeltaTable.forPath(spark, path)

#     #  commute_mode change must also invalidate enrichment
#     # WHY: Distance route/length depends on mode (walking vs bicycling vs driving).
#     commute_key_changed = (
#         "coalesce(t.home_address,'') <> coalesce(s.home_address,'') "
#         "OR coalesce(t.commute_mode,'') <> coalesce(s.commute_mode,'')"
#     )

#     # DO NOT update the PK employee_id
#     # WHY: PK updates are bad practice and can cause edge-case issues in merges.
#     business_cols_no_pk = [c for c in BUSINESS_COLS if c != "employee_id"]

#     # Build update set (only business columns except PK)
#     update_set = {c: f"s.{c}" for c in business_cols_no_pk}
#     update_set.update({
#         "business_hash": "s.business_hash",
#         "updated_at": "current_timestamp()",
#         "created_at": "t.created_at",
#         # Preserve enrichment unless commute key changed
#         "distance_km": f"CASE WHEN {commute_key_changed} THEN NULL ELSE t.distance_km END",
#         "commute_valid_for_bonus": f"CASE WHEN {commute_key_changed} THEN NULL ELSE t.commute_valid_for_bonus END",
#         "commute_checked_at": f"CASE WHEN {commute_key_changed} THEN NULL ELSE t.commute_checked_at END",
#     })

#     insert_values = {c: f"s.{c}" for c in (BUSINESS_COLS + [
#         "business_hash", "created_at", "updated_at",
#         "distance_km", "commute_valid_for_bonus", "commute_checked_at"
#     ])}

#     (t.alias("t").merge(df_new.alias("s"), "t.employee_id = s.employee_id")
#       .whenMatchedUpdate(condition="t.business_hash <> s.business_hash", set=update_set)
#       .whenNotMatchedInsert(values=insert_values)
#       .execute()
#     )


# def main():
#     spark = build_spark_session()

#     print(f"[INFO] HR path: {HR_RAW_PATH}", flush=True)
#     print(f"[INFO] Sportive path: {SPORTIVE_RAW_PATH}", flush=True)
#     print(f"[INFO] Silver Delta path: {EMPLOYEE_REF_SILVER_PATH}", flush=True)

#     df_hr_raw = read_csv_strict(spark, HR_RAW_PATH, HR_SCHEMA)
#     df_sp_raw = read_csv_strict(spark, SPORTIVE_RAW_PATH, SPORTIVE_SCHEMA)

#     df_employee_ref = transform_to_employee_ref(df_hr_raw, df_sp_raw)

#     # Basic guard: employee_id must exist
#     if df_employee_ref.filter(F.col("employee_id").isNull()).limit(1).count() > 0:
#         raise RuntimeError("Found NULL employee_id in transformed employee_ref. Fix input parsing first.")

#     upsert_employee_ref(spark, df_employee_ref, EMPLOYEE_REF_SILVER_PATH)

#     print("[INFO] ✅ Silver employee_ref UPSERT completed.", flush=True)
#     spark.stop()


# if __name__ == "__main__":
#     main()

