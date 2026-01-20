# src/bronze_batch.py
# src/bronze_batch.py

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    current_timestamp,
    input_file_name,
    lit,
    to_date
)

def ingest_to_bronze(
    spark,
    source_name,
    ingest_type,
    landing_path,
    bronze_path
):
    """
    Generic Bronze ingestion:
    - Reads CSV as-is (no enforced schema)
    - Appends metadata columns
    - Writes append-only Parquet
    """

    df = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(landing_path)
    )

    if df.rdd.isEmpty():
        print(f"[INFO] No new data for {source_name}")
        return

    df_bronze = (
        df
        .withColumn("ingest_timestamp", current_timestamp())
        .withColumn("ingest_date", to_date("ingest_timestamp"))
        .withColumn("source_system", lit(source_name))
        .withColumn("ingest_type", lit(ingest_type))
        .withColumn("source_file", input_file_name())
    )

    (
        df_bronze.write
        .mode("append")
        .partitionBy("ingest_date")
        .parquet(bronze_path)
    )

    print(f"[OK] Bronze ingestion completed for {source_name}")

def main():
    spark = (
        SparkSession.builder
        .appName("bronze_batch_ingestion")
        .master("local[*]")
        .getOrCreate()
    )

    # -------- CREDIT EVENTS (FACT) --------
    ingest_to_bronze(
        spark=spark,
        source_name="credit_events",
        ingest_type="FACT",
        landing_path="data/landing/credit_events",
        bronze_path="data/bronze/credit_events"
    )

    # -------- REGION REFERENCE (DIMENSION) --------
    ingest_to_bronze(
        spark=spark,
        source_name="region_reference",
        ingest_type="DIMENSION",
        landing_path="data/landing/region_reference",
        bronze_path="data/bronze/region_reference"
    )

    spark.stop()

if __name__ == "__main__":
    main()
