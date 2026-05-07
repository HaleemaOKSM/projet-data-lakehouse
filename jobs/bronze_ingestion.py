"""
=============================================================
  BRONZE INGESTION JOB
  Reads raw CSVs from MinIO (raw/) and writes them as
  Apache Iceberg tables into the Bronze layer (bronze/)
  Catalog : Hive Metastore (thrift://hive-metastore:9083)
  Storage : MinIO S3a     (http://minio:9000)

  Run with:
  docker exec spark-master /opt/spark/bin/spark-submit \
    --master spark://spark-master:7077 \
    --executor-memory 2g \
    --driver-memory 2g \
    --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.3,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 \
    /opt/spark/jobs/bronze_ingestion.py
=============================================================
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, lit
import traceback

# ── Config ────────────────────────────────────────────
MINIO_ENDPOINT     = "http://minio:9000"
MINIO_ACCESS_KEY   = "minioadmin"
MINIO_SECRET_KEY   = "minioadmin123"
HIVE_METASTORE_URI = "thrift://hive-metastore:9083"
RAW_BASE           = "s3a://lakehouse/raw"
WAREHOUSE          = "s3a://lakehouse/"

# ── Tables: (name, csv_file, partition_col) ───────────
# NOTE: partition only in Silver/Gold, not Bronze
# Bronze = raw data as-is, no transformation
TABLES = [
    ("clients",  "clients.csv",  None),
    ("produits", "produits.csv", None),
    ("ventes",   "ventes.csv",   None),   # no partition in bronze
    ("stocks",   "stocks.csv",   None),
    ("retours",  "retours.csv",  None),
    ("canaux",   "canaux.csv",   None),
]


def create_spark_session():
    return (
        SparkSession.builder
        .appName("BronzeIngestion")
        # ── Iceberg ───────────────────────────────────
        .config("spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.catalog.lakehouse",
                "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.lakehouse.type",      "hive")
        .config("spark.sql.catalog.lakehouse.uri",       HIVE_METASTORE_URI)
        .config("spark.sql.catalog.lakehouse.warehouse", WAREHOUSE)
        # ── MinIO S3a ─────────────────────────────────
        .config("spark.hadoop.fs.s3a.endpoint",          MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key",        MINIO_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key",        MINIO_SECRET_KEY)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl",
                "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        # ── Memory & performance tuning ───────────────
        .config("spark.sql.shuffle.partitions",          "4")
        .config("spark.default.parallelism",             "4")
        .config("spark.sql.adaptive.enabled",            "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.sql.files.maxPartitionBytes",     "67108864")
        .config("spark.memory.fraction",                 "0.8")
        .config("spark.memory.storageFraction",          "0.3")
        .enableHiveSupport()
        .getOrCreate()
    )


def ingest_table(spark, name, filename, partition_col):
    raw_path   = f"{RAW_BASE}/{name}/{filename}"
    table_name = f"lakehouse.bronze.bronze_{name}"

    print(f"\n{'─'*55}")
    print(f"  📥  {name}")
    print(f"  Source : {raw_path}")
    print(f"  Target : {table_name}")
    print(f"{'─'*55}")

    df = (
        spark.read
        .option("header",      "true")
        .option("inferSchema", "true")
        .option("encoding",    "UTF-8")
        .csv(raw_path)
        .repartition(4)   # keep partitions small → avoid OOM
    )

    row_count = df.count()
    print(f"  ✅ Read {row_count} rows")
    df.printSchema()

    df = (
        df
        .withColumn("_ingested_at", current_timestamp())
        .withColumn("_source_file", lit(filename))
        .withColumn("_layer",       lit("bronze"))
    )

    writer = (
        df.writeTo(table_name)
        .tableProperty("write.format.default",            "parquet")
        .tableProperty("write.parquet.compression-codec", "snappy")
        .tableProperty("format-version",                  "2")
        .tableProperty("write.target-file-size-bytes",    "67108864")
    )

    if partition_col and partition_col in df.columns:
        writer = writer.partitionedBy(partition_col)
        print(f"  📂 Partitioned by: {partition_col}")

    writer.createOrReplace()
    print(f"  ✅ Done → {table_name} ({row_count} rows)")
    return row_count


def main():
    print("\n" + "="*55)
    print("  BRONZE INGESTION — Starting")
    print(f"  Catalog : Hive Metastore → {HIVE_METASTORE_URI}")
    print(f"  Storage : MinIO          → {MINIO_ENDPOINT}")
    print("="*55)

    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    spark.sql("CREATE DATABASE IF NOT EXISTS lakehouse.bronze")
    print("\n  ✅ Database 'bronze' ready in Hive Metastore")

    results = {}
    for name, filename, partition_col in TABLES:
        try:
            count = ingest_table(spark, name, filename, partition_col)
            results[name] = ("✅ OK", count)
        except Exception as e:
            print(f"\n  ❌ FAILED: {name} → {e}")
            traceback.print_exc()
            results[name] = ("❌ FAILED", 0)

    print("\n" + "="*55)
    print("  BRONZE INGESTION — Summary")
    print("="*55)
    total = 0
    for name, (status, count) in results.items():
        print(f"  {status}  bronze_{name:<15} {count:>6} rows")
        total += count
    print(f"\n  Total: {total} rows across {len(TABLES)} tables")

    print("\n  Tables registered in Hive Metastore:")
    spark.sql("SHOW TABLES IN lakehouse.bronze").show(truncate=False)

    spark.stop()
    print("\n🎉 Bronze ingestion complete!")
    print("   MinIO    : s3a://lakehouse/bronze/")
    print("   Metastore: thrift://hive-metastore:9083")


if __name__ == "__main__":
    main()