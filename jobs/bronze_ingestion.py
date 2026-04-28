# jobs/bronze_ingestion.py
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("BronzeIngestion") \
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.iceberg.type", "hive") \
    .config("spark.sql.catalog.iceberg.uri", "thrift://hive-metastore:9083") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
    .getOrCreate()

# Créer la base de données Bronze
spark.sql("CREATE DATABASE IF NOT EXISTS iceberg.bronze")

# Lire et écrire chaque fichier CSV en table Iceberg Bronze
tables = ["ventes", "clients", "produits", "stocks", "retours", "canaux"]

for table in tables:
    df = spark.read.option("header", True).option("inferSchema", True) \
        .csv(f"s3a://lakehouse/raw/{table}.csv")
    
    df.writeTo(f"iceberg.bronze.bronze_{table}") \
      .tableProperty("write.format.default", "parquet") \
      .createOrReplace()
    
    print(f"Bronze {table} : {df.count()} lignes")

spark.stop()