# jobs/silver_cleaning.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, upper, trim, when, coalesce, lit

spark = SparkSession.builder.appName("SilverCleaning") \
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.iceberg.type", "hive") \
    .config("spark.sql.catalog.iceberg.uri", "thrift://hive-metastore:9083") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
    .getOrCreate()

spark.sql("CREATE DATABASE IF NOT EXISTS iceberg.silver")

# Silver Ventes — nettoyage principal
df_ventes = spark.table("iceberg.bronze.bronze_ventes")
df_clients = spark.table("iceberg.bronze.bronze_clients")
df_produits = spark.table("iceberg.bronze.bronze_produits")
df_canaux = spark.table("iceberg.bronze.bronze_canaux")

silver_ventes = df_ventes \
    .dropDuplicates(["vente_id"]) \
    .filter(col("montant") > 0) \
    .filter(col("quantite") > 0) \
    .withColumn("date_vente", to_date(col("date_vente"))) \
    .join(df_clients.select("client_id","ville","segment"), "client_id", "left") \
    .join(df_produits.select("produit_id","categorie","prix"), "produit_id", "left") \
    .join(df_canaux.select("canal_id","nom").withColumnRenamed("nom","canal"), "canal_id", "left") \
    .na.fill({"segment": "inconnu", "categorie": "autre"})

silver_ventes.writeTo("iceberg.silver.silver_ventes").createOrReplace()
print(f"Silver ventes : {silver_ventes.count()} lignes")

spark.stop()