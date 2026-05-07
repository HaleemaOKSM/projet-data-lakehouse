"""
=============================================================
  GOLD AGGREGATIONS JOB
  Reads Silver Iceberg tables → aggregates → writes Gold tables
  Catalog : Hive Metastore (thrift://hive-metastore:9083)
  Storage : MinIO S3a     (http://minio:9000)

  Gold tables produced (all required KPIs from project doc):
  1.  gold_ca_daily          — CA par jour
  2.  gold_ca_weekly         — CA par semaine
  3.  gold_ca_monthly        — CA par mois
  4.  gold_ca_by_canal       — CA par canal de vente
  5.  gold_top_products      — Top 10 produits les plus vendus
  6.  gold_sales_by_region   — Ventes par région/ville
  7.  gold_customer_basket   — Panier moyen par client
  8.  gold_return_rate       — Taux de retour par produit
  9.  gold_sales_by_category — Évolution ventes par catégorie
  10. gold_sales_by_segment  — Répartition par segment clientèle

  Run with:
  docker exec spark-master /opt/spark/bin/spark-submit \
    --master spark://spark-master:7077 \
    --executor-memory 2g \
    --driver-memory 2g \
    --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.3,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 \
    /opt/spark/jobs/gold_aggregations.py
=============================================================
"""
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import (
    col, sum, count, avg, round, rank, desc,
    weekofyear, month, year, date_format,
    lit, current_timestamp, when
)
from pyspark.sql.window import Window
import traceback

# ── Config ────────────────────────────────────────────
MINIO_ENDPOINT     = "http://minio:9000"
MINIO_ACCESS_KEY   = "minioadmin"
MINIO_SECRET_KEY   = "minioadmin123"
HIVE_METASTORE_URI = "thrift://hive-metastore:9083"
WAREHOUSE          = "s3a://lakehouse/"
SILVER_DB          = "lakehouse.silver"
GOLD_DB            = "lakehouse.gold"


def create_spark_session():
    return (
        SparkSession.builder
        .appName("GoldAggregations")
        .config("spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.catalog.lakehouse",
                "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.lakehouse.type",      "hive")
        .config("spark.sql.catalog.lakehouse.uri",       HIVE_METASTORE_URI)
        .config("spark.sql.catalog.lakehouse.warehouse", WAREHOUSE)
        .config("spark.hadoop.fs.s3a.endpoint",          MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key",        MINIO_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key",        MINIO_SECRET_KEY)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl",
                "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.sql.shuffle.partitions",          "4")
        .config("spark.sql.adaptive.enabled",            "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .enableHiveSupport()
        .getOrCreate()
    )


def write_gold(df, table_name, partition_col=None):
    """Write Gold DataFrame as Iceberg table."""
    df = (
        df
        .withColumn("_aggregated_at", current_timestamp())
        .withColumn("_layer",         lit("gold"))
        .repartition(4)
    )
    writer = (
        df.writeTo(table_name)
        .tableProperty("write.format.default",            "parquet")
        .tableProperty("write.parquet.compression-codec", "snappy")
        .tableProperty("format-version",                  "2")
        .tableProperty("write.target-file-size-bytes",    "67108864")
    )
    if partition_col:
        writer = writer.partitionedBy(partition_col)
    writer.createOrReplace()


# =============================================================
#  1. CA PAR JOUR — gold_ca_daily
#  KPI: chiffre d'affaires par jour
# =============================================================
def build_ca_daily(spark, ventes):
    print("\n  📊 Building gold_ca_daily...")
    df = (
        ventes
        .groupBy("date_vente")
        .agg(
            round(sum("montant"), 2).alias("ca_total"),
            count("vente_id").alias("nb_ventes"),
            round(avg("montant"), 2).alias("panier_moyen"),
            sum("quantite").alias("total_quantite")
        )
        .orderBy("date_vente")
    )
    write_gold(df, f"{GOLD_DB}.gold_ca_daily", partition_col="date_vente")
    rows = df.count()
    print(f"     ✅ gold_ca_daily ({rows} rows)")
    return rows


# =============================================================
#  2. CA PAR SEMAINE — gold_ca_weekly
#  KPI: chiffre d'affaires par semaine
# =============================================================
def build_ca_weekly(spark, ventes):
    print("\n  📊 Building gold_ca_weekly...")
    df = (
        ventes
        .withColumn("annee",   year("date_vente"))
        .withColumn("semaine", weekofyear("date_vente"))
        .groupBy("annee", "semaine")
        .agg(
            round(sum("montant"), 2).alias("ca_total"),
            count("vente_id").alias("nb_ventes"),
            round(avg("montant"), 2).alias("panier_moyen"),
            sum("quantite").alias("total_quantite")
        )
        .orderBy("annee", "semaine")
    )
    write_gold(df, f"{GOLD_DB}.gold_ca_weekly")
    rows = df.count()
    print(f"     ✅ gold_ca_weekly ({rows} rows)")
    return rows


# =============================================================
#  3. CA PAR MOIS — gold_ca_monthly
#  KPI: chiffre d'affaires par mois
# =============================================================
def build_ca_monthly(spark, ventes):
    print("\n  📊 Building gold_ca_monthly...")
    df = (
        ventes
        .withColumn("annee", year("date_vente"))
        .withColumn("mois",  month("date_vente"))
        .withColumn("mois_label", date_format(col("date_vente"), "yyyy-MM"))
        .groupBy("annee", "mois", "mois_label")
        .agg(
            round(sum("montant"), 2).alias("ca_total"),
            count("vente_id").alias("nb_ventes"),
            round(avg("montant"), 2).alias("panier_moyen"),
            sum("quantite").alias("total_quantite")
        )
        .orderBy("annee", "mois")
    )
    write_gold(df, f"{GOLD_DB}.gold_ca_monthly")
    rows = df.count()
    print(f"     ✅ gold_ca_monthly ({rows} rows)")
    return rows


# =============================================================
#  4. CA PAR CANAL — gold_ca_by_canal
#  KPI: chiffre d'affaires par canal de vente
# =============================================================
def build_ca_by_canal(spark, ventes):
    print("\n  📊 Building gold_ca_by_canal...")
    df = (
        ventes
        .groupBy("canal_id", "nom_canal")
        .agg(
            round(sum("montant"), 2).alias("ca_total"),
            count("vente_id").alias("nb_ventes"),
            round(avg("montant"), 2).alias("panier_moyen"),
            sum("quantite").alias("total_quantite"),
            round(sum("montant") / sum(sum("montant")).over(Window.orderBy(lit(1)).rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)) * 100, 2).alias("part_ca_pct")
        )
        .orderBy(desc("ca_total"))
    )
    write_gold(df, f"{GOLD_DB}.gold_ca_by_canal")
    rows = df.count()
    print(f"     ✅ gold_ca_by_canal ({rows} rows)")
    return rows


# =============================================================
#  5. TOP PRODUITS — gold_top_products
#  KPI: top 10 produits les plus vendus
# =============================================================
def build_top_products(spark, ventes, produits):
    print("\n  📊 Building gold_top_products...")
    df = (
        ventes
        .join(produits.select("produit_id", "nom", "categorie", "marque", "prix"),
              on="produit_id", how="left")
        .groupBy("produit_id", "nom", "categorie", "marque", "prix")
        .agg(
            sum("quantite").alias("total_quantite_vendue"),
            round(sum("montant"), 2).alias("ca_total"),
            count("vente_id").alias("nb_transactions"),
            round(avg("montant"), 2).alias("panier_moyen")
        )
        .orderBy(desc("total_quantite_vendue"))
        .limit(10)
        .withColumn("rang", rank().over(Window.orderBy(desc("total_quantite_vendue"))))
    )
    write_gold(df, f"{GOLD_DB}.gold_top_products")
    rows = df.count()
    print(f"     ✅ gold_top_products ({rows} rows — top 10)")
    return rows


# =============================================================
#  6. VENTES PAR RÉGION — gold_sales_by_region
#  KPI: ventes par région/ville
# =============================================================
def build_sales_by_region(spark, ventes, clients):
    print("\n  📊 Building gold_sales_by_region...")
    df = (
        ventes
        .join(clients.select("client_id", "ville", "region"),
              on="client_id", how="left")
        .groupBy("region", "ville")
        .agg(
            round(sum("montant"), 2).alias("ca_total"),
            count("vente_id").alias("nb_ventes"),
            round(avg("montant"), 2).alias("panier_moyen"),
            sum("quantite").alias("total_quantite")
        )
        .orderBy(desc("ca_total"))
    )
    write_gold(df, f"{GOLD_DB}.gold_sales_by_region")
    rows = df.count()
    print(f"     ✅ gold_sales_by_region ({rows} rows)")
    return rows


# =============================================================
#  7. PANIER MOYEN PAR CLIENT — gold_customer_basket
#  KPI: panier moyen par client
# =============================================================
def build_customer_basket(spark, ventes, clients):
    print("\n  📊 Building gold_customer_basket...")
    df = (
        ventes
        .join(clients.select("client_id", "nom", "prenom", "ville", "segment", "region"),
              on="client_id", how="left")
        .groupBy("client_id", "nom", "prenom", "ville", "segment", "region")
        .agg(
            count("vente_id").alias("nb_achats"),
            round(sum("montant"), 2).alias("ca_total_client"),
            round(avg("montant"), 2).alias("panier_moyen"),
            sum("quantite").alias("total_articles")
        )
        .orderBy(desc("ca_total_client"))
    )
    write_gold(df, f"{GOLD_DB}.gold_customer_basket")
    rows = df.count()
    print(f"     ✅ gold_customer_basket ({rows} rows)")
    return rows


# =============================================================
#  8. TAUX DE RETOUR — gold_return_rate
#  KPI: taux de retour par produit
# =============================================================
def build_return_rate(spark, ventes, retours, produits):
    print("\n  📊 Building gold_return_rate...")

    ventes_by_product = (
        ventes
        .groupBy("produit_id")
        .agg(
            count("vente_id").alias("nb_ventes"),
            round(sum("montant"), 2).alias("ca_total")
        )
    )

    retours_by_product = (
        retours
        .groupBy("produit_id")
        .agg(
            count("retour_id").alias("nb_retours"),
            round(sum("montant_retour"), 2).alias("montant_retours")
        )
    )

    df = (
        ventes_by_product
        .join(retours_by_product, on="produit_id", how="left")
        .join(produits.select("produit_id", "nom", "categorie", "marque"),
              on="produit_id", how="left")
        .withColumn("nb_retours", when(col("nb_retours").isNull(), lit(0)).otherwise(col("nb_retours")))
        .withColumn("montant_retours", when(col("montant_retours").isNull(), lit(0.0)).otherwise(col("montant_retours")))
        .withColumn("taux_retour_pct",
            round(col("nb_retours") / col("nb_ventes") * 100, 2)
        )
        .orderBy(desc("taux_retour_pct"))
    )
    write_gold(df, f"{GOLD_DB}.gold_return_rate")
    rows = df.count()
    print(f"     ✅ gold_return_rate ({rows} rows)")
    return rows


# =============================================================
#  9. VENTES PAR CATÉGORIE — gold_sales_by_category
#  KPI: évolution des ventes par catégorie
# =============================================================
def build_sales_by_category(spark, ventes):
    print("\n  📊 Building gold_sales_by_category...")
    df = (
        ventes
        .withColumn("annee",      year("date_vente"))
        .withColumn("mois",       month("date_vente"))
        .withColumn("mois_label", date_format(col("date_vente"), "yyyy-MM"))
        .groupBy("annee", "mois", "mois_label", "categorie_produit")
        .agg(
            round(sum("montant"), 2).alias("ca_total"),
            count("vente_id").alias("nb_ventes"),
            sum("quantite").alias("total_quantite")
        )
        .orderBy("annee", "mois", "categorie_produit")
    )
    write_gold(df, f"{GOLD_DB}.gold_sales_by_category")
    rows = df.count()
    print(f"     ✅ gold_sales_by_category ({rows} rows)")
    return rows


# =============================================================
#  10. VENTES PAR SEGMENT — gold_sales_by_segment
#  KPI: répartition des ventes par segment de clientèle
# =============================================================
def build_sales_by_segment(spark, ventes, clients):
    print("\n  📊 Building gold_sales_by_segment...")
    df = (
        ventes
        .join(clients.select("client_id", "segment"), on="client_id", how="left")
        .groupBy("segment")
        .agg(
            round(sum("montant"), 2).alias("ca_total"),
            count("vente_id").alias("nb_ventes"),
            round(avg("montant"), 2).alias("panier_moyen"),
            F.countDistinct("client_id").alias("nb_clients_actifs"),
            sum("quantite").alias("total_quantite")
        )
        .orderBy(desc("ca_total"))
    )
    write_gold(df, f"{GOLD_DB}.gold_sales_by_segment")
    rows = df.count()
    print(f"     ✅ gold_sales_by_segment ({rows} rows)")
    return rows


# =============================================================
#  MAIN
# =============================================================
def main():
    print("\n" + "="*55)
    print("  GOLD AGGREGATIONS — Starting")
    print(f"  Catalog : Hive Metastore → {HIVE_METASTORE_URI}")
    print(f"  Storage : MinIO          → {MINIO_ENDPOINT}")
    print("="*55)

    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    spark.sql("CREATE DATABASE IF NOT EXISTS lakehouse.gold")
    print("\n  ✅ Database 'gold' ready in Hive Metastore")

    # ── Load Silver tables once (reused across aggregations) ──
    print("\n  📂 Loading Silver tables...")
    ventes   = spark.table(f"{SILVER_DB}.silver_ventes").cache()
    clients  = spark.table(f"{SILVER_DB}.silver_clients").cache()
    produits = spark.table(f"{SILVER_DB}.silver_produits").cache()
    retours  = spark.table(f"{SILVER_DB}.silver_retours").cache()
    print("     ✅ Silver tables loaded")

    # ── Run all aggregations ───────────────────────────────────
    jobs = [
        ("ca_daily",          lambda: build_ca_daily(spark, ventes)),
        ("ca_weekly",         lambda: build_ca_weekly(spark, ventes)),
        ("ca_monthly",        lambda: build_ca_monthly(spark, ventes)),
        ("ca_by_canal",       lambda: build_ca_by_canal(spark, ventes)),
        ("top_products",      lambda: build_top_products(spark, ventes, produits)),
        ("sales_by_region",   lambda: build_sales_by_region(spark, ventes, clients)),
        ("customer_basket",   lambda: build_customer_basket(spark, ventes, clients)),
        ("return_rate",       lambda: build_return_rate(spark, ventes, retours, produits)),
        ("sales_by_category", lambda: build_sales_by_category(spark, ventes)),
        ("sales_by_segment",  lambda: build_sales_by_segment(spark, ventes, clients)),
    ]

    results = {}
    for name, job in jobs:
        try:
            count = job()
            results[name] = ("✅ OK", count)
        except Exception as e:
            print(f"\n  ❌ FAILED: {name} → {e}")
            traceback.print_exc()
            results[name] = ("❌ FAILED", 0)

    # ── Summary ───────────────────────────────────────────────
    print("\n" + "="*55)
    print("  GOLD AGGREGATIONS — Summary")
    print("="*55)
    for name, (status, count) in results.items():
        print(f"  {status}  gold_{name:<25} {count:>6} rows")

    print("\n  Tables registered in Hive Metastore (gold):")
    spark.sql("SHOW TABLES IN lakehouse.gold").show(truncate=False)

    spark.stop()
    print("\n🎉 Gold aggregations complete!")
    print("   MinIO    : s3a://lakehouse/gold.db/")
    print("   Metastore: thrift://hive-metastore:9083")
    print("   Next     : query via Trino → connect Superset")


if __name__ == "__main__":
    main()