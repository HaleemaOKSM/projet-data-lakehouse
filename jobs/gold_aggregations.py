"""
=============================================================
  GOLD AGGREGATIONS JOB
  Catalog : Hive Metastore (thrift://hive-metastore:9083)
  Storage : MinIO S3a     (http://minio:9000)

  Gold tables:
  ── Required (10 KPIs from project document) ──────────
  1.  gold_ca_daily
  2.  gold_ca_weekly
  3.  gold_ca_monthly
  4.  gold_ca_by_canal
  5.  gold_top_products
  6.  gold_sales_by_region
  7.  gold_customer_basket
  8.  gold_return_rate
  9.  gold_sales_by_category
  10. gold_sales_by_segment

  ── Added value (business intelligence) ───────────────
  11. gold_stock_alert        — produits en rupture/sur-stock
  12. gold_client_rfm         — segmentation RFM clients
  13. gold_canal_performance  — performance détaillée par canal/mois
  14. gold_product_affinity   — produits souvent achetés ensemble
  15. gold_daily_trend        — tendance CA + moyenne mobile 7j
=============================================================
"""
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import (
    col, sum, count, avg, round, rank, desc, asc,
    weekofyear, month, year, date_format, datediff,
    lit, current_timestamp, when, max, min,
    countDistinct, expr, lag, coalesce
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


# ═══════════════════════════════════════════════════════
#  REQUIRED KPIs
# ═══════════════════════════════════════════════════════

def build_ca_daily(spark, ventes):
    print("\n  📊 [1/15] gold_ca_daily")
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
    print(f"     ✅ {rows} rows")
    return rows


def build_ca_weekly(spark, ventes):
    print("\n  📊 [2/15] gold_ca_weekly")
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
    print(f"     ✅ {rows} rows")
    return rows


def build_ca_monthly(spark, ventes):
    print("\n  📊 [3/15] gold_ca_monthly")
    df = (
        ventes
        .withColumn("annee",      year("date_vente"))
        .withColumn("mois",       month("date_vente"))
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
    print(f"     ✅ {rows} rows")
    return rows


def build_ca_by_canal(spark, ventes):
    print("\n  📊 [4/15] gold_ca_by_canal")
    total_ca = ventes.agg(sum("montant").alias("total")).collect()[0]["total"]
    df = (
        ventes
        .groupBy("canal_id", "nom_canal")
        .agg(
            round(sum("montant"), 2).alias("ca_total"),
            count("vente_id").alias("nb_ventes"),
            round(avg("montant"), 2).alias("panier_moyen"),
            sum("quantite").alias("total_quantite")
        )
        .withColumn("part_ca_pct", round(col("ca_total") / total_ca * 100, 2))
        .orderBy(desc("ca_total"))
    )
    write_gold(df, f"{GOLD_DB}.gold_ca_by_canal")
    rows = df.count()
    print(f"     ✅ {rows} rows")
    return rows


def build_top_products(spark, ventes, produits):
    print("\n  📊 [5/15] gold_top_products")
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
    )
    w = Window.orderBy(desc("total_quantite_vendue"))
    df = df.withColumn("rang", rank().over(w))
    write_gold(df, f"{GOLD_DB}.gold_top_products")
    rows = df.count()
    print(f"     ✅ {rows} rows (top 10)")
    return rows


def build_sales_by_region(spark, ventes, clients):
    print("\n  📊 [6/15] gold_sales_by_region")
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
    print(f"     ✅ {rows} rows")
    return rows


def build_customer_basket(spark, ventes, clients):
    print("\n  📊 [7/15] gold_customer_basket")
    df = (
        ventes
        .join(clients.select("client_id", "nom", "prenom", "ville",
                             "segment", "region"),
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
    print(f"     ✅ {rows} rows")
    return rows


def build_return_rate(spark, ventes, retours, produits):
    print("\n  📊 [8/15] gold_return_rate")
    ventes_by_product = (
        ventes.groupBy("produit_id")
        .agg(
            count("vente_id").alias("nb_ventes"),
            round(sum("montant"), 2).alias("ca_total")
        )
    )
    retours_by_product = (
        retours.groupBy("produit_id")
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
        .withColumn("nb_retours",      coalesce(col("nb_retours"),      lit(0)))
        .withColumn("montant_retours", coalesce(col("montant_retours"), lit(0.0)))
        .withColumn("taux_retour_pct",
            round(col("nb_retours") / col("nb_ventes") * 100, 2))
        .orderBy(desc("taux_retour_pct"))
    )
    write_gold(df, f"{GOLD_DB}.gold_return_rate")
    rows = df.count()
    print(f"     ✅ {rows} rows")
    return rows


def build_sales_by_category(spark, ventes):
    print("\n  📊 [9/15] gold_sales_by_category")
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
    print(f"     ✅ {rows} rows")
    return rows


def build_sales_by_segment(spark, ventes, clients):
    print("\n  📊 [10/15] gold_sales_by_segment")
    df = (
        ventes
        .join(clients.select("client_id", "segment"), on="client_id", how="left")
        .groupBy("segment")
        .agg(
            round(sum("montant"), 2).alias("ca_total"),
            count("vente_id").alias("nb_ventes"),
            round(avg("montant"), 2).alias("panier_moyen"),
            countDistinct("client_id").alias("nb_clients_actifs"),
            sum("quantite").alias("total_quantite")
        )
        .orderBy(desc("ca_total"))
    )
    write_gold(df, f"{GOLD_DB}.gold_sales_by_segment")
    rows = df.count()
    print(f"     ✅ {rows} rows")
    return rows


# ═══════════════════════════════════════════════════════
#  ADDED VALUE KPIs
# ═══════════════════════════════════════════════════════

def build_stock_alert(spark, stocks, ventes, produits):
    """
    [11] STOCK ALERT
    Business value: operations team sees which products are
    out of stock or overstocked vs actual sales velocity.
    Columns: produit, stock_total, avg_daily_sales,
             days_of_stock_remaining, alert_level
    """
    print("\n  📊 [11/15] gold_stock_alert")

    # Total stock per product
    stock_total = (
        stocks
        .groupBy("produit_id")
        .agg(sum("quantite_disponible").alias("stock_total"))
    )

    # Average daily sales velocity
    nb_days = (
        ventes
        .agg(
            datediff(max("date_vente"), min("date_vente")).alias("nb_days")
        )
        .collect()[0]["nb_days"]
    )
    nb_days = max(nb_days, 1)

    sales_velocity = (
        ventes
        .groupBy("produit_id")
        .agg(sum("quantite").alias("total_vendu"))
        .withColumn("ventes_par_jour", round(col("total_vendu") / nb_days, 2))
    )

    df = (
        stock_total
        .join(sales_velocity, on="produit_id", how="left")
        .join(produits.select("produit_id", "nom", "categorie", "marque", "prix"),
              on="produit_id", how="left")
        .withColumn("ventes_par_jour", coalesce(col("ventes_par_jour"), lit(0.0)))
        .withColumn("total_vendu",     coalesce(col("total_vendu"),     lit(0)))
        # Days of stock remaining
        .withColumn("jours_de_stock",
            when(col("ventes_par_jour") > 0,
                 round(col("stock_total") / col("ventes_par_jour"), 0)
            ).otherwise(lit(9999))
        )
        # Alert classification
        .withColumn("alerte",
            when(col("stock_total") == 0,           lit("🔴 RUPTURE"))
            .when(col("jours_de_stock") <= 7,        lit("🟠 CRITIQUE"))
            .when(col("jours_de_stock") <= 30,       lit("🟡 FAIBLE"))
            .when(col("jours_de_stock") >= 365,      lit("🔵 SUR-STOCK"))
            .otherwise(                              lit("🟢 NORMAL"))
        )
        .withColumn("valeur_stock", round(col("stock_total") * col("prix"), 2))
        .orderBy("jours_de_stock")
    )

    write_gold(df, f"{GOLD_DB}.gold_stock_alert")
    rows = df.count()
    ruptures = df.filter(col("alerte") == "🔴 RUPTURE").count()
    critiques = df.filter(col("alerte") == "🟠 CRITIQUE").count()
    print(f"     ✅ {rows} rows | {ruptures} ruptures | {critiques} critiques")
    return rows


def build_client_rfm(spark, ventes, clients):
    """
    [12] CLIENT RFM SEGMENTATION
    Business value: marketing team identifies VIP, at-risk,
    and lost customers for targeted campaigns.
    RFM = Recency (days since last purchase),
          Frequency (nb of purchases),
          Monetary (total spend)
    """
    print("\n  📊 [12/15] gold_client_rfm")

    reference_date = ventes.agg(max("date_vente")).collect()[0][0]

    rfm_raw = (
        ventes
        .groupBy("client_id")
        .agg(
            datediff(lit(reference_date), max("date_vente")).alias("recence_jours"),
            count("vente_id").alias("frequence"),
            round(sum("montant"), 2).alias("montant_total")
        )
    )

    # Score each dimension 1-4 using quartiles
    r_quantiles = rfm_raw.approxQuantile("recence_jours", [0.25, 0.5, 0.75], 0.05)
    f_quantiles = rfm_raw.approxQuantile("frequence",     [0.25, 0.5, 0.75], 0.05)
    m_quantiles = rfm_raw.approxQuantile("montant_total", [0.25, 0.5, 0.75], 0.05)

    df = (
        rfm_raw
        # R score: lower recency = better = higher score
        .withColumn("r_score",
            when(col("recence_jours") <= r_quantiles[0], lit(4))
            .when(col("recence_jours") <= r_quantiles[1], lit(3))
            .when(col("recence_jours") <= r_quantiles[2], lit(2))
            .otherwise(lit(1))
        )
        # F score: higher frequency = better
        .withColumn("f_score",
            when(col("frequence") >= f_quantiles[2], lit(4))
            .when(col("frequence") >= f_quantiles[1], lit(3))
            .when(col("frequence") >= f_quantiles[0], lit(2))
            .otherwise(lit(1))
        )
        # M score: higher monetary = better
        .withColumn("m_score",
            when(col("montant_total") >= m_quantiles[2], lit(4))
            .when(col("montant_total") >= m_quantiles[1], lit(3))
            .when(col("montant_total") >= m_quantiles[0], lit(2))
            .otherwise(lit(1))
        )
        .withColumn("rfm_score", col("r_score") + col("f_score") + col("m_score"))
        # Segment label
        .withColumn("rfm_segment",
            when(col("rfm_score") >= 11,                    lit("💎 Champion"))
            .when((col("rfm_score") >= 9) & (col("r_score") >= 3), lit("⭐ Fidèle"))
            .when((col("rfm_score") >= 9) & (col("r_score") < 3),  lit("😴 A Risque"))
            .when(col("rfm_score") >= 7,                    lit("📈 Potentiel"))
            .when(col("r_score") >= 3,                      lit("🆕 Nouveau"))
            .otherwise(                                     lit("💤 Perdu"))
        )
        .join(clients.select("client_id", "nom", "prenom", "ville",
                             "segment", "region"),
              on="client_id", how="left")
        .orderBy(desc("rfm_score"))
    )

    write_gold(df, f"{GOLD_DB}.gold_client_rfm")
    rows = df.count()
    champions = df.filter(col("rfm_segment") == "💎 Champion").count()
    perdus    = df.filter(col("rfm_segment") == "💤 Perdu").count()
    print(f"     ✅ {rows} clients | {champions} champions | {perdus} perdus")
    return rows


def build_canal_performance(spark, ventes):
    """
    [13] CANAL PERFORMANCE PAR MOIS
    Business value: commercial team tracks which channel
    is growing or declining month over month.
    """
    print("\n  📊 [13/15] gold_canal_performance")

    monthly = (
        ventes
        .withColumn("annee",      year("date_vente"))
        .withColumn("mois",       month("date_vente"))
        .withColumn("mois_label", date_format(col("date_vente"), "yyyy-MM"))
        .groupBy("annee", "mois", "mois_label", "canal_id", "nom_canal")
        .agg(
            round(sum("montant"), 2).alias("ca_total"),
            count("vente_id").alias("nb_ventes"),
            round(avg("montant"), 2).alias("panier_moyen"),
            countDistinct("client_id").alias("nb_clients_uniques")
        )
    )

    # Month-over-month growth per canal
    w = Window.partitionBy("canal_id").orderBy("annee", "mois")
    df = (
        monthly
        .withColumn("ca_mois_precedent", lag("ca_total", 1).over(w))
        .withColumn("croissance_pct",
            when(col("ca_mois_precedent").isNotNull() & (col("ca_mois_precedent") > 0),
                 round((col("ca_total") - col("ca_mois_precedent"))
                       / col("ca_mois_precedent") * 100, 2)
            ).otherwise(lit(None))
        )
        .orderBy("annee", "mois", "nom_canal")
    )

    write_gold(df, f"{GOLD_DB}.gold_canal_performance")
    rows = df.count()
    print(f"     ✅ {rows} rows")
    return rows


def build_daily_trend(spark, ventes):
    """
    [14] DAILY TREND WITH 7-DAY MOVING AVERAGE
    Business value: executive dashboard shows smoothed
    revenue trend to spot seasonality and anomalies.
    """
    print("\n  📊 [14/15] gold_daily_trend")

    daily = (
        ventes
        .groupBy("date_vente")
        .agg(
            round(sum("montant"), 2).alias("ca_jour"),
            count("vente_id").alias("nb_ventes"),
            countDistinct("client_id").alias("nb_clients_actifs")
        )
        .orderBy("date_vente")
    )

    # 7-day rolling average
    w7 = (Window.orderBy(col("date_vente").cast("long"))
          .rowsBetween(-6, 0))
    # 30-day rolling average
    w30 = (Window.orderBy(col("date_vente").cast("long"))
           .rowsBetween(-29, 0))

    df = (
        daily
        .withColumn("moyenne_mobile_7j",  round(avg("ca_jour").over(w7),  2))
        .withColumn("moyenne_mobile_30j", round(avg("ca_jour").over(w30), 2))
        .withColumn("ca_cumule",          round(
            sum("ca_jour").over(Window.orderBy("date_vente")
                                .rowsBetween(Window.unboundedPreceding, 0)), 2))
    )

    write_gold(df, f"{GOLD_DB}.gold_daily_trend", partition_col="date_vente")
    rows = df.count()
    print(f"     ✅ {rows} rows (with 7j & 30j moving averages)")
    return rows


def build_product_affinity(spark, ventes):
    """
    [15] PRODUCT AFFINITY (frequently bought together)
    Business value: recommendation engine / cross-sell
    opportunities for marketing campaigns.
    Shows pairs of products bought by same client.
    """
    print("\n  📊 [15/15] gold_product_affinity")

    # Get all product pairs bought by the same client
    v1 = ventes.select(
        col("client_id"),
        col("produit_id").alias("produit_a"),
        col("categorie_produit").alias("categorie_a")
    )
    v2 = ventes.select(
        col("client_id"),
        col("produit_id").alias("produit_b"),
        col("categorie_produit").alias("categorie_b")
    )

    df = (
        v1.join(v2, on="client_id")
        # Only keep pairs where a < b to avoid duplicates
        .filter(col("produit_a") < col("produit_b"))
        .groupBy("produit_a", "categorie_a", "produit_b", "categorie_b")
        .agg(count("client_id").alias("nb_clients_communs"))
        .filter(col("nb_clients_communs") >= 2)
        .orderBy(desc("nb_clients_communs"))
        .limit(100)   # top 100 pairs
    )

    write_gold(df, f"{GOLD_DB}.gold_product_affinity")
    rows = df.count()
    print(f"     ✅ {rows} product pairs (top 100 affinities)")
    return rows


# ═══════════════════════════════════════════════════════
#  MAIN
# ═══════════════════════════════════════════════════════
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

    print("\n  📂 Loading Silver tables...")
    ventes   = spark.table(f"{SILVER_DB}.silver_ventes").cache()
    clients  = spark.table(f"{SILVER_DB}.silver_clients").cache()
    produits = spark.table(f"{SILVER_DB}.silver_produits").cache()
    retours  = spark.table(f"{SILVER_DB}.silver_retours").cache()
    stocks   = spark.table(f"{SILVER_DB}.silver_stocks").cache()
    print("     ✅ Silver tables loaded and cached")

    jobs = [
        # Required
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
        # Added value
        ("stock_alert",       lambda: build_stock_alert(spark, stocks, ventes, produits)),
        ("client_rfm",        lambda: build_client_rfm(spark, ventes, clients)),
        ("canal_performance", lambda: build_canal_performance(spark, ventes)),
        ("daily_trend",       lambda: build_daily_trend(spark, ventes)),
        ("product_affinity",  lambda: build_product_affinity(spark, ventes)),
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

    print("\n" + "="*55)
    print("  GOLD AGGREGATIONS — Summary")
    print("="*55)
    print(f"\n  {'─'*50}")
    print(f"  {'Required KPIs':}")
    print(f"  {'─'*50}")
    required = list(results.items())[:10]
    for name, (status, count) in required:
        print(f"  {status}  gold_{name:<25} {count:>6} rows")

    print(f"\n  {'─'*50}")
    print(f"  Added Value KPIs")
    print(f"  {'─'*50}")
    added = list(results.items())[10:]
    for name, (status, count) in added:
        print(f"  {status}  gold_{name:<25} {count:>6} rows")

    print("\n  Tables in Hive Metastore (gold):")
    spark.sql("SHOW TABLES IN lakehouse.gold").show(truncate=False)

    spark.stop()
    print("\n🎉 Gold aggregations complete! 15 tables ready.")
    print("   MinIO    : s3a://lakehouse/gold.db/")
    print("   Metastore: thrift://hive-metastore:9083")
    print("   Next     : configure Trino → connect Superset")


if __name__ == "__main__":
    main()