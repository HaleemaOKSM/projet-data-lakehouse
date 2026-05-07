"""
=============================================================
  SILVER CLEANING JOB
  Reads Bronze Iceberg tables → cleans → writes Silver tables
  Catalog : Hive Metastore (thrift://hive-metastore:9083)
  Storage : MinIO S3a     (http://minio:9000)

  Treatments (as required by project document):
  1. Suppression des doublons
  2. Uniformisation des formats de dates
  3. Normalisation des codes produits
  4. Traitement des valeurs manquantes
  5. Enrichissement avec des dimensions métier
  + Extra: ville/région normalization for dashboard KPIs

  Run with:
  docker exec spark-master /opt/spark/bin/spark-submit \
    --master spark://spark-master:7077 \
    --executor-memory 2g \
    --driver-memory 2g \
    --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.3,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 \
    /opt/spark/jobs/silver_cleaning.py
=============================================================
"""
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import (
    col, trim, upper, lower, to_date, when, coalesce,
    lit, current_timestamp, regexp_replace, initcap,
    count, isnan, isnull
)
import traceback

# ── Config ────────────────────────────────────────────
MINIO_ENDPOINT     = "http://minio:9000"
MINIO_ACCESS_KEY   = "minioadmin"
MINIO_SECRET_KEY   = "minioadmin123"
HIVE_METASTORE_URI = "thrift://hive-metastore:9083"
WAREHOUSE          = "s3a://lakehouse/"
BRONZE_DB          = "lakehouse.bronze"
SILVER_DB          = "lakehouse.silver"


def create_spark_session():
    return (
        SparkSession.builder
        .appName("SilverCleaning")
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
        .config("spark.default.parallelism",             "4")
        .config("spark.sql.adaptive.enabled",            "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .enableHiveSupport()
        .getOrCreate()
    )


def write_silver(df, table_name, partition_col=None):
    """Write cleaned DataFrame as Iceberg Silver table."""
    # Add silver audit columns
    df = (
        df
        .withColumn("_cleaned_at", current_timestamp())
        .withColumn("_layer",      lit("silver"))
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
#  1. CLIENTS
#  - Suppression doublons sur client_id
#  - Traitement valeurs manquantes (ville, segment)
#  - Normalisation: ville en Title Case, segment en UPPER
#  - Uniformisation date_inscription
#  - Ajout région (enrichissement dimension métier)
# =============================================================
def clean_clients(spark):
    print("\n  🧹 Cleaning clients...")
    df = spark.table(f"{BRONZE_DB}.bronze_clients")
    bronze_count = df.count()

    df = (
        df
        # 1. Suppression des doublons
        .dropDuplicates(["client_id"])

        # 2. Traitement valeurs manquantes
        .withColumn("ville",   when(col("ville").isNull()   | (trim(col("ville"))   == ""), lit("Inconnu")).otherwise(col("ville")))
        .withColumn("segment", when(col("segment").isNull() | (trim(col("segment")) == ""), lit("Standard")).otherwise(col("segment")))
        .withColumn("nom",     when(col("nom").isNull()     | (trim(col("nom"))     == ""), lit("Inconnu")).otherwise(col("nom")))
        .withColumn("prenom",  when(col("prenom").isNull()  | (trim(col("prenom"))  == ""), lit("Inconnu")).otherwise(col("prenom")))

        # 3. Normalisation
        .withColumn("ville",   initcap(trim(col("ville"))))    # "casablanca" → "Casablanca"
        .withColumn("segment", upper(trim(col("segment"))))    # "premium" → "PREMIUM"
        .withColumn("nom",     upper(trim(col("nom"))))
        .withColumn("prenom",  initcap(trim(col("prenom"))))

        # 4. Uniformisation format date
        .withColumn("date_inscription", to_date(col("date_inscription")))

        # 5. Enrichissement: ajout région à partir de la ville
        # (useful for gold_sales_by_region KPI)
        .withColumn("region", 
            when(col("ville").isin("Casablanca","Mohammedia","Settat","El Jadida"), lit("Casablanca-Settat"))
            .when(col("ville").isin("Rabat","Salé","Kenitra","Skhirate"), lit("Rabat-Salé-Kénitra"))
            .when(col("ville").isin("Fès","Meknès","Ifrane","Taza"), lit("Fès-Meknès"))
            .when(col("ville").isin("Marrakech","Safi","Essaouira","Kelaa"), lit("Marrakech-Safi"))
            .when(col("ville").isin("Tanger","Tétouan","Al Hoceima","Larache"), lit("Tanger-Tétouan-Al Hoceima"))
            .when(col("ville").isin("Agadir","Tiznit","Taroudant","Ouarzazate"), lit("Souss-Massa"))
            .when(col("ville").isin("Oujda","Nador","Berkane","Taourirt"), lit("Oriental"))
            .otherwise(lit("Autres"))
        )

        # Drop bronze audit columns
        .drop("_ingested_at", "_source_file", "_layer")
    )

    silver_count = df.count()
    duplicates_removed = bronze_count - silver_count
    print(f"     Bronze: {bronze_count} rows → Silver: {silver_count} rows (removed {duplicates_removed} duplicates)")

    write_silver(df, f"{SILVER_DB}.silver_clients")
    print(f"     ✅ silver_clients written ({silver_count} rows)")
    return silver_count


# =============================================================
#  2. PRODUITS
#  - Suppression doublons sur produit_id
#  - Traitement valeurs manquantes (categorie, marque)
#  - Normalisation: codes produit, categorie en Title Case
#  - Prix négatifs ou nuls → remplacer par médiane
# =============================================================
def clean_produits(spark):
    print("\n  🧹 Cleaning produits...")
    df = spark.table(f"{BRONZE_DB}.bronze_produits")
    bronze_count = df.count()

    # Calculate median price for imputation
    median_price = df.approxQuantile("prix", [0.5], 0.01)[0]

    df = (
        df
        # 1. Suppression des doublons
        .dropDuplicates(["produit_id"])

        # 2. Traitement valeurs manquantes
        .withColumn("categorie", when(col("categorie").isNull() | (trim(col("categorie")) == ""), lit("Non classé")).otherwise(col("categorie")))
        .withColumn("marque",    when(col("marque").isNull()    | (trim(col("marque"))    == ""), lit("Inconnue")).otherwise(col("marque")))
        .withColumn("nom",       when(col("nom").isNull()       | (trim(col("nom"))       == ""), lit("Produit Inconnu")).otherwise(col("nom")))

        # 3. Normalisation des codes produit et catégories
        .withColumn("categorie", initcap(trim(col("categorie"))))  # "electronique" → "Electronique"
        .withColumn("marque",    initcap(trim(col("marque"))))
        .withColumn("nom",       trim(col("nom")))

        # 4. Prix invalides → remplacés par médiane (traitement valeurs manquantes)
        .withColumn("prix", when(
            col("prix").isNull() | (col("prix") <= 0),
            lit(median_price)
        ).otherwise(col("prix")))

        .drop("_ingested_at", "_source_file", "_layer")
    )

    silver_count = df.count()
    print(f"     Bronze: {bronze_count} rows → Silver: {silver_count} rows | Median price used for nulls: {median_price:.2f}")

    write_silver(df, f"{SILVER_DB}.silver_produits")
    print(f"     ✅ silver_produits written ({silver_count} rows)")
    return silver_count


# =============================================================
#  3. VENTES
#  - Suppression doublons sur vente_id
#  - Uniformisation date_vente
#  - Traitement valeurs manquantes (montant, quantite)
#  - Filtre lignes invalides (montant <= 0, quantite <= 0)
#  - Enrichissement: ajout nom_canal via jointure canaux
#  - Enrichissement: ajout categorie produit via jointure produits
#  - Partition par date_vente (pour performance analytique)
# =============================================================
def clean_ventes(spark):
    print("\n  🧹 Cleaning ventes...")
    df       = spark.table(f"{BRONZE_DB}.bronze_ventes")
    canaux   = spark.table(f"{BRONZE_DB}.bronze_canaux")
    produits = spark.table(f"{BRONZE_DB}.bronze_produits")
    bronze_count = df.count()

    df = (
        df
        # 1. Suppression des doublons
        .dropDuplicates(["vente_id"])

        # 2. Uniformisation format date
        .withColumn("date_vente", to_date(col("date_vente")))

        # 3. Traitement valeurs manquantes
        .withColumn("montant",  when(col("montant").isNull()  | (col("montant")  <= 0), lit(0.0)).otherwise(col("montant")))
        .withColumn("quantite", when(col("quantite").isNull() | (col("quantite") <= 0), lit(1))  .otherwise(col("quantite")))

        # 4. Filtre lignes invalides (montant nul après imputation = donnée corrompue)
        .filter(col("date_vente").isNotNull())
        .filter(col("client_id").isNotNull())
        .filter(col("produit_id").isNotNull())

        .drop("_ingested_at", "_source_file", "_layer")
    )

    # 5. Enrichissement: jointure avec canaux → nom_canal
    canaux_clean = canaux.select(
        col("canal_id"),
        upper(trim(col("nom"))).alias("nom_canal")
    )
    df = df.join(canaux_clean, on="canal_id", how="left")
    df = df.withColumn("nom_canal",
        when(col("nom_canal").isNull(), lit("INCONNU")).otherwise(col("nom_canal"))
    )

    # 6. Enrichissement: jointure avec produits → categorie
    produits_dim = produits.select(
        col("produit_id"),
        initcap(trim(col("categorie"))).alias("categorie_produit"),
        col("prix").alias("prix_unitaire")
    )
    df = df.join(produits_dim, on="produit_id", how="left")
    df = df.withColumn("categorie_produit",
        when(col("categorie_produit").isNull(), lit("Non classé")).otherwise(col("categorie_produit"))
    )

    silver_count = df.count()
    invalid = bronze_count - silver_count
    print(f"     Bronze: {bronze_count} rows → Silver: {silver_count} rows (removed {invalid} invalid rows)")

    # Partition by date_vente for analytical performance
    write_silver(df, f"{SILVER_DB}.silver_ventes", partition_col="date_vente")
    print(f"     ✅ silver_ventes written ({silver_count} rows) — partitioned by date_vente")
    return silver_count


# =============================================================
#  4. STOCKS
#  - Suppression doublons
#  - Traitement valeurs manquantes (depot, quantite)
#  - Quantités négatives → 0
#  - Normalisation: depot en Title Case
# =============================================================
def clean_stocks(spark):
    print("\n  🧹 Cleaning stocks...")
    df = spark.table(f"{BRONZE_DB}.bronze_stocks")
    bronze_count = df.count()

    df = (
        df
        # 1. Suppression des doublons
        .dropDuplicates(["produit_id", "depot"])

        # 2. Traitement valeurs manquantes
        .withColumn("depot", when(col("depot").isNull() | (trim(col("depot")) == ""), lit("Dépôt Inconnu")).otherwise(col("depot")))
        .withColumn("quantite_disponible", when(col("quantite_disponible").isNull(), lit(0)).otherwise(col("quantite_disponible")))

        # 3. Normalisation
        .withColumn("depot", initcap(trim(col("depot"))))

        # 4. Quantités négatives → 0
        .withColumn("quantite_disponible",
            when(col("quantite_disponible") < 0, lit(0)).otherwise(col("quantite_disponible"))
        )

        .drop("_ingested_at", "_source_file", "_layer")
    )

    silver_count = df.count()
    print(f"     Bronze: {bronze_count} rows → Silver: {silver_count} rows")

    write_silver(df, f"{SILVER_DB}.silver_stocks")
    print(f"     ✅ silver_stocks written ({silver_count} rows)")
    return silver_count


# =============================================================
#  5. RETOURS
#  - Suppression doublons sur retour_id
#  - Uniformisation date_retour
#  - Traitement valeurs manquantes (motif)
#  - Normalisation motif
#  - Enrichissement: jointure ventes → montant_retour, canal
# =============================================================
def clean_retours(spark):
    print("\n  🧹 Cleaning retours...")
    df     = spark.table(f"{BRONZE_DB}.bronze_retours")
    ventes = spark.table(f"{BRONZE_DB}.bronze_ventes")
    bronze_count = df.count()

    df = (
        df
        # 1. Suppression des doublons
        .dropDuplicates(["retour_id"])

        # 2. Uniformisation format date
        .withColumn("date_retour", to_date(col("date_retour")))

        # 3. Traitement valeurs manquantes
        .withColumn("motif", when(col("motif").isNull() | (trim(col("motif")) == ""), lit("Non spécifié")).otherwise(col("motif")))

        # 4. Normalisation du motif
        .withColumn("motif", initcap(trim(col("motif"))))

        .drop("_ingested_at", "_source_file", "_layer")
    )

    # 5. Enrichissement: jointure avec ventes → montant et canal_id
    ventes_dim = ventes.select(
        col("vente_id"),
        col("montant").alias("montant_retour"),
        col("canal_id"),
        col("client_id")
    ).drop("_ingested_at", "_source_file", "_layer")

    df = df.join(ventes_dim, on="vente_id", how="left")
    df = df.withColumn("montant_retour",
        when(col("montant_retour").isNull(), lit(0.0)).otherwise(col("montant_retour"))
    )

    silver_count = df.count()
    print(f"     Bronze: {bronze_count} rows → Silver: {silver_count} rows")

    write_silver(df, f"{SILVER_DB}.silver_retours")
    print(f"     ✅ silver_retours written ({silver_count} rows)")
    return silver_count


# =============================================================
#  6. CANAUX
#  - Suppression doublons
#  - Normalisation nom canal
# =============================================================
def clean_canaux(spark):
    print("\n  🧹 Cleaning canaux...")
    df = spark.table(f"{BRONZE_DB}.bronze_canaux")
    bronze_count = df.count()

    df = (
        df
        .dropDuplicates(["canal_id"])
        .withColumn("nom", upper(trim(col("nom"))))
        .withColumn("nom", when(col("nom").isNull() | (col("nom") == ""), lit("INCONNU")).otherwise(col("nom")))
        .drop("_ingested_at", "_source_file", "_layer")
    )

    silver_count = df.count()
    write_silver(df, f"{SILVER_DB}.silver_canaux")
    print(f"     ✅ silver_canaux written ({silver_count} rows)")
    return silver_count


# =============================================================
#  MAIN
# =============================================================
def main():
    print("\n" + "="*55)
    print("  SILVER CLEANING — Starting")
    print(f"  Catalog : Hive Metastore → {HIVE_METASTORE_URI}")
    print(f"  Storage : MinIO          → {MINIO_ENDPOINT}")
    print("="*55)

    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    spark.sql("CREATE DATABASE IF NOT EXISTS lakehouse.silver")
    print("\n  ✅ Database 'silver' ready in Hive Metastore")

    cleaners = [
        ("clients",  clean_clients),
        ("produits", clean_produits),
        ("ventes",   clean_ventes),
        ("stocks",   clean_stocks),
        ("retours",  clean_retours),
        ("canaux",   clean_canaux),
    ]

    results = {}
    for name, cleaner in cleaners:
        try:
            count = cleaner(spark)
            results[name] = ("✅ OK", count)
        except Exception as e:
            print(f"\n  ❌ FAILED: {name} → {e}")
            traceback.print_exc()
            results[name] = ("❌ FAILED", 0)

    print("\n" + "="*55)
    print("  SILVER CLEANING — Summary")
    print("="*55)
    total = 0
    for name, (status, count) in results.items():
        print(f"  {status}  silver_{name:<15} {count:>6} rows")
        total += count
    print(f"\n  Total: {total} rows across {len(cleaners)} tables")

    print("\n  Tables registered in Hive Metastore (silver):")
    spark.sql("SHOW TABLES IN lakehouse.silver").show(truncate=False)

    spark.stop()
    print("\n🎉 Silver cleaning complete!")
    print("   MinIO    : s3a://lakehouse/silver.db/")
    print("   Metastore: thrift://hive-metastore:9083")
    print("   Next     : run gold_aggregations.py")


if __name__ == "__main__":
    main()