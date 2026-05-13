#  Plateforme Big Data Lakehouse , Analyse des Ventes Omnicanales

> Projet Big Data  
> Architecture Lakehouse basée sur Apache Iceberg, Apache Spark, MinIO, Trino et Apache Superset

---

##  Table des matières

- [Contexte](#-contexte)
- [Architecture](#-architecture)
- [Stack technique](#-stack-technique)
- [Structure du projet](#-structure-du-projet)
- [Prérequis](#-prérequis)
- [Installation et démarrage](#-installation-et-démarrage)
- [Pipeline de données](#-pipeline-de-données)
- [Couches Lakehouse](#-couches-lakehouse)
- [Requêtes analytiques Trino](#-requêtes-analytiques-trino)
- [Dashboard Superset](#-dashboard-superset)
- [Indicateurs analytiques](#-indicateurs-analytiques)
- [Interfaces web](#-interfaces-web)
- [Dépannage](#-dépannage)

---

##  Contexte

Les entreprises modernes commercialisent leurs produits via plusieurs canaux : magasins physiques, sites e-commerce, applications mobiles, marketplaces et partenaires externes. Ces canaux génèrent des données hétérogènes provenant de plusieurs systèmes.

Ce projet conçoit et implémente une **plateforme Big Data de type Lakehouse** permettant d'intégrer, stocker, transformer et analyser les données de ventes omnicanales selon une logique **Bronze / Silver / Gold**.

---

## Architecture

```
Sources CSV (clients, produits, ventes, stocks, retours, canaux)
        │
        ▼
  PyAirbyte (ingestion)
        │
        ▼
  MinIO — raw/ (stockage objet S3)
        │
        ▼
  Apache Spark — bronze_ingestion.py
        │
        ▼
  Iceberg Bronze (données brutes)
        │
        ▼
  Apache Spark — silver_cleaning.py
        │
        ▼
  Iceberg Silver (données nettoyées)
        │
        ▼
  Apache Spark — gold_aggregations.py
        │
        ▼
  Iceberg Gold (tables analytiques)
        │
        ▼
  Trino (SQL distribué)
        │
        ▼
  Apache Superset (Dashboard BI)
```

---

## Stack technique

| Technologie | Version | Rôle |
|---|---|---|
| **PyAirbyte** | latest | Ingestion des données CSV vers MinIO |
| **Apache Spark** | 3.5 | Traitements Bronze / Silver / Gold |
| **Apache Iceberg** | 1.4.3 | Format de tables analytiques |
| **MinIO** | latest | Stockage objet compatible S3 |
| **Hive Metastore** | 3.1.3 | Catalogue des métadonnées Iceberg |
| **MySQL** | 8 | Base de métadonnées Hive |
| **Trino** | 435 | Moteur SQL distribué |
| **Apache Superset** | 3.1.0 | Visualisation et tableaux de bord |
| **Docker Compose** | v3.8 | Orchestration des services |

---

## Structure du projet

```
projet-data-lakehouse/
├── docker-compose.yml          # Orchestration de tous les services
├── .env                        # Variables d'environnement
├── README.md                   # Documentation du projet
│
├── trino/
│   ├── catalog/
│   │   └── iceberg.properties  # Configuration catalog Iceberg
│   └── config/
│       ├── config.properties   # Configuration Trino
│       └── jvm.config          # Configuration JVM Trino
│
├── hive/
│   └── conf/
│       └── hive-site.xml       # Configuration Hive Metastore → MinIO
│
├── spark/
│   └── conf/
│       └── spark-defaults.conf # Configuration Spark + Iceberg
│
├── data/                       # Fichiers CSV générés
│   ├── clients.csv             # 1 000 clients
│   ├── produits.csv            # 200 produits
│   ├── ventes.csv              # 50 000 ventes
│   ├── stocks.csv              # 200 entrées stocks
│   ├── retours.csv             # 2 000 retours
│   └── canaux.csv              # 4 canaux de vente
│
├── scripts/
│   ├── generate_data.py        # Génération des données synthétiques
│   ├── airbyte_ingestion.py    # Ingestion CSV → MinIO via PyAirbyte
│   └── run_pipeline.sh         # Script de lancement du pipeline complet
│
├── jobs/
│   ├── bronze_ingestion.py     # Spark : CSV raw → Iceberg Bronze
│   ├── silver_cleaning.py      # Spark : Bronze → Iceberg Silver
│   ├── gold_aggregations.py    # Spark : Silver → Iceberg Gold
│   └── verify_bronze.py        # Vérification des tables Bronze
│
└── dashboard/
    └── dashboard_export.zip    # Export du dashboard Superset
```

---

## Prérequis

- **Docker Desktop** avec au moins **10 Go RAM** alloués
- **Python 3.10+**
- **Git**

### Vérifier les ressources Docker Desktop
```
Docker Desktop → Settings → Resources → Memory → 10 GB minimum
```

### Installer les dépendances Python
```bash
pip install airbyte boto3 pandas faker
```

---

##  Installation et démarrage

### Étape 1 : Cloner le projet
```bash
git clone https://github.com/votre-repo/projet-data-lakehouse.git
cd projet-data-lakehouse
```

### Étape 2 : Démarrer la stack Docker
```bash
docker-compose up -d
```

Attendez que tous les services soient **healthy** (~2 minutes) :
```bash
docker-compose ps
```

### Étape 3 : Fixer le cache Ivy (à faire une fois après chaque démarrage)
```bash
docker exec -u root spark-master mkdir -p /home/spark/.ivy2/cache
docker exec -u root spark-master chmod -R 777 /home/spark/.ivy2
```

### Étape 4 : Ingestion des données vers MinIO
```bash
python scripts/airbyte_ingestion.py
```

Vérifiez que les 6 fichiers CSV sont dans MinIO : `http://localhost:9001` → Buckets → lakehouse → raw

### Étape 5 : Lancer le pipeline Spark complet

#### Couche Bronze
```bash
docker exec spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --executor-memory 2g \
  --driver-memory 2g \
  --packages "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.3,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262" \
  /opt/spark/jobs/bronze_ingestion.py
```

#### Couche Silver
```bash
docker exec spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --executor-memory 2g \
  --driver-memory 2g \
  --packages "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.3,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262" \
  /opt/spark/jobs/silver_cleaning.py
```

#### Couche Gold
```bash
docker exec spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --executor-memory 2g \
  --driver-memory 2g \
  --packages "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.3,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262" \
  /opt/spark/jobs/gold_aggregations.py
```

### Étape 6 : Configurer Superset

#### Installer le driver Trino
```bash
docker exec -u root superset pip install trino[sqlalchemy]
docker restart superset
```

#### Importer le dashboard
```
http://localhost:8088 → Dashboards → Import Dashboard → exports/dashboard_export.zip
```

---

##  Pipeline de données

### Données générées (synthétiques)

| Fichier | Lignes | Description |
|---|---|---|
| clients.csv | 1 000 | Clients avec ville, segment, date inscription |
| produits.csv | 200 | Produits avec catégorie, prix, marque |
| ventes.csv | 50 000 | Transactions sur 2 ans, 4 canaux |
| stocks.csv | 200 | Stocks par dépôt |
| retours.csv | 2 000 | Retours avec motif (~4% du volume) |
| canaux.csv | 4 | magasin, web, mobile, marketplace |

---

##  Couches Lakehouse

### Bronze : Données brutes
Stockées dans `s3a://lakehouse/bronze.db/`

| Table | Description |
|---|---|
| bronze_clients | Clients bruts depuis CSV |
| bronze_produits | Produits bruts depuis CSV |
| bronze_ventes | Ventes brutes depuis CSV |
| bronze_stocks | Stocks bruts depuis CSV |
| bronze_retours | Retours bruts depuis CSV |
| bronze_canaux | Canaux bruts depuis CSV |

### Silver : Données nettoyées
Stockées dans `s3a://lakehouse/silver.db/`

Traitements appliqués :
- ✅ Suppression des doublons
- ✅ Uniformisation des formats de dates
- ✅ Normalisation des codes produits
- ✅ Traitement des valeurs manquantes
- ✅ Enrichissement avec dimensions métier
- ✅ Ajout colonne `region` depuis `ville`
- ✅ Jointures entre ventes, produits et canaux

### Gold : Tables analytiques
Stockées dans `s3a://lakehouse/gold.db/`

| Table | Indicateur |
|---|---|
| gold_ca_daily | CA par jour |
| gold_ca_monthly | CA par mois |
| gold_ca_weekly | CA par semaine |
| gold_ca_by_canal | CA par canal de vente |
| gold_top_products | Top 10 produits |
| gold_customer_basket | Panier moyen par client |
| gold_return_rate | Taux de retour par produit |
| gold_sales_by_category | Évolution par catégorie |
| gold_sales_by_segment | Répartition par segment |
| gold_canal_performance | Performance détaillée canal |
| gold_client_rfm | Analyse RFM clients |
| gold_daily_trend | Tendance quotidienne |

---

## Requêtes analytiques Trino

### Accéder au shell Trino
```bash
docker exec -it trino trino
```

### Vérifier les tables disponibles
```sql
SHOW SCHEMAS FROM iceberg;
SHOW TABLES FROM iceberg.gold;
```

### CA par jour
```sql
SELECT date_vente, ROUND(ca_total, 2) AS ca_jour, nb_ventes
FROM iceberg.gold.gold_ca_daily
ORDER BY date_vente DESC LIMIT 30;
```

### CA par mois
```sql
SELECT annee, mois, mois_label, ROUND(ca_total, 2) AS ca_mensuel, nb_ventes
FROM iceberg.gold.gold_ca_monthly
ORDER BY annee DESC, mois DESC;
```

### CA par canal
```sql
SELECT nom_canal, ROUND(ca_total, 2) AS ca_total,
       nb_ventes, ROUND(part_ca_pct, 2) AS part_pct
FROM iceberg.gold.gold_ca_by_canal
ORDER BY ca_total DESC;
```

### Top 10 produits
```sql
SELECT rang, nom, categorie, total_quantite_vendue, ROUND(ca_total, 2)
FROM iceberg.gold.gold_top_products
WHERE rang <= 10 ORDER BY rang;
```

### Taux de retour
```sql
SELECT nom, categorie, nb_ventes, nb_retours,
       ROUND(taux_retour_pct, 2) AS taux_retour_pct
FROM iceberg.gold.gold_return_rate
ORDER BY taux_retour_pct DESC LIMIT 20;
```

### Vue exécutive globale
```sql
SELECT 'CA Total' AS indicateur, CAST(ROUND(SUM(ca_total), 2) AS VARCHAR) AS valeur
FROM iceberg.gold.gold_ca_monthly
UNION ALL
SELECT 'Nb Ventes', CAST(SUM(nb_ventes) AS VARCHAR)
FROM iceberg.gold.gold_ca_monthly
UNION ALL
SELECT 'Panier Moyen', CAST(ROUND(AVG(panier_moyen), 2) AS VARCHAR)
FROM iceberg.gold.gold_customer_basket
UNION ALL
SELECT 'Taux Retour %', CAST(ROUND(AVG(taux_retour_pct), 2) AS VARCHAR)
FROM iceberg.gold.gold_return_rate;
```

---

##  Dashboard Superset

### Connexion Trino dans Superset
```
Settings → Database Connections → + Database
SQLAlchemy URI : trino://admin@trino:8080/iceberg
```

### Charts disponibles

| Chart | Type | Dataset |
|---|---|---|
| CA Total | Big Number | gold_ca_monthly |
| Nombre de Ventes | Big Number | gold_ca_monthly |
| Panier Moyen | Big Number | gold_customer_basket |
| Taux de Retour % | Big Number | gold_return_rate |
| Évolution CA journalier | Line Chart | gold_ca_daily |
| CA mensuel par année | Bar Chart | gold_ca_monthly |
| Répartition CA par canal | Pie Chart | gold_ca_by_canal |
| Performance par canal | Bar Chart | gold_ca_by_canal |
| Top 10 produits | Bar Chart | gold_top_products |
| CA par région et ville | Table | gold_customer_basket |
| Panier moyen par segment | Bar Chart | gold_customer_basket |
| Taux de retour par produit | Table | gold_return_rate |
| Évolution par catégorie | Line Chart | gold_sales_by_category |
| Répartition par segment | Pie Chart | gold_sales_by_segment |
| Clients actifs par segment | Bar Chart | gold_sales_by_segment |


##  Indicateurs analytiques

| # | Indicateur | Table Gold | Statut |
|---|---|---|---|
| 1 | CA par jour | gold_ca_daily | ✅ |
| 2 | CA par semaine | gold_ca_weekly | ✅ |
| 3 | CA par mois | gold_ca_monthly | ✅ |
| 4 | CA par canal de vente | gold_ca_by_canal | ✅ |
| 5 | Top 10 produits les plus vendus | gold_top_products | ✅ |
| 6 | Ventes par région / ville | gold_customer_basket | ✅ |
| 7 | Panier moyen par client | gold_customer_basket | ✅ |
| 8 | Taux de retour par produit | gold_return_rate | ✅ |
| 9 | Évolution des ventes par catégorie | gold_sales_by_category | ✅ |
| 10 | Répartition par segment de clientèle | gold_sales_by_segment | ✅ |

<img width="1905" height="916" alt="Screenshot 2026-05-08 220858" src="https://github.com/user-attachments/assets/be297a71-6055-4e34-85b3-2ea41f41e1ff" />
<img width="1888" height="932" alt="Screenshot 2026-05-08 220912" src="https://github.com/user-attachments/assets/859dead2-ec22-4451-95fe-68bb84ad614f" />
<img width="1901" height="908" alt="Screenshot 2026-05-08 220941" src="https://github.com/user-attachments/assets/27104795-5ab2-408a-8de5-9636ca76aff1" />
<img width="1901" height="578" alt="Screenshot 2026-05-08 221002" src="https://github.com/user-attachments/assets/eadef518-0142-4704-843b-83052ac35b84" />

---

##  Interfaces web

| Service | URL | Identifiants |
|---|---|---|
| **MinIO Console** | http://localhost:9001 | minioadmin / minioadmin123 |
| **Trino UI** | http://localhost:8080 | aucun login |
| **Spark UI** | http://localhost:8081 | aucun login |
| **Superset** | http://localhost:8088 | admin / admin123 |

---

##  Dépannage

### Erreur : Ivy cache FileNotFoundException
```bash
docker exec -u root spark-master mkdir -p /home/spark/.ivy2/cache
docker exec -u root spark-master chmod -R 777 /home/spark/.ivy2
```

### Erreur : Table not found dans Silver/Gold
Les tables Bronze ont été perdues après redémarrage — relancez Bronze d'abord :
```bash
# Relancer bronze_ingestion.py puis silver_cleaning.py puis gold_aggregations.py
```

### Erreur : Could not load database driver TrinoEngineSpec
```bash
docker exec -u root superset pip install trino[sqlalchemy]
docker restart superset
```

### Erreur : MinIO unreachable depuis Superset/Spark
Vérifiez que tous les services sont sur le même réseau Docker :
```bash
docker-compose ps
docker network ls
```

### Vérifier les logs d'un service
```bash
docker logs minio --tail 20
docker logs hive-metastore --tail 30
docker logs trino --tail 20
docker logs spark-master --tail 20
```

### Redémarrer proprement (repart de zéro)
```bash
docker-compose down -v   # Supprime aussi les volumes
docker-compose up -d
```

---

## Équipe

ID OUAKSIM Halima et 
MOUTAWAKIL Latifa
**II-BDCC**

