#!/bin/bash
# scripts/run_pipeline.sh
set -e
echo "=== Démarrage du pipeline Lakehouse ==="

echo "[1/3] Couche Bronze..."
docker exec spark spark-submit /opt/jobs/bronze_ingestion.py

echo "[2/3] Couche Silver..."
docker exec spark spark-submit /opt/jobs/silver_cleaning.py

echo "[3/3] Couche Gold..."
docker exec spark spark-submit /opt/jobs/gold_aggregations.py

echo "=== Pipeline terminé. Ouvrez Trino sur localhost:8080 ==="