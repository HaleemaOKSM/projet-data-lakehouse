import boto3
import os
import pandas as pd
from pathlib import Path

# ── Config ──────────────────────────────────────────
MINIO_ENDPOINT   = "http://localhost:9000"
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin123"
BUCKET           = "lakehouse"

# ── Resolve data directory ───────────────────────────
SCRIPT_DIR = Path(__file__).parent
DATA_DIR   = SCRIPT_DIR.parent / "data"

# ── Files to ingest ──────────────────────────────────
files = {
    "clients":  "clients.csv",
    "produits": "produits.csv",
    "ventes":   "ventes.csv",
    "stocks":   "stocks.csv",
    "retours":  "retours.csv",
    "canaux":   "canaux.csv",
}

# ── MinIO client ─────────────────────────────────────
print("🔌 Connecting to MinIO...")
s3 = boto3.client(
    "s3",
    endpoint_url=MINIO_ENDPOINT,
    aws_access_key_id=MINIO_ACCESS_KEY,
    aws_secret_access_key=MINIO_SECRET_KEY,
    region_name="us-east-1",
)

# ── Test connection ───────────────────────────────────
try:
    s3.list_buckets()
    print("✅ MinIO connection OK\n")
except Exception as e:
    print(f"❌ Cannot connect to MinIO: {e}")
    print("   Make sure your docker-compose stack is running (docker-compose up -d)")
    exit(1)

# ── Ingest each file ──────────────────────────────────
success = 0
for name, filename in files.items():
    filepath = DATA_DIR / filename

    if not filepath.exists():
        print(f"⚠️  File not found: {filepath} — skipping")
        continue

    try:
        # Read with pandas (Airbyte CDK uses pandas under the hood for file sources)
        df = pd.read_csv(filepath)
        rows = len(df)

        # Upload raw CSV to MinIO
        dest_key = f"raw/{name}/{filename}"
        s3.upload_file(str(filepath), BUCKET, dest_key)

        print(f"✅ {filename:20s} → {rows:6d} rows → s3://lakehouse/{dest_key}")
        success += 1

    except Exception as e:
        print(f"❌ Error on {filename}: {e}")

print(f"\n{'='*55}")
print(f"🎉 Ingestion complete: {success}/{len(files)} files uploaded to MinIO")
print(f"{'='*55}")
print(f"\n📂 Verify at: http://localhost:9001")
print(f"   Bucket: lakehouse → raw → (clients, produits, ventes, stocks, retours, canaux)")