import os
from datetime import date, timedelta

import pandas as pd
from dotenv import load_dotenv
from minio import Minio
from minio.error import S3Error
from sqlalchemy import create_engine, text

# ==========================================================
# CONFIG
# ==========================================================

load_dotenv()

RETENTION_DAYS = 7

POSTGRES_HOST = os.getenv("DB_HOST", "localhost")
POSTGRES_PORT = int(os.getenv("DB_PORT", 5432))
POSTGRES_DB = os.getenv("DB_NAME")
POSTGRES_USER = os.getenv("DB_USER")
POSTGRES_PASSWORD = os.getenv("DB_PASSWORD")

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT")
MINIO_ACCESS_KEY = os.getenv("MINIO_USER")
MINIO_SECRET_KEY = os.getenv("MINIO_PASSWORD")
BUCKET_NAME = os.getenv("MINIO_BUCKET")

EXPORT_DIR = "./exports"
os.makedirs(EXPORT_DIR, exist_ok=True)

print("=" * 60)
print("AIS PARTITION ARCHIVAL")
print("=" * 60)

# ==========================================================
# CONNECTIONS
# ==========================================================

engine = create_engine(
    f"postgresql+psycopg2://{POSTGRES_USER}:{POSTGRES_PASSWORD}"
    f"@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
)

minio_client = Minio(
    MINIO_ENDPOINT,
    access_key=MINIO_ACCESS_KEY,
    secret_key=MINIO_SECRET_KEY,
    secure=False
)

# ==========================================================
# TARGET PARTITION
# ==========================================================

archive_date = date.today() - timedelta(days=RETENTION_DAYS + 1)

partition_name = (
    f"vessel_positions_{archive_date.strftime('%Y_%m_%d')}"
)

year = archive_date.strftime("%Y")
month = archive_date.strftime("%m")
day = archive_date.strftime("%d")

object_path = (
    f"year={year}/"
    f"month={month}/"
    f"day={day}/"
    f"{partition_name}.parquet"
)

print(f"Target Partition : {partition_name}")

# ==========================================================
# CHECK PARTITION EXISTS
# ==========================================================

with engine.begin() as conn:

    partition_exists = conn.execute(
        text("""
            SELECT EXISTS (
                SELECT 1
                FROM pg_tables
                WHERE schemaname = 'public'
                AND tablename = :partition_name
            )
        """),
        {
            "partition_name": partition_name
        }
    ).scalar()

if not partition_exists:
    print(f"Partition does not exist: {partition_name}")
    raise SystemExit()

# ==========================================================
# CHECK ARCHIVE REGISTRY
# ==========================================================

with engine.begin() as conn:

    already_archived = conn.execute(
        text("""
            SELECT 1
            FROM archived_partitions
            WHERE partition_name = :partition_name
        """),
        {
            "partition_name": partition_name
        }
    ).fetchone()

if already_archived:
    print("Partition already archived.")
    raise SystemExit()

# ==========================================================
# READ PARTITION
# ==========================================================

query = f"""
SELECT *
FROM {partition_name};
"""

print("Reading partition...")

df = pd.read_sql(query, engine)

row_count = len(df)

if row_count == 0:
    print("Partition contains zero rows.")
    raise SystemExit()

print(f"Rows Found      : {row_count:,}")

# ==========================================================
# WRITE PARQUET
# ==========================================================

parquet_file = os.path.join(
    EXPORT_DIR,
    f"{partition_name}.parquet"
)

print("Creating parquet file...")

df.to_parquet(
    parquet_file,
    engine="pyarrow",
    compression="snappy",
    index=False
)

print(f"Parquet Created : {parquet_file}")

# ==========================================================
# UPLOAD TO MINIO
# ==========================================================

try:

    minio_client.stat_object(
        BUCKET_NAME,
        object_path
    )

    print("Object already exists in MinIO.")

except S3Error:

    print("Uploading to MinIO...")

    minio_client.fput_object(
        BUCKET_NAME,
        object_path,
        parquet_file
    )

    print(f"Uploaded        : {object_path}")

# ==========================================================
# VERIFY UPLOAD
# ==========================================================

uploaded_object = minio_client.stat_object(
    BUCKET_NAME,
    object_path
)

if uploaded_object.size <= 0:
    raise RuntimeError(
        "Upload verification failed. Partition will NOT be dropped."
    )

print(
    f"Verified Upload : {uploaded_object.size:,} bytes"
)

# ==========================================================
# REGISTER ARCHIVE
# ==========================================================

with engine.begin() as conn:

    conn.execute(
        text("""
            INSERT INTO archived_partitions
            (
                partition_name,
                row_count,
                minio_path
            )
            VALUES
            (
                :partition_name,
                :row_count,
                :minio_path
            )
        """),
        {
            "partition_name": partition_name,
            "row_count": row_count,
            "minio_path": object_path
        }
    )

print("Archive registry updated.")

# ==========================================================
# DROP PARTITION
# ==========================================================

with engine.begin() as conn:

    conn.execute(
        text(f'DROP TABLE "{partition_name}"')
    )

print(f"Dropped Partition: {partition_name}")

# ==========================================================
# CLEANUP LOCAL FILE
# ==========================================================

if os.path.exists(parquet_file):
    os.remove(parquet_file)
    print(f"Deleted Local File: {parquet_file}")

# ==========================================================
# COMPLETE
# ==========================================================

print("\n" + "=" * 60)
print("ARCHIVE COMPLETED SUCCESSFULLY")
print("=" * 60)

print(f"Partition : {partition_name}")
print(f"Rows      : {row_count:,}")
print(f"MinIO Path: {object_path}")