"""
Glue Job 1 — Ingest CSV from Google Drive to S3 bronze (Delta).

Args (Glue Job Parameters):
  --JOB_NAME             (Glue-managed)
  --secret_name          AWS Secrets Manager id holding GCP service account JSON
  --drive_folder_id      Google Drive folder containing source files
  --file_name_contains   Substring to match against file names in that folder
  --bronze_bucket        e.g. nh-202604-bronze
  --bronze_database      e.g. nh_bronze
  --bronze_table         e.g. pbj_daily

Required Glue job parameters (set in console):
  --datalake-formats         delta
  --enable-glue-datacatalog  true
  --conf                     spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog
"""

import io
import json
import re
import sys
from datetime import date

import boto3
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
from pyspark.context import SparkContext
from pyspark.sql.functions import current_timestamp, lit


def sanitize_column(name: str) -> str:
    """Lower-case + replace any chars Delta forbids (and friends) with underscores."""
    s = name.strip().lower()
    s = re.sub(r"[ ,;{}()\t=\n\-/.\\\[\]]+", "_", s)
    s = re.sub(r"_+", "_", s)
    return s.strip("_") or "col"


# ---------- Glue / Spark bootstrap ----------
args = getResolvedOptions(
    sys.argv,
    ["JOB_NAME", "secret_name", "drive_folder_id", "file_name_contains",
     "bronze_bucket", "bronze_database", "bronze_table"],
)
gc = GlueContext(SparkContext.getOrCreate())
spark = gc.spark_session
job = Job(gc)
job.init(args["JOB_NAME"], args)

print(f"[args] {json.dumps({k: v for k, v in args.items() if k != 'secret_name'})}")

# ---------- Pull GCP service-account JSON from Secrets Manager ----------
sm = boto3.client("secretsmanager")
sa_info = json.loads(sm.get_secret_value(SecretId=args["secret_name"])["SecretString"])
creds = service_account.Credentials.from_service_account_info(
    sa_info, scopes=["https://www.googleapis.com/auth/drive.readonly"],
)

# ---------- Resolve filename → file_id in the given Drive folder ----------
drive = build("drive", "v3", credentials=creds, cache_discovery=False)
query = (f"'{args['drive_folder_id']}' in parents "
         f"and name contains '{args['file_name_contains']}' "
         f"and trashed = false")
listing = drive.files().list(q=query, fields="files(id, name, modifiedTime)",
                             pageSize=10).execute().get("files", [])
if not listing:
    raise RuntimeError(f"No Drive files in folder {args['drive_folder_id']} "
                       f"matching '{args['file_name_contains']}'")
listing.sort(key=lambda f: f["modifiedTime"], reverse=True)
chosen = listing[0]
print(f"[drive] matched {len(listing)} file(s); using {chosen['name']} (id={chosen['id']})")

# ---------- Download CSV from Drive to /tmp ----------
buf = io.BytesIO()
downloader = MediaIoBaseDownload(buf, drive.files().get_media(fileId=chosen["id"]))
done = False
while not done:
    _, done = downloader.next_chunk()
buf.seek(0)
local_path = "/tmp/source.csv"
with open(local_path, "wb") as f:
    f.write(buf.read())
print(f"[drive] downloaded {len(buf.getvalue())} bytes to {local_path}")

# ---------- Stage to S3 so Spark can read it ----------
s3 = boto3.client("s3")
staging_key = f"_staging/{args['bronze_table']}/{date.today().isoformat()}.csv"
s3.upload_file(local_path, args["bronze_bucket"], staging_key)
staging_uri = f"s3://{args['bronze_bucket']}/{staging_key}"
print(f"[stage] uploaded to {staging_uri}")

# ---------- Read CSV with Spark ----------
df = (spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(staging_uri))

# ---------- Sanitize column names (Delta-safe snake_case) ----------
sanitized = [sanitize_column(c) for c in df.columns]
df = df.toDF(*sanitized)
# Disambiguate any collisions (e.g. "Provider Name" and "Provider_Name" both → "provider_name")
seen = {}
final = []
for c in sanitized:
    if c in seen:
        seen[c] += 1
        final.append(f"{c}_{seen[c]}")
    else:
        seen[c] = 0
        final.append(c)
if final != sanitized:
    df = df.toDF(*final)
print(f"[sanitize] {len(df.columns)} columns; sample: {df.columns[:8]}")

# ---------- Add ingestion metadata ----------
df = (df.withColumn("load_date", lit(date.today().isoformat()))
        .withColumn("ingested_at", current_timestamp()))
row_count = df.count()
print(f"[read] {row_count} rows, {len(df.columns)} columns")

# ---------- Write Delta directly to S3 (explicit path, no catalog routing) ----------
table_uri = f"s3://{args['bronze_bucket']}/{args['bronze_table']}/"
(df.write.format("delta")
   .mode("append")
   .partitionBy("load_date")
   .option("mergeSchema", "true")
   .save(table_uri))
print(f"[write] wrote {row_count} rows to {table_uri}")

# ---------- Register table in Glue Catalog via Spark SQL (native Delta) ----------
db = args["bronze_database"]
table = args["bronze_table"]
spark.sql(f"CREATE DATABASE IF NOT EXISTS {db}")
print(f"[catalog] database {db} ready")

# CREATE TABLE USING DELTA registers with native Delta metadata.
# Athena engine v3 reads this natively (no symlink manifests needed).
# Schema is inferred from the Delta log at table_uri.
spark.sql(f"DROP TABLE IF EXISTS {db}.{table}")
spark.sql(f"CREATE TABLE {db}.{table} USING DELTA LOCATION '{table_uri}'")
print(f"[catalog] registered {db}.{table} as native Delta at {table_uri}")

# ---------- Cleanup staging file ----------
s3.delete_object(Bucket=args["bronze_bucket"], Key=staging_key)
print(f"[cleanup] deleted {staging_uri}")

job.commit()
print("[done] job committed")
