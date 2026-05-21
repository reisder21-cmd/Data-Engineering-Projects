# GlobalPartners Data Gather from S3 gold tables

import os
from pathlib import Path
import awswrangler as wr

DATABASE = 'gpb-gold'
TABLES = [
    "fact_order_items",
    "customer_summary"
]

OUT = Path(__file__).parent / "data"
OUT.mkdir(parents=True, exist_ok=True)

S3_OUTPUT = os.environ.get("ATHENA_OUTPUT", "s3://gbp-202605-gold/_athena_results/")

for table in TABLES:
    df = wr.athena.read_sql_query(
        sql = f"SELECT * FROM {table}",
        database=DATABASE,
        s3_output=f"{S3_OUTPUT}"
    )
    out_path = OUT / f"{table}.parquet"
    df.to_parquet(out_path, index=False)
    print(f"{len(df):>8,} rows -> {out_path.name}")
