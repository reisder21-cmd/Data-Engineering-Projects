"""
Pulls the four nh_gold tables from Athena to local Parquet for the
Streamlit dashboard. Run manually whenever you want fresh numbers:

    python refresh_data.py

Requires AWS credentials configured locally (aws sts get-caller-identity
should work). Athena query results land in s3://nh-202604-gold/_athena_results/
by default — override with the ATHENA_OUTPUT env var if you want a different
staging location.
"""

import os
from pathlib import Path

import awswrangler as wr


DATABASE = "nh_gold"
TABLES = [
    "facility_summary",
    "state_summary",
    "facility_monthly_trend",
    "state_monthly_trend",
]

OUT = Path(__file__).parent / "data"
OUT.mkdir(parents=True, exist_ok=True)

S3_OUTPUT = os.environ.get("ATHENA_OUTPUT", "s3://nh-202604-gold/_athena_results/")


def main() -> None:
    print(f"Refreshing dashboard data from {DATABASE}")
    print(f"Local output: {OUT}")
    print(f"Athena staging: {S3_OUTPUT}")
    print()

    for table in TABLES:
        print(f"  {table:<30}", end=" ", flush=True)
        df = wr.athena.read_sql_query(
            sql=f"SELECT * FROM {table}",
            database=DATABASE,
            s3_output=S3_OUTPUT,
        )
        out_path = OUT / f"{table}.parquet"
        df.to_parquet(out_path, index=False)
        print(f"{len(df):>8,} rows -> {out_path.name}")

    print("\nDone. Run `streamlit run app.py` to view.")


if __name__ == "__main__":
    main()
