# src/bronze_batch.py

import os
import pandas as pd
from datetime import datetime

BASE_LANDING = "data/landing"
BASE_BRONZE = "data/bronze"

def ingest_to_bronze(source_name, ingest_type):
    landing_path = os.path.join(BASE_LANDING, source_name)
    bronze_path = os.path.join(BASE_BRONZE, source_name)

    os.makedirs(bronze_path, exist_ok=True)

    files = [f for f in os.listdir(landing_path) if f.endswith(".csv")]

    if not files:
        print(f"[INFO] No files found for {source_name}")
        return

    dfs = []
    for f in files:
        df = pd.read_csv(os.path.join(landing_path, f))
        df["ingest_timestamp"] = datetime.utcnow()
        df["ingest_date"] = df["ingest_timestamp"].dt.date
        df["source_system"] = source_name
        df["ingest_type"] = ingest_type
        df["source_file"] = f
        dfs.append(df)

    final_df = pd.concat(dfs, ignore_index=True)

    partition_dir = os.path.join(
        bronze_path,
        f"ingest_date={final_df['ingest_date'].iloc[0]}"
    )
    os.makedirs(partition_dir, exist_ok=True)

    out_file = os.path.join(partition_dir, "data.parquet")
    final_df.to_parquet(out_file, index=False)

    print(f"[OK] Bronze written for {source_name}")

def main():
    ingest_to_bronze("credit_events", "FACT")
    ingest_to_bronze("region_reference", "DIMENSION")

if __name__ == "__main__":
    main()
