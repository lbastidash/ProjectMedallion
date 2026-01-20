
# src/silver_batch.py

import os
import pandas as pd
from datetime import datetime
from glob import glob

BRONZE_BASE = "data/bronze"
SILVER_BASE = "data/silver"


# -------------------------
# Utils genéricos
# -------------------------
def normalize_columns(df):
    df.columns = (
        df.columns
        .str.strip()
        .str.lower()
        .str.replace(" ", "_")
        .str.replace(".", "", regex=False)
        .str.replace("/", "_", regex=False)
    )
    return df


def drop_fully_empty(df):
    df = df.dropna(axis=1, how="all")
    df = df.dropna(how="all")

    mask_empty = df.astype(str).apply(
        lambda row: all(str(x).strip() in ["", "nan", "None"] for x in row),
        axis=1
    )
    return df[~mask_empty]


def heuristic_numeric_cast(df, threshold=0.9):
    for col in df.columns:
        series = df[col].astype(str).str.replace(",", "").str.strip()
        non_null = series[~series.isin(["", "nan", "None"])]
        if len(non_null) == 0:
            continue

        coerced = pd.to_numeric(non_null, errors="coerce")
        if coerced.notna().sum() / len(non_null) >= threshold:
            df[col] = pd.to_numeric(series, errors="coerce")

    return df


def heuristic_datetime_cast(df, threshold=0.8):
    keywords = ["date", "time", "ts", "timestamp", "fecha", "hora"]

    for col in df.columns:
        series = df[col].astype(str).str.strip()
        non_null = series[~series.isin(["", "nan", "None"])]
        if len(non_null) == 0:
            continue

        coerced = pd.to_datetime(non_null, errors="coerce")
        ratio = coerced.notna().sum() / len(non_null)

        if ratio >= threshold or any(k in col for k in keywords):
            df[col] = pd.to_datetime(df[col], errors="coerce")

    return df


def trim_strings(df):
    for col in df.select_dtypes(include=["object"]).columns:
        df[col] = df[col].astype(str).str.strip().replace({"^nan$": None}, regex=True)
    return df


# -------------------------
# Core Silver
# -------------------------
def load_bronze_parquets(table_name):
    """
    Lee TODOS los parquet individuales evitando conflictos de schema
    """
    pattern = os.path.join(
        BRONZE_BASE,
        table_name,
        "**",
        "*.parquet"
    )

    files = glob(pattern, recursive=True)

    if not files:
        raise FileNotFoundError(f"No parquet files found for {table_name}")

    dfs = []
    for f in files:
        df = pd.read_parquet(f)

        # 🔑 Normalizar ingest_date SI existe
        if "ingest_date" in df.columns:
            df["ingest_date"] = pd.to_datetime(
                df["ingest_date"], errors="coerce"
            ).dt.date

        dfs.append(df)

    return pd.concat(dfs, ignore_index=True)


def silverize_table(table_name):
    print(f"▶ Processing Silver for table: {table_name}")

    df = load_bronze_parquets(table_name)
    rows_before = len(df)

    df = normalize_columns(df)
    df = drop_fully_empty(df)
    df = heuristic_numeric_cast(df)
    df = heuristic_datetime_cast(df)
    df = trim_strings(df)

    # deduplicación segura
    df = df.drop_duplicates()

    rows_after = len(df)

    silver_path = os.path.join(SILVER_BASE, table_name)
    os.makedirs(silver_path, exist_ok=True)

    out_file = os.path.join(silver_path, "data.parquet")
    df.to_parquet(out_file, index=False)

    print(
        f"✔ Silver {table_name} written | "
        f"rows: {rows_before} → {rows_after}"
    )


def main():
    silverize_table("credit_events")
    silverize_table("region_reference")


if __name__ == "__main__":
    main()
