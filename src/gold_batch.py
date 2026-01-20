# src/gold_batch.py
import pandas as pd
from pathlib import Path

SILVER = Path("data/silver")
GOLD = Path("data/gold")


def load_silver(table):
    return pd.read_parquet(SILVER / table)


def main():
    print("▶ Building Gold layer")

    # ----------------------------
    # Load Silver
    # ----------------------------
    events = load_silver("credit_events")
    regions = load_silver("region_reference")

    # ----------------------------
    # Enrichment
    # ----------------------------
    df = events.merge(regions, on="region", how="left")

    # ----------------------------
    # Type normalization (CRITICAL)
    # ----------------------------
    numeric_cols = [
        "principal_amount",
        "installment_amount",
        "outstanding_balance",
        "interest_rate",
        "days_past_due",
        "term_months",
    ]

    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    df["event_time"] = pd.to_datetime(df["event_time"], errors="coerce")

    # ----------------------------
    # Derived fields
    # ----------------------------
    df["cohort_month"] = df["event_time"].dt.to_period("M").astype(str)
    df["is_in_default"] = df["days_past_due"] > 30
    df["balance_at_risk"] = df["outstanding_balance"].where(
        df["is_in_default"], 0
    )

    # ----------------------------
    # PRODUCT 1: Cohort metrics
    # ----------------------------
    cohort_metrics = (
        df.groupby(["cohort_month", "macro_region", "risk_segment"], dropna=False)
        .agg(
            loans=("loan_id", "nunique"),
            total_balance=("outstanding_balance", "sum"),
            balance_at_risk=("balance_at_risk", "sum"),
            avg_dpd=("days_past_due", "mean"),
            default_rate=("is_in_default", "mean"),
        )
        .reset_index()
    )

    out1 = GOLD / "marts" / "loan_cohort_metrics"
    out1.mkdir(parents=True, exist_ok=True)
    cohort_metrics.to_parquet(out1 / "data.parquet", index=False)

    # ----------------------------
    # PRODUCT 2: Current loan state
    # ----------------------------
    latest = (
        df.sort_values("event_time")
        .groupby("loan_id", dropna=False)
        .tail(1)
        .reset_index(drop=True)
    )

    out2 = GOLD / "marts" / "loan_current_state"
    out2.mkdir(parents=True, exist_ok=True)
    latest.to_parquet(out2 / "data.parquet", index=False)

    print("✔ Gold layer built successfully")


if __name__ == "__main__":
    main()
