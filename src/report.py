# src/report.py
import pandas as pd
import matplotlib.pyplot as plt
from pathlib import Path
from datetime import datetime

# -----------------------------
# Paths
# -----------------------------
DATA = Path("data")
BRONZE = DATA / "bronze" / "credit_events"
SILVER = DATA / "silver"
GOLD = DATA / "gold" / "marts"

OUT = Path("outputs")
FIG = OUT / "figures"
OUT.mkdir(exist_ok=True)
FIG.mkdir(parents=True, exist_ok=True)

REPORT_PATH = OUT / "report.md"


# -----------------------------
# Utils
# -----------------------------
def read_all_parquet(folder):
    files = list(folder.rglob("*.parquet"))
    if not files:
        return pd.DataFrame()
    return pd.concat([pd.read_parquet(f) for f in files], ignore_index=True)


def pct(x, total):
    return round((x / total) * 100, 2) if total else 0


# -----------------------------
# Main
# -----------------------------
def main():
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # -------------------------
    # Load data
    # -------------------------
    bronze_df = read_all_parquet(BRONZE)
    silver_df = pd.read_parquet(SILVER / "credit_events")

    # -------------------------
    # Normalize types for quality checks
    # -------------------------
    numeric_cols = [
        "outstanding_balance",
        "principal_amount",
        "installment_amount",
        "interest_rate",
        "days_past_due",
        ]
    for col in numeric_cols:
        if col in silver_df.columns:
            silver_df[col] = pd.to_numeric(silver_df[col], errors="coerce")

    quarantine_path = SILVER / "quarantine" / "credit_events"
    quarantine_df = (
        pd.read_parquet(quarantine_path)
        if quarantine_path.exists()
        else pd.DataFrame()
    )

    cohort = pd.read_parquet(GOLD / "loan_cohort_metrics" / "data.parquet")
    current = pd.read_parquet(GOLD / "loan_current_state" / "data.parquet")

    # -------------------------
    # Metrics
    # -------------------------
    bronze_count = len(bronze_df)
    silver_count = len(silver_df)
    quarantine_count = len(quarantine_df)
    gold_count = len(current)

    micro_batches = len(list(BRONZE.rglob("*.parquet")))

    # -------------------------
    # Data Quality
    # -------------------------
    duplicate_count = silver_df.duplicated(subset=["event_id"]).sum()

    rule_invalid_dpd = (silver_df["days_past_due"] < 0).sum()
    rule_invalid_balance = (silver_df["outstanding_balance"] < 0).sum()
    rule_invalid_interest = (
        (silver_df["interest_rate"] < 0)
        | (silver_df["interest_rate"] > 1)
    ).sum()

    critical_fields = ["loan_id", "customer_id", "event_time"]
    nulls = {
        col: round(silver_df[col].isna().mean() * 100, 2)
        for col in critical_fields
    }

    # -------------------------
    # Plot 1: Data quality
    # -------------------------
    plt.figure()
    plt.bar(
        ["Valid", "Quarantine", "Duplicates"],
        [
            silver_count - duplicate_count,
            quarantine_count,
            duplicate_count,
        ],
    )
    plt.title("Data Quality Summary")
    plt.ylabel("Records")
    plt.savefig(FIG / "data_quality.png")
    plt.close()

    # -------------------------
    # Plot 2: Business metric
    # -------------------------
    cohort_plot = (
        cohort.groupby("cohort_month", as_index=False)
        .agg(balance_at_risk=("balance_at_risk", "sum"))
        .sort_values("cohort_month")
    )

    plt.figure()
    plt.plot(
        cohort_plot["cohort_month"],
        cohort_plot["balance_at_risk"],
        marker="o",
    )
    plt.title("Balance at Risk by Cohort")
    plt.xlabel("Cohort Month")
    plt.ylabel("Balance at Risk")
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.savefig(FIG / "balance_at_risk_by_cohort.png")
    plt.close()

    # -------------------------
    # Markdown Report
    # -------------------------
    with open(REPORT_PATH, "w", encoding="utf-8") as f:
        f.write(f"# 📊 Data Pipeline Report\n\n")
        f.write(f"**Execution time:** {now}\n\n")

        f.write("## 1️⃣ Execution Summary\n")
        f.write(f"- Bronze path: `{BRONZE}`\n")
        f.write(f"- Silver path: `{SILVER}`\n")
        f.write(f"- Gold path: `{GOLD}`\n")
        f.write(f"- Micro-batches processed: **{micro_batches}**\n\n")

        f.write("## 2️⃣ Records by Medallion Layer\n")
        f.write(f"- Bronze records: **{bronze_count}**\n")
        f.write(f"- Silver valid records: **{silver_count}**\n")
        f.write(f"- Silver quarantine records: **{quarantine_count}**\n")
        f.write(f"- Gold records: **{gold_count}**\n\n")

        f.write("## 3️⃣ Data Quality\n")
        f.write(f"- Duplicate events detected: **{duplicate_count}**\n")
        f.write(f"- Invalid DPD records: **{rule_invalid_dpd}**\n")
        f.write(f"- Invalid balance records: **{rule_invalid_balance}**\n")
        f.write(f"- Invalid interest rate records: **{rule_invalid_interest}**\n\n")

        f.write("### Null percentage (critical fields)\n")
        for k, v in nulls.items():
            f.write(f"- {k}: **{v}%**\n")

        f.write("\n## 4️⃣ Visualizations\n")
        f.write("### Data Quality Overview\n")
        f.write("![](figures/data_quality.png)\n\n")
        f.write("### Balance at Risk by Cohort\n")
        f.write("![](figures/balance_at_risk_by_cohort.png)\n")

    print(f"✔ Report generated at {REPORT_PATH}")


if __name__ == "__main__":
    main()
