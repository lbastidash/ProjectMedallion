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
    # Plot 1: Calidad de datos (mejorada)
    # -------------------------
    plt.figure(figsize=(6, 4))
    plt.bar(
        ["Válidos", "Cuarentena", "Duplicados"],
        [
            silver_count - duplicate_count,
            quarantine_count,
            duplicate_count,
        ],
    )

    plt.title("Resumen de Calidad de Datos")
    plt.ylabel("Cantidad de registros")
    plt.grid(axis="y", alpha=0.3)
    plt.tight_layout()
    plt.savefig(FIG / "calidad_datos.png")
    plt.close()


        # -------------------------
    # Plot 2: Métrica de negocio (mejorada)
    # -------------------------
    cohort_plot = (
        cohort.groupby("cohort_month", as_index=False)
        .agg(balance_at_risk=("balance_at_risk", "sum"))
        .sort_values("cohort_month")
    )

    plt.figure(figsize=(10, 5))  # ← más ancho
    plt.plot(
        cohort_plot["cohort_month"],
        cohort_plot["balance_at_risk"],
        marker="o",
    )

    plt.title("Saldo en Riesgo por Mes de Cohorte")
    plt.xlabel("Mes de cohorte")
    plt.ylabel("Saldo en riesgo")

    # Mostrar solo algunas etiquetas del eje X
    step = max(len(cohort_plot) // 10, 1)
    plt.xticks(
        cohort_plot["cohort_month"][::step],
        rotation=45,
        ha="right",
    )

    plt.grid(True, alpha=0.3)
    plt.tight_layout()
    plt.savefig(FIG / "saldo_en_riesgo_por_cohorte.png")
    plt.close()

    # -------------------------
    # Markdown Report
    # -------------------------
    with open(REPORT_PATH, "w", encoding="utf-8") as f:
        f.write("# 📊 Reporte del Pipeline de Datos\n\n")
        f.write(f"**Fecha y hora de ejecución:** {now}\n\n")

        f.write("## 1️⃣ Resumen de Ejecución\n")
        f.write(
            "Este reporte presenta un resumen del estado final del pipeline "
            "implementado bajo una arquitectura Medallion (Bronze, Silver, Gold).\n\n"
        )
        f.write("**Parámetros de ejecución:**\n")
        f.write(f"- Ruta Bronze: `{BRONZE}`\n")
        f.write(f"- Ruta Silver: `{SILVER}`\n")
        f.write(f"- Ruta Gold: `{GOLD}`\n")
        f.write(f"- Micro-batches procesados en Bronze: **{micro_batches}**\n\n")

        f.write("## 2️⃣ Métricas por Capa (Medallion)\n")
        f.write(f"- Registros en Bronze: **{bronze_count}**\n")
        f.write(f"- Registros válidos en Silver: **{silver_count}**\n")
        f.write(f"- Registros enviados a cuarentena (Silver): **{quarantine_count}**\n")
        f.write(f"- Registros finales en Gold: **{gold_count}**\n\n")

        f.write("## 3️⃣ Calidad de Datos\n")
        f.write("### Reglas de validación aplicadas\n")
        f.write(f"- Registros duplicados detectados: **{duplicate_count}**\n")
        f.write(f"- Registros con días de mora inválidos: **{rule_invalid_dpd}**\n")
        f.write(f"- Registros con saldo inválido: **{rule_invalid_balance}**\n")
        f.write(f"- Registros con tasa de interés inválida: **{rule_invalid_interest}**\n\n")

        f.write("### Porcentaje de valores nulos en campos críticos\n")
        for k, v in nulls.items():
            f.write(f"- {k}: **{v}%**\n")

        f.write("\n## 4️⃣ Visualizaciones\n")
        f.write(
            "Las siguientes visualizaciones permiten evaluar rápidamente "
            "la calidad del pipeline y el valor analítico de la capa Gold.\n\n"
        )

        f.write("### Resumen de calidad de datos\n")
        f.write("![](figures/calidad_datos.png)\n\n")

        f.write("### Saldo en riesgo por mes de cohorte\n")
        f.write("![](figures/saldo_en_riesgo_por_cohorte.png)\n")


    print(f"✔ Report generated at {REPORT_PATH}")


if __name__ == "__main__":
    main()
