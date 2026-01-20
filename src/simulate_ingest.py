# src/simulate_ingest.py
"""
Simula llegada incremental de eventos escribiendo pequeños archivos CSV en data/landing/.
Uso: python src/simulate_ingest.py --input data/raw/archivo1.csv --rate 1 --out data/landing/
"""
import argparse
import time
import os
import uuid
import pandas as pd

def write_rows_as_files(df, out_dir, delay_sec=1, batch_size=1, prefix="batch"):
    os.makedirs(out_dir, exist_ok=True)
    total = len(df)
    i = 0
    batch_id = 0
    while i < total:
        batch = df.iloc[i:i+batch_size]
        ts = pd.Timestamp.utcnow().strftime("%Y%m%dT%H%M%S%f")
        fname = f"{prefix}_{ts}_{batch_id}_{uuid.uuid4().hex}.csv"
        path = os.path.join(out_dir, fname)
        batch.to_csv(path, index=False)
        print(f"Wrote {len(batch)} rows -> {path}")
        i += batch_size
        batch_id += 1
        time.sleep(delay_sec)  # simula llegada
    print("Simulación finalizada.")

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True, help="CSV de entrada")
    parser.add_argument("--out", default="data/landing", help="carpeta landing")
    parser.add_argument("--rate", type=float, default=1.0, help="segundos entre micro-batches")
    parser.add_argument("--batch-size", type=int, default=1, help="filas por archivo (micro-batch)")
    args = parser.parse_args()

    df = pd.read_csv(args.input)
    write_rows_as_files(df, args.out, delay_sec=args.rate, batch_size=args.batch_size)

if __name__ == "__main__":
    main()
