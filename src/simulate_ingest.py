# src/simulate_ingest.py
"""
Simula llegada incremental de datos escribiendo micro-batches CSV.
Cada archivo representa un micro-batch realista.

Uso: python src/simulate_ingest.py 
  --input data/raw_zip/credit_events.csv 
  --out data/landing/credit_events 
  --batch-size 500 
  --rate 1 
  --source-name credit_events

  
Uso: python src/simulate_ingest.py 
  --input data/raw_zip/region_reference.csv 
  --out data/landing/region_reference 
  --batch-size 10 
  --rate 1 
  --source-name region_reference

"""

import argparse
import time
import os
import uuid
import pandas as pd

def simulate_micro_batches(
    input_csv,
    out_dir,
    batch_size=500,
    delay_sec=1,
    source_name="source"
):
    os.makedirs(out_dir, exist_ok=True)
    df = pd.read_csv(input_csv)

    total_rows = len(df)
    batch_id = 0

    for start in range(0, total_rows, batch_size):
        end = min(start + batch_size, total_rows)
        batch_df = df.iloc[start:end]

        ts = pd.Timestamp.utcnow().strftime("%Y%m%dT%H%M%S")
        
        filename = f"{source_name}_batch_{batch_id}_{ts}.csv"
        path = os.path.join(out_dir, filename)

        batch_df.to_csv(path, index=False)
        print(f"✔ Batch {batch_id}: filas {start}-{end} → {path}")

        batch_id += 1
        time.sleep(delay_sec)

    print("✔ Simulación completada")

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True)
    parser.add_argument("--out", required=True)
    parser.add_argument("--batch-size", type=int, default=500)
    parser.add_argument("--rate", type=float, default=1.0)
    parser.add_argument("--source-name", default="source")

    args = parser.parse_args()

    simulate_micro_batches(
        input_csv=args.input,
        out_dir=args.out,
        batch_size=args.batch_size,
        delay_sec=args.rate,
        source_name=args.source_name
    )

if __name__ == "__main__":
    main()
