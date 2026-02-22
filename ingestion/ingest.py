import pandas as pd
import os

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))  # folder where this script lives

RAW = os.path.join(SCRIPT_DIR, "..", "raw")        # raw folder is one level up
PROC = os.path.join(SCRIPT_DIR, "..", "processed") # processed folder alongside raw
AUDIT_LOG = os.path.join(PROC, "audit_log.csv")    # audit log path

os.makedirs(PROC, exist_ok=True)

audit_rows = []

for file in os.listdir(RAW):
    if file.endswith(".csv"):
        path = os.path.join(RAW, file)
        df = pd.read_csv(path)

        # Save processed version
        out_file = os.path.join(PROC, file.replace(".csv", "_proc.csv"))
        df.to_csv(out_file, index=False)

        # Add simple audit info
        audit_rows.append({
            "file": file,
            "rows": len(df),
            "duplicates": df.duplicated().sum(),
            "nulls": df.isnull().sum().sum()
        })
        print(f"Processed {file}: {len(df)} rows")

# Save audit log
pd.DataFrame(audit_rows).to_csv(AUDIT_LOG, index=False)
print("Ingestion complete. Audit log saved.")
