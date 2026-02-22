import pandas as pd
import os

RAW = os.path.join("raw")        # input files
PROC = os.path.join("processed") # output processed files
AUDIT_LOG = os.path.join(PROC, "audit_log.csv")

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
