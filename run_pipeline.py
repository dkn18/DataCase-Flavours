import os
import subprocess

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
INGEST_SCRIPT = os.path.join(SCRIPT_DIR, "ingest.py")
TRANSFORM_SCRIPT = os.path.join(SCRIPT_DIR, "transform.py")

print("\n=== Running Ingestion: raw → processed layer ===")
subprocess.run(["python", INGEST_SCRIPT], check=True)

print("\n=== Running Transformation: processed → warehouse ===")
subprocess.run(["python", TRANSFORM_SCRIPT], check=True)

print("\nPipeline completed successfully!")
