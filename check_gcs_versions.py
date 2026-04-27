"""
check_gcs_versions.py
---------------------
Lists all versions of scorecard.db in your GCS bucket and optionally
restores a previous one.

Usage:
    python check_gcs_versions.py path/to/your-key-file.json your-bucket-name

Example:
    python check_gcs_versions.py my-key.json edgar-scorecard-db
"""
import sys
import json

if len(sys.argv) < 3:
    print("Usage: python check_gcs_versions.py <key-file.json> <bucket-name>")
    sys.exit(1)

key_path    = sys.argv[1]
bucket_name = sys.argv[2]

from google.cloud import storage
from google.oauth2 import service_account

with open(key_path) as f:
    creds_dict = json.load(f)

creds  = service_account.Credentials.from_service_account_info(creds_dict)
client = storage.Client(credentials=creds, project=creds_dict["project_id"])
bucket = client.bucket(bucket_name)

# ── 1. Versioning status ──────────────────────────────────────────────────────
bucket.reload()
if bucket.versioning_enabled:
    print("✅  Object versioning is ON — previous versions can be restored.\n")
else:
    print("❌  Object versioning is OFF — only the current file exists.\n")
    print("    Enabling versioning now for future protection...")
    bucket.versioning_enabled = True
    bucket.patch()
    print("    ✅ Versioning enabled. Future uploads will be versioned.\n")

# ── 2. List all versions of scorecard.db ─────────────────────────────────────
blobs = list(client.list_blobs(bucket_name, prefix="scorecard.db", versions=True))

if not blobs:
    print("No scorecard.db found in the bucket at all.")
    sys.exit(0)

print(f"{'#':<4} {'Generation':<22} {'Updated (UTC)':<28} {'Size (KB)':<12} {'Status'}")
print("-" * 80)
for i, b in enumerate(blobs):
    size_kb = f"{b.size / 1024:.1f}" if b.size else "0"
    status  = "DELETED" if b.time_deleted else "LIVE"
    print(f"{i:<4} {str(b.generation):<22} {str(b.updated)[:26]:<28} {size_kb:<12} {status}")

# ── 3. Restore prompt ─────────────────────────────────────────────────────────
print()
answer = input("Enter # to restore that version as the live file (or press Enter to skip): ").strip()
if answer == "":
    print("No restore performed.")
    sys.exit(0)

try:
    idx = int(answer)
    chosen = blobs[idx]
    print(f"\nRestoring generation {chosen.generation} ({chosen.updated}) ...")

    # Copy chosen generation → overwrites the live blob (creates new live version)
    live_blob = bucket.blob("scorecard.db")
    bucket.copy_blob(chosen, bucket, "scorecard.db")
    print("✅  Restored successfully.")
    print("    Restart your Streamlit app — it will download the restored DB on next load.")
except (ValueError, IndexError) as e:
    print(f"Invalid selection: {e}")
