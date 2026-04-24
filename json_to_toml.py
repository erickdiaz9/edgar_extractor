"""
Run this script once to convert your GCS service account JSON key
into the TOML format needed for Streamlit secrets.

Usage:
    python json_to_toml.py path/to/your-key-file.json edgar-scorecard-db
"""
import json
import sys

if len(sys.argv) < 3:
    print("Usage: python json_to_toml.py <path-to-key.json> <bucket-name>")
    sys.exit(1)

key_path   = sys.argv[1]
bucket     = sys.argv[2]

with open(key_path, encoding="utf-8") as f:
    key = json.load(f)

lines = [
    f'[gcs]',
    f'bucket = "{bucket}"',
    f'',
    f'[gcs.credentials]',
]
for k, v in key.items():
    if isinstance(v, str):
        # Escape for TOML double-quoted string
        v_escaped = (v
            .replace("\\", "\\\\")
            .replace("\n", "\\n")
            .replace("\r", "\\r")
            .replace('"', '\\"'))
        lines.append(f'{k} = "{v_escaped}"')
    else:
        lines.append(f'{k} = {json.dumps(v)}')

print("\n".join(lines))
print("\n--- Copy everything above and paste into Streamlit Secrets ---")
