#!/usr/bin/env python3
"""Upload a CSV to Dune as dune.<handle>.dataset_<table_name>, replacing any existing table.

Usage: DUNE_API_KEY=... python3 upload_dune.py file.csv table_name ["description"]
Stdlib only. Docs: https://docs.dune.com/api-reference/tables/endpoint/upload
"""
import json, os, sys, urllib.request

if len(sys.argv) < 3:
    sys.exit("usage: upload_dune.py file.csv table_name [description]")
path, table = sys.argv[1], sys.argv[2]
desc = sys.argv[3] if len(sys.argv) > 3 else ""
key = os.environ.get("DUNE_API_KEY")
if not key:
    sys.exit("DUNE_API_KEY not set")

data = open(path, encoding="utf-8").read()
body = json.dumps({"table_name": table, "description": desc, "is_private": False, "data": data}).encode()
req = urllib.request.Request(
    "https://api.dune.com/api/v1/table/upload/csv",
    data=body,
    headers={"Content-Type": "application/json", "X-DUNE-API-KEY": key},
)
try:
    with urllib.request.urlopen(req, timeout=300) as r:
        print("uploaded", path, "->", table, "|", r.read().decode()[:300])
except urllib.error.HTTPError as e:
    sys.exit(f"dune upload failed: HTTP {e.code}: {e.read().decode()[:500]}")
