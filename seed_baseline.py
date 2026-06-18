#!/usr/bin/env python3
"""One-off: convert the uploaded full-catalog xlsx into a day-zero snapshot
and seed seen_ids.json, so the first live scraper run diffs against real
baseline state instead of cold-starting."""
import sys, gzip, json
from pathlib import Path
import openpyxl
sys.path.insert(0, '/home/claude/competitor-tracker')
from scraper import parse_title

SRC = "/mnt/user-data/uploads/shopify_catalog_full.xlsx"
DATA = Path("/home/claude/competitor-tracker/data")
SNAP = DATA / "snapshots"
SNAP.mkdir(parents=True, exist_ok=True)
TS = "2026-06-18"

wb = openpyxl.load_workbook(SRC, read_only=True)
ws = wb["Combined"]
rows = ws.iter_rows(values_only=True)
next(rows)  # banner
hdr = next(rows)
idx = {h: i for i, h in enumerate(hdr)}

snap = []
seen = {}
for r in rows:
    if not r or not r[idx["Store"]]:
        continue
    store = r[idx["Store"]]
    url = r[idx["Product URL"]]
    pid = url.rsplit("/", 1)[-1] if url else None
    if not pid:
        continue
    avail_raw = str(r[idx["Available"]])
    available = "TRUE" in avail_raw.upper()
    title = r[idx["Product title"]]
    parsed = parse_title(title or "")
    price = r[idx["Price"]]
    try:
        price = float(price) if price is not None else None
    except (TypeError, ValueError):
        price = None
    cmp_at = r[idx["Compare-at price"]]
    try:
        cmp_at = float(cmp_at) if cmp_at is not None else None
    except (TypeError, ValueError):
        cmp_at = None
    rec = {
        "id": pid, "store": store, "title": title,
        "handle": url.rsplit("/", 1)[-1] if url else None,
        "sku": r[idx["SKU"]], "price": price, "compare_at": cmp_at,
        "available": available, "vendor": r[idx["Vendor"]],
        "created_at": str(r[idx["Created"]]) if r[idx["Created"]] else None,
        "image": r[idx["Image URL"]], "url": url, **parsed,
    }
    snap.append(rec)
    seen.setdefault(store, {})[pid] = 1

with gzip.open(SNAP / f"{TS}.json.gz", "wt", encoding="utf-8") as f:
    json.dump(snap, f, separators=(",", ":"))
with open(DATA / "seen_ids.json", "w", encoding="utf-8") as f:
    json.dump(seen, f, separators=(",", ":"))
with open(DATA / "events.json", "w", encoding="utf-8") as f:
    json.dump([], f)

from collections import Counter
print("baseline snapshot:", len(snap), "products")
print("by store:", {s: len(v) for s, v in seen.items()})
print("in stock:", sum(1 for r in snap if r["available"]))
print("categories:", dict(Counter(r["category"] for r in snap)))
