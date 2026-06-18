#!/usr/bin/env python3
"""
Competitor inventory & sales-velocity tracker.

Scrapes two public Shopify parts stores via the collection-enumeration method
(bypasses the 100-page / 25k-product cap on /products.json), parses Japanese
used-parts titles into structured fields, snapshots the full catalog, and diffs
against the previous snapshot to emit availability-transition events.

An in-stock -> out-of-stock transition (or product disappearance) is the SALE
proxy. This is a directional "sales velocity index", not exact unit sales:
it undercounts SKUs that sell-and-relist between snapshots and cannot see
multi-unit sales. Stores expose variant.available (bool) only; inventory_quantity
is always null.
"""

import json
import os
import re
import sys
import time
import gzip
import datetime as dt
from pathlib import Path

import requests

# ----------------------------------------------------------------------------
# Config
# ----------------------------------------------------------------------------

STORES = [
    "epartsworld-kenya",
    "kaihoindustry",
]

ROOT = Path(__file__).resolve().parent
DATA = ROOT / "data"
SNAP_DIR = DATA / "snapshots"
EVENTS_FILE = DATA / "events.json"
SEEN_FILE = DATA / "seen_ids.json"
LATEST_FILE = DATA / "latest.json"

REQUEST_DELAY = 0.25          # polite inter-request delay (s)
MAX_RETRIES = 6               # per-request retry budget
BACKOFF_BASE = 2.0            # exponential backoff base
BACKOFF_CAP = 30.0            # max sleep between retries (s)
PER_PAGE = 250                # Shopify hard max
HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; catalog-monitor/1.0)",
    "Accept": "application/json",
}

# Fail-loud guard: reject a store snapshot that collapses vs last good run.
MIN_RETENTION = 0.70          # require >=70% of previous product count

# ----------------------------------------------------------------------------
# Title parsing
# ----------------------------------------------------------------------------

MAKES = [
    "TOYOTA", "NISSAN", "HONDA", "MAZDA", "SUBARU", "SUZUKI", "MITSUBISHI",
    "DAIHATSU", "ISUZU", "LEXUS", "MERCEDES", "BMW", "VW", "VOLKSWAGEN",
    "AUDI", "FORD", "VOLVO", "LAND ROVER", "LANDROVER", "JAGUAR", "PEUGEOT",
    "RENAULT", "CHEVROLET", "HYUNDAI", "KIA", "MINI", "PORSCHE", "FIAT",
    "JEEP", "CHRYSLER", "DODGE",
]
# Longest-first so "LAND ROVER" matches before "LAND".
MAKES.sort(key=len, reverse=True)

CHASSIS_RE = re.compile(r"^[A-Z]{1,4}[0-9]{1,4}[A-Z]?$|^[A-Z0-9]{4,8}$")
YEAR_RE = re.compile(r"\b(19[5-9]\d|20[0-4]\d)\b")
ENGINE_RE = re.compile(r"^[A-Z0-9]{1,3}-?[A-Z0-9]{1,5}$")
TRANS_RE = re.compile(r"\b(CVT|AT|MT)\b")
DRIVE_RE = re.compile(r"\b(FF|FR|MR|RR)\b")
AWD_RE = re.compile(r"\b(4WD|AWD|2WD)\b")

# Category rollup (13 buckets). First matching bucket wins; order matters.
CATEGORY_RULES = [
    ("Engine", ["ENGINE", "LONG BLOCK", "SHORT BLOCK", "HALF ENGINE"]),
    ("Drivetrain", ["TRANSMISSION", "MISSION", "GEARBOX", "CVT", "DIFFERENTIAL",
                    "TRANSFER", "TORQUE CONVERTER", "PROPELLER", "PROPSHAFT",
                    "DRIVE SHAFT", "DRIVESHAFT", "AXLE", "CLUTCH", "COUPLING"]),
    ("Body / Exterior", ["DOOR", "BONNET", "BUMPER", "FENDER", "GATE", "NOSE CUT",
                         "HOOD", "TRUNK", "ROOF", "QUARTER", "PILLAR"]),
    ("Lighting & Mirrors", ["LAMP", "LIGHT", "HEADLIGHT", "MIRROR", "SIGNAL",
                            "FOG", "REFLECTOR"]),
    ("Interior", ["SEAT", "DASHBOARD", "GLOVE BOX", "SUN VISOR", "CONSOLE",
                  "TRIM", "CARPET", "HEADLINER", "SHIFT LEVER", "STEERING WHEEL",
                  "METER", "SWITCH"]),
    ("Cooling & A/C", ["RADIATOR", "CONDENSER", "HEATER", "BLOWER", "COMPRESSOR",
                       "COOLING", "INTERCOOLER", "FAN MOTOR"]),
    ("Fuel / Intake / Exhaust", ["FUEL", "THROTTLE", "INJECT", "AIR CLEANER",
                                 "TURBO", "MANIFOLD", "CATALY", "MUFFLER",
                                 "EXHAUST", "PUMP"]),
    ("Brakes", ["BRAKE", "ABS", "DRUM", "ROTOR", "CALIPER", "MASTER"]),
    ("Suspension & Steering", ["STRUT", "SHOCK", "ARM", "SUSPENSION", "MEMBER",
                               "STABILIZER", "SPRING", "KNUCKLE", "HUB",
                               "STEERING", "RACK", "PINION", "TIE ROD"]),
    ("Electrical & Electronics", ["ECU", "COMPUTER", "RELAY", "HARNESS",
                                  "ACTUATOR", "SENSOR", "MODULE", "NAVIGATION",
                                  "RADIO", "AUDIO", "HORN", "WIPER MOTOR",
                                  "MOTOR", "ALTERNATOR", "STARTER"]),
    ("Wheels & Tyres", ["WHEEL", "TIRE", "TYRE", "RIM", "ALLOY"]),
    ("Glass", ["GLASS", "WINDOW", "WINDSHIELD", "REGULATOR"]),
]


def categorize(part_label: str) -> str:
    up = (part_label or "").upper()
    for bucket, keywords in CATEGORY_RULES:
        for kw in keywords:
            if kw in up:
                return bucket
    return "Other / Misc"


def parse_title(title: str) -> dict:
    """Extract structured fields from a title string like:
    [R DRUM LH] TOYOTA COROLLA FIELDER ZRE144G 2010 2ZR-FAE FF AT 4WD #0401..."""
    out = {
        "part_label": None, "make": None, "model": None, "chassis": None,
        "year": None, "engine": None, "transmission": None, "drivetrain": None,
        "awd": None, "category": "Other / Misc",
    }
    if not title:
        return out
    t = title.strip()

    # Part label = leading [...]
    m = re.match(r"\s*\[([^\]]*)\]\s*(.*)", t)
    if m:
        out["part_label"] = m.group(1).strip()
        rest = m.group(2).strip()
    else:
        rest = t

    out["category"] = categorize(out["part_label"] or rest)

    # Strip trailing #SKU token from the parse region.
    rest = re.sub(r"#\S+\s*$", "", rest).strip()

    # Make (longest-first).
    up_rest = rest.upper()
    for mk in MAKES:
        if up_rest.startswith(mk + " ") or up_rest == mk:
            out["make"] = mk.title() if mk not in ("BMW", "VW") else mk
            rest = rest[len(mk):].strip()
            break

    # Year.
    ym = YEAR_RE.search(rest)
    if ym:
        out["year"] = ym.group(1)

    # Transmission / drivetrain / awd.
    up = rest.upper()
    tm = TRANS_RE.search(up)
    if tm:
        out["transmission"] = tm.group(1)
    dm = DRIVE_RE.search(up)
    if dm:
        out["drivetrain"] = dm.group(1)
    am = AWD_RE.search(up)
    if am:
        out["awd"] = am.group(1)

    # Model + chassis: tokens between make and year.
    if ym:
        pre_year = rest[:ym.start()].strip()
    else:
        # No year — take tokens up to first trans/drive marker.
        cut = len(rest)
        for rx in (TRANS_RE, DRIVE_RE, AWD_RE):
            mm = rx.search(up)
            if mm:
                cut = min(cut, mm.start())
        pre_year = rest[:cut].strip()
    toks = pre_year.split()
    if toks:
        # Chassis = trailing alphanumeric token if it looks like one.
        if len(toks) > 1 and CHASSIS_RE.match(toks[-1]) and any(c.isdigit() for c in toks[-1]):
            out["chassis"] = toks[-1]
            out["model"] = " ".join(toks[:-1]) or None
        else:
            out["model"] = " ".join(toks) or None

    # Engine code: token right after year.
    if ym:
        after = rest[ym.end():].strip().split()
        if after and ENGINE_RE.match(after[0]) and after[0] not in ("AT", "MT", "CVT", "FF", "FR", "MR", "RR", "2WD", "4WD", "AWD"):
            out["engine"] = after[0]

    return out


# ----------------------------------------------------------------------------
# HTTP with retry/backoff
# ----------------------------------------------------------------------------

def get_json(url: str) -> dict | None:
    """GET with exponential backoff on 429/503. Returns parsed JSON or None."""
    for attempt in range(MAX_RETRIES):
        try:
            r = requests.get(url, headers=HEADERS, timeout=30)
            if r.status_code == 200:
                time.sleep(REQUEST_DELAY)
                return r.json()
            if r.status_code in (429, 503, 502, 500):
                sleep = min(BACKOFF_CAP, BACKOFF_BASE ** attempt)
                time.sleep(sleep)
                continue
            if r.status_code == 404:
                return None
            # Other codes: brief pause then retry.
            time.sleep(min(BACKOFF_CAP, BACKOFF_BASE ** attempt))
        except requests.RequestException:
            time.sleep(min(BACKOFF_CAP, BACKOFF_BASE ** attempt))
    return None


# ----------------------------------------------------------------------------
# Catalog pull (collection-enumeration method)
# ----------------------------------------------------------------------------

def list_collections(store: str) -> list[str]:
    handles = []
    page = 1
    while True:
        url = f"https://{store}.myshopify.com/collections.json?limit={PER_PAGE}&page={page}"
        data = get_json(url)
        if not data or not data.get("collections"):
            break
        for c in data["collections"]:
            h = c.get("handle")
            if h:
                handles.append(h)
        page += 1
        if page > 200:  # safety
            break
    return handles


def normalize_product(store: str, p: dict) -> dict | None:
    variants = p.get("variants") or []
    if not variants:
        return None
    v = variants[0]  # one product = one SKU on these stores
    price = v.get("price")
    try:
        price = float(price) if price is not None else None
    except (TypeError, ValueError):
        price = None
    cmp_at = v.get("compare_at_price")
    try:
        cmp_at = float(cmp_at) if cmp_at is not None else None
    except (TypeError, ValueError):
        cmp_at = None

    parsed = parse_title(p.get("title", ""))
    pid = str(p.get("id"))
    return {
        "id": pid,
        "store": store,
        "title": p.get("title"),
        "handle": p.get("handle"),
        "sku": v.get("sku"),
        "price": price,
        "compare_at": cmp_at,
        "available": bool(v.get("available")),
        "vendor": p.get("vendor"),
        "created_at": p.get("created_at"),
        "image": (p.get("images") or [{}])[0].get("src") if p.get("images") else None,
        "url": f"https://{store}.myshopify.com/products/{p.get('handle')}",
        **parsed,
    }


def pull_store(store: str) -> dict[str, dict]:
    """Return {product_id: normalized_product} for the full catalog."""
    products: dict[str, dict] = {}
    handles = list_collections(store)
    print(f"  [{store}] {len(handles)} collections")
    for i, h in enumerate(handles, 1):
        page = 1
        while True:
            url = (f"https://{store}.myshopify.com/collections/{h}/products.json"
                   f"?limit={PER_PAGE}&page={page}")
            data = get_json(url)
            if not data or not data.get("products"):
                break
            for p in data["products"]:
                norm = normalize_product(store, p)
                if norm:
                    products[norm["id"]] = norm  # dedupe by id
            page += 1
            if page > 100:  # per-collection safety (collections are small)
                break
        if i % 50 == 0:
            print(f"  [{store}] {i}/{len(handles)} collections, {len(products)} products")
    print(f"  [{store}] DONE: {len(products)} unique products")
    return products


# ----------------------------------------------------------------------------
# Snapshot / diff / events
# ----------------------------------------------------------------------------

def load_json(path: Path, default):
    if path.exists():
        opener = gzip.open if path.suffix == ".gz" else open
        with opener(path, "rt", encoding="utf-8") as f:
            return json.load(f)
    return default


def save_json(path: Path, obj, gz=False):
    path.parent.mkdir(parents=True, exist_ok=True)
    if gz:
        with gzip.open(path, "wt", encoding="utf-8") as f:
            json.dump(obj, f, separators=(",", ":"))
    else:
        with open(path, "w", encoding="utf-8") as f:
            json.dump(obj, f, separators=(",", ":"))


def latest_snapshot_path() -> Path | None:
    snaps = sorted(SNAP_DIR.glob("*.json*"))
    return snaps[-1] if snaps else None


def diff_snapshots(prev: dict, curr: dict, seen: dict, ts: str) -> list[dict]:
    """Emit events from prev->curr. prev/curr are {id: product}. seen is
    {store: {id: 1}} of every id ever observed (cold-start guard)."""
    events = []
    prev_ids = set(prev)
    curr_ids = set(curr)

    for pid in curr_ids:
        c = curr[pid]
        store = c["store"]
        seen_store = seen.setdefault(store, {})
        is_known = pid in seen_store

        if pid in prev:
            p = prev[pid]
            # Sale proxy: available true -> false
            if p.get("available") and not c.get("available"):
                events.append(_evt("sold", c, ts))
            # Restock: false -> true
            elif (not p.get("available")) and c.get("available"):
                events.append(_evt("restocked", c, ts))
            # Price change
            if p.get("price") is not None and c.get("price") is not None and p["price"] != c["price"]:
                e = _evt("price_change", c, ts)
                e["old_price"] = p["price"]
                e["new_price"] = c["price"]
                events.append(e)
        else:
            # Not in previous snapshot. Only "new" if never seen before AND in stock.
            if not is_known and c.get("available"):
                events.append(_evt("new", c, ts))
        seen_store[pid] = 1

    # Disappeared products = sold (only if previously in stock).
    for pid in prev_ids - curr_ids:
        p = prev[pid]
        if p.get("available"):
            events.append(_evt("sold", p, ts, disappeared=True))

    return events


def _evt(kind, prod, ts, disappeared=False):
    e = {
        "type": kind,
        "ts": ts,
        "id": prod["id"],
        "store": prod["store"],
        "title": prod.get("title"),
        "make": prod.get("make"),
        "model": prod.get("model"),
        "category": prod.get("category"),
        "part_label": prod.get("part_label"),
        "price": prod.get("price"),
        "url": prod.get("url"),
    }
    if disappeared:
        e["disappeared"] = True
    return e


# ----------------------------------------------------------------------------
# Main
# ----------------------------------------------------------------------------

def main():
    ts = dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%d")
    ts_full = dt.datetime.now(dt.timezone.utc).isoformat()
    print(f"=== Catalog scrape {ts_full} ===")

    # 1. Pull current full catalog for both stores.
    curr: dict[str, dict] = {}
    per_store_counts = {}
    for store in STORES:
        prod = pull_store(store)
        per_store_counts[store] = len(prod)
        curr.update(prod)

    # 2. Fail-loud guards.
    if not curr:
        print("FATAL: zero products pulled across all stores. Aborting (no commit).")
        sys.exit(1)
    for store, n in per_store_counts.items():
        if n == 0:
            print(f"FATAL: store {store} returned 0 products. Aborting.")
            sys.exit(1)

    prev_path = latest_snapshot_path()
    prev = {}
    if prev_path:
        prev_list = load_json(prev_path, [])
        prev = {p["id"]: p for p in prev_list}
        # Retention guard per store.
        prev_counts = {}
        for p in prev.values():
            prev_counts[p["store"]] = prev_counts.get(p["store"], 0) + 1
        for store, n in per_store_counts.items():
            old = prev_counts.get(store, 0)
            if old and n < old * MIN_RETENTION:
                print(f"FATAL: {store} collapsed {old} -> {n} (<{int(MIN_RETENTION*100)}%). "
                      f"Likely partial scrape. Aborting (no commit, no diff).")
                sys.exit(1)

    # 3. Load seen-ids (cold-start guard).
    seen = load_json(SEEN_FILE, {})
    first_run = not prev and not seen

    # 4. Diff -> events.
    if first_run:
        print("First run: seeding seen_ids, emitting ZERO sold events.")
        for pid, p in curr.items():
            seen.setdefault(p["store"], {})[pid] = 1
        new_events = []
    else:
        new_events = diff_snapshots(prev, curr, seen, ts)
        print(f"Events this run: {len(new_events)}")
        from collections import Counter
        print("  by type:", dict(Counter(e["type"] for e in new_events)))

    # 5. Persist. Snapshots are gzipped (diff source). The dashboard reads the
    #    compact dashboard.json below, NOT the full catalog, so we don't ship a
    #    30MB latest.json.
    snap_list = list(curr.values())
    save_json(SNAP_DIR / f"{ts}.json.gz", snap_list, gz=True)

    all_events = load_json(EVENTS_FILE, [])
    all_events.extend(new_events)
    save_json(EVENTS_FILE, all_events)

    save_json(SEEN_FILE, seen)

    # 6. Emit a lightweight dashboard payload (events + daily aggregates).
    #    The full snapshot is 30MB+; the dashboard only needs the velocity
    #    signal and current standing-state rollups, not every product.
    build_dashboard_payload(curr, all_events, per_store_counts)

    print(f"Snapshot: {len(snap_list)} products. Total events logged: {len(all_events)}.")
    print("=== done ===")


def build_dashboard_payload(curr: dict, events: list, store_counts: dict):
    """Write data/dashboard.json: compact rollups for the static dashboard."""
    from collections import Counter, defaultdict

    # Current standing-state rollups (in-stock catalog composition).
    in_stock = [p for p in curr.values() if p.get("available")]
    cat_stock = Counter(p["category"] for p in in_stock)
    make_stock = Counter((p.get("make") or "Unknown") for p in in_stock)
    store_stock = Counter(p["store"] for p in in_stock)

    # Event rollups by day.
    by_day = defaultdict(lambda: defaultdict(int))      # day -> type -> count
    sold_by_cat = defaultdict(lambda: defaultdict(int)) # day -> cat -> sold
    sold_by_make = Counter()
    sold_by_store = Counter()
    sold_by_cat_total = Counter()
    for e in events:
        d = e["ts"]
        by_day[d][e["type"]] += 1
        if e["type"] == "sold":
            sold_by_cat[d][e.get("category") or "Other / Misc"] += 1
            sold_by_make[e.get("make") or "Unknown"] += 1
            sold_by_store[e.get("store")] += 1
            sold_by_cat_total[e.get("category") or "Other / Misc"] += 1

    # Recent events feed (last 500, newest first).
    recent = sorted(events, key=lambda e: e["ts"], reverse=True)[:500]

    payload = {
        "generated": dt.datetime.now(dt.timezone.utc).isoformat(),
        "totals": {
            "catalog": sum(store_counts.values()),
            "in_stock": len(in_stock),
            "by_store": store_counts,
            "in_stock_by_store": dict(store_stock),
        },
        "stock_composition": {
            "category": dict(cat_stock.most_common()),
            "make": dict(make_stock.most_common(20)),
        },
        "events_by_day": {d: dict(v) for d, v in sorted(by_day.items())},
        "sold_by_day_category": {d: dict(v) for d, v in sorted(sold_by_cat.items())},
        "sold_totals": {
            "by_make": dict(sold_by_make.most_common(20)),
            "by_store": dict(sold_by_store),
            "by_category": dict(sold_by_cat_total.most_common()),
        },
        "recent_events": recent,
    }
    save_json(DATA / "dashboard.json", payload)
    print(f"Dashboard payload: {len(recent)} recent events, "
          f"{len(by_day)} active days.")


if __name__ == "__main__":
    main()
