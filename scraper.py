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
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
from requests.adapters import HTTPAdapter

# Force line-buffered, unbuffered stdout so every print() reaches the GitHub
# Actions log the instant it runs — not held in a buffer that's lost if the
# job is killed. This is why earlier runs showed an empty log: buffered output
# never flushed before the timeout cancelled the process.
try:
    sys.stdout.reconfigure(line_buffering=True, write_through=True)
    sys.stderr.reconfigure(line_buffering=True, write_through=True)
except Exception:
    pass

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

REQUEST_DELAY = 0.0           # no artificial delay; the pool paces itself
MAX_RETRIES = 3               # per-request retry budget (was 6 — too many on
                              # a throttled store, each retry burns the timeout)
BACKOFF_BASE = 1.6            # gentler exponential backoff
BACKOFF_CAP = 6.0            # max sleep between retries (was 30 — a single
                              # throttled request could otherwise stall ~minutes)
REQUEST_TIMEOUT = 12          # per-request hard timeout (was 30 — throttled
                              # requests hung to the wall and doubled on retry)
PER_PAGE = 250                # Shopify hard max
WORKERS = 5                   # concurrent collection fetches per store.
                              # 8 was too aggressive — it triggered MORE store
                              # throttling (60s+ stalls). 5 is the balance:
                              # enough concurrency to be fast, gentle enough to
                              # avoid rate-limit walls.
SAFETY_MINUTES = 30           # wall-clock budget. The tuned pull finishes in
                              # long the store is pathologically throttled, so we
                              # abort WITHOUT committing rather than risk a partial
                              # snapshot being misread as mass "sold" events.
                              # Sits under the 60-min job timeout for a clean exit.
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

# Shared session with a connection pool sized for our worker count, so
# parallel requests reuse TCP connections instead of reopening each time.
SESSION = requests.Session()
SESSION.headers.update(HEADERS)
_adapter = HTTPAdapter(pool_connections=WORKERS * 2, pool_maxsize=WORKERS * 2)
SESSION.mount("https://", _adapter)
SESSION.mount("http://", _adapter)


class FetchError(Exception):
    """Raised when a request fails after exhausting retries (throttle/network).
    Distinct from a 404, which legitimately means 'nothing here'. This lets the
    caller avoid mistaking a throttled response for an empty collection — the
    bug that caused ~34% undercounts and tripped the retention guard."""


def get_json(url: str):
    """GET with exponential backoff. Returns parsed JSON, or None for a real
    404 (definitively empty). Raises FetchError if all retries are exhausted."""
    last_status = None
    for attempt in range(MAX_RETRIES):
        try:
            r = SESSION.get(url, timeout=REQUEST_TIMEOUT)
            if r.status_code == 200:
                if REQUEST_DELAY:
                    time.sleep(REQUEST_DELAY)
                return r.json()
            if r.status_code == 404:
                return None  # genuinely nothing here
            last_status = r.status_code
            # 429/503/502/500 and anything else: back off and retry.
            time.sleep(min(BACKOFF_CAP, BACKOFF_BASE ** attempt))
        except requests.RequestException as e:
            last_status = repr(e)
            time.sleep(min(BACKOFF_CAP, BACKOFF_BASE ** attempt))
    # Exhausted retries — signal a real failure, do NOT return None (which the
    # caller would read as "empty collection").
    raise FetchError(f"{url} failed after {MAX_RETRIES} retries (last={last_status})")


# ----------------------------------------------------------------------------
# Catalog pull (collection-enumeration method)
# ----------------------------------------------------------------------------

def list_collections(store: str) -> list[str]:
    """Enumerate all collection handles. Each page is retried hard; if a page
    truly can't be fetched we raise rather than silently returning a short list
    (a short list would drop whole collections and undercount the catalog)."""
    handles = []
    page = 1
    while True:
        url = f"https://{store}.myshopify.com/collections.json?limit={PER_PAGE}&page={page}"
        # Retry this page a few times on FetchError before giving up the run.
        data = None
        for tries in range(3):
            try:
                data = get_json(url)
                break
            except FetchError as e:
                if tries == 2:
                    raise FetchError(f"list_collections[{store}] page {page}: {e}")
                time.sleep(3 * (tries + 1))
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


def fetch_collection(store: str, handle: str) -> dict[str, dict]:
    """Fetch every product in one collection (paginated). Returns {id: product}.

    Raises FetchError if a page fails after retries — the caller treats that as
    a failed collection (to retry later), NOT as an empty one. Silently treating
    a throttled response as empty was the cause of catalog undercounts."""
    out: dict[str, dict] = {}
    page = 1
    while True:
        url = (f"https://{store}.myshopify.com/collections/{handle}/products.json"
               f"?limit={PER_PAGE}&page={page}")
        data = get_json(url)            # may raise FetchError → propagates up
        if data is None:               # genuine 404 — collection gone/empty
            break
        if not data.get("products"):   # legitimately no (more) products
            break
        for p in data["products"]:
            norm = normalize_product(store, p)
            if norm:
                out[norm["id"]] = norm
        page += 1
        if page > 100:  # per-collection safety (collections are small)
            break
    return out


def pull_store(store: str, deadline: float | None = None) -> tuple[dict[str, dict], bool]:
    """Return ({product_id: normalized_product}, complete).

    Collections are fetched concurrently (WORKERS threads). This is the hot
    path: a serial pull of ~770 collections across both stores runs >90 min
    and gets killed by GitHub's job ceiling; the parallel pull finishes in a
    few minutes. Dedupe by product id across overlapping collections.

    `deadline` is a time.monotonic() value; if it passes mid-pull we stop
    collecting and return complete=False so the caller can abort without
    committing a partial snapshot.
    """
    products: dict[str, dict] = {}
    handles = list_collections(store)
    print(f"  [{store}] {len(handles)} collections; fetching with {WORKERS} workers")

    def run_batch(batch, workers, budget):
        """Fetch a batch of collection handles concurrently.
        Returns (failed_handles, timed_out)."""
        nonlocal products
        failed_local = []
        timed_out = False
        ex = ThreadPoolExecutor(max_workers=workers)
        futs = {ex.submit(fetch_collection, store, h): h for h in batch}
        seen = 0
        try:
            for fut in as_completed(futs, timeout=budget):
                h = futs[fut]
                try:
                    for pid, p in fut.result().items():
                        products[pid] = p
                except Exception as e:
                    failed_local.append(h)
                    if len(failed_local) <= 5:
                        print(f"  [{store}] collection {h!r} failed: {e}")
                seen += 1
                if seen % 50 == 0:
                    print(f"  [{store}] {seen}/{len(batch)} collections, "
                          f"{len(products)} products")
        except TimeoutError:
            timed_out = True
            print(f"  [{store}] SAFETY: {SAFETY_MINUTES}m budget hit "
                  f"({seen}/{len(batch)} done); abandoning rest.")
        finally:
            ex.shutdown(wait=False, cancel_futures=True)
        return failed_local, timed_out

    # --- Pass 1: all collections ---
    budget = None if deadline is None else max(1.0, deadline - time.monotonic())
    failed, timed_out = run_batch(handles, WORKERS, budget)
    complete = not timed_out

    # --- Pass 2: retry failed collections once, gently (throttle recovery) ---
    # Throttle-dropped collections are the main undercount cause; a cooldown +
    # lower concurrency recovers almost all of them and keeps us above the
    # retention guard.
    if failed and complete:
        print(f"  [{store}] retrying {len(failed)} failed collection(s) "
              f"after cooldown…")
        time.sleep(10)
        budget2 = None if deadline is None else max(1.0, deadline - time.monotonic())
        if budget2 is None or budget2 > 30:  # only if we have time left
            still_failed, timed_out2 = run_batch(failed, max(2, WORKERS // 2), budget2)
            failed = still_failed
            if timed_out2:
                complete = False

    if failed:
        print(f"  [{store}] WARNING: {len(failed)} collection(s) still failed "
              f"after retry.")
    print(f"  [{store}] DONE: {len(products)} unique products"
          f"{'' if complete else ' (INCOMPLETE — time budget hit)'}")
    return products, complete


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
    print(f"=== scraper v2 | workers={WORKERS} timeout={REQUEST_TIMEOUT}s "
          f"retries={MAX_RETRIES} backoff_cap={BACKOFF_CAP}s "
          f"safety={SAFETY_MINUTES}m ===")
    sys.stdout.flush()

    run_start = time.monotonic()
    deadline = run_start + SAFETY_MINUTES * 60

    # 1. Pull current full catalog for both stores.
    curr: dict[str, dict] = {}
    per_store_counts = {}
    for store in STORES:
        prod, complete = pull_store(store, deadline=deadline)
        # Abort WITHOUT committing if the pull was cut short — a partial
        # snapshot would make every un-fetched product look "sold" tomorrow.
        if not complete:
            print(f"FATAL: {store} pull incomplete (hit {SAFETY_MINUTES}m safety "
                  f"budget). Aborting with no commit so we never write a partial "
                  f"snapshot. Re-run the workflow; the stores were unusually slow.")
            sys.exit(1)
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


CATALOG_FIELDS = ("id", "store", "title", "make", "model", "category",
                  "part_label", "price", "available", "url", "created_at", "vendor")


def build_dashboard_payload(curr: dict, events: list, store_counts: dict):
    """Write two files:
      - data/dashboard.json : compact aggregates (loads instantly; every tab's
        summary numbers + the full recent-event log).
      - data/catalog.json   : trimmed full catalog (one row per listing, no
        images) for the Catalog / Categories / Raw-data tables.
    All aggregates are kept per-store so the dashboard's store toggle
    (Epartsworld / Kaiho / All) can slice without re-fetching.
    """
    from collections import defaultdict

    STORES_K = list(store_counts.keys())

    def store_buckets():
        return {s: 0 for s in STORES_K}

    # ---- Standing-state catalog composition, per store ----
    cat_comp = defaultdict(lambda: {s: {"listings": 0, "in_stock": 0,
                                        "value": 0.0} for s in STORES_K})
    make_in_stock = defaultdict(store_buckets)
    in_stock_count = store_buckets()
    in_stock_value = {s: 0.0 for s in STORES_K}
    catalog_value = {s: 0.0 for s in STORES_K}

    for p in curr.values():
        s = p["store"]
        cat = p.get("category") or "Other / Misc"
        price = p.get("price") or 0
        cat_comp[cat][s]["listings"] += 1
        catalog_value[s] += price
        if p.get("available"):
            cat_comp[cat][s]["in_stock"] += 1
            cat_comp[cat][s]["value"] += price
            in_stock_count[s] += 1
            in_stock_value[s] += price
            make_in_stock[(p.get("make") or "Unknown")][s] += 1

    # ---- Event aggregates by day, per store, with KES value ----
    def day_store():
        return {s: defaultdict(lambda: {"count": 0, "value": 0.0})
                for s in STORES_K}
    by_day = defaultdict(day_store)
    sold_make = defaultdict(store_buckets)
    sold_cat = defaultdict(store_buckets)
    sold_cat_value = defaultdict(lambda: {s: 0.0 for s in STORES_K})

    for e in events:
        d = e["ts"]
        s = e.get("store")
        if s not in store_counts:
            continue
        t = e["type"]
        price = e.get("price") or 0
        rec = by_day[d][s][t]
        rec["count"] += 1
        rec["value"] += price
        if t == "sold":
            sold_make[e.get("make") or "Unknown"][s] += 1
            cat = e.get("category") or "Other / Misc"
            sold_cat[cat][s] += 1
            sold_cat_value[cat][s] += price

    events_by_day = {}
    for d, stores in sorted(by_day.items()):
        events_by_day[d] = {
            s: {t: dict(v) for t, v in types.items()}
            for s, types in stores.items()
        }

    recent = sorted(events, key=lambda e: e["ts"], reverse=True)[:1000]

    make_totals = sorted(make_in_stock.items(),
                         key=lambda kv: -sum(kv[1].values()))[:30]

    payload = {
        "generated": dt.datetime.now(dt.timezone.utc).isoformat(),
        "stores": STORES_K,
        "totals": {
            "catalog": store_counts,
            "in_stock": in_stock_count,
            "in_stock_value": {s: round(v) for s, v in in_stock_value.items()},
            "catalog_value": {s: round(v) for s, v in catalog_value.items()},
        },
        "category_composition": {
            cat: {s: {"listings": d[s]["listings"],
                      "in_stock": d[s]["in_stock"],
                      "value": round(d[s]["value"])} for s in STORES_K}
            for cat, d in cat_comp.items()
        },
        "make_in_stock": {m: dict(v) for m, v in make_totals},
        "events_by_day": events_by_day,
        "sold_by_make": {m: dict(v) for m, v in sorted(
            sold_make.items(), key=lambda kv: -sum(kv[1].values()))[:30]},
        "sold_by_category": {c: dict(v) for c, v in sold_cat.items()},
        "sold_value_by_category": {c: {s: round(val) for s, val in v.items()}
                                    for c, v in sold_cat_value.items()},
        "recent_events": recent,
    }
    save_json(DATA / "dashboard.json", payload)

    catalog = [{k: p.get(k) for k in CATALOG_FIELDS} for p in curr.values()]
    save_json(DATA / "catalog.json.gz", catalog, gz=True)

    import os
    size = os.path.getsize(DATA / "catalog.json.gz") / 1e6
    print(f"Dashboard payload: {len(recent)} recent events, "
          f"{len(events_by_day)} active days. Catalog: {len(catalog)} rows, "
          f"{size:.1f}MB gzipped.")


if __name__ == "__main__":
    main()
