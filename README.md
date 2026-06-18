# Kaiho Tracker — Competitor Inventory & Sales-Velocity Tracker

Snapshots two public Shopify used-Japanese-parts catalogs daily, diffs
consecutive snapshots, and surfaces a **directional sales-velocity index** plus
intake rate and price-change feed on a static dashboard.

Stores: `epartsworld-kenya` (~33k products), `kaihoindustry` (~11k).

## How it works
- **Catalog pull** uses collection enumeration (`/collections.json` →
  `/collections/{handle}/products.json`), deduped by product id — this bypasses
  the 100-page / 25k cap on `/products.json`.
- **Sale proxy**: a variant `available` flip `true → false` (or a product
  disappearing) = a "sold" event. Restocks, new listings, and price changes are
  also logged. Standing out-of-stock state is NOT a sale.
- **Cold-start guard**: `seen_ids.json` records every id ever seen, so
  long-standing OOS items aren't misread as sales on the first runs.
- **Fail-loud**: the scraper exits non-zero (Action goes red, nothing commits)
  if a store returns zero products or collapses below 70% of its last count —
  prevents a partial scrape reading as "sold everything".

## Files
- `scraper.py` — pull, parse, diff, write snapshot + events + dashboard payload
- `data/snapshots/YYYY-MM-DD.json.gz` — full daily catalog (diff source)
- `data/events.json` — append-only event log
- `data/seen_ids.json` — every id ever observed (per store)
- `data/dashboard.json` — compact rollup the dashboard reads (~KB, not 30MB)
- `index.html` — dashboard (GitHub Pages)
- `seed_baseline.py` — one-off: builds day-zero state from the catalog xlsx

## Setup
1. Create a repo, drop these files in.
2. The baseline (`2026-06-18`) is already seeded, so the first live run diffs
   against real state — no blind cold-start week.
3. Settings → Pages → deploy from branch root → dashboard lives at the Pages URL.
4. Actions runs daily at 02:30 UTC (05:30 EAT); trigger manually anytime via
   "Run workflow".

## Honest limitations
Directional proxy. Undercounts SKUs that sell and relist between snapshots;
cannot see multi-unit sales (no quantity is exposed); a slow daily cadence
misses same-day churn. Labelled a velocity *index*, not exact unit sales.
