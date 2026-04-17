# Thai Street Survival Index (TSSI)

Proprietary real-time macroeconomic indicator tracking localized shrinkflation
in Thailand, built on a five-asset PPU micro-basket mapped to the NSO
Household Socio-Economic Survey (COICOP 2018):

| Asset | Weight | COICOP 2018 |
| --- | --- | --- |
| Mahboonkrong Jasmine Rice 5 kg | 25% | 01.1.1 Cereals & Cereal Products |
| Mama Tom Yum Koong instant noodles | 15% | 01.1.1 Cereals & Cereal Products |
| 7-Eleven EZYGO Kaphrao chilled box | 35% | 01.1.9 Ready-made Food / Prepared |
| Crystal Drinking Water 600 ml | 15% | 01.2.2 Non-alcoholic Beverages |
| M-150 Energy Drink 150 ml | 10% | 01.2.2 Non-alcoholic Beverages |

The service scrapes daily nominal prices and net weights from Thai retail
surfaces (Lazada / Shopee / 7-Eleven), computes **Price-Per-Unit (PPU)** for
each staple, applies a 7-day rolling-median smoother per asset, and
aggregates them into a composite index normalized to **January 2020 = 100**.
Forward-fill imputation is capped at 3 consecutive days; beyond that an
asset is flagged `structural_missing` and the composite weight is
renormalized across remaining assets. The full conceptual framework
(Executive Summary, SCR framing, Methodology, Alternative Proxy Analysis,
Appendix A.1–A.4, Appendix B API contract) is rendered live at
`/methodology`.

---

## Phase 1 – Infrastructure (this commit)

What's in place:

- `docker-compose.yml` provisioning TimescaleDB (`latest-pg15`) and the
  FastAPI service, both pinned to `linux/arm64` for native Apple Silicon.
- `requirements.txt` with FastAPI / Uvicorn / Playwright / Pandas / asyncpg /
  SQLAlchemy / Alembic and support libs.
- `Dockerfile` based on the official Playwright-Python image so Chromium is
  pre-installed and kept in sync with the Playwright client version.
- `db/init/01_init_hypertable.sql` – idempotent bootstrap that creates the
  `tssi_raw_data` hypertable (composite PK on `time`) with a generated
  `ppu` column plus a `tssi_daily_ppu` continuous aggregate.
- `app/db/models.py` – SQLAlchemy 2.0 ORM mirror of that schema.
- `app/db/session.py` – async engine + session factory.
- `app/core/config.py` – typed settings via `pydantic-settings`.
- `app/main.py` – minimal FastAPI shell with `/health` and `/ready`.
- `alembic.ini` + `alembic/env.py` wired to app settings for future migrations.

## Phase 2 – Scraper + Calculation engine (this commit)

- `app/scraper/playwright_client.py` – async `fetch_asset_data(url,
  price_selector, weight_selector)` that launches headless Chromium,
  threads traffic through an env- or kwargs-configured proxy, waits on
  every selector (never `time.sleep`), and detects CAPTCHA challenges
  before they masquerade as timeouts.
- `app/scraper/parser.py` – deterministic price + weight extractor that
  handles ฿ / THB / comma-grouped / Thai-script tokens and converts
  kg / mg / l / cl / ลิตร / กรัม to canonical g / ml.
- `app/scraper/assets.py` – the five-asset registry (Rice 25%, Mama 15%,
  EZYGO 35%, Water 15%, M-150 10%) with placeholder Lazada / Shopee /
  7-Eleven URLs and CSS selectors; a registry-level assertion guarantees
  the basket weights sum to 1.00 on import.
- `app/scraper/pipeline.py` – orchestrator with bounded tenacity retry on
  transient timeouts (NOT on CAPTCHA), single-transaction idempotent
  upserts via `INSERT ... ON CONFLICT DO NOTHING`, and structured
  `IngestionReport` output.
- `app/calc/engine.py` – Pandas pipeline that pivots per-asset daily PPU
  to a wide frame, forward-fills with a **3-day cap** (flagging any
  gap beyond as `structural_missing` and dropping the weight from the
  composite with dynamic renormalization), applies a **7-day rolling
  median** smoother per asset, computes the weighted composite PPU,
  normalizes against the baseline date (**default 2020-01-01 = 100**),
  and emits both the index value and the symmetric percent change
  `2(C-C₀)/(C+C₀)`.

Verified offline:

- `pyflakes app/` is clean.
- Parser smoke tests pass for ฿ / THB / Thai-script / comma-grouped /
  kg→g / l→ml / unit-mismatch / unknown-unit cases.
- Calc engine end-to-end tests pass against a stubbed `AsyncSession` —
  composite arithmetic is exact, ffill + `imputed` flag behave correctly,
  empty datasets return empty series, and baseline re-anchoring works
  when the request window starts after the baseline.

## Phase 3 – API surface (this commit)

- `app/api/deps.py` – `require_api_key` dependency using
  `hmac.compare_digest` for constant-time comparison against
  `settings.api_key`. Returns `401` + `WWW-Authenticate: APIKey` on
  missing or mismatched header.
- `app/api/v1/tssi.py` – three read endpoints with typed query params
  and range validation:
  - `GET /api/v1/tssi/current` – returns `CurrentIndexResponse`
    (baseline metadata + latest `IndexPoint`).
  - `GET /api/v1/tssi/historical?start_date=&end_date=&baseline_date=`
    – returns the full `IndexResponse` time-series; rejects reversed
    ranges and end-before-baseline requests with `400`.
  - `GET /api/v1/tssi/assets/ppu?start_date=&end_date=` – returns the
    per-asset PPU spread with an `imputed` flag on ffilled rows.
- `app/api/v1/scraper.py` – `POST /api/v1/scraper/trigger` protected by
  the API-key dep, serialized through a module-level `asyncio.Lock` so
  concurrent triggers get a clean `409 Conflict` instead of racing two
  Chromium instances.
- `app/api/v1/router.py` – aggregate `api_router` mounted at `/api/v1`
  and wired into `app.main` alongside the existing `/health` + `/ready`
  probes (the latter now returns `503` when the DB is unreachable
  instead of crashing the process).

Verified offline with FastAPI's `TestClient`:

- Every endpoint returns the correct status + body.
- `historical` composite / index / sym-pct values are computed
  deterministically with the Phase B fractional 7-day rolling median and
  3-day ffill cap applied to the 5-asset basket.
- `assets/ppu` correctly flags imputed rows.
- `scraper/trigger` returns `401` on missing key, `401` on wrong key,
  `200` with an `IngestionReport` on success, and `409` when the
  ingestion lock is already held.
- `pyflakes app/` stays clean.

## Try it

```bash
cp .env.example .env
docker compose up --build

curl http://localhost:8000/api/v1/tssi/current
curl "http://localhost:8000/api/v1/tssi/historical?start_date=2024-01-01&end_date=2024-03-01"
curl "http://localhost:8000/api/v1/tssi/assets/ppu"
curl -X POST -H "X-API-Key: $API_KEY" http://localhost:8000/api/v1/scraper/trigger
```

OpenAPI docs are served at http://localhost:8000/docs.

---

## Phase 6 – Public website (`/`)

A server-rendered Jinja2 page at the root (`GET /`) presents TSSI in a format
modeled after a country-ranking index page (think: The Economist's Big Mac
Index), but single-country focused:

- A hero with the **live composite index value**, the symmetric percent
  change pill, and the baseline date.
- Three **basket cards** for Mama Tom Yum Koong, M-150, and 7-Eleven EZYGO
  Kaphrao showing baseline PPU, current PPU, weight pill, and delta.
- A Chart.js **historical series** with the composite (filled amber) plus
  each asset normalized to its own first observation.
- A **daily values table** with per-asset PPUs and an `FFILL` pill
  highlighting forward-filled (missed-scrape) rows.
- A **methodology** section with the canonical six-step definition and
  every API endpoint.

The page is powered by the same calc engine as the JSON API, so the HTML
view and `/api/v1/tssi/*` always agree.

### Demo mode (no database required)

Running the public site is normally a `docker compose up` away, but for a
hosted demo or a quick local preview without TimescaleDB you can set:

```bash
DEMO_MODE=true DEMO_WINDOW_DAYS=2200 API_KEY=demo-secret \
  .venv/bin/uvicorn app.main:app --host 127.0.0.1 --port 8000
```

That will:

1. Auto-anchor `TSSI_BASELINE_DATE` to `today − (DEMO_WINDOW_DAYS − 1)`
   (Asia/Bangkok). Default `DEMO_WINDOW_DAYS=2200` gives ~6 years of
   demo history, starting mid-2020 when all five basket SKUs were
   plausibly on the market.
2. Generate an **era-aware** synthetic series — piecewise-linear across
   four regimes that mirror real Thai retail dynamics:
   - Pre-shock stable (2020-01 → 2020-06), multiplier x1.00.
   - Pandemic-era mild deflation (2020-07 → 2021-12), trough x0.97.
   - Post-Ukraine inflation resurgence (2022-01 → 2023-12), x1.06.
   - Shrinkflation era with per-asset divergence (2024-01 → today):
     Mama x1.10, M-150 x1.12, EZYGO x1.14.
   Deliberate missed-scrape days are sprinkled throughout so the
   `FFILL` imputation path stays visible in the UI.
3. Swap the `get_session` dependency for an in-memory feed so **both**
   the HTML page at `/` and the JSON API at `/api/v1/tssi/*` return
   matching numbers.

The chart at `/` thins the series to at most 500 parallel-strided
points so a six-year window still renders crisply, and collapses x-axis
tick labels to `YYYY-MM` when the window exceeds one year. The daily
table below the chart keeps raw daily values for the last 60 days.

This gives a hand-off-able preview of the full surface area without
provisioning Docker. For a shorter demo, override the window:

```bash
DEMO_MODE=true DEMO_WINDOW_DAYS=365 API_KEY=demo-secret \
  .venv/bin/uvicorn app.main:app --host 127.0.0.1 --port 8000
```

---

## Phase 7 – Historical backfill pipeline

Live scrapes only capture the present. Phase 7 ships a second ingestion
track that reconstructs the historical tail of every basket SKU and
writes it into the same `tssi_raw_data` hypertable, distinguished only
by a **namespaced `platform_source`** so the schema stays flat:

| `platform_source`      | Meaning                                              | Bucket     |
| --- | --- | --- |
| `lazada` / `shopee` / `7eleven` | Live Playwright scrape (Phase 2)           | `scrape`   |
| `wayback:lazada` …     | Internet Archive snapshot of the retail surface      | `archive`  |
| `seed:manual`          | Hand-curated anchor from receipts / news / PDFs      | `seed`     |
| `derived:bot_cpi`      | CPI-anchored synthetic fill between seed anchors     | `derived`  |

The three adapters live in `app/backfill/`:

- `app/backfill/wayback.py` — queries the **Wayback CDX** API per-URL
  window, downloads archived HTML, and extracts price + weight via the
  same `app/scraper/parser.py` helpers the live scraper uses.
- `app/backfill/seed.py` — CSV importer for hand-curated anchors. The
  skeleton at `data/seed/tssi_seed.csv` ships with column documentation
  and comment-row support so a reviewer can paste real data in place.
- `app/backfill/bot_cpi.py` — reads the seed anchors and the monthly
  BoT food-and-beverage CPI series at
  `data/cpi/thailand_food_bev_cpi.csv`, then emits a derived monthly
  PPU for every month without a real anchor:
  `ppu_m = anchor_ppu · (cpi_m / anchor_cpi)`.

All three satisfy the `HistoricalSource` protocol in
`app/backfill/base.py` and flow through the existing idempotent
`ON CONFLICT DO NOTHING` insert, so re-running is always safe.

The same three adapters also have a CSV sibling CLI at
`scripts/backfill_to_csv.py` that writes to `data/observations.csv`
(the source-of-truth file consumed by the static publishing pipeline —
see Phase 8 below).

### CLI

```bash
# Preview what the seed CSV would insert.
python scripts/backfill.py --source seed --dry-run

# Backfill every adapter for the last six years.
python scripts/backfill.py --source all --since 2020-01-01

# Wayback-only for a single window.
python scripts/backfill.py --source wayback --since 2022-01-01 --until 2023-12-31
```

### API honors provenance

`GET /api/v1/tssi/historical?verified_only=true` (and the mirror flag
on `/api/v1/tssi/assets/ppu`) drops `derived:*` rows from the composite
so the returned series reflects only scraped, archived, and seeded
anchors. The public site at `/` also renders a **Source mix** panel
under the chart showing per-year row counts by provenance bucket, so a
visitor can see exactly where each region of the series came from.

---

## Phase 8 – Static publishing (zero-cost hosting)

For public hosting we skip the live database entirely and ship TSSI as
a pre-rendered static site backed by a single CSV. No scraper, no
Postgres, no demo mode — the Jinja template, CSS, chart, and calc
engine all stay, they just run **at build time** instead of per
request.

| Artifact | Role |
| --- | --- |
| `data/observations.csv` | **Single source of truth.** Every row = one PPU observation on its day. Edit this file to update the index. |
| `scripts/backfill_to_csv.py` | One-time seed: runs the Phase 7 adapters (seed, Wayback, BoT CPI) and appends their output to `data/observations.csv`. |
| `app/db/csv_session.py` | In-memory `AsyncSession` shim that answers the three SQL statements the calc engine issues, sourced from the CSV. |
| `scripts/build_static.py` | Renders `/` and the `/api/v1/tssi/*` JSON endpoints via FastAPI's `TestClient` into a self-contained `dist/` tree. |
| `vercel.json` + `package.json` + `requirements-static.txt` | Vercel: `npm run build` runs the Python static generator (avoids Python serverless auto-detection) + URL rewrites. |
| `.github/workflows/deploy.yml` | On push to `main`, rebuilds `dist/` and publishes it to Cloudflare Pages. |
| `docs/PUBLISHING.md` | One-time setup for **Cloudflare Pages** or **Vercel** (free tiers). |

### Weekly update workflow

Every Sunday (or whenever you do a retail price check):

```bash
git pull
# append three rows to data/observations.csv for today's date
git commit -am "data: $(date +%Y-%m-%d) observations"
git push
```

GitHub Actions rebuilds `dist/` and redeploys to Cloudflare Pages in
~45 seconds. No server to pay for, no DB to maintain, no cold starts.

### Preview locally

```bash
.venv/bin/python scripts/backfill_to_csv.py --source seed --source cpi --since 2020-01-01
.venv/bin/python scripts/build_static.py --clean -v
cd dist && python -m http.server 8012
# open http://127.0.0.1:8012
```

### Going live

See [docs/PUBLISHING.md](docs/PUBLISHING.md):

- **Cloudflare Pages** — GitHub Actions builds `dist/` and deploys (no Python on Cloudflare’s servers).
- **Vercel** — connect the repo; `vercel.json` runs the same Python build, or build in CI and upload `dist/`.

### Cost

| Component | Free tier | Limit |
| --- | --- | --- |
| Cloudflare Pages | Forever | Unlimited bandwidth, 500 builds / mo |
| GitHub Actions | Forever (public repo) | 2000 minutes / mo |
| Custom domain | Optional | ~$10 / yr via Cloudflare Registrar |

**Total ongoing cost: $0.**

---

## Quick start

```bash
cp .env.example .env
docker compose up --build
```

Then:

```bash
curl http://localhost:8000/health
curl http://localhost:8000/ready
```

The TimescaleDB instance is reachable on `localhost:5432` with credentials
from your `.env`.

## Layout

```
.
├── app/
│   ├── core/config.py         # Pydantic settings
│   ├── db/
│   │   ├── base.py            # Declarative Base
│   │   ├── models.py          # TSSIRawData ORM model
│   │   └── session.py         # Async engine + get_session dep
│   └── main.py                # FastAPI bootstrap (expanded in later phases)
├── alembic/                   # Migration environment
│   ├── env.py
│   ├── script.py.mako
│   └── versions/
├── db/init/01_init_hypertable.sql   # Runs on first DB boot
├── Dockerfile                 # linux/arm64 Playwright-Python base
├── docker-compose.yml
├── requirements.txt
└── .env.example
```
