# Publishing TSSI as a static site (Cloudflare Pages or Vercel)

This guide covers shipping the Thai Street Survival Index as a static
site. **Recommended for zero cost + commercial use:** Cloudflare Pages
(via GitHub Actions). **Alternative:** Vercel (see below). After setup,
the weekly update workflow is: edit `data/observations.csv`, commit, push.

Total time: ~20 minutes. Ongoing cost: **$0/month** (or ~$10/yr if you
want a custom domain).

---

## Why Cloudflare Pages


| Tier                 | Bandwidth      | Commercial use      | Builds      | Custom domain |
| -------------------- | -------------- | ------------------- | ----------- | ------------- |
| **Cloudflare Pages** | **Unlimited**  | Allowed             | 500 / month | Free          |
| GitHub Pages         | Soft 100 GB/mo | Allowed             | Free        | Free          |
| Vercel Hobby         | 100 GB/mo      | Non-commercial only | 6000 min/mo | Free          |
| Netlify Free         | 100 GB/mo      | Restricted          | 300 min/mo  | Free          |


Cloudflare's free tier has no bandwidth cap, is commercial-use friendly,
and delivers from 300+ edge locations with no cold starts. You'll want
it even once the project grows.

---

## Vercel (alternative)

The repo ships `[vercel.json](../vercel.json)`: it installs
`[requirements-static.txt](../requirements-static.txt)` (no Playwright — stays
under Vercel’s **245 MB** deployment limit), runs
`[scripts/build_static.py](../scripts/build_static.py)`, then serves `dist/`,
with **rewrites** so `/methodology` and `/api/v1/tssi/`* resolve to the
pre-rendered HTML/JSON (same URLs as the live FastAPI app).

### One-time setup

1. Sign up at [vercel.com](https://vercel.com) (free tier; GitHub login is fine).
2. **Add New… → Project** → import this GitHub repository.
3. Vercel will read `vercel.json`. Confirm:
  - **Framework Preset**: Other (or “No framework”).
  - **Install Command**: `npm install` (see [`package.json`](../package.json) — avoids Python serverless auto-detection)
  - **Build Command**: `npm run build` (runs pip + [`scripts/build_static.py`](../scripts/build_static.py))
  - **Output Directory**: `dist`
4. Deploy. Production URL will look like `https://<project>.vercel.app`.

If the build fails with **Python not found**, the Vercel image for your
project may not expose `python3` on `PATH`. Fix one of these ways:

- In **Project → Settings → Environment Variables**, add nothing extra
first; if it still fails, use **Option B** below.
- **Option B — build in GitHub Actions, deploy a folder** (same idea as
Cloudflare): keep the existing build job, then add a step that runs
`npx vercel deploy dist --prod --token $VERCEL_TOKEN` with a Vercel
[token](https://vercel.com/account/tokens). You can skip the install/build
on Vercel and only upload `dist/` as static files.

### Custom domain (Vercel)

Project → **Settings → Domains** → add `www.example.com` or apex; follow
DNS instructions (often a `CNAME` to `cname.vercel-dns.com`).

### Cost note (Hobby tier)

Vercel’s Hobby plan is **non-commercial** by its terms. For a personal
portfolio or research project that is fine; for a commercial product,
compare with Cloudflare Pages (commercial use allowed on the free tier).

---

## Prerequisites (Cloudflare path)

- A GitHub repository with this codebase pushed to `main`.
- An email address for the Cloudflare account (free).

That's it. You do NOT need Docker, a database, or a VPS.

---

## Step 1 — Create the Cloudflare Pages project

1. Sign up at [https://dash.cloudflare.com/sign-up](https://dash.cloudflare.com/sign-up) (free, no card).
2. In the left nav, click **Workers & Pages** -> **Create application**
  -> **Pages** -> **Connect to Git**.
3. Authorise Cloudflare to read your GitHub repo; pick this repo.
4. On the "Set up builds and deployments" screen:
  - **Project name**: `tssi` (this becomes your URL:
   `https://tssi.pages.dev`). If you pick something else, update
   `PROJECT_NAME` in [.github/workflows/deploy.yml](../.github/workflows/deploy.yml).
  - **Production branch**: `main`.
  - **Build command**: *leave blank* (we build in GitHub Actions, not
  Cloudflare's builder, so the build environment is fully
  reproducible and uses exactly the Python version pinned in the
  workflow).
  - **Build output directory**: `dist`.
5. Click **Save and Deploy**. The first deploy will fail because the
  Cloudflare builder doesn't run Python — that's expected; we'll push
   pre-built `dist/` from GitHub Actions in the next step.

---

## Step 2 — Create a Cloudflare API token

GitHub Actions needs a token scoped to push builds to Pages.

1. Go to [https://dash.cloudflare.com/profile/api-tokens](https://dash.cloudflare.com/profile/api-tokens).
2. Click **Create Token** -> **Custom token** (bottom of the page).
3. Fill in:
  - **Token name**: `tssi-pages-deploy`.
  - **Permissions**: Add one row:
    - **Account** -> **Cloudflare Pages** -> **Edit**.
  - **Account Resources**: Include -> pick your account.
  - **TTL**: leave blank (no expiry) or set as you prefer.
4. Click **Continue to summary** -> **Create Token**.
5. **Copy the token value immediately** — Cloudflare only shows it once.

You will also need your **Account ID**:

1. Go back to [https://dash.cloudflare.com/](https://dash.cloudflare.com/).
2. On any "Workers & Pages" or "Overview" page, the Account ID is shown
  in the right-hand sidebar. Copy it.

---

## Step 3 — Add the secrets to GitHub

1. In GitHub, open your repo -> **Settings** -> **Secrets and variables**
  -> **Actions** -> **New repository secret**.
2. Add both secrets:

  | Name                    | Value                      |
  | ----------------------- | -------------------------- |
  | `CLOUDFLARE_API_TOKEN`  | the token from step 2      |
  | `CLOUDFLARE_ACCOUNT_ID` | the account ID from step 2 |


The workflow at [.github/workflows/deploy.yml](../.github/workflows/deploy.yml)
reads both.

---

## Step 4 — Trigger the first real build

You have two ways to kick off the first successful deploy:

**Option A — push a trivial change**

```bash
git commit --allow-empty -m "ci: trigger first Cloudflare deploy"
git push
```

**Option B — run the workflow manually**

1. In GitHub -> **Actions** -> **Build and deploy TSSI** -> **Run workflow**
  -> **main** -> **Run workflow**.

Either way, in ~45 seconds your site is live at
`https://tssi.pages.dev` (or whatever project name you chose).

---

## Step 5 (optional) — Attach a custom domain

Cloudflare Pages gives you free HTTPS on any domain you own. If you
want `tssi.example.com` instead of `tssi.pages.dev`:

1. In Cloudflare dashboard: **Workers & Pages** -> your `tssi` project
  -> **Custom domains** -> **Set up a custom domain**.
2. Enter the domain (e.g. `tssi.example.com` or apex `example.com`).
3. Cloudflare walks you through the DNS record to add (a `CNAME` for
  subdomains, or `A`/`AAAA` for the apex via Cloudflare's proxy).

If you don't own a domain yet, the cheapest honest route is Cloudflare's
own registrar, which sells `.com` domains at wholesale (~$10/yr, no
markup). There is no other domain registrar that comes close on price
transparency. Go to [https://dash.cloudflare.com/?to=/:account/registrar](https://dash.cloudflare.com/?to=/:account/registrar).

---

## Weekly update workflow (the whole point)

Every Sunday — or whenever you do a retail price check — the operator
workflow is:

```bash
git pull
# open data/observations.csv, append FIVE rows (one per basket asset:
#   mahboonkrong_rice_5kg, mama_tomyum_pack, ezygo_kaphrao_box,
#   crystal_water_600ml, m150_bottle) for today's date, save
git commit -am "data: $(date +%Y-%m-%d) weekly observations"
git push
```

GitHub Actions sees the push, rebuilds `dist/`, deploys to Cloudflare.
Live in ~45 seconds, globally.

You can also preview locally first:

```bash
.venv/bin/python scripts/build_static.py --clean
cd dist && python -m http.server 8012
# open http://127.0.0.1:8012
```

---

## Troubleshooting

### "Error: Could not find a project with that name"

The project name in
[.github/workflows/deploy.yml](../.github/workflows/deploy.yml)
(`PROJECT_NAME`) must exactly match the Cloudflare Pages project slug.
Edit the env block in the workflow YAML if you used a different name.

### Build succeeds locally, fails in Actions

- Check the **Build static site** step in the failed run's logs.
- The most common cause is a new row in `data/observations.csv` with a
malformed date, missing column, or non-numeric price/weight. Runs in
CI exactly like a local build — reproduce with
`python scripts/build_static.py --clean -v` on your machine.

### Deploy succeeds, site shows 404

- Cloudflare Pages caches 404 responses aggressively. Hard-refresh the
browser (Cmd-Shift-R).
- Confirm `dist/index.html` is in the committed artifact by looking at
the **Build static site** step output — it prints the `dist/` tree
and the index.html size.

### I want to invalidate the Cloudflare CDN cache

Pushes auto-invalidate. If you want to force it:
**Cloudflare dashboard** -> your `tssi` project -> **Deployments** ->
find the latest deploy -> **...** -> **Retry deployment**.

---

## Real retail SKU URL discovery (once, before live scraping)

`app/scraper/assets.py` currently holds **five placeholder URLs** marked
`REPLACE`. Until a human operator replaces them with canonical product
pages, the Playwright scraper cannot produce real rows — the pipeline
falls back entirely to Wayback archives, seed anchors, and BoT-CPI
derivation.

The discovery task list (run once, commit the resulting assets.py):


| #   | Asset                              | Platform          | What to find                                                                 |
| --- | ---------------------------------- | ----------------- | ---------------------------------------------------------------------------- |
| 1   | Mahboonkrong Jasmine Rice 5 kg     | Lazada            | Canonical PDP URL for the 5 kg factory-sealed bag (ตรามาบุญครอง, 5 กิโลกรัม) |
| 2   | Mama Tom Yum Koong pack            | Lazada            | Current single-pack PDP (55 g net weight, 7 THB list)                        |
| 3   | 7-Eleven EZYGO Kaphrao chilled box | 7-Eleven Delivery | Basil Pork/Chicken EZYGO chilled box PDP                                     |
| 4   | Crystal Drinking Water 600 ml      | Shopee            | Standard 600 ml PET bottle, single-unit PDP                                  |
| 5   | M-150 Energy Drink 150 ml          | Shopee            | Original brown glass bottle, 150 ml, single-unit PDP                         |


For each URL, also verify the current `price_selector` and
`weight_selector` CSS paths in `assets.py` still resolve against the
rendered DOM. Update them if the vendor has restructured the page.

Once the 5 real URLs are committed, run `python -m app.scraper.pipeline`
once locally to populate real rows, then re-run the Wayback backfill
(`python scripts/backfill_to_csv.py --source wayback`) so the historical
tail is archived against the same canonical URLs.

---

## Alternative: GitHub Pages

If you want to stay entirely inside GitHub (no Cloudflare account at
all), you can point the workflow at `peaceiris/actions-gh-pages@v3`
instead of `cloudflare/pages-action@v1`. The trade-offs: GitHub Pages
has a soft 100 GB/mo bandwidth cap and serves from a smaller CDN
footprint. For a project at TSSI's expected traffic, either works fine.

Ask if you want the Pages variant of the workflow checked in.