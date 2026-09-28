# slensvikcom — static frontend for the ENEXT market platform

Owner: Jonas Slensvik (GitHub: JonasSlensvik). Plain HTML + Chart.js, IBM Plex Mono.
Served via the iMac's Cloudflare tunnel; data comes from PostgREST at
**https://api.slensvik.com** (anon, SELECT-only).

**The full project brief lives in the ENEXT repo: `../ENEXT/CLAUDE.md`** — read it for the
backend architecture, scheduled jobs, DB access, and working agreements.

## Pages

- `index.html` — landing.
- `radar.html (formerly markedsradar.html)` — dark-flow conviction shortlist (`api.conviction`) plus the
  Catalyst Radar section (`api.catalyst_radar` — event-first precursor scoring, added 2026-09-02). Do **not**
  edit this file with `sed` — it wiped the file once; use the Edit tool.
- `portfolio.html` — personal portfolio tracker; 5D chart merges `api.intraday_live` for
  minute-level live resolution during Oslo market hours.
- `galton.html` — Galton weekly-strategy page; live tracker mirrors the portfolio's
  minute-level resolution (cold-start seeds `prev_close` at week-open, then merges
  `api.intraday_live`; refreshes every 3 min, paused during what-if slider previews).
- `fastrente.html` — savings/rate tool.
- `world.html` + `assets/world.js` — World Intel: canvas globe (d3-geo + topojson, layers: tension,
  policy/real rates, inflation, growth, FX, equities, sanctions; chokepoints, disasters, market beacons,
  day/night), ranked intel brief, risk regime, cross-asset matrix, curves, central-bank map, FX,
  commodities/vol, geopolitics, chokepoints, sanctions, Norway lens, live data-health table. Reads the
  ENEXT `api.world_*` tables (see `../ENEXT/docs/world-intel.md`); `?static=1` freezes animation for
  screenshots.

## Useful API views

- World Intel: `api.world_board` (every series + snapshot metrics), `api.world_brief`, `api.world_regime`,
  `api.world_country_snapshot`, `api.world_hotspots`, `api.world_chokepoint_snapshot`,
  `api.world_source_health`, `rpc/world_history?ids={a,b}&since=YYYY-MM-DD` (compact arrays).
- `api.latest_price` — per-ISIN best price (live tick > EOD), `fidelity` = 'live'|'eod'.
- `api.intraday_live` — 15-min delayed live ticks (`isin, ts, price`), today's session.
- `api.conviction`, `api.galton_weights` / `api.galton_metrics` / `api.galton_matrices`.
- `api.catalyst_radar` — one row per (isin, upcoming event ≤35d), fused insider/dark/signal/whale/volume/trend/
  short precursor score (`catalyst_score` 0-100) + `direction_score`. Defined in ENEXT `sql/catalyst_radar.sql`.

## Working agreements

- **Never commit or push unless explicitly asked.** Summarize and ask when ready.
- **Reap headless Chrome / Playwright screenshot processes** after any browser check.
- Keep the iMac lean — avoid global installs. (See `../ENEXT/CLAUDE.md` for the rest.)
