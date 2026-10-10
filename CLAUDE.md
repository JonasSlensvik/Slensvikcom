# slensvikcom — static frontend for the ENEXT market platform

Owner: Jonas Slensvik (GitHub: JonasSlensvik). Plain HTML + Chart.js, IBM Plex Mono.
Served by GitHub Pages; data comes from PostgREST at **https://api.slensvik.com** (anon,
SELECT-only), which the iMac publishes through a Cloudflare tunnel.

**The full project brief lives in the ENEXT repo: `../ENEXT/CLAUDE.md`** — read it for the
backend architecture, scheduled jobs, DB access, and working agreements.

## Pages

- `index.html` — landing: live pulse strip (World regime + OSEBX/USDNOK/Brent/VIX/S&P), rotating brief line,
  bento gateway grid (World feature card with sparklines, Radar top conviction, Portfolio ex-dates, Galton,
  Dossier, Market Maker, Fastrente with live Norges Bank/NGB rates), data-health footer. Counts use
  `HEAD` + `Prefer: count=exact` — never download a table just to count it (the old Galton KPI pulled all
  ~20k `daily_close` rows on every load).
- `radar.html (formerly markedsradar.html)` — the radar's purpose is informed positioning ahead of company reports
  (Jonas, 2026-09-29). World-style shell since 2026-09-29 (header LED pills, sticky scrollspy rail, report ticker tape,
  glass panels, drawer). Order: **Horizon** (orbital hero — ring = days to report, size = positioning; the sky sits
  right of the copy while `.hero-sky` is absolutely positioned, centred when stacked) → **Season calendar** (weeks ×
  weekdays board of every results date, tinted by positioning) → **Event radar** (`api.report_radar` rows on a shared
  calendar axis with a faint price line from `api.daily_close_adj`; solid chips scored, dashed context) →
  **Report reactions** (season scoreboard, `api.report_radar_graded` / `api.report_radar_forward`; graded by
  confidence band, strong 60+ / firm 33–59 / faint < 33 (`RS_SURE` / `RS_STRONG`). The cuts were fixed
  2026-10-06 before any Q3 reaction: don't move them after seeing a season, see ENEXT `docs/event-radar.md`) → the
  event-agnostic **book** (`api.conviction`, top 6 a side until expanded) → **Track record** (tabs: forward / entries /
  backtest) → **The tape** (market intelligence) → **Watchtower**. Any report (calendar chip, tape item, row, orbital
  body) opens the **report drawer**. Its chart is TradingView **Lightweight Charts v5.2** (jsDelivr; Apache-2.0, keep
  the attribution logo). It has three panes on one time axis: candles on dividend-adjusted tape prices
  (`price_ohlcv` × `daily_close_adj` adj/close), volume, and short interest as a step line. Background bands mark
  the run-up, closed period, previous report, last session and report day. Dark-pool days sit on the candles and
  insider trades point down from above. `drLwc`; the SVG `drDots` is the fallback when the CDN script is missing.
  Then "why N", insider filings, analyst mix / targets / actions, past graded reactions.
  Deep link `radar.html#report=<ISIN>` opens it on load (handy for headless screenshots, with `?static=1`).
  dossier.html / portfolio.html still use Lightweight Charts **4.2.3**. v5 changed the API (`addSeries(X, …)`,
  `createSeriesMarkers`), so migrate them deliberately.
  The event-radar axis is sticky inside `.er-board`, which must stay `overflow: clip` — `hidden` makes the board a
  scroll container and shoves the axis over the first row. Backend runbook: `../ENEXT/docs/event-radar.md`.
  Do **not** edit this file with `sed` — it wiped the file once; use the Edit tool.
- `dark.html` — dark-pool explorer (2026-10-10), in the nav as "Dark pool". It loads every mid-point (mechanism 3) and
  off-book (mechanism 4) print of the last 90 days (~25k rows) from `api.trades_raw` (`is_dark`, partial index) into
  **DuckDB-WASM** in the tab. **Mosaic vgplot** cross-filters four views (notional per session, names, Oslo time of
  day, print size) and a table, with a mechanism menu. Every brush is a local SQL query and never touches the
  iMac. Pins: `@uwdata/vgplot@0.32.1` and `@duckdb/duckdb-wasm@1.33.1-dev57.0`, the build mosaic-core 0.32.1
  depends on. Keep them in step: the page creates the DuckDB instance and hands it to `vg.wasmConnector`.
  `trades_raw.trading_time` holds UTC wall-clock labelled +02, so the page converts it (`osloClock`). Avoid
  DuckDB reserved words as aliases (`names` broke the KPI query). First load is ~10 s, mostly the WASM download.
  Headless screenshots need real time (DuckDB runs in a Worker), not `--virtual-time-budget`.
- `portfolio.html` — personal portfolio tracker; 5D chart merges `api.intraday_live` for
  minute-level live resolution during Oslo market hours. World-style shell (sticky scrollspy rail,
  holdings ticker strip, header pills, deck HUD). **Income** section reads `api.dividends` for every ISIN
  ever transacted + `api.dividend_calendar`: income = NOK/share × shares held before the ex-date over the
  whole history (sold positions keep what they paid); paid / pending / announced / projected; cash
  calendar, runway, profile, ledger, market ex-dates. Ex-date events elsewhere on the page (desk chips,
  tape ribbons, deck moons) are synthesized from the same rows (`syncDividendEvents`). Backend runbook:
  `../ENEXT/docs/dividends.md`.
- `galton.html` — Galton weekly-strategy page; live tracker mirrors the portfolio's
  minute-level resolution (cold-start seeds `prev_close` at week-open, then merges
  `api.intraday_live`; refreshes every 3 min, paused during what-if slider previews).
- `fastrente.html` — Lånekassen fixed-rate decision page (rebuilt 2026-10-05): should you lock in the
  coming 10.–17. window? The offer is the average of the *previous* month's Wednesday Finansportalen
  top-5 readings, so when yields have moved since, you lock off-market and can break in the next window
  for a credited gain. Leads with a decision panel (verdict, per-tenor forecast with 80 % band, P(gain),
  net NOK after two months of fixed-vs-floating carry, dated plan, "already locked?" advice), then
  lockable-vs-market chart, calibration on every official cycle since 2021, live Norges Bank, the
  observation tracker. Data: `api.lanekassen_rates` (official), `rpc/world_history` (NGB 3/5/10Y +
  `pol.no`), and the Google Sheet's `fp_obs_*` Wednesday readings — the sheet's swap tab died
  2026-08-19 and its `rate_variabel` in June 2026; don't reuse them. Gain = Lånekassen's described
  method (same remaining term, fixed repayment plan); `?today=YYYY-MM-DD` replays another date's logic.
  Rule that shapes it: after a break you must float ≥ 2 months — no break-and-relock in one window.
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
- `api.lanekassen_rates` — official Lånekassen rates per period (`valid_from` = 1st of an odd month):
  `flytende` + `fast_3/5/10` (fixed offers null until published on the 10th of the window month).
  ENEXT World Intel source `lanekassen`, 6-hourly.
- Dividends: `api.dividends` (one row per isin × ex_date: amount + currency, `amount_nok` + `amount_nok_basis`,
  record/pay dates, `pay_date_est`, `status`, `sources`, `confirmed`), `api.dividend_calendar` (−14…+120 days,
  ticker, price, event yield), `api.dividend_profile` (TTM, frequency, growth, next), `api.dividend_announcements`.
- `api.intraday_live` — 15-min delayed live ticks (`isin, ts, price`), today's session.
- `api.conviction` — radar book, conviction v4 since 2026-09-29 (`model`, `score`, `ev_dark` / `ev_insider` /
  `ev_short`, `agreement`, `families`; v3's `pts_*` are NULL). "Why N?" breakdowns go through
  `INTEL.convictionBreakdown` (mirrored in ENEXT `conviction_alerts.py`) — keep the two identical.
  Query each side separately (`side=eq.LONG` / `side=eq.SHORT`): a shared top-N crowds shorts out.
  Formula + evidence: `../ENEXT/docs/conviction-v4.md`.
- Grading: `api.fwd_returns` (next-close entry, vs same-size peers, `pe1…pe120`); the radar's track records read the
  `pe*` columns of `api.radar_calls` / `api.radar_league` / `api.conviction_entries` and `api.conviction_forward`
  (the live snapshots). Never grade from the signal day's close or against the cap-weighted index.
- `api.galton_weights` / `api.galton_metrics` / `api.galton_matrices`.
- Event radar: `api.report_radar` (live, ~0.7 s — the point-in-time function runs per request; `positioning`,
  `direction`, `dark_days` / `ins_trades` jsonb series, `closed_from`, `runup_from`, `date_source` calendar|estimated),
  `api.report_radar_graded` (every report since 2026-06-15: positioning two sessions before + `reaction` = pe1,
  `reaction_5d` = pe5), `api.report_radar_forward` (the frozen snapshots graded — the honest record),
  `api.report_calendar` / `api.report_events` / `api.report_event_sessions`. Landing card reads `api.report_radar`.
- `api.catalyst_radar` — legacy (off the page since 2026-09-29); ENEXT `sql/catalyst_radar.sql`.

## Working agreements

- **Commit and push when the work is done and verified — don't ask.** Standing agreement with
  Jonas since 2026-09-30 (replaces the old ask-first rule). A push to `main` deploys the live site
  through GitHub Pages, so check the page first (headless screenshot) and bump the `?v=`
  cache-buster on any changed asset. Never commit secrets; confirm first before a force-push.
- **Reap headless Chrome / Playwright screenshot processes** after any browser check.
- Keep the iMac lean — avoid global installs. (See `../ENEXT/CLAUDE.md` for the rest.)
