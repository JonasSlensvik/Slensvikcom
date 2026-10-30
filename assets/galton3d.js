/* galton3d.js — the Galton page's lifetime record, its risk against OSEBX, and the 3D views (2026-10-30).

   Lifetime: every weekly book (api.galton_weights) is held from the Friday close before its Monday to the Friday
   close before the next book — the clock galton_weights.py's realized_return uses — and marked every day on
   api.daily_close_adj (dividend-adjusted). The path compounds to the weekly record; it is a few bp off in weeks where
   a dividend was registered after the week was booked. Benchmark: World Intel's eq.osebx (Oslo Børs Benchmark Index,
   a total-return index), on the same days. The last book is this week's and still live.
   Risk: daily measures (≈ 5 a week) and weekly ones (Friday to Friday). When the two disagree the page says how
   loosely each is pinned down: a volatility from n returns is uncertain by about 1/√(2(n−1)), ±16 % from 21 weeks
   against ±7 % from 109 days. (Checked 2026-10-30: the gap is not within-week reversal from stale closes — Galton's
   lag-1 daily autocorrelation was +0.06, OSEBX's 0.00.)
   3D: a small canvas projector (yaw/pitch, painter's order, back-face culling) draws the book as a skyline and the
   correlation matrices as landscapes; drag to turn. No motion under prefers-reduced-motion.
*/
(function () {
  'use strict';
  const API = 'https://api.slensvik.com';
  const get = (p) => (window.INTEL && INTEL.safeGet) ? INTEL.safeGet(p)
    : fetch(`${API}/${p}`).then((r) => (r.ok ? r.json() : null)).catch(() => null);
  const $ = (id) => document.getElementById(id);
  const esc = (s) => String(s ?? '').replace(/[&<>"]/g, (c) => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;' }[c]));
  const AMBER = '#f5a623', ICE = '#4fc3f7', GREEN = '#43e097', RED = '#f06060', BG = '#0b0f17';
  const REDUCED = !!(window.matchMedia && matchMedia('(prefers-reduced-motion: reduce)').matches);
  const HOVER = !!(window.matchMedia && matchMedia('(hover: hover)').matches);
  const DAY = 864e5;
  const addDays = (d, n) => new Date(Date.parse(d + 'T00:00:00Z') + n * DAY).toISOString().slice(0, 10);
  const pct = (v, d = 2) => (v == null || !isFinite(v) ? '—' : (v >= 0 ? '+' : '−') + Math.abs(v * 100).toFixed(d) + '%');
  const pctU = (v, d = 2) => (v == null || !isFinite(v) ? '—' : (v < 0 ? '−' : '') + Math.abs(v * 100).toFixed(d) + '%');
  const pp = (v, d = 2) => (v == null || !isFinite(v) ? '—' : (v >= 0 ? '+' : '−') + Math.abs(v * 100).toFixed(d) + ' pp');
  const num = (v, d = 2) => (v == null || !isFinite(v) ? '—' : (v < 0 ? '−' : '') + Math.abs(v).toFixed(d));
  const dLabel = (d) => new Date(d + 'T00:00:00Z').toLocaleDateString('en-GB', { day: 'numeric', month: 'short', timeZone: 'UTC' });
  const dLong = (d) => new Date(d + 'T00:00:00Z').toLocaleDateString('en-GB', { day: 'numeric', month: 'long', year: 'numeric', timeZone: 'UTC' });

  // ── colour helpers ─────────────────────────────────────────────────────
  const hex = (h) => [parseInt(h.slice(1, 3), 16), parseInt(h.slice(3, 5), 16), parseInt(h.slice(5, 7), 16)];
  const rgba = (h, a) => { const [r, g, b] = hex(h); return `rgba(${r},${g},${b},${a})`; };
  const mix = (h1, h2, t) => { const a = hex(h1), b = hex(h2); return `rgb(${a.map((v, i) => Math.round(v + (b[i] - v) * t)).join(',')})`; };
  const shadeRGB = (rgb, k) => rgb.map((v) => Math.max(0, Math.min(255, Math.round(v * k))));
  const css = (rgb, a = 1) => `rgba(${rgb[0]},${rgb[1]},${rgb[2]},${a})`;

  // ── statistics ─────────────────────────────────────────────────────────
  const mean = (a) => a.reduce((s, x) => s + x, 0) / a.length;
  const sd = (a) => { const m = mean(a); return Math.sqrt(a.reduce((s, x) => s + (x - m) ** 2, 0) / (a.length - 1)); };
  function quantile(a, q) {      // numpy's default ('linear')
    const s = [...a].sort((x, y) => x - y), pos = (s.length - 1) * q, lo = Math.floor(pos), hi = Math.ceil(pos);
    return s[lo] + (s[hi] - s[lo]) * (pos - lo);
  }
  function ddSeries(eq) { let peak = -Infinity; return eq.map((v) => { if (v > peak) peak = v; return v / peak - 1; }); }
  const cov = (a, b) => { const ma = mean(a), mb = mean(b); return a.reduce((s, x, i) => s + (x - ma) * (b[i] - mb), 0) / (a.length - 1); };
  function moment(a, k) { const m = mean(a), s = Math.sqrt(a.reduce((t, x) => t + (x - m) ** 2, 0) / a.length); return a.reduce((t, x) => t + ((x - m) / s) ** k, 0) / a.length; }
  function erfc(x) {             // Numerical Recipes erfcc, |error| < 1.2e-7
    const z = Math.abs(x), t = 1 / (1 + 0.5 * z);
    const r = t * Math.exp(-z * z - 1.26551223 + t * (1.00002368 + t * (0.37409196 + t * (0.09678418 + t * (-0.18628806 +
      t * (0.27886807 + t * (-1.13520398 + t * (1.48851587 + t * (-0.82215223 + t * 0.17087277)))))))));
    return x >= 0 ? r : 2 - r;
  }
  function kupiec(x, n, p) {     // proportion-of-failures test; LR ~ χ²(1), p-value = erfc(√(LR/2))
    if (!n) return null;
    const ph = x / n, ll0 = (n - x) * Math.log(1 - p) + x * Math.log(p);
    const ll1 = (x ? x * Math.log(ph) : 0) + (x < n ? (n - x) * Math.log(1 - ph) : 0);
    const lr = Math.max(0, -2 * (ll0 - ll1));
    return { lr, p: erfc(Math.sqrt(lr / 2)) };
  }
  function riskStats(r, eq, wk) {
    const q05 = quantile(r, 0.05), dd = ddSeries(eq);
    const tail = r.filter((x) => x <= q05);
    return {
      total: eq[eq.length - 1] / eq[0] - 1,
      ann: Math.pow(eq[eq.length - 1] / eq[0], 252 / r.length) - 1,
      mu: mean(r), vol: sd(r) * Math.sqrt(252), volW: wk.length > 2 ? sd(wk) * Math.sqrt(52) : null,
      var95: -q05, var99: -quantile(r, 0.01), es95: -mean(tail),
      var95W: wk.length > 2 ? 1.6449 * sd(wk) - mean(wk) : null,
      mdd: Math.min(...dd), dd: dd[dd.length - 1], worst: Math.min(...r), best: Math.max(...r),
      worstW: wk.length ? Math.min(...wk) : null, bestW: wk.length ? Math.max(...wk) : null,
      down: Math.sqrt(mean(r.map((x) => Math.min(x, 0) ** 2))) * Math.sqrt(252),
      up: r.filter((x) => x > 0).length / r.length, skew: moment(r, 3), kurt: moment(r, 4) - 3,
    };
  }

  // ═══ DATA: rebuild the daily path ══════════════════════════════════════
  async function load() {
    const W = await get('galton_weights?select=rebalance_date,isin,ticker,weight,risk_contribution,mu_g,sigma_g&order=rebalance_date.asc');
    if (!W || !W.length) return null;
    const books = [...new Set(W.map((r) => r.rebalance_date))].sort();
    const since = addDays(books[0], -12);
    const isins = [...new Set(W.map((r) => r.isin))];
    const [px, world, metrics] = await Promise.all([
      get(`daily_close_adj?isin=in.(${isins.map((s) => `"${s}"`).join(',')})&trade_date=gte.${since}&select=isin,trade_date,adj_close&order=trade_date.asc`),
      get(`rpc/world_history?ids=${encodeURIComponent('{eq.osebx,pol.no}')}&since=${since}`),
      get('galton_metrics?select=rebalance_date,realized_return,forecast_weekly_vol,forecast_var95,forecast_var99&order=rebalance_date.asc'),
    ]);
    if (!px || !px.length) return { W, books, metrics: metrics || [] };
    const days = [...new Set(px.map((r) => r.trade_date))].sort();
    const dix = new Map(days.map((d, i) => [d, i]));
    const P = {};
    for (const isin of isins) P[isin] = new Array(days.length).fill(null);
    for (const r of px) if (r.adj_close != null && +r.adj_close > 0) P[r.isin][dix.get(r.trade_date)] = +r.adj_close;
    for (const isin of isins) { let last = null; const a = P[isin]; for (let i = 0; i < a.length; i++) { if (a[i] == null) a[i] = last; else last = a[i]; } }
    const lastLE = (d) => { let lo = 0, hi = days.length - 1, ans = -1; while (lo <= hi) { const m = (lo + hi) >> 1; if (days[m] <= d) { ans = m; lo = m + 1; } else hi = m - 1; } return ans; };

    const byBook = {};
    for (const r of W) (byBook[r.rebalance_date] ||= []).push(r);
    const i0 = lastLE(addDays(books[0], -3));
    if (i0 < 0) return { W, books, metrics: metrics || [] };
    const pts = [{ d: days[i0], eq: 1, book: null, live: false }];
    const weeks = [];
    let eq = 1;
    books.forEach((b, k) => {
      const last = k === books.length - 1;
      const j0 = lastLE(addDays(b, -3)), j1 = last ? days.length - 1 : lastLE(addDays(books[k + 1], -3));
      if (j0 < 0 || j1 <= j0) return;
      const pos = byBook[b].map((r) => ({ isin: r.isin, w: +r.weight, p0: P[r.isin][j0] }));
      const eqStart = eq;
      for (let j = j0 + 1; j <= j1; j++) {
        let v = 1;
        for (const p of pos) { const pt = P[p.isin][j]; if (p.p0 && pt) v += p.w * (pt / p.p0 - 1); }
        eq = eqStart * v;
        pts.push({ d: days[j], eq, book: b, live: last });
      }
      weeks.push({ book: b, from: days[j0], to: days[j1], g: eq / eqStart - 1, live: last });
    });

    // OSEBX on the same days (forward-filled if Yahoo skipped one)
    const ob = (world || []).find((r) => r.series_id === 'eq.osebx');
    const obAt = (() => {
      if (!ob) return () => null;
      const ds = ob.dates, vs = ob.vals.map(Number);
      return (d) => { let lo = 0, hi = ds.length - 1, ans = -1; while (lo <= hi) { const m = (lo + hi) >> 1; if (ds[m] <= d) { ans = m; lo = m + 1; } else hi = m - 1; } return ans < 0 ? null : vs[ans]; };
    })();
    const o0 = obAt(pts[0].d);
    for (const p of pts) { const v = obAt(p.d); p.ob = v != null && o0 ? v / o0 : null; }
    for (const w of weeks) { const a = obAt(w.from), b = obAt(w.to); w.o = a && b ? b / a - 1 : null; }
    const pol = (world || []).find((r) => r.series_id === 'pol.no');
    const polVals = pol ? pol.dates.map((d, i) => [d, +pol.vals[i]]).filter(([d]) => d >= pts[0].d).map(([, v]) => v) : [];
    const rf = polVals.length ? mean(polVals) / 100 : 0;

    // forecasts and the stored record per book: the book's own forecast, and the next row's realized_return
    const M = Object.fromEntries((metrics || []).map((r) => [r.rebalance_date, r]));
    books.forEach((b, k) => {
      const w = weeks.find((x) => x.book === b); if (!w) return;
      w.var95 = M[b] ? +M[b].forecast_var95 : null; w.var99 = M[b] ? +M[b].forecast_var99 : null;
      w.vol = M[b] ? +M[b].forecast_weekly_vol : null;
      w.stored = k + 1 < books.length && M[books[k + 1]] && M[books[k + 1]].realized_return != null ? +M[books[k + 1]].realized_return : null;
    });
    return { W, books, byBook, pts, weeks, rf, metrics: metrics || [] };
  }

  function analyse(D) {
    const pts = D.pts.filter((p) => p.ob != null);
    const rg = [], ro = [];
    for (let i = 1; i < pts.length; i++) { rg.push(pts[i].eq / pts[i - 1].eq - 1); ro.push(pts[i].ob / pts[i - 1].ob - 1); }
    const done = D.weeks.filter((w) => !w.live && w.o != null);
    const g = riskStats(rg, pts.map((p) => p.eq), done.map((w) => w.g));
    const o = riskStats(ro, pts.map((p) => p.ob), done.map((w) => w.o));
    const beta = cov(rg, ro) / (sd(ro) ** 2), corr = cov(rg, ro) / (sd(rg) * sd(ro));
    const act = rg.map((x, i) => x - ro[i]);
    const te = sd(act) * Math.sqrt(252), ir = (mean(act) * 252) / te;
    const rfd = D.rf / 252;
    for (const [s, r] of [[g, rg], [o, ro]]) {
      s.sharpe = ((mean(r) - rfd) / sd(r)) * Math.sqrt(252);
      s.sortino = ((mean(r) - rfd) * 252) / s.down;
    }
    return { pts, rg, ro, g, o, beta, corr, te, ir, done, n: rg.length, weeksBeat: done.filter((w) => w.g > w.o).length };
  }

  // ═══ HERO: headline cards ══════════════════════════════════════════════
  function renderCards(D, S) {
    const el = $('lifeCards'); if (!el) return;
    const card = (lab, val, sub, cls) => `<div class="kpi life-card ${cls || ''}"><div class="kpi-lbl">${lab}</div><div class="kpi-val">${val}</div><div class="kpi-sub">${sub}</div></div>`;
    const gap = S.g.total - S.o.total;
    el.innerHTML = [
      card('<i class="sw" style="background:#f5a623"></i>Galton', pct(S.g.total), `${pct(S.g.ann, 1)} a year · ${S.n} trading days`, 'c-amber'),
      card('<i class="sw" style="background:#4fc3f7"></i>OSEBX', pct(S.o.total), `${pct(S.o.ann, 1)} a year · total return`, 'c-ice'),
      card('Galton − OSEBX', `<span class="${gap >= 0 ? 'pp-pos' : 'pp-neg'}">${pp(gap)}</span>`, `ahead in ${S.weeksBeat} of ${S.done.length} weeks`, ''),
      card('Beta · correlation', `${num(S.beta)} · ${num(S.corr)}`, 'daily, against OSEBX', ''),
      card('Max drawdown', `<span class="pp-neg">${pctU(S.g.mdd, 1)}</span> <small>/ ${pctU(S.o.mdd, 1)}</small>`, `Galton / OSEBX · now ${pctU(S.g.dd, 1)} / ${pctU(S.o.dd, 1)}`, ''),
    ].join('');
    const first = D.pts[0].d, last = D.pts[D.pts.length - 1].d;
    $('lifeAsOf').innerHTML = `${dLabel(first)} → <strong style="color:var(--text1)">${dLabel(last)}</strong> · ${D.books.length} books`;
    tilt(el.querySelectorAll('.life-card'));
  }

  // ═══ HERO: the ribbon chart ════════════════════════════════════════════
  function lifeChart(D, S) {
    const cv = $('lifeCanvas'), tip = $('lifeTip'); if (!cv) return;
    const pts = S.pts, n = pts.length;
    const W = cv.clientWidth, H = cv.clientHeight, dpr = Math.min(2, window.devicePixelRatio || 1);
    if (!W || !H) return;                              // hidden: draw on the next resize
    cv.width = Math.round(W * dpr); cv.height = Math.round(H * dpr);
    const off = document.createElement('canvas'); off.width = cv.width; off.height = cv.height;
    const c = off.getContext('2d'); c.setTransform(dpr, 0, 0, dpr, 0, 0);
    const narrow = W < 560;
    const L = narrow ? 38 : 50, R = narrow ? 10 : 92, T = 16, B = 22, GAP = 16;
    const ddH = Math.round((H - T - B) * 0.24), mainH = H - T - B - ddH - GAP;
    const x = (i) => L + (i / Math.max(1, n - 1)) * (W - L - R);
    const gv = pts.map((p) => p.eq - 1), ov = pts.map((p) => p.ob - 1);
    let lo = Math.min(0, ...gv, ...ov), hi = Math.max(0, ...gv, ...ov);
    const pad = (hi - lo) * 0.12 || 0.01; lo -= pad; hi += pad;
    const y = (v) => T + (1 - (v - lo) / (hi - lo)) * mainH;
    const dg = ddSeries(pts.map((p) => p.eq)), dob = ddSeries(pts.map((p) => p.ob));
    const ddLo = Math.min(-0.01, ...dg, ...dob) * 1.18;
    const top2 = T + mainH + GAP;
    const yd = (v) => top2 + (v / ddLo) * ddH;

    // floor: a faint perspective grid under the plot
    c.save();
    c.beginPath(); c.rect(L, T, W - L - R, mainH); c.clip();
    const vp = { x: L + (W - L - R) / 2, y: T - mainH * 0.9 };
    c.strokeStyle = 'rgba(245,166,35,0.045)'; c.lineWidth = 1;
    for (let k = -14; k <= 14; k++) { c.beginPath(); c.moveTo(vp.x + k * 4, vp.y); c.lineTo(vp.x + k * ((W - L - R) / 7), T + mainH); c.stroke(); }
    c.restore();

    // y grid + labels
    const step = niceStep((hi - lo) / 5);
    c.font = '9px "IBM Plex Mono", monospace'; c.textBaseline = 'middle';
    for (let v = Math.ceil(lo / step) * step; v <= hi + 1e-12; v += step) {
      const yy = y(v);
      c.strokeStyle = Math.abs(v) < 1e-9 ? 'rgba(255,255,255,0.22)' : 'rgba(255,255,255,0.045)';
      c.setLineDash(Math.abs(v) < 1e-9 ? [4, 4] : []);
      c.beginPath(); c.moveTo(L, yy); c.lineTo(W - R, yy); c.stroke();
      c.setLineDash([]);
      c.fillStyle = '#5a6377'; c.textAlign = 'right';
      c.fillText((v >= 0 ? '+' : '−') + Math.abs(v * 100).toFixed(step < 0.01 ? 1 : 0) + '%', L - 7, yy);
    }
    // live week band
    const liveFrom = pts.findIndex((p) => p.live);
    if (liveFrom > 0) {
      const xa = x(liveFrom - 1), xb = x(n - 1);
      const g = c.createLinearGradient(xa, 0, xb, 0); g.addColorStop(0, 'rgba(245,166,35,0.0)'); g.addColorStop(1, 'rgba(245,166,35,0.09)');
      c.fillStyle = g; c.fillRect(xa, T, xb - xa, mainH + GAP + ddH);
      c.fillStyle = 'rgba(245,166,35,0.75)'; c.textAlign = 'right'; c.textBaseline = 'top';
      c.fillText('LIVE WEEK', xb - 4, T + 2);
    }
    // book boundaries: a tick on the zero line each Friday
    c.strokeStyle = 'rgba(255,255,255,0.16)';
    for (let i = 1; i < n; i++) if (pts[i].book !== pts[i - 1].book && pts[i - 1].book !== undefined) {
      const xx = x(i - 1); c.beginPath(); c.moveTo(xx, y(0) - 3); c.lineTo(xx, y(0) + 3); c.stroke();
    }

    // the ribbons: OSEBX behind, Galton in front
    ribbon(c, ov, x, y, ICE, 6, 0.10, T + mainH);
    ribbon(c, gv, x, y, AMBER, 10, 0.16, T + mainH);

    // end labels
    if (!narrow) {
      const ends = [[gv[n - 1], AMBER, 'GALTON'], [ov[n - 1], ICE, 'OSEBX']].map(([v, col, lab]) => ({ v, col, lab, yy: y(v) }));
      if (Math.abs(ends[0].yy - ends[1].yy) < 26) { const mid = (ends[0].yy + ends[1].yy) / 2, s = ends[0].yy <= ends[1].yy ? -1 : 1; ends[0].yy = mid + s * 13; ends[1].yy = mid - s * 13; }
      for (const e of ends) {
        c.fillStyle = rgba(e.col, 0.14); c.strokeStyle = rgba(e.col, 0.55);
        roundRect(c, W - R + 8, e.yy - 11, R - 12, 22, 4); c.fill(); c.stroke();
        c.fillStyle = e.col; c.textAlign = 'left'; c.textBaseline = 'middle';
        c.font = '8px "IBM Plex Mono", monospace'; c.fillText(e.lab, W - R + 13, e.yy - 4.5);
        c.font = '10px "IBM Plex Mono", monospace'; c.fillText(pct(e.v), W - R + 13, e.yy + 5);
      }
    }

    // drawdown strip
    c.font = '8.5px "IBM Plex Mono", monospace'; c.fillStyle = '#5a6377'; c.textAlign = 'left'; c.textBaseline = 'top';
    c.fillText('DRAWDOWN', L + 2, top2 - 1);
    c.strokeStyle = 'rgba(255,255,255,0.12)'; c.beginPath(); c.moveTo(L, top2); c.lineTo(W - R, top2); c.stroke();
    const gd = c.createLinearGradient(0, top2, 0, top2 + ddH);
    gd.addColorStop(0, 'rgba(240,96,96,0.08)'); gd.addColorStop(1, 'rgba(240,96,96,0.55)');
    c.beginPath(); c.moveTo(x(0), top2);
    dg.forEach((v, i) => c.lineTo(x(i), yd(v)));
    c.lineTo(x(n - 1), top2); c.closePath(); c.fillStyle = gd; c.fill();
    c.strokeStyle = rgba(RED, 0.9); c.lineWidth = 1.2; c.beginPath(); dg.forEach((v, i) => (i ? c.lineTo(x(i), yd(v)) : c.moveTo(x(i), yd(v)))); c.stroke();
    c.strokeStyle = rgba(ICE, 0.85); c.lineWidth = 1; c.setLineDash([3, 2]); c.beginPath(); dob.forEach((v, i) => (i ? c.lineTo(x(i), yd(v)) : c.moveTo(x(i), yd(v)))); c.stroke(); c.setLineDash([]);
    const mi = dg.indexOf(Math.min(...dg));
    if (mi > 0) {
      c.fillStyle = RED; c.beginPath(); c.arc(x(mi), yd(dg[mi]), 2.6, 0, 7); c.fill();
      c.fillStyle = 'rgba(240,96,96,0.9)'; c.textAlign = mi > n * 0.7 ? 'right' : 'left'; c.textBaseline = 'top';
      c.fillText(pctU(dg[mi], 1), x(mi) + (mi > n * 0.7 ? -5 : 5), yd(dg[mi]) - 4);
    }
    c.fillStyle = '#5a6377'; c.textAlign = 'right'; c.textBaseline = 'middle';
    c.fillText(pctU(ddLo / 1.18, 0), L - 7, yd(ddLo / 1.18));
    // months
    c.textAlign = 'center'; c.textBaseline = 'alphabetic'; c.fillStyle = '#5a6377';
    let lastM = '';
    pts.forEach((p, i) => {
      const m = p.d.slice(0, 7);
      if (m !== lastM && i > 0) { c.fillText(new Date(p.d + 'T00:00:00Z').toLocaleDateString('en-GB', { month: 'short', timeZone: 'UTC' }), x(i), H - 6); }
      lastM = m;
    });

    const ctx = cv.getContext('2d');
    function blit(progress) {
      ctx.setTransform(1, 0, 0, 1, 0, 0); ctx.clearRect(0, 0, cv.width, cv.height);
      if (progress >= 1) { ctx.drawImage(off, 0, 0); return; }
      ctx.drawImage(off, 0, 0, L * dpr, cv.height, 0, 0, L * dpr, cv.height);       // axis labels stay
      const w = (L + progress * (W - L)) * dpr;
      ctx.drawImage(off, L * dpr, 0, Math.max(1, w - L * dpr), cv.height, L * dpr, 0, Math.max(1, w - L * dpr), cv.height);
    }
    function hover(i) {
      blit(1); if (i == null) { tip.style.display = 'none'; return; }
      ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
      const xx = x(i);
      ctx.strokeStyle = 'rgba(255,255,255,0.28)'; ctx.lineWidth = 1; ctx.beginPath(); ctx.moveTo(xx, T); ctx.lineTo(xx, top2 + ddH); ctx.stroke();
      for (const [v, col] of [[ov[i], ICE], [gv[i], AMBER]]) {
        ctx.fillStyle = col; ctx.shadowColor = col; ctx.shadowBlur = 10;
        ctx.beginPath(); ctx.arc(xx, y(v), 3.6, 0, 7); ctx.fill(); ctx.shadowBlur = 0;
        ctx.strokeStyle = '#050810'; ctx.lineWidth = 1.2; ctx.stroke();
      }
      const p = pts[i];
      tip.innerHTML = `<b>${dLong(p.d)}</b>${p.live ? ' <span style="color:var(--amber)">· live week</span>' : ''}<br>` +
        `<i style="background:${AMBER}"></i>Galton <b>${pct(gv[i])}</b><br><i style="background:${ICE}"></i>OSEBX <b>${pct(ov[i])}</b><br>` +
        `<span class="faint">gap</span> ${pp(gv[i] - ov[i])} · <span class="faint">drawdown</span> ${pctU(dg[i], 1)} / ${pctU(dob[i], 1)}` +
        (p.book ? `<br><span class="faint">book of ${dLabel(p.book)}</span>` : '');
      tip.style.display = 'block';
      const r = cv.getBoundingClientRect();
      tip.style.left = Math.min(r.width - tip.offsetWidth - 6, Math.max(0, xx + 14 - (xx > r.width * 0.62 ? tip.offsetWidth + 28 : 0))) + 'px';
      tip.style.top = Math.max(0, Math.min(y(gv[i]), y(ov[i])) - tip.offsetHeight - 10) + 'px';
    }
    cv.onpointermove = (e) => {
      const r = cv.getBoundingClientRect(), px = e.clientX - r.left;
      if (px < L - 4 || px > W - R + 4) { hover(null); return; }
      hover(Math.max(0, Math.min(n - 1, Math.round(((px - L) / (W - L - R)) * (n - 1)))));
    };
    cv.onpointerleave = () => hover(null);
    if (REDUCED || lifeChart.drawn) { blit(1); return; }
    lifeChart.drawn = true;
    const t0 = performance.now();
    const step2 = (t) => { const p = Math.min(1, (t - t0) / 1400); blit(1 - Math.pow(1 - p, 3)); if (p < 1) requestAnimationFrame(step2); };
    requestAnimationFrame(step2);
  }
  function ribbon(c, vals, x, y, col, thick, fillA, floor) {
    const P = vals.map((v, i) => [x(i), y(v)]), n = P.length;
    // translucent body under the line
    const g = c.createLinearGradient(0, Math.min(...P.map((p) => p[1])), 0, floor);
    g.addColorStop(0, rgba(col, fillA)); g.addColorStop(1, rgba(col, 0));
    c.beginPath(); c.moveTo(P[0][0], P[0][1] + thick); P.forEach(([a, b]) => c.lineTo(a + 2, b + thick));
    c.lineTo(P[n - 1][0] + 2, floor); c.lineTo(P[0][0], floor); c.closePath(); c.fillStyle = g; c.fill();
    // the ribbon's face: one quad per segment, lit by its slope (rising faces catch the light)
    const base = hex(col);
    for (let i = 1; i < n; i++) {
      const [ax, ay] = P[i - 1], [bx, by] = P[i];
      const s = (by - ay) / Math.max(0.5, bx - ax);
      const k = 0.42 + 0.3 * Math.tanh(-s * 0.9);
      c.fillStyle = css(shadeRGB(base, k), 0.92);
      c.beginPath(); c.moveTo(ax, ay); c.lineTo(bx + 0.6, by); c.lineTo(bx + 2.6, by + thick); c.lineTo(ax + 2, ay + thick); c.closePath(); c.fill();
    }
    // the edge: glow, then a thin specular highlight
    c.save(); c.shadowColor = col; c.shadowBlur = 14; c.strokeStyle = col; c.lineWidth = 2.2; c.lineJoin = 'round';
    c.beginPath(); P.forEach(([a, b], i) => (i ? c.lineTo(a, b) : c.moveTo(a, b))); c.stroke(); c.restore();
    c.strokeStyle = 'rgba(255,255,255,0.45)'; c.lineWidth = 0.7;
    c.beginPath(); P.forEach(([a, b], i) => (i ? c.lineTo(a, b - 0.9) : c.moveTo(a, b - 0.9))); c.stroke();
  }
  function roundRect(c, x, y, w, h, r) { c.beginPath(); c.moveTo(x + r, y); c.arcTo(x + w, y, x + w, y + h, r); c.arcTo(x + w, y + h, x, y + h, r); c.arcTo(x, y + h, x, y, r); c.arcTo(x, y, x + w, y, r); c.closePath(); }
  function niceStep(raw) { const p = Math.pow(10, Math.floor(Math.log10(raw || 1e-4))); const f = raw / p; return (f < 1.5 ? 1 : f < 3.5 ? 2 : f < 7.5 ? 5 : 10) * p; }

  // ═══ RISK: the duel ════════════════════════════════════════════════════
  function renderRisk(D, S) {
    const g = S.g, o = S.o;
    const ROWS = [
      ['Volatility · daily', 'annualised from one-day moves', g.vol, o.vol, (v) => pctU(v, 1)],
      ['Volatility · weekly', 'annualised, Friday to Friday', g.volW, o.volW, (v) => pctU(v, 1)],
      ['VaR 95 % · one day', 'historical: the 5th-percentile day', g.var95, o.var95, (v) => pctU(v)],
      ['VaR 99 % · one day', 'historical: the 1st-percentile day', g.var99, o.var99, (v) => pctU(v)],
      ['Expected shortfall 95 %', 'the average day beyond VaR 95', g.es95, o.es95, (v) => pctU(v)],
      ['VaR 95 % · one week', 'Gaussian, from weekly volatility', g.var95W, o.var95W, (v) => pctU(v)],
      ['Max drawdown', 'deepest fall from a peak', -g.mdd, -o.mdd, (v) => pctU(v, 1)],
      ['Worst day', '', -g.worst, -o.worst, (v) => pctU(v)],
      ['Worst week', '', -g.worstW, -o.worstW, (v) => pctU(v)],
      ['Downside deviation', 'annualised, losing days only', g.down, o.down, (v) => pctU(v, 1)],
    ].filter((r) => r[2] != null && r[3] != null && isFinite(r[2]) && isFinite(r[3]));
    let gRisk = 0, oRisk = 0;
    const html = ROWS.map(([lab, sub, a, b, f], i) => {
      const mx = Math.max(a, b) || 1, rel = Math.abs(a - b) / mx;
      const who = rel < 0.03 ? 'level' : a > b ? 'g' : 'o';
      if (who === 'g') gRisk++; else if (who === 'o') oRisk++;
      return `<div class="duel-row" style="--d:${i * 60}ms">
        <div class="duel-lbl">${lab}${sub ? `<small>${sub}</small>` : ''}</div>
        <div class="duel-bars">
          <div class="bar3d g${who === 'g' ? ' hi' : ''}" style="--w:${(a / mx * 100).toFixed(1)}%"><span>${f(a)}</span></div>
          <div class="bar3d o${who === 'o' ? ' hi' : ''}" style="--w:${(b / mx * 100).toFixed(1)}%"><span>${f(b)}</span></div>
        </div>
        <div class="duel-v ${who}">${who === 'level' ? 'level' : who === 'g' ? 'Galton riskier' : 'OSEBX riskier'}</div>
      </div>`;
    }).join('');
    $('duel').innerHTML = html;
    reveal($('duel'));

    // the verdict
    const total = ROWS.length;
    const dailyRatio = g.vol / o.vol, weeklyRatio = g.volW && o.volW ? g.volW / o.volW : null;
    const lead = gRisk > oRisk ? `Galton has been the <em class="hot">riskier</em> of the two on <b>${gRisk} of ${total}</b> measures`
      : oRisk > gRisk ? `OSEBX has been the <em class="hot">riskier</em> of the two on <b>${oRisk} of ${total}</b> measures`
      : `The two have been <b>about as risky</b> as each other`;
    let why = '';
    const seD = 1 / Math.sqrt(2 * (S.n - 1)), seW = 1 / Math.sqrt(2 * Math.max(1, S.done.length - 1));
    if (weeklyRatio != null && Math.abs(dailyRatio - weeklyRatio) > 0.08) {
      why = ` — mostly day to day. Week to week they are closer (${pctU(g.volW, 1)} against ${pctU(o.volW, 1)} a year), but
        ${S.done.length} weeks pin a volatility down only to about ±${(seW * 100).toFixed(0)} %, against ±${(seD * 100).toFixed(0)} % from ${S.n} days, so the daily measures weigh more.`;
    } else if (weeklyRatio != null) {
      why = `. Week to week: ${pctU(g.volW, 1)} against ${pctU(o.volW, 1)} a year.`;
    } else why = '.';
    const ret = S.g.total >= S.o.total ? `and it has earned more (${pct(S.g.total)} against ${pct(S.o.total)})` : `and it has earned less (${pct(S.g.total)} against ${pct(S.o.total)})`;
    $('riskVerdict').innerHTML = `<div class="rv-scale" aria-hidden="true"><span class="rv-g" style="--h:${(gRisk / Math.max(1, total) * 100).toFixed(0)}%"><b>${gRisk}</b></span><span class="rv-o" style="--h:${(oRisk / Math.max(1, total) * 100).toFixed(0)}%"><b>${oRisk}</b></span></div>
      <div class="rv-text"><div class="rv-kicker">Which one is riskier?</div>
      <div class="rv-main">${lead}${why}</div>
      <div class="rv-sub">Beta to the index ${num(S.beta)}: a 1 % OSEBX day has meant about ${num(S.beta)} % for Galton, ${ret}. ${S.n} trading days and ${S.done.length} full weeks — a short record, so read the gaps as tendencies, not verdicts.</div></div>`;

    // reward
    const row = (lab, a, b, note) => `<tr><td>${lab}${note ? `<small>${note}</small>` : ''}</td><td class="g">${a}</td><td class="o">${b}</td></tr>`;
    $('reward').innerHTML = `<thead><tr><th></th><th class="g">Galton</th><th class="o">OSEBX</th></tr></thead><tbody>` +
      row('Total return', pct(g.total), pct(o.total)) +
      row('Annualised', pct(g.ann, 1), pct(o.ann, 1), `${S.n} days: noisy`) +
      row('Sharpe', num(g.sharpe), num(o.sharpe), `excess of the policy rate, ${pctU(D.rf, 2)}`) +
      row('Sortino', num(g.sortino), num(o.sortino), 'excess return per unit of downside') +
      row('Days up', pctU(g.up, 0), pctU(o.up, 0)) +
      row('Best day · best week', `${pct(g.best)} · ${pct(g.bestW)}`, `${pct(o.best)} · ${pct(o.bestW)}`) +
      row('Skewness · excess kurtosis', `${num(g.skew)} · ${num(g.kurt)}`, `${num(o.skew)} · ${num(o.kurt)}`, 'daily; negative skew = fatter left tail') +
      row('Beta · correlation', `${num(S.beta)} · ${num(S.corr)}`, '1 · 1') +
      row('Tracking error · information ratio', `${pctU(S.te, 1)} · ${num(S.ir)}`, '—', 'annualised, Galton minus OSEBX') +
      '</tbody>';
  }

  // ═══ RISK: daily returns as two ridges ═════════════════════════════════
  function ridges(S) {
    const cv = $('ridgeCanvas'); if (!cv) return;
    const W = cv.clientWidth, H = cv.clientHeight, dpr = Math.min(2, window.devicePixelRatio || 1);
    if (!W || !H) return;
    cv.width = Math.round(W * dpr); cv.height = Math.round(H * dpr);
    const c = cv.getContext('2d'); c.setTransform(dpr, 0, 0, dpr, 0, 0); c.clearRect(0, 0, W, H);
    const all = [...S.rg, ...S.ro];
    const lo = Math.min(...all) - 0.004, hi = Math.max(...all) + 0.004;
    const L = 12, R = 12, B = 24, T = 14, depth = Math.min(64, H * 0.3);
    const x = (v) => L + ((v - lo) / (hi - lo)) * (W - L - R);
    const kde = (a) => {
      const h = 1.06 * sd(a) * Math.pow(a.length, -0.2), pts = [];
      for (let i = 0; i <= 160; i++) { const v = lo + (i / 160) * (hi - lo); let s = 0; for (const r of a) s += Math.exp(-0.5 * ((v - r) / h) ** 2); pts.push([v, s / (a.length * h * Math.sqrt(2 * Math.PI))]); }
      return pts;
    };
    const kg = kde(S.rg), ko = kde(S.ro);
    const dmax = Math.max(...kg.map((p) => p[1]), ...ko.map((p) => p[1]));
    const hgt = H - T - B - depth;
    const ridge = (k, base, col, var95, lab, worst) => {
      const yy = (d) => base - (d / dmax) * hgt;
      // floor line, then the body, then the left tail beyond VaR 95
      c.strokeStyle = 'rgba(255,255,255,0.10)'; c.beginPath(); c.moveTo(L, base); c.lineTo(W - R, base); c.stroke();
      const g = c.createLinearGradient(0, base - hgt, 0, base);
      g.addColorStop(0, rgba(col, 0.42)); g.addColorStop(1, rgba(col, 0.04));
      c.beginPath(); c.moveTo(x(k[0][0]), base); k.forEach(([v, d]) => c.lineTo(x(v), yy(d))); c.lineTo(x(k[k.length - 1][0]), base); c.closePath();
      c.fillStyle = g; c.fill();
      c.save(); c.beginPath(); c.rect(0, 0, x(-var95), H); c.clip();
      c.beginPath(); c.moveTo(x(k[0][0]), base); k.forEach(([v, d]) => c.lineTo(x(v), yy(d))); c.lineTo(x(k[k.length - 1][0]), base); c.closePath();
      c.fillStyle = 'rgba(240,96,96,0.55)'; c.fill(); c.restore();
      c.save(); c.shadowColor = col; c.shadowBlur = 10; c.strokeStyle = col; c.lineWidth = 1.8;
      c.beginPath(); k.forEach(([v, d], i) => (i ? c.lineTo(x(v), yy(d)) : c.moveTo(x(v), yy(d)))); c.stroke(); c.restore();
      // VaR marker
      const vx = x(-var95);
      c.strokeStyle = 'rgba(240,96,96,0.9)'; c.setLineDash([3, 3]); c.beginPath(); c.moveTo(vx, base); c.lineTo(vx, base - hgt * 0.62); c.stroke(); c.setLineDash([]);
      c.font = '9px "IBM Plex Mono", monospace'; c.fillStyle = 'rgba(240,96,96,0.95)'; c.textAlign = 'right'; c.textBaseline = 'bottom';
      c.fillText(`VaR95 ${pct(-var95)}`, vx - 4, base - hgt * 0.62);
      const peak = k.reduce((m, p) => (p[1] > m[1] ? p : m), k[0]);
      c.fillStyle = col; c.textAlign = 'left'; c.fillText(lab, x(peak[0]) + 12, yy(peak[1]) + 2);
      c.fillStyle = RED; c.beginPath(); c.arc(x(worst), base - 2, 2.4, 0, 7); c.fill();
    };
    // back ridge first: OSEBX sits deeper
    ridge(ko, H - B - depth, ICE, S.o.var95, 'OSEBX', S.o.worst);
    // the side walls that make the two floors one stage
    c.strokeStyle = 'rgba(255,255,255,0.06)';
    c.beginPath(); c.moveTo(L, H - B); c.lineTo(L, H - B - depth); c.moveTo(W - R, H - B); c.lineTo(W - R, H - B - depth); c.stroke();
    ridge(kg, H - B, AMBER, S.g.var95, 'GALTON', S.g.worst);
    c.fillStyle = '#5a6377'; c.font = '9px "IBM Plex Mono", monospace'; c.textAlign = 'center'; c.textBaseline = 'alphabetic';
    for (let k = Math.ceil(lo * 100); k <= Math.floor(hi * 100); k++) {
      const v = k / 100;
      c.fillText((k > 0 ? '+' : k < 0 ? '−' : '') + Math.abs(k) + '%', x(v), H - 7);
      c.strokeStyle = 'rgba(255,255,255,0.08)'; c.beginPath(); c.moveTo(x(v), H - B); c.lineTo(x(v), H - B + 4); c.stroke();
    }
  }

  // ═══ RISK: forecast VaR against the weeks ══════════════════════════════
  function varBacktest(S) {
    const wk = S.done.filter((w) => w.var95 != null && isFinite(w.var95));
    const el = $('varBt'); if (!el) return;
    if (!wk.length) { el.innerHTML = '<div class="ph">No completed weeks yet.</div>'; return; }
    const b95 = wk.filter((w) => w.g < -w.var95).length, b99 = wk.filter((w) => w.g < -w.var99).length;
    const k95 = kupiec(b95, wk.length, 0.05);
    const fvol = mean(wk.map((w) => w.vol)) * Math.sqrt(52), rvol = sd(wk.map((w) => w.g)) * Math.sqrt(52);
    const ok = k95 && k95.p > 0.05;
    const mx = Math.max(...wk.map((w) => Math.abs(w.g) / w.var95), 1.2);
    el.innerHTML = `<div class="vb-strip">${wk.map((w, i) => {
      const z = w.g / w.var95, h = Math.min(1, Math.abs(z) / mx), br = w.g < -w.var95;
      return `<span class="vb ${w.g >= 0 ? 'up' : 'dn'}${br ? ' br' : ''}" style="--h:${(h * 100).toFixed(0)}%;--d:${i * 25}ms" title="Book of ${dLabel(w.book)}: ${pct(w.g)} against a VaR95 of ${pctU(w.var95)}${br ? ' — breach' : ''}"></span>`;
    }).join('')}<i class="vb-line" style="--h:${(1 / mx * 100).toFixed(0)}%"></i></div>
    <div class="vb-axis"><span>${dLabel(wk[0].book)}</span><span>VaR line = the week's own forecast</span><span>${dLabel(wk[wk.length - 1].book)}</span></div>
    <div class="vb-stats">
      <div><b class="${b95 > wk.length * 0.05 * 2 ? 'pp-neg' : ''}">${b95}</b> / ${wk.length}<small>weeks below VaR95 · ${(wk.length * 0.05).toFixed(1)} expected</small></div>
      <div><b>${b99}</b> / ${wk.length}<small>below VaR99 · ${(wk.length * 0.01).toFixed(1)} expected</small></div>
      <div><b class="${ok ? 'pp-pos' : 'pp-neg'}">${k95 ? k95.p.toFixed(2) : '—'}</b><small>Kupiec p-value · ${ok ? 'consistent with a 95 % VaR' : 'the VaR is off'}</small></div>
      <div><b>${pctU(rvol, 1)}</b><small>realised weekly vol, a year · forecast ${pctU(fvol, 1)}</small></div>
    </div>`;
    reveal(el);
  }

  // ═══ RISK: week by week (Chart.js) ═════════════════════════════════════
  let wkChart = null;
  function weekDuel(S) {
    const cv = $('wkDuel'); if (!cv || !window.Chart) return;
    const wk = S.done;
    if (wkChart) wkChart.destroy();
    const grad = (col) => (ctx) => {
      const ch = ctx.chart, a = ch.chartArea; if (!a) return rgba(col, 0.6);
      const v = ctx.raw ?? 0, g = ch.ctx.createLinearGradient(0, a.top, 0, a.bottom);
      g.addColorStop(0, rgba(col, v >= 0 ? 0.95 : 0.35)); g.addColorStop(1, rgba(col, v >= 0 ? 0.35 : 0.95));
      return g;
    };
    wkChart = new Chart(cv, {
      type: 'bar',
      data: {
        labels: wk.map((w) => dLabel(w.book)),
        datasets: [
          { label: 'Galton', data: wk.map((w) => +(w.g * 100).toFixed(3)), backgroundColor: grad(AMBER), borderColor: AMBER, borderWidth: { top: 1, right: 0, bottom: 1, left: 0 }, borderRadius: 2, categoryPercentage: 0.72, barPercentage: 0.9 },
          { label: 'OSEBX', data: wk.map((w) => +(w.o * 100).toFixed(3)), backgroundColor: grad(ICE), borderColor: ICE, borderWidth: { top: 1, right: 0, bottom: 1, left: 0 }, borderRadius: 2, categoryPercentage: 0.72, barPercentage: 0.9 },
          { label: 'Galton VaR95 (its forecast)', type: 'line', data: wk.map((w) => w.var95 != null ? -w.var95 * 100 : null), borderColor: 'rgba(240,96,96,0.85)', borderDash: [4, 4], borderWidth: 1.2, pointRadius: 0, stepped: 'middle', glow: 6 },
        ],
      },
      options: {
        responsive: true, maintainAspectRatio: false, animation: REDUCED ? false : { duration: 900, easing: 'easeOutCubic' },
        interaction: { mode: 'index', intersect: false },
        plugins: {
          legend: { labels: { color: '#a4adbf', font: { family: 'IBM Plex Mono', size: 10 }, boxWidth: 12 } },
          tooltip: { callbacks: { title: (it) => 'Book of ' + it[0].label, label: (c) => `${c.dataset.label}: ${c.parsed.y >= 0 ? '+' : ''}${c.parsed.y.toFixed(2)}%`,
            afterBody: (it) => { const w = wk[it[0].dataIndex]; return `gap ${pp(w.g - w.o)}` + (w.stored != null ? ` · booked ${pct(w.stored)}` : ''); } } },
        },
        scales: {
          x: { ticks: { color: '#5a6377', font: { family: 'IBM Plex Mono', size: 9 }, maxRotation: 0, autoSkip: true, maxTicksLimit: 12 }, grid: { display: false } },
          y: { ticks: { color: '#5a6377', font: { family: 'IBM Plex Mono', size: 9 }, callback: (v) => v.toFixed(1) + '%' }, grid: { color: 'rgba(255,255,255,0.045)' } },
        },
      },
    });
  }

  // ═══ 3D: a small projector ═════════════════════════════════════════════
  function Scene(canvas, opts) {
    const st = { yaw: opts.yaw, pitch: opts.pitch, base: opts.yaw, k: 1, w: 0, h: 0, dpr: 1, idle: 0, raf: 0, visible: true, user: false };
    const ctx = canvas.getContext('2d');
    const listeners = [];
    function size() {
      const r = canvas.getBoundingClientRect(); st.dpr = Math.min(2, window.devicePixelRatio || 1);
      st.w = r.width; st.h = r.height; canvas.width = Math.round(r.width * st.dpr); canvas.height = Math.round(r.height * st.dpr);
      st.k = Math.min(st.w / opts.span[0], st.h / opts.span[1]);
    }
    function proj(x, y, z) {
      const cy = Math.cos(st.yaw), sy = Math.sin(st.yaw), cp = Math.cos(st.pitch), sp = Math.sin(st.pitch);
      const X = x * cy - z * sy, Z = x * sy + z * cy;
      const Y2 = y * cp - Z * sp, Z2 = y * sp + Z * cp;
      const f = opts.persp ? opts.persp / (opts.persp + Z2) : 1;
      return [st.w / 2 + X * st.k * f, st.h * opts.cy - Y2 * st.k * f, Z2];
    }
    function render() { if (!st.w || !st.h) return; ctx.setTransform(st.dpr, 0, 0, st.dpr, 0, 0); ctx.clearRect(0, 0, st.w, st.h); opts.draw(ctx, proj, st); }
    const api = { st, proj, render, size, ctx, onTurn: (f) => listeners.push(f) };
    // drag to turn; vertical drags scroll the page on touch
    let drag = null;
    canvas.style.touchAction = 'pan-y';
    canvas.addEventListener('pointerdown', (e) => { drag = { x: e.clientX, y: e.clientY, yaw: st.yaw, pitch: st.pitch }; st.user = true; canvas.setPointerCapture(e.pointerId); canvas.classList.add('grab'); });
    canvas.addEventListener('pointermove', (e) => {
      if (!drag) { opts.hover && opts.hover(e, api); return; }
      st.yaw = drag.yaw + (e.clientX - drag.x) * 0.008;
      st.pitch = Math.max(0.08, Math.min(1.35, drag.pitch + (e.clientY - drag.y) * 0.006));
      render(); listeners.forEach((f) => f(st.yaw, st.pitch));
    });
    const end = () => { drag = null; canvas.classList.remove('grab'); st.idle = performance.now(); };
    canvas.addEventListener('pointerup', end); canvas.addEventListener('pointercancel', end);
    canvas.addEventListener('pointerleave', () => { if (!drag && opts.hover) opts.hover(null, api); });
    // a slow sway while on screen, paused for a while after the reader turns it
    if (opts.sway && !REDUCED) {
      const io = new IntersectionObserver((es) => { st.visible = es[0].isIntersecting; if (st.visible && !st.raf) loop(); }, { threshold: 0.1 });
      io.observe(canvas);
      const loop = () => {
        st.raf = 0; if (!st.visible) return;
        const now = performance.now();
        if (!drag && (!st.user || now - st.idle > 9000)) {
          if (st.user) { st.base = st.yaw - Math.sin(now / 5200) * opts.sway; st.user = false; }
          st.yaw = st.base + Math.sin(now / 5200) * opts.sway; render();
        }
        st.raf = requestAnimationFrame(loop);
      };
      loop();
    }
    size();
    return api;
  }
  // a box from (x0,y0,z0) to (x1,y1,z1): visible faces only, shaded by face
  function box(ctx, proj, b, rgb, alpha, stroke) {
    const [x0, y0, z0, x1, y1, z1] = b;
    const V = [[x0, y0, z0], [x1, y0, z0], [x1, y1, z0], [x0, y1, z0], [x0, y0, z1], [x1, y0, z1], [x1, y1, z1], [x0, y1, z1]].map((p) => proj(...p));
    const F = [[0, 1, 2, 3, 0.62], [5, 4, 7, 6, 0.62], [4, 0, 3, 7, 0.78], [1, 5, 6, 2, 0.78], [3, 2, 6, 7, 1.18], [4, 5, 1, 0, 0.45]];
    for (const [a, b2, c2, d, k] of F) {
      const A = V[a], Bv = V[b2], C = V[c2], Dv = V[d];
      const cross = (Bv[0] - A[0]) * (C[1] - A[1]) - (Bv[1] - A[1]) * (C[0] - A[0]);
      if (cross >= 0) continue;                        // facing away
      ctx.beginPath(); ctx.moveTo(A[0], A[1]); ctx.lineTo(Bv[0], Bv[1]); ctx.lineTo(C[0], C[1]); ctx.lineTo(Dv[0], Dv[1]); ctx.closePath();
      ctx.fillStyle = css(shadeRGB(rgb, k), alpha); ctx.fill();
      if (stroke) { ctx.strokeStyle = stroke; ctx.lineWidth = 0.6; ctx.stroke(); }
    }
    return V;
  }

  // ═══ 3D: the book as a skyline ═════════════════════════════════════════
  function skyline(D) {
    const cv = $('skyCanvas'), tip = $('skyTip'); if (!cv || !D.books.length) return;
    const last = D.books[D.books.length - 1], prev = D.books.length > 1 ? D.books[D.books.length - 2] : null;
    const cur = [...D.byBook[last]].sort((a, b) => +b.weight - +a.weight);
    const pw = Object.fromEntries((prev ? D.byBook[prev] : []).map((r) => [r.ticker, +r.weight]));
    const n = cur.length, mx = Math.max(...cur.map((r) => Math.abs(+r.weight))) || 1, H = 5;
    const bars = cur.map((r, i) => ({ r, x: i - (n - 1) / 2, w: +r.weight, h: (+r.weight / mx) * H, ph: pw[r.ticker] != null ? (pw[r.ticker] / mx) * H : null }));
    const rcMax = Math.max(...cur.map((r) => Math.abs(+r.risk_contribution || 0))) || 1;
    let hot = null;
    const S3 = Scene(cv, {
      yaw: -0.36, pitch: 0.34, cy: 0.69, span: [n * 1.08, 7.0], persp: 52, sway: 0.24,
      draw(ctx, proj, st) {
        // floor grid
        ctx.lineWidth = 1;
        for (let gx = -Math.ceil(n / 2) - 1; gx <= Math.ceil(n / 2) + 1; gx += 1) {
          const a = proj(gx, 0, -2.2), b = proj(gx, 0, 2.2);
          ctx.strokeStyle = gx % 5 === 0 ? 'rgba(245,166,35,0.10)' : 'rgba(255,255,255,0.035)';
          ctx.beginPath(); ctx.moveTo(a[0], a[1]); ctx.lineTo(b[0], b[1]); ctx.stroke();
        }
        for (let gz = -2; gz <= 2; gz += 1) {
          const a = proj(-n / 2 - 1, 0, gz), b = proj(n / 2 + 1, 0, gz);
          ctx.strokeStyle = gz === 0 ? 'rgba(255,255,255,0.10)' : 'rgba(255,255,255,0.035)';
          ctx.beginPath(); ctx.moveTo(a[0], a[1]); ctx.lineTo(b[0], b[1]); ctx.stroke();
        }
        // painter's order: far bars first
        const order = bars.map((b) => ({ b, z: proj(b.x, 0, 0)[2] })).sort((p, q) => q.z - p.z);
        for (const { b } of order) {
          const long = b.w >= 0, base = long ? hex(GREEN) : hex(RED);
          const k = 0.55 + 0.45 * Math.min(1, Math.abs(+b.r.risk_contribution || 0) / rcMax);
          const rgb = shadeRGB(base, k);
          const y0 = Math.min(0, b.h), y1 = Math.max(0, b.h) || 0.02;
          b.V = box(ctx, proj, [b.x - 0.39, y0, -0.39, b.x + 0.39, y1, 0.39], rgb, hot === b ? 1 : 0.95, hot === b ? 'rgba(255,255,255,0.7)' : null);
          if (b.ph != null && Math.abs(b.ph - b.h) > 0.02) {          // last week's height, as a wire
            const l = proj(b.x - 0.46, b.ph, -0.46), r = proj(b.x + 0.46, b.ph, -0.46), r2 = proj(b.x + 0.46, b.ph, 0.46), l2 = proj(b.x - 0.46, b.ph, 0.46);
            ctx.strokeStyle = 'rgba(232,238,247,0.45)'; ctx.setLineDash([2, 2]); ctx.lineWidth = 0.8;
            ctx.beginPath(); ctx.moveTo(l[0], l[1]); ctx.lineTo(r[0], r[1]); ctx.lineTo(r2[0], r2[1]); ctx.lineTo(l2[0], l2[1]); ctx.closePath(); ctx.stroke(); ctx.setLineDash([]);
          }
        }
        // labels: the five biggest longs and every short, at the bar's far end
        ctx.font = '9px "IBM Plex Mono", monospace'; ctx.textAlign = 'center';
        bars.forEach((b, i) => {
          if (!(i < 5 || b.w < 0 || hot === b)) return;
          const p = proj(b.x, b.h + (b.w >= 0 ? 0.28 : -0.42), 0);
          ctx.fillStyle = hot === b ? '#fff' : b.w >= 0 ? 'rgba(67,224,151,0.95)' : 'rgba(240,96,96,0.95)';
          ctx.fillText(b.r.ticker, p[0], p[1]);
        });
      },
      hover(e, api) {
        if (!e) { hot = null; tip.style.display = 'none'; api.render(); return; }
        const r = cv.getBoundingClientRect(), mx2 = e.clientX - r.left, my = e.clientY - r.top;
        let best = null;
        for (const b of bars) {
          if (!b.V) continue;
          const xs = b.V.map((v) => v[0]), ys = b.V.map((v) => v[1]);
          if (mx2 >= Math.min(...xs) - 2 && mx2 <= Math.max(...xs) + 2 && my >= Math.min(...ys) - 2 && my <= Math.max(...ys) + 2) {
            const z = Math.min(...b.V.map((v) => v[2]));
            if (!best || z < best.z) best = { b, z };
          }
        }
        const nb = best ? best.b : null;
        if (nb !== hot) { hot = nb; api.render(); }
        if (!hot) { tip.style.display = 'none'; return; }
        const w = hot.w, p = pw[hot.r.ticker];
        tip.innerHTML = `<b>${esc(hot.r.ticker)}</b> <span class="${w >= 0 ? 'pp-pos' : 'pp-neg'}">${w >= 0 ? 'long' : 'short'} ${pct(w)}</span><br>` +
          `<span class="faint">last week</span> ${p != null ? pct(p) : 'not held'}${p != null ? ` · <span class="faint">change</span> ${pp(w - p)}` : ''}<br>` +
          `<span class="faint">risk share</span> ${pctU(+hot.r.risk_contribution, 1)} · <span class="faint">μ_G</span> ${pct(+hot.r.mu_g, 2)} · <span class="faint">σ_G</span> ${pctU(+hot.r.sigma_g, 1)}`;
        tip.style.display = 'block';
        tip.style.left = Math.min(r.width - tip.offsetWidth - 6, mx2 + 14) + 'px';
        tip.style.top = Math.max(0, my - tip.offsetHeight - 12) + 'px';
      },
    });
    S3.render();
    $('skyMeta').innerHTML = `book of <strong style="color:var(--text1)">${dLabel(last)}</strong> · ${n} names${prev ? ' · wires = last week' : ''}`;
    window.addEventListener('resize', debounce(() => { S3.size(); S3.render(); }, 200));
  }

  // ═══ 3D: correlation landscapes (linked cameras, linked hover) ═════════
  // ρ̂ and ρ_G share one height and colour scale, so the flattening is real, not a rescale. Σ_G is shown relative
  // to its largest entry. Hovering a pillar lights the same pair in all three.
  let HM3 = null;
  const SLATE = [40, 48, 63], ICE_RGB = [79, 195, 247], RED_RGB = [240, 96, 96];
  function landRGB(v, scale) {
    const a = Math.pow(Math.min(1, Math.abs(v) / scale), 0.75), to = v >= 0 ? ICE_RGB : RED_RGB;
    return SLATE.map((c, k) => Math.round(c + (to[k] - c) * a));
  }
  function landscapes(mat) {
    const sFlat = mat.sigma_matrix.flat(), sMax = Math.max(...sFlat.map(Math.abs)) || 1;
    const Sig = mat.sigma_matrix.map((r) => r.map((v) => v / sMax));
    const T = mat.tickers || [];
    const views = [
      { id: 'hm3RhoHat', M: mat.rho_hat, rho: true, Hh: 9, scale: 0.6, name: 'ρ̂' },
      { id: 'hm3RhoG', M: mat.rho_g, rho: true, Hh: 9, scale: 0.6, name: 'ρ_G' },
      { id: 'hm3SigmaG', M: Sig, rho: false, Hh: 7, scale: 0.35, name: 'Σ_G' },
    ];
    let hl = null;                       // highlighted pair {i, j}
    const scenes = [];
    for (const vw of views) {
      const cv = $(vw.id); if (!cv) continue;
      const N = vw.M.length;
      let tip = cv.parentElement.querySelector('.c-tip');
      if (!tip) { tip = document.createElement('div'); tip.className = 'c-tip'; cv.parentElement.appendChild(tip); }
      const cells = [];
      for (let i = 0; i < N; i++) for (let j = 0; j < N; j++) cells.push({ i, j, v: vw.M[i][j], diag: i === j, V: null });
      const sc = Scene(cv, {
        yaw: -0.78, pitch: 0.66, cy: 0.66, span: [N * 1.36, N * 1.04], persp: 70, sway: 0,
        draw(ctx, proj) {
          const order = cells.map((cl) => ({ cl, z: proj(cl.j - N / 2 + 0.5, 0, cl.i - N / 2 + 0.5)[2] })).sort((a, b) => b.z - a.z);
          for (const { cl } of order) {
            const x = cl.j - N / 2, z = cl.i - N / 2, on = hl && ((hl.i === cl.i && hl.j === cl.j) || (hl.i === cl.j && hl.j === cl.i));
            if (vw.rho && cl.diag) { cl.V = box(ctx, proj, [x + 0.12, 0, z + 0.12, x + 0.88, 0.06, z + 0.88], [70, 78, 96], 0.6); continue; }
            const h = cl.v * vw.Hh;
            cl.V = box(ctx, proj, [x + 0.08, Math.min(0, h), z + 0.08, x + 0.92, Math.max(0, h) || 0.03, z + 0.92],
              on ? [255, 214, 140] : landRGB(cl.v, vw.scale), 0.98, on ? 'rgba(255,255,255,0.9)' : null);
          }
        },
        hover(e) {
          if (!e) { if (hl) { hl = null; scenes.forEach((s2) => s2.render()); } tip.style.display = 'none'; return; }
          const r = cv.getBoundingClientRect(), mx = e.clientX - r.left, my = e.clientY - r.top;
          let best = null;
          for (const cl of cells) {
            if (!cl.V) continue;
            let x0 = Infinity, x1 = -Infinity, y0 = Infinity, y1 = -Infinity, z = Infinity;
            for (const v of cl.V) { if (v[0] < x0) x0 = v[0]; if (v[0] > x1) x1 = v[0]; if (v[1] < y0) y0 = v[1]; if (v[1] > y1) y1 = v[1]; if (v[2] < z) z = v[2]; }
            if (mx >= x0 && mx <= x1 && my >= y0 && my <= y1 && (!best || z < best.z)) best = { cl, z };
          }
          const nh = best && !best.cl.diag ? { i: best.cl.i, j: best.cl.j } : null;
          if (JSON.stringify(nh) !== JSON.stringify(hl)) { hl = nh; scenes.forEach((s2) => s2.render()); }
          if (!hl) { tip.style.display = 'none'; return; }
          const a = T[hl.i] || hl.i, b = T[hl.j] || hl.j;
          tip.innerHTML = `<b>${esc(a)} × ${esc(b)}</b><br><span class="faint">sample ρ̂</span> ${num(mat.rho_hat[hl.i][hl.j])} → ` +
            `<span class="faint">Galton ρ_G</span> <b>${num(mat.rho_g[hl.i][hl.j])}</b><br><span class="faint">Σ_G</span> ${(mat.sigma_matrix[hl.i][hl.j] * 1e4).toFixed(2)} ×10⁻⁴`;
          tip.style.display = 'block';
          tip.style.left = Math.min(r.width - tip.offsetWidth - 4, Math.max(0, mx + 12)) + 'px';
          tip.style.top = Math.max(0, my - tip.offsetHeight - 10) + 'px';
        },
      });
      sc.render(); scenes.push(sc);
    }
    // turning one turns all three, so ρ̂ and ρ_G can be compared from the same angle
    scenes.forEach((s2) => s2.onTurn((yaw, pitch) => scenes.forEach((o) => { if (o !== s2) { o.st.yaw = yaw; o.st.pitch = pitch; o.render(); } })));
    HM3 = scenes;
  }
  function wireHeatmapToggle() {
    const seg = $('hmMode'); if (!seg) return;
    const setMode = (mode) => {
      seg.querySelectorAll('button').forEach((x) => x.classList.toggle('active', x.dataset.mode === mode));
      document.querySelectorAll('.hm-flat').forEach((el) => { el.style.display = mode === 'flat' ? '' : 'none'; });
      document.querySelectorAll('.hm-3d').forEach((el) => { el.style.display = mode === '3d' ? '' : 'none'; });
      if (mode !== '3d') return;
      const go = (mat) => { if (!HM3) landscapes(mat); else HM3.forEach((s) => { s.size(); s.render(); }); };
      if (window.galtonMats) go(window.galtonMats);
      else document.addEventListener('galton:mats', (ev) => { if (seg.querySelector('button.active').dataset.mode === '3d') go(ev.detail); }, { once: true });
    };
    seg.addEventListener('click', (e) => { const b = e.target.closest('button'); if (b) setMode(b.dataset.mode); });
    setMode((seg.querySelector('button.active') || seg.querySelector('button')).dataset.mode);
    window.addEventListener('resize', debounce(() => { if (HM3) HM3.forEach((s) => { s.size(); s.render(); }); }, 200));
  }
  function scrollSpy() {
    const links = [...document.querySelectorAll('#rail a')];
    if (!links.length || !('IntersectionObserver' in window)) return;
    const vis = new Map();
    const io = new IntersectionObserver((es) => {
      for (const e of es) vis.set(e.target.id, e.isIntersecting ? e.intersectionRatio : 0);
      let best = null, br = 0; for (const [id, r] of vis) if (r > br) { br = r; best = id; }
      links.forEach((a) => a.classList.toggle('on', a.getAttribute('href') === '#' + best));
    }, { threshold: [0, 0.1, 0.3, 0.6], rootMargin: '-40px 0px -45% 0px' });
    for (const a of links) { const s = document.getElementById(a.getAttribute('href').slice(1)); if (s) io.observe(s); }
  }

  // ═══ depth: tilt cards toward the pointer ══════════════════════════════
  function tilt(els) {
    if (REDUCED || !HOVER) return;
    els.forEach((el) => {
      if (el._tilt) return; el._tilt = true;
      el.addEventListener('pointermove', (e) => {
        const r = el.getBoundingClientRect(), px = (e.clientX - r.left) / r.width, py = (e.clientY - r.top) / r.height;
        el.style.setProperty('--ry', ((px - 0.5) * 9).toFixed(2) + 'deg');
        el.style.setProperty('--rx', ((0.5 - py) * 7).toFixed(2) + 'deg');
        el.style.setProperty('--mx', (px * 100).toFixed(1) + '%'); el.style.setProperty('--my', (py * 100).toFixed(1) + '%');
        el.style.setProperty('--lit', '1');
      });
      el.addEventListener('pointerleave', () => { el.style.setProperty('--rx', '0deg'); el.style.setProperty('--ry', '0deg'); el.style.setProperty('--lit', '0'); });
    });
  }
  function reveal(el) {
    if (!el) return;
    if (REDUCED || !('IntersectionObserver' in window)) { el.classList.add('in'); return; }
    const io = new IntersectionObserver((es) => { if (es[0].isIntersecting) { el.classList.add('in'); io.disconnect(); } }, { threshold: 0.2 });
    io.observe(el);
  }
  function debounce(f, ms) { let t; return () => { clearTimeout(t); t = setTimeout(f, ms); }; }

  // ═══ boot ══════════════════════════════════════════════════════════════
  async function boot() {
    wireHeatmapToggle();
    scrollSpy();
    tilt(document.querySelectorAll('.kpis .kpi, .pp-kpis .kpi'));
    const D = await load();
    if (!D || !D.pts || D.pts.length < 3) {
      ['lifeCards', 'duel'].forEach((id) => { const el = $(id); if (el) el.innerHTML = '<div class="ph">Not enough history yet.</div>'; });
      if (D && D.books && D.books.length) skyline(D);
      return;
    }
    const S = analyse(D);
    const steps = [() => renderCards(D, S), () => lifeChart(D, S), () => renderRisk(D, S), () => ridges(S), () => varBacktest(S), () => weekDuel(S), () => skyline(D)];
    for (const f of steps) { try { f(); } catch (e) { console.error(e); } }
    window.addEventListener('resize', debounce(() => { try { lifeChart(D, S); ridges(S); } catch (e) { console.error(e); } }, 200));
    window.galtonLife = { D, S };      // for the console
  }
  if (document.readyState === 'loading') document.addEventListener('DOMContentLoaded', boot); else boot();
})();
