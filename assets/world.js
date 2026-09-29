/* ═══════════════════════════════════════════════════════════════════════════
   WORLD.JS — slensvik.com global intelligence board (world.html)
   ─────────────────────────────────────────────────────────────────────────────
   Reads the ENEXT world-intel tables through PostgREST (api.slensvik.com):
     world_board               registry ⨝ snapshot — every series, one request
     world_brief               ranked intel cards (rules engine, per day)
     world_regime              daily risk-regime score + components
     world_country_snapshot    one JSON doc per country (globe, drawer, table)
     world_hotspots / world_chokepoint_snapshot / world_hazards
     world_tension_global / world_sanctions_* / world_power / world_reservoir*
     world_source_health       pipeline observability
     rpc/world_history         compact multi-series history (arrays)
   Depends on d3 v7 + topojson-client (jsDelivr) and assets/intel.js.
   Everything renders from data; every text that came from an API is escaped.
   ═══════════════════════════════════════════════════════════════════════ */
(function () {
  'use strict';

  const API = (window.INTEL && window.INTEL.API) || 'https://api.slensvik.com';
  const QS = new URLSearchParams(location.search);
  const REDUCE = !!(window.matchMedia && matchMedia('(prefers-reduced-motion: reduce)').matches) || QS.has('static');
  const COARSE = !!(window.matchMedia && matchMedia('(pointer: coarse)').matches);
  const YEAR = new Date().getUTCFullYear();
  const MINUS = '−';
  const $ = (s, r = document) => r.querySelector(s);
  const $$ = (s, r = document) => Array.from(r.querySelectorAll(s));
  const esc = (s) => String(s == null ? '' : s).replace(/[&<>"']/g, (c) =>
    ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]));
  const css = (name) => getComputedStyle(document.documentElement).getPropertyValue(name).trim();
  const clamp = (v, lo, hi) => Math.max(lo, Math.min(hi, v));
  const isNum = (v) => v != null && isFinite(v);

  /* ── formatting (en-US grouping; true minus signs) ─────────────────────── */
  const NF = {};
  const nf = (dp) => (NF[dp] = NF[dp] || new Intl.NumberFormat('en-US', { minimumFractionDigits: dp, maximumFractionDigits: dp }));
  const fmt = (v, dp = 2) => (isNum(v) ? nf(dp).format(v).replace('-', MINUS) : '—');
  const sgn = (v, dp = 2, suf = '') => {
    if (!isNum(v)) return '—';
    const r = +(+v).toFixed(dp);          // sign of what is shown, so −0.004 never renders as "−0.00"
    return (r > 0 ? '+' : r < 0 ? MINUS : '±') + nf(dp).format(Math.abs(r)) + suf;
  };
  const cls = (v) => (!isNum(v) || v === 0 ? 'flat' : v > 0 ? 'up' : 'dn');
  const fixMinus = (s) => String(s).replace(/(^|[\s(:~])-(?=[\d$€.])/g, '$1' + MINUS);
  const pDate = (s) => (s ? new Date(String(s).slice(0, 10) + 'T00:00:00Z') : null);
  const iso = (d) => d.toISOString().slice(0, 10);
  const addDays = (d, n) => new Date(d.getTime() + n * 864e5);
  const dShort = (d) => (d ? d.toLocaleDateString('en-GB', { day: 'numeric', month: 'short', timeZone: 'UTC' }) : '—');
  const dLong = (d) => (d ? d.toLocaleDateString('en-GB', { day: 'numeric', month: 'short', year: 'numeric', timeZone: 'UTC' }) : '—');
  function ago(ts) {
    if (!ts) return '—';
    const s = (Date.now() - new Date(ts).getTime()) / 1000;
    if (s < 90) return 'just now';
    if (s < 3600) return Math.round(s / 60) + ' min ago';
    if (s < 86400 * 1.5) return Math.round(s / 3600) + ' h ago';
    return Math.round(s / 86400) + ' d ago';
  }

  // level of a series in its natural unit
  function lvl(r, v) {
    v = v === undefined ? r.last : v;
    if (!isNum(v)) return '—';
    const u = r.unit || '', dp = r.decimals != null ? r.decimals : 2;
    if (u === '%') return fmt(v, Math.min(2, dp)) + '%';
    if (u.indexOf('USD/') === 0) return '$' + fmt(v, dp);
    if (u === 'EUR/MWh') return '€' + fmt(v, dp);
    if (u === 'USc/bu' || u === 'USc/lb') return fmt(v, dp) + '¢';
    if (u === 'NOK/kWh') return fmt(v, 3);
    if (u === 'NOK/kg') return fmt(v, 2);
    if (u === 'USD bn') return '$' + fmt(v, 0) + 'bn';
    if (u === 'k') return fmt(v, 0) + 'k';
    if (u === 'pp') return sgn(v * 100, 0, 'bp');
    if (r.asset_class === 'crypto') return '$' + fmt(v, dp);
    return fmt(v, dp);
  }
  // a move in the series' own change mode
  function chg(r, v) {
    if (!isNum(v)) return '—';
    if (r.chg_mode === 'bp') return sgn(v, 0, 'bp');
    if (r.chg_mode === 'pct') return sgn(v, Math.abs(v) >= 10 ? 1 : 2, '%');
    if (r.unit === '%') return sgn(v, Math.min(2, r.decimals != null ? r.decimals : 1), 'pp');
    return sgn(v, r.decimals != null ? Math.min(3, r.decimals) : 2);
  }

  /* ── data layer ────────────────────────────────────────────────────────── */
  async function api(path, timeout = 25000) {
    const ctl = new AbortController();
    const t = setTimeout(() => ctl.abort(), timeout);
    try {
      const r = await fetch(`${API}/${path}`, { headers: { Accept: 'application/json' }, signal: ctl.signal });
      if (!r.ok) throw new Error('HTTP ' + r.status);
      return await r.json();
    } finally { clearTimeout(t); }
  }
  const HCACHE = new Map();      // sid → {days, dates:[Date], vals:[number]}
  async function history(ids, days = 400) {
    const need = ids.filter((id) => !(HCACHE.has(id) && HCACHE.get(id).days >= days));
    if (need.length) {
      const since = iso(addDays(new Date(), -days));
      const rows = await api(`rpc/world_history?ids=${encodeURIComponent('{' + need.join(',') + '}')}&since=${since}`);
      for (const id of need) HCACHE.set(id, { days, dates: [], vals: [] });
      for (const r of rows || []) HCACHE.set(r.series_id, { days, dates: r.dates.map(pDate), vals: r.vals });
    }
    const out = {};
    const cut = addDays(new Date(), -days);
    for (const id of ids) {
      const h = HCACHE.get(id);
      if (!h) continue;
      const pts = [];
      for (let i = 0; i < h.dates.length; i++) if (h.dates[i] >= cut) pts.push([h.dates[i], h.vals[i]]);
      out[id] = pts;
    }
    return out;
  }

  const S = {
    board: new Map(), boardList: [], docs: new Map(), brief: [], regime: [], regimeNow: null,
    hot: [], choke: [], hazards: [], health: [], lastRun: null, geoGlobal: [],
  };
  const B = (id) => S.board.get(id);

  /* ═══ CHART KIT (SVG, d3 scales/shapes) ═══════════════════════════════════ */
  const RO = ('ResizeObserver' in window) ? new ResizeObserver((entries) => {
    for (const e of entries) {
      const el = e.target;
      clearTimeout(el._rz);
      el._rz = setTimeout(() => { if (el._draw && el.clientWidth !== el._w) el._draw(); }, 120);
    }
  }) : null;
  function mount(el, drawFn) {
    el._draw = () => { el._w = el.clientWidth; drawFn(el); };
    if (RO && !el._ro) { RO.observe(el); el._ro = true; }
    el._draw();
  }
  function tipAt(el, html, x, y) {
    let tt = el.querySelector(':scope > .tt');
    if (!tt) { tt = document.createElement('div'); tt.className = 'tt'; el.appendChild(tt); }
    tt.innerHTML = html;
    tt.style.display = 'block';
    const w = tt.offsetWidth, h = tt.offsetHeight, W = el.clientWidth;
    let left = x + 14; if (left + w > W) left = x - w - 14; if (left < 0) left = 0;
    tt.style.left = left + 'px';
    tt.style.top = clamp(y - h / 2, -10, (el.clientHeight || 200) - h + 10) + 'px';
  }
  function tipHide(el) { const tt = el.querySelector(':scope > .tt'); if (tt) tt.style.display = 'none'; }
  const trow = (color, val, label) =>
    `<div class="tr"><i style="background:${color}"></i><b>${val}</b><span>${esc(label)}</span></div>`;

  function timeTickFmt(domain) {
    const span = (domain[1] - domain[0]) / 864e5;
    if (span > 500) return (d) => d.toLocaleDateString('en-GB', { month: 'short', year: '2-digit', timeZone: 'UTC' });
    if (span > 120) return (d) => d.toLocaleDateString('en-GB', { month: 'short', timeZone: 'UTC' });
    return (d) => d.toLocaleDateString('en-GB', { day: 'numeric', month: 'short', timeZone: 'UTC' });
  }

  /* lineChart(el, {series:[{key,label,color,data:[[x,y]],width,area,muted}], yFmt, xType:'time'|'num',
       xTicks:[[v,label]], bands:[{y0,y1,color}], hLines:[{y,color,label}], endLabels, yDomain, tipFmt}) */
  function lineChart(el, cfg) { mount(el, (e) => drawLineChart(e, cfg)); }
  function drawLineChart(el, cfg) {
    const W = el.clientWidth, H = el.clientHeight || 200;
    if (W < 60) return;
    const series = (cfg.series || []).filter((s) => s.data && s.data.filter((p) => isNum(p[1])).length > 1);
    if (!series.length) { el.innerHTML = '<div class="empty">No data yet</div>'; return; }
    const m = Object.assign({ t: 10, r: cfg.endLabels ? 70 : 12, b: 22, l: 46 }, cfg.margin || {});
    const num = cfg.xType === 'num';
    const xs = num ? (cfg.xScale === 'linear' ? d3.scaleLinear() : d3.scalePow().exponent(0.5)) : d3.scaleUtc();
    const allX = series.flatMap((s) => s.data.map((p) => p[0]));
    xs.domain(cfg.xDomain || d3.extent(allX)).range([m.l, W - m.r]);
    let ext = d3.extent(series.flatMap((s) => s.data.map((p) => p[1]).filter(isNum))
      .concat((cfg.hLines || []).map((h) => h.y).filter(isNum)));
    if (cfg.yDomain) ext = cfg.yDomain;
    const pad = (ext[1] - ext[0]) * 0.08 || Math.abs(ext[0]) * 0.05 || 1;
    const lo = ext[0] >= 0 ? Math.max(0, ext[0] - pad) : ext[0] - pad;
    const ys = d3.scaleLinear().domain(cfg.yDomain ? ext : [lo, ext[1] + pad]).nice(4).range([H - m.b, m.t]);
    const yf = cfg.yFmt || ((v) => fmt(v, 2));
    let s = `<svg viewBox="0 0 ${W} ${H}" height="${H}" role="img" aria-label="${esc(cfg.aria || 'chart')}">`;
    for (const b of cfg.bands || []) {
      const y0 = ys(clamp(b.y1, ys.domain()[0], ys.domain()[1])), y1 = ys(clamp(b.y0, ys.domain()[0], ys.domain()[1]));
      if (y1 > y0) s += `<rect x="${m.l}" y="${y0}" width="${W - m.l - m.r}" height="${y1 - y0}" fill="${b.color}"/>`;
    }
    for (const t of ys.ticks(4)) {
      s += `<line x1="${m.l}" x2="${W - m.r}" y1="${ys(t)}" y2="${ys(t)}" stroke="var(--grid)"/>`;
      s += `<text x="${m.l - 7}" y="${ys(t) + 3}" text-anchor="end" fill="var(--text3)" font-size="9.5">${esc(yf(t))}</text>`;
    }
    if (num) {
      for (const [v, lab] of cfg.xTicks || []) {
        s += `<text x="${xs(v)}" y="${H - 6}" text-anchor="middle" fill="var(--text3)" font-size="9.5">${esc(lab)}</text>`;
      }
    } else {
      const tf = timeTickFmt(xs.domain());
      for (const t of xs.ticks(Math.max(2, Math.floor(W / 95)))) {
        s += `<text x="${xs(t)}" y="${H - 6}" text-anchor="middle" fill="var(--text3)" font-size="9.5">${esc(tf(t))}</text>`;
      }
    }
    for (const h of cfg.hLines || []) {
      s += `<line x1="${m.l}" x2="${W - m.r}" y1="${ys(h.y)}" y2="${ys(h.y)}" stroke="${h.color || 'var(--axis)'}" stroke-width="1"/>`;
      if (h.label) s += `<text x="${W - m.r - 4}" y="${ys(h.y) - 4}" text-anchor="end" fill="var(--text4)" font-size="9">${esc(h.label)}</text>`;
    }
    const line = d3.line().defined((p) => isNum(p[1])).x((p) => xs(p[0])).y((p) => ys(p[1]))
      .curve(cfg.step ? d3.curveStepAfter : (num ? d3.curveMonotoneX : d3.curveLinear));
    const ord = series.slice().sort((a, b) => (a.muted ? 0 : 1) - (b.muted ? 0 : 1));
    for (const se of ord) {
      if (se.area) {
        const area = d3.area().defined((p) => isNum(p[1])).x((p) => xs(p[0])).y0(ys(Math.max(ys.domain()[0], Math.min(0, ys.domain()[1])))).y1((p) => ys(p[1]));
        s += `<path d="${area(se.data)}" fill="${se.color}" opacity="0.1"/>`;
      }
      s += `<path d="${line(se.data)}" fill="none" stroke="${se.color}" stroke-width="${se.width || (se.muted ? 1.2 : 2)}" stroke-linejoin="round" stroke-linecap="round" opacity="${se.muted ? 0.9 : 1}"/>`;
    }
    // end dots + labels (dropped if they would collide — legend + tooltip carry identity then)
    const ends = series.filter((se) => !se.muted).map((se) => {
      const pts = se.data.filter((p) => isNum(p[1]));
      const p = pts[pts.length - 1];
      return { se, x: xs(p[0]), y: ys(p[1]), v: p[1] };
    }).sort((a, b) => a.y - b.y);
    let labelsOk = cfg.endLabels;
    for (let i = 1; i < ends.length; i++) if (ends[i].y - ends[i - 1].y < 13) labelsOk = false;
    for (const e of ends) {
      s += `<circle cx="${e.x}" cy="${e.y}" r="4" fill="${e.se.color}" stroke="#0b0f17" stroke-width="2"/>`;
      if (labelsOk) s += `<text x="${e.x + 8}" y="${e.y + 3.5}" fill="var(--text2)" font-size="10">${esc(e.se.label)} <tspan fill="var(--text1)">${esc(yf(e.v))}</tspan></text>`;
    }
    s += `<line class="xh" x1="0" x2="0" y1="${m.t}" y2="${H - m.b}" stroke="var(--text3)" stroke-width="1" style="display:none"/>`;
    s += `<rect class="hit" x="${m.l}" y="0" width="${W - m.l - m.r}" height="${H}" fill="transparent"/></svg>`;
    el.innerHTML = s;
    const svg = el.querySelector('svg'), xh = svg.querySelector('.xh');
    const move = (ev) => {
      const r = svg.getBoundingClientRect();
      const px = (ev.clientX - r.left) * (W / r.width);
      const xv = xs.invert(px);
      let rows = '', snapX = null;
      const list = series.slice().sort((a, b) => (a.muted ? 1 : 0) - (b.muted ? 1 : 0));
      let head = '';
      for (const se of list) {
        const pts = se.data;
        let i = d3.bisector((p) => p[0]).center(pts, xv);
        if (i < 0 || i >= pts.length || !isNum(pts[i][1])) continue;
        const p = pts[i];
        if (snapX == null) { snapX = xs(p[0]); head = num ? (cfg.xLabel ? cfg.xLabel(p[0]) : fmt(p[0], 2)) : dLong(p[0]); }
        rows += trow(se.color, esc(cfg.tipFmt ? cfg.tipFmt(se, p[1]) : yf(p[1])), se.label);
      }
      if (snapX == null) return;
      xh.setAttribute('x1', snapX); xh.setAttribute('x2', snapX); xh.style.display = '';
      tipAt(el, `<div class="td">${esc(head)}</div>${rows}`, snapX * (r.width / W), (ev.clientY - r.top));
    };
    svg.addEventListener('pointermove', move);
    svg.addEventListener('pointerleave', () => { xh.style.display = 'none'; tipHide(el); });
  }

  /* hbars(el, {items:[{label,value,sub,color,hl}], fmt, diverging}) — horizontal bars, labels left */
  function hbars(el, cfg) { mount(el, (e) => drawHbars(e, cfg)); }
  function drawHbars(el, cfg) {
    const items = (cfg.items || []).filter((it) => isNum(it.value));
    const W = el.clientWidth;
    if (!items.length) { el.innerHTML = '<div class="empty">No data yet</div>'; return; }
    const rowH = cfg.rowH || 20, H = items.length * rowH + 6;
    el.style.height = H + 'px';
    const lab = cfg.labelW || 92, vw = 62;
    const max = d3.max(items, (d) => Math.abs(d.value)) || 1;
    const x = cfg.diverging ? d3.scaleLinear().domain([-max, max]).range([lab + vw, W - vw])
      : d3.scaleLinear().domain([0, max]).range([lab, W - vw]);
    const f = cfg.fmt || ((v) => fmt(v, 2));
    const x0 = x(0);
    let s = `<svg viewBox="0 0 ${W} ${H}" height="${H}">`;
    if (cfg.diverging) s += `<line x1="${x0}" x2="${x0}" y1="0" y2="${H}" stroke="var(--axis)"/>`;
    items.forEach((it, i) => {
      const y = i * rowH + 3, bh = Math.min(12, rowH - 7), cy = y + rowH / 2 - 1;
      const xv = x(it.value), bx = Math.min(x0, xv), bw = Math.max(1.5, Math.abs(xv - x0));
      const col = it.color || (cfg.diverging ? (it.value >= 0 ? 'var(--up)' : 'var(--dn)') : 'var(--s1)');
      s += `<g class="hb" data-i="${i}">`;
      s += `<rect x="0" y="${y - 2}" width="${W}" height="${rowH}" fill="transparent"/>`;
      s += `<text x="${cfg.diverging ? lab + vw - 8 - (it.value < 0 ? Math.abs(xv - x0) + 4 : 0) : lab - 8}" y="${cy + 3.5}" text-anchor="end" fill="${it.hl ? 'var(--amber)' : 'var(--text2)'}" font-size="10.5">${esc(it.label)}</text>`;
      s += `<rect x="${bx}" y="${cy - bh / 2}" width="${bw}" height="${bh}" rx="2" fill="${col}" opacity="${it.hl ? 1 : 0.82}"/>`;
      const tx = it.value >= 0 || !cfg.diverging ? xv + 6 : xv - 6;
      s += `<text x="${tx}" y="${cy + 3.5}" text-anchor="${it.value >= 0 || !cfg.diverging ? 'start' : 'end'}" fill="var(--text1)" font-size="10" class="num">${esc(f(it.value))}</text>`;
      s += `</g>`;
    });
    // re-place labels of negative bars so they sit left of the bar's end
    s += `</svg>`;
    el.innerHTML = s;
    if (cfg.diverging) {
      // labels: always in the left gutter
      $$('g.hb text:first-of-type', el).forEach((t) => { t.setAttribute('x', lab - 8 + vw - vw); });
    }
    el.querySelectorAll('g.hb').forEach((g) => {
      const it = items[+g.dataset.i];
      g.addEventListener('pointermove', (ev) => {
        const r = el.getBoundingClientRect();
        tipAt(el, `<div class="td">${esc(it.label)}</div>${trow(it.value >= 0 ? 'var(--up)' : 'var(--dn)', esc(f(it.value)), it.sub || '')}`,
          ev.clientX - r.left, ev.clientY - r.top);
      });
      g.addEventListener('pointerleave', () => tipHide(el));
    });
  }

  /* columns(el, {items:[{x:Date,value,label}], fmt, color}) — vertical bars on a time axis */
  function columns(el, cfg) { mount(el, (e) => drawColumns(e, cfg)); }
  function drawColumns(el, cfg) {
    const items = (cfg.items || []).filter((it) => isNum(it.value));
    const W = el.clientWidth, H = el.clientHeight || 200;
    if (!items.length || W < 60) { el.innerHTML = '<div class="empty">No data yet</div>'; return; }
    const m = { t: 8, r: 8, b: 22, l: 40 };
    const x = d3.scaleBand().domain(items.map((d, i) => i)).range([m.l, W - m.r]).paddingInner(0.18);
    const y = d3.scaleLinear().domain([0, d3.max(items, (d) => d.value) || 1]).nice(4).range([H - m.b, m.t]);
    const bw = Math.min(24, x.bandwidth());
    const f = cfg.fmt || ((v) => fmt(v, 0));
    let s = `<svg viewBox="0 0 ${W} ${H}" height="${H}">`;
    for (const t of y.ticks(4)) {
      s += `<line x1="${m.l}" x2="${W - m.r}" y1="${y(t)}" y2="${y(t)}" stroke="var(--grid)"/>`;
      s += `<text x="${m.l - 6}" y="${y(t) + 3}" text-anchor="end" fill="var(--text3)" font-size="9.5">${esc(f(t))}</text>`;
    }
    const every = Math.ceil(items.length / Math.max(2, Math.floor(W / 80)));
    items.forEach((it, i) => {
      const bx = x(i) + (x.bandwidth() - bw) / 2, by = y(it.value), bh = Math.max(0.5, y(0) - by);
      const r = Math.min(2, bw / 2, bh);
      s += `<path class="cb" data-i="${i}" d="M${bx},${y(0)}V${by + r}Q${bx},${by} ${bx + r},${by}H${bx + bw - r}Q${bx + bw},${by} ${bx + bw},${by + r}V${y(0)}Z" fill="${it.color || cfg.color || 'var(--s1)'}"/>`;
      if (i % every === 0 && it.x) {
        const lab = it.x instanceof Date ? it.x.toLocaleDateString('en-GB', { month: 'short', year: '2-digit', timeZone: 'UTC' }) : it.x;
        s += `<text x="${x(i) + x.bandwidth() / 2}" y="${H - 6}" text-anchor="middle" fill="var(--text3)" font-size="9.5">${esc(lab)}</text>`;
      }
    });
    s += `<rect class="hit" x="${m.l}" y="0" width="${W - m.l - m.r}" height="${H}" fill="transparent"/></svg>`;
    el.innerHTML = s;
    const svg = el.querySelector('svg');
    svg.addEventListener('pointermove', (ev) => {
      const r = svg.getBoundingClientRect();
      const px = (ev.clientX - r.left) * (W / r.width);
      const i = clamp(Math.floor((px - m.l) / x.step()), 0, items.length - 1);
      const it = items[i];
      svg.querySelectorAll('.cb').forEach((p) => p.setAttribute('opacity', +p.dataset.i === i ? 1 : 0.55));
      tipAt(el, `<div class="td">${esc(it.label || (it.x instanceof Date ? dLong(it.x) : it.x))}</div>${trow(it.color || cfg.color || 'var(--s1)', esc(f(it.value)), cfg.unit || '')}`,
        ev.clientX - r.left, ev.clientY - r.top);
    });
    svg.addEventListener('pointerleave', () => { svg.querySelectorAll('.cb').forEach((p) => p.setAttribute('opacity', 1)); tipHide(el); });
  }

  /* spark(values) → inline SVG string; end dot coloured by direction over the window */
  function spark(vals, o = {}) {
    const w = o.w || 120, h = o.h || 26;
    const v = (vals || []).map((x) => (isNum(x) ? +x : null));
    const pts = v.map((y, i) => [i, y]).filter((p) => p[1] != null);
    if (pts.length < 2) return `<svg width="${w}" height="${h}"></svg>`;
    let lo = d3.min(pts, (p) => p[1]), hi = d3.max(pts, (p) => p[1]);
    if (o.ref) { lo = Math.min(lo, d3.min(o.ref.filter(isNum))); hi = Math.max(hi, d3.max(o.ref.filter(isNum))); }
    if (hi === lo) { hi += 1; lo -= 1; }
    const X = (i) => 2 + (i / (v.length - 1)) * (w - 6), Y = (y) => h - 3 - ((y - lo) / (hi - lo)) * (h - 6);
    const line = d3.line().defined((p) => p[1] != null).x((p) => X(p[0])).y((p) => Y(p[1]));
    const col = o.color || 'var(--s1)';
    const first = pts[0][1], last = pts[pts.length - 1];
    const dir = o.dirColor === false ? col : (last[1] > first ? 'var(--up)' : last[1] < first ? 'var(--dn)' : 'var(--text3)');
    // fluid: stretch to the container's width (tiles) — non-scaling strokes keep the line crisp;
    // the end dot is dropped there because a non-uniform stretch would turn it into an ellipse
    let s = o.fluid
      ? `<svg viewBox="0 0 ${w} ${h}" preserveAspectRatio="none" style="width:100%;height:${h}px;display:block" aria-hidden="true">`
      : `<svg width="${w}" height="${h}" viewBox="0 0 ${w} ${h}" aria-hidden="true">`;
    if (o.ref) {
      const rp = o.ref.map((y, i) => [i, isNum(y) ? y : null]);
      s += `<path d="${line(rp)}" fill="none" stroke="var(--s-muted)" stroke-width="1.2"${o.fluid ? ' vector-effect="non-scaling-stroke"' : ''}/>`;
    }
    if (o.fill !== false) {
      const area = d3.area().defined((p) => p[1] != null).x((p) => X(p[0])).y0(h - 2).y1((p) => Y(p[1]));
      s += `<path d="${area(v.map((y, i) => [i, y]))}" fill="${col}" opacity="0.09"/>`;
    }
    s += `<path d="${line(v.map((y, i) => [i, y]))}" fill="none" stroke="${col}" stroke-width="1.4" stroke-linejoin="round"${o.fluid ? ' vector-effect="non-scaling-stroke"' : ''}/>`;
    if (!o.fluid) s += `<circle cx="${X(last[0])}" cy="${Y(last[1])}" r="2.6" fill="${dir}" stroke="#0b0f17" stroke-width="1.5"/>`;
    return s + '</svg>';
  }

  /* ═══ GLOBE (canvas 2D + d3-geo orthographic) ═══════════════════════════════ */
  const LANES = [   // stylised main sea lanes (waypoints hug the water); `via` = chokepoints that colour them
    { via: ['chokepoint1', 'chokepoint4'], pts: [[121.5, 31], [119.8, 24.7], [113, 14], [104.2, 1.3], [95, 6], [80.5, 5.5], [62, 13], [50, 12.6], [43.35, 12.6], [38.5, 20], [33.5, 27.5], [32.5, 29.9], [32.3, 31.3], [25, 34], [12, 37.3], [-5.6, 35.95], [-10, 41], [-9, 44], [-5, 48.5], [1.5, 51], [4, 52]] },
    { via: ['chokepoint7'], pts: [[104.2, 1.3], [90, -6], [60, -26], [20.5, -35.5], [5, -25], [-12, 5], [-18, 26], [-11, 37], [-9.5, 43], [-5, 48.5], [1.5, 51]] },
    { via: ['chokepoint6'], pts: [[50, 27], [56.4, 26.4], [58.5, 24], [63, 20], [73, 11], [80.5, 5.5]] },
    { via: ['chokepoint6'], pts: [[56.4, 26.4], [58.5, 24], [60, 16], [62, 13]] },
    { via: [], pts: [[121.5, 31], [130, 33], [142, 35.5], [165, 40], [-170, 42], [-150, 40], [-130, 36], [-118.3, 33.7]] },
    { via: ['chokepoint2'], pts: [[-118.3, 33.7], [-110, 20], [-92, 12], [-79.9, 9.1], [-79.5, 9.4], [-76, 13], [-73.7, 20], [-70, 26], [-74, 40.5]] },
    { via: ['chokepoint22'], pts: [[-79.5, 9.4], [-83, 15], [-85.6, 21.8], [-89, 26], [-94.8, 29.3]] },
    { via: [], pts: [[-74, 40.5], [-60, 42], [-40, 46], [-20, 49], [-5, 49.5], [1.5, 51]] },
    { via: ['chokepoint3'], pts: [[30.7, 46.5], [29.6, 43.5], [29.1, 41.2], [26.4, 40.2], [25, 37.5], [25, 34]] },
    { via: ['chokepoint10'], pts: [[24, 59.5], [19, 57], [14, 55.3], [12.85, 55.5], [11, 57.5], [8, 57.2], [4, 55], [1.5, 51]] },
    { via: ['chokepoint11', 'chokepoint12'], pts: [[113, 14], [119.8, 24.7], [122.5, 30], [129.2, 34.1], [135, 35]] },
    { via: ['chokepoint15'], pts: [[104.2, 1.3], [110, -5], [115.8, -8.4], [118, -15], [112, -25], [115.5, -32]] },
  ];
  const HOT_RGB = '240,96,96';

  function sunPos(date) {        // sub-solar point (NOAA low-precision formulae; ~0.01°)
    const d = date.getTime() / 864e5 + 2440587.5 - 2451545.0;
    const rad = Math.PI / 180;
    const g = (357.529 + 0.98560028 * d) * rad;
    const q = 280.459 + 0.98564736 * d;
    const L = (q + 1.915 * Math.sin(g) + 0.020 * Math.sin(2 * g)) * rad;
    const e = (23.439 - 0.00000036 * d) * rad;
    const ra = Math.atan2(Math.cos(e) * Math.sin(L), Math.cos(L)) / rad;
    const dec = Math.asin(Math.sin(e) * Math.sin(L)) / rad;
    const gmst = (18.697374558 + 24.06570982441908 * d) % 24;
    let lon = ra - gmst * 15;
    lon = ((lon + 540) % 360) - 180;
    return [lon, dec];
  }

  function starLayer(W, H, density, bright) {
    const dpr = Math.min(window.devicePixelRatio || 1, 2);
    const c = document.createElement('canvas');
    c.width = Math.max(2, Math.round(W * dpr)); c.height = Math.max(2, Math.round(H * dpr));
    const g = c.getContext('2d');
    for (let i = 0; i < Math.round(W * H / density); i++) {
      g.fillStyle = `rgba(190,208,255,${(bright ? 0.4 + Math.random() * 0.45 : 0.12 + Math.random() * 0.2).toFixed(2)})`;
      g.beginPath();
      g.arc(Math.random() * c.width, Math.random() * c.height, (0.35 + Math.random() * (bright ? 0.95 : 0.5)) * dpr, 0, Math.PI * 2);
      g.fill();
    }
    return c;
  }

  function makeGlobe(canvas, hooks) {
    const ctx = canvas.getContext('2d');
    const proj = d3.geoOrthographic().clipAngle(90).precision(0.6);
    const path = d3.geoPath(proj, ctx);
    const grat = d3.geoGraticule10();
    let W = 0, H = 0, R = 0, zoom = 1, zoomTo = 1;
    let rot = [-14, -30], rotTo = null;
    let spinning = !REDUCE, lastTouch = 0, vx = 0, vy = 0;
    let feats = [], mesh = null, fill = () => null;
    let hoverKey = null, stars = null, starsFar = null;
    let raf = 0, lastDraw = 0, inView = true;
    let chokes = [], hazards = [], beacons = [], hots = [];
    const over = { night: true, lanes: true, choke: true, hazards: true, beacons: true, hot: true };
    const t0 = performance.now();

    function place() {
      const wide = W > 900, phone = W < 600;
      // phones: the globe sits between the stacked copy (top) and the legend + layer row (bottom)
      R = Math.min(wide ? W * 0.33 : phone ? W * 0.42 : W * 0.44, H * (phone ? 0.29 : 0.43)) * zoom;
      proj.scale(R).translate([wide ? W * 0.55 : W / 2, phone ? H * 0.47 : H / 2 + (wide ? 0 : 18)]).rotate([rot[0], rot[1], 0]);
    }
    function resize() {
      const r = canvas.getBoundingClientRect();
      W = r.width; H = r.height;
      const dpr = Math.min(window.devicePixelRatio || 1, 2);
      canvas.width = Math.round(W * dpr); canvas.height = Math.round(H * dpr);
      ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
      stars = starsFar = null;
      place(); draw(performance.now());
    }
    const visible = (lonlat) => d3.geoDistance(lonlat, [-rot[0], -rot[1]]) < Math.PI / 2 - 0.03;

    function draw(now) {
      if (W < 50) return;
      const s = (now - t0) / 1000;
      place();
      const [cx, cy] = proj.translate();
      ctx.clearRect(0, 0, W, H);
      if (!stars) { stars = starLayer(W, H, 5200, true); starsFar = starLayer(W, H, 1500, false); }
      ctx.globalAlpha = REDUCE ? 0.7 : 0.55 + 0.2 * Math.sin(s * 0.3); ctx.drawImage(starsFar, 0, 0, W, H);
      ctx.globalAlpha = REDUCE ? 0.9 : 0.75 + 0.25 * Math.sin(1.3 + s * 0.2); ctx.drawImage(stars, 0, 0, W, H);
      ctx.globalAlpha = 1;
      // atmosphere
      let g = ctx.createRadialGradient(cx, cy, R * 0.94, cx, cy, R * 1.28);
      g.addColorStop(0, 'rgba(79,195,247,0.20)'); g.addColorStop(0.35, 'rgba(79,195,247,0.07)'); g.addColorStop(1, 'rgba(79,195,247,0)');
      ctx.fillStyle = g; ctx.beginPath(); ctx.arc(cx, cy, R * 1.28, 0, Math.PI * 2); ctx.fill();
      // ocean
      g = ctx.createRadialGradient(cx - R * 0.35, cy - R * 0.4, R * 0.05, cx, cy, R);
      g.addColorStop(0, '#12233a'); g.addColorStop(0.55, '#0a1526'); g.addColorStop(1, '#050a14');
      ctx.fillStyle = g; ctx.beginPath(); path({ type: 'Sphere' }); ctx.fill();
      // graticule
      ctx.beginPath(); path(grat); ctx.strokeStyle = 'rgba(120,160,220,0.07)'; ctx.lineWidth = 0.6; ctx.stroke();
      // countries
      for (const f of feats) {
        const c = fill(f);
        ctx.beginPath(); path(f);
        ctx.fillStyle = c || '#141b27';
        ctx.fill();
      }
      if (mesh) { ctx.beginPath(); path(mesh); ctx.strokeStyle = 'rgba(170,200,240,0.16)'; ctx.lineWidth = 0.55; ctx.stroke(); }
      // night side
      if (over.night) {
        const [slon, slat] = sunPos(new Date());
        const anti = [slon + 180, -slat];
        for (const [rad, a] of [[96, 0.07], [92, 0.09], [88, 0.1], [82, 0.1], [74, 0.08]]) {
          ctx.beginPath(); path(d3.geoCircle().center(anti).radius(rad)());
          ctx.fillStyle = `rgba(2,5,14,${a})`; ctx.fill();
        }
      }
      // lanes
      if (over.lanes) {
        ctx.save();
        ctx.setLineDash([1.6, 7.5]);
        ctx.lineDashOffset = REDUCE ? 0 : -(s * 16) % 1000;
        ctx.lineCap = 'round';
        for (const ln of LANES) {
          const st = worstStatus(ln.via);
          ctx.strokeStyle = st === 'disrupted' ? 'rgba(240,96,96,0.62)' : st === 'surging' ? 'rgba(245,166,35,0.6)' : 'rgba(126,210,250,0.5)';
          ctx.lineWidth = 1.3;
          ctx.beginPath(); path({ type: 'LineString', coordinates: ln.pts }); ctx.stroke();
        }
        ctx.restore();
      }
      // hotspot pulses
      if (over.hot) {
        for (const h of hots) {
          if (!h.c || !visible(h.c)) continue;
          const [x, y] = proj(h.c);
          const ph = REDUCE ? 0.4 : ((s * 0.45 + h.ph) % 1);
          const rr = 5 + ph * (10 + 8 * Math.min(2, (h.spike || 1) - 1));
          ctx.beginPath(); ctx.arc(x, y, rr, 0, Math.PI * 2);
          ctx.strokeStyle = `rgba(${HOT_RGB},${(0.75 * (1 - ph)).toFixed(3)})`; ctx.lineWidth = 1.4; ctx.stroke();
          ctx.beginPath(); ctx.arc(x, y, 2.2, 0, Math.PI * 2); ctx.fillStyle = `rgba(${HOT_RGB},0.95)`; ctx.fill();
        }
      }
      // chokepoints
      if (over.choke) {
        for (const c of chokes) {
          if (!visible([c.lon, c.lat])) { c.px = null; continue; }
          const [x, y] = proj([c.lon, c.lat]); c.px = [x, y];
          const col = c.status === 'disrupted' ? '240,96,96' : c.status === 'surging' ? '245,166,35' : '126,210,250';
          const big = (c.ref_vessels || 0) > 15000;
          if (c.status === 'disrupted' || c.status === 'surging') {
            const ph = REDUCE ? 0.5 : (s * 0.6 + c.ph) % 1;
            ctx.beginPath(); ctx.arc(x, y, 4 + ph * 12, 0, Math.PI * 2);
            ctx.strokeStyle = `rgba(${col},${(0.7 * (1 - ph)).toFixed(3)})`; ctx.lineWidth = 1.2; ctx.stroke();
          }
          const k = big ? 4.2 : 3.2;
          ctx.beginPath(); ctx.moveTo(x, y - k); ctx.lineTo(x + k * 0.9, y + k * 0.7); ctx.lineTo(x - k * 0.9, y + k * 0.7); ctx.closePath();
          ctx.fillStyle = `rgba(${col},0.95)`; ctx.fill();
          ctx.strokeStyle = '#050a14'; ctx.lineWidth = 1; ctx.stroke();
          if (zoom > 1.35 && big) {
            ctx.font = '500 9px IBM Plex Mono, monospace'; ctx.fillStyle = 'rgba(200,215,235,0.8)'; ctx.textAlign = 'left';
            ctx.fillText(c.name, x + 7, y + 3);
          }
        }
      }
      // hazards
      if (over.hazards) {
        for (const h of hazards) {
          if (!isNum(h.lat) || !visible([h.lon, h.lat])) { h.px = null; continue; }
          const [x, y] = proj([h.lon, h.lat]); h.px = [x, y];
          const col = h.alert_level === 'Red' ? '240,96,96' : '245,166,35';
          const ph = REDUCE ? 0.3 : (s * 0.35 + h.ph) % 1;
          ctx.beginPath(); ctx.arc(x, y, 3.5 + ph * 9, 0, Math.PI * 2);
          ctx.strokeStyle = `rgba(${col},${(0.55 * (1 - ph)).toFixed(3)})`; ctx.lineWidth = 1; ctx.stroke();
          ctx.beginPath(); ctx.arc(x, y, 3.2, 0, Math.PI * 2);
          ctx.strokeStyle = `rgba(${col},0.95)`; ctx.lineWidth = 1.6; ctx.stroke();
        }
      }
      // market beacons
      if (over.beacons) {
        for (const b of beacons) {
          if (!visible([b.lon, b.lat])) { b.px = null; continue; }
          const [x, y] = proj([b.lon, b.lat]); b.px = [x, y];
          const up = (b.chg || 0) >= 0;
          const col = up ? '67,224,151' : '240,96,96';
          const inten = clamp(0.45 + Math.abs(b.z || 0) * 0.25, 0.45, 1);
          if (b.open) {
            const ph = REDUCE ? 0.5 : (s * 0.8 + b.ph) % 1;
            ctx.beginPath(); ctx.arc(x, y, 3 + ph * 9, 0, Math.PI * 2);
            ctx.strokeStyle = `rgba(${col},${(0.6 * (1 - ph)).toFixed(3)})`; ctx.lineWidth = 1; ctx.stroke();
          }
          const gg = ctx.createRadialGradient(x, y, 0, x, y, 10);
          gg.addColorStop(0, `rgba(${col},${(0.45 * inten).toFixed(3)})`); gg.addColorStop(1, `rgba(${col},0)`);
          ctx.fillStyle = gg; ctx.beginPath(); ctx.arc(x, y, 10, 0, Math.PI * 2); ctx.fill();
          ctx.beginPath(); ctx.arc(x, y, 2.6, 0, Math.PI * 2);
          ctx.fillStyle = `rgba(${col},${inten.toFixed(3)})`; ctx.fill();
          if (zoom > 1.3) {
            ctx.font = '500 9px IBM Plex Mono, monospace'; ctx.textAlign = 'left';
            ctx.fillStyle = 'rgba(210,222,240,0.85)';
            ctx.fillText(`${b.city} ${b.chgTxt}`, x + 7, y - 5);
          }
        }
      }
      // hovered country outline
      if (hoverKey) {
        const f = feats.find((x) => x.key === hoverKey);
        if (f) { ctx.beginPath(); path(f); ctx.strokeStyle = 'rgba(245,166,35,0.95)'; ctx.lineWidth = 1.3; ctx.stroke(); }
      }
      // rim light
      ctx.beginPath(); path({ type: 'Sphere' }); ctx.strokeStyle = 'rgba(79,195,247,0.28)'; ctx.lineWidth = 1; ctx.stroke();
    }
    function worstStatus(ids) {
      let st = 'normal';
      for (const id of ids) {
        const c = chokes.find((x) => x.portid === id);
        if (!c) continue;
        if (c.status === 'disrupted') return 'disrupted';
        if (c.status === 'surging') st = 'surging';
      }
      return st;
    }

    function tick(now) {
      raf = requestAnimationFrame(tick);
      if (now - lastDraw < 33) return;
      const dt = Math.min(0.1, (now - (lastDraw || now)) / 1000);
      lastDraw = now;
      if (rotTo) {
        rot[0] += (rotTo[0] - rot[0]) * 0.12; rot[1] += (rotTo[1] - rot[1]) * 0.12;
        if (Math.abs(rotTo[0] - rot[0]) < 0.05 && Math.abs(rotTo[1] - rot[1]) < 0.05) rotTo = null;
      } else if (Math.abs(vx) > 0.01 || Math.abs(vy) > 0.01) {
        rot[0] += vx; rot[1] = clamp(rot[1] + vy, -72, 72); vx *= 0.93; vy *= 0.93;
      } else if (spinning && now - lastTouch > 3500) {
        rot[0] += 3.2 * dt;
      }
      zoom += (zoomTo - zoom) * 0.14;
      draw(now);
    }
    function start() {
      if (REDUCE || document.hidden || !inView) { draw(performance.now()); return; }
      if (!raf) raf = requestAnimationFrame(tick);
    }
    function stop() { if (raf) { cancelAnimationFrame(raf); raf = 0; } }

    // ── hit-testing ──
    function hit(px, py) {
      const near = (arr, lim) => {
        let best = null, bd = lim;
        for (const m of arr) {
          if (!m.px) continue;
          const d = Math.hypot(px - m.px[0], py - m.px[1]);
          if (d < bd) { bd = d; best = m; }
        }
        return best;
      };
      const lim = COARSE ? 16 : 9;
      if (over.hazards) { const h = near(hazards, lim); if (h) return { kind: 'hazard', item: h }; }
      if (over.choke) { const c = near(chokes, lim); if (c) return { kind: 'choke', item: c }; }
      if (over.beacons) { const b = near(beacons, lim); if (b) return { kind: 'beacon', item: b }; }
      const ll = proj.invert([px, py]);
      if (!ll || d3.geoDistance(ll, [-rot[0], -rot[1]]) > Math.PI / 2) return null;
      for (const f of feats) if (d3.geoContains(f, ll)) return { kind: 'country', item: f };
      return null;
    }

    // ── pointer interaction ──
    let drag = null;
    canvas.addEventListener('pointerdown', (e) => {
      drag = { x: e.clientX, y: e.clientY, x0: e.clientX, y0: e.clientY, moved: false, id: e.pointerId };
      lastTouch = performance.now(); vx = vy = 0; rotTo = null;
    });
    window.addEventListener('pointermove', (e) => {
      const r = canvas.getBoundingClientRect();
      if (drag && e.pointerId === drag.id) {
        const dx = e.clientX - drag.x, dy = e.clientY - drag.y;
        if (!drag.moved && Math.hypot(e.clientX - drag.x0, e.clientY - drag.y0) > 4) {
          drag.moved = true; canvas.classList.add('dragging');
          try { canvas.setPointerCapture(e.pointerId); } catch (_) {}
          hooks.onHover(null);
        }
        if (drag.moved) {
          const k = 0.26 / zoom;
          rot[0] += dx * k; rot[1] = clamp(rot[1] - dy * k, -72, 72);
          vx = dx * k * 0.6; vy = -dy * k * 0.6;
          drag.x = e.clientX; drag.y = e.clientY; lastTouch = performance.now();
          if (!raf) draw(performance.now());
        }
        return;
      }
      if (e.target !== canvas) return;
      const px = e.clientX - r.left, py = e.clientY - r.top;
      const h = hit(px, py);
      const key = h && h.kind === 'country' ? h.item.key : null;
      if (key !== hoverKey) { hoverKey = key; if (!raf) draw(performance.now()); }
      if (h) lastTouch = performance.now();
      hooks.onHover(h, px, py);
    });
    window.addEventListener('pointerup', (e) => {
      if (!drag || e.pointerId !== drag.id) return;
      const wasMoved = drag.moved;
      canvas.classList.remove('dragging');
      if (!wasMoved) {
        const r = canvas.getBoundingClientRect();
        const h = hit(e.clientX - r.left, e.clientY - r.top);
        if (h) hooks.onClick(h);
      }
      drag = null;
    });
    canvas.addEventListener('pointerleave', () => { if (!drag) { hoverKey = null; hooks.onHover(null); if (!raf) draw(performance.now()); } });
    canvas.addEventListener('dblclick', (e) => {
      const r = canvas.getBoundingClientRect();
      const ll = proj.invert([e.clientX - r.left, e.clientY - r.top]);
      zoomTo = zoomTo < 1.5 ? 1.8 : zoomTo < 2.4 ? 2.7 : 1;
      if (ll && zoomTo > 1) rotTo = [-ll[0], clamp(-ll[1], -72, 72)];
      lastTouch = performance.now();
      if (REDUCE) { zoom = zoomTo; if (rotTo) { rot = rotTo.slice(); rotTo = null; } draw(performance.now()); }
    });
    canvas.tabIndex = 0;
    canvas.addEventListener('keydown', (e) => {
      const k = { ArrowLeft: [8, 0], ArrowRight: [-8, 0], ArrowUp: [0, -6], ArrowDown: [0, 6] }[e.key];
      if (k) { e.preventDefault(); rot[0] += k[0]; rot[1] = clamp(rot[1] + k[1], -72, 72); lastTouch = performance.now(); draw(performance.now()); }
      if (e.key === '+' || e.key === '=') { zoomTo = Math.min(2.8, zoomTo + 0.4); if (REDUCE) { zoom = zoomTo; draw(performance.now()); } }
      if (e.key === '-') { zoomTo = Math.max(1, zoomTo - 0.4); if (REDUCE) { zoom = zoomTo; draw(performance.now()); } }
    });

    let rz = 0;
    window.addEventListener('resize', () => { clearTimeout(rz); rz = setTimeout(resize, 150); });
    if ('IntersectionObserver' in window) {
      new IntersectionObserver((es) => { for (const e of es) inView = e.isIntersecting; inView ? start() : stop(); },
        { rootMargin: '60px' }).observe(canvas);
    }
    document.addEventListener('visibilitychange', () => (document.hidden ? stop() : start()));
    resize();

    return {
      setWorld(features, borderMesh) { feats = features; mesh = borderMesh; draw(performance.now()); start(); },
      setFill(fn) { fill = fn; draw(performance.now()); },
      setMarkers(m) {
        const ph = () => Math.random();
        if (m.chokes) chokes = m.chokes.map((c) => Object.assign({ ph: ph() }, c));
        if (m.hazards) hazards = m.hazards.map((h) => Object.assign({ ph: ph() }, h));
        if (m.beacons) beacons = m.beacons.map((b) => Object.assign({ ph: ph() }, b));
        if (m.hots) hots = m.hots.map((h) => Object.assign({ ph: ph() }, h));
        draw(performance.now());
      },
      setOverlay(k, v) { over[k] = v; draw(performance.now()); },
      overlays: over,
      focus(lonlat, z = 1.6) { rotTo = [-lonlat[0], clamp(-lonlat[1], -72, 72)]; zoomTo = z; lastTouch = performance.now();
        if (REDUCE) { rot = rotTo.slice(); rotTo = null; zoom = z; draw(performance.now()); } },
      redraw() { draw(performance.now()); },
    };
  }

  /* ── globe layers ── */
  const BASE_LAND = '#18212f';
  function seqScale(color, gamma = 0.8) {
    const it = d3.interpolateLab('#1c2636', color);
    return (t) => it(Math.pow(clamp(t, 0, 1), gamma));
  }
  function divScale(neg, pos) {
    const a = d3.interpolateLab('#2c3444', neg), b = d3.interpolateLab('#2c3444', pos);
    return (t) => (t < 0 ? a(Math.min(1, -t)) : b(Math.min(1, t)));
  }
  const LAYERS = {
    tension: { label: 'Tension', title: 'Violence share of news coverage', sub: 'GDELT · 7 days', kind: 'seq',
      get: (d) => d.tension && d.tension.tension, domain: [0, 100], scale: seqScale('#ff5c50', 1.7),
      ticks: [[0, '0'], [50, '50'], [100, '100']], fmt: (v) => fmt(v, 0) + '/100' },
    policy: { label: 'Policy rate', title: 'Central bank policy rate', sub: 'BIS · Norges Bank', kind: 'seq',
      get: (d) => d.policy && d.policy.rate, domain: [0, 15], scale: seqScale('#6fd0ff', 0.7),
      ticks: [[0, '0%'], [5, '5%'], [10, '10%'], [15, '15%+']], fmt: (v) => fmt(v, 2) + '%' },
    real: { label: 'Real rate', title: 'Real policy rate (policy − inflation)', sub: 'IMF ' + YEAR + ' inflation', kind: 'div',
      get: (d) => d.real_rate, domain: [-6, 6], scale: divScale('#f5a623', '#6fd0ff'),
      ticks: [[-6, '−6'], [0, '0'], [6, '+6 pp']], fmt: (v) => sgn(v, 2, ' pp') },
    inflation: { label: 'Inflation', title: 'Consumer price inflation', sub: 'IMF WEO ' + YEAR, kind: 'seq',
      get: (d) => d.macro && d.macro.inflation && d.macro.inflation[YEAR], domain: [0, 12], scale: seqScale('#ffb13b', 0.8),
      ticks: [[0, '0%'], [6, '6%'], [12, '12%+']], fmt: (v) => fmt(v, 1) + '%' },
    growth: { label: 'GDP growth', title: 'Real GDP growth', sub: 'IMF WEO ' + YEAR, kind: 'div',
      get: (d) => d.macro && d.macro.gdp_growth && d.macro.gdp_growth[YEAR], domain: [-5, 7], mid: 0,
      scale: divScale('#f06060', '#43e097'), ticks: [[-5, '−5%'], [0, '0'], [7, '+7%']], fmt: (v) => sgn(v, 1, '%') },
    fx: { label: 'Currency 1M', title: 'Currency vs USD, 1 month', sub: 'Yahoo · + = stronger', kind: 'div',
      get: (d) => d.fx && d.fx.strength_1m, domain: [-5, 5], scale: divScale('#f06060', '#43e097'),
      ticks: [[-5, '−5%'], [0, '0'], [5, '+5%']], fmt: (v) => sgn(v, 2, '%') },
    equity: { label: 'Equities 1M', title: 'Main equity index, 1 month', sub: 'Yahoo', kind: 'div',
      get: (d) => d.equity && d.equity.chg_1m, domain: [-8, 8], scale: divScale('#f06060', '#43e097'),
      ticks: [[-8, '−8%'], [0, '0'], [8, '+8%']], fmt: (v) => sgn(v, 2, '%') },
    sanctions: { label: 'Sanctions', title: 'Active OFAC SDN entries linked', sub: 'address · nationality · flag', kind: 'log',
      get: (d) => d.sanctions && d.sanctions.listed, domain: [1, 5000], scale: seqScale('#e0609f', 0.9),
      ticks: [[1, '1'], [70, '70'], [5000, '5k']], fmt: (v) => fmt(v, 0) + ' entries' },
  };
  function layerColor(L, v) {
    if (!isNum(v)) return null;
    if (L.kind === 'seq') return L.scale((v - L.domain[0]) / (L.domain[1] - L.domain[0]));
    if (L.kind === 'log') return v <= 0 ? null : L.scale(Math.log(v) / Math.log(L.domain[1]));
    const mid = L.mid || 0;
    return L.scale(v >= mid ? (v - mid) / (L.domain[1] - mid) : -(mid - v) / (mid - L.domain[0]));
  }

  let GLOBE = null, LAYER = 'tension', FEATS = [];
  const TOPO_URL = 'https://cdn.jsdelivr.net/npm/world-atlas@2.0.2/countries-110m.json';
  const NAME_KEY = { 'Kosovo': 'XK', 'N. Cyprus': 'CY', 'Somaliland': 'SO' };

  async function initGlobe() {
    const status = $('#globeStatus');
    if (!window.d3 || !window.topojson) { status.textContent = 'Globe unavailable (map libraries blocked)'; return; }
    const canvas = $('#globeCanvas');
    GLOBE = makeGlobe(canvas, { onHover: globeHover, onClick: globeClick });
    buildLayerButtons();
    buildToggles();
    try {
      const topo = await (await fetch(TOPO_URL)).json();
      const fc = topojson.feature(topo, topo.objects.countries);
      const mesh = topojson.mesh(topo, topo.objects.countries, (a, b) => a !== b);
      FEATS = fc.features.filter((f) => f.properties.name !== 'Antarctica').map((f) => {
        f.key = f.id || NAME_KEY[f.properties.name] || f.properties.name;
        // label/pulse anchor: centroid of the largest polygon (MultiPolygon-safe)
        let geom = f.geometry;
        if (geom.type === 'MultiPolygon') {
          let best = null, ba = -1;
          for (const poly of geom.coordinates) {
            const a = d3.geoArea({ type: 'Polygon', coordinates: poly });
            if (a > ba) { ba = a; best = poly; }
          }
          geom = { type: 'Polygon', coordinates: best };
        }
        f.c = d3.geoCentroid(geom);
        return f;
      });
      GLOBE.setWorld(FEATS, mesh);
      status.style.display = 'none';
      applyLayer();
    } catch (e) {
      status.textContent = 'Map data unavailable';
      console.error('globe', e);
    }
  }
  function docForFeature(f) {
    if (!f) return null;
    const byNum = S.docsByNum && S.docsByNum.get(String(f.id || ''));
    if (byNum) return byNum;
    const k = NAME_KEY[f.properties && f.properties.name];
    return k ? S.docs.get(k) : null;
  }
  function applyLayer() {
    if (!GLOBE) return;
    const L = LAYERS[LAYER];
    GLOBE.setFill((f) => { const d = docForFeature(f); return d ? layerColor(L, L.get(d)) : null; });
    renderLegend();
    $$('.layer-btn').forEach((b) => b.setAttribute('aria-pressed', b.dataset.k === LAYER ? 'true' : 'false'));
  }
  function buildLayerButtons() {
    const box = $('#layers');
    for (const [k, L] of Object.entries(LAYERS)) {
      const b = document.createElement('button');
      b.type = 'button'; b.className = 'layer-btn'; b.dataset.k = k; b.textContent = L.label;
      b.addEventListener('click', () => { LAYER = k; applyLayer(); if (CT.open) renderCountryTable(); });
      box.appendChild(b);
    }
  }
  function buildToggles() {
    const box = $('#toggles');
    const T = [['lanes', 'Sea lanes'], ['choke', 'Chokepoints'], ['hazards', 'Disasters'], ['beacons', 'Markets'],
      ['hot', 'Hotspots'], ['night', 'Night']];
    for (const [k, lab] of T) {
      const b = document.createElement('button');
      b.type = 'button'; b.className = 'tg'; b.textContent = lab;
      b.setAttribute('aria-pressed', 'true');
      b.addEventListener('click', () => {
        const on = b.getAttribute('aria-pressed') !== 'true';
        b.setAttribute('aria-pressed', on ? 'true' : 'false');
        GLOBE && GLOBE.setOverlay(k, on);
      });
      box.appendChild(b);
    }
  }
  function renderLegend() {
    const L = LAYERS[LAYER], el = $('#legend');
    let n = 0;
    for (const d of S.docs.values()) if (isNum(L.get(d))) n++;
    el.innerHTML = `<div class="lg-t"><span>${esc(L.title)}</span><i>${esc(L.sub)}</i></div><canvas width="240" height="8"></canvas>
      <div class="lg-ticks">${L.ticks.map((t) => `<span>${esc(t[1])}</span>`).join('')}</div>
      <div class="lg-nd"><span>no data</span><span class="k-choke">chokepoint</span><span class="k-beacon">market</span><span class="k-hz">disaster</span><span style="margin-left:auto">${n} countries</span></div>`;
    const c = el.querySelector('canvas'), g = c.getContext('2d');
    for (let i = 0; i < 240; i++) {
      const t = i / 239;
      let v;
      if (L.kind === 'log') v = Math.exp(t * Math.log(L.domain[1]));
      else v = L.domain[0] + t * (L.domain[1] - L.domain[0]);
      g.fillStyle = layerColor(L, v) || BASE_LAND; g.fillRect(i, 0, 1, 8);
    }
  }

  /* hover card + click → drawer */
  function globeHover(h, x, y) {
    const fly = $('#flyout');
    if (!h) { fly.style.display = 'none'; return; }
    let html = '';
    if (h.kind === 'country') {
      const d = docForFeature(h.item);
      html = countryCard(d, h.item.properties.name);
    } else if (h.kind === 'choke') {
      const c = h.item;
      html = `<div class="fn">${esc(c.name)}</div><div class="fr">Chokepoint · ${esc(c.status)}</div>
        ${fr('Transits (7d avg)', isNum(c.avg7) ? fmt(c.avg7, 0) + '/day' : '—')}
        ${fr('vs same week last year', pctTxt(c.vs_ly), c.status === 'disrupted')}
        ${fr('vs 2023', pctTxt(c.vs_ref))}${fr('vs prior 90 days', pctTxt(c.vs_base))}
        ${fr('Tankers y/y', pctTxt(c.tanker_vs_ly))}${fr('Container y/y', pctTxt(c.container_vs_ly))}
        <div class="hint">Week to ${esc(dShort(pDate(c.latest_date)))} · IMF PortWatch</div>`;
    } else if (h.kind === 'beacon') {
      const b = h.item;
      html = `<div class="fn">${esc(b.city)}</div><div class="fr">${esc(b.name)} · ${b.open ? 'open' : 'closed'} · ${esc(b.local)}</div>
        ${fr('Last', esc(b.last))}${fr('Day', `<span class="${cls(b.chg)}">${esc(b.chgTxt)}</span>`, true)}
        ${fr('σ-move', isNum(b.z) ? sgn(b.z, 1, 'σ') : '—')}${fr('1 month', esc(b.m1))}${fr('YTD', esc(b.ytd))}`;
    } else if (h.kind === 'hazard') {
      const z = h.item;
      html = `<div class="fn">${esc(z.name || 'Hazard')}</div><div class="fr">GDACS · ${esc(z.alert_level)} alert</div>
        <div style="color:var(--text2);max-width:260px;white-space:normal">${esc(/^magnitude 0\b/i.test(z.severity || '') ? '' : (z.severity || ''))}</div>
        <div class="hint">${esc(z.country || '')}${z.from_date ? ' · since ' + esc(dShort(pDate(z.from_date))) : ''}</div>`;
    }
    fly.innerHTML = html;
    fly.style.display = 'block';
    const hero = $('#globe');
    const w = fly.offsetWidth, hh = fly.offsetHeight;
    let left = x + 18, top = y - hh / 2;
    if (left + w > hero.clientWidth - 8) left = x - w - 18;
    top = clamp(top, 8, hero.clientHeight - hh - 8);
    fly.style.left = left + 'px'; fly.style.top = top + 'px';
  }
  const pctTxt = (v) => (isNum(v) ? sgn(v * 100, 0, '%') : '—');
  const fr = (k, v, hl) => `<div class="row${hl ? ' hl' : ''}"><span>${esc(k)}</span><b>${v}</b></div>`;
  function countryCard(d, fallbackName) {
    if (!d) return `<div class="fn">${esc(fallbackName || '—')}</div><div class="fr">No data</div>`;
    const t = d.tension, p = d.policy, m = d.macro || {};
    const L = LAYERS[LAYER];
    const hl = (k) => LAYER === k;
    let s = `<div class="fn">${esc(d.name)}</div><div class="fr">${esc([d.region, d.sub].filter(Boolean).join(' · '))}</div>`;
    s += fr('Tension', t ? `${fmt(t.tension, 0)}/100${isNum(t.spike) ? ' · ' + fmt(t.spike, 1) + '×' : ''}` : '—', hl('tension'));
    if (p) s += fr('Policy rate', `${fmt(p.rate, 2)}%${isNum(p.step_bp) ? ` <span class="${cls(p.step_bp)}">${sgn(p.step_bp, 0, 'bp')}</span>` : ''}`, hl('policy'));
    if (isNum(d.real_rate)) s += fr('Real rate', sgn(d.real_rate, 2, ' pp'), hl('real'));
    if (d.y10) s += fr('10Y yield', fmt(d.y10.v, 2) + '%');
    const inf = m.inflation && m.inflation[YEAR], gdp = m.gdp_growth && m.gdp_growth[YEAR];
    if (isNum(inf)) s += fr(`Inflation ${YEAR}e`, fmt(inf, 1) + '%', hl('inflation'));
    if (isNum(gdp)) s += fr(`GDP growth ${YEAR}e`, sgn(gdp, 1, '%'), hl('growth'));
    if (d.fx && isNum(d.fx.strength_1m)) s += fr(`${d.ccy} vs USD 1M`, `<span class="${cls(d.fx.strength_1m)}">${sgn(d.fx.strength_1m, 2, '%')}</span>`, hl('fx'));
    if (d.equity) s += fr(`${d.equity.name} 1M`, `<span class="${cls(d.equity.chg_1m)}">${sgn(d.equity.chg_1m, 2, '%')}</span>`, hl('equity'));
    if (d.sanctions) s += fr('OFAC entries', fmt(d.sanctions.listed, 0) + (d.sanctions.new_90d ? ` · +${d.sanctions.new_90d} 90d` : ''), hl('sanctions'));
    if (d.hazards && d.hazards.length) s += fr('Disaster alerts', `${d.hazards.length} active`);
    s += `<div class="hint">${COARSE ? 'Tap again' : 'Click'} for the country dossier</div>`;
    return s;
  }
  let lastTapKey = null;
  function globeClick(h) {
    if (h.kind === 'country') {
      const d = docForFeature(h.item);
      if (COARSE && lastTapKey !== h.item.key) { lastTapKey = h.item.key; return; }
      if (d) openDrawer(d);
    } else if (h.kind === 'choke') {
      location.hash = '#shipping';
    } else if (h.kind === 'hazard' && h.item.url) {
      window.open(h.item.url, '_blank', 'noopener');
    }
  }

  /* ═══ COUNTRY DRAWER ═════════════════════════════════════════════════════ */
  function kv(k, v) { return `<div class="kv"><span>${esc(k)}</span><b>${v}</b></div>`; }
  function openDrawer(d) {
    const m = d.macro || {};
    const yv = (k, y) => (m[k] && isNum(m[k][y]) ? m[k][y] : null);
    let s = `<div class="dr-name">${esc(d.name)}</div><div class="dr-reg">${esc([d.region, d.sub, d.ccy].filter(Boolean).join(' · '))}</div>`;
    const t = d.tension;
    if (t) {
      const hs = S.hot.find((h) => h.iso2 === d.iso2);
      s += `<div class="dr-sec"><h4>Geopolitics · GDELT</h4>${kv('Tension (7d)', fmt(t.tension, 0) + '/100')}
        ${kv('Spike vs 60-day norm', isNum(t.spike) ? fmt(t.spike, 2) + '×' : '—')}
        ${kv('Violence articles, 7d', fmt(isNum(t.violence_7d) ? t.violence_7d : t.conflict_7d, 0) + ' of ' + fmt(t.articles_7d, 0))}
        ${t.rank ? kv('Hotspot rank', '#' + t.rank) : ''}`;
      if (hs && hs.spark) s += `<div style="margin-top:8px">${spark(hs.spark, { w: 390, h: 44, color: 'var(--dn)', dirColor: false })}</div>`;
      if (hs && hs.top_dyads && hs.top_dyads.length) {
        s += `<div class="note" style="margin-top:6px">Most-reported: ${hs.top_dyads.filter((x) => x.a1 && x.a2).slice(0, 3).map((x) =>
          esc(title(x.a1) + (x.a2 ? ' → ' + title(x.a2) : '') + ' (' + x.label.toLowerCase() + ')')).join(' · ')}</div>`;
      }
      if (hs && hs.top_sources && hs.top_sources.length) {
        s += `<div class="note">${hs.top_sources.slice(0, 3).map((x) =>
          `<a href="${esc(safeUrl(x.url))}" target="_blank" rel="noopener nofollow">${esc(x.domain)}</a>${x.title ? ' — ' + esc(x.title) : ''}`).join('<br>')}</div>`;
      }
      s += `</div>`;
    }
    if (d.policy || d.y10 || isNum(d.real_rate)) {
      const p = d.policy || {};
      s += `<div class="dr-sec"><h4>Rates</h4>`;
      if (d.policy) {
        s += kv(p.bank || 'Policy rate', p.range ? `${fmt(p.range[0], 2)}–${fmt(p.range[1], 2)}%` : fmt(p.rate, 2) + '%');
        if (p.step_date) s += kv('Last move', `<span class="${cls(p.step_bp)}">${sgn(p.step_bp, 0, ' bp')}</span> · ${esc(dLong(pDate(p.step_date)))}`);
        if (isNum(p.net_bp_12m)) s += kv('12 months', `${p.hikes_12m || 0} hike(s), ${p.cuts_12m || 0} cut(s) · ${sgn(p.net_bp_12m, 0, ' bp')}`);
      }
      if (isNum(d.real_rate)) s += kv(`Real rate (vs ${YEAR} inflation)`, sgn(d.real_rate, 2, ' pp'));
      if (d.y10) s += kv('10-year yield', `${fmt(d.y10.v, 2)}% <span class="${cls(d.y10.chg_1m)}">${sgn(d.y10.chg_1m, 0, 'bp')} 1M</span>`);
      s += `</div>`;
    }
    if (d.equity || d.fx) {
      s += `<div class="dr-sec"><h4>Markets</h4>`;
      if (d.equity) s += kv(d.equity.name, `${fmt(d.equity.last, 0)} · <span class="${cls(d.equity.chg_1d)}">${sgn(d.equity.chg_1d, 2, '%')}</span> 1D · <span class="${cls(d.equity.chg_ytd)}">${sgn(d.equity.chg_ytd, 1, '%')}</span> YTD`);
      if (d.fx) s += kv(`${d.ccy} vs USD`, `<span class="${cls(d.fx.strength_1m)}">${sgn(d.fx.strength_1m, 2, '%')}</span> 1M · <span class="${cls(d.fx.strength_ytd)}">${sgn(d.fx.strength_ytd, 1, '%')}</span> YTD`);
      s += `</div>`;
    }
    if (Object.keys(m).length) {
      s += `<div class="dr-sec"><h4>Macro · IMF WEO</h4><div class="kv"><span></span><b style="color:var(--text3)">${YEAR - 1} · ${YEAR}e · ${YEAR + 1}f</b></div>`;
      const rowM = (lab, k, dp, suf, signed) => {
        const vals = [YEAR - 1, YEAR, YEAR + 1].map((y) => yv(k, y));
        if (vals.every((v) => v == null)) return '';
        return kv(lab, vals.map((v) => (v == null ? '—' : signed ? sgn(v, dp, suf) : fmt(v, dp) + suf)).join(' · '));
      };
      s += rowM('Real GDP growth', 'gdp_growth', 1, '%', true) + rowM('Inflation', 'inflation', 1, '%') +
        rowM('Unemployment', 'unemployment', 1, '%') + rowM('Current account, % GDP', 'current_account', 1, '%', true) +
        rowM('Gov. balance, % GDP', 'gov_balance', 1, '%', true) + rowM('Gov. gross debt, % GDP', 'gov_debt', 0, '%') +
        rowM('GDP, USD bn', 'gdp_usd_bn', 0, '');
      s += `</div>`;
    }
    if (d.sanctions) {
      s += `<div class="dr-sec"><h4>Sanctions · OFAC SDN</h4>${kv('Active linked entries', fmt(d.sanctions.listed, 0))}
        ${kv('New, 90 days', fmt(d.sanctions.new_90d, 0))}${kv('New, 12 months', fmt(d.sanctions.new_365d, 0))}</div>`;
    }
    if (d.hazards && d.hazards.length) {
      s += `<div class="dr-sec"><h4>Disaster alerts · GDACS</h4>${d.hazards.map((z) =>
        kv(`${z.level} · ${z.type}`, esc(z.name || ''))).join('')}</div>`;
    }
    $('#drawerBody').innerHTML = s;
    $('#drawer').classList.add('open'); $('#scrim').classList.add('open');
    $('#drawer').setAttribute('aria-hidden', 'false');
    $('#drawerX').focus();
    const f = FEATS.find((x) => docForFeature(x) === d);
    if (f && GLOBE) GLOBE.focus(f.c, 1.5);
  }
  function closeDrawer() {
    $('#drawer').classList.remove('open'); $('#scrim').classList.remove('open');
    $('#drawer').setAttribute('aria-hidden', 'true');
  }
  const title = (s) => String(s || '').toLowerCase().replace(/\b[a-z]/g, (c) => c.toUpperCase());
  // mirrors the brief's rule (ENEXT analytics/brief.py GENERIC_ACTORS): "Community (fight)" says nothing
  const GENERIC = new Set(['COMMUNITY', 'EMPLOYEE', 'POLICE', 'PRISON', 'SCHOOL', 'STUDENT', 'TEACHER', 'PROTESTER',
    'MILITANT', 'ARMY', 'FIGHTER', 'SOLDIER', 'GOVERNMENT', 'AUTHORITIES', 'CITIZEN', 'RESIDENT', 'HOSPITAL', 'MEDIA',
    'COMPANY', 'BUSINESS', 'WORKER', 'CRIMINAL', 'SUSPECT', 'GUNMAN', 'PRESIDENT', 'MINISTER', 'OFFICIAL', 'JUDGE',
    'COURT', 'LAWMAKER', 'PARLIAMENT', 'UNIVERSITY', 'PARTY', 'OPPOSITION', 'REBEL', 'MIGRANT', 'REFUGEE', 'CHILD',
    'WOMAN', 'MEN', 'FAMILY', 'VILLAGE', 'TOWN', 'CITY', 'MILITARY', 'SECURITY FORCES', 'PRIME MINISTER', 'COMMANDER',
    'HEALTH MINIST', 'MEMBER NATION', 'THE UN', 'ATTACKER', 'KILLER', 'INMATE', 'DOCTOR']);
  const goodDyad = (list) => (list || []).find((x) => {
    const a1 = String(x.a1 || '').toUpperCase(), a2 = String(x.a2 || '').toUpperCase();
    return a1 && a2 && a1 !== a2 && !(GENERIC.has(a1) && GENERIC.has(a2));
  }) || null;
  const safeUrl = (u) => (/^https?:\/\//i.test(u || '') ? u : '#');

  /* ═══ COUNTRY TABLE VIEW (the globe's accessible twin) ═══════════════════ */
  const CT = { open: false, sort: null, dir: -1 };
  const CT_COLS = [
    ['name', 'Country', (d) => d.name, (v) => esc(v)],
    ['tension', 'Tension', (d) => d.tension && d.tension.tension, (v) => fmt(v, 0)],
    ['spike', 'Spike', (d) => d.tension && d.tension.spike, (v) => (isNum(v) ? fmt(v, 1) + '×' : '—')],
    ['policy', 'Policy', (d) => d.policy && d.policy.rate, (v) => (isNum(v) ? fmt(v, 2) + '%' : '—')],
    ['real', 'Real', (d) => d.real_rate, (v) => sgn(v, 1)],
    ['infl', 'Infl. ' + YEAR, (d) => d.macro && d.macro.inflation && d.macro.inflation[YEAR], (v) => (isNum(v) ? fmt(v, 1) + '%' : '—')],
    ['gdp', 'GDP ' + YEAR, (d) => d.macro && d.macro.gdp_growth && d.macro.gdp_growth[YEAR], (v) => sgn(v, 1, '%')],
    ['fx', 'FX 1M', (d) => d.fx && d.fx.strength_1m, (v) => sgn(v, 2, '%')],
    ['eq', 'Equity 1M', (d) => d.equity && d.equity.chg_1m, (v) => sgn(v, 2, '%')],
    ['sanc', 'OFAC', (d) => d.sanctions && d.sanctions.listed, (v) => fmt(v, 0)],
  ];
  function renderCountryTable() {
    const el = $('#ctable');
    const key = CT.sort || ({ tension: 'tension', policy: 'policy', real: 'real', inflation: 'infl', growth: 'gdp', fx: 'fx', equity: 'eq', sanctions: 'sanc' })[LAYER];
    const col = CT_COLS.find((c) => c[0] === key) || CT_COLS[1];
    const rows = Array.from(S.docs.values()).filter((d) => d.iso2 !== 'XM' && CT_COLS.slice(1).some((c) => isNum(c[2](d))));
    rows.sort((a, b) => {
      const va = col[2](a), vb = col[2](b);
      if (col[0] === 'name') return CT.dir * String(va).localeCompare(String(vb)) * -1;
      return (isNum(vb) ? vb : -Infinity) * -CT.dir - (isNum(va) ? va : -Infinity) * -CT.dir;
    });
    el.innerHTML = `<table class="intel-table"><thead><tr>${CT_COLS.map((c) =>
      `<th class="${c[0] === 'name' ? '' : 'r'}" data-k="${c[0]}">${esc(c[1])}${c[0] === col[0] ? (CT.dir < 0 ? ' ▾' : ' ▴') : ''}</th>`).join('')}</tr></thead>
      <tbody>${rows.map((d) => `<tr data-iso="${esc(d.iso2)}" style="cursor:pointer">${CT_COLS.map((c) =>
        `<td class="${c[0] === 'name' ? '' : 'r'}">${c[3](c[2](d))}</td>`).join('')}</tr>`).join('')}</tbody></table>`;
    el.querySelectorAll('th').forEach((th) => th.addEventListener('click', () => {
      CT.dir = CT.sort === th.dataset.k ? -CT.dir : -1; CT.sort = th.dataset.k; renderCountryTable();
    }));
    el.querySelectorAll('tbody tr').forEach((tr) => tr.addEventListener('click', () => {
      const d = S.docs.get(tr.dataset.iso); if (d) openDrawer(d);
    }));
  }

  /* ═══ SECTIONS ═══════════════════════════════════════════════════════════ */

  /* ── ticker tape ── */
  const TAPE = ['eq.spx', 'eq.ixic', 'eq.sx5e', 'eq.dax', 'eq.ftse', 'eq.osebx', 'eq.n225', 'eq.hsi', 'eq.shcomp',
    'eq.nifty', 'fx.dxy', 'fx.eurusd', 'fx.usdjpy', 'fx.usdnok', 'fx.eurnok', 'yc.us.2y', 'yc.us.10y', 'yc.de.10y',
    'yc.jp.10y', 'yc.no.10y', 'cmd.brent', 'cmd.ttf', 'cmd.gold', 'cmd.copper', 'vol.vix', 'cr.us_hy_oas', 'cx.btc',
    'cx.eth', 'nor.salmon'];
  function renderTape() {
    const items = TAPE.map(B).filter(Boolean);
    if (!items.length) return;
    const html = items.map((r) => `<span class="tape-item"><span class="tk">${esc(r.short)}</span><span class="v">${esc(lvl(r))}</span><span class="c ${cls(r.chg_1d)}">${esc(chg(r, r.chg_1d))}</span></span>`).join('');
    const track = $('#tapeTrack');
    track.innerHTML = html + (REDUCE ? '' : html);
    if (!REDUCE) {
      requestAnimationFrame(() => {
        const w = track.scrollWidth / 2;
        track.style.setProperty('--tape-dur', Math.round(w / 38) + 's');
        track.classList.add('run');
      });
    }
  }

  /* ── header pills + regime chip ── */
  function renderHeader() {
    const h = S.health;
    const now = Date.now();
    let ok = 0, warn = 0, bad = 0;
    for (const x of h) {
      const st = healthState(x, now);
      if (st === 'ok') ok++; else if (st === 'warn') warn++; else bad++;
    }
    const pill = $('#pillHealth');
    const last = S.lastRun ? S.lastRun.finished_at || S.lastRun.started_at : null;
    // the scheduler's own heartbeat outranks per-source states: a stalled pipeline can't be "healthy"
    const pipe = INTEL.pipelineHealth(last, now);
    pill.className = 'meta-pill ' + (pipe.state === 'bad' || bad ? 'bad' : pipe.state === 'warn' || warn ? 'warn' : 'ok');
    $('#pillHealthT').innerHTML = pipe.state === 'bad'
      ? `<b>Pipeline stalled</b> · last run ${ago(last)}`
      : h.length ? `<b>${ok}/${h.length}</b> sources healthy` : 'No health data';
    $('#pillAsof').textContent = last ? 'Updated ' + ago(last) : '—';
    $('#railAsof').textContent = last ? 'pipeline ' + ago(last) : '';
    const rg = S.regimeNow;
    if (rg) {
      const c = regimeColor(rg.label);
      $('#heroRegimeVal').textContent = rg.label;
      $('#heroRegimeVal').style.color = c;
      $('#heroRegimeSc').textContent = sgn(rg.score, 2);
    }
  }
  const regimeColor = (l) => (l === 'RISK-ON' ? 'var(--green)' : l === 'RISK-OFF' || l === 'STRESS' ? 'var(--red)' : 'var(--text2)');

  /* ── brief ── */
  const CATS = { pattern: ['✦', 'Pattern'], markets: ['◆', 'Markets'], rates: ['∿', 'Rates'], fx: ['⇄', 'FX'],
    commodities: ['◈', 'Commodities'], credit: ['▤', 'Credit'], macro: ['≡', 'Macro'], policy: ['▣', 'Central banks'],
    regime: ['◐', 'Regime'], geopolitics: ['⊕', 'Geopolitics'], shipping: ['◎', 'Shipping'], sanctions: ['⊘', 'Sanctions'],
    hazards: ['⚠︎', 'Hazards'], norway: ['◇', 'Norway'] };
  const sevColor = (s) => (s >= 70 ? 'var(--red)' : s >= 50 ? 'var(--amber)' : 'var(--ice)');
  function renderBrief(day) {
    const grid = $('#briefGrid');
    const days = Array.from(new Set(S.brief.map((b) => b.brief_date)));
    const sel = $('#briefDay');
    if (sel.options.length !== days.length) {
      sel.innerHTML = days.map((d, i) => `<option value="${esc(d)}">${i === 0 ? 'Today — ' : ''}${esc(dLong(pDate(d)))}</option>`).join('');
    }
    day = day || sel.value || days[0];
    const items = S.brief.filter((b) => b.brief_date === day).sort((a, b) => a.rank - b.rank);
    if (!items.length) { grid.innerHTML = '<div class="empty" style="grid-column:1/-1">No brief items — a quiet tape.</div>'; return; }
    $('#briefAsof').textContent = `${items.length} items · generated ${ago(items[0].created_at)}`;
    grid.innerHTML = items.map((b, i) => {
      const [g, lab] = CATS[b.category] || ['◆', b.category];
      const sid = (b.series_ids || [])[0];
      const r = sid && B(sid);
      const pips = Array.from({ length: 5 }, (_, k) => `<i class="${b.severity >= (k + 1) * 18 ? 'on' : ''}"></i>`).join('');
      const asof = b.data && b.data.as_of ? dShort(pDate(b.data.as_of)) : '';
      return `<a class="bcard${i < 2 ? ' lead' : ''}" href="#${esc(b.anchor || 'brief')}" style="--sev:${sevColor(b.severity)}"
          data-iso="${esc((b.data && b.data.iso2) || '')}">
        <div class="bmeta"><span class="cat"><b>${g}</b>${esc(lab)}</span><span class="bsev" title="Severity ${b.severity}/100">${pips}</span></div>
        <div class="bh">${esc(fixMinus(b.headline))}</div>
        ${b.detail ? `<div class="bd">${esc(fixMinus(b.detail))}</div>` : ''}
        <div class="bfoot"><span class="asof">${asof ? 'as of ' + esc(asof) : ''}</span>${r && r.spark ? spark(r.spark.slice(-60), { w: i < 2 ? 150 : 96, h: 24 }) : ''}</div>
      </a>`;
    }).join('');
    grid.querySelectorAll('.bcard[data-iso]').forEach((a) => {
      if (!a.dataset.iso) return;
      a.addEventListener('click', (e) => { const d = S.docs.get(a.dataset.iso); if (d) { e.preventDefault(); openDrawer(d); } });
    });
  }

  /* ── regime ── */
  const COMP = {
    vix: ['Equity implied vol (VIX)', (v) => fmt(v, 1)], move: ['Rates implied vol (MOVE)', (v) => fmt(v, 0)],
    hy: ['US high-yield spreads', (v) => fmt(v, 2) + '%'], trend: ['S&P 500 vs 200-day avg', (v) => sgn(v * 100, 1, '%')],
    breadth: ['Global breadth (>50d MA)', (v) => fmt(v, 0) + '%'], cu_au: ['Copper/gold, 20d', (v) => sgn(v * 100, 1, '%')],
    dxy: ['Dollar (DXY), 20d', (v) => sgn(v * 100, 1, '%')], em_fx: ['EM FX vs USD, 20d', (v) => sgn(v * 100, 1, '%')],
    btc: ['Bitcoin, 20d', (v) => sgn(v * 100, 1, '%')],
  };
  function renderRegime() {
    const rg = S.regimeNow;
    if (!rg) { $('#regimeLabel').textContent = '—'; return; }
    $('#regimeLabel').textContent = rg.label;
    $('#regimeLabel').style.color = regimeColor(rg.label);
    const hist = S.regime.slice().reverse();
    const p5 = hist.length > 6 ? hist[hist.length - 6].score : null;
    $('#regimeScore').innerHTML = `score <b style="color:var(--text1)">${sgn(rg.score, 2)}</b>${isNum(p5) ? ` · <span class="${cls(rg.score - p5)}">${sgn(rg.score - p5, 2)}</span> over 5 sessions` : ''}`;
    $('#regimeAsof').textContent = 'as of ' + dLong(pDate(rg.obs_date));
    mount($('#regimeGauge'), (el) => drawGauge(el, rg.score));
    lineChart($('#regimeHist'), {
      aria: 'Risk regime score history', yFmt: (v) => sgn(v, 1),
      series: [{ key: 'score', label: 'Regime score', color: 'var(--s1)', data: hist.map((r) => [pDate(r.obs_date), r.score]), area: true }],
      bands: [{ y0: 0.45, y1: 9, color: 'rgba(67,224,151,0.06)' }, { y0: -9, y1: -0.45, color: 'rgba(240,96,96,0.06)' }],
      hLines: [{ y: 0, color: 'var(--axis)' }],
      tipFmt: (se, v) => sgn(v, 2),
    });
    const comps = rg.components || {};
    const box = $('#regimeComps');
    const keys = Object.keys(COMP).filter((k) => comps[k]);
    const maxC = d3.max(keys, (k) => Math.abs(comps[k].z * comps[k].w)) || 1;
    box.innerHTML = keys.map((k) => {
      const c = comps[k], [lab, f] = COMP[k];
      const contrib = c.z * c.w;
      const wpx = Math.abs(contrib) / maxC * 50;
      return `<div class="comp-row"><span class="cl" title="${esc(lab)}">${esc(lab)} <span class="faint">${esc(isNum(c.raw) ? f(c.raw) : '')}</span></span>
        <svg viewBox="0 0 100 10" preserveAspectRatio="none" style="width:100%;height:10px"><line x1="50" x2="50" y1="0" y2="10" stroke="var(--axis)" stroke-width="0.6"/>
        <rect x="${contrib >= 0 ? 50 : 50 - wpx}" y="2" width="${Math.max(0.8, wpx)}" height="6" rx="1" fill="${contrib >= 0 ? 'var(--up)' : 'var(--dn)'}"/></svg>
        <span class="cz ${cls(c.z)}">${sgn(c.z, 2, 'σ')}</span></div>`;
    }).join('');
  }
  function drawGauge(el, score) {
    const W = el.clientWidth, H = el.clientHeight || 170;
    const cx = W / 2, cy = H - 14, R = Math.min(W / 2 - 16, H - 30);
    const a = (v) => Math.PI + (clamp(v, -2, 2) + 2) / 4 * Math.PI;   // −2 → left, +2 → right
    const arc = (v0, v1, r0, r1) => d3.arc()({ innerRadius: r0, outerRadius: r1, startAngle: a(v0) - Math.PI * 1.5, endAngle: a(v1) - Math.PI * 1.5 });
    const t = `translate(${cx},${cy})`;
    let s = `<svg viewBox="0 0 ${W} ${H}" height="${H}" role="img" aria-label="Regime gauge ${esc(fmt(score, 2))}">`;
    s += `<path transform="${t}" d="${arc(-2, -0.45, R - 12, R)}" fill="rgba(240,96,96,0.55)"/>`;
    s += `<path transform="${t}" d="${arc(-0.45, 0.45, R - 12, R)}" fill="rgba(164,173,191,0.28)"/>`;
    s += `<path transform="${t}" d="${arc(0.45, 2, R - 12, R)}" fill="rgba(67,224,151,0.55)"/>`;
    for (const v of [-2, -1, 0, 1, 2]) {
      const an = a(v), x1 = cx + Math.cos(an) * (R + 3), y1 = cy + Math.sin(an) * (R + 3), x2 = cx + Math.cos(an) * (R + 8), y2 = cy + Math.sin(an) * (R + 8);
      s += `<line x1="${x1}" y1="${y1}" x2="${x2}" y2="${y2}" stroke="var(--text4)"/>`;
    }
    const an = a(score);
    s += `<line x1="${cx}" y1="${cy}" x2="${cx + Math.cos(an) * (R - 18)}" y2="${cy + Math.sin(an) * (R - 18)}" stroke="var(--text1)" stroke-width="2.4" stroke-linecap="round"/>`;
    s += `<circle cx="${cx}" cy="${cy}" r="5" fill="var(--text1)"/>`;
    s += `<text x="${cx - R}" y="${cy + 12}" fill="var(--red)" font-size="9" letter-spacing="1.5">RISK-OFF</text>`;
    s += `<text x="${cx + R}" y="${cy + 12}" fill="var(--green)" font-size="9" text-anchor="end" letter-spacing="1.5">RISK-ON</text></svg>`;
    el.innerHTML = s;
  }

  /* ── cross-asset matrix ── */
  const MX_GROUPS = [
    ['equity', 'Equities'], ['rates', 'Rates & spreads'], ['fx', 'Currencies'], ['commodity', 'Commodities & freight'],
    ['credit', 'Credit'], ['vol', 'Volatility'], ['crypto', 'Crypto'], ['macro', 'Macro & liquidity'],
  ];
  const groupOf = (r) => (r.asset_class === 'shipping' ? 'commodity' : r.asset_class === 'derived' ? 'macro' : r.asset_class);
  const MX = { filter: 'all', mode: 'sigma', sort: null, dir: -1, open: null };
  const HORIZON = { chg_1d: 1, chg_1w: 5, chg_1m: 21, chg_3m: 63, chg_ytd: null, chg_1y: 252 };
  function ytdDays() { const n = new Date(); return Math.max(1, Math.round((n - Date.UTC(n.getUTCFullYear(), 0, 1)) / 864e5 * 252 / 365)); }
  function heat(r, key) {
    const v = r[key];
    if (!isNum(v) || v === 0) return 'transparent';
    let t;
    const sd = isNum(r.z_1d) && r.z_1d !== 0 && isNum(r.chg_1d) ? Math.abs(r.chg_1d / r.z_1d) : null;
    const h = HORIZON[key] == null ? ytdDays() : HORIZON[key];
    const f = r.frequency === 'W' ? 5 : 1;
    if (MX.mode === 'sigma' && sd) t = Math.abs(v) / (2.2 * sd * Math.sqrt(Math.max(1, h / f)));
    else {
      const ref = { chg_1d: 1.5, chg_1w: 3, chg_1m: 6, chg_3m: 10, chg_ytd: 15, chg_1y: 20 }[key];
      t = Math.abs(v) / (r.chg_mode === 'bp' ? ref * 8 : ref);
    }
    t = clamp(t, 0, 1);
    const a = (0.06 + 0.5 * Math.pow(t, 0.85)).toFixed(3);
    return v > 0 ? `rgba(67,224,151,${a})` : `rgba(240,96,96,${a})`;
  }
  function renderMatrix() {
    const tb = $('#mxTable');
    let rows = S.boardList.filter((r) => (r.tags || []).includes('matrix'));
    if (MX.filter !== 'all') rows = rows.filter((r) => groupOf(r) === MX.filter);
    const cols = [['nm', 'Instrument'], ['last', 'Last'], ['chg_1d', '1D'], ['chg_1w', '1W'], ['chg_1m', '1M'], ['chg_3m', '3M'],
      ['chg_ytd', 'YTD'], ['chg_1y', '1Y'], ['z_1d', 'σ today'], ['rng', '52-week'], ['sp', '120 days']];
    let h = `<thead><tr>${cols.map((c) => `<th data-k="${c[0]}">${esc(c[1])}${MX.sort === c[0] ? (MX.dir < 0 ? ' ▾' : ' ▴') : ''}</th>`).join('')}</tr></thead><tbody>`;
    for (const [g, glab] of MX_GROUPS) {
      let grp = rows.filter((r) => groupOf(r) === g);
      if (!grp.length) continue;
      if (MX.sort && MX.sort in HORIZON || MX.sort === 'z_1d') {
        grp = grp.slice().sort((a, b) => (isNum(b[MX.sort]) ? b[MX.sort] : -1e9) * -MX.dir - (isNum(a[MX.sort]) ? a[MX.sort] : -1e9) * -MX.dir);
      }
      h += `<tr class="grp"><td colspan="${cols.length}">${esc(glab)}</td></tr>`;
      for (const r of grp) {
        const cell = (k) => `<td class="hc" style="--hc:${heat(r, k)}"><span class="${cls(r[k])}">${esc(chg(r, r[k]))}</span></td>`;
        const rng = isNum(r.pct_52w) ? `<span class="rng" title="52w low ${esc(lvl(r, r.lo_52w))} · high ${esc(lvl(r, r.hi_52w))}"><i style="left:calc(${(r.pct_52w * 100).toFixed(1)}% - 1.5px)"></i></span>` : '<span class="faint">—</span>';
        h += `<tr class="row" data-sid="${esc(r.series_id)}" tabindex="0">
          <td class="nm">${esc(r.short)}<small>${esc(r.unit && r.unit !== 'index' ? r.unit : '')}</small>${r.stale ? '<span class="stale-tag">STALE</span>' : ''}${r.meta && r.meta.roll ? `<span class="roll-tag" title="Front-month contract rolled today (next ÷ old = ${esc(fmt(r.meta.roll.ratio, 4))}); the 1D move is the new contract's own change, not the gap">↻ ROLL</span>` : ''}</td>
          <td>${esc(lvl(r))}</td>${cell('chg_1d')}${cell('chg_1w')}${cell('chg_1m')}${cell('chg_3m')}${cell('chg_ytd')}${cell('chg_1y')}
          <td class="zc ${isNum(r.z_1d) && Math.abs(r.z_1d) >= 2.5 ? (r.z_1d > 0 ? 'up' : 'dn') : 'muted'}">${isNum(r.z_1d) ? sgn(r.z_1d, 1, 'σ') : '—'}</td>
          <td>${rng}</td><td>${spark(r.spark, { w: 110, h: 22 })}</td></tr>`;
        if (MX.open === r.series_id) h += `<tr class="xp"><td colspan="${cols.length}" style="padding:10px 12px 14px;height:auto"><div class="chart" id="mxOpen" style="height:190px"></div></td></tr>`;
      }
    }
    tb.innerHTML = h + '</tbody>';
    tb.querySelectorAll('th').forEach((th) => th.addEventListener('click', () => {
      const k = th.dataset.k;
      if (!(k in HORIZON) && k !== 'z_1d') return;
      if (MX.sort === k) { if (MX.dir < 0) MX.dir = 1; else { MX.sort = null; MX.dir = -1; } } else { MX.sort = k; MX.dir = -1; }
      renderMatrix();
    }));
    tb.querySelectorAll('tr.row').forEach((tr) => {
      const go = () => { MX.open = MX.open === tr.dataset.sid ? null : tr.dataset.sid; renderMatrix(); };
      tr.addEventListener('click', go);
      tr.addEventListener('keydown', (e) => { if (e.key === 'Enter') go(); });
    });
    if (MX.open) openMatrixRow(MX.open);
  }
  async function openMatrixRow(sid) {
    const el = $('#mxOpen'); if (!el) return;
    const r = B(sid);
    try {
      const h = await history([sid], 800);
      const pts = h[sid] || [];
      const bands = [];
      lineChart(el, {
        aria: r.name, series: [{ key: sid, label: r.short, color: 'var(--s1)', data: pts, area: r.chg_mode === 'pct' }],
        yFmt: (v) => lvl(r, v), hLines: isNum(r.ma200) ? [{ y: r.ma200, color: 'rgba(245,166,35,0.45)', label: '200-day avg' }] : [], bands,
      });
    } catch (e) { el.innerHTML = '<div class="empty">History unavailable</div>'; }
  }
  function buildMatrixControls() {
    const f = $('#mxFilter');
    f.innerHTML = [['all', 'All']].concat(MX_GROUPS.map(([g, l]) => [g, l.split(' ')[0]])).map(([k, l]) =>
      `<button type="button" class="chip" data-k="${k}" aria-pressed="${MX.filter === k}">${esc(l)}</button>`).join('');
    f.querySelectorAll('.chip').forEach((b) => b.addEventListener('click', () => {
      MX.filter = b.dataset.k; f.querySelectorAll('.chip').forEach((x) => x.setAttribute('aria-pressed', x === b)); renderMatrix();
    }));
    const m = $('#mxMode');
    m.innerHTML = [['sigma', 'σ-adjusted colour'], ['raw', 'Raw colour']].map(([k, l]) =>
      `<button type="button" class="chip" data-k="${k}" aria-pressed="${MX.mode === k}">${esc(l)}</button>`).join('');
    m.querySelectorAll('.chip').forEach((b) => b.addEventListener('click', () => {
      MX.mode = b.dataset.k; m.querySelectorAll('.chip').forEach((x) => x.setAttribute('aria-pressed', x === b)); renderMatrix();
    }));
  }

  /* ── rates ── */
  const UST = [['yc.us.1m', 1 / 12, '1M'], ['yc.us.3m', 0.25, '3M'], ['yc.us.6m', 0.5, '6M'], ['yc.us.1y', 1, '1Y'],
    ['yc.us.2y', 2, '2Y'], ['yc.us.3y', 3, '3Y'], ['yc.us.5y', 5, '5Y'], ['yc.us.7y', 7, '7Y'], ['yc.us.10y', 10, '10Y'],
    ['yc.us.20y', 20, '20Y'], ['yc.us.30y', 30, '30Y']];
  const G10 = [['yc.us.10y', 'US'], ['yc.de.10y', 'Germany'], ['yc.gb.10y', 'UK'], ['yc.jp.10y', 'Japan'], ['yc.no.10y', 'Norway'],
    ['yc.se.10y', 'Sweden'], ['yc.ca.10y', 'Canada'], ['yc.au.10y', 'Australia'], ['yc.ea.10y', 'Euro AAA']];
  const G10_HL = { 'yc.us.10y': 'var(--s1)', 'yc.no.10y': 'var(--s2)' };
  async function renderRates() {
    const ids = UST.map((u) => u[0]).concat(G10.map((g) => g[0]));
    let h;
    try { h = await history(ids, 400); } catch (e) { $('#ustCurve').innerHTML = '<div class="empty">History unavailable</div>'; return; }
    // curve snapshots: today, 1 month, 1 year ago (ordinal ramp — one hue, three lightness steps)
    const at = (pts, when) => { let v = null; for (const p of pts) { if (p[0] <= when) v = p[1]; else break; } return v; };
    const last = d3.max(UST, (u) => { const p = h[u[0]]; return p && p.length ? p[p.length - 1][0] : null; });
    if (last) {
      const snaps = [['Today', last, '#6fd0ff', 2.4], ['1 month ago', addDays(last, -30), '#2b8fbd', 1.8], ['1 year ago', addDays(last, -365), '#29546b', 1.8]];
      const series = snaps.map(([lab, when, col, w]) => ({ key: lab, label: lab, color: col, width: w,
        data: UST.map((u) => [u[1], at(h[u[0]] || [], when)]).filter((p) => isNum(p[1])) }));
      $('#ustLegend').innerHTML = series.map((s) => `<span><i style="background:${s.color}"></i>${esc(s.label)}</span>`).join('');
      $('#ustAsof').textContent = 'as of ' + dLong(last);
      lineChart($('#ustCurve'), { aria: 'US Treasury yield curve', xType: 'num', series, endLabels: false,
        yFmt: (v) => fmt(v, 2) + '%', xTicks: UST.filter((u) => ['3M', '1Y', '2Y', '5Y', '10Y', '20Y', '30Y'].includes(u[2])).map((u) => [u[1], u[2]]),
        xLabel: (x) => (UST.find((u) => Math.abs(u[1] - x) < 1e-6) || [0, 0, fmt(x, 1) + 'y'])[2] + ' maturity' });
    }
    // 10Y comparison — emphasis: US + Norway in colour, the rest context grey
    const g10 = (hl) => G10.map(([id, lab]) => ({ key: id, label: lab, data: (h[id] || []).filter((p) => p[0] >= addDays(new Date(), -370)),
      color: hl && hl === id ? 'var(--s3)' : G10_HL[id] || 'var(--s-muted)', muted: !(G10_HL[id] || hl === id), width: hl === id ? 2.4 : undefined }));
    const drawG10 = (hl) => lineChart($('#g10Chart'), { aria: '10-year government yields', series: g10(hl), yFmt: (v) => fmt(v, 1) + '%',
      tipFmt: (se, v) => fmt(v, 2) + '%' });
    drawG10(null);
    $('#g10Legend').innerHTML = G10.map(([id, lab]) => {
      const r = B(id);
      return `<span data-id="${id}" style="cursor:pointer"><i style="background:${G10_HL[id] || 'var(--s-muted)'}"></i>${esc(lab)} <span class="faint num">${r ? fmt(r.last, 2) + '%' : ''}</span></span>`;
    }).join('');
    $$('#g10Legend span[data-id]').forEach((s) => {
      s.addEventListener('pointerenter', () => drawG10(s.dataset.id));
      s.addEventListener('pointerleave', () => drawG10(null));
    });
    // spread tiles
    const ids2 = ['der.us_2s10s', 'der.us_3m10y', 'der.us_5s30s', 'der.us_be10', 'yc.usr.10y', 'mm.sofr', 'der.de_2s10s',
      'der.ea_frag', 'der.us_de_10y', 'der.us_jp_10y', 'der.no_de_10y', 'cr.us_hy_oas', 'cr.us_ig_oas', 'cr.eu_hy_oas',
      'fc.nfci', 'fc.stlfsi', 'der.net_liq', 'liq.rrp'];
    $('#spreadTiles').innerHTML = ids2.map(B).filter(Boolean).map((r) => tile(r, { chgKey: 'chg_1m', chgLab: '1M' })).join('');
  }
  function tile(r, o = {}) {
    const k = o.chgKey || 'chg_1d';
    return `<div class="tile" title="${esc(r.name)}"><div class="tl">${esc(r.short)}${r.stale ? ' <span class="stale-tag">STALE</span>' : ''}</div>
      <div class="tv">${esc(lvl(r))}</div>
      <div class="ts"><span class="${cls(r[k])}">${esc(chg(r, r[k]))} ${esc(o.chgLab || '1D')}</span>${o.extra ? `<span class="muted">${o.extra(r)}</span>` : ''}</div>
      ${spark(r.spark, { w: 160, h: 26, fluid: true })}</div>`;
  }

  /* ── central banks ── */
  const PB = { sort: 'region', dir: 1 };
  function policyRows() {
    const rows = [];
    for (const r of S.boardList) {
      if (r.asset_class !== 'policy') continue;
      const m = r.meta || {};
      const cc = r.country;
      const doc = S.docs.get(cc);
      rows.push({ r, cc, bank: (r.series_meta && r.series_meta.bank) || r.short, region: r.region || '', rate: r.last,
        step_bp: m.step_bp, step_date: m.step_date, hikes: m.hikes_12m || 0, cuts: m.cuts_12m || 0, net: m.net_bp_12m || 0,
        real: doc && isNum(doc.real_rate) ? doc.real_rate : null, range: m.range, stale: r.stale });
    }
    return rows;
  }
  function renderPolicy() {
    const rows = policyRows();
    if (!rows.length) { $('#policyBoard').innerHTML = '<tr><td class="empty">No data</td></tr>'; return; }
    // 90-day tilt
    let hk = 0, ct = 0;
    const cut90 = addDays(new Date(), -90);
    for (const x of rows) if (x.step_date && pDate(x.step_date) >= cut90) (x.step_bp > 0 ? hk++ : ct++);
    $('#policyTilt').textContent = `last 90 days: ${ct} cut(s) · ${hk} hike(s) among ${rows.length} banks`;
    // map
    const pts = rows.filter((x) => isNum(x.real));
    mount($('#policyMap'), (el) => drawPolicyMap(el, pts));
    // board
    const sorters = { region: (a, b) => a.region.localeCompare(b.region) || b.rate - a.rate, bank: (a, b) => a.bank.localeCompare(b.bank),
      rate: (a, b) => a.rate - b.rate, move: (a, b) => (pDate(a.step_date) || 0) - (pDate(b.step_date) || 0),
      net: (a, b) => a.net - b.net, real: (a, b) => (isNum(a.real) ? a.real : -99) - (isNum(b.real) ? b.real : -99) };
    const sorted = rows.slice().sort((a, b) => sorters[PB.sort](a, b) * PB.dir);
    const th = (k, l, r) => `<th data-k="${k}" style="cursor:pointer${r ? '' : ';text-align:left'}">${esc(l)}${PB.sort === k ? (PB.dir < 0 ? ' ▾' : ' ▴') : ''}</th>`;
    $('#policyBoard').innerHTML = `<thead><tr>${th('bank', 'Central bank')}${th('rate', 'Rate', 1)}${th('move', 'Last move', 1)}${th('net', '12 months', 1)}${th('real', 'Real', 1)}<th>1 year</th></tr></thead><tbody>` +
      sorted.map((x) => `<tr class="${x.cc === 'NO' ? 'hl' : ''}"><td class="bk">${esc(x.bank)}<small>${esc(x.cc)}</small>${x.stale ? '<span class="stale-tag">STALE</span>' : ''}</td>
        <td>${x.range ? fmt(x.range[0], 2) + '–' + fmt(x.range[1], 2) : fmt(x.rate, 2)}%</td>
        <td>${x.step_date ? `<span class="${cls(x.step_bp)}">${sgn(x.step_bp, 0, 'bp')}</span> <span class="faint">${esc(dShort(pDate(x.step_date)))} ’${esc(x.step_date.slice(2, 4))}</span>` : '<span class="faint">—</span>'}</td>
        <td><span class="${cls(x.net)}">${sgn(x.net, 0, 'bp')}</span> <span class="faint">${x.hikes}↑ ${x.cuts}↓</span></td>
        <td class="${cls(x.real)}">${isNum(x.real) ? sgn(x.real, 1) : '—'}</td>
        <td>${spark((x.r.spark || []).slice(-120), { w: 90, h: 18, fill: false, color: 'var(--s1)' })}</td></tr>`).join('') + '</tbody>';
    $$('#policyBoard th[data-k]').forEach((t) => t.addEventListener('click', () => {
      const k = t.dataset.k; PB.dir = PB.sort === k ? -PB.dir : (k === 'bank' || k === 'region' ? 1 : -1); PB.sort = k; renderPolicy();
    }));
  }
  function drawPolicyMap(el, pts) {
    const W = el.clientWidth, H = el.clientHeight || 380;
    if (!pts.length || W < 100) { el.innerHTML = '<div class="empty">No data</div>'; return; }
    const m = { t: 16, r: 22, b: 34, l: 42 };
    // robust domains: a couple of extreme banks (Argentina's −1 000 bp year) would otherwise squash
    // everyone else into the middle — they are pinned to the edge with a ‹ › marker instead
    const qt = (arr, p) => d3.quantile(arr.slice().sort(d3.ascending), p);
    const nets = pts.map((p) => p.net), reals = pts.map((p) => p.real);
    const xm = Math.max(100, Math.abs(qt(nets, 0.06)), Math.abs(qt(nets, 0.94))) * 1.3;
    const ym = Math.max(2, Math.abs(qt(reals, 0.05)), Math.abs(qt(reals, 0.95))) * 1.3;
    const x = d3.scaleLinear().domain([-xm, xm]).range([m.l, W - m.r]).nice();
    const y = d3.scaleLinear().domain([-ym, ym]).range([H - m.b, m.t]).nice();
    const [x0d, x1d] = x.domain(), [y0d, y1d] = y.domain();
    let s = `<svg viewBox="0 0 ${W} ${H}" height="${H}" role="img" aria-label="Central bank policy map">`;
    for (const t of y.ticks(5)) s += `<line x1="${m.l}" x2="${W - m.r}" y1="${y(t)}" y2="${y(t)}" stroke="var(--grid)"/><text x="${m.l - 6}" y="${y(t) + 3}" text-anchor="end" fill="var(--text3)" font-size="9.5">${esc(sgn(t, 0))}</text>`;
    for (const t of x.ticks(6)) s += `<text x="${x(t)}" y="${H - m.b + 14}" text-anchor="middle" fill="var(--text3)" font-size="9.5">${esc(sgn(t, 0))}</text>`;
    s += `<line x1="${x(0)}" x2="${x(0)}" y1="${m.t}" y2="${H - m.b}" stroke="var(--axis)"/><line x1="${m.l}" x2="${W - m.r}" y1="${y(0)}" y2="${y(0)}" stroke="var(--axis)"/>`;
    const q = (tx, ty, lab, anchor) => `<text x="${tx}" y="${ty}" text-anchor="${anchor}" fill="var(--text4)" font-size="9" letter-spacing="1">${esc(lab)}</text>`;
    s += q(W - m.r - 4, m.t + 10, 'TIGHTENING · RESTRICTIVE', 'end') + q(m.l + 6, m.t + 10, 'EASING · STILL RESTRICTIVE', 'start') +
      q(m.l + 6, H - m.b - 8, 'EASING · ACCOMMODATIVE', 'start') + q(W - m.r - 4, H - m.b - 8, 'TIGHTENING · STILL LOOSE', 'end');
    s += `<text x="${(m.l + W - m.r) / 2}" y="${H - 4}" text-anchor="middle" fill="var(--text3)" font-size="9.5">net change over 12 months (bp)</text>`;
    s += `<text transform="translate(11,${(m.t + H - m.b) / 2}) rotate(-90)" text-anchor="middle" fill="var(--text3)" font-size="9.5">real policy rate (pp)</text>`;
    const major = new Set(['US', 'XM', 'GB', 'JP', 'CN']);
    pts.forEach((p, i) => {
      const pinX = p.net < x0d ? -1 : p.net > x1d ? 1 : 0, pinY = p.real < y0d ? -1 : p.real > y1d ? 1 : 0;
      const px = x(clamp(p.net, x0d, x1d)), py = y(clamp(p.real, y0d, y1d));
      const hl = p.cc === 'NO';
      const col = hl ? 'var(--s2)' : major.has(p.cc) ? 'var(--s1)' : '#5d7c93';
      const pin = (pinX < 0 ? '‹' : pinX > 0 ? '›' : '') + (pinY > 0 ? '˄' : pinY < 0 ? '˅' : '');
      s += `<g class="pm" data-i="${i}"><circle cx="${px}" cy="${py}" r="12" fill="transparent"/>
        <circle cx="${px}" cy="${py}" r="${hl || major.has(p.cc) ? 5 : 4}" fill="${col}" stroke="#0b0f17" stroke-width="2"/>
        ${(hl || major.has(p.cc) || pin || Math.abs(p.net) > xm * 0.35 || Math.abs(p.real) > ym * 0.4)
          ? `<text x="${px + (pinX > 0 ? -8 : 7)}" y="${py - 6}" text-anchor="${pinX > 0 ? 'end' : 'start'}" fill="${hl ? 'var(--amber)' : 'var(--text3)'}" font-size="9">${esc((p.cc === 'XM' ? 'EA' : p.cc) + (pin ? ' ' + pin : ''))}</text>` : ''}</g>`;
    });
    s += `<text x="${W - m.r}" y="${H - m.b - 22}" text-anchor="end" fill="var(--text4)" font-size="8.5">‹ › ˄ ˅ = beyond the axis, pinned to the edge</text>`;
    el.innerHTML = s + '</svg>';
    el.querySelectorAll('g.pm').forEach((g) => {
      const p = pts[+g.dataset.i];
      g.addEventListener('pointermove', (ev) => {
        const r = el.getBoundingClientRect();
        tipAt(el, `<div class="td">${esc(p.bank)}</div>${trow('var(--s1)', fmt(p.rate, 2) + '%', 'policy rate')}${trow('var(--s1)', sgn(p.net, 0, 'bp'), '12-month change')}${trow('var(--s1)', sgn(p.real, 2, 'pp'), 'real rate')}${p.step_date ? trow('var(--s1)', sgn(p.step_bp, 0, 'bp'), 'last move ' + dLong(pDate(p.step_date))) : ''}`,
          ev.clientX - r.left, ev.clientY - r.top);
      });
      g.addEventListener('pointerleave', () => tipHide(el));
    });
  }

  /* ── currencies ── */
  let FXWIN = 'chg_1m';
  function renderFx() {
    const box = $('#fxWin');
    if (!box.children.length) {
      box.innerHTML = [['chg_1d', '1D'], ['chg_1w', '1W'], ['chg_1m', '1M'], ['chg_ytd', 'YTD'], ['chg_1y', '1Y']].map(([k, l]) =>
        `<button type="button" class="chip" data-k="${k}" aria-pressed="${k === FXWIN}">${l}</button>`).join('');
      box.querySelectorAll('.chip').forEach((b) => b.addEventListener('click', () => {
        FXWIN = b.dataset.k; box.querySelectorAll('.chip').forEach((x) => x.setAttribute('aria-pressed', x === b)); renderFx();
      }));
    }
    const seen = new Set(), items = [];
    for (const r of S.boardList) {
      if (r.asset_class !== 'fx' || !r.meta || !r.meta.strength || r.series_id === 'fx.eurnok') continue;
      const ccy = r.meta.ccy;
      if (seen.has(ccy)) continue;
      seen.add(ccy);
      items.push({ label: ccy, value: r.meta.strength[FXWIN], sub: `${r.short} ${lvl(r)}`, hl: ccy === 'NOK' });
    }
    items.sort((a, b) => (b.value ?? -99) - (a.value ?? -99));
    hbars($('#fxBars'), { items, diverging: true, fmt: (v) => sgn(v, 2, '%'), labelW: 44, rowH: 21 });
    const t = ['fx.dxy', 'der.em_fx', 'fx.i44', 'fx.eurusd', 'fx.usdjpy', 'fx.usdcny'].map(B).filter(Boolean);
    $('#fxTiles').innerHTML = t.map((r) => tile(r, { chgKey: 'chg_1m', chgLab: '1M', w: 130 })).join('');
    history(['fx.dxy', 'der.em_fx'], 380).then((h) => {
      const idx = (pts) => { const p0 = pts.find((p) => isNum(p[1])); return p0 ? pts.map((p) => [p[0], p[1] / p0[1] * 100]) : []; };
      const s = [{ key: 'dxy', label: 'DXY', color: 'var(--s1)', data: idx(h['fx.dxy'] || []) },
        { key: 'em', label: 'EM FX', color: 'var(--s2)', data: idx(h['der.em_fx'] || []) }];
      $('#dxyLegend').innerHTML = s.map((x) => `<span><i style="background:${x.color}"></i>${x.label === 'EM FX' ? 'EM FX basket (BRL MXN ZAR INR KRW CNY)' : x.label}</span>`).join('') + '<span class="faint">indexed = 100 a year ago</span>';
      lineChart($('#dxyChart'), { aria: 'Dollar vs EM currencies', series: s, yFmt: (v) => fmt(v, 0), tipFmt: (se, v) => fmt(v, 1), endLabels: true });
    }).catch(() => {});
  }

  /* ── commodities & vol ── */
  function renderCommodities() {
    const cmd = S.boardList.filter((r) => (r.asset_class === 'commodity' && r.source === 'yahoo') || r.series_id === 'sh.bdry');
    $('#cmdTiles').innerHTML = cmd.map((r) => tile(r, { chgKey: 'chg_1d', extra: (x) => `${esc(chg(x, x.chg_1m))} 1M` })).join('');
    const vol = S.boardList.filter((r) => r.asset_class === 'vol');
    $('#volTiles').innerHTML = vol.map((r) => tile(r, { chgKey: 'chg_1d',
      extra: (x) => (isNum(x.pctile_5y) ? `${fmt(x.pctile_5y * 100, 0)}th pct 5y` : '') })).join('');
  }

  /* ── geopolitics ── */
  function renderGeo() {
    const box = $('#hotList');
    const hs = S.hot.slice(0, 14);
    if (!hs.length) { box.innerHTML = '<div class="empty">No GDELT data yet</div>'; }
    else {
      $('#geoAsof').textContent = 'GDELT day ' + dLong(pDate(hs[0].latest_date));
      box.innerHTML = hs.map((h) => {
        const dy = goodDyad(h.top_dyads), src = (h.top_sources || [])[0];
        const sub = [dy ? title(dy.a1) + (dy.a2 ? ' → ' + title(dy.a2) : '') + ' · ' + dy.label.toLowerCase() : '',
          src ? `<a href="${esc(safeUrl(src.url))}" target="_blank" rel="noopener nofollow">${esc(src.domain)}</a>` : ''].filter(Boolean);
        const spikeCls = isNum(h.spike) && h.spike >= 2 ? 'dn' : isNum(h.spike) && h.spike >= 1.4 ? '' : 'muted';
        return `<div class="hs-row" data-iso="${esc(h.iso2 || '')}" role="button" tabindex="0">
          <span class="hs-rk">${h.rank}</span>
          <span style="min-width:0"><div class="hs-nm">${esc(h.name)}</div><div class="hs-sub">${sub.map((x, i) => (i === 0 ? esc(x) : x)).join(' · ')}</div></span>
          <span><div class="tbar"><i style="width:${clamp(h.tension, 0, 100)}%;background:${LAYERS.tension.scale(h.tension / 100)}"></i></div>
            <div class="spk" style="text-align:left;margin-top:3px">${fmt(h.tension, 0)}<span class="faint">/100</span></div></span>
          <span class="spk ${spikeCls}">${isNum(h.spike) ? fmt(h.spike, 1) + '×' : '—'}</span>
          <span class="spkcol">${spark(h.spark, { w: 118, h: 24, color: 'var(--dn)', dirColor: false })}</span></div>`;
      }).join('');
      box.querySelectorAll('.hs-row').forEach((row) => {
        const go = () => { const d = S.docs.get(row.dataset.iso); if (d) openDrawer(d); };
        row.addEventListener('click', (e) => { if (e.target.closest('a')) return; go(); });
        row.addEventListener('keydown', (e) => { if (e.key === 'Enter') go(); });
      });
    }
    // global pulse (small multiples)
    const P = [['conflict_articles', 'Material conflict'], ['violence_articles', 'Fighting & assault'], ['protest_articles', 'Protests'],
      ['coerce_articles', 'Coercion'], ['threat_articles', 'Threats'], ['posture_articles', 'Military posture'],
      ['sanction_articles', 'Sanctions & embargoes'], ['coop_articles', 'Cooperation']];
    const g = S.geoGlobal.slice().sort((a, b) => (a.obs_date < b.obs_date ? -1 : 1));
    const pb = $('#pulse');
    if (!g.length) { pb.innerHTML = '<div class="empty">No data yet</div>'; return; }
    pb.innerHTML = P.map(([k, lab]) => `<div><div class="tl muted" style="font-size:9px;letter-spacing:.14em;text-transform:uppercase;display:flex;justify-content:space-between"><span>${esc(lab)}</span><span id="pv_${k}" class="num"></span></div><div class="chart" id="pc_${k}" style="height:58px"></div></div>`).join('');
    for (const [k, lab] of P) {
      const pts = g.map((r) => [pDate(r.obs_date), r[k]]);
      const ma = pts.map((p, i) => [p[0], i >= 6 ? d3.mean(pts.slice(i - 6, i + 1), (q) => q[1]) : null]);
      const lastMa = ma[ma.length - 1][1], base = d3.mean(pts.slice(-97, -7), (q) => q[1]);
      const rel = isNum(lastMa) && base ? (lastMa / base - 1) * 100 : null;
      $('#pv_' + k).innerHTML = `${fmt(lastMa, 0)}/d <span class="${rel > 15 ? 'dn' : rel < -15 ? 'up' : 'muted'}">${sgn(rel, 0, '%')}</span>`;
      lineChart($('#pc_' + k), { aria: lab, margin: { l: 4, r: 4, t: 4, b: 4 }, yFmt: () => '', series: [
        { key: 'd', label: 'daily', color: 'var(--s-muted)', data: pts, muted: true, width: 1 },
        { key: 'm', label: '7-day mean', color: k === 'coop_articles' ? 'var(--s1)' : 'var(--s3)', data: ma }],
        tipFmt: (se, v) => fmt(v, 0) + ' articles' });
      $('#pc_' + k).querySelectorAll('svg text').forEach((t) => t.remove());
    }
  }
  function renderHazards() {
    const box = $('#hazList');
    const hz = S.hazards;
    if (!hz.length) { box.innerHTML = '<div class="empty">No orange or red GDACS alerts are current.</div>'; return; }
    const kind = { TC: 'Tropical cyclone', EQ: 'Earthquake', FL: 'Flood', VO: 'Volcano', DR: 'Drought', WF: 'Wildfire' };
    box.innerHTML = hz.map((z) => `<div class="hz-row"><span class="hz-lv ${esc(z.alert_level)}">${esc(z.alert_level)}</span>
      <span style="min-width:0"><span class="hz-n">${z.url ? `<a href="${esc(safeUrl(z.url))}" target="_blank" rel="noopener" style="text-decoration:none">${esc(z.name || kind[z.eventtype])}</a>` : esc(z.name || kind[z.eventtype])}</span>
      <span class="hz-s"> · ${esc(kind[z.eventtype] || z.eventtype)}${z.country ? ' · ' + esc(z.country) : ''}${z.from_date ? ' · since ' + esc(dShort(pDate(z.from_date))) : ''}</span>
      ${z.severity && !/^magnitude 0\b/i.test(z.severity) ? `<div class="hz-s">${esc(z.severity)}</div>` : ''}</span></div>`).join('');
  }

  /* ── shipping ── */
  function renderShipping() {
    const box = $('#chokeGrid');
    const cps = S.choke.filter((c) => (c.ref_vessels || 0) >= 3000);
    if (!cps.length) { box.innerHTML = '<div class="empty">No PortWatch data yet</div>'; return; }
    $('#shipAsof').textContent = 'week to ' + dLong(pDate(cps[0].latest_date));
    box.innerHTML = cps.map((c) => `<div class="cp">
      <div class="cp-h"><span class="cp-n">${esc(c.name)}</span><span class="cp-s ${esc(c.status)}">${esc(c.status)}</span></div>
      <div class="cp-v"><span><b class="num">${fmt(c.avg7, 0)}</b><span class="muted">/day</span></span>
        <span class="${cls(c.vs_ly)}">${pctTxt(c.vs_ly)} y/y</span><span class="muted">${pctTxt(c.vs_ref)} vs 2023</span></div>
      ${spark(c.spark, { w: 220, h: 34, ref: c.spark_ly, dirColor: false, fluid: true })}
      <div class="muted" style="font-size:9.5px">tankers ${pctTxt(c.tanker_vs_ly)} · containers ${pctTxt(c.container_vs_ly)} · vs 90d ${pctTxt(c.vs_base)}</div>
    </div>`).join('');
  }

  /* ── sanctions ── */
  async function renderSanctions() {
    try {
      const [weekly, progs, ctry, latest, pubs] = await Promise.all([
        api('world_sanctions_weekly?order=week.asc'),
        api('world_sanctions_programs?order=new_365d.desc&limit=10'),
        api('world_sanctions_countries?order=new_365d.desc&limit=14'),
        api('world_sanctions?select=entity_id,name,entity_type,programs,countries,listed_on&removed_on=is.null&order=listed_on.desc,entity_id.desc&limit=60'),
        api('world_sanctions_pubs?order=published_at.desc&limit=1'),
      ]);
      if (pubs && pubs[0]) $('#sancAsof').textContent = 'OFAC publication ' + dLong(pDate(pubs[0].published_at)) + ` · ${fmt(pubs[0].n_entities, 0)} entries`;
      const byWeek = new Map();
      for (const r of weekly || []) byWeek.set(r.week, (byWeek.get(r.week) || 0) + r.designations);
      const start = addDays(new Date(), -730);
      const items = [];
      for (let d = new Date(Date.UTC(start.getUTCFullYear(), start.getUTCMonth(), start.getUTCDate())); d <= new Date(); d = addDays(d, 7)) {
        const mon = addDays(d, -((d.getUTCDay() + 6) % 7));
        const k = iso(mon);
        items.push({ x: mon, value: byWeek.get(k) || 0, label: 'Week of ' + dLong(mon) });
      }
      const seen = new Set(); const uniq = items.filter((i) => (seen.has(+i.x) ? false : seen.add(+i.x)));
      columns($('#sancWeekly'), { items: uniq, color: 'var(--s3)', fmt: (v) => fmt(v, 0), unit: 'designations' });
      hbars($('#sancProgs'), { items: (progs || []).map((p) => ({ label: p.program, value: p.new_365d, sub: `${fmt(p.active, 0)} active` })),
        fmt: (v) => fmt(v, 0), labelW: 128, rowH: 21 });
      hbars($('#sancCountries'), { items: (ctry || []).map((c) => ({ label: (S.docs.get(c.iso2) || {}).name || c.iso2, value: c.new_365d,
        sub: `${fmt(c.active, 0)} active` })), fmt: (v) => fmt(v, 0), labelW: 110, rowH: 20 });
      $('#sancList').innerHTML = (latest || []).map((e) => `<div class="sl-row"><span class="d">${esc(dShort(pDate(e.listed_on)))} ’${esc(String(e.listed_on).slice(2, 4))}</span>
        <span class="n">${esc(e.name)}<small>${esc(e.entity_type || '')}</small></span>
        <span class="p">${esc((e.programs || []).join(', '))}${(e.countries || []).length ? ' · ' + esc(e.countries.join(', ')) : ''}</span></div>`).join('');
    } catch (e) {
      $('#sancList').innerHTML = '<div class="empty">Sanctions data unavailable</div>';
    }
  }

  /* ── Norway ── */
  let ZONE = 'NO1';
  async function renderNorway() {
    const ids = ['pol.no', 'mm.nowa', 'fx.usdnok', 'fx.eurnok', 'fx.i44', 'yc.no.10y', 'der.no_de_10y', 'eq.osebx', 'cmd.brent',
      'nor.salmon', 'res.no', 'pwr.no1', 'pwr.no2'];
    $('#noTiles').innerHTML = ids.map(B).filter(Boolean).map((r) => tile(r, {
      chgKey: r.asset_class === 'policy' ? 'chg_3m' : r.frequency === 'W' ? 'chg_1d' : 'chg_1d',
      chgLab: r.asset_class === 'policy' ? '3M' : r.frequency === 'W' ? 'w/w' : '1D' })).join('');
    try {
      const h = await history(['fx.usdnok', 'cmd.brent', 'der.corr_nok_oil', 'nor.salmon'], 1100);
      const yr = addDays(new Date(), -370);
      const nok = (h['fx.usdnok'] || []).filter((p) => p[0] >= yr), oil = (h['cmd.brent'] || []).filter((p) => p[0] >= yr);
      const n0 = nok.find((p) => isNum(p[1])), o0 = oil.find((p) => isNum(p[1]));
      const nokIdx = n0 ? nok.map((p) => [p[0], n0[1] / p[1] * 100]) : [];
      const oilIdx = o0 ? oil.map((p) => [p[0], p[1] / o0[1] * 100]) : [];
      // two small multiples, each on its own scale: on one shared axis Brent's ±80 % swings flatten
      // the krone's ±5 % into a line; the correlation strip underneath says how tightly they move
      lineChart($('#nokIdx'), { aria: 'Krone vs USD, indexed', margin: { b: 16 },
        series: [{ key: 'nok', label: 'NOK vs USD', color: 'var(--s1)', data: nokIdx, area: true }],
        yFmt: (v) => fmt(v, 0), tipFmt: (se, v) => fmt(v, 1) + ' (100 = a year ago)', hLines: [{ y: 100, color: 'var(--axis)' }] });
      lineChart($('#brentIdx'), { aria: 'Brent, indexed', margin: { b: 16 },
        series: [{ key: 'oil', label: 'Brent', color: 'var(--s2)', data: oilIdx, area: true }],
        yFmt: (v) => fmt(v, 0), tipFmt: (se, v) => fmt(v, 1) + ' (100 = a year ago)', hLines: [{ y: 100, color: 'var(--axis)' }] });
      const cr = (h['der.corr_nok_oil'] || []).filter((p) => p[0] >= yr);
      lineChart($('#nokCorr'), { aria: 'NOK–Brent 60-day correlation', series: [{ key: 'c', label: '60-day correlation NOK vs Brent', color: 'var(--s1)', data: cr, area: true }],
        yFmt: (v) => fmt(v, 1), yDomain: [-1, 1], hLines: [{ y: 0, color: 'var(--axis)' }], tipFmt: (se, v) => sgn(v, 2) });
      const sal = h['nor.salmon'] || [];
      lineChart($('#salmonChart'), { aria: 'Salmon export price', series: [{ key: 's', label: 'NOK/kg', color: 'var(--s1)', data: sal, area: true }],
        yFmt: (v) => fmt(v, 0), tipFmt: (se, v) => 'NOK ' + fmt(v, 2) + '/kg' });
    } catch (e) { /* charts show their own empty state */ }
    buildZoneChips();
    renderPower();
    renderReservoir();
  }
  function buildZoneChips() {
    const box = $('#pwrZones');
    if (box.children.length) return;
    const names = { NO1: 'Oslo', NO2: 'Kristiansand', NO3: 'Trondheim', NO4: 'Tromsø', NO5: 'Bergen' };
    box.innerHTML = Object.keys(names).map((z) => `<button type="button" class="chip" data-z="${z}" title="${names[z]}" aria-pressed="${z === ZONE}">${z}</button>`).join('');
    box.querySelectorAll('.chip').forEach((b) => b.addEventListener('click', () => {
      ZONE = b.dataset.z; box.querySelectorAll('.chip').forEach((x) => x.setAttribute('aria-pressed', x === b)); renderPower();
    }));
  }
  async function renderPower() {
    try {
      const today = new Date(); today.setHours(0, 0, 0, 0);
      const rows = await api(`world_power?zone=eq.${ZONE}&ts=gte.${encodeURIComponent(today.toISOString())}&order=ts.asc&select=ts,nok_kwh`);
      const td = new Date().toDateString();
      const items = (rows || []).map((r) => {
        const t = new Date(r.ts);
        const isToday = t.toDateString() === td;
        return { x: t.toLocaleTimeString('en-GB', { hour: '2-digit', timeZone: 'Europe/Oslo' }), value: r.nok_kwh,
          color: isToday ? 'var(--s1)' : 'var(--s2)',
          label: (isToday ? 'Today ' : 'Tomorrow ') + t.toLocaleTimeString('en-GB', { hour: '2-digit', minute: '2-digit', timeZone: 'Europe/Oslo' }) };
      });
      columns($('#pwrHourly'), { items, fmt: (v) => fmt(v, 2), unit: 'NOK/kWh' });
      const h = await history(['pwr.' + ZONE.toLowerCase()], 380);
      lineChart($('#pwrDaily'), { aria: 'Daily average power price', series: [{ key: 'p', label: ZONE + ' daily average', color: 'var(--s1)', data: h['pwr.' + ZONE.toLowerCase()] || [], area: true }],
        yFmt: (v) => fmt(v, 2), tipFmt: (se, v) => fmt(v, 3) + ' NOK/kWh' });
      const hl = $('#pwrHourly');
      if (!hl.querySelector('.pwr-note')) {
        const n = document.createElement('div'); n.className = 'note pwr-note';
        n.innerHTML = '<span style="color:var(--s1)">■</span> today · <span style="color:var(--s2)">■</span> tomorrow (day-ahead, published ≈13:00 CET) · NOK/kWh ex. VAT; line = one-year daily average';
        hl.after(n);
      }
    } catch (e) { $('#pwrHourly').innerHTML = '<div class="empty">Power data unavailable</div>'; }
  }
  async function renderReservoir() {
    try {
      const [rows, norm] = await Promise.all([api('world_reservoir?area=eq.NO&order=obs_date.desc&limit=110'), api('world_reservoir_norm?area=eq.NO&order=iso_week.asc')]);
      if (!rows || !rows.length) return;
      const y = rows[0].iso_year;
      const cur = rows.filter((r) => r.iso_year === y).map((r) => [r.iso_week, r.fill_pct]).sort((a, b) => a[0] - b[0]);
      const prev = rows.filter((r) => r.iso_year === y - 1).map((r) => [r.iso_week, r.fill_pct]).sort((a, b) => a[0] - b[0]);
      const med = (norm || []).map((n) => [n.iso_week, n.median_pct]);
      const lo = (norm || []).map((n) => [n.iso_week, n.min_pct]), hi = (norm || []).map((n) => [n.iso_week, n.max_pct]);
      const latest = rows[0];
      const nm = (norm || []).find((n) => n.iso_week === latest.iso_week);
      $('#resAsof').textContent = `week ${latest.iso_week} · ${fmt(latest.fill_pct, 1)}% full` + (nm ? ` · ${sgn(latest.fill_pct - nm.median_pct, 1, ' pp')} vs median` : '');
      $('#resLegend').innerHTML = `<span><i style="background:var(--s1)"></i>${y}</span><span><i style="background:var(--s-muted)"></i>${y - 1}</span><span><i style="background:var(--text3)"></i>median (NVE norm)</span><span><i class="band" style="background:rgba(164,173,191,0.14)"></i>min–max</span>`;
      const el = $('#resChart');
      mount(el, (e) => {
        drawLineChart(e, { aria: 'Reservoir fill', xType: 'num', xScale: 'linear', xDomain: [1, 53],
          xTicks: [[1, 'wk 1'], [13, '13'], [26, '26'], [39, '39'], [52, '52']], xLabel: (w) => 'Week ' + w,
          series: [{ key: 'med', label: 'median', color: 'var(--text3)', data: med, width: 1.2, muted: true },
            { key: 'prev', label: String(y - 1), color: 'var(--s-muted)', data: prev, muted: true, width: 1.4 },
            { key: 'cur', label: String(y), color: 'var(--s1)', data: cur }],
          yFmt: (v) => fmt(v, 0) + '%', tipFmt: (se, v) => fmt(v, 1) + '%', yDomain: [0, 100] });
        // min–max band behind the lines
        const svg = e.querySelector('svg'); if (!svg || !lo.length) return;
        const W = e.clientWidth, H = e.clientHeight || 240, m = { t: 10, r: 12, b: 22, l: 46 };
        const xs = d3.scaleLinear().domain([1, 53]).range([m.l, W - m.r]);
        const ys = d3.scaleLinear().domain([0, 100]).nice(4).range([H - m.b, m.t]);
        const area = d3.area().x((p) => xs(p[0])).y0((p, i) => ys(lo[i][1])).y1((p) => ys(p[1]))(hi);
        const p = document.createElementNS('http://www.w3.org/2000/svg', 'path');
        p.setAttribute('d', area); p.setAttribute('fill', 'rgba(164,173,191,0.12)');
        svg.insertBefore(p, svg.firstChild);
      });
    } catch (e) { $('#resChart').innerHTML = '<div class="empty">Reservoir data unavailable</div>'; }
  }

  /* ── health ── */
  // shared rule (assets/intel.js): late after ~2 missed cycles, stale past stale_after_min, failing
  function healthState(x, now) { return INTEL.sourceHealth(x, now).state; }
  const ORDER = ['yahoo', 'deribit', 'treasury', 'treasury_real', 'bundesbank', 'boe', 'mof', 'norgesbank', 'riksbank', 'boc', 'ecb', 'rba',
    'fred', 'bis', 'power', 'nve', 'ssb', 'gdacs', 'portwatch', 'gdelt', 'ofac', 'imf', 'countries', 'analytics'];
  function renderHealth() {
    const now = Date.now();
    const rows = S.health.slice().sort((a, b) => ORDER.indexOf(a.source) - ORDER.indexOf(b.source));
    const cad = (m) => (m >= 1440 ? Math.round(m / 1440) + ' d' : m >= 60 ? Math.round(m / 60) + ' h' : m + ' min');
    $('#healthAsof').textContent = S.lastRun ? `last run ${ago(S.lastRun.finished_at || S.lastRun.started_at)}` : '';
    $('#healthTable').innerHTML = `<thead><tr><th>Source</th><th>Status</th><th>Last success</th><th>Newest data</th><th>Cadence</th><th class="r">Rows</th><th class="r">Time</th><th>Notes</th></tr></thead><tbody>` +
      rows.map((x) => {
        const st = healthState(x, now);
        return `<tr><td class="src">${esc(x.label || x.source)}<small>${esc(x.provider || '')}</small></td>
          <td><span class="led ${st}"></span>${esc(x.last_status || '—')}${x.consecutive_failures ? ` <span class="faint">×${x.consecutive_failures}</span>` : ''}${st !== 'ok' ? `<div class="err">${esc(INTEL.sourceHealth(x, now).why)}</div>` : ''}</td>
          <td>${esc(ago(x.last_success))}</td><td>${esc(x.latest_data ? dLong(pDate(x.latest_data)) : '—')}</td>
          <td>${esc(cad(x.cadence_min || 0))}</td><td class="r">${fmt(x.last_rows, 0)}</td><td class="r">${isNum(x.last_duration_ms) ? fmt(x.last_duration_ms / 1000, 1) + 's' : '—'}</td>
          <td>${x.last_error && st !== 'ok' ? `<div class="err">${esc(x.last_error)}</div>` : ''}<span class="faint">${esc(x.notes || '')}</span></td></tr>`;
      }).join('') + '</tbody>';
  }

  /* ── globe markers from state ── */
  function beaconData() {
    const out = [];
    const now = new Date();
    for (const r of S.boardList) {
      const m = r.series_meta || {};
      if (!(r.tags || []).includes('beacon') || !isNum(m.lat)) continue;
      let open = false, local = '';
      try {
        const parts = new Intl.DateTimeFormat('en-GB', { timeZone: m.tz, weekday: 'short', hour: '2-digit', minute: '2-digit', hour12: false }).formatToParts(now);
        const g = (t) => (parts.find((p) => p.type === t) || {}).value;
        const wd = g('weekday'), hm = +g('hour') * 60 + +g('minute');
        const [a, b] = (m.hours || '09:00-17:00').split('-').map((s) => +s.slice(0, 2) * 60 + +s.slice(3, 5));
        open = !['Sat', 'Sun'].includes(wd) && hm >= a && hm < b;
        local = `${g('hour')}:${g('minute')} local`;
      } catch (e) { /* unknown tz */ }
      out.push({ city: m.city, lat: m.lat, lon: m.lon, name: r.name, chg: r.chg_1d, z: r.z_1d, chgTxt: chg(r, r.chg_1d),
        last: lvl(r), m1: chg(r, r.chg_1m), ytd: chg(r, r.chg_ytd), open, local });
    }
    return out;
  }
  function refreshGlobeMarkers() {
    if (!GLOBE) return;
    const hots = S.hot.filter((h) => (isNum(h.spike) && h.spike >= 1.6) || h.rank <= 6).map((h) => {
      const d = S.docs.get(h.iso2);
      const f = d && FEATS.find((x) => docForFeature(x) === d);
      return { c: f ? f.c : null, spike: h.spike };
    }).filter((h) => h.c);
    GLOBE.setMarkers({ chokes: S.choke, hazards: S.hazards, beacons: beaconData(), hots });
  }

  /* ═══ LOAD & REFRESH ═════════════════════════════════════════════════════ */
  async function loadCore() {
    const since = iso(addDays(new Date(), -7));
    const res = await Promise.allSettled([
      api('world_board?select=series_id,name,short,asset_class,region,country,unit,chg_mode,frequency,decimals,sort_key,tags,source,series_meta,last_date,last,chg_1d,chg_1w,chg_1m,chg_3m,chg_ytd,chg_1y,z_1d,vol_ann,hi_52w,lo_52w,pct_52w,pctile_5y,ma50,ma200,spark,spark_start,age_days,stale,meta&order=sort_key.asc'),
      api(`world_brief?brief_date=gte.${since}&order=brief_date.desc,rank.asc`),
      api('world_regime?select=obs_date,score,label&order=obs_date.desc&limit=520'),
      api('world_regime?order=obs_date.desc&limit=1'),
      api('world_country_snapshot?select=iso2,data'),
      api('world_hotspots?order=rank.asc'),
      api('world_chokepoint_snapshot?order=ref_vessels.desc.nullslast'),
      api('world_hazards?is_current=is.true&order=alert_score.desc.nullslast&limit=40'),
      api('world_source_health'),
      api('world_runs?select=started_at,finished_at,status&order=run_id.desc&limit=1'),
    ]);
    const ok = (i) => (res[i].status === 'fulfilled' ? res[i].value : null);
    if (ok(0)) { S.boardList = ok(0); S.board = new Map(S.boardList.map((r) => [r.series_id, r])); }
    if (ok(1)) S.brief = ok(1);
    if (ok(2)) S.regime = ok(2);
    if (ok(3)) S.regimeNow = ok(3)[0] || null;
    if (ok(4)) {
      S.docs = new Map(ok(4).map((r) => [r.iso2, r.data]));
      S.docsByNum = new Map();
      for (const d of S.docs.values()) if (d.num) S.docsByNum.set(String(+d.num), d);
      // world-atlas ids are zero-padded strings ("004"); index both spellings
      for (const d of S.docs.values()) if (d.num) S.docsByNum.set(d.num, d);
    }
    if (ok(5)) S.hot = ok(5);
    if (ok(6)) S.choke = ok(6);
    if (ok(7)) S.hazards = ok(7);
    if (ok(8)) S.health = ok(8);
    if (ok(9)) S.lastRun = ok(9)[0] || null;
    const failed = res.filter((r) => r.status === 'rejected').length;
    if (failed === res.length) throw new Error('API unreachable');
  }
  async function loadGeoGlobal() {
    try { S.geoGlobal = await api('world_tension_global?order=obs_date.desc&limit=181'); } catch (e) { S.geoGlobal = []; }
  }

  function renderAll() {
    renderHeader(); renderTape(); renderBrief(); renderRegime(); renderMatrix();
    renderPolicy(); renderFx(); renderCommodities(); renderShipping(); renderHazards(); renderHealth();
    applyLayer(); refreshGlobeMarkers();
    if (CT.open) renderCountryTable();
  }

  // deferred sections: fetch/draw when they approach the viewport
  function lazy(id, fn) {
    const el = document.getElementById(id);
    if (!el) return;
    if (!('IntersectionObserver' in window)) { fn(); return; }
    const io = new IntersectionObserver((es) => { if (es.some((e) => e.isIntersecting)) { io.disconnect(); fn(); } }, { rootMargin: '600px 0px' });
    io.observe(el);
  }

  function scrollSpy() {
    const links = $$('#rail a');
    const map = new Map(links.map((a) => [a.getAttribute('href').slice(1), a]));
    if (!('IntersectionObserver' in window)) return;
    const vis = new Map();
    const io = new IntersectionObserver((es) => {
      for (const e of es) vis.set(e.target.id, e.isIntersecting ? e.intersectionRatio : 0);
      let best = null, br = 0;
      for (const [id, r] of vis) if (r > br) { br = r; best = id; }
      links.forEach((a) => a.classList.toggle('on', a.getAttribute('href') === '#' + best));
    }, { threshold: [0, 0.15, 0.4, 0.7], rootMargin: '-40px 0px -40% 0px' });
    for (const id of map.keys()) { const s = document.getElementById(id); if (s) io.observe(s); }
  }

  async function init() {
    if (!window.d3) {           // d3 is loaded with defer before this file; if the CDN failed, say so
      $('#globeStatus').textContent = 'Chart libraries could not load';
      return;
    }
    buildMatrixControls();
    initGlobe();
    scrollSpy();
    $('#briefDay').addEventListener('change', (e) => renderBrief(e.target.value));
    $('#drawerX').addEventListener('click', closeDrawer);
    $('#scrim').addEventListener('click', closeDrawer);
    document.addEventListener('keydown', (e) => { if (e.key === 'Escape') closeDrawer(); });
    $('#ctableBtn').addEventListener('click', () => {
      CT.open = !CT.open;
      $('#ctable').classList.toggle('open', CT.open);
      $('#ctableBtn').setAttribute('aria-expanded', CT.open);
      if (CT.open) renderCountryTable();
    });
    try {
      await loadCore();
    } catch (e) {
      $('#pillHealthT').textContent = 'API unreachable';
      $('#pillHealth').className = 'meta-pill bad';
      $('#briefGrid').innerHTML = '<div class="empty" style="grid-column:1/-1">Could not reach api.slensvik.com — retrying shortly.</div>';
      setTimeout(init2, 30000);
      return;
    }
    renderAll();
    lazy('rates', renderRates);
    lazy('geopolitics', async () => { await loadGeoGlobal(); renderGeo(); });
    lazy('sanctions', renderSanctions);
    lazy('norway', renderNorway);
    // live refresh: the pipeline runs every 15 min — re-read every 5 while visible, and at once when a
    // backgrounded tab comes back (it used to wait for the next tick, showing hours-old data meanwhile)
    let lastLoad = Date.now();
    const refresh = async () => {
      try { await loadCore(); lastLoad = Date.now(); renderAll(); if (S.geoGlobal.length) renderGeo(); } catch (e) { /* keep last frame */ }
    };
    setInterval(() => { if (!document.hidden) refresh(); }, 5 * 60 * 1000);
    document.addEventListener('visibilitychange', () => { if (!document.hidden && Date.now() - lastLoad > 60 * 1000) refresh(); });
    window.addEventListener('pageshow', (e) => { if (e.persisted) refresh(); });
    // keep "… ago" and the health states honest between data refreshes
    setInterval(() => { if (!document.hidden) { renderHeader(); renderHealth(); } }, 60 * 1000);
  }
  async function init2() { try { await loadCore(); renderAll(); } catch (e) { setTimeout(init2, 60000); } }

  if (document.readyState === 'loading') document.addEventListener('DOMContentLoaded', init);
  else init();
})();
