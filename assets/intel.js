/* ═══════════════════════════════════════════════════════════════════════════
   INTEL.JS — slensvik.com shared utilities
   ─────────────────────────────────────────────────────────────────────────────
   Load with <script src="assets/intel.js"></script> AFTER Chart.js (if the
   page uses charts) and BEFORE page scripts. No modules, no build step.

   Exposes a single global: window.INTEL
     INTEL.API                  'https://api.slensvik.com'
     INTEL.safeGet(path)        fetch `${API}/${path}` → parsed JSON, or null
                                on any network/HTTP error (never throws)
     INTEL.fmtNum(v, dp=2)      nb-NO grouped number, '—' for null/NaN
     INTEL.fmtPct(v, dp=2)      signed percent '+1.23%' / '−0.45%', '—' null
     INTEL.fmtNOK(v, dp=0)      'NOK 12 345', '—' for null
     INTEL.fmtCompact(v)        1 234 → '1.2k', 3 400 000 → '3.4M'
     INTEL.selskapHref(isin)    'selskap.html?isin=NO0010096985'
     INTEL.tkLink(isin, label)  '<a class="tk-link" …>label</a>' — ticker
                                cross-link to the company dossier page;
                                stops propagation so row onclick still works
     INTEL.chartDefaults()      applies shared Chart.js defaults (Plex Mono,
                                slate ticks, bg2 tooltips) — call once before
                                creating charts; no-op if Chart undefined
     INTEL.liveNumber(el, v)    living-number setter: first call just writes
                                the text; later calls odometer-roll the digits
                                that changed and flash a green/red afterglow
                                by numeric direction. Reduced-motion → plain
                                textContent. Always safe to call in render
                                loops (no-op when the value is unchanged).
     INTEL.openPalette()        opens the ⌘K command palette (also bound to
                                Cmd/Ctrl+K and `/` site-wide; fuzzy search
                                over tickers + site pages)
     INTEL.mood                 market-mood scalar in [-1, 1] (breadth from
                                /market_pulse; 0 until the fetch lands) —
                                also drives the --mood-rgb/--mood-a CSS vars

   Motion system (auto-initialised on DOMContentLoaded, all gated on
   prefers-reduced-motion): ambient grid/scanline layer with scroll parallax,
   staggered .panel/.kpi-strip/.sec-head reveal, market-mood accent tinting.

   Nav injection: on DOMContentLoaded a <nav class="site-nav"> linking every
   page is rendered into the element marked [data-intel-nav] (place one inside
   the page topbar). If no mount exists, nav injection is skipped — pages
   opt in explicitly. Current page gets .active.
   ═══════════════════════════════════════════════════════════════════════ */
(function () {
  'use strict';

  const API = 'https://api.slensvik.com';

  /* ── density mode (COMFORT ⇄ COMPACT) ──────────────────────────────────
     Applied immediately (intel.js loads synchronously before first paint)
     so a compact reader never sees a comfort-spaced flash. The CSS lives in
     intel.css under html[data-density="compact"]. */
  const DENSITY_KEY = 'intel_density_v1';
  function applyDensity(mode) {
    if (mode === 'compact') document.documentElement.setAttribute('data-density', 'compact');
    else document.documentElement.removeAttribute('data-density');
  }
  let density = 'comfort';
  try { if (localStorage.getItem(DENSITY_KEY) === 'compact') density = 'compact'; } catch (e) {}
  applyDensity(density);
  function toggleDensity() {
    density = density === 'compact' ? 'comfort' : 'compact';
    try { localStorage.setItem(DENSITY_KEY, density); } catch (e) {}
    applyDensity(density);
    document.querySelectorAll('.sn-density').forEach(b => {
      b.setAttribute('aria-pressed', density === 'compact' ? 'true' : 'false');
      b.textContent = density === 'compact' ? 'COMPACT' : 'COMFORT';
    });
  }

  /* fetch-with-fallback: resolves to parsed JSON or null, never throws */
  async function safeGet(path) {
    try {
      const r = await fetch(`${API}/${path}`, { headers: { Accept: 'application/json' } });
      if (!r.ok) return null;
      return await r.json();
    } catch (e) { return null; }
  }

  /* ── formatters (nb-NO conventions, em-dash for missing) ── */
  const fmtNum = (v, dp = 2) =>
    (v == null || isNaN(v)) ? '—'
      : (+v).toLocaleString('nb-NO', { minimumFractionDigits: dp, maximumFractionDigits: dp });

  const fmtPct = (v, dp = 2) =>
    (v == null || isNaN(v)) ? '—'
      : (v >= 0 ? '+' : '−') + Math.abs(+v).toFixed(dp) + '%';

  const fmtNOK = (v, dp = 0) =>
    (v == null || isNaN(v)) ? '—' : 'NOK ' + fmtNum(v, dp);

  const fmtCompact = (v) => {
    if (v == null || isNaN(v)) return '—';
    const a = Math.abs(v);
    if (a >= 1e9) return (v / 1e9).toFixed(1) + 'B';
    if (a >= 1e6) return (v / 1e6).toFixed(1) + 'M';
    if (a >= 1e3) return (v / 1e3).toFixed(1) + 'k';
    return String(Math.round(v * 100) / 100);
  };

  /* ── company dossier cross-links ── */
  const selskapHref = (isin) => 'selskap.html?isin=' + encodeURIComponent(isin || '');

  // Inline anchor for ticker cells living inside clickable rows: stops
  // propagation so the row's own onclick (drill-down etc.) is not triggered.
  const tkLink = (isin, label) =>
    isin
      ? `<a class="tk-link" href="${selskapHref(isin)}" title="Åpne selskapsside" onclick="event.stopPropagation()">${label}</a>`
      : (label || '—');

  /* ── shared Chart.js defaults ── */
  function chartDefaults() {
    if (typeof Chart === 'undefined') return;
    Chart.defaults.color = '#5a6377';
    Chart.defaults.borderColor = 'rgba(255,255,255,0.06)';
    Chart.defaults.font.family = "'IBM Plex Mono', monospace";
    Chart.defaults.font.size = 10;
    Chart.defaults.plugins.tooltip.backgroundColor = '#11161f';
    Chart.defaults.plugins.tooltip.borderColor = 'rgba(255,255,255,0.12)';
    Chart.defaults.plugins.tooltip.borderWidth = 1;
    Chart.defaults.plugins.tooltip.titleColor = '#e8eef7';
    Chart.defaults.plugins.tooltip.bodyColor = '#a4adbf';
    Chart.defaults.plugins.tooltip.padding = 10;
    Chart.defaults.plugins.tooltip.cornerRadius = 4;
    Chart.defaults.plugins.legend.labels.boxWidth = 12;
  }

  /* ═══ MOTION & DEPTH SYSTEM ═══════════════════════════════════════════ */

  const REDUCE = !!(window.matchMedia &&
    window.matchMedia('(prefers-reduced-motion: reduce)').matches);

  const escHtml = (s) => String(s)
    .replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;');

  /* ── living numbers: odometer roll + direction afterglow ──────────────── */
  function lnVal(s) {
    const m = String(s).replace(/−/g, '-').replace(/[\s  ]/g, '')
      .match(/-?\d+(?:[.,]\d+)?/);
    return m ? parseFloat(m[0].replace(',', '.')) : NaN;
  }
  function liveNumber(el, next) {
    if (!el) return;
    next = String(next);
    const prev = el.dataset.ln;
    if (prev === next) return;
    el.dataset.ln = next;
    if (prev == null || REDUCE) { el.textContent = next; return; }
    const a = lnVal(prev), b = lnVal(next);
    const dir = (isNaN(a) || isNaN(b)) ? 0 : Math.sign(b - a);
    // roll only the characters that changed (aligned from the right, where
    // numbers move); unchanged prefix stays perfectly still
    const off = prev.length - next.length;
    let html = '', rolled = 0;
    for (let i = 0; i < next.length; i++) {
      const ch = next[i];
      if (prev[i + off] !== ch && ch.trim()) {
        html += `<span class="ln-digit roll" style="animation-delay:${Math.min(rolled++, 8) * 24}ms">${escHtml(ch)}</span>`;
      } else {
        html += escHtml(ch);
      }
    }
    el.innerHTML = html;
    el.classList.remove('ln-up', 'ln-dn');
    if (dir) {
      void el.offsetWidth;   // restart afterglow animation
      el.classList.add(dir > 0 ? 'ln-up' : 'ln-dn');
    }
  }

  /* ── ambient background layer + scroll parallax ────────────────────────── */
  function initAmbient() {
    if (document.querySelector('.intel-ambient')) return;
    const div = document.createElement('div');
    div.className = 'intel-ambient';
    div.setAttribute('aria-hidden', 'true');
    document.body.prepend(div);
    if (REDUCE) return;
    let raf = 0;
    window.addEventListener('scroll', () => {
      if (raf) return;
      raf = requestAnimationFrame(() => {
        raf = 0;
        div.style.setProperty('--ambient-y', (window.scrollY * -0.06).toFixed(1) + 'px');
      });
    }, { passive: true });
  }

  /* ── staggered entrance reveal (panels, KPI strips, section heads) ───────
     FAIL-SAFE BY DESIGN: content is visible by default — the hidden
     pre-animation state (.intel-reveal without .in) is only applied here,
     after the IntersectionObserver is armed. A force-reveal timer then
     guarantees nothing can stay hidden even if IO callbacks never fire
     (full-page captures, instant scroll jumps, bfcache restores, etc.):
     the stagger plays for content near the fold, everything else is
     quietly revealed shortly after load. */
  function initReveal() {
    if (REDUCE || !('IntersectionObserver' in window)) return;
    const els = Array.from(document.querySelectorAll('.panel, .kpi-strip, .sec-head'));
    if (!els.length) return;
    let io = null;
    const revealAll = () => {
      els.forEach(el => el.classList.add('in'));
      if (io) { io.disconnect(); io = null; }
    };
    let batch = 0, batchReset = 0;
    try {
      io = new IntersectionObserver((entries) => {
        for (const e of entries) {
          if (!e.isIntersecting) continue;
          e.target.style.setProperty('--reveal-delay', (Math.min(batch++, 7) * 70) + 'ms');
          e.target.classList.add('in');
          if (io) io.unobserve(e.target);
        }
        cancelAnimationFrame(batchReset);
        batchReset = requestAnimationFrame(() => { batch = 0; });
      }, { rootMargin: '0px 0px -6% 0px' });
      els.forEach(el => { el.classList.add('intel-reveal'); io.observe(el); });
    } catch (e) { revealAll(); return; }
    // safety net: no element may ever be stuck hidden
    setTimeout(revealAll, 1500);
    window.addEventListener('pageshow', (e) => { if (e.persisted) revealAll(); });
  }

  /* ── market mood: breadth from /market_pulse → ambient accent tint ─────── */
  let mood = 0;
  async function initMood() {
    try {
      let row = null;
      const cached = sessionStorage.getItem('intel_mood_v1');
      if (cached) {
        const c = JSON.parse(cached);
        if (Date.now() - c.t < 10 * 60 * 1000) row = c.row;
      }
      if (!row) {
        const rows = await safeGet('market_pulse');
        row = rows && rows[0];
        if (row) sessionStorage.setItem('intel_mood_v1',
          JSON.stringify({ t: Date.now(), row: { pct_advancers: row.pct_advancers } }));
      }
      if (!row || row.pct_advancers == null) return;
      mood = Math.max(-1, Math.min(1, (row.pct_advancers - 50) / 25));
      INTEL.mood = mood;
      const rs = document.documentElement.style;
      if (Math.abs(mood) < 0.06) return;                       // neutral → keep ice
      rs.setProperty('--mood-rgb', mood > 0 ? '67,224,151' : '240,96,96');
      rs.setProperty('--mood-a', (0.03 + 0.02 * Math.abs(mood)).toFixed(3));
    } catch (e) { /* neutral fallback */ }
  }

  /* ═══ ⌘K COMMAND PALETTE ═══════════════════════════════════════════════ */

  const K_PAGES = [
    { kind: 'page', glyph: '▣', tick: 'RADAR',       name: 'Markedsradar — dark-flow conviction radar', href: 'markedsradar.html' },
    { kind: 'page', glyph: '▤', tick: 'PORTFOLIO',   name: 'Portfolio tracker — holdings & signals',    href: 'portfolio.html' },
    { kind: 'page', glyph: '◆', tick: 'GALTON',      name: 'Galton strategy — weekly long/short book',  href: 'galton.html' },
    { kind: 'page', glyph: '◈', tick: 'SELSKAP',     name: 'Company intelligence dossier',              href: 'selskap.html' },
    { kind: 'page', glyph: '⧉', tick: 'MARKETMAKER', name: 'Deribit options market maker',              href: 'marketmaker.html' },
    { kind: 'page', glyph: '▥', tick: 'FASTRENTE',   name: 'Fixed-rate mortgage strategy',              href: 'fastrente.html' },
    { kind: 'page', glyph: '◇', tick: 'TERMINAL',    name: 'Front page — instrument gateway',           href: 'index.html' },
  ];

  let kEl = null, kInput = null, kList = null, kItems = [], kSel = 0,
      kTickers = null, kPrevFocus = null;

  async function kLoadTickers() {
    if (kTickers) return kTickers;
    try {
      const c = sessionStorage.getItem('intel_k_tickers_v1');
      if (c) { kTickers = JSON.parse(c); return kTickers; }
    } catch (e) {}
    const rows = await safeGet('instruments?select=isin,ticker,name&order=ticker.asc&limit=1000');
    kTickers = (rows || []).filter(r => r.ticker).map(r => ({
      kind: 'ticker', glyph: '◎', tick: r.ticker, name: r.name || '', isin: r.isin,
      href: selskapHref(r.isin),
    }));
    try { sessionStorage.setItem('intel_k_tickers_v1', JSON.stringify(kTickers)); } catch (e) {}
    return kTickers;
  }

  /* fuzzy scorer: prefix ≫ word-start ≫ in-order subsequence; 0 = no match */
  function kScore(q, str) {
    str = str.toUpperCase();
    if (!q) return 1;
    const idx = str.indexOf(q);
    if (idx === 0) return 1000 - str.length;
    if (idx > 0) return (str[idx - 1] === ' ' ? 700 : 400) - idx - str.length * 0.1;
    let si = 0, score = 200, run = 0;
    for (const ch of q) {
      const found = str.indexOf(ch, si);
      if (found < 0) return 0;
      score -= (found - si) * 2;          // gap penalty
      run = found === si ? run + 1 : 0;
      score += run * 8;                   // contiguity bonus
      si = found + 1;
    }
    return Math.max(score, 1);
  }

  function kHighlight(text, q) {
    if (!q) return escHtml(text);
    const up = text.toUpperCase(), idx = up.indexOf(q);
    if (idx < 0) return escHtml(text);
    return escHtml(text.slice(0, idx)) + '<b>' + escHtml(text.slice(idx, idx + q.length)) + '</b>' +
           escHtml(text.slice(idx + q.length));
  }

  function kRender() {
    const q = kInput.value.trim().toUpperCase();
    const pool = [...K_PAGES, ...(kTickers || [])];
    let rows;
    if (!q) {
      rows = [...K_PAGES.slice(0, 6), ...(kTickers || []).slice(0, 6)];
    } else {
      rows = pool
        .map(it => ({ it, s: Math.max(kScore(q, it.tick) * 1.2, kScore(q, it.name), kScore(q, it.isin || '')) }))
        .filter(r => r.s > 0)
        .sort((a, b) => b.s - a.s)
        .slice(0, 12)
        .map(r => r.it);
    }
    kItems = rows;
    kSel = 0;
    kList.innerHTML = rows.length ? rows.map((it, i) => `
      <div class="intel-k-item${i === 0 ? ' sel' : ''}" data-i="${i}" role="option" aria-selected="${i === 0}">
        <span class="ki-glyph">${it.glyph}</span>
        <span class="ki-main">
          <span class="ki-tick">${kHighlight(it.tick, q)}</span>
          <span class="ki-name">${kHighlight(it.name, q)}</span>
        </span>
        <span class="ki-kind">${it.kind}</span>
      </div>`).join('')
      : '<div class="intel-k-empty">No match — try a ticker, name or page</div>';
  }

  function kMove(d) {
    if (!kItems.length) return;
    kSel = (kSel + d + kItems.length) % kItems.length;
    kList.querySelectorAll('.intel-k-item').forEach((n, i) => {
      n.classList.toggle('sel', i === kSel);
      n.setAttribute('aria-selected', i === kSel);
      if (i === kSel) n.scrollIntoView({ block: 'nearest' });
    });
  }

  function kGo(i) {
    const it = kItems[i == null ? kSel : i];
    if (it) { closePalette(); location.href = it.href; }
  }

  function buildPalette() {
    kEl = document.createElement('div');
    kEl.className = 'intel-k';
    kEl.setAttribute('role', 'dialog');
    kEl.setAttribute('aria-label', 'Command palette');
    const isMac = /Mac/i.test(navigator.platform || '');
    kEl.innerHTML = `
      <div class="intel-k-box">
        <div class="intel-k-inputrow">
          <span class="glyph">⌕</span>
          <input class="intel-k-input" type="text" spellcheck="false" autocomplete="off"
                 placeholder="Ticker, company or page …" aria-label="Search">
          <span class="ki-kind" style="font-family:var(--mono);font-size:9px;color:var(--text4);letter-spacing:.12em">${isMac ? '⌘K' : 'CTRL K'}</span>
        </div>
        <div class="intel-k-list" role="listbox"></div>
        <div class="intel-k-hints">
          <span><kbd>↑↓</kbd>navigate</span><span><kbd>↵</kbd>open</span><span><kbd>esc</kbd>close</span>
        </div>
      </div>`;
    kInput = kEl.querySelector('.intel-k-input');
    kList  = kEl.querySelector('.intel-k-list');
    kInput.addEventListener('input', kRender);
    kInput.addEventListener('keydown', (e) => {
      if (e.key === 'ArrowDown' || (e.key === 'Tab' && !e.shiftKey)) { e.preventDefault(); kMove(1); }
      else if (e.key === 'ArrowUp' || (e.key === 'Tab' && e.shiftKey)) { e.preventDefault(); kMove(-1); }
      else if (e.key === 'Enter')  { e.preventDefault(); kGo(); }
      else if (e.key === 'Escape') { e.preventDefault(); closePalette(); }
      e.stopPropagation();
    });
    kList.addEventListener('click', (e) => {
      const n = e.target.closest('.intel-k-item');
      if (n) kGo(+n.dataset.i);
    });
    kList.addEventListener('mousemove', (e) => {
      const n = e.target.closest('.intel-k-item');
      if (n && +n.dataset.i !== kSel) { kSel = +n.dataset.i; kMove(0); }
    });
    kEl.addEventListener('mousedown', (e) => { if (e.target === kEl) closePalette(); });
    document.body.appendChild(kEl);
  }

  function openPalette() {
    if (kEl) { kEl.style.display = 'flex'; }
    else buildPalette();
    kPrevFocus = document.activeElement;
    kInput.value = '';
    kRender();
    kLoadTickers().then(() => { if (kEl.style.display !== 'none') kRender(); });
    kInput.focus();
  }
  function closePalette() {
    if (!kEl) return;
    kEl.style.display = 'none';
    if (kPrevFocus && kPrevFocus.focus) { try { kPrevFocus.focus(); } catch (e) {} }
  }
  const paletteOpen = () => kEl && kEl.style.display !== 'none';

  document.addEventListener('keydown', (e) => {
    if ((e.metaKey || e.ctrlKey) && e.key.toLowerCase() === 'k') {
      e.preventDefault();
      paletteOpen() ? closePalette() : openPalette();
      return;
    }
    if (!paletteOpen()) {
      if (e.key === '/' && !e.metaKey && !e.ctrlKey && !e.altKey) {
        const t = e.target;
        const typing = t && (t.tagName === 'INPUT' || t.tagName === 'TEXTAREA' ||
                             t.tagName === 'SELECT' || t.isContentEditable);
        if (!typing) { e.preventDefault(); openPalette(); }
      }
      return;
    }
    if (e.key === 'Escape') { e.preventDefault(); closePalette(); }
    else if (e.key === 'Tab') { e.preventDefault(); kInput.focus(); }   // keyboard trap
  });

  /* ── site nav ── */
  const NAV_PAGES = [
    { href: 'index.html',        label: 'Terminal' },
    { href: 'markedsradar.html', label: 'Radar' },
    { href: 'portfolio.html',    label: 'Portfolio' },
    { href: 'galton.html',       label: 'Galton' },
    { href: 'marketmaker.html',  label: 'MarketMaker' },
    { href: 'selskap.html',      label: 'Selskap' },
    { href: 'fastrente.html',    label: 'Fastrente' },
  ];

  function injectNav() {
    const mount = document.querySelector('[data-intel-nav]');
    if (!mount) return;
    const here = (location.pathname.split('/').pop() || 'index.html');
    const nav = document.createElement('nav');
    nav.className = 'site-nav';
    nav.setAttribute('aria-label', 'Site');
    nav.innerHTML = NAV_PAGES.map(p =>
      `<a class="sn-link${p.href === here ? ' active' : ''}" href="${p.href}">${p.label}</a>`
    ).join('');
    // ⌘K affordance chip
    const k = document.createElement('button');
    k.type = 'button';
    k.className = 'sn-klaunch';
    k.title = 'Command palette — search tickers & pages';
    k.textContent = /Mac/i.test(navigator.platform || '') ? '⌘K' : 'CTRL K';
    k.addEventListener('click', openPalette);
    nav.appendChild(k);
    // density toggle chip (COMFORT ⇄ COMPACT)
    const d = document.createElement('button');
    d.type = 'button';
    d.className = 'sn-density';
    d.title = 'Toggle reading density (compact ⇄ comfort)';
    d.setAttribute('aria-pressed', density === 'compact' ? 'true' : 'false');
    d.textContent = density === 'compact' ? 'COMPACT' : 'COMFORT';
    d.addEventListener('click', toggleDensity);
    nav.appendChild(d);
    mount.replaceChildren(nav);
  }

  function initMotion() {
    injectNav();
    initAmbient();
    initReveal();
    initMood();
  }
  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', initMotion);
  } else {
    initMotion();
  }

  /* ═══ FUNDAMENTALS: analyst consensus + balance-sheet health ═══════════
     Shared so portfolio, markedsradar and selskap all read the yfinance
     fundamentals (api.analyst_consensus / api.financials_key) the same way
     and render identical chips. Scoring mirrors the selskap dossier. */

  const REC_LABEL = { strong_buy: 'STR BUY', buy: 'BUY', hold: 'HOLD',
                      sell: 'SELL', strong_sell: 'STR SELL' };
  function recColor(key) {
    if (key === 'strong_buy' || key === 'buy') return '#43e097';
    if (key === 'hold') return '#f5a623';
    if (key === 'sell' || key === 'strong_sell') return '#f06060';
    return '#5a6377';
  }
  // From an api.analyst_consensus row + current price. Upside assumes price
  // and targets share a currency (Oslo names quote in NOK, as do the targets).
  function analystRead(c, price) {
    if (!c) return null;
    const tm = c.target_mean == null ? null : +c.target_mean;
    const p  = price == null ? null : +price;
    return {
      recKey: c.rec_key || null,
      recMean: c.rec_mean == null ? null : +c.rec_mean,
      label: REC_LABEL[c.rec_key] || (c.rec_key ? String(c.rec_key).toUpperCase() : '—'),
      color: recColor(c.rec_key),
      n: c.num_analysts == null ? null : +c.num_analysts,
      targetMean: tm,
      targetHigh: c.target_high == null ? null : +c.target_high,
      targetLow:  c.target_low  == null ? null : +c.target_low,
      ccy: c.quote_ccy || '',
      upside: (tm != null && p && p > 0) ? (tm / p - 1) * 100 : null,
    };
  }

  const _clamp = (v, lo, hi) => Math.max(lo, Math.min(hi, v));
  function healthBand(s) {
    if (s == null)  return { c: '#5a6377', t: 'n/a' };
    if (s >= 80)    return { c: '#43e097', t: 'robust' };
    if (s >= 65)    return { c: '#7ed99f', t: 'solid' };
    if (s >= 50)    return { c: '#f5a623', t: 'adequate' };
    if (s >= 35)    return { c: '#f0883e', t: 'stretched' };
    return { c: '#f06060', t: 'fragile' };
  }
  // Composite 0–100 from leverage/liquidity/solvency/interest-coverage on a
  // single latest-annual api.financials_key row. null pillars (banks/insurers
  // with no net-debt / working-capital concept) drop out of the weighting.
  function finHealth(row) {
    if (!row) return null;
    const n = (v) => v == null ? null : +v;
    const nd = n(row.net_debt), eb = n(row.ebitda);
    const ca = n(row.current_assets), cl = n(row.current_liabilities);
    const eq = n(row.equity), ta = n(row.total_assets);
    const ebit = n(row.ebit), ie = n(row.interest_expense);
    const sLev = (nd == null || eb == null || eb <= 0) ? null
      : nd <= 0 ? 100 : _clamp(95 - (nd / eb) * 20, 5, 95);
    const sLiq = (ca == null || cl == null || cl <= 0) ? null
      : _clamp(25 + (ca / cl - 0.5) * 50, 5, 100);
    const sSol = (eq == null || ta == null || ta <= 0) ? null
      : _clamp((eq / ta) * 160, 5, 100);
    let sCov;
    if (ebit == null) sCov = null;
    else { const i = ie == null ? 0 : Math.abs(ie);
      sCov = i < 1 ? (ebit > 0 ? 100 : 40) : (ebit / i <= 0 ? 5 : _clamp((ebit / i) * 9, 5, 100)); }
    const W = { lev: 0.30, sol: 0.25, cov: 0.25, liq: 0.20 };
    let acc = 0, wsum = 0;
    [[sLev, W.lev], [sSol, W.sol], [sCov, W.cov], [sLiq, W.liq]].forEach(([s, w]) => {
      if (s != null) { acc += s * w; wsum += w; }
    });
    const score = wsum ? Math.round(acc / wsum) : null;
    const div = n(row.dividends_paid), fcf = n(row.fcf);
    return {
      score, band: healthBand(score),
      pillars: { lev: sLev, liq: sLiq, sol: sSol, cov: sCov },
      ndE: (nd != null && eb && eb > 0) ? nd / eb : null,
      netCash: nd != null && nd <= 0,
      fcf, dividends: div,
      fcfCover: (fcf != null && div != null && Math.abs(div) > 0) ? fcf / Math.abs(div) : null,
      period: row.period_end, ccy: row.report_ccy || '',
    };
  }

  // Self-contained pill (inline styles → no per-page CSS needed).
  function _pill(color, text, title) {
    const t = title ? ` title="${escHtml(title)}"` : '';
    return `<span class="intel-pill"${t} style="display:inline-flex;align-items:center;gap:4px;`
      + `font:600 9.5px/1 'IBM Plex Mono',monospace;letter-spacing:.04em;padding:3px 6px;`
      + `border-radius:4px;border:1px solid ${color}55;color:${color};background:${color}14;`
      + `white-space:nowrap">${escHtml(text)}</span>`;
  }

  function analystChip(consensus, price) {
    const a = analystRead(consensus, price);
    if (!a || (!a.recKey && a.upside == null)) return '';
    const ups = a.upside == null ? '' : ` ${a.upside >= 0 ? '+' : '−'}${Math.abs(a.upside).toFixed(0)}%`;
    const title = [
      a.n != null ? `${a.n} analysts` : '',
      a.recMean != null ? `mean rec ${a.recMean.toFixed(2)}/5` : '',
      a.targetMean != null ? `target ${fmtNum(a.targetMean, 2)} ${a.ccy}` : '',
      (a.targetLow != null && a.targetHigh != null) ? `range ${fmtNum(a.targetLow, 0)}–${fmtNum(a.targetHigh, 0)}` : '',
    ].filter(Boolean).join(' · ');
    return _pill(a.color, '◈ ' + a.label + ups, title);
  }

  function healthChip(finRow) {
    const h = finHealth(finRow);
    if (!h || h.score == null) return '';
    const lev = h.netCash ? 'net cash' : (h.ndE != null ? fmtNum(h.ndE, 1) + '× ND/EBITDA' : '');
    const fcf = h.fcfCover != null ? `FCF÷div ${fmtNum(h.fcfCover, 1)}×` : '';
    const title = [`health ${h.score}/100 · ${h.band.t}`, lev, fcf,
                   `FY ${String(h.period).slice(0, 4)}`].filter(Boolean).join(' · ');
    return _pill(h.band.c, '❤ ' + h.score, title);
  }

  function leverageChip(finRow) {
    const h = finHealth(finRow);
    if (!h || (h.ndE == null && !h.netCash)) return '';
    const txt = h.netCash ? 'net cash' : fmtNum(h.ndE, 1) + '× lev';
    const col = healthBand(h.pillars.lev).c;
    return _pill(col, '⚖ ' + txt, h.netCash ? 'Net cash — no net debt'
      : `Net debt / EBITDA ${fmtNum(h.ndE, 2)}× · lower = safer · FY ${String(h.period).slice(0, 4)}`);
  }

  function fcfChip(finRow) {
    const h = finHealth(finRow);
    if (!h || h.fcf == null) return '';
    if (h.fcfCover != null) {
      const col = h.fcfCover >= 1 ? '#43e097' : h.fcfCover >= 0.5 ? '#f5a623' : '#f06060';
      return _pill(col, '⊳ FCF ' + fmtNum(h.fcfCover, 1) + '×',
        `Free cash flow covers ${fmtNum(h.fcfCover, 2)}× of dividends paid · >1× = self-funded · FY ${String(h.period).slice(0, 4)}`);
    }
    const col = h.fcf >= 0 ? '#7ed99f' : '#f06060';
    return _pill(col, '⊳ FCF ' + (h.fcf >= 0 ? '+' : '−') + fmtCompact(Math.abs(h.fcf)),
      `Free cash flow ${fmtCompact(h.fcf)} ${h.ccy} · no dividend paid · FY ${String(h.period).slice(0, 4)}`);
  }

  // injectNav is exposed so pages that render their shell via JS (fastrente)
  // can re-run nav injection after the [data-intel-nav] mount appears.
  window.INTEL = { API, safeGet, fmtNum, fmtPct, fmtNOK, fmtCompact, selskapHref,
                   tkLink, chartDefaults, injectNav, liveNumber, openPalette,
                   toggleDensity, mood,
                   analystRead, finHealth, healthBand,
                   analystChip, healthChip, leverageChip, fcfChip };
})();
