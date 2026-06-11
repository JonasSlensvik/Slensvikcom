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

   Nav injection: on DOMContentLoaded a <nav class="site-nav"> linking every
   page is rendered into the element marked [data-intel-nav] (place one inside
   the page topbar). If no mount exists, nav injection is skipped — pages
   opt in explicitly. Current page gets .active.
   ═══════════════════════════════════════════════════════════════════════ */
(function () {
  'use strict';

  const API = 'https://api.slensvik.com';

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
    mount.replaceChildren(nav);
  }

  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', injectNav);
  } else {
    injectNav();
  }

  // injectNav is exposed so pages that render their shell via JS (fastrente)
  // can re-run nav injection after the [data-intel-nav] mount appears.
  window.INTEL = { API, safeGet, fmtNum, fmtPct, fmtNOK, fmtCompact, selskapHref, tkLink, chartDefaults, injectNav };
})();
