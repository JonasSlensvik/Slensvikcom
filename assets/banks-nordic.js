/* Banks · Nordic — every Nordic bank's Pillar 3, as filed to the EBA (2026-11-11).
   The Nordic map (who lends where: each banking group's credit exposure by country from its CCyB1 template, cross-border
   lending as arcs, countries shaded by market size, concentration, foreign lenders or mortgage risk weights), market
   shares per country and for the Nordics, the Nordic league table and one bank's profile (capital over time, risk
   exposure by type, where it lends, stages 1–3 and coverage, IRB parameters with the PD distribution, deposits, rate
   risk, own funds) with the data-quality notes behind every figure.
   Data: ENEXT api.nordic_banks, api.nordic_exposure, api.nordic_metrics, api.nordic_credit_quality, api.nordic_irb,
   api.p3dh_entities, api.p3dh_submissions, api.p3dh_scale — the EBA Pillar 3 data hub, loaded and cleaned by
   research/banks/nordic.py and nordic_clean.py. Runbook: ../ENEXT/docs/banks-nordic.md.
   Needs d3 v7 and topojson-client (jsDelivr); without them the map is skipped and the rest still renders. */
(() => {
  const API = 'https://api.slensvik.com/';
  const TOPO = 'https://cdn.jsdelivr.net/npm/world-atlas@2.0.2/countries-50m.json';
  const $ = (s, r = document) => r.querySelector(s);
  const esc = (s) => String(s ?? '').replace(/[&<>"]/g, (c) => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;' }[c]));
  const get = (p) => window.INTEL ? INTEL.safeGet(p) : fetch(API + p).then((r) => r.ok ? r.json() : null).catch(() => null);
  const ok = (v) => v != null && v !== '' && isFinite(v);
  const nb = (v, dp = 0) => !ok(v) ? '—' : (+v).toLocaleString('nb-NO', { minimumFractionDigits: dp, maximumFractionDigits: dp });
  const pct = (v, dp = 1) => !ok(v) ? '—' : nb(v * 100, dp) + ' %';               // ratios arrive as decimals
  const eur = (v, dp) => !ok(v) ? '—' : Math.abs(v) >= 1e12 ? 'EUR ' + nb(v / 1e12, dp ?? 2) + ' tn'
    : 'EUR ' + nb(v / 1e9, dp ?? (Math.abs(v) < 1e10 ? 1 : 0)) + ' bn';
  const bnv = (v, dp) => !ok(v) ? '—' : nb(v / 1e9, dp ?? (Math.abs(v) < 1e10 ? 1 : 0));
  const sum = (a) => a.reduce((s, x) => s + (ok(x) ? +x : 0), 0);
  const dl = (d) => d ? new Date(d + 'T00:00:00Z').toLocaleString('en-GB', { month: 'short', year: 'numeric', timeZone: 'UTC' }) : '—';
  const dlong = (d) => d ? new Date(d + 'T00:00:00Z').toLocaleString('en-GB', { day: 'numeric', month: 'short', year: 'numeric', timeZone: 'UTC' }) : '—';

  const NORDIC = ['NO', 'SE', 'DK', 'FI', 'IS'];
  const CNAME = { NO: 'Norway', SE: 'Sweden', DK: 'Denmark', FI: 'Finland', IS: 'Iceland', NORDIC: 'The Nordics' };
  const NUM = { '578': 'NO', '752': 'SE', '208': 'DK', '246': 'FI', '352': 'IS', '248': 'FI', '234': 'DK' };   // Åland → FI, Faroes → DK
  const ANCHOR = { NO: [8.9, 61.0], SE: [15.4, 60.2], DK: [9.25, 56.0], FI: [25.6, 62.4], IS: [-18.7, 64.85] };
  const LABEL_AT = { NO: [9.4, 62.9], SE: [16.2, 64.6], DK: [8.3, 57.35], FI: [27.4, 65.4], IS: [-18.6, 65.95] };
  const EEA_AT = [9.6, 54.1];                       // Schleswig-Holstein: where the other EEA banks' lending comes from
  const REL = { home: 'var(--s1)', nordic: 'var(--s2)', eea: 'var(--s3)' };
  const REL_NAME = { home: 'Banks at home', nordic: 'From another Nordic country', eea: 'From the rest of the EEA' };
  const KIND = { universal: 'Universal bank', savings: 'Savings bank', cooperative: 'Cooperative', mortgage: 'Mortgage bank',
    consumer: 'Consumer finance', investment: 'Investment bank / broker', public: 'Public-sector lender', specialist: 'Specialist lender' };
  const MORT = 'Retail exposures - Secured by residential real estate';
  const LAYERS = [
    { id: 'size', name: 'Market size', unit: 'credit exposure' },
    { id: 'conc', name: 'Concentration', unit: 'share of the three largest' },
    { id: 'cross', name: 'Foreign lenders', unit: 'share lent by banks based abroad' },
    { id: 'mort', name: 'Mortgage risk weight', unit: 'IRB mortgages of the banks based there' },
  ];

  const ST = { date: 'latest', country: 'NORDIC', layer: 'conc', bank: null, focus: null, league: 'ALL', sort: 'exp', dir: -1 };
  // focus: what the map highlights — the last thing clicked ('bank' or 'country'), nothing on load
  let BANKS = {}, ENT = {}, EXP = [], MET = {}, CQ = {}, IRBT = {}, DATES = [];
  let VIEW = null;                                  // the selection the map, shares and league currently show

  // ── data ─────────────────────────────────────────────────────────────────────────────────────────────────────────
  const HEAD = ['cet1_ratio', 'tier1_ratio', 'total_ratio', 'trea', 'cet1', 'lev_ratio', 'lev_exposure', 'lcr', 'nsfr', 'p2r_cet1', 'cbr',
    'ocr', 'mda_headroom', 'retail_deposits'];
  async function load() {
    const [banks, ents, exp, met, cq, irb] = await Promise.all([
      get('nordic_banks?select=*'),
      get('p3dh_entities?select=lei,name,country,highest_eea,parent_lei,nordic,size'),
      get('nordic_exposure?quality=in.(filed,scaled,total)&select=lei,ref_date,country,exposure_eur,exposure_sa_eur,exposure_irb_eur,rwea_eur,quality'),
      get(`nordic_metrics?metric=in.(${HEAD.join(',')})&select=lei,ref_date,metric,value,value_eur,currency,quality,note`),
      get('nordic_credit_quality?sector=in.(loans,households,nfc,sme)&select=*'),
      get(`nordic_irb?bucket=eq.total&select=lei,ref_date,exposure_class,approach,ead,pd,lgd,density,obligors,ccf,rwea,el,provisions,currency,eur_rate,quality`),
    ]);
    if (!banks || !exp) return false;
    BANKS = Object.fromEntries(banks.map((b) => [b.lei, b]));
    ENT = Object.fromEntries((ents || []).map((e) => [e.lei, e]));
    EXP = exp.map((r) => ({ ...r, e: +r.exposure_eur || 0 }));
    for (const r of met || []) ((MET[r.lei] ||= {})[r.ref_date] ||= {})[r.metric] = r;
    for (const r of cq || []) ((CQ[r.lei] ||= {})[r.ref_date] ||= {})[r.sector + '.' + r.measure] = r;
    for (const r of irb || []) ((IRBT[r.lei] ||= {})[r.ref_date] ||= []).push(r);
    DATES = [...new Set(EXP.map((r) => r.ref_date))].sort();
    return true;
  }
  const bank = (lei) => BANKS[lei] || { lei, short: (ENT[lei] || {}).name || lei, country: (ENT[lei] || {}).country, nordic: false };
  const short = (lei) => bank(lei).short;
  const home = (lei) => bank(lei).country || (ENT[lei] || {}).country;
  const counts = (lei) => {                          // in market shares: the top of a group, not a part-owned company
    const b = BANKS[lei], e = ENT[lei];
    if (b) return b.shares !== false && b.highest_eea !== false;
    return !e || e.highest_eea !== false;
  };
  const rel = (lei, c) => { const h = home(lei); return h === c ? 'home' : NORDIC.includes(h) ? 'nordic' : 'eea'; };
  function latestMetric(lei, m, on) {
    const d = MET[lei]; if (!d) return null;
    const ds = Object.keys(d).filter((x) => d[x][m] && (!on || x <= on)).sort();
    return ds.length ? { ...d[ds[ds.length - 1]][m], ref_date: ds[ds.length - 1] } : null;
  }

  // the exposure rows on show: one filing per bank, its latest (within ~13 months of the newest) or the chosen date
  function select() {
    const newest = DATES[DATES.length - 1];
    const floor = newest ? new Date(Date.parse(newest) - 400 * 864e5).toISOString().slice(0, 10) : null;
    const pick = {};
    for (const r of EXP) {
      if (!counts(r.lei) || r.country === 'ALL') continue;
      if (ST.date === 'latest' ? r.ref_date < floor : r.ref_date !== ST.date) continue;
      if (!pick[r.lei] || r.ref_date > pick[r.lei]) pick[r.lei] = r.ref_date;
    }
    const rows = EXP.filter((r) => pick[r.lei] === r.ref_date && r.country !== 'ALL' && NORDIC.includes(r.country) && counts(r.lei));
    const all = EXP.filter((r) => pick[r.lei] === r.ref_date && r.country === 'ALL');
    const market = {}, byBank = {};
    for (const c of NORDIC) market[c] = { c, total: 0, rows: [] };
    for (const r of rows) {
      market[r.country].total += r.e; market[r.country].rows.push(r);
      (byBank[r.lei] ||= { lei: r.lei, date: r.ref_date, by: {}, nordic: 0 }).by[r.country] = r.e;
      byBank[r.lei].nordic += r.e;
    }
    for (const r of all) if (byBank[r.lei]) byBank[r.lei].all = r.e;
    for (const m of Object.values(market)) {
      m.rows.sort((a, b) => b.e - a.e);
      const sh = m.rows.map((r) => r.e / (m.total || 1));
      m.top3 = sum(sh.slice(0, 3)); m.hhi = sum(sh.map((s) => s * s * 1e4)); m.n = m.rows.length;
      m.cross = sum(m.rows.filter((r) => rel(r.lei, m.c) !== 'home').map((r) => r.e)) / (m.total || 1);
      m.eea = sum(m.rows.filter((r) => rel(r.lei, m.c) === 'eea').map((r) => r.e));
      m.mort = mortRW(m.c);
    }
    const nordic = { c: 'NORDIC', total: sum(Object.values(market).map((m) => m.total)), rows: [] };
    for (const b of Object.values(byBank)) nordic.rows.push({ lei: b.lei, e: b.nordic, ref_date: b.date });
    nordic.rows.sort((a, b) => b.e - a.e);
    const sh = nordic.rows.map((r) => r.e / (nordic.total || 1));
    nordic.top3 = sum(sh.slice(0, 3)); nordic.hhi = sum(sh.map((s) => s * s * 1e4)); nordic.n = nordic.rows.length;
    nordic.cross = sum(Object.values(market).map((m) => m.cross * m.total)) / (nordic.total || 1);
    const dates = Object.values(pick);
    VIEW = { market, nordic, byBank, from: dates.length ? dates.reduce((a, b) => a < b ? a : b) : null,
             to: dates.length ? dates.reduce((a, b) => a > b ? a : b) : null };
    return VIEW;
  }
  function mortRW(c) {        // EAD-weighted IRB mortgage density of the banks based in c, each at its latest CR6
    let e = 0, r = 0;
    for (const [lei, byDate] of Object.entries(IRBT)) {
      if (home(lei) !== c || !counts(lei)) continue;
      const ds = Object.keys(byDate).filter((d) => ST.date === 'latest' || d <= ST.date).sort();
      const rows = ds.length ? byDate[ds[ds.length - 1]].filter((x) => x.exposure_class === MORT && ok(x.density) && ok(x.ead) && x.quality !== 'suspect') : [];
      for (const x of rows) { const w = x.ead * (x.eur_rate || 1); e += w; r += w * x.density; }
    }
    return e ? r / e : null;
  }

  // ── tooltip ──────────────────────────────────────────────────────────────────────────────────────────────────────
  let TT;
  function tip(html, ev) {
    if (!TT) { TT = document.createElement('div'); TT.className = 'nm-tt'; document.body.appendChild(TT); }
    if (!html) { TT.style.display = 'none'; return; }
    TT.innerHTML = html; TT.style.display = 'block';
    const x = ev.clientX, y = ev.clientY, w = TT.offsetWidth, h = TT.offsetHeight;
    TT.style.left = Math.min(window.innerWidth - w - 10, x + 14) + 'px';
    TT.style.top = (y + h + 18 > window.innerHeight ? y - h - 12 : y + 14) + 'px';
  }
  function bankTip(lei, c) {
    const b = bank(lei), v = VIEW.byBank[lei];
    if (!v) return `<b>${esc(b.short)}</b>`;
    const rows = NORDIC.filter((k) => v.by[k]).sort((x, y) => v.by[y] - v.by[x])
      .map((k) => `<tr><td>${CNAME[k]}</td><td>${eur(v.by[k])}</td><td>${pct(v.by[k] / VIEW.market[k].total)}</td></tr>`).join('');
    const cet1 = latestMetric(lei, 'cet1_ratio');
    return `<b>${esc(b.short)}</b> <span class="faint">· ${esc(b.city || '')}${b.city ? ', ' : ''}${CNAME[home(lei)] || home(lei) || ''}</span>
      <table class="tg"><tr><th></th><th>exposure</th><th>share</th></tr>${rows}</table>
      <div class="tm">Nordic exposure ${eur(v.nordic)}${v.all ? ` of ${eur(v.all)} in all countries` : ''} · CCyB1 ${dlong(v.date)}${cet1 ? ` · CET1 ${pct(cet1.value)}` : ''}</div>`;
  }

  // ── the map ──────────────────────────────────────────────────────────────────────────────────────────────────────
  let GEO = null;
  async function drawMap() {
    const box = $('#nmMap'); if (!box) return;
    const svgHost = $('#nmSvg');
    if (!window.d3 || !window.topojson) { svgHost.innerHTML = '<div class="empty">Map libraries unavailable</div>'; return; }
    if (!GEO) {
      try { GEO = await (await fetch(TOPO)).json(); } catch (e) { svgHost.innerHTML = '<div class="empty">Map data unavailable</div>'; return; }
    }
    const W = svgHost.clientWidth || 860, H = svgHost.clientHeight || 700;
    const mobile = W < 640;
    const feats = topojson.feature(GEO, GEO.objects.countries).features;
    // the main map: Norway, Sweden, Denmark and Finland with the North and Norwegian Seas to the west (the copy sits there);
    // Iceland gets an inset in the bottom-left corner, as on most maps of the region
    const west = mobile ? [[4.6, 58.0], [5.0, 62.2]] : [[-1.5, 58.4], [2.5, 66.2]];   // on phones the copy sits above the map, not on the sea
    const frame = { type: 'MultiPoint', coordinates: [...west, [8.1, 54.5], [15.2, 54.6], [31.5, 59.8],
      [31.4, 70.1], [27.9, 71.3], [15.6, 69.7]] };
    const pr = mobile ? 10 : 150, pb = mobile ? 26 : 44;
    const proj = d3.geoConicConformal().parallels([58, 69]).rotate([-12, 0]).fitExtent([[10, 12], [W - pr, H - pb]], frame);
    proj.clipExtent([[0, 0], [W, H]]);
    const path = d3.geoPath(proj);
    const iw = mobile ? 126 : 176, ih = mobile ? 92 : 124, ix = 12, iy = H - ih - 12;
    const isl = feats.find((f) => String(f.id).padStart(3, '0') === '352');
    const projI = d3.geoConicConformal().parallels([63.5, 66]).rotate([19, 0]).fitExtent([[ix + 10, iy + 26], [ix + iw - 10, iy + ih - 8]], isl);
    projI.clipExtent([[ix, iy], [ix + iw, iy + ih]]);
    const pathI = d3.geoPath(projI);
    const P = (lonlat, c) => (c === 'IS' ? projI : proj)(lonlat);
    const ns = 'http://www.w3.org/2000/svg';
    const svg = document.createElementNS(ns, 'svg');
    svg.setAttribute('viewBox', `0 0 ${W} ${H}`); svg.setAttribute('role', 'img');
    svg.setAttribute('aria-label', 'Map of the Nordic countries with each banking group at its head office and its lending across borders');
    const g = (cls) => { const e = document.createElementNS(ns, 'g'); e.setAttribute('class', cls); svg.appendChild(e); return e; };
    const gGrat = g('nm-grat'), gCtx = g('nm-ctx'), gCty = g('nm-cty'), gIns = g('nm-inset'), gFlow = g('nm-flows'), gNode = g('nm-nodes'), gLab = g('nm-labs');
    const grat = document.createElementNS(ns, 'path'); grat.setAttribute('d', path(d3.geoGraticule().step([5, 2.5])()));
    gGrat.appendChild(grat);
    const near = feats.filter((f) => {        // Russia's bounds cross the antimeridian (west > east): keep it
      const b = d3.geoBounds(f), wrap = b[0][0] > b[1][0];
      return (wrap || (b[1][0] > -32 && b[0][0] < 45)) && b[1][1] > 50 && b[0][1] < 75;
    });
    const ctyEls = {};
    const addCountry = (parent, d, c) => {
      const p = document.createElementNS(ns, 'path'); p.setAttribute('d', d);
      if (c) {
        p.setAttribute('class', 'nm-country'); p.dataset.c = c; p.setAttribute('tabindex', '0');
        p.setAttribute('aria-label', CNAME[c]); (ctyEls[c] ||= []).push(p);
      }
      parent.appendChild(p);
    };
    for (const f of near) {
      const id = String(f.id).padStart(3, '0'), c = NUM[id];
      if (id === '352') continue;
      const d = path(f); if (!d) continue;
      addCountry(c ? gCty : gCtx, d, c);
    }
    const r = document.createElementNS(ns, 'rect');
    Object.entries({ x: ix, y: iy, width: iw, height: ih, rx: 7 }).forEach(([k, v]) => r.setAttribute(k, v));
    gIns.appendChild(r);
    addCountry(gIns, pathI(isl), 'IS');
    svgHost.innerHTML = ''; svgHost.appendChild(svg);
    GEO._els = { svg, ctyEls, gFlow, gNode, gLab, gIns, P, W, H, mobile, inset: { ix, iy, iw, ih } };
    paintMap();
    // events
    svg.addEventListener('click', (ev) => {
      const t = ev.target.closest('[data-c],[data-lei]');
      if (!t) { setCountry('NORDIC'); return; }
      if (t.dataset.lei) setBank(t.dataset.lei, true); else setCountry(t.dataset.c);
    });
    svg.addEventListener('keydown', (ev) => {
      if (ev.key !== 'Enter' && ev.key !== ' ') return;
      const t = ev.target.closest('[data-c],[data-lei]'); if (!t) return;
      ev.preventDefault(); if (t.dataset.lei) setBank(t.dataset.lei, true); else setCountry(t.dataset.c);
    });
    svg.addEventListener('mousemove', (ev) => {
      const t = ev.target.closest('[data-c],[data-lei],[data-flow]');
      if (!t) { tip(null); hover(null); return; }
      if (t.dataset.lei) { tip(bankTip(t.dataset.lei), ev); hover({ lei: t.dataset.lei }); }
      else if (t.dataset.flow) { tip(t.dataset.flow, ev); }
      else { tip(countryTip(t.dataset.c), ev); hover({ c: t.dataset.c }); }
    });
    svg.addEventListener('mouseleave', () => { tip(null); hover(null); });
  }
  function countryTip(c) {
    const m = VIEW.market[c]; if (!m) return CNAME[c];
    const top = m.rows.slice(0, 5).map((r) => `<tr><td><i style="background:${REL[rel(r.lei, c)]}"></i>${esc(short(r.lei))}</td><td>${pct(r.e / m.total)}</td></tr>`).join('');
    return `<b>${CNAME[c]}</b> <span class="faint">· ${eur(m.total)} · ${m.n} banks</span>
      <table class="tg">${top}</table>
      <div class="tm">Three largest ${pct(m.top3, 0)} · HHI ${nb(m.hhi)} · banks based abroad ${pct(m.cross, 0)}${ok(m.mort) ? ` · mortgage RW ${pct(m.mort)}` : ''}</div>`;
  }
  function layerValue(c) {
    const m = VIEW.market[c]; if (!m) return null;
    return { size: m.total, conc: m.top3, cross: m.cross, mort: m.mort }[ST.layer];
  }
  function paintMap() {
    if (!GEO || !GEO._els) return;
    const { ctyEls, gFlow, gNode, gLab, P, W, H, mobile } = GEO._els;
    const vals = NORDIC.map(layerValue).filter(ok);
    const lo = ST.layer === 'size' ? 0 : Math.min(...vals), hi = Math.max(...vals);
    const scale = (v) => {
      if (!ok(v)) return '#141a26';
      const t = ST.layer === 'size' ? Math.sqrt(v / hi) : hi > lo ? (v - lo) / (hi - lo) : 1;
      return d3.interpolateLab('#13283a', '#2bb3e8')(0.12 + 0.88 * t);
    };
    for (const [c, els] of Object.entries(ctyEls)) for (const p of els) {
      p.style.fill = scale(layerValue(c));
      p.classList.toggle('on', ST.country === c);
    }
    // legend
    const L = LAYERS.find((x) => x.id === ST.layer);
    const fmtL = (v) => ST.layer === 'size' ? eur(v) : pct(v, ST.layer === 'mort' ? 1 : 0);
    const stops = [0, 0.25, 0.5, 0.75, 1].map((t) => d3.interpolateLab('#13283a', '#2bb3e8')(0.12 + 0.88 * t));
    $('#nmLegend').innerHTML = `<div class="lt">${esc(L.name)} <span class="faint">· ${esc(L.unit)}</span></div>
      <div class="ramp" style="background:linear-gradient(90deg,${stops.join(',')})"></div>
      <div class="rl"><span>${fmtL(lo)}</span><span>${fmtL(hi)}</span></div>
      <div class="lk"><span><i class="dot"></i>Banking group at its head office · area = Nordic credit exposure</span>
      <span><i class="arc"></i>Lending into another Nordic country</span><span><i class="arc eea"></i>Lending by banks from the rest of the EEA</span></div>`;
    // nodes: every Nordic group that counts, at its head office, collisions resolved around the city
    const nodes = [];
    const maxE = Math.max(1, ...Object.values(VIEW.byBank).map((b) => b.nordic));
    const rMax = mobile ? 20 : 30;
    for (const b of Object.values(VIEW.byBank)) {
      const bk = BANKS[b.lei];
      if (!bk || !bk.nordic || !ok(bk.lat)) continue;
      const [x, y] = P([bk.lon, bk.lat], bk.country);
      nodes.push({ lei: b.lei, hx: x, hy: y, x, y, r: Math.max(2.4, Math.sqrt(b.nordic / maxE) * rMax), e: b.nordic });
    }
    nodes.sort((a, b) => b.r - a.r);
    const sim = d3.forceSimulation(nodes).force('x', d3.forceX((d) => d.hx).strength(0.5)).force('y', d3.forceY((d) => d.hy).strength(0.5))
      .force('c', d3.forceCollide((d) => d.r + 1.6).iterations(3)).stop();
    for (let i = 0; i < 160; i++) sim.tick();
    const byLei = Object.fromEntries(nodes.map((n) => [n.lei, n]));
    // flows
    const fl = [];
    const maxF = Math.max(1, ...Object.values(VIEW.byBank).flatMap((b) => NORDIC.filter((c) => c !== home(b.lei)).map((c) => b.by[c] || 0)));
    for (const b of Object.values(VIEW.byBank)) {
      const n = byLei[b.lei]; if (!n) continue;
      for (const c of NORDIC) {
        const e = b.by[c] || 0;
        if (c === home(b.lei) || e < 1.5e9) continue;
        fl.push({ lei: b.lei, c, e, from: [n.x, n.y], to: P(ANCHOR[c], c), eea: false });
      }
    }
    const eeaAt = P(EEA_AT);
    const maxEEA = Math.max(1, ...NORDIC.map((c) => VIEW.market[c].eea));
    for (const c of NORDIC) if (VIEW.market[c].eea > 5e8 && c !== 'IS') fl.push({ c, e: VIEW.market[c].eea, from: eeaAt, to: P(ANCHOR[c], c), eea: true });
    let fh = '';
    for (const f of fl.sort((a, b) => b.e - a.e)) {
      const [x1, y1] = f.from, [x2, y2] = f.to;
      const dx = x2 - x1, dy = y2 - y1, len = Math.hypot(dx, dy) || 1;
      const k = (f.eea ? 0.18 : 0.26) * len, mx = (x1 + x2) / 2 - dy / len * k, my = (y1 + y2) / 2 + dx / len * k;
      const w = Math.max(0.9, Math.sqrt(f.e / (f.eea ? maxEEA : maxF)) * (mobile ? 7 : 10));
      const label = f.eea ? `<b>Other EEA banks → ${CNAME[f.c]}</b><div class="tm">${eur(f.e)} · ${pct(f.e / VIEW.market[f.c].total)} of the market</div>`
        : `<b>${esc(short(f.lei))} → ${CNAME[f.c]}</b><div class="tm">${eur(f.e)} · ${pct(f.e / VIEW.market[f.c].total)} of ${CNAME[f.c]}'s market</div>`;
      fh += `<path class="nm-flow${f.eea ? ' eea' : ''}" data-flow="${esc(label)}" data-from="${f.lei || 'EEA'}" data-to="${f.c}" d="M${x1.toFixed(1)},${y1.toFixed(1)} Q${mx.toFixed(1)},${my.toFixed(1)} ${x2.toFixed(1)},${y2.toFixed(1)}" stroke-width="${w.toFixed(2)}"/>`;
    }
    fh += `<g class="nm-eea"><circle cx="${eeaAt[0].toFixed(1)}" cy="${eeaAt[1].toFixed(1)}" r="5"/><text x="${(eeaAt[0] + 9).toFixed(1)}" y="${(eeaAt[1] + 4).toFixed(1)}">Other EEA banks</text></g>`;
    gFlow.innerHTML = fh;
    // nodes, leader lines, labels
    let nh = '';
    for (const n of nodes) {
      if (Math.hypot(n.x - n.hx, n.y - n.hy) > n.r + 2) nh += `<line class="nm-lead" x1="${n.hx.toFixed(1)}" y1="${n.hy.toFixed(1)}" x2="${n.x.toFixed(1)}" y2="${n.y.toFixed(1)}"/>`;
    }
    for (const n of nodes) {
      nh += `<g class="nm-node${ST.focus === 'bank' && ST.bank === n.lei ? ' on' : ''}" data-lei="${n.lei}" tabindex="0" role="button" aria-label="${esc(short(n.lei))}, ${eur(n.e)} Nordic credit exposure">
        <circle cx="${n.x.toFixed(1)}" cy="${n.y.toFixed(1)}" r="${n.r.toFixed(1)}"/></g>`;
    }
    gNode.innerHTML = nh;
    // labels: the biggest groups first, each in the first free spot around its circle
    const placed = [];
    const free = (b) => b.x0 >= 4 && b.x1 <= W - 4 && b.y0 >= 4 && b.y1 <= H - 4 && !placed.some((p) => !(b.x1 < p.x0 || b.x0 > p.x1 || b.y1 < p.y0 || b.y0 > p.y1));
    let lh = '';
    for (const c of NORDIC) {
      const m = VIEW.market[c];
      const t1 = CNAME[c].toUpperCase(), t2 = eur(m.total);
      if (c === 'IS') {                          // the inset's own header
        const { ix, iy } = GEO._els.inset;
        lh += `<text class="nm-clab" x="${ix + 10}" y="${iy + 15}">${t1} <tspan class="v">${t2}</tspan></text>`;
        placed.push({ x0: ix, x1: ix + GEO._els.inset.iw, y0: iy, y1: iy + 20 });
        continue;
      }
      const [x, y] = P(LABEL_AT[c], c);
      const w = Math.max(t1.length * 7.2, t2.length * 6.2);
      placed.push({ x0: x - w / 2, x1: x + w / 2, y0: y - 12, y1: y + 16 });
      lh += `<text class="nm-clab" x="${x.toFixed(1)}" y="${y.toFixed(1)}" text-anchor="middle">${t1}<tspan x="${x.toFixed(1)}" dy="13" class="v">${t2}</tspan></text>`;
    }
    for (const n of nodes) placed.push({ x0: n.x - n.r, x1: n.x + n.r, y0: n.y - n.r, y1: n.y + n.r });
    const maxLabels = mobile ? 8 : 18;
    let k = 0;
    for (const n of nodes) {
      if (k >= maxLabels || n.r < (mobile ? 7 : 6.5)) break;
      const t = short(n.lei), w = t.length * 6.1 + 4, h = 12;
      const spots = [[n.x + n.r + 4, n.y + 4, 'start'], [n.x - n.r - 4, n.y + 4, 'end'], [n.x, n.y - n.r - 5, 'middle'], [n.x, n.y + n.r + 12, 'middle']];
      for (const [x, y, a] of spots) {
        const x0 = a === 'start' ? x : a === 'end' ? x - w : x - w / 2;
        const b = { x0, x1: x0 + w, y0: y - h + 2, y1: y + 2 };
        if (free(b)) { placed.push(b); lh += `<text class="nm-blab" data-for="${n.lei}" x="${x.toFixed(1)}" y="${y.toFixed(1)}" text-anchor="${a}">${esc(t)}</text>`; k++; break; }
      }
    }
    gLab.innerHTML = lh;
    hover(null);
  }
  function hover(h) {
    if (!GEO || !GEO._els) return;
    const focus = h || (ST.focus === 'bank' && ST.bank ? { lei: ST.bank } : ST.focus === 'country' && ST.country !== 'NORDIC' ? { c: ST.country } : null);
    const { gFlow, gNode } = GEO._els;
    for (const p of gFlow.querySelectorAll('.nm-flow')) {
      const on = focus && (focus.lei ? p.dataset.from === focus.lei : p.dataset.to === focus.c);
      p.classList.toggle('on', !!on); p.classList.toggle('off', !!focus && !on);
    }
    for (const n of gNode.querySelectorAll('.nm-node')) {
      const lei = n.dataset.lei, b = VIEW.byBank[lei];
      const on = focus && (focus.lei ? lei === focus.lei : b && b.by[focus.c] > 0);
      n.classList.toggle('dim', !!focus && !on); n.classList.toggle('on', ST.focus === 'bank' && ST.bank === lei);
    }
  }

  // ── market shares ────────────────────────────────────────────────────────────────────────────────────────────────
  function renderShares() {
    const box = $('#nmShares'); if (!box) return;
    const c = ST.country, m = c === 'NORDIC' ? VIEW.nordic : VIEW.market[c];
    const tabs = ['NORDIC', ...NORDIC].map((k) => `<button data-c="${k}" class="${k === c ? 'on' : ''}">${k === 'NORDIC' ? 'Nordics' : CNAME[k]}</button>`).join('');
    const TOP = 14;
    const relOf = (lei) => c === 'NORDIC' ? (NORDIC.includes(home(lei)) ? 'home' : 'eea') : rel(lei, c);
    const top = m.rows.slice(0, TOP), rest = m.rows.slice(TOP);
    const max = top.length ? top[0].e / m.total : 1;
    const bar = (lei, e, d, other) => {
      const r = other ? null : relOf(lei), s = e / (m.total || 1);
      const b = other ? null : bank(lei);
      const hc = other ? '' : home(lei);
      return `<div class="nm-bar${ST.bank === lei ? ' on' : ''}" ${other ? '' : `data-lei="${lei}" tabindex="0" role="button"`}>
        <span class="n">${esc(other ? `${rest.length} others in the hub` : b.short)}${other || !hc ? '' : `<small>${hc}</small>`}</span>
        <span class="t"><i style="width:${Math.max(0.6, s / max * 100).toFixed(1)}%;background:${other ? 'var(--s-muted)' : REL[r]}"></i></span>
        <span class="v">${pct(s)} <small>${bnv(e)}</small></span></div>`;
    };
    const bars = top.map((r) => bar(r.lei, r.e, r.ref_date)).join('') + (rest.length ? bar(null, sum(rest.map((r) => r.e)), null, true) : '');
    const comp = c === 'NORDIC'
      ? [['home', sum(m.rows.filter((r) => NORDIC.includes(home(r.lei))).map((r) => r.e))], ['eea', sum(m.rows.filter((r) => !NORDIC.includes(home(r.lei))).map((r) => r.e))]]
      : ['home', 'nordic', 'eea'].map((k) => [k, sum(m.rows.filter((r) => rel(r.lei, c) === k).map((r) => r.e))]);
    const strip = comp.filter(([, v]) => v > 0).map(([k, v]) => `<i style="flex:${v};background:${REL[k]}" title="${esc(REL_NAME[k])}: ${pct(v / m.total)}"></i>`).join('');
    const legend = comp.map(([k, v]) => `<span><i style="background:${REL[k]}"></i>${c === 'NORDIC' && k === 'home' ? 'Nordic banks' : REL_NAME[k]} <b>${pct(v / (m.total || 1), 0)}</b></span>`).join('');
    const span = VIEW.from === VIEW.to ? dlong(VIEW.to) : `${dl(VIEW.from)} – ${dl(VIEW.to)}`;
    box.innerHTML = `<div class="seg-tabs nm-tabs">${tabs}</div>
      <div class="nm-head"><div class="big">${eur(m.total)}</div>
        <div class="sub">credit exposure in ${c === 'NORDIC' ? 'the five countries' : CNAME[c]}, as disclosed by <b>${m.n}</b> banks
        · three largest <b>${pct(m.top3, 0)}</b> · HHI <b>${nb(m.hhi)}</b></div></div>
      <div class="nm-strip">${strip}</div><div class="nm-lg">${legend}</div>
      <div class="nm-bars">${bars}</div>
      <p class="note">Share of the credit exposure every EEA bank in the EBA's Pillar 3 data hub reports for borrowers in ${c === 'NORDIC' ? 'the Nordics' : CNAME[c]}
        (CCyB1: loans, guarantees and undrawn lines to households and companies, after conversion factors; public sector and banks excluded).
        Small savings banks that need not file to the hub, and banks from outside the EEA, are not in it. ${ST.date === 'latest' ? `Each bank's latest disclosure (${span}).` : `At ${dlong(ST.date)}.`}
        EUR bn at the ECB rate of each bank's reference date.</p>`;
    box.querySelector('.nm-tabs').addEventListener('click', (ev) => { const b = ev.target.closest('button'); if (b) setCountry(b.dataset.c); });
    box.querySelector('.nm-bars').addEventListener('click', (ev) => { const b = ev.target.closest('[data-lei]'); if (b) setBank(b.dataset.lei, true); });
    box.querySelector('.nm-bars').addEventListener('keydown', (ev) => {
      if (ev.key !== 'Enter') return; const b = ev.target.closest('[data-lei]'); if (b) setBank(b.dataset.lei, true);
    });
    box.querySelector('.nm-bars').addEventListener('mousemove', (ev) => {
      const b = ev.target.closest('[data-lei]'); if (!b) { tip(null); return; } tip(bankTip(b.dataset.lei, c), ev);
    });
    box.querySelector('.nm-bars').addEventListener('mouseleave', () => tip(null));
  }

  // ── hero KPIs, controls ──────────────────────────────────────────────────────────────────────────────────────────
  function renderKpis() {
    const n = VIEW.nordic, el = $('#nmKpis'); if (!el) return;
    const nordicBanks = n.rows.filter((r) => NORDIC.includes(home(r.lei))).length;
    const foreign = n.rows.length - nordicBanks;
    el.innerHTML = `<span class="chip"><b>${eur(n.total)}</b> credit exposure</span>
      <span class="chip"><b>${nordicBanks}</b> Nordic groups · <b>${foreign}</b> other EEA lenders</span>
      <span class="chip"><b>${pct(n.cross, 0)}</b> lent across a border</span>`;
    const dates = $('#nmDates');
    if (dates) {
      const ds = DATES.filter((d) => /-(06-30|12-31)$/.test(d)).slice(-3).reverse();
      dates.innerHTML = [['latest', 'Latest'], ...ds.map((d) => [d, dl(d)])]
        .map(([k, t]) => `<button data-d="${k}" class="${ST.date === k ? 'on' : ''}" aria-pressed="${ST.date === k}">${t}</button>`).join('');
    }
    const layers = $('#nmLayers');
    if (layers) layers.innerHTML = '<div class="lt">Shade countries by</div>' + LAYERS.map((l) =>
      `<button class="layer-btn" data-l="${l.id}" aria-pressed="${ST.layer === l.id}">${esc(l.name)}</button>`).join('');
  }
  function bindControls() {
    $('#nmDates') && $('#nmDates').addEventListener('click', (ev) => {
      const b = ev.target.closest('button'); if (!b) return; ST.date = b.dataset.d; refresh();
    });
    $('#nmLayers') && $('#nmLayers').addEventListener('click', (ev) => {
      const b = ev.target.closest('button'); if (!b) return; ST.layer = b.dataset.l; renderKpis(); paintMap();
    });
  }
  function setCountry(c) {
    ST.country = c; ST.focus = c === 'NORDIC' ? null : 'country'; renderShares(); paintMap();
    const lg = c === 'NORDIC' ? 'ALL' : c;
    if (ST.league !== lg) { ST.league = lg; renderLeague(); }
  }
  function setBank(lei, scroll) {
    if (lei) { ST.bank = lei; ST.focus = 'bank'; } else ST.focus = null;
    paintMap(); renderShares(); renderLeague();
    if (lei) { renderBank(lei); if (scroll) { const s = $('#nordic-bank'); s && s.scrollIntoView({ behavior: 'smooth', block: 'start' }); } }
  }
  function refresh() { select(); renderKpis(); paintMap(); renderShares(); renderLeague(); }

  // ── the Nordic league ────────────────────────────────────────────────────────────────────────────────────────────
  function leagueRows() {
    const out = [];
    for (const b of Object.values(BANKS)) {
      if (!b.nordic || !counts(b.lei)) continue;
      const v = VIEW.byBank[b.lei];
      const k = (m) => latestMetric(b.lei, m);
      const cq = latestCQ(b.lei), mort = latestMort(b.lei);
      const cet1 = k('cet1_ratio'), req = (ok(val(k('p2r_cet1'))) && ok(val(k('cbr')))) ? 0.045 + val(k('p2r_cet1')) + val(k('cbr')) : null;
      out.push({
        lei: b.lei, b, v, exp: v ? (v.all || v.nordic) : null, nordic: v ? v.nordic : null,
        trea: k('trea'), cet1, head: cet1 && ok(req) ? cet1.value - req : null, lev: k('lev_ratio'), lcr: k('lcr'), nsfr: k('nsfr'),
        s2: cq ? cq.s2 : null, s3: cq ? cq.s3 : null, cov: cq ? cq.cov : null, cqDate: cq ? cq.date : null, mort,
        asof: (cet1 || {}).ref_date,
      });
    }
    return out;
  }
  const val = (r) => r ? +r.value : null;
  function latestCQ(lei) {
    const d = CQ[lei]; if (!d) return null;
    for (const date of Object.keys(d).sort().reverse()) {
      const g = d[date]['loans.gross'], a = d[date]['loans.allowance'];
      if (!g) continue;
      const tot = (+g.performing || 0) + (+g.npe || 0);
      if (!tot) continue;
      return { date, tot, s1: (+g.s1 || 0) / tot, s2: ((+g.s2 || 0) + (+g.npe_s2 || 0)) / tot, s3: (+g.s3 || 0) / tot, npe: (+g.npe || 0) / tot,
        cov: a && +g.s3 ? (+a.s3 || 0) / +g.s3 : null, g, a };
    }
    return null;
  }
  function latestMort(lei) {
    const d = IRBT[lei]; if (!d) return null;
    for (const date of Object.keys(d).sort().reverse()) {
      const rows = d[date].filter((x) => x.exposure_class === MORT && ok(x.density) && x.quality !== 'suspect');
      if (rows.length) { const e = sum(rows.map((x) => x.ead)); return { date, density: sum(rows.map((x) => x.ead * x.density)) / (e || 1), ead: e, cur: rows[0].currency }; }
    }
    return null;
  }
  const COLS = [
    ['bank', 'Bank', (r) => r.b.short], ['exp', 'Credit exposure', (r) => r.exp], ['foot', 'Where it lends', (r) => r.nordic],
    ['trea', 'TREA', (r) => r.trea && +r.trea.value_eur], ['cet1', 'CET1', (r) => val(r.cet1)], ['head', 'Above req.', (r) => r.head],
    ['lev', 'Leverage', (r) => val(r.lev)], ['lcr', 'LCR', (r) => val(r.lcr)], ['nsfr', 'NSFR', (r) => val(r.nsfr)],
    ['s2', 'Stage 2', (r) => r.s2], ['s3', 'Stage 3', (r) => r.s3], ['cov', 'S3 cover', (r) => r.cov], ['mort', 'Mortgage RW', (r) => r.mort && r.mort.density],
  ];
  function qm(r) { return r && r.quality && !['filed'].includes(r.quality) ? `<span class="mk q" title="${esc(r.quality + (r.note ? ': ' + r.note : ''))}">${r.quality === 'scaled' ? '×' : '~'}</span>` : ''; }
  function renderLeague() {
    const box = $('#nmLeague'); if (!box) return;
    let rows = leagueRows();
    if (ST.league !== 'ALL') rows = rows.filter((r) => r.b.country === ST.league);
    const col = COLS.find((c) => c[0] === ST.sort) || COLS[1];
    rows.sort((a, b) => {
      const x = col[2](a), y = col[2](b);
      if (typeof x === 'string' || typeof y === 'string') return ST.dir * String(x || '').localeCompare(String(y || ''), 'nb');
      return (ok(y) ? 1 : 0) - (ok(x) ? 1 : 0) || ST.dir * ((x ?? 0) - (y ?? 0));
    });
    const chips = ['ALL', ...NORDIC].map((k) => `<button data-c="${k}" class="${ST.league === k ? 'on' : ''}">${k === 'ALL' ? 'All countries' : CNAME[k]}</button>`).join('');
    const foot = (r) => {
      if (!r.v) return '<span class="faint">—</span>';
      const parts = NORDIC.map((k) => [k, r.v.by[k] || 0]).filter(([, e]) => e > 0);
      const rest = Math.max(0, (r.v.all || 0) - r.v.nordic);
      const tot = sum(parts.map((p) => p[1])) + rest;
      return `<span class="nm-foot" title="${esc(parts.map(([k, e]) => `${CNAME[k]} ${pct(e / tot, 0)}`).join(' · ') + (rest ? ` · outside the Nordics ${pct(rest / tot, 0)}` : ''))}">`
        + parts.map(([k, e]) => `<i style="flex:${e};background:${k === r.b.country ? 'var(--s1)' : 'var(--s2)'}"></i>`).join('')
        + (rest ? `<i style="flex:${rest};background:var(--s-muted)"></i>` : '') + '</span>';
    };
    const cls = (v, good, bad) => !ok(v) ? '' : v < bad ? 'hot' : v < good ? 'warm' : '';
    const body = rows.map((r) => `<tr class="row${ST.bank === r.lei ? ' open' : ''}" data-lei="${r.lei}" tabindex="0">
      <td class="l bn">${esc(r.b.short)}<small>${esc(r.b.city || '')}${r.b.city ? ' · ' : ''}${esc(KIND[r.b.kind] || r.b.kind || '')}${r.b.ecb ? ' · ECB' : ''}</small></td>
      <td>${r.exp ? bnv(r.exp) : '—'}</td><td class="l">${foot(r)}</td>
      <td>${r.trea ? bnv(r.trea.value_eur) : '—'}${qm(r.trea)}</td>
      <td>${pct(val(r.cet1))}${qm(r.cet1)}</td><td class="${cls(r.head, 0.02, 0.01)}">${ok(r.head) ? (r.head >= 0 ? '+' : '') + nb(r.head * 100, 1) + ' pp' : '—'}</td>
      <td>${pct(val(r.lev))}${qm(r.lev)}</td><td>${pct(val(r.lcr), 0)}${qm(r.lcr)}</td><td>${pct(val(r.nsfr), 0)}${qm(r.nsfr)}</td>
      <td>${pct(r.s2)}</td><td>${pct(r.s3, 2)}</td><td>${pct(r.cov, 0)}</td><td>${r.mort ? pct(r.mort.density) : '—'}</td></tr>`).join('');
    const th = COLS.map(([id, name]) => `<th data-s="${id}" class="${id === 'bank' || id === 'foot' ? 'l' : ''}${ST.sort === id ? ' on' : ''}">${name}${ST.sort === id ? (ST.dir < 0 ? ' ↓' : ' ↑') : ''}</th>`).join('');
    box.innerHTML = `<div class="seg-tabs nm-league-tabs">${chips}</div>
      <div class="ut-wrap"><table class="ut nm-ut"><thead><tr>${th}</tr></thead><tbody>${body}</tbody></table></div>
      <p class="note">EUR bn. Credit exposure: CCyB1, all countries, at the bank's latest disclosure; the bar splits it between the home country
        (ice), the other Nordic countries (amber) and the rest of the world (grey). CET1, leverage, LCR and NSFR: KM1 at the latest quarter.
        Above req.: CET1 ratio less 4.5 %, the CET1 part of Pillar 2 and the combined buffer (KM1 rows 5, EU 7e, 11): the distance to the
        MDA trigger. Stage 2 and 3: share of gross loans and advances (CR1), S3 cover: allowances on stage 3 over stage 3 loans. Mortgage RW: the
        IRB risk-weight density of residential mortgages (CR6). <span class="mk q">×</span> rescaled, <span class="mk q">~</span> derived or taken
        from a fallback row — hover for why.</p>`;
    box.querySelector('.nm-league-tabs').addEventListener('click', (ev) => { const b = ev.target.closest('button'); if (b) { ST.league = b.dataset.c; renderLeague(); } });
    box.querySelector('thead').addEventListener('click', (ev) => {
      const t = ev.target.closest('th'); if (!t) return;
      if (ST.sort === t.dataset.s) ST.dir = -ST.dir; else { ST.sort = t.dataset.s; ST.dir = t.dataset.s === 'bank' ? 1 : -1; }
      renderLeague();
    });
    const tb = box.querySelector('tbody');
    tb.addEventListener('click', (ev) => { const r = ev.target.closest('tr[data-lei]'); if (r) setBank(r.dataset.lei, true); });
    tb.addEventListener('keydown', (ev) => { if (ev.key === 'Enter') { const r = ev.target.closest('tr[data-lei]'); if (r) setBank(r.dataset.lei, true); } });
  }

  // ── one bank ─────────────────────────────────────────────────────────────────────────────────────────────────────
  const OV1 = [['rwea_credit_sa', 'Credit · standardised', 'var(--s1)'], ['irb', 'Credit · IRB models', 'var(--s2)'],
    ['addon', 'Credit · on top of the models (floors, add-ons)', 'repeating-linear-gradient(135deg, var(--s2) 0 3px, rgba(203,127,0,.25) 3px 6px)'],
    ['rwea_op', 'Operational', 'var(--s3)'], ['other', 'Counterparty, CVA, market, securitisation, threshold items', 'var(--s-muted)']];
  const SHOCKS = [['parallel_up', 'Parallel up'], ['parallel_down', 'Parallel down'], ['steepener', 'Steepener'], ['flattener', 'Flattener'],
    ['short_up', 'Short rates up'], ['short_down', 'Short rates down']];
  const BUCKETS = [['0.00-0.15', ['0.00-0.10', '0.10-0.15']], ['0.15-0.25', []], ['0.25-0.50', []], ['0.50-0.75', []], ['0.75-2.50', ['0.75-1.75', '1.75-2.5']],
    ['2.50-10.00', ['2.5-5', '5-10']], ['10.00-100.00', ['10-20', '20-30', '30-100']], ['default', []]];
  let BANK_REQ = 0;
  async function renderBank(lei) {
    const box = $('#nmBank'); if (!box) return;
    const req = ++BANK_REQ;
    const b = bank(lei);
    box.innerHTML = `<div class="empty">Loading ${esc(b.short)} …</div>`;
    const [met, irb, cq, subs, scale, fixes] = await Promise.all([
      get(`nordic_metrics?lei=eq.${lei}&select=ref_date,metric,value,value_eur,currency,quality,note&order=ref_date`),
      get(`nordic_irb?lei=eq.${lei}&select=*`),
      get(`nordic_credit_quality?lei=eq.${lei}&select=*`),
      get(`p3dh_submissions?lei=eq.${lei}&latest=is.true&select=ref_date,module,file_url,received_at&order=ref_date.desc`),
      get(`p3dh_scale?lei=eq.${lei}&select=*`),
      get(`nordic_fixes?lei=eq.${lei}&select=*&order=ref_date.desc`),
    ]);
    if (req !== BANK_REQ) return;
    const M = {};
    for (const r of met || []) (M[r.ref_date] ||= {})[r.metric] = r;
    const dates = Object.keys(M).sort();
    const last = (m) => { for (let i = dates.length - 1; i >= 0; i--) if (M[dates[i]][m]) return { ...M[dates[i]][m], ref_date: dates[i] }; return null; };
    const v = VIEW.byBank[lei];
    const cur = (last('trea') || {}).currency || 'EUR';
    const k = (m) => last(m);
    const cet1 = k('cet1_ratio'), req4 = (ok(val(k('p2r_cet1'))) && ok(val(k('cbr')))) ? 0.045 + val(k('p2r_cet1')) + val(k('cbr')) : null;
    const tile = (label, value, sub) => `<div class="kpi"><div class="kl">${label}</div><div class="kv">${value}</div><div class="ks">${sub}</div></div>`;
    const local = (r) => r && r.currency && r.currency !== 'EUR' ? ` · ${r.currency} ${nb(r.value / 1e9, Math.abs(r.value) < 1e10 ? 1 : 0)} bn` : '';
    const tiles = [
      tile('CET1 ratio', pct(val(cet1)), cet1 ? `${ok(req4) ? `requirement ${pct(req4)} · ${val(cet1) - req4 >= 0 ? '+' : ''}${nb((val(cet1) - req4) * 100, 1)} pp above` : 'KM1'} · ${dlong(cet1.ref_date)}` : 'not disclosed'),
      tile('Risk exposure (TREA)', k('trea') ? eur(k('trea').value_eur) : '—', k('trea') ? `${local(k('trea')).slice(3)}${ok(val(k('cet1'))) ? ` · CET1 ${eur(k('cet1').value_eur)}` : ''}` : ''),
      tile('Leverage ratio', pct(val(k('lev_ratio'))), k('lev_exposure') ? `exposure ${eur(k('lev_exposure').value_eur)}` : ''),
      tile('LCR · NSFR', `${pct(val(k('lcr')), 0)} <small>· ${pct(val(k('nsfr')), 0)}</small>`, 'liquidity coverage · net stable funding'),
      tile('Credit exposure', v ? eur(v.all || v.nordic) : '—', v ? `${pct(v.nordic / (v.all || v.nordic), 0)} in the Nordics · CCyB1 ${dlong(v.date)}` : 'no CCyB1 rows'),
      tile('Retail deposits', k('retail_deposits') ? eur(k('retail_deposits').value_eur) : '—', k('stable_deposits') ? `${pct(k('stable_deposits').value / k('retail_deposits').value, 0)} insured (stable) · 12-month average` : 'LIQ1'),
    ].join('');
    const files = (subs || []).filter((s) => s.file_url && /CODIS|FINDIS|IRRBBDIS|NONREM/.test(s.module)).slice(0, 6)
      .map((s) => `<a class="dlink" href="${esc(s.file_url)}" rel="noopener">${esc(s.module.replace('P3_NONREM_DIS_DOCS', 'PDF'))} ${dl(s.ref_date)}</a>`).join('');
    box.innerHTML = `
      <div class="nb-head"><div><div class="hero-kicker">◉ ${esc(CNAME[b.country] || b.country || '')} · ${esc(KIND[b.kind] || b.kind || 'bank')}${b.osii ? ' · O-SII' : ''}${b.ecb ? ' · supervised by the ECB' : ''}</div>
        <h2 class="nb-title">${esc(b.short)}</h2><div class="nb-sub">${esc(b.name)} · LEI ${esc(lei)}${b.city ? ' · ' + esc(b.city) : ''}</div></div>
        <div class="nb-files">EBA filings ${files || '<span class="faint">—</span>'}</div></div>
      <div class="kpis nb-kpis">${tiles}</div>
      <div class="grid g12 nb-grid">
        <div class="panel span6"><h3><span><span class="g">◆</span>Capital ratios</span><small>KM1, each quarter filed</small></h3><div class="chart" id="nbCap"></div><div class="legend-row" id="nbCapLeg"></div></div>
        <div class="panel span6"><h3><span><span class="g">▤</span>What the risk exposure is made of</span><small>OV1</small></h3><div id="nbOv1"></div></div>
        <div class="panel span6"><h3><span><span class="g">⌖</span>Where it lends</span><small>CCyB1</small></h3><div id="nbGeo"></div></div>
        <div class="panel span6"><h3><span><span class="g">◑</span>Loans by stage</span><small>CR1, gross carrying amount</small></h3><div id="nbStages"></div></div>
        <div class="panel span12"><h3><span><span class="g">∑</span>The models: IRB parameters by portfolio</span><small>CR6 · click a row for its PD grades</small></h3><div id="nbIrb"></div></div>
        <div class="panel span6"><h3><span><span class="g">≈</span>Interest-rate risk in the banking book</span><small>IRRBB1, the six supervisory shocks</small></h3><div id="nbRate"></div></div>
        <div class="panel span6"><h3><span><span class="g">▥</span>Own funds: from equity to CET1</span><small>CC1</small></h3><div id="nbCc1"></div></div>
        <div class="panel span12"><h3><span><span class="g">!</span>What we fixed in this bank's filings</span><small>so every figure can be traced</small></h3><div id="nbNotes"></div></div>
      </div>`;
    try { capChart(M, dates); } catch (e) { console.error(e); }
    try { ov1(M, dates, cur); } catch (e) { console.error(e); }
    try { geo(lei); } catch (e) { console.error(e); }
    try { stages(cq || []); } catch (e) { console.error(e); }
    try { irbTable(irb || []); } catch (e) { console.error(e); }
    try { rate(M, dates, cur); } catch (e) { console.error(e); }
    try { cc1(M, dates); } catch (e) { console.error(e); }
    try { notes(met || [], fixes || [], scale || []); } catch (e) { console.error(e); }
  }

  // capital ratios over the filed quarters, with the CET1 requirement as a threshold line
  function capChart(M, dates) {
    const host = $('#nbCap');
    const pts = dates.map((d) => ({ d, cet1: val(M[d].cet1_ratio), tot: val(M[d].total_ratio),
      req: ok(val(M[d].p2r_cet1)) && ok(val(M[d].cbr)) ? 0.045 + val(M[d].p2r_cet1) + val(M[d].cbr) : null })).filter((p) => ok(p.cet1) || ok(p.tot));
    if (!pts.length) { host.innerHTML = '<div class="empty">No KM1 ratios filed</div>'; return; }
    const W = host.clientWidth || 560, H = 210, L = 42, R = 64, T = 12, B = 26;
    const all = pts.flatMap((p) => [p.cet1, p.tot, p.req]).filter(ok);
    const lo = Math.max(0, Math.floor((Math.min(...all) - 0.01) * 50) / 50), hi = Math.ceil((Math.max(...all) + 0.01) * 50) / 50;
    const x = (i) => pts.length === 1 ? (L + W - R) / 2 : L + i / (pts.length - 1) * (W - L - R);
    const y = (v) => T + (1 - (v - lo) / (hi - lo || 1)) * (H - T - B);
    let s = `<svg viewBox="0 0 ${W} ${H}" role="img" aria-label="Capital ratios by quarter">`;
    for (let t = lo; t <= hi + 1e-9; t += (hi - lo) / 4) s += `<line x1="${L}" x2="${W - R}" y1="${y(t)}" y2="${y(t)}" stroke="var(--grid)"/><text x="${L - 6}" y="${y(t) + 3}" text-anchor="end" font-size="9.5" fill="var(--text4)">${nb(t * 100, 0)} %</text>`;
    const line = (key, color, dash) => {
      const p = pts.map((q, i) => ok(q[key]) ? [x(i), y(q[key])] : null).filter(Boolean);
      if (!p.length) return '';
      return `<path d="M${p.map((q) => q.join(',')).join(' L')}" fill="none" stroke="${color}" stroke-width="2"${dash ? ' stroke-dasharray="4 3"' : ''}/>`
        + p.map((q) => `<circle cx="${q[0]}" cy="${q[1]}" r="3.2" fill="${color}" stroke="#0b0f17" stroke-width="2"/>`).join('');
    };
    s += line('req', 'var(--text3)', true) + line('tot', 'var(--s2)') + line('cet1', 'var(--s1)');
    pts.forEach((p, i) => { if (pts.length <= 8 || i % 2 === 0 || i === pts.length - 1) s += `<text x="${x(i)}" y="${H - 8}" text-anchor="middle" font-size="9.5" fill="var(--text4)">${dl(p.d)}</text>`; });
    const lp = pts[pts.length - 1], lx = x(pts.length - 1) + 8;
    if (ok(lp.cet1)) s += `<text x="${lx}" y="${y(lp.cet1) + 3}" font-size="10" fill="var(--text2)">${pct(lp.cet1)}</text>`;
    if (ok(lp.tot)) s += `<text x="${lx}" y="${y(lp.tot) + 3}" font-size="10" fill="var(--text2)">${pct(lp.tot)}</text>`;
    host.innerHTML = s + '</svg>';
    $('#nbCapLeg').innerHTML = `<span><i style="background:var(--s1)"></i>CET1 ratio</span><span><i style="background:var(--s2)"></i>Total capital ratio</span>
      <span><i style="background:var(--text3)"></i>CET1 requirement (4.5 % + Pillar 2 + buffers), dashed</span>`;
  }
  function ov1(M, dates, cur) {
    const host = $('#nbOv1');
    const d = [...dates].reverse().find((x) => M[x].rwea_total || M[x].rwea_credit);
    if (!d) { host.innerHTML = '<div class="empty">No OV1 filed</div>'; return; }
    const g = (m) => M[d][m] ? +M[d][m].value_eur : 0;
    const tot = g('rwea_total') || sum(['rwea_credit', 'rwea_ccr', 'rwea_cva', 'rwea_market', 'rwea_op', 'rwea_securitisation'].map(g));
    const irb = g('rwea_credit_firb') + g('rwea_credit_airb') + g('rwea_slotting') + g('rwea_equity_irb');
    const parts = { rwea_credit_sa: g('rwea_credit_sa'), irb, rwea_op: g('rwea_op') };
    parts.addon = Math.max(0, g('rwea_credit') - parts.rwea_credit_sa - irb);     // credit risk the approach rows don't explain
    if (parts.addon < 0.005 * tot) parts.addon = 0;
    parts.other = Math.max(0, tot - parts.rwea_credit_sa - irb - parts.addon - parts.rwea_op);
    const seg = OV1.filter(([k]) => parts[k] > 0).map(([k, n, c]) => `<i style="flex:${parts[k]};background:${c}" title="${esc(n)}: ${eur(parts[k])} (${pct(parts[k] / tot, 0)})"></i>`).join('');
    const rows = OV1.filter(([k]) => parts[k] > 0 || k !== 'addon').map(([k, n, c]) => `<div class="nb-row"><span><i style="background:${c}"></i>${esc(n)}</span><b>${eur(parts[k])}</b><span class="faint">${pct(parts[k] / (tot || 1), 0)}</span></div>`).join('');
    const ab = irb ? `F-IRB ${eur(g('rwea_credit_firb'))} · A-IRB ${eur(g('rwea_credit_airb'))}${g('rwea_slotting') ? ` · slotting ${eur(g('rwea_slotting'))}` : ''}` : 'No internal models for credit risk';
    const opMissing = !g('rwea_op') ? ' The filing leaves operational risk out of its rows; it is inside the total and so in the grey part.' : '';
    host.innerHTML = `<div class="nb-total">${eur(tot)} <small>at ${dlong(d)}</small></div><div class="nb-stack">${seg}</div>${rows}
      <p class="note">${ab}.${opMissing} Amounts in EUR at the ECB rate of the date${cur !== 'EUR' ? `; filed in ${cur}` : ''}.</p>`;
  }
  function geo(lei) {
    const host = $('#nbGeo');
    const rows = EXP.filter((r) => r.lei === lei);
    const ds = [...new Set(rows.map((r) => r.ref_date))].sort();
    const d = ds[ds.length - 1];
    if (!d) { host.innerHTML = '<div class="empty">No CCyB1 by country</div>'; return; }
    const at = rows.filter((r) => r.ref_date === d);
    const all = (at.find((r) => r.country === 'ALL') || {}).e;
    const by = NORDIC.map((c) => [c, (at.find((r) => r.country === c) || {}).e || 0]).filter(([, e]) => e > 0);
    const nord = sum(by.map((x) => x[1])), rest = all ? Math.max(0, all - nord) : 0;
    const items = [...by.sort((a, b) => b[1] - a[1]), ...(rest ? [['REST', rest]] : [])];
    const max = Math.max(...items.map((x) => x[1]));
    const tot = all || nord;
    host.innerHTML = items.map(([c, e]) => `<div class="nm-bar static"><span class="n">${c === 'REST' ? 'Outside the Nordics' : CNAME[c]}${c === home(lei) ? '<small>home</small>' : ''}</span>
      <span class="t"><i style="width:${(e / max * 100).toFixed(1)}%;background:${c === 'REST' ? 'var(--s-muted)' : c === home(lei) ? 'var(--s1)' : 'var(--s2)'}"></i></span>
      <span class="v">${pct(e / tot, 0)} <small>${bnv(e)}</small></span></div>`).join('')
      + `<p class="note">EUR bn, CCyB1 at ${dlong(d)}${(VIEW.byBank[lei] && NORDIC.map((c) => VIEW.market[c]).length) ? '. Market share where it lends: ' + by.map(([c, e]) => `${CNAME[c]} ${pct(e / (VIEW.market[c].total || 1))}`).join(' · ') : ''}.</p>`;
  }
  function stages(rows) {
    const host = $('#nbStages');
    const byD = {};
    for (const r of rows) ((byD[r.ref_date] ||= {})[r.sector + '.' + r.measure] = r);
    const ds = Object.keys(byD).sort();
    const d = ds.reverse().find((x) => byD[x]['loans.gross']);
    if (!d) { host.innerHTML = '<div class="empty">No CR1 filed</div>'; return; }
    const S = [['loans', 'All loans and advances'], ['households', 'Households'], ['nfc', 'Non-financial companies'], ['sme', '  of which SMEs']];
    const rate = (byD[d]['loans.gross'] || {}).eur_rate || 1;
    const out = S.map(([k, n]) => {
      const g = byD[d][k + '.gross'], a = byD[d][k + '.allowance'];
      if (!g) return '';
      const tot = (+g.performing || 0) + (+g.npe || 0); if (!tot) return '';
      const s1 = +g.s1 || 0, s2 = (+g.s2 || 0) + (+g.npe_s2 || 0), s3 = +g.s3 || 0, other = Math.max(0, tot - s1 - s2 - s3);
      const cov = (x, y) => a && y ? pct((+a[x] || 0) / y, x === 's1' ? 2 : 1) : '—';
      return `<div class="nb-st"><div class="nb-st-h"><span>${esc(n.trim())}</span><b>${eur(tot * rate)}</b></div>
        <div class="nb-stack thin"><i style="flex:${s1};background:var(--flat)" title="Stage 1 ${pct(s1 / tot)}"></i><i style="flex:${s2};background:var(--warm)" title="Stage 2 ${pct(s2 / tot)}"></i><i style="flex:${s3};background:var(--hot)" title="Stage 3 ${pct(s3 / tot, 2)}"></i>${other ? `<i style="flex:${other};background:#252c3a" title="Not in a stage (fair value)"></i>` : ''}</div>
        <div class="nb-st-v"><span>S1 <b>${pct(s1 / tot)}</b> · cover ${cov('s1', s1)}</span><span class="warm">S2 <b>${pct(s2 / tot)}</b> · cover ${cov('s2', +g.s2 || 0)}</span><span class="hot">S3 <b>${pct(s3 / tot, 2)}</b> · cover ${cov('s3', s3)}</span></div></div>`;
    }).join('');
    const noAllow = !byD[d]['loans.allowance'];
    host.innerHTML = out + `<p class="note">${dlong(d)}. Cover = accumulated impairment of the stage over its gross amount. Stage 2 includes the
      non-performing loans still in stage 2. Grey: loans at fair value, outside the impairment stages.${noAllow ? ' <b>No cover:</b> the impairment table this bank filed repeats its gross amounts, so it is left out (see the notes below).' : ''}</p>`;
  }
  function irbTable(rows) {
    const host = $('#nbIrb');
    const ds = [...new Set(rows.map((r) => r.ref_date))].sort();
    const d = ds[ds.length - 1];
    if (!d) { host.innerHTML = '<div class="empty">Standardised approach only: no internal models filed in CR6</div>'; return; }
    const at = rows.filter((r) => r.ref_date === d);
    const all = at.filter((r) => r.bucket === 'total' && r.exposure_class !== 'All exposure classes' && ok(r.ead) && r.ead > 0);
    const allEad = sum(all.map((r) => r.ead));
    const totals = all.filter((r) => r.ead >= 0.001 * allEad).sort((a, b) => b.ead - a.ead);     // classes under 0.1 % of EAD: noise
    const rate = (totals[0] || {}).eur_rate || 1;
    const maxD = Math.max(0.01, ...totals.map((r) => r.density || 0));
    const body = totals.map((r, i) => `<tr class="row${i === 0 ? ' open' : ''}" data-k="${esc(r.exposure_class + '|' + r.approach)}" tabindex="0">
      <td class="l">${esc(r.exposure_class)}<small>${r.approach === 'AIRB' ? 'A-IRB: own LGD and conversion factors' : r.approach === 'FIRB' ? 'F-IRB: supervisory LGD' : 'IRB'}</small></td>
      <td>${bnv(r.ead * rate)}</td><td>${ok(r.obligors) && r.obligors > 0 ? nb(r.obligors) : '—'}</td><td>${pct(r.pd, 2)}</td><td>${pct(r.lgd)}</td>
      <td>${ok(r.ccf) && r.off_bs ? pct(r.ccf, 0) : '—'}</td><td>${ok(r.maturity) ? nb(r.maturity, 1) : '—'}</td>
      <td class="l"><span class="nb-rw"><i style="width:${(Math.min(1, (r.density || 0) / maxD) * 100).toFixed(0)}%"></i></span>${pct(r.density)}</td>
      <td>${ok(r.el) ? bnv(r.el * rate, 2) : '—'}</td><td>${ok(r.provisions) ? bnv(Math.abs(r.provisions) * rate, 2) : '—'}</td>
      <td>${r.quality === 'suspect' ? '<span class="mk x" title="risk weight or LGD more than doubled or halved since the previous filing: shown as filed, left out of comparisons">!</span>'
        : r.quality !== 'filed' ? `<span class="mk q" title="${r.quality === 'derived' ? 'class subtotal rebuilt from the PD grades: the filed subtotal disagreed with its own grades' : 'ratios filed as percentages, divided by 100'}">${r.quality === 'derived' ? '~' : '×'}</span>` : ''}</td></tr>`).join('');
    host.innerHTML = `<div class="nb-irb"><div class="ut-wrap"><table class="ut nb-irb-t"><thead><tr><th class="l">Portfolio</th><th>EAD</th><th>Obligors</th><th>PD</th>
      <th>LGD</th><th>CCF</th><th>Maturity</th><th class="l">Risk weight</th><th>EL</th><th>Provisions</th><th></th></tr></thead><tbody>${body}</tbody></table></div>
      <div class="nb-pd"><div class="nb-pd-h" id="nbPdH"></div><div id="nbPd"></div></div></div>
      <p class="note">${dlong(d)}. EUR bn. PD and LGD are exposure-weighted averages including the defaulted grade; risk weight = risk-weighted exposure / EAD
        (the bank's own, before Norway's or Sweden's mortgage floors when those sit in Pillar 2 or an Article 458 add-on).</p>`;
    const pdChart = (key) => {
      const [cls, appr] = key.split('|');
      const sub = at.filter((r) => r.exposure_class === cls && r.approach === appr);
      const g = (b) => sub.find((r) => r.bucket === b);
      const bars = BUCKETS.map(([b, parts]) => {
        const r = g(b); let e = r && ok(r.ead) ? r.ead : null, pd = r ? r.pd : null;
        if (!ok(e) && parts.length) { const ps = parts.map(g).filter((x) => x && ok(x.ead)); e = ps.length ? sum(ps.map((x) => x.ead)) : null; pd = e ? sum(ps.map((x) => x.ead * (x.pd || 0))) / e : null; }
        return { b, e: e || 0, pd };
      });
      const tot = sum(bars.map((x) => x.e)) || 1, max = Math.max(...bars.map((x) => x.e / tot));
      $('#nbPdH').innerHTML = `<b>${esc(cls)}</b> <span class="faint">· share of EAD by PD grade</span>`;
      $('#nbPd').innerHTML = bars.map((x) => `<div class="nb-pdb" title="PD ${x.b === 'default' ? '100 %' : x.b.replace('-', '–') + ' %'}: ${pct(x.e / tot)} of EAD${ok(x.pd) && x.b !== 'default' ? `, average PD ${pct(x.pd, 2)}` : ''}">
        <span class="bb"><i style="height:${Math.max(1, x.e / tot / (max || 1) * 100).toFixed(0)}%;background:${x.b === 'default' ? 'var(--hot)' : 'var(--s1)'}"></i></span>
        <span class="bv">${pct(x.e / tot, 0)}</span><span class="bl">${x.b === 'default' ? 'def.' : x.b.split('-')[0].replace(/\.?0+$/, '') + '+'}</span></div>`).join('');
    };
    if (totals.length) pdChart(totals[0].exposure_class + '|' + totals[0].approach);
    const tb = host.querySelector('tbody');
    const pick = (tr) => { tb.querySelectorAll('tr').forEach((t) => t.classList.toggle('open', t === tr)); pdChart(tr.dataset.k); };
    tb.addEventListener('click', (ev) => { const tr = ev.target.closest('tr[data-k]'); if (tr) pick(tr); });
    tb.addEventListener('keydown', (ev) => { if (ev.key === 'Enter') { const tr = ev.target.closest('tr[data-k]'); if (tr) pick(tr); } });
  }
  function rate(M, dates, cur) {
    const host = $('#nbRate');
    const d = [...dates].reverse().find((x) => SHOCKS.some(([k]) => M[x]['irrbb_eve_' + k] || M[x]['irrbb_nii_' + k]));
    if (!d) { host.innerHTML = '<div class="empty">No IRRBB1 filed</div>'; return; }
    const g = (m) => M[d][m] ? +M[d][m].value_eur : null;
    const cet1 = (M[d].cet1 || {}).value_eur;
    const vals = SHOCKS.flatMap(([k]) => [g('irrbb_eve_' + k), g('irrbb_nii_' + k)]).filter(ok);
    const max = Math.max(1, ...vals.map(Math.abs));
    const col = (kind, title) => `<div class="nb-div"><div class="nb-div-h">${title}</div>` + SHOCKS.map(([k, n]) => {
      const v = g(`irrbb_${kind}_${k}`); if (!ok(v)) return `<div class="nb-dr"><span>${n}</span><span class="faint">—</span></div>`;
      const w = Math.abs(v) / max * 50;
      return `<div class="nb-dr"><span>${n}</span><span class="track"><i style="${v < 0 ? `right:50%;width:${w}%;background:var(--hot)` : `left:50%;width:${w}%;background:var(--s1)`}"></i></span>
        <span class="num">${v >= 0 ? '+' : '−'}${bnv(Math.abs(v), 2)}${cet1 ? ` <small>${nb(Math.abs(v) / cet1 * 100, 1)} %</small>` : ''}</span></div>`;
    }).join('') + '</div>';
    host.innerHTML = `<div class="nb-divs">${col('eve', 'Economic value of equity')}${col('nii', 'Net interest income, next 12 months')}</div>
      <p class="note">${dlong(d)}. EUR bn, a gain right (ice), a loss left (red); small figures are the change as a share of CET1. As the bank filed
        IRRBB1, in the scale of its capital filing.</p>`;
  }
  function cc1(M, dates) {
    const host = $('#nbCc1');
    const d = [...dates].reverse().find((x) => M[x].retained_earnings || M[x].cet1_cc1);
    if (!d) { host.innerHTML = '<div class="empty">No CC1 filed</div>'; return; }
    const g = (m) => M[d][m] ? +M[d][m].value_eur : 0;
    const before = g('cet1_before_adj'), parts = [['Share capital and premium', g('share_capital')], ['Retained earnings', g('retained_earnings')],
      ['OCI and other reserves', g('aoci_reserves')]];
    const known = sum(parts.map((p) => p[1]));
    if (before && Math.abs(before - known) > Math.abs(before) * 0.002) parts.push(['Profit, minorities, other', before - known]);
    const ded = g('cet1_deductions'), intang = g('intangibles_ded'), els = g('el_shortfall_ded');
    const rows = [...parts.map(([n, v]) => [n, v, 'var(--s1)']), ['Regulatory deductions', ded, 'var(--hot)']];
    const max = Math.max(1, ...rows.map((r) => Math.abs(r[1])), before);
    host.innerHTML = rows.map(([n, v, c]) => `<div class="nm-bar static"><span class="n">${esc(n)}</span>
      <span class="t"><i style="width:${(Math.abs(v) / max * 100).toFixed(1)}%;background:${c}"></i></span><span class="v">${v < 0 ? '−' : ''}${bnv(Math.abs(v), 2)}</span></div>`).join('')
      + `<div class="nb-row strong"><span>CET1 capital</span><b>${eur(g('cet1_cc1') || before + ded, 2)}</b><span></span></div>
      <p class="note">${dlong(d)}. EUR bn. Deductions include intangibles ${eur(Math.abs(intang), 2)}${els ? ` and the shortfall of provisions against
        IRB expected loss ${eur(Math.abs(els), 2)}` : ''}.</p>`;
  }
  function notes(met, fixes, scale) {
    const host = $('#nbNotes');
    const items = [];
    for (const s of scale) if (s.factor !== 1 || s.note) items.push(`<li><b>${dlong(s.ref_date)} · ${esc(s.module)}</b>: ${s.factor !== 1 ? `amounts multiplied by ${nb(s.factor)} (the filing's ${esc(s.anchor || 'size')} only makes sense in ${s.factor === 1e6 ? 'millions' : 'thousands'})` : ''}${s.note ? (s.factor !== 1 ? '; ' : '') + esc(s.note) : ''}</li>`);
    for (const m of met) if (m.quality !== 'filed' && m.note) items.push(`<li><b>${dlong(m.ref_date)} · KM1 ${esc(m.metric.replace(/_/g, ' '))}</b>: ${esc(m.note)}</li>`);
    for (const f of fixes) items.push(`<li><b>${dlong(f.ref_date)} · ${esc(f.template)}${f.item ? ' · ' + esc(f.item) : ''}</b>: ${esc(f.note)}</li>`);
    host.innerHTML = items.length ? `<ul class="nb-notes">${items.slice(0, 60).join('')}</ul>${items.length > 60 ? `<p class="note">and ${items.length - 60} more</p>` : ''}`
      : '<p class="note">Nothing: every figure on this page is as the bank filed it.</p>';
  }

  // ── start ────────────────────────────────────────────────────────────────────────────────────────────────────────
  async function init() {
    if (!$('#nordic')) return;
    const okLoad = await load();
    if (!okLoad) { $('#nmShares').innerHTML = '<div class="empty">Nordic data unavailable</div>'; return; }
    select();
    renderKpis(); bindControls();
    try { await drawMap(); } catch (e) { console.error(e); }
    renderShares(); renderLeague();
    const first = leagueRows().sort((a, b) => (b.exp || 0) - (a.exp || 0))[0];
    const want = (location.hash.match(/bank=([A-Z0-9]{20})/) || [])[1];
    if (want && BANKS[want]) setBank(want); else if (first) { ST.bank = first.lei; renderBank(first.lei); paintMap(); renderShares(); renderLeague(); }
    let rt; window.addEventListener('resize', () => { clearTimeout(rt); rt = setTimeout(() => { drawMap(); }, 250); });
    const pill = $('#pillNordic'); if (pill) pill.innerHTML = `<b>${Object.values(BANKS).filter((b) => b.nordic).length}</b>&nbsp;Nordic banks · EBA`;
  }
  window.BankNordic = { init, state: ST };
  if (document.readyState === 'loading') document.addEventListener('DOMContentLoaded', init); else init();
})();
