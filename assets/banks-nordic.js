/* Banks · Nordic — every Nordic bank's Pillar 3, as filed to the EBA (2026-11-11).
   The Nordic map (who lends where: each banking group's credit exposure by country from its CCyB1 template, cross-border
   lending as arcs, countries shaded by market size, concentration, foreign lenders or mortgage risk weights), market
   shares per country and for the Nordics, the Nordic league table and one bank's profile (capital over time, risk
   exposure by type, where it lends, stages 1–3 and coverage, IRB parameters with the PD distribution, deposits, rate
   risk, own funds) with the data-quality notes behind every figure — and the home markets: every bank's share of lending
   and deposits in Norway, Sweden, Denmark and Finland from the national statistics, with its history.
   Data: ENEXT api.nordic_banks, api.nordic_exposure, api.nordic_metrics, api.nordic_credit_quality, api.nordic_irb,
   api.p3dh_entities, api.p3dh_submissions, api.p3dh_scale — the EBA Pillar 3 data hub, loaded and cleaned by
   research/banks/nordic.py and nordic_clean.py — and api.nordic_shares, api.nordic_share_dates from the national
   statistics (research/banks/national.py). Runbook: ../ENEXT/docs/banks-nordic.md.
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
    if (HOME.includes(c) && c !== NH.country) nhSet({ country: c });
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
      const rk = RS.byLei[b.lei], rq = rk && RS.q[rk] ? Object.keys(RS.q[rk]).sort().reverse()
        .find((p) => mval(RS.q[rk][p], 'return_on_equity_pct') != null || mval(RS.q[rk][p], 'cost_income_ratio_pct') != null) : null;
      out.push({
        roe: rq ? mval(RS.q[rk][rq], 'return_on_equity_pct') : null, ci: rq ? mval(RS.q[rk][rq], 'cost_income_ratio_pct') : null, rq,
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
    ['roe', 'ROE', (r) => r.roe], ['ci', 'C/I', (r) => r.ci],
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
      <td>${pct(r.s2)}</td><td>${pct(r.s3, 2)}</td><td>${pct(r.cov, 0)}</td><td>${r.mort ? pct(r.mort.density) : '—'}</td>
      <td title="${r.rq ? esc(qlab(r.rq)) : ''}">${ok(r.roe) ? nb(r.roe, 1) + ' %' : '—'}</td><td>${ok(r.ci) ? nb(r.ci, 0) + ' %' : '—'}</td></tr>`).join('');
    const th = COLS.map(([id, name]) => `<th data-s="${id}" class="${id === 'bank' || id === 'foot' ? 'l' : ''}${ST.sort === id ? ' on' : ''}">${name}${ST.sort === id ? (ST.dir < 0 ? ' ↓' : ' ↑') : ''}</th>`).join('');
    box.innerHTML = `<div class="seg-tabs nm-league-tabs">${chips}</div>
      <div class="ut-wrap"><table class="ut nm-ut"><thead><tr>${th}</tr></thead><tbody>${body}</tbody></table></div>
      <p class="note">EUR bn. Credit exposure: CCyB1, all countries, at the bank's latest disclosure; the bar splits it between the home country
        (ice), the other Nordic countries (amber) and the rest of the world (grey). CET1, leverage, LCR and NSFR: KM1 at the latest quarter.
        Above req.: CET1 ratio less 4.5 %, the CET1 part of Pillar 2 and the combined buffer (KM1 rows 5, EU 7e, 11): the distance to the
        MDA trigger. Stage 2 and 3: share of gross loans and advances (CR1), S3 cover: allowances on stage 3 over stage 3 loans. Mortgage RW: the
        IRB risk-weight density of residential mortgages (CR6). ROE and C/I: the latest quarterly report, as the bank states them (hover
        for the quarter). <span class="mk q">×</span> rescaled, <span class="mk q">~</span> derived or taken
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
        <div class="panel span12"><h3><span><span class="g">▦</span>Quarterly results</span><small>from its own reports, each read twice</small></h3><div id="nbRes"><div class="empty">Loading the quarters …</div></div></div>
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
    nbResults(lei).catch((e) => console.error(e));
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

  // ── home markets: the national statistics ────────────────────────────────────────────────────────────────────────
  // Every bank's lending and deposits in its home market, small savings banks and foreign branches included — what the
  // EBA hub above can't show. ENEXT api.nordic_shares / api.nordic_share_dates, built by research/banks/national.py from
  // SCB (SE), the Bank of Finland (FI), Bankenes sikringsfond + SSB + Norges Bank (NO), Finanstilsynet + Danmarks
  // Nationalbank (DK).
  const HOME = ['NO', 'SE', 'DK', 'FI'];
  const CUR = { NO: 'NOK', SE: 'SEK', DK: 'DKK', FI: 'EUR' };
  const HM = {                                       // measures per country (Norway has two groupings), in tab order
    NO: { bank: ['lend', 'lend_hh', 'lend_nfc', 'dep'], alliance: ['lend', 'lend_hh', 'lend_nfc', 'dep', 'dep_hh', 'dep_nfc'] },
    SE: { bank: ['lend', 'lend_hh', 'lend_housing', 'lend_nfc', 'dep', 'dep_hh', 'dep_nfc'] },
    DK: { bank: ['lend_mortgage', 'lend_bank', 'dep'] },
    FI: { bank: ['lend', 'lend_housing', 'lend_nfc', 'dep'] },
  };
  const HLAB = { lend: 'All', lend_hh: 'Households', lend_housing: 'Home loans', lend_nfc: 'Companies', lend_mortgage: 'Mortgage banks',
    lend_bank: 'Banks', dep: 'All', dep_hh: 'Households', dep_nfc: 'Companies' };
  const HLAB_NO = { lend_hh: 'Retail', lend_nfc: 'Corporate', dep_hh: 'Retail', dep_nfc: 'Corporate' };
  const HWHAT = {                                    // what the market is, under the total
    NO: { lend: 'gross lending to the retail and corporate market, mortgage companies included',
          lend_hh: 'lending to the retail market (wage earners, pensioners, students), mortgage companies included',
          lend_nfc: 'lending to the corporate market (companies, the self-employed, housing co-operatives)',
          dep: 'deposits from customers: households, companies and the public sector', dep_hh: 'deposits from the retail market',
          dep_nfc: 'deposits from the corporate market' },
    SE: { lend: 'lending to Swedish households and non-financial companies', lend_hh: 'lending to Swedish households',
          lend_housing: 'household loans secured on homes', lend_nfc: 'lending to Swedish non-financial companies',
          dep: 'deposits from Swedish households and non-financial companies', dep_hh: 'deposits from Swedish households',
          dep_nfc: 'deposits from Swedish non-financial companies' },
    DK: { lend_mortgage: 'lending by the Danish mortgage banks', lend_bank: 'loans by banks in Denmark to Danish borrowers',
          dep: 'deposits in banks in Denmark from Danish depositors' },
    FI: { lend: 'loans to the Finnish public (non-MFIs)', lend_housing: 'housing loans to Finnish households',
          lend_nfc: 'loans to Finnish companies, housing corporations excluded', dep: 'deposits from the Finnish public (non-MFIs)' },
  };
  const HDEF = {
    NO: { bank: `Each Norwegian bank's lending to the retail and corporate market — the loans it has moved to a mortgage company counted with it —
            and its customer deposits, from the quarterly figures every bank reports to Bankenes sikringsfond, against SSB's totals for all banks
            and mortgage companies in Norway. Foreign banks' branches (Nordea, Handelsbanken, Danske Bank …) don't report there and sit in
            <i>foreign branches and others</i>, with Santander Consumer, whose Norwegian business can't be told from its Nordic branches. DNB's
            parent-bank figures include its branches abroad and are scaled to its domestic share in Norges Bank's statistics (marked ~).`,
          alliance: `Norges Bank's own grouping of the Norwegian market (banking statistics, table 6): banks and mortgage companies by alliance,
            domestic business only, at year-ends, shares rounded to whole percent.` },
    SE: `Every institution's lending to and deposits from Swedish households and non-financial companies, as reported to SCB each month (the MFI
         statistics, table MFIM1). Banking groups include their mortgage and finance companies (Stadshypotek in Handelsbanken, Swedbank Hypotek in
         Swedbank …), foreign banks their Swedish branches. <i>Others</i> are the institutions SCB doesn't publish by name — smaller savings banks,
         finance companies, Kommuninvest; mortgage funds are the non-bank lenders that take no deposits.`,
    DK: `Mortgage banks: the six Danish mortgage banks' loans, from Finanstilsynet's accounts statistics at year-ends — the whole mortgage market,
         about three quarters of what Danish households and companies borrow. Banks: each Danish bank's loans and deposits against Danmarks
         Nationalbank's domestic totals. Danske Bank A/S books its Swedish, Norwegian and Finnish branches in the Danish entity, so its Danish
         banking and the foreign banks' branches (Nordea Danmark …) form the residual; bank figures are legal-entity balances and can include a
         little business abroad.`,
    FI: `The Bank of Finland's market-share report: each banking group's loans to and deposits from the Finnish public (non-MFIs), half-yearly
         since 2017, groups as the Bank of Finland draws them (OP with OP Mortgage Bank, Nordea with its mortgage bank and finance company);
         <i>others</i> as published.`,
  };
  const HSRC = {
    NO: [['Bankenes sikringsfond', 'https://www.bankenessikringsfond.no/for-foretakene/regnskapstall-for-norske-banker-fra-3-kvartal-2024/'],
         ['SSB 09560, 09565', 'https://www.ssb.no/statbank/table/09560/'], ['Norges Bank', 'https://www.norges-bank.no/en/topics/statistics/banking-statistics/']],
    SE: [['SCB, MFIM1', 'https://www.statistikdatabasen.scb.se/pxweb/sv/ssd/START__FM__FM0401__FM0401X/MFIM1/']],
    DK: [['Finanstilsynet', 'https://www.finanstilsynet.dk/tal-og-fakta/statistik/tidsserier'], ['Danmarks Nationalbank', 'https://www.statbank.dk/DNMUDL']],
    FI: [['Suomen Pankki', 'https://reports.suomenpankki.fi/WebForms/ReportViewerPage.aspx?report=%2ftilastot%2frahalaitosten_lainat_talletukset_ja_korot%2flainat%2fmarkkinaosuudet_luottolaitokset_fi&output=']],
  };
  // the dark steps of a validated eight-hue categorical palette; a group keeps its slot across measures and dates
  const PAL = ['#3987e5', '#d95926', '#199e70', '#c98500', '#d55181', '#008300', '#9085e9', '#e66767'];
  const SLOT = { nordea: 0, danske: 1, handelsbanken: 2, swedbank: 3, seb: 4, dnb: 5, sbab: 6, lansforsakringar: 7,
    'no:832554332': 6, 'no:937895321': 3, 'no:937901003': 4, 'no:920426530': 2, 'no:952706365': 7, 'no:937899319': 0,
    sb1: 3, eika: 4, other_savings: 6, other_commercial: 2, branches: 7,
    nykredit: 2, jyske: 3, dlr: 4, al_sydbank: 6, 'dk:9380': 5, 'dk:1149': 7, 'dk:9070': 0, 'dk:7670': 4,
    op: 3, munifin: 6, savings_banks_fi: 5, s_bank: 4, aktia: 2, pop: 7 };
  const NH = { country: 'NO', view: 'bank', measure: 'lend', date: null, cache: {}, color: {}, x: null, dragging: false };
  const SHARE_COLS = 'grp,name,lei,tag,value,total,share,n,quality,source,note';
  const hbase = () => `country=eq.${NH.country}&measure=eq.${NH.measure}&view=eq.${NH.view}`;
  const hkey = () => `${NH.country}|${NH.measure}|${NH.view}`;
  const money = (v, c) => !ok(v) ? '—' : `${CUR[c]} ${nb(v / 1000, Math.abs(v) < 1e4 ? 1 : 0)} bn`;     // v in millions
  const isRes = (r) => r.grp === '_other';

  async function nhData() {                          // per country × measure × view: the dates, the top groups' history
    const k = hkey();
    if (NH.cache[k]) return NH.cache[k];
    const dates = await get(`nordic_share_dates?${hbase()}&select=ref_date,total,groups,sources&order=ref_date`);
    if (!dates || !dates.length) return null;
    const last = dates[dates.length - 1].ref_date;
    const rows = await get(`nordic_shares?${hbase()}&ref_date=eq.${last}&select=${SHARE_COLS}&order=value.desc.nullslast`) || [];
    const top = rows.filter((r) => !isRes(r) && ok(r.share)).slice(0, 6).map((r) => r.grp);
    const keys = [...top, ...(rows.some(isRes) ? ['_other'] : [])];
    const hist = keys.length ? await get(`nordic_shares?${hbase()}&grp=${encodeURIComponent(`in.(${keys.map((g) => `"${g}"`).join(',')})`)}`
      + '&select=ref_date,grp,share,value&order=ref_date') : [];
    const names = Object.fromEntries(rows.map((r) => [r.grp, r.name]));
    return (NH.cache[k] = { dates, rows: { [last]: rows }, top, keys, hist: hist || [], names });
  }
  async function nhRowsAt(d, date) {
    if (!d.rows[date]) d.rows[date] = await get(`nordic_shares?${hbase()}&ref_date=eq.${date}&select=${SHARE_COLS}&order=value.desc.nullslast`) || [];
    return d.rows[date];
  }
  function nhColors(top) {                           // a group's own slot when it is free, else the first free one
    const used = new Set(), col = {};
    for (const g of top) {
      let s = SLOT[g];
      if (s == null || used.has(s)) s = PAL.findIndex((_, i) => !used.has(i));
      used.add(s); col[g] = PAL[s];
    }
    return col;
  }
  async function nhSet(patch) {
    const before = hkey();
    Object.assign(NH, patch);
    if (NH.country !== 'NO') NH.view = 'bank';
    const ms = HM[NH.country][NH.view] || HM[NH.country].bank;
    if (!ms.includes(NH.measure)) NH.measure = ms.includes(patch.measure) ? patch.measure : ms[0];
    if (hkey() !== before) NH.date = null;
    await nhRender();
  }

  async function nhRender() {
    const box = $('#nhPanel'); if (!box) return;
    const d = await nhData();
    const c = NH.country, ms = HM[c][NH.view] || HM[c].bank;
    const lab = (m) => (c === 'NO' && HLAB_NO[m]) || HLAB[m];
    const tabs = (pre) => ms.filter((m) => m.startsWith(pre)).map((m) =>
      `<button data-m="${m}" class="${m === NH.measure ? 'on' : ''}" aria-pressed="${m === NH.measure}">${lab(m)}</button>`).join('');
    const ctrl = `<div class="nh-ctrl">
        <div class="seg-tabs nh-c">${HOME.map((k) => `<button data-c="${k}" class="${k === c ? 'on' : ''}" aria-pressed="${k === c}">${CNAME[k]}</button>`).join('')}</div>
        <div class="grp"><span class="lt">Lending</span><div class="seg-tabs nh-m">${tabs('lend')}</div></div>
        <div class="grp"><span class="lt">Deposits</span><div class="seg-tabs nh-m">${tabs('dep')}</div></div>
        ${c === 'NO' ? `<div class="grp"><span class="lt">Grouped as</span><div class="seg-tabs nh-v">
          <button data-v="bank" class="${NH.view === 'bank' ? 'on' : ''}" aria-pressed="${NH.view === 'bank'}">Banks · quarterly</button>
          <button data-v="alliance" class="${NH.view === 'alliance' ? 'on' : ''}" aria-pressed="${NH.view === 'alliance'}">Alliances · Norges Bank</button></div></div>` : ''}
      </div>`;
    if (!d) { box.innerHTML = ctrl + '<div class="empty">No national statistics for this selection yet</div>'; nhBind(box); return; }
    NH.color = nhColors(d.top);
    const dates = d.dates.map((x) => x.ref_date);
    const def = typeof HDEF[c] === 'string' ? HDEF[c] : HDEF[c][NH.view];
    const src = HSRC[c].map(([t, u]) => `<a href="${u}" target="_blank" rel="noopener">${esc(t)}</a>`).join(' · ');
    box.innerHTML = `${ctrl}
      <div class="nh-kpis" id="nhKpis"></div>
      <div class="nh-body">
        <div class="nh-left"><div class="nm-bars" id="nhBars"></div></div>
        <div class="nh-right">
          <div class="nh-ch-h">Share of the market over time <small>${esc(dl(dates[0]))} – ${esc(dl(dates[dates.length - 1]))} · ${dates.length} ${
            c === 'SE' ? 'months' : c === 'FI' ? 'half-years' : c === 'DK' || NH.view === 'alliance' ? 'year-ends' : 'quarters'} · click to pick a date</small></div>
          <div class="nh-chart" id="nhChart"></div>
          ${dates.length > 1 ? `<div class="nh-slide"><span>${esc(dl(dates[0]))}</span>
            <input type="range" id="nhSlide" min="0" max="${dates.length - 1}" step="1" value="${dates.length - 1}" aria-label="Reference date">
            <span>${esc(dl(dates[dates.length - 1]))}</span></div>` : ''}
        </div>
      </div>
      <p class="note">${def} Sources: ${src}. <span id="nhNotes"></span></p>`;
    nhBind(box);
    nhChart($('#nhChart'), d);
    const sl = $('#nhSlide');
    if (sl) {
      let t;
      sl.addEventListener('input', () => { clearTimeout(t); NH.date = dates[+sl.value]; t = setTimeout(() => nhPaint(d), 90); });
    }
    await nhPaint(d);
  }
  function nhBind(box) {
    box.querySelector('.nh-c').addEventListener('click', (ev) => { const b = ev.target.closest('button'); if (b) nhSet({ country: b.dataset.c }); });
    for (const el of box.querySelectorAll('.nh-m')) el.addEventListener('click', (ev) => { const b = ev.target.closest('button'); if (b) nhSet({ measure: b.dataset.m }); });
    const v = box.querySelector('.nh-v');
    if (v) v.addEventListener('click', (ev) => { const b = ev.target.closest('button'); if (b) nhSet({ view: b.dataset.v }); });
  }

  async function nhPaint(d) {                        // the bars, the readout and the chart's marker at NH.date
    const c = NH.country, dates = d.dates.map((x) => x.ref_date);
    const date = NH.date && dates.includes(NH.date) ? NH.date : dates[dates.length - 1];
    const rows = await nhRowsAt(d, date);
    if (date !== (NH.date && dates.includes(NH.date) ? NH.date : dates[dates.length - 1])) return;   // a newer pick won
    const sl = $('#nhSlide'); if (sl && +sl.value !== dates.indexOf(date)) sl.value = dates.indexOf(date);
    const named = rows.filter((r) => !isRes(r) && ok(r.share));
    const res = rows.find(isRes);
    const meta = d.dates.find((x) => x.ref_date === date) || {};
    const total = +meta.total;
    const yearAgo = new Date(Date.parse(date) - 365.25 * 864e5);
    const prev = d.dates.filter((x) => Math.abs(Date.parse(x.ref_date) - yearAgo) < 46 * 864e5).pop();
    const yoy = prev && +prev.total ? total / prev.total - 1 : null;
    const sh = named.map((r) => +r.share).sort((a, b) => b - a);
    const cr3 = sum(sh.slice(0, 3)), hhi = sum(sh.map((s) => s * s * 1e4));
    const inst = sum(named.map((r) => r.n || 1));
    $('#nhKpis').innerHTML = `<div><div class="big">${money(total, c)}</div>
        <div class="k">${esc(HWHAT[c][NH.measure] || '')} · ${dlong(date)}</div></div>
      ${ok(yoy) ? `<div class="k">growth on the year <b class="${yoy >= 0 ? '' : 'hot'}">${yoy >= 0 ? '+' : '−'}${nb(Math.abs(yoy) * 100, 1)} %</b></div>` : ''}
      <div class="k">three largest <b>${pct(cr3, 0)}</b></div>
      <div class="k">HHI <b>${nb(hhi)}</b></div>
      <div class="k">${res ? `named ${NH.view === 'alliance' || c === 'FI' ? 'groups' : 'institutions'} cover <b>${pct(1 - +res.share, 0)}</b>` : `<b>${named.length}</b> groups`}${
        c !== 'FI' && NH.view !== 'alliance' ? ` · <b>${nb(inst)}</b> institutions` : ''}</div>`;
    // bars: the twelve largest, the rest folded, the residual last
    const TOP = 15, list = named.slice(0, TOP), more = named.slice(TOP);
    const max = Math.max(0.01, ...[...list, res].filter(Boolean).map((r) => +r.share));
    const bar = (r, o = {}) => {
      const s = +r.share, link = r.lei && BANKS[r.lei], col = NH.color[r.grp];
      return `<div class="nm-bar${o.res ? ' res' : ''}${link ? '' : ' static'}${link && ST.bank === r.lei ? ' on' : ''}" data-g="${esc(r.grp)}"${
        link ? ` data-lei="${r.lei}" tabindex="0" role="button"` : ''}>
        <span class="n">${col ? `<i class="sw" style="background:${col}"></i>` : '<i class="sw"></i>'}${esc(o.label || r.name)}${
          r.tag && !String(r.name).toLowerCase().includes(r.tag.toLowerCase()) ? `<small>${esc(r.tag)}</small>` : ''}${
          r.quality === 'derived' ? '<small title="adjusted: see the note below">~</small>' : ''}</span>
        <span class="t"><i style="width:${Math.max(0.6, s / max * 100).toFixed(1)}%"></i></span>
        <span class="v">${pct(s)} <small>${nb(r.value / 1000, Math.abs(r.value) < 1e4 ? 1 : 0)}</small></span></div>`;
    };
    const mv = sum(more.map((r) => +r.value)), msh = sum(more.map((r) => +r.share));
    $('#nhBars').innerHTML = list.map((r) => bar(r)).join('')
      + (more.length ? bar({ grp: '_more', name: `${more.length} smaller ${c === 'FI' || NH.view === 'alliance' ? 'groups' : 'institutions'}`, share: msh, value: mv }) : '')
      + (res ? bar(res, { res: true }) : '')
      + `<div class="nh-unit">${CUR[c]} bn · share of ${esc(HWHAT[c][NH.measure] || 'the market')}</div>`;
    const notes = [...new Set(rows.filter((r) => r.note && r.quality === 'derived').map((r) => `${r.name}: ${r.note}.`))];
    $('#nhNotes').textContent = notes.join(' ');
    const bars = $('#nhBars');
    bars.onclick = (ev) => { const b = ev.target.closest('[data-lei]'); if (b) setBank(b.dataset.lei, true); };
    bars.onkeydown = (ev) => { if (ev.key !== 'Enter') return; const b = ev.target.closest('[data-lei]'); if (b) setBank(b.dataset.lei, true); };
    bars.onmousemove = (ev) => {
      const b = ev.target.closest('[data-g]'); if (!b) { tip(null); return; }
      const g = b.dataset.g, r = g === '_more' ? null : rows.find((x) => x.grp === g);
      if (g === '_more') { tip(`<b>${more.length} smaller institutions</b><div class="tm">${more.slice(0, 14).map((x) => `${esc(x.name)} ${pct(+x.share, 2)}`).join(' · ')}${more.length > 14 ? ' …' : ''}</div>`, ev); return; }
      if (!r) { tip(null); return; }
      tip(`<b>${esc(r.name)}</b>${r.tag ? ` <span class="faint">· ${esc(r.tag)}</span>` : ''}
        <table class="tg"><tr><td>share</td><td>${pct(+r.share, 2)}</td></tr><tr><td>${CUR[c]}</td><td>${nb(r.value / 1000, 1)} bn</td></tr>
        ${r.n > 1 ? `<tr><td>institutions</td><td>${r.n}</td></tr>` : ''}</table>
        ${r.note ? `<div class="tm">${esc(r.note)}</div>` : ''}${r.lei && BANKS[r.lei] ? '<div class="tm">Click for the bank profile</div>' : ''}`, ev);
    };
    bars.onmouseleave = () => tip(null);
    if (NH.x) {                                      // move the chart's date marker
      const m = $('#nhChart .cur');
      if (m) { const x = NH.x(new Date(date + 'T00:00:00Z')); m.setAttribute('x1', x); m.setAttribute('x2', x); }
    }
  }

  function nhChart(host, d) {                        // the top groups' shares over time, one line each, the residual dashed
    if (!host) return;
    if (!window.d3) { host.innerHTML = ''; return; }
    const W = Math.max(300, host.clientWidth || 600), narrow = W < 520, H = narrow ? 240 : 300;
    const m = { t: 10, r: narrow ? 92 : 148, b: 24, l: 40 };
    const series = d.keys.map((g) => ({ g, pts: d.hist.filter((h) => h.grp === g && ok(h.share))
      .map((h) => ({ t: new Date(h.ref_date + 'T00:00:00Z'), v: +h.share, d: h.ref_date })) })).filter((s) => s.pts.length);
    const all = series.flatMap((s) => s.pts);
    if (!all.length) { host.innerHTML = ''; return; }
    const ext = d3.extent(all, (p) => p.t);
    if (+ext[0] === +ext[1]) { ext[0] = new Date(+ext[0] - 180 * 864e5); ext[1] = new Date(+ext[1] + 180 * 864e5); }
    const x = d3.scaleUtc().domain(ext).range([m.l, W - m.r]);
    const y = d3.scaleLinear().domain([0, d3.max(all, (p) => p.v) * 1.08]).nice().range([H - m.b, m.t]);
    NH.x = x;
    const col = (g) => g === '_other' ? 'var(--text4)' : NH.color[g] || 'var(--text3)';
    const svg = d3.create('svg').attr('viewBox', `0 0 ${W} ${H}`).attr('role', 'img')
      .attr('aria-label', `Market shares in ${CNAME[NH.country]} over time`);
    svg.append('g').attr('class', 'gl').selectAll('line').data(y.ticks(5)).join('line')
      .attr('x1', m.l).attr('x2', W - m.r).attr('y1', (v) => y(v)).attr('y2', (v) => y(v));
    svg.append('g').attr('class', 'ax').attr('transform', `translate(0,${H - m.b})`)
      .call(d3.axisBottom(x).ticks(Math.max(2, Math.min(8, Math.floor((W - m.l - m.r) / 80)))).tickSizeOuter(0));
    svg.append('g').attr('class', 'ax').attr('transform', `translate(${m.l},0)`)
      .call(d3.axisLeft(y).ticks(5).tickSize(0).tickPadding(6).tickFormat((v) => `${Math.round(v * 100)} %`))
      .call((g) => g.select('.domain').remove());
    const line = d3.line().x((p) => x(p.t)).y((p) => y(p.v));
    for (const s of [...series].reverse()) {         // the largest drawn last, on top
      if (s.pts.length > 1) svg.append('path').attr('class', 'ln' + (s.g === '_other' ? ' res' : '')).attr('d', line(s.pts)).attr('stroke', col(s.g));
      if (s.pts.length <= 12) svg.append('g').selectAll('circle').data(s.pts).join('circle')
        .attr('cx', (p) => x(p.t)).attr('cy', (p) => y(p.v)).attr('r', 3).attr('fill', col(s.g)).attr('stroke', 'var(--bg0)').attr('stroke-width', 1.5);
    }
    // direct labels at the line ends, nudged apart
    const ends = series.map((s) => { const p = s.pts[s.pts.length - 1]; return { g: s.g, v: p.v, x: x(p.t), y: y(p.v) }; }).sort((a, b) => a.y - b.y);
    for (let i = 1; i < ends.length; i++) if (ends[i].y - ends[i - 1].y < 12) ends[i].y = ends[i - 1].y + 12;
    const over = ends.length ? ends[ends.length - 1].y - (H - m.b - 2) : 0;
    if (over > 0) for (const e of ends) e.y -= over;
    const lbl = (g) => { const n = d.names[g] || (g === '_other' ? 'Others' : g); return n.length > (narrow ? 11 : 19) ? n.slice(0, narrow ? 10 : 18) + '…' : n; };
    svg.append('g').selectAll('text').data(ends).join('text').attr('class', 'lab')
      .attr('x', (e) => e.x + 7).attr('y', (e) => e.y + 3.5).attr('fill', 'var(--text2)')
      .text((e) => `${lbl(e.g)} ${nb(e.v * 100, 0)} %`);
    svg.append('g').selectAll('rect').data(ends).join('rect').attr('x', (e) => e.x + 1).attr('y', (e) => e.y - 1).attr('width', 4).attr('height', 2).attr('fill', (e) => col(e.g));
    svg.append('line').attr('class', 'cur').attr('y1', m.t).attr('y2', H - m.b).attr('x1', W - m.r).attr('x2', W - m.r);
    const hov = svg.append('line').attr('class', 'hov').attr('y1', m.t).attr('y2', H - m.b).style('display', 'none');
    const dates = d.dates.map((q) => q.ref_date), dts = dates.map((q) => +new Date(q + 'T00:00:00Z'));
    const near = (px) => { const t = +x.invert(px); let b = 0; for (let i = 1; i < dts.length; i++) if (Math.abs(dts[i] - t) < Math.abs(dts[b] - t)) b = i; return b; };
    svg.append('rect').attr('x', m.l).attr('y', m.t).attr('width', W - m.l - m.r).attr('height', H - m.t - m.b).attr('fill', 'transparent')
      .style('cursor', 'crosshair')
      .on('mousemove', (ev) => {
        const [px] = d3.pointer(ev), i = near(px), dd = dates[i], xx = x(new Date(dd + 'T00:00:00Z'));
        hov.style('display', null).attr('x1', xx).attr('x2', xx);
        const vals = series.map((s) => ({ g: s.g, p: s.pts.find((p) => p.d === dd) })).filter((o) => o.p).sort((a, b) => b.p.v - a.p.v);
        tip(`<b>${dlong(dd)}</b><table class="tg">${vals.map((o) => `<tr><td><i style="background:${col(o.g)}"></i>${esc(d.names[o.g] || o.g)}</td><td>${pct(o.p.v)}</td></tr>`).join('')}</table>
          <div class="tm">Market ${money(+d.dates[i].total, NH.country)}</div>`, ev);
      })
      .on('mouseleave', () => { hov.style('display', 'none'); tip(null); })
      .on('click', (ev) => { const [px] = d3.pointer(ev); NH.date = dates[near(px)]; nhPaint(d); });
    host.innerHTML = '';
    host.appendChild(svg.node());
  }

  // ── the reporting season: every Nordic bank's quarterly report ───────────────────────────────────────────────────
  // ENEXT api.nordic_report_feed, api.nordic_quarters, api.nordic_report_upcoming: the Nasdaq banks' reports read by
  // research/banks/nordic_reports.py and Norway's 30 listed banks' by reader.py — two DeepSeek readings each, a figure
  // published when they agree, checked against the report's own totals.
  const RS = { country: 'ALL', n: 25, feed: [], q: {}, up: [], byLei: {} };
  const RM = ['net_interest_income', 'net_commission_income', 'total_income', 'operating_expenses', 'impairment_losses',
    'pre_tax_profit', 'net_profit', 'loans_to_customers', 'deposits_from_customers', 'return_on_equity_pct',
    'cost_income_ratio_pct', 'net_interest_margin_pct', 'cet1_ratio_pct'];
  const prevYear = (p) => `${+p.slice(0, 4) - 1}${p.slice(4)}`;
  const qlab = (p) => p ? `Q${p.slice(5)} ${p.slice(0, 4)}` : '—';
  const mval = (v, m) => v && v[m] && ok(v[m].value) ? +v[m].value : null;
  const growth = (a, b) => ok(a) && ok(b) && b !== 0 ? (a - b) / Math.abs(b) : null;
  const lossBp = (v) => { const i = mval(v, 'impairment_losses'), l = mval(v, 'loans_to_customers'); return ok(i) && l ? 4 * i / l * 1e4 : null; };
  const mm = (v) => !ok(v) ? '—' : nb(v, Math.abs(v) < 100 ? 1 : 0);
  const sgn = (g, dp = 0) => !ok(g) ? '<span class="faint">—</span>'
    : `<span class="${g < -0.1 ? 'hot' : g < 0 ? 'warm' : ''}">${g >= 0 ? '+' : '−'}${nb(Math.abs(g) * 100, dp)} %</span>`;
  const mark = (r) => r && r.status && r.status !== 'agreed' && ok(r.value) ? `<span class="mk q" title="${esc(r.status === 'derived'
    ? 'derived: the year to date less the previous quarter\'s' : r.status === 'disputed' ? 'the two readings disagree: the careful one is shown' : 'read once so far')}">~</span>` : '';

  async function rsLoad() {
    const since = new Date(Date.now() - 400 * 864e5).toISOString().slice(0, 10);
    const from = `${new Date().getUTCFullYear() - 2}Q1`;
    const [feed, q, up] = await Promise.all([
      get(`nordic_report_feed?published=gte.${since}&select=key,lei,short,country,period,published,headline,url,src&order=published.desc&limit=800`),
      get(`nordic_quarters?period=gte.${from}&metric=in.(${RM.join(',')})&select=key,lei,currency,period,metric,value,status`),
      get('nordic_report_upcoming?select=key,lei,short,country,planned,src&order=planned&limit=120'),
    ]);
    if (!feed) return false;
    RS.feed = feed; RS.up = (up || []).filter((u) => u.planned);
    RS.q = {};
    for (const r of q || []) {
      ((RS.q[r.key] ||= {})[r.period] ||= {})[r.metric] = r;
      if (r.lei) RS.byLei[r.lei] = r.key;
    }
    return true;
  }
  function rsSeason() {                              // the newest quarter a third of the banks have reported
    const byP = {};
    for (const [k, per] of Object.entries(RS.q)) for (const [p, v] of Object.entries(per)) if (v.net_profit || v.total_income) (byP[p] ||= []).push(k);
    const n = Object.keys(RS.q).length;
    const p = Object.keys(byP).sort().reverse().find((x) => byP[x].length >= n / 3);
    if (!p) return null;
    const med = (xs) => { const a = xs.filter(ok).sort((x, y) => x - y); return a.length ? a[Math.floor(a.length / 2)] : null; };
    const ks = byP[p];
    const roe = med(ks.map((k) => mval(RS.q[k][p], 'return_on_equity_pct')));
    const nii = med(ks.map((k) => growth(mval(RS.q[k][p], 'net_interest_income'), mval((RS.q[k] || {})[prevYear(p)], 'net_interest_income'))));
    const prof = med(ks.map((k) => growth(mval(RS.q[k][p], 'net_profit'), mval((RS.q[k] || {})[prevYear(p)], 'net_profit'))));
    const ci = med(ks.map((k) => mval(RS.q[k][p], 'cost_income_ratio_pct')));
    const loss = med(ks.map((k) => lossBp(RS.q[k][p])));
    const expected = Object.values(RS.q).filter((per) => per[prevYear(p)] || Object.keys(per).some((x) => x < p && x >= prevYear(p))).length;
    return { p, reported: ks.length, expected: Math.max(expected, ks.length), roe, nii, prof, ci, loss };
  }

  function rsRender() {
    const box = $('#rsFeed'); if (!box) return;
    const s = rsSeason();
    const C = ['ALL', ...NORDIC];
    const tabs = C.map((k) => `<button data-c="${k}" class="${RS.country === k ? 'on' : ''}" aria-pressed="${RS.country === k}">${k === 'ALL' ? 'All countries' : CNAME[k]}</button>`).join('');
    const items = RS.feed.filter((it) => (RS.country === 'ALL' || it.country === RS.country) && (RS.q[it.key] || {})[it.period]);
    const rows = items.slice(0, RS.n).map((it) => {
      const v = RS.q[it.key][it.period], w = (RS.q[it.key] || {})[prevYear(it.period)];
      const cur = (v.net_profit || v.total_income || v.net_interest_income || {}).currency || '';
      const link = it.lei && BANKS[it.lei];
      const bp = lossBp(v);
      const pub = String(it.published).slice(0, 10);
      const day = new Date(pub + 'T00:00:00Z').toLocaleString('en-GB', { day: 'numeric', month: 'short', timeZone: 'UTC',
        ...(pub.slice(0, 4) !== String(new Date().getUTCFullYear()) ? { year: '2-digit' } : {}) });
      return `<tr class="row${link ? '' : ' lite'}" data-key="${esc(it.key)}"${it.lei ? ` data-lei="${it.lei}"` : ''} tabindex="0">
        <td class="l nowrap opt2">${esc(day)} <small class="faint">Q${it.period.slice(5)}${it.period.slice(0, 4) !== pub.slice(0, 4) ? ' ' + it.period.slice(2, 4) : ''}</small></td>
        <td class="l bn">${esc(it.short)} <small class="faint">${esc(it.country)}</small></td>
        <td>${mm(mval(v, 'net_profit'))}${mark(v.net_profit)} <small class="faint">${esc(cur)}</small></td><td>${sgn(growth(mval(v, 'net_profit'), mval(w, 'net_profit')))}</td>
        <td class="opt">${mm(mval(v, 'net_interest_income'))}${mark(v.net_interest_income)}</td><td class="opt">${sgn(growth(mval(v, 'net_interest_income'), mval(w, 'net_interest_income')))}</td>
        <td class="opt">${ok(bp) ? nb(bp, 0) : '—'}</td>
        <td>${ok(mval(v, 'return_on_equity_pct')) ? nb(mval(v, 'return_on_equity_pct'), 1) + ' %' : '—'}${mark(v.return_on_equity_pct)}</td>
        <td class="opt">${ok(mval(v, 'cost_income_ratio_pct')) ? nb(mval(v, 'cost_income_ratio_pct'), 0) + ' %' : '—'}</td>
        <td class="opt">${ok(mval(v, 'cet1_ratio_pct')) ? nb(mval(v, 'cet1_ratio_pct'), 1) + ' %' : '—'}</td>
        <td><a class="dlink" href="${esc(it.url || '#')}" target="_blank" rel="noopener" title="${esc(it.headline || 'the report')}">↗</a></td></tr>`;
    }).join('');
    const head = s ? `<div class="rs-season"><div><div class="big">${qlab(s.p)}</div>
        <div class="k"><b>${s.reported}</b> of ${s.expected} banks have reported</div></div>
        <div class="k">median net profit <b>${sgn(s.prof)}</b> on the year</div>
        <div class="k">net interest income <b>${sgn(s.nii)}</b></div>
        <div class="k">return on equity <b>${ok(s.roe) ? nb(s.roe, 1) + ' %' : '—'}</b></div>
        <div class="k">cost/income <b>${ok(s.ci) ? nb(s.ci, 0) + ' %' : '—'}</b></div>
        <div class="k">credit losses <b>${ok(s.loss) ? nb(s.loss, 0) + ' bp' : '—'}</b> of loans</div></div>` : '';
    box.innerHTML = `${head}<div class="seg-tabs rs-tabs">${tabs}</div>
      <div class="ut-wrap"><table class="ut rs-ut"><thead><tr><th class="l opt2">Out · quarter</th><th class="l">Bank</th>
        <th>Net profit, m</th><th>y/y</th><th class="opt">NII, m</th><th class="opt">y/y</th><th class="opt">Losses, bp</th><th>ROE</th><th class="opt">C/I</th><th class="opt">CET1</th><th></th></tr></thead>
        <tbody>${rows || '<tr><td colspan="11" class="faint">No reports yet</td></tr>'}</tbody></table></div>
      ${items.length > RS.n ? `<button class="rs-more" type="button">${items.length - RS.n} more reports</button>` : ''}
      <p class="note">The quarter alone, in millions of the bank's own currency; y/y against the same quarter a year earlier. NII: net interest
        income. Losses: credit
        losses for the quarter, annualised, in basis points of loans. ROE, cost/income and CET1 as each bank reports them. Every report is
        read twice by DeepSeek; <span class="mk q">~</span> marks a figure the two readings disagreed on, read once, or derived from two
        year-to-date figures — hover for which. Medians across the banks that have reported.</p>`;
    box.querySelector('.rs-tabs').addEventListener('click', (ev) => { const b = ev.target.closest('button'); if (b) { RS.country = b.dataset.c; RS.n = 25; rsRender(); } });
    const more = box.querySelector('.rs-more');
    if (more) more.addEventListener('click', () => { RS.n += 50; rsRender(); });
    const tb = box.querySelector('tbody');
    tb.addEventListener('click', (ev) => { if (ev.target.closest('a')) return; const r = ev.target.closest('tr[data-key]'); if (r) openBank(r.dataset.lei, r.dataset.key); });
    tb.addEventListener('keydown', (ev) => { if (ev.key === 'Enter') { const r = ev.target.closest('tr[data-key]'); if (r) openBank(r.dataset.lei, r.dataset.key); } });
    rsUpcoming();
  }
  function rsUpcoming() {
    const box = $('#rsUp'); if (!box) return;
    const today = new Date().toISOString().slice(0, 10);
    const up = RS.up.filter((u) => u.planned >= today && (RS.country === 'ALL' || u.country === RS.country)).slice(0, 45);
    const days = {};
    for (const u of up) (days[u.planned] ||= []).push(u);
    const wd = (d) => new Date(d + 'T00:00:00Z').toLocaleString('en-GB', { weekday: 'short', day: 'numeric', month: 'short', timeZone: 'UTC' });
    box.innerHTML = `<h3><span><span class="g">◷</span>Who reports next</span><small>${RS.country === 'ALL' ? 'all countries' : CNAME[RS.country]}</small></h3>
      ${up.length ? Object.entries(days).map(([d, us]) => `<div class="rs-day"><div class="d">${esc(wd(d))}</div><div class="b">${us.map((u) =>
        `<span class="lnk" data-key="${esc(u.key)}"${u.lei ? ` data-lei="${u.lei}"` : ''} tabindex="0" role="button">${esc(u.short)}<small>${esc(u.country)}</small></span>`).join('')}</div></div>`).join('')
        : '<p class="note">No dates announced yet.</p>'}
      <p class="note">The date each bank's latest report gives for its next one (Nasdaq banks) and Oslo Børs's financial calendar (Norway).</p>`;
    box.onclick = (ev) => { const b = ev.target.closest('[data-key]'); if (b) openBank(b.dataset.lei, b.dataset.key); };
  }

  // one bank's quarters, for its profile: income stacked by kind, costs and credit losses, profit; the table under it
  async function nbResults(lei, rkey) {
    const host = $('#nbRes'); if (!host) return;
    const rows = await get(`nordic_quarters?${rkey ? `key=eq.${encodeURIComponent(rkey)}` : `lei=eq.${lei}`}&select=key,period,metric,value,currency,status&order=period`);
    if (!rows || !rows.length) { host.closest('.panel').hidden = true; return; }
    const keys = [...new Set(rows.map((r) => r.key))];
    const key = rkey || (keys.length > 1 ? keys.find((k) => RS.byLei[lei] === k) || keys[0] : keys[0]);
    const Q = {};
    for (const r of rows) if (r.key === key) (Q[r.period] ||= {})[r.metric] = r;
    const ps = Object.keys(Q).sort().slice(-12);
    const cur = (rows.find((r) => r.currency) || {}).currency || '';
    const data = ps.map((p) => {
      const v = Q[p], ti = mval(v, 'total_income'), ni = mval(v, 'net_interest_income'), fe = mval(v, 'net_commission_income');
      return { p, ni, fe, other: ok(ti) && ok(ni) ? ti - ni - (fe || 0) : null, ti, cost: mval(v, 'operating_expenses'),
        loss: mval(v, 'impairment_losses'), np: mval(v, 'net_profit'), v };
    });
    const tbl = [['Net interest income', 'net_interest_income'], ['Net fees', 'net_commission_income'], ['Total income', 'total_income'],
      ['Operating expenses', 'operating_expenses'], ['Credit losses', 'impairment_losses'], ['Pre-tax profit', 'pre_tax_profit'],
      ['Net profit', 'net_profit'], ['Return on equity, %', 'return_on_equity_pct'], ['Cost/income, %', 'cost_income_ratio_pct'],
      ['Net interest margin, %', 'net_interest_margin_pct'], ['CET1 ratio, %', 'cet1_ratio_pct'], ['Loans', 'loans_to_customers'],
      ['Deposits', 'deposits_from_customers']];
    const last8 = ps.slice(-8);
    host.innerHTML = `<div class="nb-res"><div class="chart" id="nbResChart"></div>
      <div class="legend-row"><span><i class="sq" style="background:var(--s1)"></i>Net interest income</span><span><i class="sq" style="background:var(--s2)"></i>Net fees</span>
        <span><i class="sq" style="background:var(--s-muted)"></i>Other income</span><span><i style="background:var(--text2)"></i>Operating expenses (dashed)</span>
        <span><i style="background:var(--s3)"></i>Net profit</span><span><i class="sq" style="background:var(--hot)"></i>Credit losses (below zero)</span></div></div>
      <div class="ut-wrap"><table class="ut nb-res-t"><thead><tr><th class="l">${esc(cur)} m</th>${last8.map((p) => `<th>${qlab(p)}</th>`).join('')}</tr></thead>
        <tbody>${tbl.map(([lab, m]) => `<tr><td class="l">${lab}</td>${last8.map((p) => {
          const r = Q[p][m]; return `<td>${r && ok(r.value) ? mm(+r.value) : '<span class="faint">—</span>'}${mark(r)}</td>`; }).join('')}</tr>`).join('')}</tbody></table></div>
      <p class="note">From the bank's own quarterly reports, read twice; the quarter alone. Credit losses: positive = charged.</p>`;
    resChart($('#nbResChart'), data, cur);
  }
  // a bank outside the EBA hub (small and non-complex banks publish Pillar 3 only in their own reports): its results alone
  function renderResultsOnly(key) {
    const box = $('#nmBank'); if (!box) return;
    ++BANK_REQ;
    ST.focus = null; ST.bank = null;
    const it = RS.feed.find((f) => f.key === key) || RS.up.find((u) => u.key === key) || { short: key, country: '' };
    box.innerHTML = `<div class="nb-head"><div><div class="hero-kicker">◉ ${esc(CNAME[it.country] || it.country || '')} · quarterly reports only</div>
        <h2 class="nb-title">${esc(it.short)}</h2><div class="nb-sub">Not in the EBA's Pillar 3 data hub: a small bank that publishes its
        Pillar 3 only in its own reports, so its capital, stages and models aren't on this page — its quarterly results are.</div></div></div>
      <div class="grid g12 nb-grid"><div class="panel span12"><h3><span><span class="g">▦</span>Quarterly results</span><small>from its own reports, each read twice</small></h3>
        <div id="nbRes"><div class="empty">Loading the quarters …</div></div></div></div>`;
    paintMap(); renderLeague();
    nbResults(null, key).catch((e) => console.error(e));
    const sec = $('#nordic-bank'); if (sec) sec.scrollIntoView({ behavior: 'smooth', block: 'start' });
  }
  const openBank = (lei, key) => { if (lei && BANKS[lei]) setBank(lei, true); else if (key) renderResultsOnly(key); };

  function resChart(host, data, cur) {
    if (!host || !window.d3 || !data.length) return;
    const W = Math.max(300, host.clientWidth || 600), H = 230, m = { t: 10, r: 10, b: 24, l: 52 };
    const neg = d3.min(data, (d) => -(Math.max(0, d.loss || 0))) || 0;
    const top = d3.max(data, (d) => Math.max(d.ti || 0, (d.ni || 0) + (d.fe || 0) + Math.max(0, d.other || 0), d.cost || 0, d.np || 0)) || 1;
    const x = d3.scaleBand().domain(data.map((d) => d.p)).range([m.l, W - m.r]).padding(0.28);
    const y = d3.scaleLinear().domain([Math.min(0, neg * 1.1, d3.min(data, (d) => d.np || 0) * 1.1), top * 1.08]).nice().range([H - m.b, m.t]);
    const svg = d3.create('svg').attr('viewBox', `0 0 ${W} ${H}`).attr('role', 'img').attr('aria-label', 'Quarterly income, costs and profit');
    svg.append('g').attr('class', 'gl').selectAll('line').data(y.ticks(4)).join('line').attr('x1', m.l).attr('x2', W - m.r).attr('y1', (v) => y(v)).attr('y2', (v) => y(v));
    svg.append('g').attr('class', 'ax').attr('transform', `translate(0,${H - m.b})`)
      .call(d3.axisBottom(x).tickFormat((p) => `Q${p.slice(5)}${p.slice(5) === '1' || data.length <= 6 ? ' ' + p.slice(2, 4) : ''}`).tickSizeOuter(0));
    svg.append('g').attr('class', 'ax').attr('transform', `translate(${m.l},0)`).call(d3.axisLeft(y).ticks(4).tickSize(0).tickPadding(6)
      .tickFormat((v) => nb(v, 0))).call((g) => g.select('.domain').remove());
    const bw = x.bandwidth();
    for (const d of data) {
      let y0 = 0;
      for (const [val0, col] of [[d.ni, 'var(--s1)'], [d.fe, 'var(--s2)'], [d.other, 'var(--s-muted)']]) {
        if (!ok(val0) || val0 <= 0) continue;
        svg.append('rect').attr('x', x(d.p)).attr('width', bw).attr('y', y(y0 + val0)).attr('height', Math.max(0.5, y(y0) - y(y0 + val0) - 1)).attr('fill', col);
        y0 += val0;
      }
      if (ok(d.loss) && d.loss > 0) svg.append('rect').attr('x', x(d.p) + bw * 0.2).attr('width', bw * 0.6).attr('y', y(0) + 1)
        .attr('height', Math.max(0.5, y(-d.loss) - y(0))).attr('fill', 'var(--hot)');
    }
    const line = (k) => d3.line().defined((d) => ok(d[k])).x((d) => x(d.p) + bw / 2).y((d) => y(d[k]));
    svg.append('path').attr('d', line('cost')(data)).attr('fill', 'none').attr('stroke', 'var(--text2)').attr('stroke-width', 1.5).attr('stroke-dasharray', '4 3');
    svg.append('path').attr('d', line('np')(data)).attr('fill', 'none').attr('stroke', 'var(--s3)').attr('stroke-width', 2);
    svg.append('g').selectAll('circle').data(data.filter((d) => ok(d.np))).join('circle').attr('cx', (d) => x(d.p) + bw / 2).attr('cy', (d) => y(d.np))
      .attr('r', 3).attr('fill', 'var(--s3)').attr('stroke', 'var(--bg0)').attr('stroke-width', 1.5);
    svg.append('rect').attr('x', m.l).attr('y', m.t).attr('width', W - m.l - m.r).attr('height', H - m.t - m.b).attr('fill', 'transparent')
      .on('mousemove', (ev) => {
        const [px] = d3.pointer(ev);
        const d = data.find((q) => px >= x(q.p) - x.step() * x.paddingInner() / 2 && px <= x(q.p) + bw + x.step() * x.paddingInner() / 2);
        if (!d) { tip(null); return; }
        tip(`<b>${qlab(d.p)}</b><table class="tg">${[['Net interest income', d.ni], ['Net fees', d.fe], ['Other income', d.other], ['Total income', d.ti],
          ['Operating expenses', d.cost], ['Credit losses', d.loss], ['Net profit', d.np]].map(([l, v]) => `<tr><td>${l}</td><td>${mm(v)}</td></tr>`).join('')}</table>
          <div class="tm">${esc(cur)} million</div>`, ev);
      })
      .on('mouseleave', () => tip(null));
    host.innerHTML = '';
    host.appendChild(svg.node());
  }
  async function rsInit() {
    if (!$('#rsFeed')) return;
    if (!(await rsLoad())) { $('#rsFeed').innerHTML = '<div class="empty">The reports are unavailable</div>'; return; }
    rsRender();
    renderLeague();                                  // the league's ROE and C/I come from the reports
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
    nhRender();
    rsInit();
    let rt, rw = window.innerWidth;
    window.addEventListener('resize', () => {
      clearTimeout(rt);
      rt = setTimeout(() => { if (window.innerWidth === rw) return; rw = window.innerWidth; drawMap(); nhRender(); }, 250);
    });
    const pill = $('#pillNordic'); if (pill) pill.innerHTML = `<b>${Object.values(BANKS).filter((b) => b.nordic).length}</b>&nbsp;Nordic banks · EBA`;
    if (window.__bnReady) window.__bnReady();
  }
  // for assets/banks-nordic-capital.js: the registry, opening a profile, and the shared tooltip
  window.BankNordic = { init, state: ST, banks: () => BANKS, open: (lei) => openBank(lei), tip,
    ready: new Promise((res) => { window.__bnReady = res; }) };
  if (document.readyState === 'loading') document.addEventListener('DOMContentLoaded', init); else init();
})();
