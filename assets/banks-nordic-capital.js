/* Banks · capital and models across the Nordics (2026-11-12): every Nordic bank's Pillar 3 on one scale, from the EBA's
   data hub. How far CET1 sits above what the bank must hold and what its risk exposure is made of (KM1, OV1, CR6-A); the
   risk weights the models put on mortgages and companies against the standardised approach (CR6, CR4); where the PDs and
   LGDs sit on the IRB formula (CR6, performing exposure); how close the CRR3 output floor is (CMS1); whether the PDs held
   in the last year (CR9); and how much the models moved the capital (CR8).
   Data: ENEXT api.nordic_metrics, api.nordic_irb_classes, api.nordic_sa, api.nordic_irb_scope, api.nordic_floor,
   api.nordic_backtest, api.nordic_rwa_flow (research/banks/nordic_clean.py). The bank registry, the profile and the
   tooltip come from assets/banks-nordic.js (window.BankNordic). Runbook: ../ENEXT/docs/banks-nordic.md → Step 4. */
(() => {
  const API = 'https://api.slensvik.com/';
  const $ = (s, r = document) => r.querySelector(s);
  const esc = (s) => String(s ?? '').replace(/[&<>"]/g, (c) => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;' }[c]));
  const get = (p) => window.INTEL ? INTEL.safeGet(p) : fetch(API + p).then((r) => r.ok ? r.json() : null).catch(() => null);
  const ok = (v) => v != null && v !== '' && isFinite(v);
  const nb = (v, dp = 0) => !ok(v) ? '—' : (+v).toLocaleString('nb-NO', { minimumFractionDigits: dp, maximumFractionDigits: dp });
  const pct = (v, dp = 1) => !ok(v) ? '—' : nb(v * 100, dp) + ' %';              // fractions in, percent out
  const pp = (v, dp = 1) => !ok(v) ? '—' : (v >= 0 ? '+' : '−') + nb(Math.abs(v) * 100, dp) + ' pp';
  const sp = (v, dp = 1) => !ok(v) ? '—' : (v >= 0 ? '+' : '−') + nb(Math.abs(v) * 100, dp) + ' %';
  const eurbn = (v) => !ok(v) ? '—' : 'EUR ' + nb(v / 1e9, Math.abs(v) < 1e10 ? 1 : 0) + ' bn';
  const median = (a) => { const s = a.filter(ok).sort((x, y) => x - y); if (!s.length) return null; const m = s.length >> 1; return s.length % 2 ? s[m] : (s[m - 1] + s[m]) / 2; };
  const dl = (d) => d ? new Date(d + 'T00:00:00Z').toLocaleString('en-GB', { month: 'short', year: 'numeric', timeZone: 'UTC' }) : '—';
  const CNAME = { NO: 'Norway', SE: 'Sweden', DK: 'Denmark', FI: 'Finland', IS: 'Iceland' };
  const tip = (h, e) => window.BankNordic ? BankNordic.tip(h, e) : null;
  const enc = (h) => encodeURIComponent(h);

  const MORT = 'Retail exposures - Secured by residential real estate';
  const PORT = {      // the capital-curve portfolios: CR6 classes and the formula each uses
    mort: { name: 'Mortgages', classes: [MORT], kind: 'mort', levels: [5, 10, 15, 25, 35] },
    corp: { name: 'Corporates', classes: ['Corporates - Other'], kind: 'corp', levels: [25, 40, 60, 80, 100] },
    sl: { name: 'Specialised lending', classes: ['Corporates - Specialised Lending'], kind: 'corp', levels: [25, 40, 60, 80, 100] },
    ret: { name: 'Other retail', classes: ['Retail exposures - Other'], kind: 'cons', levels: [15, 25, 35, 50, 75] },
  };
  // what the risk exposure is made of (OV1), in stacking order; the hues are the page's validated palette in its order
  const MIX = [['sa', 'Credit · standardised', '#5a6377'], ['irb', 'Credit · internal models', '#3987e5'],
    ['ccr', 'Counterparty and CVA', '#d95926'], ['mkt', 'Market', '#199e70'], ['op', 'Operational', '#c98500'],
    ['oth', 'Other (securitisation, settlement, floor …)', '#3a4252']];
  const SORTS = [['head', 'tightest first'], ['size', 'largest first'], ['cet1', 'highest CET1'], ['irb', 'most on models']];
  const SHOW = 24;
  const MIN_EAD = 5e7;                               // model classes under EUR 50 m are left off the charts
  const ST = { country: 'ALL', subs: false, port: 'mort', sort: 'head', all: false };
  let D = null;

  // ── the IRB formula (CRR3, no 1.06 scaling), as in assets/banks-capital.js ─────────────────────────────────────
  function ncdf(x) {
    const t = 1 / (1 + 0.2316419 * Math.abs(x)), d = 0.3989422804014327 * Math.exp(-x * x / 2);
    const p = d * t * (0.31938153 + t * (-0.356563782 + t * (1.781477937 + t * (-1.821255978 + t * 1.330274429))));
    return x >= 0 ? 1 - p : p;
  }
  function ninv(p) {
    const a = [-39.69683028665376, 220.9460984245205, -275.9285104469687, 138.357751867269, -30.66479806614716, 2.506628277459239];
    const b = [-54.47609879822406, 161.5858368580409, -155.6989798598866, 66.80131188771972, -13.28068155288572];
    const c = [-0.007784894002430293, -0.3223964580411365, -2.400758277161838, -2.549732539343734, 4.374664141464968, 2.938163982698783];
    const d = [0.007784695709041462, 0.3224671290700398, 2.445134137142996, 3.754408661907416];
    const lo = 0.02425;
    if (p < lo) { const q = Math.sqrt(-2 * Math.log(p)); return (((((c[0] * q + c[1]) * q + c[2]) * q + c[3]) * q + c[4]) * q + c[5]) / ((((d[0] * q + d[1]) * q + d[2]) * q + d[3]) * q + 1); }
    if (p <= 1 - lo) { const q = p - 0.5, r = q * q; return (((((a[0] * r + a[1]) * r + a[2]) * r + a[3]) * r + a[4]) * r + a[5]) * q / (((((b[0] * r + b[1]) * r + b[2]) * r + b[3]) * r + b[4]) * r + 1); }
    const q = Math.sqrt(-2 * Math.log(1 - p)); return -(((((c[0] * q + c[1]) * q + c[2]) * q + c[3]) * q + c[4]) * q + c[5]) / ((((d[0] * q + d[1]) * q + d[2]) * q + d[3]) * q + 1);
  }
  const G999 = ninv(0.999);
  function corr(kind, pd) {
    if (kind === 'mort') return 0.15;
    if (kind === 'cons') { const w = (1 - Math.exp(-35 * pd)) / (1 - Math.exp(-35)); return 0.03 * w + 0.16 * (1 - w); }
    const w = (1 - Math.exp(-50 * pd)) / (1 - Math.exp(-50)); return 0.12 * w + 0.24 * (1 - w);
  }
  function rwOf(kind, pd, lgd, M = 2.5) {          // risk weight (fraction) for a PD and LGD given as fractions
    pd = Math.min(0.999, Math.max(pd, 0.0005));
    const R = corr(kind, pd);
    let K = lgd * (ncdf((ninv(pd) + Math.sqrt(R) * G999) / Math.sqrt(1 - R)) - pd);
    if (kind === 'corp') { const b = Math.pow(0.11852 - 0.05478 * Math.log(pd), 2); K *= (1 + (M - 2.5) * b) / (1 - 1.5 * b); }
    return 12.5 * K;
  }

  // ── data ──────────────────────────────────────────────────────────────────────────────────────────────────────────
  const KM = ['cet1_ratio', 'p2r_cet1', 'cbr', 'trea', 'rwea_credit', 'rwea_credit_sa', 'rwea_credit_firb', 'rwea_credit_airb', 'rwea_slotting',
    'rwea_equity_irb', 'rwea_ccr', 'rwea_cva', 'rwea_market', 'rwea_op', 'rwea_total'];
  const RANGES = '0.00-0.15,0.15-0.25,0.25-0.50,0.50-0.75,0.75-2.50,2.50-10.00,10.00-100.00';
  async function load() {
    const since = new Date(Date.now() - 460 * 864e5).toISOString().slice(0, 10);
    const [met, irb, sa, scope, floor, bt, flow] = await Promise.all([
      get(`nordic_metrics?metric=in.(${KM.join(',')})&ref_date=gte.${since}&select=lei,ref_date,metric,value,value_eur,quality,note`),
      get('nordic_irb_classes?exposure_class=neq.All%20exposure%20classes&select=lei,ref_date,exposure_class,approach,eur_rate,ead_perf,pd_perf,lgd_perf,density_perf,ead,density,maturity,suspect'),
      get(`nordic_sa?exposure_class=in.(re_res,corp)&ref_date=gte.${since}&select=lei,ref_date,exposure_class,exposure_post,rwea,density,eur_rate`),
      get('nordic_irb_scope?exposure_class=eq.total&select=lei,ref_date,irb_value,total_value,pct_irb,pct_ppu,pct_rollout'),
      get('nordic_floor?risk=eq.total&floor_ratio=not.is.null&select=lei,ref_date,floor_ratio,quality'),
      get(`nordic_backtest?bucket=in.(${RANGES})&select=lei,ref_date,exposure_class,approach,obligors,defaulted,odr,pd_avg,pd_ew`),
      get('nordic_rwa_flow?item=in.(start,end,model_updates,methodology)&select=lei,ref_date,item,value'),
    ]);
    if (!met) return false;
    const M = {};                                   // lei → date → metric → row
    for (const r of met) ((M[r.lei] ||= {})[r.ref_date] ||= {})[r.metric] = r;
    const latestBy = (rows) => { const o = {}; for (const r of rows || []) if (!o[r.lei] || r.ref_date >= o[r.lei].ref_date) o[r.lei] = r; return o; };
    const irbBy = {};
    for (const r of irb || []) ((irbBy[r.lei] ||= {})[`${r.exposure_class}|${r.approach}`] = r);
    const saBy = {};
    for (const r of sa || []) { const o = (saBy[r.lei] ||= {}); if (!o[r.exposure_class] || r.ref_date >= o[r.exposure_class].ref_date) o[r.exposure_class] = r; }
    // CR9 at the bank's latest year-end, pooled over the PD grades per class and approach
    const btY = {};
    for (const r of bt || []) if (!btY[r.lei] || r.ref_date > btY[r.lei]) btY[r.lei] = r.ref_date;
    const btBy = {};
    for (const r of bt || []) {
      if (r.ref_date !== btY[r.lei] || !ok(r.obligors) || r.obligors <= 0 || /^Total/.test(r.exposure_class)) continue;
      const p = ok(r.pd_avg) ? r.pd_avg : r.pd_ew;
      if (!ok(p)) continue;
      const k = `${r.lei}|${r.exposure_class}|${r.approach}`;
      const o = (btBy[k] ||= { lei: r.lei, cls: r.exposure_class, appr: r.approach, ref: r.ref_date, n: 0, def: 0, pdn: 0 });
      o.n += r.obligors;
      o.def += ok(r.defaulted) ? r.defaulted : ok(r.odr) ? r.odr * r.obligors : 0;
      o.pdn += p * r.obligors;
    }
    const fl = {};
    for (const r of flow || []) ((fl[r.lei] ||= {})[r.ref_date] ||= {})[r.item] = r.value;
    D = { M, irbBy, saBy, scope: latestBy(scope), floor: latestBy(floor), bt: Object.values(btBy), fl };
    return true;
  }

  // ── which banks ───────────────────────────────────────────────────────────────────────────────────────────────────
  const REG = () => (window.BankNordic ? BankNordic.banks() : {});
  function banks() {
    return Object.values(REG()).filter((b) => b.nordic && (ST.country === 'ALL' || b.country === ST.country)
      && (ST.subs || (b.highest_eea !== false && b.shares !== false)));
  }
  const name = (lei) => (REG()[lei] || {}).short || lei;
  const ctry = (lei) => (REG()[lei] || {}).country || '';
  function latest(lei, m) {                         // the latest row of a metric
    const by = D.M[lei] || {};
    const ds = Object.keys(by).filter((d) => by[d][m] && ok(by[d][m].value)).sort();
    return ds.length ? by[ds[ds.length - 1]][m] : null;
  }
  function row(b) {                                  // one bank's capital and what its risk exposure is made of
    const c1 = latest(b.lei, 'cet1_ratio'), p2r = latest(b.lei, 'p2r_cet1'), cbr = latest(b.lei, 'cbr'), tr = latest(b.lei, 'trea');
    const cet1 = c1 ? +c1.value : null, p2 = p2r ? +p2r.value : null, cb = cbr ? +cbr.value : null;
    const req = ok(p2) && ok(cb) ? 0.045 + p2 + cb : null;
    // OV1 from one date: the latest OV1 (the semi-annual filers' quarters carry KM1 alone)
    const by = D.M[b.lei] || {};
    const od = Object.keys(by).filter((d) => by[d].rwea_total).sort().pop() || Object.keys(by).filter((d) => by[d].trea).sort().pop();
    const g = (m) => od && by[od][m] && ok(by[od][m].value) ? +by[od][m].value : 0;
    const tot = g('rwea_total') || g('trea');
    // IRB = its OV1 rows, or credit risk less the standardised part when a filer leaves them blank (SpareBank 1 SMN)
    const irbRows = g('rwea_credit_firb') + g('rwea_credit_airb') + g('rwea_slotting') + g('rwea_equity_irb');
    const parts = { sa: g('rwea_credit_sa'), irb: Math.max(irbRows, g('rwea_credit') - g('rwea_credit_sa')),
      ccr: g('rwea_ccr') + g('rwea_cva'), mkt: g('rwea_market'), op: g('rwea_op') };
    const named = Object.values(parts).reduce((a, x) => a + x, 0);
    parts.oth = tot > named * 1.001 ? tot - named : 0;
    const credit = parts.sa + parts.irb;
    const fl = D.floor[b.lei], sc = D.scope[b.lei];
    const cover = sc ? (ok(sc.pct_irb) && sc.pct_irb > 0 ? +sc.pct_irb : sc.total_value > 0 && ok(sc.irb_value) ? sc.irb_value / sc.total_value : null) : null;
    const derived = [c1, p2r, cbr].filter((r) => r && r.quality === 'derived' && r.note).map((r) => r.note);
    return { b, cet1, p2, cb, req, head: ok(cet1) && ok(req) ? cet1 - req : null, tot, parts,
      irbShare: credit > 0 ? parts.irb / credit : null, cover,
      floor: fl ? fl.floor_ratio : null, floorDerived: fl && fl.quality === 'derived', floorDate: fl && fl.ref_date,
      asof: c1 && c1.ref_date, trea_eur: tr && tr.value_eur, derived };
  }
  const SORTF = {
    head: (a, b) => (ok(a.head) ? 0 : 1) - (ok(b.head) ? 0 : 1) || (a.head ?? 0) - (b.head ?? 0),
    size: (a, b) => (b.trea_eur || 0) - (a.trea_eur || 0),
    cet1: (a, b) => (b.cet1 || 0) - (a.cet1 || 0),
    irb: (a, b) => (b.irbShare || 0) - (a.irbShare || 0) || (b.trea_eur || 0) - (a.trea_eur || 0),
  };

  // ── render ────────────────────────────────────────────────────────────────────────────────────────────────────────
  function render() {
    const box = $('#ncPanel'); if (!box || !D) return;
    const rows = banks().map(row).filter((r) => ok(r.cet1)).sort(SORTF[ST.sort]);
    const irbRows = rows.filter((r) => r.irbShare > 0.05);
    const fl = irbRows.filter((r) => ok(r.floor));
    const below = fl.filter((r) => r.floor < 0.725);
    const tight = rows.filter((r) => ok(r.head) && r.head < 0.02);
    const shown = ST.all ? rows : rows.slice(0, SHOW);
    const tabs = ['ALL', 'NO', 'SE', 'DK', 'FI', 'IS'].map((k) => `<button data-c="${k}" class="${ST.country === k ? 'on' : ''}" aria-pressed="${ST.country === k}">${k === 'ALL' ? 'All countries' : CNAME[k]}</button>`).join('');
    const sorts = SORTS.map(([k, t]) => `<button data-s="${k}" class="${ST.sort === k ? 'on' : ''}" aria-pressed="${ST.sort === k}">${t}</button>`).join('');
    box.innerHTML = `<div class="panel nc-top">
      <div class="nc-ctrl"><div class="seg-tabs nc-c">${tabs}</div>
        <label class="nc-subs"><input type="checkbox" ${ST.subs ? 'checked' : ''}> include subsidiaries (mortgage banks, part-owned companies)</label></div>
      <div class="nc-kpis">
        <div><div class="big">${rows.length}</div><div class="k">banks with a KM1</div></div>
        <div class="k">median CET1 ratio<br><b>${pct(median(rows.map((r) => r.cet1)))}</b></div>
        <div class="k">median above its requirement<br><b>${pp(median(rows.map((r) => r.head)))}</b></div>
        <div class="k">within 2 pp of it<br><b class="${tight.length ? 'warm' : ''}">${tight.length}</b> bank${tight.length === 1 ? '' : 's'}</div>
        <div class="k">credit risk on models, median IRB bank<br><b>${pct(median(irbRows.map((r) => r.irbShare)), 0)}</b> · ${irbRows.length} banks</div>
        <div class="k">IRB banks below 72.5 % of the floor base<br><b>${below.length}</b> of ${fl.length}</div></div>
      <div class="nc-sort"><span class="lt">Sort</span><div class="seg-tabs nc-s">${sorts}</div></div>
      <div class="nc-head" aria-hidden="true"><span></span><span>CET1 ratio against the requirement</span><span></span><span>What the risk exposure is made of</span><span></span></div>
      <div class="nc-rows">${shown.map(capRow).join('')}</div>
      ${rows.length > SHOW ? `<button class="rs-more nc-more">${ST.all ? 'Show the first ' + SHOW : `Show all ${rows.length} banks`}</button>` : ''}
      <div class="legend-row nc-leg"><span><i class="sq nc-p1"></i>Pillar 1, 4.5 %</span><span><i class="sq nc-p2"></i>Pillar 2, CET1 part</span>
        <span><i class="sq nc-cb"></i>Combined buffer</span><span><i class="sq nc-hdl"></i>Above the requirement</span><span><i class="nc-tick"></i>CET1 ratio</span>
        <span class="nc-legsep"></span>${MIX.map(([, n, c]) => `<span><i class="sq" style="background:${c}"></i>${esc(n)}</span>`).join('')}</div>
      <p class="note">KM1, OV1 and CR6-A at each bank's latest disclosure. The requirement is the CET1 a bank must hold before the
        restrictions on the maximum distributable amount bite: 4.5 % plus the CET1 part of its Pillar 2 requirement plus the combined
        buffer (KM1 rows EU 7b and 11). Pillar 2 guidance is not disclosed and is left out. The bar on the right splits the total risk
        exposure amount; "IRB" is the share of credit risk on internal models, "floor" the total risk exposure over the output floor's
        standardised base (CMS1). Filings that break the template's own arithmetic are repaired, and the tooltip says how.</p>
    </div>
    <div class="grid g12 nc-grid">
      <div class="panel span6"><h3><span><span class="g">⌂</span>What the models put on a mortgage</span><small>risk weight, performing loans</small></h3>
        <div id="ncMort"></div></div>
      <div class="panel span6"><h3><span><span class="g">▣</span>… and on a company</span><small>risk weight, performing loans</small></h3>
        <div id="ncCorp"></div></div>
      <div class="panel span6"><h3><span><span class="g">◍</span>On the capital curve</span><small>PD and LGD, bubble = exposure</small></h3>
        <div class="seg-tabs nc-p">${Object.entries(PORT).map(([k, p]) => `<button data-p="${k}" class="${ST.port === k ? 'on' : ''}" aria-pressed="${ST.port === k}">${p.name}</button>`).join('')}</div>
        <div class="chart nc-chart" id="ncCurve"></div><p class="note" id="ncCurveNote"></p></div>
      <div class="panel span6"><h3><span><span class="g">⫠</span>The output floor</span><small>risk exposure over the standardised base</small></h3>
        <div id="ncFloor"></div></div>
      <div class="panel span6"><h3><span><span class="g">⊿</span>Did the PDs hold?</span><small>a year's defaults against the PDs</small></h3>
        <div class="chart nc-chart" id="ncBack"></div></div>
      <div class="panel span6"><h3><span><span class="g">⟳</span>What the models moved</span><small>CR8, the last year</small></h3>
        <div id="ncMoves"></div></div>
    </div>`;
    box.querySelector('.nc-c').onclick = (ev) => { const b = ev.target.closest('button'); if (b) { ST.country = b.dataset.c; render(); } };
    box.querySelector('.nc-s').onclick = (ev) => { const b = ev.target.closest('button'); if (b) { ST.sort = b.dataset.s; render(); } };
    box.querySelector('.nc-subs input').onchange = (ev) => { ST.subs = ev.target.checked; render(); };
    box.querySelector('.nc-p').onclick = (ev) => {
      const b = ev.target.closest('button'); if (!b) return;
      ST.port = b.dataset.p; curve();
      box.querySelectorAll('.nc-p button').forEach((x) => { x.classList.toggle('on', x === b); x.setAttribute('aria-pressed', x === b); });
    };
    const more = box.querySelector('.nc-more'); if (more) more.onclick = () => { ST.all = !ST.all; render(); };
    box.onclick = (ev) => {
      if (ev.target.closest('button, input, label, svg')) return;
      const r = ev.target.closest('[data-lei]'); if (r && window.BankNordic) BankNordic.open(r.dataset.lei);
    };
    box.onkeydown = (ev) => { if (ev.key === 'Enter') { const r = ev.target.closest('[data-lei]'); if (r && window.BankNordic) BankNordic.open(r.dataset.lei); } };
    box.onpointermove = (ev) => { if (ev.target.closest('svg')) return; const el = ev.target.closest('[data-tip]'); tip(el ? decodeURIComponent(el.dataset.tip) : null, ev); };
    box.onpointerleave = () => tip(null);
    riskWeights('ncMort', [MORT], 're_res', 0.5, 'mortgages');
    riskWeights('ncCorp', ['Corporates - Other'], 'corp', 1.25, 'corporates');
    curve();
    floorChart(rows);
    backtest();
    moves();
  }

  function capRow(r) {
    const X = 0.32, x = (v) => Math.max(0, Math.min(1, v / X)) * 100;
    const p1 = 0.045, p2 = r.p2 || 0, cb = r.cb || 0, req = p1 + p2 + cb;
    const tot = Object.values(r.parts).reduce((a, v) => a + v, 0) || 1;
    const hasReq = ok(r.req);
    const t = `<b>${esc(r.b.short)}</b> <span class="faint">· ${esc(CNAME[r.b.country] || r.b.country)} · KM1 ${dl(r.asof)}</span>
      <table class="tg"><tr><td>CET1 ratio</td><td>${pct(r.cet1, 2)}</td></tr>
      ${hasReq ? `<tr><td><i class="nc-p1"></i>Pillar 1</td><td>4,50 %</td></tr><tr><td><i class="nc-p2"></i>Pillar 2, CET1 part</td><td>${pct(r.p2, 2)}</td></tr>
        <tr><td><i class="nc-cb"></i>Combined buffer</td><td>${pct(r.cb, 2)}</td></tr><tr><td>Requirement</td><td>${pct(r.req, 2)}</td></tr>
        <tr><td><b>Above it</b></td><td><b>${pp(r.head, 2)}</b></td></tr>` : '<tr><td colspan="2">Pillar 2 or the buffers not disclosed</td></tr>'}
      <tr><td>Total risk exposure</td><td>${eurbn(r.trea_eur)}</td></tr>
      ${MIX.map(([k, n, c]) => r.parts[k] > 0 ? `<tr><td><i style="background:${c}"></i>${esc(n)}</td><td>${nb(r.parts[k] / tot * 100, 0)} %</td></tr>` : '').join('')}
      ${ok(r.cover) ? `<tr><td>Exposure on IRB (CR6-A)</td><td>${pct(r.cover, 0)}</td></tr>` : ''}
      ${ok(r.floor) ? `<tr><td>Floor ratio (CMS1 ${dl(r.floorDate)})</td><td>${nb(r.floor * 100, 0)} %</td></tr>` : ''}</table>
      ${r.derived.length ? `<div class="tm">Repaired: ${r.derived.map(esc).join('; ')}</div>` : ''}
      <div class="tm">Click for the bank's profile</div>`;
    const hd = hasReq ? `<i class="nc-hd${r.head < 0 ? ' neg' : ''}" style="left:${x(Math.min(req, r.cet1))}%;width:${Math.abs(x(r.cet1) - x(req))}%"></i>` : '';
    return `<div class="nc-row" data-lei="${r.b.lei}" data-tip="${enc(t)}" tabindex="0" role="button" aria-label="${esc(r.b.short)}: CET1 ${pct(r.cet1)}${hasReq ? ', ' + pp(r.head) + ' above its requirement' : ''}">
      <span class="n">${esc(r.b.short)}<small>${esc(r.b.country)}</small></span>
      <span class="cap">${hasReq ? `<i class="nc-p1" style="left:0;width:${x(p1)}%"></i><i class="nc-p2" style="left:${x(p1)}%;width:${x(p1 + p2) - x(p1)}%"></i><i class="nc-cb" style="left:${x(p1 + p2)}%;width:${x(req) - x(p1 + p2)}%"></i>` : ''}${hd}<b style="left:${x(r.cet1)}%"></b>${r.cet1 > X ? '<em>›</em>' : ''}</span>
      <span class="v">${pct(r.cet1)} <small class="${ok(r.head) && r.head < 0.02 ? 'warm' : ''}">${ok(r.head) ? pp(r.head) : ''}</small></span>
      <span class="mix">${r.tot > 0 ? MIX.map(([k, , c]) => r.parts[k] > 0 ? `<i style="flex:${r.parts[k]};background:${c}"></i>` : '').join('') : ''}</span>
      <span class="v2">${ok(r.irbShare) && r.irbShare > 0.01 ? `IRB ${nb(r.irbShare * 100, 0)} %` : 'SA'}${ok(r.floor) && r.irbShare > 0.05 ? ` · floor ${nb(r.floor * 100, 0)} %` : ''}</span></div>`;
  }

  // risk weights of one portfolio: the models (CR6, performing grades) against the standardised approach (CR4); the banks
  // without a model for it share one row
  function riskWeights(id, classes, saClass, xmax, what) {
    const host = $('#' + id); if (!host) return;
    const own = {}, pool = [];
    for (const b of banks()) {
      const pts = [];
      for (const [k, r] of Object.entries(D.irbBy[b.lei] || {})) {
        const [cls, appr] = k.split('|');
        if (!classes.includes(cls) || !ok(r.density_perf) || !(r.ead_perf * (r.eur_rate || 1) >= MIN_EAD)) continue;
        pts.push({ lei: b.lei, kind: appr === 'FIRB' ? 'F' : 'A', rw: r.density_perf, ead: r.ead_perf * (r.eur_rate || 1), pd: r.pd_perf, lgd: r.lgd_perf,
          d: r.ref_date, suspect: r.suspect });
      }
      const s = (D.saBy[b.lei] || {})[saClass];
      const spt = s && ok(s.density) && s.exposure_post * (s.eur_rate || 1) > 2e7 ? { lei: b.lei, kind: 'S', rw: s.density, ead: s.exposure_post * (s.eur_rate || 1), d: s.ref_date } : null;
      if (pts.length) { if (spt) pts.push(spt); own[b.lei] = pts; } else if (spt) pool.push(spt);
    }
    const leis = Object.keys(own);
    if (!leis.length && !pool.length) { host.innerHTML = '<div class="empty">No bank in this selection reports it</div>'; return; }
    const minM = (lei) => Math.min(...own[lei].filter((p) => p.kind !== 'S').map((p) => p.rw));
    leis.sort((a, b) => minM(a) - minM(b));
    const x = (v) => Math.max(0, Math.min(1, v / xmax)) * 100;
    const ticks = xmax <= 0.6 ? [0, 0.1, 0.2, 0.3, 0.4, 0.5] : [0, 0.25, 0.5, 0.75, 1, 1.25];
    const dot = (p) => {
      const t = `<b>${esc(name(p.lei))}</b> · ${p.kind === 'S' ? 'standardised approach (CR4)' : p.kind === 'F' ? 'F-IRB model (CR6)' : 'A-IRB model (CR6)'}<br>
        risk weight <b>${pct(p.rw)}</b> on ${eurbn(p.ead)}${p.kind !== 'S' && ok(p.pd) ? `<br>PD ${pct(p.pd, 2)} · LGD ${pct(p.lgd, 1)} (performing)` : ''} · ${dl(p.d)}
        ${p.suspect ? '<div class="tm">Flagged: a parameter moved more than ×2 since the bank\'s previous filing</div>' : ''}`;
      return `<i class="pt ${p.kind}${p.suspect ? ' sus' : ''}" style="left:${x(p.rw)}%" data-tip="${enc(t)}"></i>`;
    };
    const grid = ticks.map((t) => `<u style="left:${x(t)}%"></u>`).join('');
    const rowsH = leis.map((lei) => {
      const m = own[lei].filter((p) => p.kind !== 'S');
      return `<div class="nc-drow" data-lei="${lei}" role="button" tabindex="0"><span class="n">${esc(name(lei))}<small>${esc(ctry(lei))}</small></span>
        <span class="trk">${grid}${own[lei].map(dot).join('')}</span><span class="v">${m.map((p) => pct(p.rw, 0)).join(' · ')}</span></div>`;
    }).join('');
    const pmed = median(pool.map((p) => p.rw));
    const poolRow = pool.length ? `<div class="nc-drow pool"><span class="n">No model<small>${pool.length} bank${pool.length === 1 ? '' : 's'}</small></span>
        <span class="trk">${grid}${pool.map(dot).join('')}${ok(pmed) ? `<s style="left:${x(pmed)}%"></s>` : ''}</span><span class="v">${ok(pmed) ? 'med. ' + pct(pmed, 0) : ''}</span></div>` : '';
    host.innerHTML = `<div class="nc-dots">${rowsH}${poolRow}</div>
      <div class="nc-axis"><span></span><span class="trk">${ticks.map((t) => `<em style="left:${x(t)}%">${nb(t * 100)} %</em>`).join('')}</span><span></span></div>
      <div class="legend-row"><span><i class="dotA"></i>A-IRB model</span><span><i class="dotF"></i>F-IRB model</span><span><i class="dotS"></i>Standardised approach</span>${pool.length ? '<span><i class="medS"></i>median, banks without a model</span>' : ''}</div>
      <p class="note">The risk weight a bank's own models give its performing ${what} (CR6: risk-weighted exposure over exposure in the
        grades below default), before any national floor or add-on: Norway's 25 % floor on the mortgage portfolio average is one. The grey
        dots are the standardised approach for the same kind of loan (CR4: ${what === 'mortgages' ? 'residential property that is not income-producing' : 'corporates'}),
        at the IRB banks for the part outside their models and at the banks without one. A hollow dot is a class the cleaning flagged.</p>`;
  }

  // PD and LGD of one portfolio on the IRB formula
  function curve() {
    const host = $('#ncCurve'); if (!host) return;
    const P = PORT[ST.port];
    const pts = [];
    for (const b of banks()) for (const [k, r] of Object.entries(D.irbBy[b.lei] || {})) {
      const [cls, appr] = k.split('|');
      if (!P.classes.includes(cls) || !ok(r.pd_perf) || !ok(r.lgd_perf) || r.pd_perf <= 0 || !(r.ead_perf * (r.eur_rate || 1) >= MIN_EAD)) continue;
      pts.push({ lei: b.lei, appr, pd: r.pd_perf * 100, lgd: r.lgd_perf * 100, ead: r.ead_perf * (r.eur_rate || 1), rw: r.density_perf, d: r.ref_date, mat: r.maturity });
    }
    $('#ncCurveNote').textContent = (P.kind === 'corp'
      ? 'Lines: the CRR3 corporate formula at 2.5 years maturity, no SME adjustment. F-IRB banks use the supervisory LGD (40 % senior unsecured), lowered for collateral.'
      : P.kind === 'mort' ? 'Lines: the retail mortgage formula (correlation 15 %), before floors. A low PD with a high LGD can give the same risk weight as the reverse.'
      : 'Lines: the other-retail formula (correlation 3–16 %, falling with PD).') + ' PD and LGD are exposure-weighted over the performing grades (CR6).';
    if (!pts.length) { host.innerHTML = '<div class="empty">No IRB bank in this selection reports it</div>'; return; }
    const W = Math.max(300, host.clientWidth || 560), H = W < 480 ? 260 : 300, L = 40, R = 60, T = 14, Bm = 30;
    const pmin = Math.min(...pts.map((p) => p.pd)), pmax = Math.max(...pts.map((p) => p.pd));
    const xlo = Math.max(0.02, pmin / 2), xhi = Math.min(40, pmax * 2.2);
    const yMax = Math.min(80, Math.max(30, Math.ceil(Math.max(...pts.map((p) => p.lgd)) * 1.25 / 10) * 10));
    const x0 = Math.log10(xlo), x1 = Math.log10(xhi);
    const x = (pd) => L + (Math.log10(Math.min(Math.max(pd, xlo), xhi)) - x0) / (x1 - x0) * (W - L - R);
    const y = (lgd) => T + (1 - lgd / yMax) * (H - T - Bm);
    let s = `<svg viewBox="0 0 ${W} ${H}" width="${W}" height="${H}" role="img" aria-label="PD and LGD per bank on the IRB formula">`;
    for (const t of [0.02, 0.05, 0.1, 0.2, 0.5, 1, 2, 5, 10, 20, 40].filter((t) => t >= xlo && t <= xhi))
      s += `<line x1="${x(t)}" x2="${x(t)}" y1="${T}" y2="${H - Bm}" stroke="var(--grid)"/><text x="${x(t)}" y="${H - Bm + 13}" text-anchor="middle" fill="var(--text4)" font-size="9">${nb(t, t < 0.1 ? 2 : t < 1 ? 1 : 0)} %</text>`;
    for (let l = 0; l <= yMax; l += 10) s += `<line x1="${L}" x2="${W - R}" y1="${y(l)}" y2="${y(l)}" stroke="var(--grid)"/><text x="${L - 6}" y="${y(l) + 3}" text-anchor="end" fill="var(--text4)" font-size="9">${l} %</text>`;
    s += `<text x="${(L + W - R) / 2}" y="${H - 4}" text-anchor="middle" fill="var(--text3)" font-size="9.5">PD (log scale)</text>`
      + `<text x="10" y="${(T + H - Bm) / 2}" transform="rotate(-90 10 ${(T + H - Bm) / 2})" text-anchor="middle" fill="var(--text3)" font-size="9.5">LGD</text>`;
    const used = [];
    for (const lv of P.levels) {
      let d = '', exit = null, on = false;
      for (let i = 0; i <= 160; i++) {
        const pd = Math.pow(10, x0 + (x1 - x0) * i / 160);
        const lgd = 100 * (lv / 100) / rwOf(P.kind, pd / 100, 1);
        if (lgd > yMax) { if (on) break; continue; }
        d += `${on ? 'L' : 'M'}${x(pd).toFixed(1)},${y(lgd).toFixed(1)}`; on = true; exit = [x(pd), y(lgd)];
      }
      if (!d) continue;
      s += `<path d="${d}" fill="none" stroke="rgba(255,255,255,.2)" stroke-width="1"/>`;
      let ly = Math.max(T + 8, exit[1] + 3);
      while (used.some((u) => Math.abs(u - ly) < 10)) ly += 10;
      used.push(ly);
      s += `<text x="${W - R + 4}" y="${ly}" fill="var(--text3)" font-size="9">RW ${lv} %</text>`;
    }
    const mx = Math.max(...pts.map((p) => p.ead), 1);
    for (const p of pts) { p.cx = x(p.pd); p.cy = y(p.lgd); p.r = 3.5 + (W < 480 ? 7 : 10) * Math.sqrt(p.ead / mx); }
    for (const p of [...pts].sort((a, b) => b.ead - a.ead)) {
      const c = p.appr === 'FIRB' ? 'var(--s2)' : 'var(--s1)';
      s += `<circle cx="${p.cx.toFixed(1)}" cy="${p.cy.toFixed(1)}" r="${p.r.toFixed(1)}" fill="${c}" fill-opacity=".22" stroke="${c}" stroke-width="1.5"/>`;
    }
    const placed = [];                               // name the largest books, above their bubble or below when crowded
    for (const p of [...pts].sort((a, b) => b.ead - a.ead).slice(0, W < 480 ? 5 : 8)) {
      const txt = name(p.lei), w = txt.length * 5.6;
      const lx = Math.min(W - R - w / 2, Math.max(L + w / 2, p.cx));
      const free = (yy) => yy > T + 6 && yy < H - Bm - 2 && !placed.some((q) => Math.abs(q.y - yy) < 10 && Math.abs(q.x - lx) < (q.w + w) / 2 + 4);
      const ly = [p.cy - p.r - 4, p.cy + p.r + 11, p.cy - p.r - 14, p.cy + p.r + 21].find(free);
      if (ly == null) continue;
      placed.push({ x: lx, y: ly, w });
      s += `<text x="${lx.toFixed(1)}" y="${ly.toFixed(1)}" text-anchor="middle" fill="var(--text2)" font-size="9.5" paint-order="stroke" stroke="rgba(5,8,16,.9)" stroke-width="3">${esc(txt)}</text>`;
    }
    host.innerHTML = s + '</svg><div class="legend-row"><span><i class="dotA"></i>A-IRB</span><span><i class="dotF"></i>F-IRB</span><span><i style="background:rgba(255,255,255,.35)"></i>equal risk weight</span></div>';
    const svg = host.querySelector('svg');
    const near = (e) => {
      const rc = svg.getBoundingClientRect(), px = (e.clientX - rc.left) * (W / rc.width), py = (e.clientY - rc.top) * (H / rc.height);
      let best = null, bd = 1e9;
      for (const p of pts) { const dd = Math.hypot(p.cx - px, p.cy - py) - p.r; if (dd < bd) { bd = dd; best = p; } }
      return best && bd <= 12 ? best : null;
    };
    svg.onpointermove = (e) => {
      const p = near(e); if (!p) { tip(null, e); return; }
      const f = rwOf(P.kind, p.pd / 100, p.lgd / 100, ok(p.mat) && p.mat > 1 ? p.mat : 2.5);
      tip(`<b>${esc(name(p.lei))}</b> · ${esc(P.name)} · ${p.appr === 'FIRB' ? 'F-IRB' : 'A-IRB'}<br>
        PD <b>${nb(p.pd, 2)} %</b> · LGD <b>${nb(p.lgd, 1)} %</b> (performing, exposure-weighted)<br>
        the formula at these averages: RW ${pct(f)} · reported ${pct(p.rw)}<br>exposure ${eurbn(p.ead)} · ${dl(p.d)}
        <div class="tm">The formula is curved: a book spread over many grades doesn't land exactly where its averages do. Click for the profile.</div>`, e);
    };
    svg.onpointerleave = (e) => tip(null, e);
    svg.onclick = (e) => { const p = near(e); if (p && window.BankNordic) BankNordic.open(p.lei); };
  }

  // the output floor: total risk exposure as a share of the standardised base (CMS1), for the banks with models
  function floorChart(rows) {
    const host = $('#ncFloor'); if (!host) return;
    const rs = rows.filter((r) => ok(r.floor) && r.irbShare > 0.05).sort((a, b) => a.floor - b.floor);
    const nSA = rows.filter((r) => !(r.irbShare > 0.05)).length;
    if (!rs.length) { host.innerHTML = '<div class="empty">No bank with models in this selection files CMS1</div>'; return; }
    const lo = 0.4, hi = Math.max(1.1, ...rs.map((r) => Math.min(r.floor, 1.6)));
    const x = (v) => Math.max(0, Math.min(1, (v - lo) / (hi - lo))) * 100;
    host.innerHTML = `<div class="nc-floor">${rs.map((r) => `<div class="nc-frow" data-lei="${r.b.lei}" role="button" tabindex="0"
        data-tip="${enc(`<b>${esc(r.b.short)}</b> · CMS1 ${dl(r.floorDate)}<br>total risk exposure <b>${nb(r.floor * 100, 1)} %</b> of the output floor's standardised base<br>
          ${r.floorDerived ? '<span class="faint">CMS1\'s own total disagrees with KM1: KM1\'s risk exposure over CMS1\'s base</span><br>' : ''}
          ${r.floor < 0.725 ? 'below the 72.5 % the floor reaches in 2030: at today\'s balance sheet it would lift the risk exposure by ' + nb((0.725 / r.floor - 1) * 100, 0) + ' %' : 'above 72.5 %: the floor would not bind at today\'s balance sheet'}`)}">
        <span class="n">${esc(r.b.short)}<small>${esc(r.b.country)}</small></span>
        <span class="trk"><u style="left:${x(0.5)}%"></u><u class="fl" style="left:${x(0.725)}%"></u><u style="left:${x(1)}%"></u>
          <i style="width:${x(r.floor)}%" class="${r.floor < 0.725 ? 'low' : ''}"></i></span>
        <span class="v">${nb(r.floor * 100, 0)} %</span></div>`).join('')}</div>
      <div class="nc-axis"><span></span><span class="trk"><em style="left:${x(0.5)}%">50 %</em><em class="fl" style="left:${x(0.725)}%">${nb(72.5, 1)} %</em><em style="left:${x(1)}%">100 %</em></span><span></span></div>
      <p class="note">CMS1 at each bank's latest disclosure: its total risk exposure before the floor (KM1, where CMS1's own total disagrees
        with it) as a share of the base the output floor is computed on, every risk on the standardised approaches. CRR3 phases the floor in from 50 % in 2025 to 72.5 % in 2030; a bank below 72.5 % today
        would have its risk exposure lifted then, unless its book changes. ${nSA ? `The ${nSA} bank${nSA === 1 ? '' : 's'} in this selection without models sit at 100 % and are left out.` : ''}</p>`;
  }

  // CR9: a year's observed default rate against the average PD at its start, per bank and portfolio
  function backtest() {
    const host = $('#ncBack'); if (!host) return;
    const keep = new Set(banks().map((b) => b.lei));
    const KIND = (cls) => cls === MORT ? 'mort' : /^Corporates/.test(cls) ? 'corp' : /^Retail/.test(cls) ? 'ret' : null;
    const COL = { mort: 'var(--s1)', corp: 'var(--s2)', ret: 'var(--s3)' };
    const pts = D.bt.filter((o) => keep.has(o.lei) && o.n >= 50 && o.pdn > 0).map((o) => ({ ...o, pd: o.pdn / o.n, odr: o.def / o.n, k: KIND(o.cls) }))
      .filter((p) => p.k);
    if (!pts.length) { host.innerHTML = '<div class="empty">No bank in this selection files CR9</div>'; return; }
    const W = Math.max(300, host.clientWidth || 560), H = W < 480 ? 250 : 280, L = 46, R = 12, T = 10, Bm = 30;
    const FLOOR = 2e-4;                              // a year with no defaults is drawn on the bottom line
    const vals = pts.flatMap((p) => [p.pd, Math.max(p.odr, FLOOR)]);
    const lo = Math.max(1e-4, Math.min(...vals) / 1.6), hi = Math.min(1, Math.max(...vals) * 1.6);
    const lx = (v) => (Math.log10(Math.max(v, lo)) - Math.log10(lo)) / (Math.log10(hi) - Math.log10(lo));
    const x = (v) => L + lx(v) * (W - L - R), y = (v) => H - Bm - lx(v) * (H - T - Bm);
    let s = `<svg viewBox="0 0 ${W} ${H}" width="${W}" height="${H}" role="img" aria-label="Observed default rates against the PDs">`;
    for (const t of [1e-4, 3e-4, 1e-3, 3e-3, 0.01, 0.03, 0.1, 0.3, 1].filter((t) => t >= lo && t <= hi)) {
      const lab = nb(t * 100, t < 0.001 ? 2 : t < 0.01 ? 1 : 0) + ' %';
      s += `<line x1="${x(t)}" x2="${x(t)}" y1="${T}" y2="${H - Bm}" stroke="var(--grid)"/><text x="${x(t)}" y="${H - Bm + 13}" text-anchor="middle" fill="var(--text4)" font-size="9">${lab}</text>`;
      s += `<line x1="${L}" x2="${W - R}" y1="${y(t)}" y2="${y(t)}" stroke="var(--grid)"/><text x="${L - 6}" y="${y(t) + 3}" text-anchor="end" fill="var(--text4)" font-size="9">${lab}</text>`;
    }
    s += `<line x1="${x(lo)}" y1="${y(lo)}" x2="${x(hi)}" y2="${y(hi)}" stroke="var(--amber)" stroke-dasharray="4 3" stroke-width="1.2"/>`;
    s += `<text x="${(L + W - R) / 2}" y="${H - 4}" text-anchor="middle" fill="var(--text3)" font-size="9.5">average PD at the start of the year (log)</text>`
      + `<text x="11" y="${(T + H - Bm) / 2}" transform="rotate(-90 11 ${(T + H - Bm) / 2})" text-anchor="middle" fill="var(--text3)" font-size="9.5">share that defaulted (log)</text>`;
    for (const p of pts) { p.cx = x(p.pd); p.cy = y(Math.max(p.odr, FLOOR)); }
    for (const p of pts) s += `<circle cx="${p.cx.toFixed(1)}" cy="${p.cy.toFixed(1)}" r="4" fill="${COL[p.k]}" fill-opacity="${p.odr > p.pd ? 0.95 : 0.15}" stroke="${COL[p.k]}" stroke-width="1.4"/>`;
    const above = pts.filter((p) => p.odr > p.pd).length;
    host.innerHTML = s + `</svg><div class="legend-row"><span><i class="sq" style="background:var(--s1)"></i>Mortgages</span><span><i class="sq" style="background:var(--s2)"></i>Corporates</span>
      <span><i class="sq" style="background:var(--s3)"></i>Other retail</span><span><i style="background:var(--amber)"></i>defaults = PD</span></div>
      <p class="note">CR9 at each bank's latest year-end: the obligors in each PD grade at the start of the year, how many defaulted in it, and
        the grade's average PD, pooled per portfolio (the default grade left out; at least 50 obligors). PDs are set for a whole cycle with
        a margin of conservatism, so a calm year lands under the dashed line. ${above} of ${pts.length} portfolios defaulted more than their
        PDs said (filled); a portfolio with no defaults sits on the bottom line.</p>`;
    const svg = host.querySelector('svg');
    const near = (e) => {
      const rc = svg.getBoundingClientRect(), px = (e.clientX - rc.left) * (W / rc.width), py = (e.clientY - rc.top) * (H / rc.height);
      let best = null, bd = 1e9;
      for (const p of pts) { const dd = Math.hypot(p.cx - px, p.cy - py); if (dd < bd) { bd = dd; best = p; } }
      return best && bd <= 10 ? best : null;
    };
    svg.onpointermove = (e) => {
      const p = near(e); if (!p) { tip(null, e); return; }
      tip(`<b>${esc(name(p.lei))}</b> · ${esc(p.cls)} · ${p.appr === 'FIRB' ? 'F-IRB' : 'A-IRB'} · CR9 ${dl(p.ref)}<br>
        ${nb(p.n)} obligors at the start of the year, ${nb(p.def)} defaulted: <b>${nb(p.odr * 100, 2)} %</b><br>their average PD: <b>${nb(p.pd * 100, 2)} %</b>
        · defaults ${nb(p.odr / p.pd, 2)} × the PD<div class="tm">Click for the profile</div>`, e);
    };
    svg.onpointerleave = (e) => tip(null, e);
    svg.onclick = (e) => { const p = near(e); if (p && window.BankNordic) BankNordic.open(p.lei); };
  }

  // CR8: risk exposure moved by model updates and by methodology and policy, over the filings of the last year
  function moves() {
    const host = $('#ncMoves'); if (!host) return;
    const out = [];
    for (const b of banks()) {
      const byDate = D.fl[b.lei]; if (!byDate) continue;
      const all = Object.keys(byDate).sort();
      const last = all[all.length - 1];
      const cut = new Date(new Date(last + 'T00:00:00Z').getTime() - 360 * 864e5).toISOString().slice(0, 10);
      const ds = all.filter((d) => d > cut);
      const base = (byDate[ds[0]] || {}).start;
      if (!(base > 0)) continue;
      const sum = (k) => ds.reduce((a, d) => a + (ok(byDate[d][k]) ? +byDate[d][k] : 0), 0);
      const end = byDate[last].end;
      out.push({ lei: b.lei, mu: sum('model_updates') / base, me: sum('methodology') / base, n: ds.length, from: ds[0], to: last, chg: ok(end) ? end / base - 1 : null });
    }
    const vis = out.filter((o) => Math.abs(o.mu) + Math.abs(o.me) > 0).sort((a, b) => (a.mu + a.me) - (b.mu + b.me));
    const none = out.length - vis.length;
    if (!vis.length) { host.innerHTML = `<div class="empty">${out.length ? `${out.length} bank${out.length === 1 ? '' : 's'} filed CR8; no model changed its risk exposure` : 'No bank in this selection files CR8'}</div>`; return; }
    const m = Math.max(0.02, ...vis.map((o) => Math.max(Math.abs(o.mu) + Math.abs(o.me), Math.abs(o.mu + o.me))));
    const x = (v) => 50 + v / m * 48;
    host.innerHTML = `<div class="nc-moves">${vis.map((o) => {
      const t = `<b>${esc(name(o.lei))}</b> · CR8, ${o.n} filing${o.n === 1 ? '' : 's'} to ${dl(o.to)}<br>model updates <b>${sp(o.mu)}</b> of the IRB risk exposure
        <br>methodology and policy <b>${sp(o.me)}</b>${ok(o.chg) ? `<br>IRB risk exposure over the period, all causes: ${sp(o.chg)}` : ''}
        <div class="tm">Click for the profile</div>`;
      const seg = (from, v, cls) => v ? `<i class="${cls}" style="left:${Math.min(x(from), x(from + v))}%;width:${Math.max(0.6, Math.abs(x(from + v) - x(from)))}%"></i>` : '';
      return `<div class="nc-mrow" data-lei="${o.lei}" role="button" tabindex="0" data-tip="${enc(t)}"><span class="n">${esc(name(o.lei))}<small>${esc(ctry(o.lei))}</small></span>
        <span class="trk"><u style="left:50%"></u>${seg(0, o.mu, 'mu')}${seg(o.mu, o.me, 'me')}</span>
        <span class="v">${sp(o.mu + o.me)}</span></div>`;
    }).join('')}</div>
      <div class="legend-row"><span><i class="sq" style="background:var(--s1)"></i>Model updates</span><span><i class="sq" style="background:var(--s2)"></i>Methodology and policy</span></div>
      <p class="note">The two lines of CR8 that are about models rather than the book: new or recalibrated models, and changes in method or
        regulation (a supervisor's add-on, a new floor), summed over the bank's filings of the last year, as a share of its IRB risk exposure
        at their start. Left of the line, the change lowered it.${none ? ` ${none} more bank${none === 1 ? '' : 's'} filed no model change.` : ''}</p>`;
  }

  async function init() {
    if (!$('#ncPanel') || !window.BankNordic) return;
    await BankNordic.ready;
    if (!(await load())) { $('#ncPanel').innerHTML = '<div class="panel"><div class="empty">The Pillar 3 data did not load</div></div>'; return; }
    render();
    let rt, rw = window.innerWidth;
    window.addEventListener('resize', () => { clearTimeout(rt); rt = setTimeout(() => { if (window.innerWidth !== rw) { rw = window.innerWidth; render(); } }, 250); });
  }
  if (document.readyState === 'loading') document.addEventListener('DOMContentLoaded', init); else init();
})();
