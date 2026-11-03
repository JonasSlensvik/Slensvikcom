/* Banks · Capital — what the models say (2026-11-03).
   The capital-side companion to banks.html's exposure map: which portfolios each bank models itself (IRB-A, IRB-F)
   and which sit on the standardised approach, the risk weights that come out, where the parameters sit on the IRB
   formula, expected against unexpected loss, how the 2025 defaults compare with the PDs, the CRR3 output floor, and
   what Finanstilsynet found when it inspected the models.
   Data: ENEXT api.bank_pillar3 (the banks' Pillar 3 templates, research/banks/capital_reader.py: KM1, OV1, CMS1/2,
   CR4, CR6, CR6-A, CR9), api.bank_irb_status (approach tables, approvals, pending applications and the errors we found
   in each bank's own tables; research/banks/irb_status.json) and api.bank_pillar3_docs. banks.html passes in the
   names, api.bank_metrics and api.bank_supervision it already holds. Runbook: ../ENEXT/docs/banks.md → Capital. */
(() => {
  const API = 'https://api.slensvik.com/';
  const $ = (s, r = document) => r.querySelector(s);
  const esc = (s) => String(s ?? '').replace(/[&<>"]/g, (c) => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;' }[c]));
  const get = (p) => window.INTEL ? INTEL.safeGet(p) : fetch(API + p).then((r) => r.ok ? r.json() : null).catch(() => null);
  const ok = (v) => v != null && isFinite(v);
  const nb = (v, dp = 0) => !ok(v) ? '—' : (+v).toLocaleString('nb-NO', { minimumFractionDigits: dp, maximumFractionDigits: dp });
  const pc = (v, dp = 1) => !ok(v) ? '—' : (+v).toFixed(dp) + ' %';
  const bn = (mnok, dp) => !ok(mnok) ? '—' : 'NOK ' + nb(mnok / 1000, dp ?? (Math.abs(mnok) < 10000 ? 1 : 0)) + ' bn';
  const sum = (a) => a.reduce((s, x) => s + (x || 0), 0);
  const median = (a) => { const s = a.filter(ok).sort((x, y) => x - y); if (!s.length) return null; const m = s.length >> 1; return s.length % 2 ? s[m] : (s[m - 1] + s[m]) / 2; };

  const C = { A: 'var(--s1)', F: 'var(--s2)', SA: '#5a6377', R: 'var(--s3)', EL: 'var(--s2)', UL: 'var(--s1)' };
  const RANGES = ['0.00-0.15', '0.15-0.25', '0.25-0.50', '0.50-0.75', '0.75-2.50', '2.50-10.00', '10.00-100.00'];
  const RANGES_ALL = [...RANGES, 'default'];

  // portfolios: the IRB exposure classes (CR6) and the standardised ones (CR4) that hold the same kind of loan
  const SEG = [
    { id: 'gov', name: 'Governments & public', irb: ['cgcb', 'rgla', 'pse'], sa: ['sa_cgcb', 'sa_rgla', 'sa_pse', 'sa_mdb', 'sa_io'] },
    { id: 'inst', name: 'Institutions & covered bonds', irb: ['inst'], sa: ['sa_inst', 'sa_cb', 'sa_st'] },
    { id: 'corp', name: 'Corporates', irb: ['corp_other', 'corp_sme', 'corp_pr'], sa: ['sa_corp'] },
    { id: 'sl', name: 'Specialised lending & property', irb: ['corp_sl'], sa: ['sa_re_com', 'sa_re_com_ipre', 'sa_adc'] },
    { id: 'mort', name: 'Mortgages', irb: ['ret_re'], sa: ['sa_re_res', 'sa_re_res_ipre'] },
    { id: 'rsme', name: 'Retail SME', irb: ['ret_re_sme', 'ret_other_sme'], sa: [] },
    { id: 'cons', name: 'Consumer & other retail', irb: ['ret_other', 'ret_qrre', 'ret_pr'], sa: ['sa_retail'] },
    { id: 'oth', name: 'Equity, funds & other', irb: ['equity', 'other'], sa: ['sa_equity', 'sa_sub', 'sa_ciu', 'sa_other', 'sa_highrisk'] },
    { id: 'dflt', name: 'In default (SA)', irb: [], sa: ['sa_default'] },
  ];
  const SEG_BY = Object.fromEntries(SEG.map((s) => [s.id, s]));
  const FOCUS = ['mort', 'corp', 'sl', 'cons'];            // the segments the formula, loss and back-test views can show
  const IRB_ORDER = ['DNB', 'SB1NO', 'SBNOR', 'MING', 'SPOL', 'NONG', 'MORG'];

  // ── data ─────────────────────────────────────────────────────────────────────────────────────────────────────────
  const IX = new Map(), DATES = {};
  const key = (t, d, tpl, a, c, b, m) => [t, d, tpl, a, c, b, m].join('|');
  function index(rows) {
    for (const r of rows) {
      IX.set(key(r.ticker, r.ref_date, r.template, r.approach, r.exposure_class, r.bucket, r.metric), +r.value);
      const s = ((DATES[r.ticker] ||= {})[r.template] ||= new Set()); s.add(r.ref_date);
    }
  }
  const val = (t, d, tpl, m, c = '', a = '', b = '') => IX.get(key(t, d, tpl, a, c, b, m));
  const dates = (t, tpl) => [...((DATES[t] || {})[tpl] || [])].sort();
  const latest = (t, tpl) => dates(t, tpl).pop() || null;
  const latestYE = (t, tpl) => { const d = dates(t, tpl); return d.filter((x) => x.endsWith('-12-31')).pop() || d.pop() || null; };
  const dlabel = (d) => d ? new Date(d + 'T00:00:00Z').toLocaleString('en-GB', { day: 'numeric', month: 'short', year: 'numeric', timeZone: 'UTC' }) : '—';

  function classesOf(t, d) {                     // CR6 class blocks (not the totals) at one date
    const out = [];
    const seen = new Set();
    for (const k of IX.keys()) {
      const [tt, dd, tpl, a, c, b, m] = k.split('|');
      if (tt !== t || dd !== d || tpl !== 'CR6' || b || c === 'total' || m !== 'ead') continue;
      if (seen.has(a + c)) continue; seen.add(a + c);
      const g = (metric, bucket = '') => val(t, d, 'CR6', metric, c, a, bucket);
      // performing averages: EAD-weighted over the PD ranges outside default (default exposures carry PD 100 %)
      let pe = 0, pp = 0, pl = 0, pr = 0, pel = 0, hasB = false;
      for (const rg of RANGES) {
        const e = g('ead', rg); if (!ok(e) || e <= 0) continue; hasB = true;
        pe += e; if (ok(g('pd', rg))) pp += e * g('pd', rg); if (ok(g('lgd', rg))) pl += e * g('lgd', rg);
        pr += g('rwa', rg) || 0; pel += g('el', rg) || 0;
      }
      out.push({ appr: a === 'IRB' ? 'AIRB' : a, cls: c, ead: g('ead'), rwa: g('rwa'), density: ok(g('density')) ? g('density') : (g('rwa') / g('ead')) * 100,
        pd: g('pd'), lgd: g('lgd'), el: g('el'), prov: g('provisions'), obligors: g('obligors'), maturity: g('maturity'),
        def_ead: g('ead', 'default'), perf: hasB && pe > 0 ? { ead: pe, pd: pp / pe, lgd: pl / pe, rwa: pr, el: pel, density: 100 * pr / pe } : null });
    }
    return out;
  }
  function saRows(t, d) {                       // CR4: exposure after CCF and CRM, RWEA per class
    const out = {};
    for (const k of IX.keys()) {
      const [tt, dd, tpl, , c, , m] = k.split('|');
      if (tt !== t || dd !== d || tpl !== 'CR4' || m !== 'rwa') continue;
      const g = (metric) => val(t, d, 'CR4', metric, c);
      const post = ok(g('exp_post_on')) || ok(g('exp_post_off')) ? (g('exp_post_on') || 0) + (g('exp_post_off') || 0) : g('exp_post');
      out[c] = { exp: post, rwa: g('rwa'), density: g('density') };
    }
    // the property class split into residential / commercial / ADC: when the bank prints only the total, it stands for
    // the whole class (savings banks' property loans are mostly homes) and is marked as not split
    if (out.sa_re && !out.sa_re_res && !out.sa_re_com) out.sa_re_res = { ...out.sa_re, unsplit: true };
    return out;
  }

  let BANKS = [], ST = {}, NAMES = {}, METRICS = [], FSA = [], DOCS = [], FOCUS_SEG = 'mort';

  function bankModel(t) {
    const d6 = latest(t, 'CR6'), dK = latest(t, 'KM1'), dO = latest(t, 'OV1'), d4 = latest(t, 'CR4');
    const dM1 = latest(t, 'CMS1'), dM2 = latest(t, 'CMS2'), d9 = latestYE(t, 'CR9'), dA = latest(t, 'CR6A');
    const cls = d6 ? classesOf(t, d6) : [];
    const sa = d4 ? saRows(t, d4) : {};
    const km = (m) => dK ? val(t, dK, 'KM1', m) : undefined, ov = (m) => dO ? val(t, dO, 'OV1', m) : undefined;
    const cr6rwa = sum(cls.map((c) => c.rwa)), cr6ead = sum(cls.map((c) => c.ead));
    const credit = ov('credit'), csa = ov('credit_sa');
    const ovIrb = sum(['credit_firb', 'credit_airb', 'credit_slotting'].map(ov));
    // RWEA the IRB book carries beyond the models' own CR6 numbers (Norway's risk-weight floors, other add-ons), at the
    // CR6 date: a newer quarter's OV1 against an older CR6 would mix two balance sheets
    const o6 = (m) => d6 ? val(t, d6, 'OV1', m) : undefined;
    const gap = ok(o6('credit')) && ok(o6('credit_sa')) && cr6rwa ? o6('credit') - o6('credit_sa') - cr6rwa : null;
    const addon = gap == null || gap < -0.02 * cr6rwa ? null : gap > 0.005 * cr6rwa ? gap : 0;   // CR6 above OV1 (Møre): not comparable
    const base = dM1 ? (val(t, dM1, 'CMS1', 'floor_base', 'total') || val(t, dM1, 'CMS1', 'full_sa', 'total')) : null;
    const trea = km('trea') ?? ov('total');
    // the output-floor ratio needs TREA at the base's own date (Østlandet's latest KM1 is newer than its CMS1)
    const treaBase = dM1 ? (val(t, dM1, 'KM1', 'trea') ?? val(t, dM1, 'OV1', 'total') ?? val(t, dM1, 'CMS1', 'actual', 'total')) : null;
    const m2 = (m, c = 'total') => dM2 ? val(t, dM2, 'CMS2', m, c) : undefined;
    const segs = {};
    for (const s of SEG) {
      const parts = cls.filter((c) => s.irb.includes(c.cls));
      const A = parts.filter((c) => c.appr !== 'FIRB'), F = parts.filter((c) => c.appr === 'FIRB');
      const S = s.sa.map((c) => sa[c]).filter(Boolean);
      segs[s.id] = { A: { ead: sum(A.map((c) => c.ead)), rwa: sum(A.map((c) => c.rwa)) }, F: { ead: sum(F.map((c) => c.ead)), rwa: sum(F.map((c) => c.rwa)) },
        SA: { ead: sum(S.map((x) => x.exp)), rwa: sum(S.map((x) => x.rwa)), unsplit: S.some((x) => x.unsplit) }, parts };
    }
    const irbEad = sum(Object.values(segs).map((s) => s.A.ead + s.F.ead)), saEad = d4 ? sum(Object.values(segs).map((s) => s.SA.ead)) : null;
    return { t, d6, dK, dO, d4, d9, dA, dM1, dM2, cls, sa, segs, trea, treaBase, cet1: km('cet1_ratio'), prefloor: km('trea_prefloor'),
      credit, csa, ovIrb, cr6rwa, cr6ead, addon, base, irbShare: saEad != null && irbEad + saEad ? 100 * irbEad / (irbEad + saEad)
        : dA && ok(val(t, dA, 'CR6A', 'pct_irb', 'total')) ? val(t, dA, 'CR6A', 'pct_irb', 'total') : null,
      cr6aIrb: dA ? val(t, dA, 'CR6A', 'pct_irb', 'total') : null, cr6aPpu: dA ? val(t, dA, 'CR6A', 'pct_ppu', 'total') : null,
      cr6aRoll: dA ? val(t, dA, 'CR6A', 'pct_rollout', 'total') : null,
      vsSA: ok(m2('modelled')) && m2('modelled_as_sa') ? 100 * m2('modelled') / m2('modelled_as_sa') : null,
      op: ov('op'), floorPct: ov('floor_pct') };
  }

  // the bank's latest REA / CET1 from its quarterly report where the Pillar 3 document prints no numbers (image tables)
  function reportFallback(t) {
    const rows = METRICS.filter((r) => r.ticker === t && (r.metric === 'risk_exposure_amount' || r.metric === 'cet1_ratio_pct'));
    const per = rows.map((r) => r.period).sort().pop();
    const g = (m) => (rows.find((r) => r.period === per && r.metric === m) || {}).value;
    return per ? { period: per, trea: g('risk_exposure_amount'), cet1: g('cet1_ratio_pct') } : null;
  }

  // ── the IRB formula (CRR3: no 1.06 scaling factor; Norway dropped it with CRR3) ──────────────────────────────────
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
  function corr(seg, pd) {
    if (seg === 'mort') return 0.15;
    if (seg === 'cons') { const w = (1 - Math.exp(-35 * pd)) / (1 - Math.exp(-35)); return 0.03 * w + 0.16 * (1 - w); }
    const w = (1 - Math.exp(-50 * pd)) / (1 - Math.exp(-50)); return 0.12 * w + 0.24 * (1 - w);
  }
  function rwOf(seg, pd, lgd, M = 2.5) {          // risk weight (fraction) for a PD and LGD given as fractions
    pd = Math.min(0.999, Math.max(pd, 0.0005));
    const R = corr(seg, pd);
    let K = lgd * (ncdf((ninv(pd) + Math.sqrt(R) * G999) / Math.sqrt(1 - R)) - pd);
    if (seg === 'corp' || seg === 'sl') { const b = Math.pow(0.11852 - 0.05478 * Math.log(pd), 2); K *= (1 + (M - 2.5) * b) / (1 - 1.5 * b); }
    return 12.5 * K;
  }

  // ── shared tooltip ───────────────────────────────────────────────────────────────────────────────────────────────
  let TT;
  function tip(html, e) {
    if (!TT) { TT = document.createElement('div'); TT.className = 'cap-tt'; document.body.appendChild(TT); }
    if (!html) { TT.style.display = 'none'; return; }
    TT.innerHTML = html; TT.style.display = 'block';
    const w = TT.offsetWidth, h = TT.offsetHeight, vw = window.innerWidth, vh = window.innerHeight;
    TT.style.left = Math.max(6, Math.min(vw - w - 8, e.clientX + 14)) + 'px';
    TT.style.top = Math.max(6, Math.min(vh - h - 8, e.clientY + 14)) + 'px';
  }
  function bindTips(root, fn) {                    // elements with data-tip="id" ask fn(id) for their tooltip
    root.addEventListener('pointermove', (e) => { const el = e.target.closest('[data-tip]'); tip(el ? fn(el.dataset.tip, el) : null, e); });
    root.addEventListener('pointerleave', () => tip(null));
    root.addEventListener('focusin', (e) => { const el = e.target.closest('[data-tip]'); if (el) { const r = el.getBoundingClientRect(); tip(fn(el.dataset.tip, el), { clientX: r.right, clientY: r.top }); } });
    root.addEventListener('focusout', () => tip(null));
  }
  const tline = (color, label, value) => `<div class="tl"><i style="background:${color}"></i><b>${value}</b> <span>${esc(label)}</span></div>`;

  // ── hero ─────────────────────────────────────────────────────────────────────────────────────────────────────────
  function renderHero(models, saBanks) {
    const irb = models.filter((m) => ST[m.t]);
    const treaIrb = sum(irb.map((m) => m.trea)), treaAll = treaIrb + sum(saBanks.map((b) => b.trea));
    const mort = irb.map((m) => { const c = m.cls.find((x) => x.cls === 'ret_re'); return c ? [m.t, c.density] : null; }).filter(Boolean);
    const mlo = Math.min(...mort.map((x) => x[1])), mhi = Math.max(...mort.map((x) => x[1]));
    const under = mort.filter(([, v]) => v < 25).length;
    const adds = irb.filter((m) => m.addon > 0), addSum = sum(adds.map((m) => m.addon));
    const vs = irb.map((m) => m.vsSA).filter(ok);
    const fl = irb.filter((m) => m.base && m.treaBase).map((m) => [m.t, 100 * m.treaBase / m.base]).sort((a, b) => a[1] - b[1]);
    const cal = irb.map((m) => calib(m.t, 'mort')).filter((c) => c && c.n > 500 && !c.suspect).map((c) => c.k / c.n / c.pd);
    const kp = (l, v, s) => `<div class="kpi"><div class="kl">${l}</div><div class="kv">${v}</div><div class="ks">${s}</div></div>`;
    $('#capKpis').innerHTML = [
      kp('Banks on their own models', `${irb.length} <small>of ${irb.length + saBanks.length}</small>`,
        `They carry ${pc(100 * treaIrb / treaAll, 0)} of the listed banks' risk-weighted assets. The other ${saBanks.length} use the standardised approach.`),
      kp('Mortgage risk weight, models', `${mlo.toFixed(0)}–${mhi.toFixed(0)} <small>%</small>`,
        `${under} of ${mort.length} sit below the 25 % floor in force since 1 July 2025, so the floor, not the model, sets their mortgage capital.`),
      kp('Lifted by floors and add-ons', `${bn(addSum)}`,
        `Risk-weighted assets on top of what ${adds.length} banks' models produce: ${adds.map((m) => `${m.t} ${bn(m.addon)}`).join(', ')}.`),
      kp('Model RWA vs the standard', `${Math.min(...vs).toFixed(0)}–${Math.max(...vs).toFixed(0)} <small>%</small>`,
        `What the models charge for their portfolios, as a share of what the standardised approach would (CMS2).` +
        (fl.length ? ` Closest to the 72.5 % output floor of 2030: ${fl[0][0]} at ${fl[0][1].toFixed(1)} %.` : '')),
    ].join('');
    const calTxt = cal.length ? `In 2025, mortgage defaults ran at ${Math.round(100 * Math.min(...cal))}–${Math.round(100 * Math.max(...cal))} % of the PDs the models had assigned: conservative, in a calm year.` : '';
    $('#capVerdict').innerHTML = `<div class="lab">What the reports say</div><div class="val">The floors set the capital; the models set the ranking.</div>
      <div class="why">${esc(calTxt)} Every IRB bank's latest inspection by Finanstilsynet found weaknesses in how the models are
      estimated or used — most often IRB estimates that play little part in credit decisions, and collateral counted in LGD
      where the rules do not allow it.</div>`;
  }

  // ── who models what ──────────────────────────────────────────────────────────────────────────────────────────────
  function renderMatrix(models, saBanks) {
    const cols = SEG.filter((s) => s.id !== 'dflt');
    const irb = IRB_ORDER.map((t) => models.find((m) => m.t === t)).filter(Boolean);
    let h = `<thead><tr><th class="l">Bank</th>${cols.map((s) => `<th>${esc(s.name)}</th>`).join('')}<th>Credit exposure on IRB</th></tr></thead><tbody>`;
    for (const m of irb) {
      const st = ST[m.t] || {};
      const since = [st.irb_since ? `IRB ${st.irb_since}` : null, st.airb_corp_since ? `A-IRB corporates ${st.airb_corp_since}` : null].filter(Boolean).join(' · ');
      h += `<tr><td class="l"><b>${esc(NAMES[m.t] || m.t)}</b><small>${esc(since || 'IRB')} · ${esc(dlabel(m.d6))}</small></td>`;
      for (const s of cols) {
        const g = m.segs[s.id], tot = g.A.ead + g.F.ead + g.SA.ead;
        if (!tot) { h += '<td class="c na">·</td>'; continue; }
        const parts = [['A', g.A], ['F', g.F], ['SA', g.SA]].filter(([, x]) => x.ead > 0).sort((a, b) => b[1].ead - a[1].ead);
        const lead = parts[0][0], second = parts[1] && parts[1][1].ead / tot >= 0.15 ? parts[1][0] : null;
        const lbl = { A: 'A-IRB', F: 'F-IRB', SA: 'SA' };
        const rwa = g.A.rwa + g.F.rwa + g.SA.rwa;
        h += `<td class="c" tabindex="0" data-tip="${m.t}|${s.id}"><span class="am-bar">${parts.map(([k, x]) => `<i style="flex:${x.ead};background:${C[k]}"></i>`).join('')}</span>` +
          `<span class="am-lab"><b>${lbl[lead]}</b>${second ? ` + ${lbl[second]}` : ''}</span><span class="am-sub num">${nb(tot / 1000, tot < 10000 ? 1 : 0)} bn · RW ${(100 * rwa / tot).toFixed(0)} %</span></td>`;
      }
      const share = m.irbShare;
      h += `<td class="c" tabindex="0" data-tip="${m.t}|share"><span class="am-meter"><i style="width:${(share || 0).toFixed(1)}%"></i></span><span class="am-sub num">${pc(share, 0)}` +
        `${ok(m.cr6aPpu) ? ` · PPU ${pc(m.cr6aPpu, 0)}` : ''}</span></td></tr>`;
    }
    h += '</tbody>';
    const t = $('#capMatrix'); t.innerHTML = h;
    bindTips(t, (id) => {
      const [tk, sid] = id.split('|'), m = models.find((x) => x.t === tk), st = ST[tk] || {};
      if (sid === 'share') return `<b>${esc(NAMES[tk] || tk)}</b><br>Exposure after CCF and CRM on IRB: <b>${pc(m.irbShare, 1)}</b> (CR6 against CR4)` +
        (ok(m.cr6aIrb) ? `<br>The bank's own CR6-A: IRB ${pc(m.cr6aIrb, 1)}, permanent partial use ${pc(m.cr6aPpu, 1)}${ok(m.cr6aRoll) ? `, roll-out ${pc(m.cr6aRoll, 1)}` : ''}` : '') +
        (st.methods ? '<div class="tm">' + st.methods.map((x) => `${esc(x.portfolio)}: <b>${esc(x.method)}</b>`).join('<br>') + '</div>' : '');
      const g = m.segs[sid], s = SEG_BY[sid];
      const row = (k, lab, x) => x.ead > 0 ? tline(C[k], `${lab}: ${bn(x.ead)} exposure, RW ${(100 * x.rwa / x.ead).toFixed(1)} %`, pc(100 * x.ead / (g.A.ead + g.F.ead + g.SA.ead), 0)) : '';
      const cls = g.parts.map((c) => `${esc(c.cls)} (${c.appr}): PD ${pc(c.perf ? c.perf.pd : c.pd, 2)}, LGD ${pc(c.perf ? c.perf.lgd : c.lgd, 1)}`).join('<br>');
      return `<b>${esc(NAMES[tk] || tk)} · ${esc(s.name)}</b>${row('A', 'A-IRB', g.A)}${row('F', 'F-IRB', g.F)}${row('SA', 'Standardised', g.SA)}` +
        (cls ? `<div class="tm">${cls}<br><span class="faint">PD and LGD: performing exposure, EAD-weighted</span></div>` : '') +
        (g.SA.unsplit ? '<div class="tm faint">The bank prints property loans as one class: shown under mortgages.</div>' : '');
    });
    $('#capMatrixAsof').textContent = `CR6 (IRB) and CR4 (standardised), exposure after CCF and credit-risk mitigation`;
    // the standardised banks
    const chips = saBanks.sort((a, b) => (b.trea || 0) - (a.trea || 0)).map((b) => {
      const via = b.irbVia ? `<span class="chip-s" title="Risk weights from part-owned IRB mortgage companies it consolidates (SpareBank 1 Boligkreditt, BN Bank): ${esc(bn(b.irbVia))} of RWA">IRB via JV</span>` : '';
      return `<div class="sa-chip" tabindex="0" data-tip="${b.t}"><b>${esc(NAMES[b.t] || b.t)}</b>${via}<span class="num">${bn(b.trea)} RWA · CET1 ${pc(b.cet1, 1)}</span>` +
        `<span class="sa-bar">${b.parts.map(([k, v, c]) => `<i style="flex:${v};background:${c}" title="${esc(k)}"></i>`).join('')}</span></div>`;
    }).join('');
    const box = $('#capSA'); box.innerHTML = chips;
    bindTips(box, (tk) => {
      const b = saBanks.find((x) => x.t === tk);
      return `<b>${esc(NAMES[tk] || tk)}</b> · ${esc(b.src)}<br>` + (b.parts.length ? b.parts.map(([k, v, c]) => tline(c, k, bn(v))).join('')
        : `Risk-weighted assets ${bn(b.trea)}; the report prints no breakdown by risk type in text form.`) +
        (b.mortRW ? `<div class="tm">Mortgages on the standardised approach: RW <b>${pc(b.mortRW, 1)}</b></div>` : '');
    });
  }

  // ── labels that do not collide: the first free spot around each point, a leader line when pushed out ───────────
  function placeLabels(items, { cw = 5.6, fh = 11, bounds = null, avoid = [] } = {}) {
    const boxes = [];
    const clash = (b, self) => boxes.some((o) => b[0] < o[2] && b[2] > o[0] && b[1] < o[3] && b[3] > o[1]) ||
      avoid.some((c) => c !== self && b[0] < c.x + c.r && b[2] > c.x - c.r && b[1] < c.y + c.r && b[3] > c.y - c.r);
    for (const it of [...items].sort((a, b) => (b.prio || 0) - (a.prio || 0) || a.x - b.x)) {
      const w = it.text.length * cw + 2, r = it.r || 5, bd = it.bounds || bounds;
      const cand = [];
      for (const d of [0, 12, 24, 38]) cand.push(
        [it.x, it.y - r - 4 - d, 'middle'], [it.x, it.y + r + fh - 1 + d, 'middle'],
        [it.x + r + 3 + d, it.y + 3.5, 'start'], [it.x - r - 3 - d, it.y + 3.5, 'end'],
        [it.x + r + 2 + d * 0.7, it.y - r - 3 - d * 0.7, 'start'], [it.x - r - 2 - d * 0.7, it.y - r - 3 - d * 0.7, 'end'],
        [it.x + r + 2 + d * 0.7, it.y + r + fh - 2 + d * 0.7, 'start'], [it.x - r - 2 - d * 0.7, it.y + r + fh - 2 + d * 0.7, 'end']);
      let pick = null;
      for (const [lx, ly, anchor] of cand) {
        const x0 = anchor === 'start' ? lx : anchor === 'end' ? lx - w : lx - w / 2, b = [x0, ly - fh + 2, x0 + w, ly + 2];
        if (bd && (b[0] < bd[0] || b[2] > bd[2] || b[1] < bd[1] || b[3] > bd[3])) continue;
        if (!clash(b, it.circle)) { pick = { lx, ly, anchor, b }; break; }
      }
      if (!pick) pick = { lx: cand[0][0], ly: cand[0][1], anchor: cand[0][2], b: null };
      if (pick.b) boxes.push(pick.b);
      Object.assign(it, pick, { far: Math.hypot(pick.lx - it.x, pick.ly - 4 - it.y) > r + 15 });
    }
    return items;
  }
  const labelSvg = (it, color = 'var(--text2)') => (it.far ? `<line x1="${it.x.toFixed(1)}" y1="${it.y.toFixed(1)}" x2="${it.lx.toFixed(1)}" y2="${(it.ly - 4).toFixed(1)}" stroke="rgba(255,255,255,.2)"/>` : '') +
    `<text x="${it.lx.toFixed(1)}" y="${it.ly.toFixed(1)}" text-anchor="${it.anchor}" fill="${color}" font-size="9.5">${esc(it.text)}</text>`;
  const LOG_TICKS = [0.01, 0.02, 0.05, 0.1, 0.2, 0.5, 1, 2, 5, 10, 20, 50];
  const logTicks = (lo, hi) => { const t = LOG_TICKS.filter((v) => v >= lo * 0.999 && v <= hi * 1.001); return t.length > 7 ? t.filter((_, i) => i % 2 === 0) : t; };
  const tickTxt = (v) => (v < 0.1 ? v.toFixed(2) : v < 1 ? String(+v.toFixed(2)) : String(v)) + ' %';

  // ── risk weights by portfolio ────────────────────────────────────────────────────────────────────────────────────
  function renderTracks(models) {
    const el = $('#capTracks'), W = el.clientWidth || 900, narrow = W < 640;
    const rows = ['mort', 'corp', 'sl', 'rsme', 'cons'];
    const L = narrow ? 92 : 178, R = 18, T = 26, rowH = narrow ? 70 : 66, H = T + rows.length * rowH + 14, X1 = 120;
    const x = (v) => L + Math.min(v, X1) / X1 * (W - L - R);
    let s = `<svg viewBox="0 0 ${W} ${H}" width="${W}" height="${H}" role="img" aria-label="Risk weights by portfolio">`;
    for (let v = 0; v <= X1; v += 20) s += `<line x1="${x(v)}" x2="${x(v)}" y1="${T - 6}" y2="${H - 10}" stroke="var(--grid)"/><text x="${x(v)}" y="${T - 10}" text-anchor="middle" fill="var(--text4)" font-size="9">${v} %</text>`;
    const pts = [];
    rows.forEach((sid, i) => {
      const y = T + i * rowH + rowH / 2;
      s += `<text x="${L - 12}" y="${y + 3}" text-anchor="end" fill="var(--text2)" font-size="10.5">${esc(narrow ? SEG_BY[sid].name.split(' ')[0] : SEG_BY[sid].name)}</text>` +
           `<line x1="${L}" x2="${W - R}" y1="${y}" y2="${y}" stroke="rgba(255,255,255,.08)"/>`;
      if (sid === 'mort') s += `<line x1="${x(25)}" x2="${x(25)}" y1="${y - rowH / 2 + 4}" y2="${y + rowH / 2 - 4}" stroke="var(--amber)" stroke-width="1.5"/>` +
        `<text x="${x(25) + 4}" y="${y + rowH / 2 - 6}" fill="var(--amber)" font-size="9">floor 25 %</text>`;
      if (sid === 'sl') s += `<line x1="${x(35)}" x2="${x(35)}" y1="${y - rowH / 2 + 4}" y2="${y + rowH / 2 - 4}" stroke="var(--amber)" stroke-width="1" opacity=".65"/>` +
        `<text x="${x(35) + 4}" y="${y + rowH / 2 - 6}" fill="var(--amber)" font-size="9" opacity=".85">CRE floor 35 %</text>`;
      for (const m of models) {          // the standardised approach for the same kind of loan, any bank: hollow
        const g = m.segs[sid]; if (!g || g.SA.ead < 1000 || (sid === 'mort' && g.SA.unsplit)) continue;
        pts.push({ kind: 'SA', t: m.t, sid, v: 100 * g.SA.rwa / g.SA.ead, ead: g.SA.ead, y });
      }
      for (const m of models) {
        const g = m.segs[sid]; if (!g) continue;
        for (const k of ['A', 'F']) if (g[k].ead > 500) pts.push({ kind: k, t: m.t, sid, v: 100 * g[k].rwa / g[k].ead, ead: g[k].ead, y });
      }
    });
    for (const p of pts) { p.cx = x(p.v); p.circle = { x: p.cx, y: p.y, r: p.kind === 'SA' ? 4.5 : 5.5 }; }
    for (const p of pts.filter((q) => q.kind === 'SA')) s += `<circle cx="${p.cx}" cy="${p.y}" r="4.5" fill="none" stroke="var(--text3)" stroke-width="1.3"/>`;
    const irb = pts.filter((q) => q.kind !== 'SA');
    for (const p of irb) s += `<circle cx="${p.cx}" cy="${p.y}" r="5.5" fill="${C[p.kind]}" stroke="var(--bg1)" stroke-width="2"/>`;
    // on a phone only each row's lowest and highest model carry a name; the rest are in the tooltip
    const shown = narrow ? irb.filter((p) => { const row = irb.filter((q) => q.sid === p.sid).map((q) => q.v); return p.v === Math.min(...row) || p.v === Math.max(...row); }) : irb;
    const labs = placeLabels(shown.map((p) => ({ x: p.cx, y: p.y, r: 5.5, text: p.t + (p.kind === 'F' ? '·F' : ''), circle: p.circle,
      bounds: [L, p.y - rowH / 2 + 1, W - R, p.y + rowH / 2 - 1] })), { avoid: pts.map((p) => p.circle) });
    for (const it of labs) s += labelSvg(it, 'var(--text3)');
    s += '</svg>';
    el.style.height = H + 'px'; el.innerHTML = s;
    el.onpointermove = (e) => {
      const r = el.querySelector('svg').getBoundingClientRect(), px = (e.clientX - r.left) * (W / r.width), py = (e.clientY - r.top) * (H / r.height);
      let best = null, bd = 1e9;
      for (const p of pts) { const d = Math.hypot(p.cx - px, p.y - py); if (d < bd) { bd = d; best = p; } }
      if (!best || bd > 22) { tip(null); return; }
      const m = models.find((q) => q.t === best.t), lbl = { A: 'A-IRB model', F: 'F-IRB model', SA: 'standardised approach' }[best.kind];
      tip(`<b>${esc(NAMES[best.t] || best.t)} · ${esc(SEG_BY[best.sid].name)}</b><br>${esc(lbl)}: risk weight <b>${pc(best.v, 1)}</b><br>Exposure ${bn(best.ead)} · ${esc(dlabel(best.kind === 'SA' ? m.d4 : m.d6))}` +
        (best.sid === 'mort' && best.kind !== 'SA' && best.v < 25 ? `<div class="tm">Below the 25 % floor: the floor lifts this book by about ${bn(best.ead * (25 - best.v) / 100)} of RWA.</div>` : ''), e);
    };
    el.onpointerleave = () => tip(null);
  }

  // ── the capital curve: performing PD × LGD against the formula's iso-risk-weight lines ──────────────────────────
  function segPoints(models, sid) {
    const out = [];
    for (const m of models) {
      const parts = (m.segs[sid] || {}).parts || [];
      for (const appr of ['AIRB', 'FIRB']) {
        const ps = parts.filter((c) => (c.appr === 'FIRB') === (appr === 'FIRB') && c.ead > 0);
        if (!ps.length) continue;
        const w = (f) => { let e = 0, s = 0; for (const c of ps) { const p = c.perf || c; const v = f(p); if (ok(v) && ok(p.ead)) { e += p.ead; s += p.ead * v; } } return e ? s / e : null; };
        const ead = sum(ps.map((c) => c.ead)), pead = sum(ps.map((c) => (c.perf || c).ead));
        const pd = w((p) => p.pd), lgd = w((p) => p.lgd);
        const prwa = sum(ps.map((c) => (c.perf || c).rwa));
        out.push({ t: m.t, appr, ead, pd, lgd, density: 100 * sum(ps.map((c) => c.rwa)) / ead, pdens: pead ? 100 * prwa / pead : null,
          el: sum(ps.map((c) => c.el)), prov: sum(ps.map((c) => c.prov)), rwa: sum(ps.map((c) => c.rwa)), def: sum(ps.map((c) => c.def_ead)),
          mat: w((p) => p.maturity), cet1: m.cet1, d: m.d6, cls: ps.map((c) => c.cls) });
      }
    }
    return out;
  }
  const SEG_KIND = { mort: 'mort', corp: 'corp', sl: 'sl', cons: 'cons' };
  function renderCurve(models) {
    const el = $('#capCurve'), W = el.clientWidth || 640, H = 340, L = 46, R = 84, T = 16, B = 34;
    const sid = FOCUS_SEG, kind = SEG_KIND[sid];
    const pts = segPoints(models, sid).filter((p) => ok(p.pd) && ok(p.lgd) && p.pd > 0);
    if (!pts.length) { el.innerHTML = '<div class="empty">No IRB bank reports this portfolio</div>'; return; }
    const pmin = Math.min(...pts.map((p) => p.pd)), pmax = Math.max(...pts.map((p) => p.pd));
    const xlo = Math.max(0.03, pmin / 2.6), xhi = Math.min(40, pmax * 2.6), yMax = Math.min(80, Math.max(30, Math.ceil(Math.max(...pts.map((p) => p.lgd)) * 1.5 / 10) * 10));
    const x0 = Math.log10(xlo), x1 = Math.log10(xhi);
    const x = (pd) => L + (Math.log10(Math.min(Math.max(pd, xlo), xhi)) - x0) / (x1 - x0) * (W - L - R), y = (lgd) => T + (1 - lgd / yMax) * (H - T - B);
    let s = `<svg viewBox="0 0 ${W} ${H}" width="${W}" height="${H}" role="img" aria-label="PD and LGD per bank with the IRB risk-weight lines">`;
    for (const pd of logTicks(xlo, xhi)) s += `<line x1="${x(pd)}" x2="${x(pd)}" y1="${T}" y2="${H - B}" stroke="var(--grid)"/><text x="${x(pd)}" y="${H - B + 13}" text-anchor="middle" fill="var(--text4)" font-size="9">${tickTxt(pd)}</text>`;
    for (let l = 0; l <= yMax; l += 10) s += `<line x1="${L}" x2="${W - R}" y1="${y(l)}" y2="${y(l)}" stroke="var(--grid)"/><text x="${L - 6}" y="${y(l) + 3}" text-anchor="end" fill="var(--text4)" font-size="9">${l} %</text>`;
    s += `<text x="${(L + W - R) / 2}" y="${H - 6}" text-anchor="middle" fill="var(--text3)" font-size="9.5">PD, performing exposure (log scale)</text>` +
         `<text x="12" y="${(T + H - B) / 2}" transform="rotate(-90 12 ${(T + H - B) / 2})" text-anchor="middle" fill="var(--text3)" font-size="9.5">LGD</text>`;
    // lines of equal risk weight: the LGD at which the formula gives RW = level, at each PD
    const levels = sid === 'mort' ? [10, 15, 20, 25, 35] : sid === 'cons' ? [15, 25, 35, 50, 75] : [25, 40, 60, 80, 100];
    const used = [];
    for (const lv of levels) {
      let d = '', exit = null, enter = false;
      for (let i = 0; i <= 160; i++) {
        const pd = Math.pow(10, x0 + (x1 - x0) * i / 160);
        const lgd = 100 * (lv / 100) / rwOf(kind, pd / 100, 1);
        if (lgd > yMax) { if (enter) break; continue; }
        d += `${enter ? 'L' : 'M'}${x(pd).toFixed(1)},${y(lgd).toFixed(1)}`; enter = true; exit = [x(pd), y(lgd)];
      }
      if (!d) continue;
      const hot = sid === 'mort' && lv === 25;
      s += `<path d="${d}" fill="none" stroke="${hot ? 'var(--amber)' : 'rgba(255,255,255,.2)'}" stroke-width="${hot ? 1.6 : 1}"/>`;
      let ly = exit[1] + 3;
      while (used.some((u) => Math.abs(u - ly) < 10)) ly += 10;
      used.push(ly);
      s += `<text x="${W - R + 4}" y="${ly}" fill="${hot ? 'var(--amber)' : 'var(--text3)'}" font-size="9">RW ${lv} %${hot ? ' floor' : ''}</text>`;
    }
    const mx = Math.max(...pts.map((p) => p.ead), 1);
    for (const p of pts) { p.cx = x(p.pd); p.cy = y(p.lgd); p.r = 4 + 9 * Math.sqrt(p.ead / mx); p.circle = { x: p.cx, y: p.cy, r: p.r }; }
    for (const p of [...pts].sort((a, b) => b.ead - a.ead)) {
      const c = p.appr === 'FIRB' ? C.F : C.A;
      s += `<circle cx="${p.cx.toFixed(1)}" cy="${p.cy.toFixed(1)}" r="${p.r.toFixed(1)}" fill="${c}" fill-opacity=".22" stroke="${c}" stroke-width="1.6"/>`;
    }
    const labs = placeLabels(pts.map((p) => ({ x: p.cx, y: p.cy, r: p.r, text: p.t + (p.appr === 'FIRB' ? '·F' : ''), circle: p.circle, prio: p.ead })),
      { bounds: [L + 2, T, W - R, H - B], avoid: pts.map((p) => p.circle) });
    for (const it of labs) s += labelSvg(it);
    el.style.height = H + 'px'; el.innerHTML = s + '</svg>';
    el.onpointermove = (e) => {
      const rc = el.querySelector('svg').getBoundingClientRect(), px = (e.clientX - rc.left) * (W / rc.width), py = (e.clientY - rc.top) * (H / rc.height);
      let best = null, bd = 1e9;
      for (const p of pts) { const d = Math.hypot(p.cx - px, p.cy - py) - p.r; if (d < bd) { bd = d; best = p; } }
      if (!best || bd > 16) { tip(null); return; }
      const f = 100 * rwOf(kind, best.pd / 100, best.lgd / 100, ok(best.mat) && best.mat > 1 ? best.mat : 2.5);
      tip(`<b>${esc(NAMES[best.t] || best.t)} · ${esc(SEG_BY[sid].name)} · ${best.appr === 'FIRB' ? 'F-IRB' : 'A-IRB'}</b><br>` +
        `PD <b>${pc(best.pd, 2)}</b> · LGD <b>${pc(best.lgd, 1)}</b> (performing, EAD-weighted)<br>` +
        `The formula at these averages: RW ${pc(f, 1)}<br>Reported: RW ${pc(best.pdens, 1)} on performing exposure, ${pc(best.density, 1)} with defaults<br>` +
        `Exposure ${bn(best.ead)}${best.def ? `, of which in default ${bn(best.def)}` : ''} · ${esc(dlabel(best.d))}` +
        `<div class="tm">The formula is curved, so a portfolio spread over many PD grades does not land exactly where its averages do.</div>`, e);
    };
    el.onpointerleave = () => tip(null);
    $('#capCurveNote').textContent = sid === 'corp' || sid === 'sl'
      ? 'Lines: the CRR3 corporate formula at 2.5 years maturity, without the SME size adjustment. F-IRB banks use the supervisory LGD (40 % senior unsecured) reduced for eligible collateral.'
      : sid === 'mort' ? 'Lines: the retail mortgage formula (correlation 15 %). Below the amber line the portfolio sits under the 25 % floor, which Norway sets on the portfolio average.'
      : 'Lines: the other-retail formula (correlation 3–16 %, falling with PD).';
  }

  // ── expected and unexpected loss per NOK 100 lent ───────────────────────────────────────────────────────────────
  function renderLoss(models) {
    const el = $('#capLoss'), W = el.clientWidth || 640, sid = FOCUS_SEG;
    const pts = segPoints(models, sid).filter((p) => p.ead > 0 && ok(p.rwa)).map((p) => ({ ...p, elp: 100 * (p.el || 0) / p.ead, ulp: 8 * p.density / 100,
      prp: 100 * Math.abs(p.prov || 0) / p.ead, cetp: ok(p.cet1) ? p.cet1 * p.density / 100 : null })).sort((a, b) => (b.elp + b.ulp) - (a.elp + a.ulp));
    if (!pts.length) { el.innerHTML = '<div class="empty">No IRB bank reports this portfolio</div>'; return; }
    const L = 96, R = 64, T = 24, rowH = 28, H = T + pts.length * rowH + 28;
    const mx = Math.max(...pts.map((p) => Math.max(p.elp + p.ulp, p.cetp || 0, p.prp)), sid === 'mort' ? 2.2 : 1) * 1.06;
    const x = (v) => L + v / mx * (W - L - R);
    let s = `<svg viewBox="0 0 ${W} ${H}" width="${W}" height="${H}" role="img" aria-label="Expected and unexpected loss per NOK 100 of exposure">`;
    const step = mx > 8 ? 2 : mx > 4 ? 1 : 0.5;
    for (let v = 0; v <= mx; v += step) s += `<line x1="${x(v)}" x2="${x(v)}" y1="${T - 4}" y2="${H - 24}" stroke="var(--grid)"/><text x="${x(v)}" y="${H - 10}" text-anchor="middle" fill="var(--text4)" font-size="9">${v} %</text>`;
    if (sid === 'mort') s += `<line x1="${x(2)}" x2="${x(2)}" y1="${T - 8}" y2="${H - 24}" stroke="var(--amber)" stroke-width="1.5"/><text x="${x(2) + 4}" y="${T - 11}" fill="var(--amber)" font-size="9">UL at the 25 % floor: 2 %</text>`;
    pts.forEach((p, i) => {
      const y = T + i * rowH, bh = 12, x0 = x(0), xe = x(p.elp), xu = x(p.elp + p.ulp), lab = (p.elp + p.ulp).toFixed(2) + ' %';
      s += `<text x="${L - 10}" y="${y + 10}" text-anchor="end" fill="var(--text2)" font-size="10">${esc(p.t)}${p.appr === 'FIRB' ? ' ·F' : ''}</text>`;
      if (ok(p.cetp) && x(p.cetp) > xu) s += `<line x1="${xu + 2}" x2="${x(p.cetp)}" y1="${y + bh / 2 + 1}" y2="${y + bh / 2 + 1}" stroke="var(--text4)" stroke-width="1"/>` +
        `<line x1="${x(p.cetp)}" x2="${x(p.cetp)}" y1="${y + 2}" y2="${y + bh}" stroke="var(--text3)" stroke-width="2"/>`;
      s += `<rect x="${x0}" y="${y + 1}" width="${Math.max(0.5, xe - x0 - 1)}" height="${bh}" rx="2" fill="${C.EL}"/>` +
        `<rect x="${xe + 1}" y="${y + 1}" width="${Math.max(0.5, xu - xe - 1)}" height="${bh}" rx="2" fill="${C.UL}"/>` +
        (p.prp > 0 ? `<line x1="${x(p.prp)}" x2="${x(p.prp)}" y1="${y - 2}" y2="${y + bh + 3}" stroke="#e8eef7" stroke-width="2"/>` : '');
      const lx = ok(p.cetp) && x(p.cetp) > xu ? x(p.cetp) + 6 : xu + 6;
      s += `<text x="${lx}" y="${y + 10}" fill="var(--text2)" font-size="10" class="num">${lab}</text>` +
        `<rect x="0" y="${y - 4}" width="${W}" height="${rowH}" fill="transparent" data-i="${i}"/>`;
    });
    el.style.height = H + 'px'; el.innerHTML = s + '</svg>';
    el.onpointermove = (e) => {
      const i = e.target.dataset ? +e.target.dataset.i : NaN, p = pts[i];
      if (!p) { tip(null); return; }
      tip(`<b>${esc(NAMES[p.t] || p.t)} · ${esc(SEG_BY[sid].name)}${p.appr === 'FIRB' ? ' · F-IRB' : ''}</b>` +
        tline(C.EL, `expected loss, ${bn(p.el)}`, pc(p.elp, 2)) + tline(C.UL, `unexpected loss, 8 % of ${bn(p.rwa)} RWA`, pc(p.ulp, 2)) +
        tline('#e8eef7', `provisions held, ${bn(Math.abs(p.prov || 0))}`, pc(p.prp, 2)) +
        (ok(p.cetp) ? tline('var(--text3)', `CET1 behind it at the bank's ${pc(p.cet1, 1)} ratio, ${bn(p.cet1 / 100 * p.rwa)}`, pc(p.cetp, 2)) : '') +
        `<div class="tm">Per NOK 100 of exposure (${bn(p.ead)}), from the models' own numbers before any floor. The formula sets capital
          for a one-in-a-thousand-year loss year; EL is what the bank expects to lose in an average one.</div>`, e);
    };
    el.onpointerleave = () => tip(null);
  }

  // ── back-testing: the 2025 default rate against the PD the models assigned ─────────────────────────────────────
  const SUSPECT = { 'SB1NO|cons': 'The defaults in this block are a copy of the corporate block’s in the bank’s own CR9' };
  function calib(t, sid, approach) {
    const d = latestYE(t, 'CR9'); if (!d) return null;
    const cl = SEG_BY[sid].irb;
    let n = 0, k = 0, pdn = 0, hn = 0, hs = 0; const grades = [];
    for (const appr of ['AIRB', 'FIRB', 'IRB']) {
      if (approach && (approach === 'FIRB') !== (appr === 'FIRB')) continue;
      for (const c of cl) for (const rg of RANGES) {
        const ni = val(t, d, 'CR9', 'obligors', c, appr, rg); if (!ok(ni) || ni <= 0) continue;
        const ki = val(t, d, 'CR9', 'defaulted', c, appr, rg) || 0;
        const p = ok(val(t, d, 'CR9', 'pd_avg', c, appr, rg)) && val(t, d, 'CR9', 'pd_avg', c, appr, rg) > 0 ? val(t, d, 'CR9', 'pd_avg', c, appr, rg) : val(t, d, 'CR9', 'pd_ew', c, appr, rg);
        if (!ok(p)) continue;
        n += ni; k += ki; pdn += ni * p / 100; grades.push({ rg, n: ni, k: ki, pd: p, dr: 100 * ki / ni });
        const h = val(t, d, 'CR9', 'dr_hist', c, appr, rg); if (ok(h)) { hn += ni; hs += ni * h / 100; }
      }
    }
    if (!n) return null;
    const pd = pdn / n, z = (k - pdn) / Math.sqrt(Math.max(pdn * (1 - pd), 1e-9));
    const zz = 1.96, phat = k / n, den = 1 + zz * zz / n, ctr = (phat + zz * zz / (2 * n)) / den, half = zz * Math.sqrt(phat * (1 - phat) / n + zz * zz / (4 * n * n)) / den;
    return { t, d, n, k, pd, z, lo: Math.max(0, ctr - half), hi: ctr + half, hist: hn ? hs / hn : null, grades, suspect: SUSPECT[`${t}|${sid}`] };
  }
  function renderCalib(models) {
    const el = $('#capCalib'), W = el.clientWidth || 640, H = 340, L = 52, R = 18, T = 16, B = 36, sid = FOCUS_SEG;
    const pts = [];
    for (const m of models) for (const appr of ['AIRB', 'FIRB']) {
      const c = calib(m.t, sid, appr); if (c && c.n >= 30) pts.push({ ...c, appr });
    }
    if (!pts.length) { el.innerHTML = '<div class="empty">No back-test for this portfolio</div>'; return; }
    const vals = pts.flatMap((p) => [100 * p.pd, 100 * Math.max(p.k / p.n, p.lo), 100 * p.hi]).filter((v) => v > 0);
    const lo = Math.max(0.005, Math.min(...vals) / 2.2), hi = Math.min(60, Math.max(...vals) * 2.2);
    const lg = (p) => Math.log10(Math.min(Math.max(p, lo), hi));
    const sx = (p) => L + (lg(p) - Math.log10(lo)) / (Math.log10(hi) - Math.log10(lo)) * (W - L - R);
    const sy = (p) => T + (1 - (lg(p) - Math.log10(lo)) / (Math.log10(hi) - Math.log10(lo))) * (H - T - B);
    let s = `<svg viewBox="0 0 ${W} ${H}" width="${W}" height="${H}" role="img" aria-label="Observed default rate against assigned PD">`;
    for (const v of logTicks(lo, hi)) s += `<line x1="${sx(v)}" x2="${sx(v)}" y1="${T}" y2="${H - B}" stroke="var(--grid)"/><text x="${sx(v)}" y="${H - B + 13}" text-anchor="middle" fill="var(--text4)" font-size="9">${tickTxt(v)}</text>` +
      `<line x1="${L}" x2="${W - R}" y1="${sy(v)}" y2="${sy(v)}" stroke="var(--grid)"/><text x="${L - 6}" y="${sy(v) + 3}" text-anchor="end" fill="var(--text4)" font-size="9">${tickTxt(v)}</text>`;
    const ang = (Math.atan2(sy(lo) - sy(hi), sx(hi) - sx(lo)) * 180 / Math.PI).toFixed(1), mid = Math.pow(10, (Math.log10(lo) * 0.25 + Math.log10(hi) * 0.75));
    s += `<line x1="${sx(lo)}" y1="${sy(lo)}" x2="${sx(hi)}" y2="${sy(hi)}" stroke="rgba(255,255,255,.35)" stroke-width="1"/>` +
         `<text x="${sx(mid)}" y="${sy(mid) - 6}" fill="var(--text3)" font-size="9" text-anchor="middle" transform="rotate(-${ang} ${sx(mid)} ${sy(mid) - 6})">defaults = PD</text>` +
         `<text x="${W - R - 4}" y="${H - B - 8}" text-anchor="end" fill="var(--cool)" font-size="9">▼ fewer defaults than the PD: conservative</text>` +
         `<text x="${L + 6}" y="${T + 10}" fill="var(--hot)" font-size="9">▲ more defaults than the PD</text>` +
         `<text x="${(L + W - R) / 2}" y="${H - 6}" text-anchor="middle" fill="var(--text3)" font-size="9.5">average PD assigned at the start of the year</text>` +
         `<text x="12" y="${(T + H - B) / 2}" transform="rotate(-90 12 ${(T + H - B) / 2})" text-anchor="middle" fill="var(--text3)" font-size="9.5">observed default rate</text>`;
    for (const p of pts) {
      p.cx = sx(100 * p.pd); p.cy = sy(100 * Math.max(p.k / p.n, lo / 100)); p.circle = { x: p.cx, y: p.cy, r: 5 };
      const col = p.appr === 'FIRB' ? C.F : C.A;
      s += `<line x1="${p.cx}" x2="${p.cx}" y1="${sy(100 * p.hi)}" y2="${sy(100 * Math.max(p.lo, lo / 100))}" stroke="${col}" stroke-width="1.5" opacity=".5"/>` +
        `<circle cx="${p.cx}" cy="${p.cy}" r="5" fill="${p.suspect ? 'none' : col}" stroke="${p.suspect ? col : 'var(--bg1)'}" stroke-width="${p.suspect ? 1.5 : 2}"/>`;
    }
    const labs = placeLabels(pts.map((p) => ({ x: p.cx, y: p.cy, r: 5, text: p.t + (p.appr === 'FIRB' ? '·F' : '') + (p.suspect ? ' !' : ''), circle: p.circle, prio: p.n })),
      { bounds: [L + 2, T + 12, W - R, H - B - 12], avoid: pts.map((p) => p.circle) });
    for (const it of labs) s += labelSvg(it);
    el.style.height = H + 'px'; el.innerHTML = s + '</svg>';
    el.onpointermove = (e) => {
      const rc = el.querySelector('svg').getBoundingClientRect(), px = (e.clientX - rc.left) * (W / rc.width), py = (e.clientY - rc.top) * (H / rc.height);
      let best = null, bd = 1e9;
      for (const p of pts) { const d = Math.hypot(p.cx - px, p.cy - py); if (d < bd) { bd = d; best = p; } }
      if (!best || bd > 24) { tip(null); return; }
      const g = best.grades.map((x) => `<tr><td>${esc(x.rg)}</td><td>${nb(x.n)}</td><td>${nb(x.k)}</td><td>${x.pd.toFixed(2)}</td><td>${x.dr.toFixed(2)}</td></tr>`).join('');
      tip(`<b>${esc(NAMES[best.t] || best.t)} · ${esc(SEG_BY[sid].name)} · ${best.appr === 'FIRB' ? 'F-IRB' : 'A-IRB'}</b> · ${esc(dlabel(best.d))}<br>` +
        `${nb(best.n)} obligors, <b>${nb(best.k)}</b> defaulted: <b>${pc(100 * best.k / best.n, 2)}</b> against a PD of <b>${pc(100 * best.pd, 2)}</b>` +
        `<br>95 % interval ${pc(100 * best.lo, 2)}–${pc(100 * best.hi, 2)} · z ${best.z.toFixed(1)}${ok(best.hist) ? ` · long-run default rate ${pc(100 * best.hist, 2)}` : ''}` +
        (best.suspect ? `<div class="tm" style="color:var(--hot)">! ${esc(best.suspect)}</div>` : '') +
        `<table class="tg"><tr><th>PD range</th><th>obligors</th><th>defaults</th><th>PD %</th><th>DR %</th></tr>${g}</table>`, e);
    };
    el.onpointerleave = () => tip(null);
  }

  // ── output floor ─────────────────────────────────────────────────────────────────────────────────────────────────
  function renderFloor(models) {
    const el = $('#capFloor'), W = el.clientWidth || 640, narrow = W < 520;
    const rows = models.filter((m) => m.base && m.treaBase).map((m) => ({ ...m, r: 100 * m.treaBase / m.base })).sort((a, b) => a.r - b.r);
    const L = narrow ? 52 : 92, R = narrow ? 96 : 120, T = 30, rowH = 30, H = T + rows.length * rowH + 22, X0 = 40, X1 = 100;
    const x = (v) => L + (Math.min(Math.max(v, X0), X1) - X0) / (X1 - X0) * (W - L - R);
    const STEPS = [[2025, 50], [2026, 55], [2027, 60], [2028, 65], [2029, 70], [2030, 72.5]];
    let s = `<svg viewBox="0 0 ${W} ${H}" width="${W}" height="${H}" role="img" aria-label="Risk-weighted assets as a share of the standardised approach, against the output floor">`;
    for (let v = X0; v <= X1; v += 10) s += `<line x1="${x(v)}" x2="${x(v)}" y1="${T - 4}" y2="${H - 18}" stroke="var(--grid)"/><text x="${x(v)}" y="${H - 6}" text-anchor="middle" fill="var(--text4)" font-size="9">${v} %</text>`;
    STEPS.forEach(([yr, v], i) => {
      s += `<line x1="${x(v)}" x2="${x(v)}" y1="${T - 8}" y2="${H - 18}" stroke="var(--amber)" stroke-width="${yr === 2030 ? 1.6 : 1}" opacity="${0.35 + i * 0.12}"/>` +
        (narrow && yr !== 2025 && yr !== 2030 ? '' : `<text x="${x(v)}" y="${T - 12}" text-anchor="middle" fill="var(--amber)" font-size="8.5" opacity="${0.5 + i * 0.1}">${yr === 2025 || yr === 2030 ? yr : String(yr).slice(2)}</text>`);
    });
    rows.forEach((m, i) => {
      const y = T + i * rowH + 10, gap = m.r - 72.5;
      s += `<text x="${L - 10}" y="${y + 4}" text-anchor="end" fill="var(--text2)" font-size="10">${esc(m.t)}</text>` +
        `<line x1="${x(X0)}" x2="${x(m.r)}" y1="${y}" y2="${y}" stroke="${C.A}" stroke-width="2" opacity=".55"/>` +
        `<circle cx="${x(m.r)}" cy="${y}" r="5.5" fill="${C.A}" stroke="var(--bg1)" stroke-width="2"/>` +
        `<text x="${W - R + 8}" y="${y + 4}" fill="${gap < 0 ? 'var(--hot)' : 'var(--text3)'}" font-size="9.5">${m.r.toFixed(1)} %${narrow ? '' : ' · '}${narrow ? (gap < 0 ? ' binds' : '') : gap < 0 ? `binds in ${STEPS.find(([, v]) => v > m.r)[0]}` : `+${gap.toFixed(1)} pp in 2030`}</text>` +
        `<rect x="0" y="${y - rowH / 2}" width="${W}" height="${rowH}" fill="transparent" data-i="${i}"/>`;
    });
    el.style.height = H + 'px'; el.innerHTML = s + '</svg>';
    el.onpointermove = (e) => {
      const m = rows[+((e.target.dataset || {}).i)]; if (!m) { tip(null); return; }
      tip(`<b>${esc(NAMES[m.t] || m.t)}</b> · ${esc(dlabel(m.dM1))}<br>Total risk exposure ${bn(m.treaBase)} · the floor's standardised base ${bn(m.base)}` +
        `<br>Ratio <b>${pc(m.r, 1)}</b>. At 72.5 % the floor would add ${m.r < 72.5 ? bn(m.base * 0.725 - m.treaBase) : 'nothing'} at this balance sheet.` +
        `<div class="tm">The base is CMS1's "RWEA that is the base of the output floor" (EU d), which already applies the transitional treatment of mortgages and unrated corporates.</div>`, e);
    };
    el.onpointerleave = () => tip(null);
  }

  // ── what Finanstilsynet found in the models ─────────────────────────────────────────────────────────────────────
  const THEMES = [
    ['use', 'IRB not used in credit decisions or pricing', /anvendelse|innvilgelse|beslutningsnotat|prising|prisfastsett|sekundær rolle/i],
    ['lgd', 'LGD, collateral, cure rates', /LGD|sikkerhet|kausjon|tilfrisk|gjenvinning/i],
    ['pd', 'PD level and calibration', /\bPD\b|PD-|kalibrer|misligholdsrate|misligholdssannsynlighet/i],
    ['mat', 'Maturity and refinancing', /løpetid|refinansier/i],
    ['size', 'SME factor, size S, correlation', /SMB-støttefaktor|størrelsesparameter|korrelasjon|IRB-kategoriser|massemarked/i],
    ['val', 'Validation', /validering|akseptansekriteri/i],
    ['under', 'Capital requirement understated', /undervurder|underrapport|for lave kapitalkrav|for lavt kapitalkrav|feilen gjelder bankens beregning av kapitalkrav/i],
  ];
  // an inspection of the models: Finanstilsynet's IRB inspections, and credit inspections whose findings name IRB
  const IRB_REPORT = /IRB|internmodell|interne modeller for (beregning|kapitalkrav)|intern modell for beregning av kapitalkrav for kredittrisiko/i;
  const MODEL_FINDING = /IRB|\bPD\b|PD-|LGD|risikoparamet|kalibr|tilfrisk|løpetidsparameter|løpetid|refinansieringsrisiko|SMB-støttefaktor|størrelsesparameter|korrelasjonsparameter|validering/i;
  const RECENT = '2025-01-01';                   // the findings grid pools every model report from here on
  function inspections(t) {
    return FSA.filter((r) => r.ticker === t)
      .map((r) => ({ ...r, irbF: (r.findings || []).filter((f) => MODEL_FINDING.test(f.finding || '')) }))
      .filter((r) => IRB_REPORT.test(r.summary || '') || (r.findings || []).some((f) => /IRB/i.test(f.finding || '')))
      .sort((a, b) => a.report_date < b.report_date ? 1 : -1);
  }
  function renderFindings(models) {
    const banks = IRB_ORDER.filter((t) => ST[t]);
    const cnt = (n) => n ? `rgba(240,96,96,${Math.min(0.62, 0.12 + n * 0.1).toFixed(2)})` : 'rgba(255,255,255,0.02)';
    let h = `<thead><tr><th class="l">Bank</th><th>Model inspections since 2025</th>${THEMES.map(([, nm]) => `<th>${esc(nm)}</th>`).join('')}</tr></thead><tbody>`;
    const detail = {};
    for (const t of banks) {
      const ins = inspections(t), recent = ins.filter((r) => r.report_date >= RECENT && r.irbF.length);
      const last = recent[0] || ins[0];
      detail[t] = last && { ...last, irbF: recent.length ? recent.flatMap((r) => r.irbF.map((f) => ({ ...f, at: r.report_date }))) : last.irbF, reports: recent };
      if (!last) { h += `<tr><td class="l">${esc(NAMES[t] || t)}</td><td colspan="${THEMES.length + 1}" class="faint">No report in Tilsynsportalen or the archive</td></tr>`; continue; }
      const D = detail[t], by = Object.fromEntries(THEMES.map(([id, , re]) => [id, D.irbF.filter((f) => re.test(f.finding || ''))]));
      h += `<tr><td class="l"><b>${esc(NAMES[t] || t)}</b><small>${ins.length} model report${ins.length === 1 ? '' : 's'} since ${esc((ins[ins.length - 1] || {}).report_date || '')}</small></td>` +
        `<td class="c" tabindex="0" data-tip="${t}|all"><b class="num">${esc(recent.map((r) => r.report_date).join(' · ') || last.report_date)}</b><span class="am-sub">${D.irbF.length} model finding${D.irbF.length === 1 ? '' : 's'}${recent.some((r) => r.severity === 'breach') ? ' · breach' : ''}</span></td>` +
        THEMES.map(([id]) => `<td class="c" tabindex="0" data-tip="${t}|${id}" style="background:${cnt(by[id].length)}">${by[id].length ? `<b class="num">${by[id].length}</b>` : '<span class="faint">·</span>'}</td>`).join('') + '</tr>';
    }
    const tb = $('#capFindings'); tb.innerHTML = h + '</tbody>';
    bindTips(tb, (id) => {
      const [t, th] = id.split('|'), r = detail[t]; if (!r) return null;
      const re = (THEMES.find(([x]) => x === th) || [])[2];
      const fs = th === 'all' ? r.irbF : r.irbF.filter((f) => re.test(f.finding || ''));
      const heads = (r.reports && r.reports.length ? r.reports : [r]).map((x) => `${esc(x.report_type || 'Tilsynsrapport')} ${esc(x.report_date)}: <span class="faint">${esc((x.summary || '').slice(0, 170))}${(x.summary || '').length > 170 ? '…' : ''}</span>`).join('<br>');
      return `<b>${esc(NAMES[t] || t)}</b><br>${heads}` +
        `<ul class="tf">${fs.slice(0, 6).map((f) => `<li>${f.at ? `<span class="faint">${esc(f.at)}</span> ` : ''}${esc(f.finding)}</li>`).join('')}</ul>${fs.length > 6 ? `<span class="faint">+${fs.length - 6} more</span>` : ''}`;
    });
    tb.onclick = (e) => { const c = e.target.closest('[data-tip]'); if (!c) return; const r = detail[c.dataset.tip.split('|')[0]]; if (r && r.source_url) window.open(r.source_url, '_blank', 'noopener'); };
  }

  // ── the IRB timeline ────────────────────────────────────────────────────────────────────────────────────────────
  function renderTimeline() {
    const el = $('#capTimeline'), W = el.clientWidth || 900, narrow = W < 640;
    const banks = IRB_ORDER.filter((t) => ST[t]);
    const y0 = 2006, y1 = 2027, L = narrow ? 64 : 150, R = 16, T = 34, rowH = 34, H = T + banks.length * rowH + 26;
    const tOf = (d) => { if (!d) return null; const [y, m] = String(d).split('-').map(Number); return y + ((m || 6) - 0.5) / 12; };
    const x = (yr) => L + (yr - y0) / (y1 - y0) * (W - L - R);
    const now = 2026 + 10.1 / 12;
    let s = `<svg viewBox="0 0 ${W} ${H}" width="${W}" height="${H}" role="img" aria-label="IRB approvals, applications and rejections per bank">`;
    for (let yr = y0; yr <= y1; yr++) if (yr % (narrow ? 4 : 2) === 0) s += `<line x1="${x(yr)}" x2="${x(yr)}" y1="${T - 8}" y2="${H - 20}" stroke="var(--grid)"/><text x="${x(yr)}" y="${H - 6}" text-anchor="middle" fill="var(--text4)" font-size="9">${yr}</text>`;
    const RULES = [['2007-01', 'Basel II'], ['2025-04', 'CRR3'], ['2025-07', '25 % floor']];
    RULES.forEach(([d, nm], i) => s += `<line x1="${x(tOf(d))}" x2="${x(tOf(d))}" y1="${T - 14}" y2="${H - 20}" stroke="var(--amber)" opacity=".5"/>` +
      `<text x="${x(tOf(d)) + (i === 2 ? 3 : -3)}" y="${T - 18 + (i === 2 ? 9 : 0)}" text-anchor="${i === 2 ? 'start' : 'end'}" fill="var(--amber)" font-size="8.5" opacity=".85">${nm}</text>`);
    s += `<line x1="${x(now)}" x2="${x(now)}" y1="${T - 8}" y2="${H - 20}" stroke="rgba(255,255,255,.3)"/><text x="${x(now) + 3}" y="${H - 22}" fill="var(--text3)" font-size="8.5">today</text>`;
    const marks = [];
    banks.forEach((t, i) => {
      const y = T + i * rowH + rowH / 2, st = ST[t];
      s += `<text x="${L - 10}" y="${y + 3}" text-anchor="end" fill="var(--text2)" font-size="10">${esc(narrow ? t : (NAMES[t] || t))}</text><line x1="${x(y0)}" x2="${x(y1)}" y1="${y}" y2="${y}" stroke="rgba(255,255,255,.07)"/>`;
      for (const p of st.pending || []) {
        const a = tOf(p.since); if (!a) continue;
        s += `<line x1="${x(a)}" x2="${x(now)}" y1="${y}" y2="${y}" stroke="${C.R}" stroke-width="3" opacity=".55" stroke-linecap="round"/>`;
      }
      for (const ev of st.events || []) {
        const a = tOf(ev.date); if (!a || ev.kind === 'rule') continue;
        const cx = x(a);
        if (ev.kind === 'approval') s += `<circle cx="${cx}" cy="${y}" r="5" fill="${C.A}" stroke="var(--bg1)" stroke-width="2"/>`;
        else if (ev.kind === 'application') s += `<circle cx="${cx}" cy="${y}" r="4.5" fill="var(--bg1)" stroke="${C.R}" stroke-width="1.8"/>`;
        else if (ev.kind === 'rejection') s += `<path d="M${cx - 5},${y - 5} L${cx + 5},${y + 5} M${cx + 5},${y - 5} L${cx - 5},${y + 5}" stroke="var(--hot)" stroke-width="2.2"/>`;
        else if (ev.kind === 'merger') s += `<rect x="${cx - 4}" y="${y - 4}" width="8" height="8" transform="rotate(45 ${cx} ${y})" fill="var(--text3)"/>`;
        else s += `<circle cx="${cx}" cy="${y}" r="3" fill="var(--text3)"/>`;
        marks.push({ t, ev, cx, y });
      }
      for (const r of inspections(t)) {
        const a = tOf(r.report_date); if (!a) continue;
        s += `<path d="M${x(a)},${y + 8} l-3.5,6 h7 z" fill="${r.irbF && r.irbF.length ? 'var(--hot)' : 'var(--text4)'}"/>`;
        marks.push({ t, ins: r, cx: x(a), y: y + 11 });
      }
    });
    el.style.height = H + 'px'; el.innerHTML = s + '</svg>';
    el.onpointermove = (e) => {
      const rc = el.querySelector('svg').getBoundingClientRect(), px = (e.clientX - rc.left) * (W / rc.width), py = (e.clientY - rc.top) * (H / rc.height);
      let best = null, bd = 1e9;
      for (const m of marks) { const d = Math.hypot(m.cx - px, m.y - py); if (d < bd) { bd = d; best = m; } }
      if (!best || bd > 18) { tip(null); return; }
      if (best.ins) tip(`<b>${esc(NAMES[best.t] || best.t)}</b> · Finanstilsynet ${esc(best.ins.report_date)}<br>${best.ins.irbF.length} model finding${best.ins.irbF.length === 1 ? '' : 's'}<br><span class="faint">${esc((best.ins.summary || '').slice(0, 240))}</span>`, e);
      else tip(`<b>${esc(NAMES[best.t] || best.t)}</b> · ${esc(best.ev.date)}<br>${esc({ approval: 'Approval', application: 'Application', rejection: 'Rejection', merger: 'Merger', change: 'Change' }[best.ev.kind] || best.ev.kind)}: ${esc(best.ev.text)}` +
        ((ST[best.t].pending || []).length ? `<div class="tm">Still pending: ${(ST[best.t].pending || []).map((p) => `${esc(p.text)}${p.since ? ` (since ${esc(p.since)})` : ''}`).join('; ')}</div>` : ''), e);
    };
    el.onpointerleave = () => tip(null);
  }

  // ── the scorecard ───────────────────────────────────────────────────────────────────────────────────────────────
  function renderScore(models) {
    const banks = IRB_ORDER.map((t) => models.find((m) => m.t === t)).filter((m) => m && ST[m.t]);
    const nowYM = 2026 * 12 + 10;
    const monthsSince = (d) => { if (!d) return null; const [y, m] = d.split('-').map(Number); return nowYM - (y * 12 + (m || 6) - 1); };
    const rows = banks.map((m) => {
      const st = ST[m.t], mort = m.cls.find((c) => c.cls === 'ret_re'), cm = calib(m.t, 'mort'), cc = calib(m.t, 'corp', 'AIRB') || calib(m.t, 'corp');
      const irbEad = sum(Object.values(m.segs).map((g) => g.A.ead + g.F.ead)), airb = sum(Object.values(m.segs).map((g) => g.A.ead));
      const pend = (st.pending || []).map((p) => monthsSince(p.since)).filter(ok);
      const rej = (st.events || []).filter((e) => e.kind === 'rejection').map((e) => e.date).pop();
      const ins = inspections(m.t), recent = ins.filter((r) => r.report_date >= RECENT && r.irbF.length), last = recent[0];
      return { m, st, mortRW: mort ? mort.density : null, airbShare: irbEad ? 100 * airb / irbEad : null,
        addonPct: m.addon > 0 && m.cr6rwa ? 100 * m.addon / m.cr6rwa : m.addon === 0 ? 0 : null,
        calM: cm && !cm.suspect ? 100 * cm.k / cm.n / (100 * cm.pd) : null, calC: cc && !cc.suspect ? 100 * cc.k / cc.n / (100 * cc.pd) : null,
        pending: (st.pending || []).length, oldest: pend.length ? Math.max(...pend) : null, rej,
        floor: m.base && m.treaBase ? 100 * m.treaBase / m.base : null, findings: recent.length ? sum(recent.map((r) => r.irbF.length)) : null,
        insDate: recent.length ? recent.map((r) => r.report_date.slice(0, 7)).join(', ') : null,
        issues: (st.disclosure || []).length };
    });
    const H = ['Bank', 'IRB since', 'On IRB', 'A-IRB share of IRB', 'Mortgage RW (model)', 'Floors & add-ons', 'Model vs SA', 'Defaults / PD, mortgages', 'Defaults / PD, corporates',
      'Output floor 2030', 'Applications pending', 'Model findings since 2025', 'Errors in own tables'];
    let h = `<thead><tr>${H.map((x, i) => `<th${i ? '' : ' class="l"'}>${esc(x)}</th>`).join('')}</tr></thead><tbody>`;
    for (const r of rows) {
      const f = (v, dp = 0, suf = ' %') => ok(v) ? v.toFixed(dp) + suf : '—';
      h += `<tr tabindex="0" data-tip="${r.m.t}"><td class="l"><b>${esc(NAMES[r.m.t] || r.m.t)}</b></td>` +
        `<td>${esc(r.st.irb_since || '—')}${r.st.airb_corp_since ? `<small>A-IRB corp. ${esc(r.st.airb_corp_since)}</small>` : ''}</td>` +
        `<td class="num">${f(r.m.irbShare)}</td><td class="num">${f(r.airbShare)}</td>` +
        `<td class="num ${ok(r.mortRW) && r.mortRW < 25 ? 'warm' : ''}">${f(r.mortRW, 1)}${ok(r.mortRW) && r.mortRW < 25 ? '<small>floor binds</small>' : ''}</td>` +
        `<td class="num">${ok(r.addonPct) && r.addonPct > 0 ? `+${r.addonPct.toFixed(0)} %<small>${bn(r.m.addon)}</small>` : r.addonPct === 0 ? '—<small>inside IRB numbers</small>' : '—'}</td>` +
        `<td class="num">${f(r.m.vsSA)}</td>` +
        `<td class="num ${ok(r.calM) && r.calM > 1 ? 'hot' : ''}">${ok(r.calM) ? r.calM.toFixed(2) + '×' : '—'}</td>` +
        `<td class="num ${ok(r.calC) && r.calC > 1 ? 'hot' : ''}">${ok(r.calC) ? r.calC.toFixed(2) + '×' : '—'}</td>` +
        `<td class="num ${ok(r.floor) && r.floor < 72.5 ? 'hot' : ''}">${f(r.floor, 1)}</td>` +
        `<td class="num">${r.pending ? `${r.pending}${ok(r.oldest) ? `<small>oldest ${Math.round(r.oldest)} months</small>` : ''}` : '0'}${r.rej ? `<small class="hot">rejected ${esc(r.rej)}</small>` : ''}</td>` +
        `<td class="num">${ok(r.findings) ? `${r.findings}<small>${esc(r.insDate)}</small>` : '—'}</td>` +
        `<td class="num ${r.issues >= 3 ? 'warm' : ''}">${r.issues}</td></tr>`;
    }
    const tb = $('#capScore'); tb.innerHTML = h + '</tbody>';
    bindTips(tb, (t) => {
      const r = rows.find((x) => x.m.t === t); if (!r) return null;
      return `<b>${esc(NAMES[t] || t)}</b><div class="tm">${esc(r.st.summary || '')}</div>` +
        ((r.st.overlays || []).length ? `<div class="tm"><b>Conditions on the models</b><br>${r.st.overlays.map(esc).join('<br>')}</div>` : '') +
        ((r.st.pending || []).length ? `<div class="tm"><b>Pending</b><br>${r.st.pending.map((p) => `${esc(p.text)}${p.since ? ` · since ${esc(p.since)}` : ''}`).join('<br>')}</div>` : '') +
        ((r.st.disclosure || []).length ? `<div class="tm"><b>What we found wrong in its own Pillar 3 tables</b><ul class="tf">${r.st.disclosure.map((x) => `<li>${esc(x)}</li>`).join('')}</ul></div>` : '') +
        `<div class="tm faint">${esc(r.st.source_title || '')}${r.st.source_pages ? `, pp. ${esc(r.st.source_pages)}` : ''}</div>`;
    });
    tb.onclick = (e) => { const tr = e.target.closest('tr[data-tip]'); if (!tr) return; const st = ST[tr.dataset.tip]; if (st && st.source_url) window.open(st.source_url, '_blank', 'noopener'); };
  }

  // ── focus selector (one row, scopes the formula, loss and back-test views) ──────────────────────────────────────
  function renderFocus(models) {
    const box = $('#capFocus');
    box.innerHTML = FOCUS.map((id) => `<button type="button" data-s="${id}" class="${id === FOCUS_SEG ? 'on' : ''}" aria-pressed="${id === FOCUS_SEG}">${esc(SEG_BY[id].name)}</button>`).join('');
    box.onclick = (e) => {
      const b = e.target.closest('button[data-s]'); if (!b) return;
      FOCUS_SEG = b.dataset.s;
      try { localStorage.setItem('capFocus', FOCUS_SEG); } catch (_) {}
      box.querySelectorAll('button').forEach((x) => { x.classList.toggle('on', x.dataset.s === FOCUS_SEG); x.setAttribute('aria-pressed', x.dataset.s === FOCUS_SEG); });
      drawFocus(models);
    };
  }
  function drawFocus(models) {
    for (const f of [renderCurve, renderLoss, renderCalib]) { try { f(models); } catch (e) { console.error(e); } }
    document.querySelectorAll('.cap-seg').forEach((s) => { s.textContent = SEG_BY[FOCUS_SEG].name.toLowerCase(); });
  }

  // ── entry ───────────────────────────────────────────────────────────────────────────────────────────────────────
  async function render(ctx) {
    NAMES = ctx.names || {}; METRICS = ctx.metrics || []; FSA = ctx.fsa || [];
    try { const f = localStorage.getItem('capFocus'); if (FOCUS.includes(f)) FOCUS_SEG = f; } catch (_) {}
    const [p3, st, docs] = await Promise.all([
      get('bank_pillar3?select=ticker,ref_date,template,approach,exposure_class,bucket,metric,value'),
      get('bank_irb_status?select=*'),
      get('bank_pillar3_docs?select=ticker,ref_date,kind,url,templates'),
    ]);
    if (!p3 || !p3.length) { $('#capital .cap-body').innerHTML = '<div class="empty">The capital data could not be loaded.</div>'; return; }
    index(p3); DOCS = docs || [];
    ST = Object.fromEntries((st || []).map((r) => [r.ticker, r]));
    BANKS = ctx.banks || Object.keys(DATES);
    const models = BANKS.map(bankModel);
    const saBanks = BANKS.filter((t) => !ST[t]).map((t) => {
      const m = models.find((x) => x.t === t), fb = reportFallback(t);
      const trea = ok(m.trea) ? m.trea : fb && fb.trea, cet1 = ok(m.cet1) ? m.cet1 : fb && fb.cet1;
      const irbVia = ok(m.ovIrb) && m.ovIrb > 0 ? m.ovIrb : null;
      const parts = [];
      if (ok(m.csa)) parts.push(['credit risk, standardised', m.csa, C.SA]);
      if (irbVia) parts.push(['credit risk, IRB via part-owned mortgage companies', irbVia, C.A]);
      if (ok(m.op)) parts.push(['operational risk', m.op, 'var(--s3)']);
      const rest = ok(trea) && parts.length ? trea - sum(parts.map((p) => p[1])) : 0;
      if (rest > 0.01 * trea) parts.push(['other (CVA, market, floors, deductions)', rest, '#3a4252']);
      const mortRW = m.segs.mort && m.segs.mort.SA.ead ? 100 * m.segs.mort.SA.rwa / m.segs.mort.SA.ead : null;
      return { t, trea, cet1, irbVia, parts, mortRW, src: ok(m.trea) ? `Pillar 3, ${dlabel(m.dK || m.dO)}` : fb ? `quarterly report ${fb.period}` : 'no figures' };
    }).filter((b) => ok(b.trea));
    const steps = [() => renderHero(models, saBanks), () => renderMatrix(models, saBanks), () => renderScore(models), () => renderTracks(models),
                   () => renderFocus(models), () => drawFocus(models), () => renderFloor(models), () => renderFindings(models), renderTimeline];
    for (const f of steps) { try { f(); } catch (e) { console.error(e); } }
    const n = new Set(p3.map((r) => r.ticker)).size;
    $('#capAsof').textContent = `${nb(p3.length)} values from ${n} banks' Pillar 3 reports · year-end 2025 unless marked`;
    let rt; window.addEventListener('resize', () => { clearTimeout(rt); rt = setTimeout(() => { renderTracks(models); drawFocus(models); renderFloor(models); renderTimeline(); }, 200); });
  }
  window.BankCapital = { render };
})();
