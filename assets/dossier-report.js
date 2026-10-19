/* dossier-report.js — the dossier's report sections (2026-10-19). Everything the radar's report drawer shows about a
   company's coming report, and what the drawer has no room for:

     #rpAhead    the coming report: countdown, the radar's positioning and each piece of its evidence, the nowcast
                 (companies with a card), consensus and how it moved, targets, the rating mix, broker actions
     #rpRoad     the road to it: one time axis from the last report to the next, a lane per signal (the share against
                 the market, the radar's daily positioning, news, dark-pool days, short interest, insiders, brokers,
                 nowcast and consensus EPS) under the run-up window, the closed period and the report day
     #rpInto     into every report: the share against the market from 20 sessions before each report since 2024 to 5
                 after, with this quarter's run-up drawn over them
     #rpHistory  every report since 2024: the moves, the EPS surprise, what the radar said, and the release itself
     #rpNews     headlines naming the company, and the broker actions parsed from them

   DossierReport.render(isin) fetches, renders and resolves to the data; the price tape takes its report bands from
   it. Reads api.report_radar, report_calendar, report_reactions, report_radar_graded / _forward / _snapshot,
   consensus_snapshot, analyst_target_snapshot, analyst_consensus, analyst_rec_latest, broker_actions, company_news,
   nowcast_latest / _daily / _attribution, price_excess, dark_flow_summary, short_interest, insider_trades, and
   report_texts on demand. Backend runbooks: ENEXT docs/event-radar.md, docs/earnings.md, docs/nowcast.md. */
(function () {
  'use strict';
  const { safeGet, fmtNum, fmtCompact } = window.INTEL;
  const $ = (id) => document.getElementById(id);
  const esc = (s) => s == null ? '' : String(s).replace(/[&<>"]/g, (c) => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;' }[c]));
  const day = (s) => Math.floor(Date.parse(String(s).slice(0, 10) + 'T00:00:00Z') / 864e5);
  const iso = (d) => new Date(d * 864e5).toISOString().slice(0, 10);
  const dLabel = (s, wd) => s ? new Date(String(s).slice(0, 10) + 'T00:00:00Z').toLocaleDateString('en-GB',
    Object.assign({ day: 'numeric', month: 'short', timeZone: 'UTC' }, wd ? { weekday: 'short' } : {})) : '—';
  const signed = (v, dp = 1) => v == null || isNaN(v) ? '—' : (+v > 0 ? '+' : +v < 0 ? '−' : '') + Math.abs(+v).toFixed(dp);
  const pct = (v, dp = 1) => v == null || isNaN(v) ? '—' : `<span class="${+v > 0 ? 'rp-u' : +v < 0 ? 'rp-d' : ''}">${signed(v, dp)}%</span>`;
  const mnok = (v) => v == null || isNaN(v) ? '—' : (+v < 0 ? '−' : '') + Math.abs(Math.round(+v)).toLocaleString('en-GB');
  const todayIso = new Intl.DateTimeFormat('en-CA', { timeZone: 'Europe/Oslo' }).format(new Date());
  const QN = { Q1: 1, Q2: 2, Q3: 3, Q4: 4, H1: 2, H2: 4, FY: 4 };
  const qKey = (fy, p) => fy * 10 + (QN[p] || 0);
  const empty = (msg) => `<div class="empty-note">${msg}</div>`;
  const bandOf = (p) => p >= 60 ? 'strong' : p >= 33 ? 'firm' : 'faint';
  const drift = (now, then) => then ? (now - then) / Math.abs(then) * 100 : null;   // signed right for losses too
  // broker headlines: +2 upgrade, −2 downgrade, ±1 target raised / cut, 0 otherwise
  const brokerDir = (a) => /upgrade|oppgrad/i.test(a.action) ? 2 : /downgrade|nedgrad/i.test(a.action) ? -2
    : /raises|lifts|increases|hever|øker/i.test(a.action) ? 1 : /lowers|cuts|reduces|trims|senker|kutter/i.test(a.action) ? -1 : 0;
  // Norwegian headlines shout the firm ("KEPLER CHEUVREUX"); short words stay capitals (DNB, SEB, ABG, SB1)
  const firmName = (s) => !s ? '—' : s !== s.toUpperCase() ? s
    : s.split(/\s+/).map((w) => w.length <= 3 ? w : w[0] + w.slice(1).toLowerCase()).join(' ');

  /* ═══ THE REPORT AHEAD ═══════════════════════════════════════════════════ */

  // countdown: the arc is the share of the quarter gone since the last report
  function ring(prevD, nextD, todayD, big, sub) {
    const tot = Math.max(1, nextD - prevD), f = Math.min(1, Math.max(0, (todayD - prevD) / tot));
    const R = 50, C = 2 * Math.PI * R;
    return `<svg class="rp-ring" viewBox="0 0 124 124" role="img" aria-label="${esc(big)} ${esc(sub)}">
      <circle cx="62" cy="62" r="${R}" class="trk"/>
      <circle cx="62" cy="62" r="${R}" class="arc" stroke-dasharray="${(C * f).toFixed(1)} ${C.toFixed(1)}" transform="rotate(-90 62 62)"/>
      <circle cx="${(62 + R * Math.sin(2 * Math.PI * f)).toFixed(1)}" cy="${(62 - R * Math.cos(2 * Math.PI * f)).toFixed(1)}" r="4" class="dot"/>
      <text x="62" y="62" class="big">${esc(big)}</text><text x="62" y="80" class="sm">${esc(sub)}</text></svg>`;
  }

  // positioning dial: −100 short … +100 long, the radar's bands (faint < 33 ≤ firm < 60 ≤ strong) on the arc
  function dial(r) {
    const dir = r ? r.direction : 'NONE', p = r ? Math.round(+r.positioning || 0) : 0;
    const v = dir === 'LONG' ? Math.min(100, p) : dir === 'SHORT' ? -Math.min(100, p) : 0;
    const cx = 80, cy = 74, R = 60;
    const pt = (val, rr) => { const a = Math.PI * (1 - (val + 100) / 200); return [(cx + rr * Math.cos(a)).toFixed(1), (cy - rr * Math.sin(a)).toFixed(1)]; };
    const arc = (a, b, col) => { const [x1, y1] = pt(a, R), [x2, y2] = pt(b, R); return `<path d="M${x1},${y1} A${R},${R} 0 0 1 ${x2},${y2}" stroke="${col}"/>`; };
    const [nx, ny] = pt(v, R - 12);
    const col = dir === 'LONG' ? 'var(--green)' : dir === 'SHORT' ? 'var(--red)' : 'var(--text3)';
    return `<svg class="rp-dial" viewBox="0 0 160 96" role="img" aria-label="Radar positioning ${dir} ${p}">
      <g class="arcs">${arc(-100, -60, 'rgba(240,96,96,.75)')}${arc(-60, -33, 'rgba(240,96,96,.38)')}${arc(-33, 33, 'rgba(255,255,255,.12)')}${arc(33, 60, 'rgba(67,224,151,.38)')}${arc(60, 100, 'rgba(67,224,151,.75)')}</g>
      <line x1="${cx}" y1="${cy}" x2="${nx}" y2="${ny}" class="needle" stroke="${col}"/><circle cx="${cx}" cy="${cy}" r="4.5" fill="${col}"/>
      <text x="${cx - R}" y="${cy + 16}" class="end">SHORT</text><text x="${cx + R}" y="${cy + 16}" class="end">LONG</text>
      <text x="${cx}" y="${cy + 18}" class="val" fill="${col}">${dir === 'NONE' ? 'quiet' : `${dir === 'LONG' ? '▲' : '▼'} ${p}`}</text></svg>`;
  }

  function evRow(label, sub, v, muted) {
    const w = Math.min(1, Math.abs(+v || 0) / 3) * 50;
    const bar = !+v ? '' : +v > 0 ? `<i style="left:50%;width:${w}%;background:${muted ? 'rgba(67,224,151,.4)' : 'var(--green)'}"></i>`
      : `<i style="right:50%;width:${w}%;background:${muted ? 'rgba(240,96,96,.4)' : 'var(--red)'}"></i>`;
    return `<div class="rp-ev"><span class="l">${label}<small>${sub}</small></span><span class="t">${bar}</span><span class="v">${signed(v, 2)}</span></div>`;
  }
  const kv = (k, v) => `<div class="rp-kv"><span>${k}</span><b>${v}</b></div>`;

  function evidence(r) {
    // a family counts once it is strong enough (dark |ev| ≥ 0.25, any short-interest evidence): sql/event_radar.sql
    const sc = !+r.agreement ? 'no directional evidence'
      : +r.families ? `${Math.round(+r.agreement * 100)}% agreement · ${r.families} famil${+r.families === 1 ? 'y' : 'ies'}`
      : `${Math.round(+r.agreement * 100)}% one-sided · below the family bar (dark ±0.25)`;
    const verdict = r.direction === 'NONE' ? 'quiet' : `${r.direction} ${Math.round(+r.positioning)} · ${bandOf(+r.positioning)}`;
    return `<div class="rp-block"><div class="rp-bh">Why ${r.direction === 'NONE' ? 'quiet' : Math.round(+r.positioning)}? <small>scored: dark flow and short sellers · insiders shown, not scored</small></div>
      ${evRow('Dark-pool flow', `▲${r.dark_buy_days} ▼${r.dark_sell_days} impact days · ${r.dark_top_days} top-decile · ${r.runup_sessions} sessions`, r.ev_dark)}
      ${evRow('Short sellers', r.si_level != null ? `${(+r.si_level).toFixed(2)}% short${r.si_change != null ? `, ${signed(+r.si_change, 2)} pp in the run-up` : ''}` : 'none disclosed', r.ev_short)}
      ${evRow('Insiders · not scored', `${r.ins_open_buyers} bought · ${r.ins_open_sellers} sold in the open window${+r.ins_closed_trades ? ` · ${r.ins_closed_trades} in the closed period` : ''}`, r.ev_insider, true)}
      ${kv('Combined', `${sc} → <span style="color:${r.direction === 'LONG' ? 'var(--green)' : r.direction === 'SHORT' ? 'var(--red)' : 'var(--text3)'}">${verdict}</span>`)}
      ${kv('Run-up · context, not scored', `${pct(r.runup_pct != null ? +r.runup_pct : null)} <small>10 sessions against same-size peers · volume ×${fmtNum(+r.vol_ratio, 2)}</small>`)}
      <div class="rp-note">The radar takes a side on about one report in six. It is graded on the reaction against same-size peers, and its cut-offs (33 firm, 60 strong) stay fixed through the season.</div></div>`;
  }

  /* nowcast — EPS ranges against consensus on one scale, the daily record, "priced in?", segments and bridge
     (the drawer's drNowcast, with room to breathe) */
  const NC_COL = { guided: 'var(--amber)', power: 'rgba(79,195,247,.95)' };
  const NC_LAB = { guided: 'Nowcast', power: 'With power' };
  function ncStrip(rows) {
    const vals = rows.flatMap((r) => [r.lo, r.hi, r.mid]).filter((v) => v != null && isFinite(v));
    if (!vals.length) return '';
    const lo = Math.min(...vals), hi = Math.max(...vals), pad = (hi - lo) * 0.06 || 0.05;
    const x = (v) => (((v - lo + pad) / (hi - lo + 2 * pad)) * 100).toFixed(2);
    return `<div class="rp-strip">${rows.map((r) => {
      const rng = r.lo != null && r.hi != null ? `<i class="rng" style="left:${x(r.lo)}%;width:${(x(r.hi) - x(r.lo)).toFixed(2)}%;background:${r.fill}"></i>` : '';
      const mid = r.mid != null ? `<b class="mid" style="left:${x(r.mid)}%;background:${r.dot}"></b>` : '';
      return `<div class="rp-strip-row"><span class="lab">${r.label}</span><span class="trk">${rng}${mid}</span><span class="v">${r.text}</span></div>`;
    }).join('')}<div class="rp-strip-axis"><span>${lo.toFixed(2)}</span><span>EPS</span><span>${hi.toFixed(2)}</span></div></div>`;
  }
  function ncSpark(hist) {
    const pts = hist.filter((h) => h.eps_mid != null);
    if (pts.length < 3) return '';
    const W = 560, H = 84, L = 4, R = 76, T = 8, B = 16;
    const d0 = Math.min(...pts.map((h) => day(h.asof_date))), d1 = Math.max(...pts.map((h) => day(h.asof_date)));
    const vals = pts.flatMap((h) => [+h.eps_mid, h.consensus_eps != null ? +h.consensus_eps : NaN]).filter(isFinite);
    const v0 = Math.min(...vals), v1 = Math.max(...vals), sp = (v1 - v0) || 0.1;
    const X = (d) => L + ((d - d0) / Math.max(1, d1 - d0)) * (W - L - R), Y = (v) => T + (1 - (v - v0 + sp * 0.08) / (sp * 1.16)) * (H - T - B);
    const qs = [...new Set(pts.map((h) => `${h.period} ${h.fiscal_year}`))]
      .sort((a, b) => qKey(+a.split(' ')[1], a.split(' ')[0]) - qKey(+b.split(' ')[1], b.split(' ')[0]));
    let s = `<svg class="rp-ncspark" viewBox="0 0 ${W} ${H}" width="100%" role="img" aria-label="Nowcast EPS by day">`;
    qs.forEach((q, i) => {
      const qp = pts.filter((h) => `${h.period} ${h.fiscal_year}` === q), c = i === 0 ? 'var(--amber)' : 'rgba(79,195,247,.95)';
      s += `<polyline fill="none" stroke="${c}" stroke-width="1.6" stroke-linejoin="round" points="${qp.map((h) => `${X(day(h.asof_date)).toFixed(1)},${Y(+h.eps_mid).toFixed(1)}`).join(' ')}"/>`;
      const last = qp[qp.length - 1];
      s += `<text x="${W - R + 6}" y="${Y(+last.eps_mid) + 3}" fill="${c}" font-size="9">${esc(q)} ${(+last.eps_mid).toFixed(2)}</text>`;
      qp.filter((h) => h.consensus_eps != null).forEach((h) => { s += `<circle cx="${X(day(h.asof_date)).toFixed(1)}" cy="${Y(+h.consensus_eps).toFixed(1)}" r="2.6" fill="var(--text1)"/>`; });
    });
    s += `<text x="${L}" y="${H - 3}" fill="var(--text4)" font-size="9">${dLabel(pts[0].asof_date)}</text><text x="${W - R}" y="${H - 3}" fill="var(--text4)" font-size="9" text-anchor="end">${dLabel(pts[pts.length - 1].asof_date)}</text>`;
    return s + '</svg>';
  }
  function nowcastBlock(nc, attr, hist) {
    if (!nc.length) return '';
    const byQ = new Map();
    nc.slice().sort((a, b) => qKey(a.fiscal_year, a.period) - qKey(b.fiscal_year, b.period)).forEach((n) => {
      const k = `${n.period} ${n.fiscal_year}`;
      if (!byQ.has(k)) byQ.set(k, {});
      byQ.get(k)[n.variant] = n;
    });
    let s = '';
    for (const [q, v] of byQ) {
      const g = v.guided || Object.values(v)[0], rows = [];
      if (g.consensus_eps != null) rows.push({ label: 'Consensus', lo: g.consensus_low != null ? +g.consensus_low : null, hi: g.consensus_high != null ? +g.consensus_high : null,
        mid: +g.consensus_eps, fill: 'rgba(255,255,255,.14)', dot: 'var(--text1)', text: `${(+g.consensus_eps).toFixed(2)}<small>${g.consensus_n || '?'} analysts</small>` });
      for (const k of ['guided', 'power']) {
        const n = v[k]; if (!n) continue;
        rows.push({ label: NC_LAB[k], lo: +n.eps_low, hi: +n.eps_high, mid: +n.eps_mid, fill: `color-mix(in srgb, ${NC_COL[k]} 30%, transparent)`, dot: NC_COL[k],
          text: `${(+n.eps_mid).toFixed(2)}${n.surprise_pct != null ? `<small>${pct(+n.surprise_pct)}</small>` : ''}` });
      }
      const verdict = g.surprise_pct == null ? 'no consensus captured yet'
        : Math.abs(+g.surprise_pct) < 5 ? 'in line with consensus' : +g.surprise_pct > 0 ? 'above consensus' : 'below consensus';
      s += `<div class="rp-ncq"><div class="rp-h"><b>${esc(q)}</b><span>${g.report_date ? `report ${dLabel(g.report_date)} · ` : ''}built on ${esc(g.base || '')}</span></div>${ncStrip(rows)}
        <div class="rp-note">Adjusted EBITDA ${mnok(+g.ebitda)} MNOK (${mnok(+g.ebitda_low)}–${mnok(+g.ebitda_high)}), EPS ${(+g.eps_low).toFixed(2)}–${(+g.eps_high).toFixed(2)}: ${verdict}.${v.power ? ` With the fitted Nordic-power effect, ${(+v.power.eps_mid).toFixed(2)}.` : ''}</div>
        <details class="rp-seg"><summary>Segments and bridge</summary><div class="rp-list">${(g.segments || []).map((sg) =>
          `<div class="rp-li"><span class="dt">${esc(sg.key)}</span><span class="n">${esc(sg.name)}<small>${mnok(+sg.base)} the quarter before</small></span><span class="r">${mnok(+sg.value)}<small> ${mnok(+sg.low)}–${mnok(+sg.high)}</small></span></div>`).join('')}
          ${(g.items || []).map((it) => `<div class="rp-li sub"><span class="dt">${esc(it.segment)}</span><span class="n">${esc(it.item)}<small>${esc(it.basis)}</small></span><span class="r">${signed(+it.mnok, 0)}</span></div>`).join('')}</div></details></div>`;
    }
    const spark = ncSpark(hist);
    if (spark) s += `<div class="rp-ncq"><div class="rp-h"><b>The daily record</b><span>nowcast EPS each day since the last report · dots: consensus</span></div>${spark}</div>`;
    const a = attr[0];
    if (a && a.parts) {
      const row = (lbl, sub, v, strong) => {
        const w = Math.min(1, Math.abs(+v || 0) / 10) * 50;
        const bar = !+v ? '' : +v > 0 ? `<i style="left:50%;width:${w}%;background:var(--green)"></i>` : `<i style="right:50%;width:${w}%;background:var(--red)"></i>`;
        return `<div class="rp-ev${strong ? ' strong' : ''}"><span class="l">${lbl}<small>${sub}</small></span><span class="t">${bar}</span><span class="v">${signed(v, 1)}%</span></div>`;
      };
      s += `<div class="rp-ncq"><div class="rp-h"><b>Priced in?</b><span>the share since ${dLabel(a.since_session)} against what the watched prices explain</span></div>` +
        row('Share price', `${a.sessions} sessions`, +a.actual_pct, true) +
        a.parts.filter((p) => Math.abs(+p.contribution_pct) >= 0.05).map((p) => row(esc(p.label), `moved ${signed(+p.factor_pct, 1)}% · beta ${(+p.beta).toFixed(2)}`, +p.contribution_pct)).join('') +
        row('Not explained', `daily fit R² ${(+a.r2).toFixed(2)}`, +a.unexplained_pct, true) + '</div>';
    }
    return `<div class="rp-block"><div class="rp-bh">What the prices say <small>nowcast from the World page's prices · against the day's consensus</small></div>${s}
      <div class="rp-note">Built from the company's published sensitivities and guidance, applied every day to the prices on the World page. Consensus is Yahoo's, captured daily. Graded when the report lands.</div></div>`;
  }

  // consensus, its drift, the target range against the price, the rating mix
  function consensusBlock(d) {
    let s = '';
    const byH = {};
    d.cons.forEach((c) => { if (!byH[c.horizon]) byH[c.horizon] = c; });
    const c0 = byH['0q'];
    if (c0 && c0.eps_avg != null) {
      const pts = [['90d', c0.eps_90d_ago], ['60d', c0.eps_60d_ago], ['30d', c0.eps_30d_ago], ['7d', c0.eps_7d_ago], ['now', c0.eps_avg]].filter((p) => p[1] != null).map((p) => [p[0], +p[1]]);
      const ch = pts.length > 1 ? drift(pts[pts.length - 1][1], pts[0][1]) : null;
      const lo = Math.min(...pts.map((p) => p[1])), hi = Math.max(...pts.map((p) => p[1])), sp = hi - lo || 1;
      const spark = pts.length > 1 ? `<svg class="rp-spark" viewBox="0 0 120 28" preserveAspectRatio="none" aria-hidden="true"><polyline fill="none" stroke="${ch >= 0 ? 'var(--green)' : 'var(--red)'}" stroke-width="1.6" vector-effect="non-scaling-stroke" points="${pts.map((p, i) => `${((i / (pts.length - 1)) * 116 + 2).toFixed(1)},${(24 - ((p[1] - lo) / sp) * 20).toFixed(1)}`).join(' ')}"/></svg>` : '';
      s += kv(`EPS, quarter to ${esc(String(c0.period_end).slice(0, 7))}`, `${fmtNum(+c0.eps_avg, 2)} <small>${fmtNum(c0.eps_low, 2)}–${fmtNum(c0.eps_high, 2)} · ${c0.eps_analysts || '?'} analysts</small>`) +
        kv('Drift over 90 days', `${spark}${ch == null ? '—' : pct(ch)}`) +
        `<div class="rp-drift">${pts.map((p) => `<span><small>${p[0]}</small>${fmtNum(p[1], 2)}</span>`).join('')}</div>`;
      if (c0.rev_up_30d != null || c0.rev_down_30d != null) s += kv('Revisions, 30 days', `<span class="rp-u">▲ ${c0.rev_up_30d || 0}</span> · <span class="rp-d">▼ ${c0.rev_down_30d || 0}</span>`);
      if (c0.rev_avg != null) s += kv('Revenue estimate', `${fmtCompact(+c0.rev_avg)} ${esc(c0.rev_ccy || '')} <small>${c0.rev_analysts || '?'} analysts</small>`);
    }
    if (byH['+1q'] && byH['+1q'].eps_avg != null) s += kv('Next quarter EPS', `${fmtNum(+byH['+1q'].eps_avg, 2)} <small>${byH['+1q'].eps_analysts || '?'} analysts</small>`);
    if (byH['0y'] && byH['0y'].eps_avg != null) s += kv('This year EPS', `${fmtNum(+byH['0y'].eps_avg, 2)} <small>${byH['0y'].eps_analysts || '?'} analysts</small>`);
    // targets: the latest daily snapshot, else the weekly consensus row
    const ts = d.targets.length ? d.targets[d.targets.length - 1] : null, ac = d.ac[0];
    const t = ts || (ac ? { target_mean: ac.target_mean, target_high: ac.target_high, target_low: ac.target_low, rec_key: ac.rec_key, n_analysts: ac.num_analysts } : null);
    const price = (ts && ts.current_price) || (d.radar && d.radar.price) || null;
    if (t && +t.target_mean > 0) {
      const lo = Math.min(+t.target_low || +t.target_mean, +price || Infinity), hi = Math.max(+t.target_high || +t.target_mean, +price || 0);
      const P = (v) => ((v - lo) / ((hi - lo) || 1) * 100).toFixed(1);
      const up = price ? (+t.target_mean / +price - 1) * 100 : null;
      s += `<div class="rp-rng"><div class="ln"></div>${+t.target_low && +t.target_high ? `<div class="band" style="left:${P(+t.target_low)}%;width:${(P(+t.target_high) - P(+t.target_low))}%"></div>` : ''}
        <div class="mk tg" style="left:${P(+t.target_mean)}%" title="mean target ${(+t.target_mean).toFixed(2)}"></div>${price ? `<div class="mk px" style="left:${P(+price)}%" title="price ${(+price).toFixed(2)}"></div>` : ''}</div>
        <div class="rp-rng-l"><span>lowest ${fmtNum(+t.target_low, 0)}</span><span><i class="tg"></i>mean ${fmtNum(+t.target_mean, 1)}${price ? ` · <i class="px"></i>price ${fmtNum(+price, 1)}` : ''}</span><span>highest ${fmtNum(+t.target_high, 0)}</span></div>
        ${kv('Target against the price', `${up == null ? '—' : pct(up)} <small>${t.n_analysts || '?'} analysts · ${esc(t.rec_key || '—')}</small>`)}`;
      if (d.targets.length > 2) {
        const tp = d.targets.filter((x) => x.target_mean != null);
        const a = Math.min(...tp.map((x) => +x.target_mean)), b = Math.max(...tp.map((x) => +x.target_mean)), w = b - a || 1;
        s += kv(`Mean target since ${dLabel(tp[0].snapshot_date)}`, `<svg class="rp-spark" viewBox="0 0 120 28" preserveAspectRatio="none" aria-hidden="true"><polyline fill="none" stroke="var(--ice)" stroke-width="1.6" vector-effect="non-scaling-stroke" points="${tp.map((x, i) => `${((i / (tp.length - 1)) * 116 + 2).toFixed(1)},${(24 - ((+x.target_mean - a) / w) * 20).toFixed(1)}`).join(' ')}"/></svg>${pct((+tp[tp.length - 1].target_mean / +tp[0].target_mean - 1) * 100)}`);
      }
    }
    const m = d.rec[0];
    if (m && +m.total) {
      const parts = [['strong buy', m.strong_buy, 'var(--green)'], ['buy', m.buy, 'rgba(67,224,151,.5)'], ['hold', m.hold, '#5a6377'], ['sell', m.sell, 'rgba(240,96,96,.5)'], ['strong sell', m.strong_sell, 'var(--red)']];
      s += `<div class="rp-mix">${parts.filter((p) => +p[1]).map((p) => `<i style="flex:${p[1]};background:${p[2]}" title="${p[1]} ${p[0]}"></i>`).join('')}</div>
        <div class="rp-mix-l">${parts.map((p) => `<span>${p[0]} <b>${p[1] || 0}</b></span>`).join('')}</div>`;
    }
    return s ? `<div class="rp-block"><div class="rp-bh">Consensus and targets <small>Yahoo, captured twice a day</small></div>${s}</div>` : '';
  }

  function brokerList(acts, n) {
    if (!acts.length) return '';
    const glyph = (a) => { const k = brokerDir(a); return k === 2 ? '<span class="rp-u">▲</span>' : k === -2 ? '<span class="rp-d">▼</span>' : k === 1 ? '<span class="rp-u">↗</span>' : k === -1 ? '<span class="rp-d">↘</span>' : '<span class="rp-m">·</span>'; };
    return `<div class="rp-list">${acts.slice(0, n).map((a) => `<div class="rp-li"><span class="dt">${dLabel(a.published_at)}</span>
      <span class="n">${glyph(a)} ${esc(firmName(a.firm))}<small>${esc(a.action)}${a.rating ? ` · ${esc(a.rating)}` : ''}${a.prior_rating ? ` (was ${esc(a.prior_rating)})` : ''}</small></span>
      <span class="r">${a.target != null ? `PT ${fmtNum(+a.target, 0)}${a.prior_target != null ? ` <small>(${fmtNum(+a.prior_target, 0)})</small>` : ''}` : ''}</span></div>`).join('')}</div>`;
  }

  function tiles(d, next) {
    const r = d.radar, out = [];
    if (r) out.push(`<div class="rp-tile"><div class="k">Run-up, 10 sessions</div><div class="v">${pct(r.runup_pct != null ? +r.runup_pct : null)}</div><div class="s">against same-size peers, to ${dLabel(r.asof_date)}</div></div>`);
    const c0 = d.cons.find((c) => c.horizon === '0q');
    if (c0 && c0.eps_avg != null && c0.eps_90d_ago != null && +c0.eps_90d_ago) {
      const ch = drift(+c0.eps_avg, +c0.eps_90d_ago);
      out.push(`<div class="rp-tile"><div class="k">Consensus drift</div><div class="v">${pct(ch)}</div><div class="s">EPS estimate over 90 days · ${fmtNum(+c0.eps_avg, 2)} now</div></div>`);
    }
    const g = next && d.nowcast.find((n) => n.variant === 'guided' && n.fiscal_year === next.fiscal_year && n.period === next.period);
    if (g && g.surprise_pct != null) out.push(`<div class="rp-tile hl"><div class="k">Nowcast against consensus</div><div class="v">${pct(+g.surprise_pct)}</div><div class="s">EPS ${fmtNum(+g.eps_mid, 2)} vs ${fmtNum(+g.consensus_eps, 2)} · ${Math.abs(+g.surprise_pct) < 5 ? 'in line' : +g.surprise_pct > 0 ? 'above' : 'below'}</div></div>`);
    const xs = d.history.filter((h) => h.x_1d != null).map((h) => +h.x_1d * 100);
    if (xs.length) out.push(`<div class="rp-tile"><div class="k">Typical report-day move</div><div class="v">±${(xs.reduce((a, b) => a + Math.abs(b), 0) / xs.length).toFixed(1)}%</div><div class="s">against the market · ${xs.length} reports, ${xs.filter((x) => x < 0).length} down</div></div>`);
    const ts = d.targets.length ? d.targets[d.targets.length - 1] : null;
    const tm = ts ? +ts.target_mean : d.ac[0] ? +d.ac[0].target_mean : null, px = (ts && +ts.current_price) || (r && +r.price) || null;
    if (tm && px) out.push(`<div class="rp-tile"><div class="k">Target upside</div><div class="v">${pct((tm / px - 1) * 100)}</div><div class="s">mean target ${fmtNum(tm, 1)} · ${esc((ts && ts.rec_key) || (d.ac[0] && d.ac[0].rec_key) || '')}</div></div>`);
    const b30 = d.brokers.filter((a) => day(a.published_at) >= day(todayIso) - 30);
    if (b30.length) {
      const up = b30.filter((a) => brokerDir(a) > 0).length, dn = b30.filter((a) => brokerDir(a) < 0).length;
      out.push(`<div class="rp-tile"><div class="k">Broker actions, 30 days</div><div class="v"><span class="rp-u">▲${up}</span> <span class="rp-d">▼${dn}</span></div><div class="s">${b30.length} on the wires · latest ${esc(firmName(b30[0].firm))}</div></div>`);
    }
    return out.length ? `<div class="rp-tiles">${out.join('')}</div>` : '';
  }

  function renderAhead(box, d) {
    const r = d.radar, cal = d.calendar;
    const next = r ? { fiscal_year: r.fiscal_year, period: r.period, date: r.report_date, est: r.date_source === 'estimated' }
      : (cal.filter((c) => c.planned_date >= todayIso).map((c) => ({ fiscal_year: c.fiscal_year, period: c.period, date: c.planned_date }))[0] || null);
    const prev = d.history.filter((h) => h.report_session && h.report_session < todayIso).slice(-1)[0];
    if (!next && !d.cons.length && !d.nowcast.length) { box.innerHTML = empty('No results date on the calendar and no consensus on file for this company.'); return null; }
    const nd = next ? day(next.date) - day(todayIso) : null;
    const prevD = prev ? day(prev.report_session) : r && r.prev_report ? day(r.prev_report) : next ? day(next.date) - 91 : day(todayIso) - 91;
    const head = `<div class="rp-hero">
      ${next ? ring(prevD, day(next.date), day(todayIso), nd <= 0 ? 'today' : String(nd), nd <= 0 ? 'report day' : `day${nd === 1 ? '' : 's'} to go`) : ''}
      <div class="rp-hero-main">
        <div class="rp-when">${next ? `<span class="am">${esc(next.period)} ${next.fiscal_year} results</span><span>${dLabel(next.date, true)}${next.est ? ' <em>estimated from last year</em>' : ''}</span>` : '<span class="am">No results date filed yet</span>'}</div>
        <div class="rp-chips">${r ? `<span class="c ice">run-up from ${dLabel(r.runup_from)}</span>` : ''}${r && r.closed_from ? `<span class="c amb">${r.in_closed_period ? 'insiders closed since' : 'closed period from'} ${dLabel(r.closed_from)}</span>` : ''}
          ${prev ? `<span class="c">last report ${dLabel(prev.report_session)} · ${pct(prev.x_1d != null ? +prev.x_1d * 100 : null)} on the day</span>` : ''}
          ${r ? `<span class="c">as of ${dLabel(r.asof_date)} close</span>` : next && nd > 35 ? '<span class="c">the radar opens 35 days before a report</span>' : ''}</div>
        ${tiles(d, next)}
      </div>
      ${r ? `<div class="rp-dialbox">${dial(r)}<div class="s">${r.direction === 'NONE' ? 'no directional evidence in the run-up' : `${bandOf(+r.positioning)} ${r.direction.toLowerCase()} positioning`}</div></div>` : ''}
    </div>`;
    const cons = consensusBlock(d), brk = d.brokers.length ? `<div class="rp-block"><div class="rp-bh">Broker actions <small>parsed from the wires · newest first</small></div>${brokerList(d.brokers, 7)}</div>` : '';
    const left = (r ? evidence(r) : '') + nowcastBlock(d.nowcast, d.ncAttr, d.ncHist);
    box.innerHTML = head + (left ? `<div class="rp-cols"><div>${left}</div><div>${cons}${brk}</div></div>`
      : cons && brk ? `<div class="rp-cols"><div>${cons}</div><div>${brk}</div></div>` : `<div>${cons}${brk}</div>`);
    const meta = $('rpMeta');
    if (meta) meta.innerHTML = (r ? `event radar as of ${r.asof_date} · ${r.date_source === 'estimated' ? 'date estimated' : 'date filed'} · <a class="rp-link" href="radar.html#report=${encodeURIComponent(r.isin)}">open in the radar ↗</a>`
      : next ? 'outside the radar window (35 days)' : 'no report on the calendar');
    return next;
  }

  /* ═══ ROAD TO THE REPORT — a lane per signal on one time axis ════════════ */
  function renderRoad(box, d, next) {
    const r = d.radar;
    const prev = d.history.filter((h) => h.report_session && h.report_session < todayIso).slice(-1)[0];
    const startD = Math.max(prev ? day(prev.report_session) - 4 : day(todayIso) - 100, day(todayIso) - 150);
    const endD = Math.max(next && next.date ? day(next.date) + 3 : day(todayIso) + 5, day(todayIso) + 2);
    const W = Math.max(300, box.clientWidth || 900), narrow = W < 620, L = narrow ? 4 : 150, R = narrow ? 4 : 14, CAP = narrow ? 14 : 0;
    const X = (dd) => L + ((dd - startD) / Math.max(1, endD - startD)) * (W - L - R);
    const lanes = [];

    const xs = d.excess.filter((p) => day(p.trade_date) >= startD);
    if (xs.length > 2) {
      let c = 1; const pts = xs.map((p) => { c *= 1 + (+p.xret || 0); return [day(p.trade_date), (c - 1) * 100]; });
      const lo = Math.min(0, ...pts.map((p) => p[1])), hi = Math.max(0, ...pts.map((p) => p[1])), sp = (hi - lo) || 1;
      lanes.push({ label: 'Share vs market', sub: `${signed(pts[pts.length - 1][1], 1)}% since ${dLabel(xs[0].trade_date)}`, h: 70,
        draw: (y0, h) => { const Y = (v) => y0 + 5 + (1 - (v - lo) / sp) * (h - 10);
          const line = pts.map((p) => `${X(p[0]).toFixed(1)},${Y(p[1]).toFixed(1)}`).join(' ');
          return `<line x1="${L}" x2="${W - R}" y1="${Y(0).toFixed(1)}" y2="${Y(0).toFixed(1)}" class="rp-zero"/>
            <polygon points="${X(pts[0][0]).toFixed(1)},${Y(0).toFixed(1)} ${line} ${X(pts[pts.length - 1][0]).toFixed(1)},${Y(0).toFixed(1)}" fill="${pts[pts.length - 1][1] >= 0 ? 'rgba(67,224,151,.09)' : 'rgba(240,96,96,.09)'}"/>
            <polyline fill="none" stroke="var(--ice)" stroke-width="1.8" stroke-linejoin="round" points="${line}"/>`; },
        hover: (dd) => { const p = pts.filter((q) => q[0] <= dd).slice(-1)[0]; return p ? [['Share vs market', `${signed(p[1], 1)}%`]] : []; } });
    }
    const snaps = d.snaps.filter((s) => next && s.fiscal_year === next.fiscal_year && s.period === next.period);
    if (r && next) snaps.push({ snapshot_date: r.asof_date, positioning: r.positioning, direction: r.direction, live: true });
    const sp = snaps.map((s) => [day(s.snapshot_date), s.direction === 'LONG' ? +s.positioning : s.direction === 'SHORT' ? -s.positioning : 0, s]).filter((p) => p[0] >= startD).sort((a, b) => a[0] - b[0]);
    if (sp.length) {
      const m = Math.max(33, ...sp.map((p) => Math.abs(p[1])));
      lanes.push({ label: 'Radar positioning', sub: `frozen each morning · ${sp.length} day${sp.length === 1 ? '' : 's'}`, h: 52,
        draw: (y0, h) => { const Y = (v) => y0 + h / 2 - (v / m) * (h / 2 - 4); const bw = Math.max(2, Math.min(8, (W - L - R) / Math.max(1, endD - startD) - 1));
          return `<line x1="${L}" x2="${W - R}" y1="${Y(0)}" y2="${Y(0)}" class="rp-zero"/>` + sp.map((p) => {
            const y = Y(p[1]), top = Math.min(y, Y(0)), hh = Math.max(1, Math.abs(y - Y(0)));
            return `<rect x="${(X(p[0]) - bw / 2).toFixed(1)}" y="${top.toFixed(1)}" width="${bw.toFixed(1)}" height="${hh.toFixed(1)}" rx="1" fill="${p[1] > 0 ? 'var(--green)' : p[1] < 0 ? 'var(--red)' : 'var(--text4)'}" opacity="${p[2].live ? 1 : 0.7}"/>`; }).join(''); },
        hover: (dd) => { const p = sp.filter((q) => q[0] <= dd).slice(-1)[0]; return p ? [['Radar', p[2].direction === 'NONE' ? 'quiet' : `${p[2].direction} ${Math.round(+p[2].positioning)}${p[0] !== dd ? ` (${dLabel(iso(p[0]))})` : ''}`]] : []; } });
    }
    const news = d.news.filter((n) => n.published_at && day(n.published_at) >= startD);
    if (news.length) {
      const per = new Map(); news.forEach((n) => { const k = day(n.published_at); per.set(k, (per.get(k) || []).concat(n)); });
      const mx = Math.max(...[...per.values()].map((v) => v.length));
      lanes.push({ label: 'News', sub: `${news.length} headline${news.length === 1 ? '' : 's'} naming it`, h: 40,
        draw: (y0, h) => [...per.entries()].map(([k, v]) => { const bh = Math.max(3, (v.length / mx) * (h - 8));
          return `<rect x="${(X(k) - 2).toFixed(1)}" y="${(y0 + h - 4 - bh).toFixed(1)}" width="4" height="${bh.toFixed(1)}" rx="1.5" fill="var(--purple)" opacity=".85"/>`; }).join(''),
        hover: (dd) => (per.get(dd) || []).slice(0, 4).map((n) => [esc(n.source || 'news'), esc(String(n.title).slice(0, 86))]) });
    }
    // dark pool: every directional day (small), and the impact days the radar counted (ringed)
    const dkBy = new Map();
    d.dark.forEach((x) => { if (!x.dark_side_bias || x.dark_side_bias === 'NEUTRAL' || day(x.trade_date) < startD) return;
      const o = dkBy.get(x.trade_date); if (!o || Math.abs(+x.dark_impact_bps || 0) > Math.abs(+o.dark_impact_bps || 0)) dkBy.set(x.trade_date, x); });
    const counted = new Map(((r && r.dark_days) || []).map((x) => [x.d, x]));
    if (dkBy.size || counted.size) {
      const days = new Set([...dkBy.keys(), ...counted.keys()]);
      lanes.push({ label: 'Dark pool', sub: `${days.size} directional day${days.size === 1 ? '' : 's'}${counted.size ? ` · ${counted.size} counted` : ''}`, h: 32,
        draw: (y0, h) => [...days].map((t) => { const x = dkBy.get(t), c = counted.get(t);
          const buy = c ? c.v > 0 : x.dark_side_bias === 'BUY', col = buy ? 'var(--green)' : 'var(--red)', cx = X(day(t)).toFixed(1), cy = y0 + h / 2;
          return c ? `<circle cx="${cx}" cy="${cy}" r="${c.top ? 6 : 4.8}" fill="${col}"/><circle cx="${cx}" cy="${cy}" r="${c.top ? 8.5 : 7}" fill="none" stroke="${col}" stroke-width="1" opacity=".55"/>`
            : `<circle cx="${cx}" cy="${cy}" r="2.8" fill="${col}" opacity=".6"/>`; }).join(''),
        hover: (dd) => { const t = iso(dd), x = dkBy.get(t), c = counted.get(t); if (!x && !c) return [];
          return [['Dark pool', c ? `${c.v > 0 ? 'buy' : 'sell'} impact ${signed(c.bps, 0)} bps${c.top ? ' · top decile' : ''} · counted` : `${x.dark_side_bias.toLowerCase()} · ${x.dark_impact_bps != null ? signed(+x.dark_impact_bps, 0) + ' bps' : esc(x.dark_evidence || '')}`]]; } });
    }
    if (d.shorts.length) {
      const pts = d.shorts.map((p) => [day(p.position_date), +p.short_pct]);
      const inWin = pts.filter((p) => p[0] >= startD), before = pts.filter((p) => p[0] < startD).slice(-1);
      const seq = before.map((p) => [startD, p[1]]).concat(inWin);
      if (seq.length) {
        const lo = Math.min(...seq.map((p) => p[1])), hi = Math.max(...seq.map((p) => p[1])), rg = (hi - lo) || Math.max(0.1, hi * 0.1);
        lanes.push({ label: 'Short interest', sub: `${seq[seq.length - 1][1].toFixed(2)}% of shares`, h: 42,
          draw: (y0, h) => { const Y = (v) => y0 + 5 + (1 - (v - lo) / rg) * (h - 10); let path = '';
            seq.forEach((p, i) => { const x = X(p[0]).toFixed(1); path += i ? ` H${x} V${Y(p[1]).toFixed(1)}` : `M${x},${Y(p[1]).toFixed(1)}`; });
            return `<path d="${path} H${X(Math.min(day(todayIso), endD)).toFixed(1)}" fill="none" stroke="var(--red)" stroke-width="1.7"/>`; },
          hover: (dd) => { const p = seq.filter((q) => q[0] <= dd).slice(-1)[0]; return p ? [['Short interest', `${p[1].toFixed(2)}%`]] : []; } });
      }
    }
    const ins = d.insiders.filter((t) => t.trade_date && day(t.trade_date) >= startD);
    if (ins.length) {
      lanes.push({ label: 'Insiders', sub: `${ins.length} trade${ins.length === 1 ? '' : 's'} · not scored`, h: 30,
        draw: (y0, h) => ins.map((t) => { const x = X(day(t.trade_date)), y = y0 + h / 2;
          return `<path d="M${x.toFixed(1)},${y - 6} L${(x + 6).toFixed(1)},${y} L${x.toFixed(1)},${y + 6} L${(x - 6).toFixed(1)},${y} Z" fill="${t.side === 'BUY' ? 'var(--green)' : 'var(--red)'}"/>`; }).join(''),
        hover: (dd) => ins.filter((t) => day(t.trade_date) === dd).map((t) => [t.side === 'BUY' ? 'Insider buy' : 'Insider sale', `${esc(t.person_name || '')}${+t.value >= 1e4 ? ` · ${fmtCompact(+t.value)} ${esc(t.currency || '')}` : ''}`]) });
    }
    const br = d.brokers.filter((a) => a.published_at && day(a.published_at) >= startD);
    if (br.length) {
      lanes.push({ label: 'Brokers', sub: `${br.length} action${br.length === 1 ? '' : 's'}`, h: 32,
        draw: (y0, h) => br.map((a) => { const x = X(day(a.published_at)), y = y0 + h / 2, k = brokerDir(a);
          return k > 0 ? `<path d="M${x.toFixed(1)},${y - 7} L${(x + 6).toFixed(1)},${y + 5} L${(x - 6).toFixed(1)},${y + 5} Z" fill="var(--green)" opacity="${k === 2 ? 1 : 0.6}"/>`
            : k < 0 ? `<path d="M${x.toFixed(1)},${y + 7} L${(x + 6).toFixed(1)},${y - 5} L${(x - 6).toFixed(1)},${y - 5} Z" fill="var(--red)" opacity="${k === -2 ? 1 : 0.6}"/>`
              : `<circle cx="${x.toFixed(1)}" cy="${y}" r="3.5" fill="var(--text2)"/>`; }).join(''),
        hover: (dd) => br.filter((a) => day(a.published_at) === dd).map((a) => [esc(firmName(a.firm)), `${esc(a.action)}${a.rating ? ` · ${esc(a.rating)}` : ''}${a.target != null ? ` · PT ${fmtNum(+a.target, 0)}${a.prior_target != null ? ` (${fmtNum(+a.prior_target, 0)})` : ''}` : ''}`]) });
    }
    const ncPath = d.ncHist.filter((h) => next && h.fiscal_year === next.fiscal_year && h.period === next.period && h.eps_mid != null && day(h.asof_date) >= startD);
    const cPath = d.consPath.filter((c) => c.eps_avg != null && day(c.snapshot_date) >= startD);
    if (ncPath.length > 1 || cPath.length) {
      const vals = ncPath.map((h) => +h.eps_mid).concat(cPath.map((c) => +c.eps_avg));
      const lo = Math.min(...vals), hi = Math.max(...vals), rg = (hi - lo) || Math.max(0.05, Math.abs(hi) * 0.05);
      lanes.push({ label: 'EPS, this quarter', sub: ncPath.length > 1 ? 'nowcast — consensus ●' : 'consensus ●', h: 54,
        draw: (y0, h) => { const Y = (v) => y0 + 6 + (1 - (v - lo) / rg) * (h - 12);
          return (ncPath.length > 1 ? `<polyline fill="none" stroke="var(--amber)" stroke-width="1.8" points="${ncPath.map((p) => `${X(day(p.asof_date)).toFixed(1)},${Y(+p.eps_mid).toFixed(1)}`).join(' ')}"/>` : '') +
            cPath.map((c) => `<circle cx="${X(day(c.snapshot_date)).toFixed(1)}" cy="${Y(+c.eps_avg).toFixed(1)}" r="3" fill="var(--text1)"/>`).join(''); },
        hover: (dd) => { const n = ncPath.filter((p) => day(p.asof_date) <= dd).slice(-1)[0], c = cPath.filter((p) => day(p.snapshot_date) <= dd).slice(-1)[0];
          return [n ? ['Nowcast EPS', fmtNum(+n.eps_mid, 2)] : null, c ? ['Consensus EPS', fmtNum(+c.eps_avg, 2)] : null].filter(Boolean); } });
    }
    if (!lanes.length) { box.innerHTML = empty('Nothing on file between the last report and the next one yet.'); return; }

    const TOP = 24, GAP = 8, H = TOP + lanes.reduce((a, l) => a + CAP + l.h + GAP, 0) + 20;
    const todayX = X(day(todayIso)), todayOn = day(todayIso) <= endD;
    let svg = `<svg class="rp-road" viewBox="0 0 ${W} ${H}" width="${W}" height="${H}" role="img" aria-label="Signals between the last report and the next">`;
    const bandR = (a, b, cls) => { const x1 = X(Math.max(a, startD)), x2 = X(Math.min(b, endD)); return x2 > x1 ? `<rect x="${x1.toFixed(1)}" y="${TOP - 4}" width="${(x2 - x1).toFixed(1)}" height="${H - TOP - 16}" class="${cls}"/>` : ''; };
    if (r && r.runup_from) svg += bandR(day(r.runup_from), day(r.report_date), 'rp-b-runup');
    if (r && r.closed_from) svg += bandR(day(r.closed_from), day(r.report_date), 'rp-b-closed');
    for (let m = new Date(iso(startD) + 'T00:00:00Z'); day(m.toISOString()) <= endD; m.setUTCMonth(m.getUTCMonth() + 1, 1)) {
      const dd = day(m.toISOString()); if (dd < startD) continue;
      const clash = todayOn && X(dd) + 30 > todayX - 36 && X(dd) < todayX;      // the month label gives way to "today"
      svg += `<line x1="${X(dd).toFixed(1)}" x2="${X(dd).toFixed(1)}" y1="${TOP - 4}" y2="${H - 16}" class="rp-grid"/>` +
        (clash ? '' : `<text x="${(X(dd) + 3).toFixed(1)}" y="${TOP - 9}" class="rp-ax">${m.toLocaleDateString('en-GB', { month: 'short', timeZone: 'UTC' })}</text>`);
    }
    const tl = (dd, cls, lab, anchor) => dd >= startD && dd <= endD ? `<line x1="${X(dd).toFixed(1)}" x2="${X(dd).toFixed(1)}" y1="${TOP - 4}" y2="${H - 16}" class="${cls}"/><text x="${(X(dd) + (anchor === 'end' ? -4 : 4)).toFixed(1)}" y="${H - 4}" class="rp-ax ${cls}-t" text-anchor="${anchor || 'start'}">${lab}</text>` : '';
    if (prev) svg += tl(day(prev.report_session), 'rp-rep', `${esc(prev.period)} report ${prev.x_1d != null ? signed(+prev.x_1d * 100, 1) + '%' : ''}`);
    if (todayOn) svg += `<line x1="${todayX.toFixed(1)}" x2="${todayX.toFixed(1)}" y1="${TOP - 4}" y2="${H - 16}" class="rp-today"/><text x="${(todayX - 4).toFixed(1)}" y="${TOP - 9}" class="rp-ax rp-today-t" text-anchor="end">today</text>`;
    if (next && next.date) svg += tl(day(next.date), 'rp-rep', `${esc(next.period)} report`, 'end');
    let y = TOP;
    for (const l of lanes) {
      svg += narrow ? `<text x="${L + 2}" y="${y + 10}" class="rp-ll">${esc(l.label)} <tspan class="rp-ls">${esc(l.sub.split(' · ')[0])}</tspan></text>`
        : `<text x="4" y="${y + l.h / 2 - 2}" class="rp-ll">${esc(l.label)}</text><text x="4" y="${y + l.h / 2 + 10}" class="rp-ls">${esc(l.sub)}</text>`;
      svg += `<line x1="0" x2="${W - R}" y1="${y + CAP + l.h + GAP / 2}" y2="${y + CAP + l.h + GAP / 2}" class="rp-sep"/>` + l.draw(y + CAP, l.h);
      y += CAP + l.h + GAP;
    }
    svg += `<line class="rp-cross" x1="-9" x2="-9" y1="${TOP - 4}" y2="${H - 16}"/><rect class="rp-hit" x="${L}" y="0" width="${W - L - R}" height="${H}" fill="transparent"/></svg>`;
    box.innerHTML = svg + `<div class="rp-tip" hidden></div><div class="rp-legend"><span><i class="lg-runup"></i>run-up window</span><span><i class="lg-closed"></i>insiders' closed period</span><span><i class="lg-rep"></i>report day</span>
      <span><i class="lg-dot"></i>dark-pool day · ringed: counted by the radar</span><span><i class="lg-tri"></i>broker upgrade / target raised · faint: target only</span></div>`;
    const svgEl = box.querySelector('svg'), tip = box.querySelector('.rp-tip'), cross = box.querySelector('.rp-cross'), hit = box.querySelector('.rp-hit');
    const move = (e) => {
      const rc = svgEl.getBoundingClientRect(), sx = (e.clientX - rc.left) * (W / rc.width);
      const dd = Math.round(startD + ((sx - L) / (W - L - R)) * (endD - startD));
      cross.setAttribute('x1', X(dd)); cross.setAttribute('x2', X(dd));
      const rows = lanes.flatMap((l) => l.hover(dd));
      tip.innerHTML = `<div class="h">${dLabel(iso(dd), true)}${r && iso(dd) === r.report_date ? ' · report day' : ''}</div>` +
        (rows.length ? rows.map((q) => `<div><span>${q[0]}</span><b>${q[1]}</b></div>`).join('') : '<div class="rp-m">nothing that day</div>');
      tip.hidden = false;
      const bx = box.getBoundingClientRect(), tw = tip.offsetWidth;
      let left = e.clientX - bx.left + 14; if (left + tw > bx.width - 4) left = e.clientX - bx.left - tw - 14;
      tip.style.left = Math.max(0, left) + 'px'; tip.style.top = Math.max(0, Math.min(e.clientY - bx.top - 10, bx.height - tip.offsetHeight - 4)) + 'px';
    };
    hit.addEventListener('pointermove', move);
    hit.addEventListener('pointerdown', move);
    hit.addEventListener('pointerleave', () => { tip.hidden = true; cross.setAttribute('x1', -9); cross.setAttribute('x2', -9); });
  }

  /* ═══ INTO EVERY REPORT — event study, −20 … +5 sessions ════════════════ */
  function intoPaths(d) {
    const px = d.excess, idx = new Map(px.map((p, i) => [p.trade_date, i])), out = [];
    for (const h of d.history) {
      const i0 = idx.get(h.report_session);
      if (i0 == null || i0 < 20) continue;
      const pts = []; let c = 1;
      for (let t = -20; t <= 5 && i0 + t < px.length; t++) { if (t > -20) c *= 1 + (+px[i0 + t].xret || 0); pts.push([t, (c - 1) * 100]); }
      out.push({ h, pts });
    }
    let cur = null;
    const r = d.radar;
    if (r && r.sessions_ahead != null && px.length) {
      let ia = -1; for (let i = px.length - 1; i >= 0; i--) if (px[i].trade_date <= r.asof_date) { ia = i; break; }
      const i0 = ia + +r.sessions_ahead, tLast = px.length - 1 - i0;
      if (ia >= 0 && tLast > -20 && tLast <= 0 && i0 - 20 >= 0) {
        const pts = []; let c = 1;
        for (let t = -20; t <= tLast; t++) { if (t > -20) c *= 1 + (+px[i0 + t].xret || 0); pts.push([t, (c - 1) * 100]); }
        cur = { pts, sessions: -tLast, label: `${r.period} ${r.fiscal_year}` };
      } else if (ia >= 0 && tLast <= -20) cur = { pts: [], sessions: -tLast, label: `${r.period} ${r.fiscal_year}`, waiting: true };
    }
    return { past: out, cur };
  }
  function renderInto(box, d) {
    const { past, cur } = intoPaths(d);
    if (!past.length) { box.innerHTML = empty('Not enough price history around past reports yet.'); return; }
    const W = Math.max(300, box.clientWidth || 900), narrow = W < 620, H = narrow ? 230 : 280, L = narrow ? 36 : 44, R = narrow ? 76 : 128, T = 14, B = 26;
    const all = past.flatMap((p) => p.pts.map((q) => q[1])).concat(cur ? cur.pts.map((q) => q[1]) : []);
    const lo = Math.min(-2, ...all), hi = Math.max(2, ...all), sp = hi - lo;
    const X = (t) => L + ((t + 20) / 25) * (W - L - R), Y = (v) => T + (1 - (v - lo + sp * 0.05) / (sp * 1.1)) * (H - T - B);
    const mean = [];
    for (let t = -20; t <= 5; t++) { const v = past.map((p) => p.pts.find((q) => q[0] === t)).filter(Boolean).map((q) => q[1]); if (v.length) mean.push([t, v.reduce((a, b) => a + b, 0) / v.length]); }
    const poly = (pts) => pts.map((q) => `${X(q[0]).toFixed(1)},${Y(q[1]).toFixed(1)}`).join(' ');
    let s = `<svg class="rp-into" viewBox="0 0 ${W} ${H}" width="${W}" height="${H}" role="img" aria-label="The share against the market around each report">`;
    s += `<rect x="${X(-1).toFixed(1)}" y="${T}" width="${(X(0) - X(-1)).toFixed(1)}" height="${H - T - B}" class="rp-into-day"/>`;
    const step = (hi - lo) > 24 ? 10 : (hi - lo) > 10 ? 5 : 2;
    for (let v = Math.ceil(lo / step) * step; v <= hi; v += step) s += `<line x1="${L}" x2="${W - R}" y1="${Y(v).toFixed(1)}" y2="${Y(v).toFixed(1)}" class="${v === 0 ? 'rp-zero' : 'rp-grid'}"/><text x="${L - 6}" y="${(Y(v) + 3).toFixed(1)}" class="rp-ax" text-anchor="end">${v > 0 ? '+' : v < 0 ? '−' : ''}${Math.abs(v)}%</text>`;
    [-20, -15, -10, -5, 0, 5].forEach((t) => { s += `<text x="${X(t).toFixed(1)}" y="${H - 8}" class="rp-ax${t === 0 ? ' rp-rep-t' : ''}" text-anchor="${t === -20 ? 'start' : 'middle'}">${t === 0 ? 'report day' : t > 0 ? '+' + t : t === -20 ? (narrow ? '−20' : '20 sessions before') : t}</text>`; });
    past.forEach((p, i) => { const x1 = p.h.x_1d != null ? +p.h.x_1d : 0;
      s += `<polyline class="rp-path ${x1 >= 0 ? 'up' : 'dn'}" data-i="${i}" fill="none" points="${poly(p.pts)}"/>`; });
    if (mean.length > 1) s += `<polyline class="rp-mean" fill="none" points="${poly(mean)}"/>`;
    if (cur && cur.pts.length > 1) {
      const e = cur.pts[cur.pts.length - 1];
      s += `<polyline class="rp-cur" fill="none" points="${poly(cur.pts)}"/><circle cx="${X(e[0]).toFixed(1)}" cy="${Y(e[1]).toFixed(1)}" r="4.5" class="rp-cur-dot"/>
        <text x="${(X(e[0]) + 9).toFixed(1)}" y="${(Y(e[1]) + 3).toFixed(1)}" class="rp-cur-t">${narrow ? '' : esc(cur.label) + ' '}${signed(e[1], 1)}%</text>`;
    }
    const m0 = mean.find((q) => q[0] === 0), m1 = mean.find((q) => q[0] === -1);
    if (mean.length) { const e = mean[mean.length - 1]; s += `<text x="${(X(e[0]) + 6).toFixed(1)}" y="${(Y(e[1]) + 3).toFixed(1)}" class="rp-mean-t">${narrow ? 'avg' : 'average'} ${signed(e[1], 1)}%</text>`; }
    past.forEach((p, i) => { s += `<polyline class="rp-path-hit" data-i="${i}" fill="none" points="${poly(p.pts)}"/>`; });
    s += '</svg>';
    const ru = past.map((p) => p.pts.find((q) => q[0] === -1)).filter(Boolean).map((q) => q[1]);
    const up = past.filter((p) => p.h.x_1d != null && +p.h.x_1d > 0).length;
    const lede = `<div class="rp-into-lede">${past.length} reports since ${String(past[0].h.report_session).slice(0, 4)}. Into them the share ran ${pct(ru.reduce((a, b) => a + b, 0) / (ru.length || 1))} on average against the market over 20 sessions, then
      rose on the day ${up} time${up === 1 ? '' : 's'} and fell ${past.length - up}${m0 && m1 ? ` (average report day ${pct(m0[1] - m1[1])})` : ''}.
      ${cur ? cur.waiting ? ` The ${esc(cur.label)} run-up window opens in ${cur.sessions - 20} session${cur.sessions - 20 === 1 ? '' : 's'}.` : ` This quarter so far: ${pct(cur.pts[cur.pts.length - 1][1])}, ${cur.sessions} session${cur.sessions === 1 ? '' : 's'} to go.` : ''}</div>`;
    box.innerHTML = lede + `<div class="rp-into-wrap">${s}<div class="rp-tip" hidden></div></div>
      <div class="rp-legend"><span><i class="lg-cur"></i>this quarter's run-up</span><span><i class="lg-mean"></i>average of past reports</span><span><i class="lg-up"></i>rose on the day</span><span><i class="lg-dn"></i>fell</span><span>hover a line</span></div>`;
    const wrap = box.querySelector('.rp-into-wrap'), tip = wrap.querySelector('.rp-tip');
    wrap.querySelectorAll('.rp-path-hit').forEach((el) => {
      el.addEventListener('pointerenter', (e) => {
        const p = past[+el.dataset.i], h = p.h;
        wrap.querySelectorAll('.rp-path').forEach((x) => x.classList.toggle('hl', x.dataset.i === el.dataset.i));
        wrap.classList.add('hov');
        const at = (t) => { const q = p.pts.find((z) => z[0] === t); return q ? q[1] : null; };
        tip.innerHTML = `<div class="h">${esc(h.period)} ${h.fiscal_year} · ${dLabel(h.report_session)}</div>
          <div><span>Run-up, 20 sessions</span><b>${pct(at(-1))}</b></div><div><span>Report day</span><b>${pct(h.x_1d != null ? +h.x_1d * 100 : null)}</b></div>
          <div><span>5 days after</span><b>${pct(h.x_5d != null ? +h.x_5d * 100 : null)}</b></div>${h.surprise_pct != null ? `<div><span>EPS surprise</span><b>${pct(+h.surprise_pct * 100)}</b></div>` : ''}`;
        tip.hidden = false;
        const bx = wrap.getBoundingClientRect(); let left = e.clientX - bx.left + 14; if (left + tip.offsetWidth > bx.width) left = e.clientX - bx.left - tip.offsetWidth - 14;
        tip.style.left = Math.max(0, left) + 'px'; tip.style.top = Math.max(0, e.clientY - bx.top - 30) + 'px';
      });
      el.addEventListener('pointerleave', () => { tip.hidden = true; wrap.classList.remove('hov'); wrap.querySelectorAll('.rp-path.hl').forEach((x) => x.classList.remove('hl')); });
    });
  }

  /* ═══ REPORT HISTORY — with the release itself on a click ═══════════════ */
  const KEYWORDS = /\b(outlook|guidance|guiding|expects?|expected|record|impairments?|write-?downs?|dividends?|buy-?backs?|utsikter|forventer|forventet|rekord\w*|nedskrivning\w*|utbytte|tilbakekjøp\w*|veiledning)\b/gi;
  function renderHistory(box, d) {
    const h = d.history.slice().reverse();
    if (!h.length) { box.innerHTML = empty('No reports since 2024 on file.'); return; }
    const graded = new Map(d.graded.map((g) => [`${g.fiscal_year} ${g.period}`, g]));
    const fwd = new Map();
    d.forward.forEach((g) => { const k = `${g.fiscal_year} ${g.period}`, o = fwd.get(k); if (!o || g.snapshot_date > o.snapshot_date) fwd.set(k, g); });
    const mx = Math.max(...h.map((x) => Math.abs(+x.x_1d || 0))) || 0.01;
    box.innerHTML = `<div class="tbl-wrap"><table class="rp-tbl"><thead><tr><th>Report</th><th>Session</th><th>Report day against the market</th><th class="num">5 days</th><th class="num">Run-up</th><th class="num">EPS surprise</th><th>Radar before</th><th>Release</th></tr></thead><tbody>
      ${h.map((x, i) => { const v = x.x_1d != null ? +x.x_1d : null, w = v == null ? 0 : Math.abs(v) / mx * 50, k = `${x.fiscal_year} ${x.period}`, f = fwd.get(k), g = graded.get(k), rr = f || g;
        const bar = v == null ? '' : `<span class="rp-bar"><i style="${v >= 0 ? 'left' : 'right'}:50%;width:${w.toFixed(1)}%;background:${v >= 0 ? 'var(--green)' : 'var(--red)'}"></i></span>`;
        const radar = rr && rr.direction && rr.direction !== 'NONE' ? `<span style="color:${rr.direction === 'LONG' ? 'var(--green)' : 'var(--red)'}">${rr.direction === 'LONG' ? '▲' : '▼'} ${Math.round(+rr.positioning)}</span> <small>${f ? 'live' : 'rebuilt'}</small>`
          : rr ? `<span class="rp-m">quiet</span> <small>${f ? 'live' : 'rebuilt'}</small>` : '<span class="rp-m">—</span>';
        return `<tr class="rp-row" data-i="${i}" tabindex="0" aria-expanded="false"><td class="nm">${esc(x.period)} ${x.fiscal_year}</td><td>${dLabel(x.report_session)}${x.pre_open ? ' <small>pre-open</small>' : ''}</td>
          <td class="bar">${bar}<b class="${v >= 0 ? 'rp-u' : 'rp-d'}">${v == null ? '—' : signed(v * 100, 1) + '%'}</b>${x.z_1d != null && Math.abs(+x.z_1d) >= 2 ? ` <small>${signed(+x.z_1d, 1)}σ</small>` : ''}</td>
          <td class="num">${x.x_5d != null ? pct(+x.x_5d * 100) : '—'}</td><td class="num">${x.x_runup_20d != null ? pct(+x.x_runup_20d * 100) : '—'}</td>
          <td class="num">${x.surprise_pct != null ? pct(+x.surprise_pct * 100) : '<span class="rp-m">—</span>'}</td><td>${radar}</td>
          <td class="tt"><span>${esc(String(x.title || '').replace(/^[^:]{2,40}:\s*/, ''))}</span><i class="chev">▾</i></td></tr>`; }).join('')}
      </tbody></table></div><div class="rp-note">Moves are against the market, from the close before the report session. "Radar before" is the frozen live snapshot where we have one (from Q3 2026), otherwise the radar rebuilt for that date. Click a report to read its release.</div>`;
    box.querySelectorAll('.rp-row').forEach((tr) => {
      const toggle = async () => {
        const nx = tr.nextElementSibling;
        if (nx && nx.classList.contains('rp-rel')) { nx.remove(); tr.setAttribute('aria-expanded', 'false'); return; }
        const x = h[+tr.dataset.i];
        const row = document.createElement('tr'); row.className = 'rp-rel';
        row.innerHTML = `<td colspan="8"><div class="rp-reltxt">Loading the release …</div></td>`;
        tr.after(row); tr.setAttribute('aria-expanded', 'true');
        const t = x.message_id ? await safeGet(`report_texts?message_id=eq.${encodeURIComponent(x.message_id)}&select=body,attachments`) : null;
        const body = t && t[0] && t[0].body ? String(t[0].body) : '';
        const cut = body.length > 1400 ? body.slice(0, 1400).replace(/\s+\S*$/, '') + ' …' : body;
        row.querySelector('td').innerHTML = `<div class="rp-reltxt"><div class="h"><b>${esc(x.title || '')}</b><span>${x.published_at ? new Date(x.published_at).toLocaleString('en-GB', { timeZone: 'Europe/Oslo', day: 'numeric', month: 'short', year: 'numeric', hour: '2-digit', minute: '2-digit' }) : ''}</span></div>
          ${cut ? `<p>${esc(cut).replace(KEYWORDS, '<mark>$1</mark>')}</p>` : '<p class="rp-m">No text stored for this release.</p>'}
          ${t && t[0] && t[0].attachments ? `<div class="att">Attachments: ${esc(t[0].attachments)}</div>` : ''}
          ${x.message_id ? `<a href="https://newsweb.oslobors.no/message/${encodeURIComponent(x.message_id)}" target="_blank" rel="noopener">The full release on NewsWeb →</a>` : ''}</div>`;
      };
      tr.addEventListener('click', toggle);
      tr.addEventListener('keydown', (e) => { if (e.key === 'Enter' || e.key === ' ') { e.preventDefault(); toggle(); } });
    });
  }

  /* ═══ IN THE NEWS ═══════════════════════════════════════════════════════ */
  function renderNews(box, d) {
    const news = d.news;
    if (!news.length && !d.brokers.length) { box.innerHTML = empty('No headlines recorded yet. The news capture started on 19 Oct 2026 with 30 days back.'); return; }
    const srcs = {}; news.forEach((n) => { const s = n.source || n.feed; srcs[s] = (srcs[s] || 0) + 1; });
    const top = Object.entries(srcs).sort((a, b) => b[1] - a[1]).slice(0, 5).map(([s, c]) => `${esc(s)} ${c}`).join(' · ');
    box.innerHTML = `<div class="rp-cols"><div><div class="rp-bh">Headlines <small>${news.length} naming the company${top ? ` · ${top}` : ''}</small></div>
      <div class="rp-list news">${news.slice(0, 24).map((n) => `<div class="rp-li"><span class="dt">${dLabel(n.published_at)}</span><span class="n wrap"><a href="${esc(n.url)}" target="_blank" rel="noopener nofollow">${esc(n.title)}</a></span><span class="r src">${esc(n.source || n.feed)}</span></div>`).join('') || empty('No headlines yet.')}</div></div>
      <div><div class="rp-bh">Broker actions <small>parsed from the headlines · ${d.brokers.length}</small></div>${brokerList(d.brokers, 16) || empty('None recorded yet.')}</div></div>`;
  }

  async function render(isin) {
    const Q = encodeURIComponent(isin);
    const box = { ahead: $('rpAhead'), road: $('rpRoad'), into: $('rpInto'), history: $('rpHistory'), news: $('rpNews') };
    if (!box.ahead) return null;
    const since = iso(day(todayIso) - 160);
    const g = (p) => safeGet(p).then((x) => x || []);
    const [radar, calendar, history, graded, forward, snaps, cons, consPath, targets, ac, rec, brokers, news, nowcast, ncAttr, ncHist, excess, dark, shorts, insiders] = await Promise.all([
      g(`report_radar?isin=eq.${Q}`),
      g(`report_calendar?isin=eq.${Q}&kind=eq.results&select=fiscal_year,period,planned_date,reported_at&order=planned_date.asc`),
      g(`report_reactions?isin=eq.${Q}&order=report_session.asc`),
      g(`report_radar_graded?isin=eq.${Q}&select=fiscal_year,period,positioning,direction,reaction,reaction_5d`),
      g(`report_radar_forward?isin=eq.${Q}&select=fiscal_year,period,snapshot_date,positioning,direction,reaction,reaction_5d`),
      g(`report_radar_snapshot?isin=eq.${Q}&snapshot_date=gte.${since}&select=snapshot_date,fiscal_year,period,positioning,direction&order=snapshot_date.asc`),
      g(`consensus_snapshot?isin=eq.${Q}&order=snapshot_date.desc&limit=8`),
      g(`consensus_snapshot?isin=eq.${Q}&horizon=eq.0q&select=snapshot_date,period_end,eps_avg&order=snapshot_date.asc`),
      g(`analyst_target_snapshot?isin=eq.${Q}&order=snapshot_date.asc`),
      g(`analyst_consensus?isin=eq.${Q}`),
      g(`analyst_rec_latest?isin=eq.${Q}`),
      g(`broker_actions?isin=eq.${Q}&order=published_at.desc&limit=40`),
      g(`company_news?isin=eq.${Q}&order=published_at.desc&limit=80`),
      g(`nowcast_latest?isin=eq.${Q}`),
      g(`nowcast_attribution?isin=eq.${Q}&order=asof_date.desc&limit=1`),
      g(`nowcast_daily?isin=eq.${Q}&variant=eq.guided&select=asof_date,fiscal_year,period,eps_mid,consensus_eps&order=asof_date.asc&limit=600`),
      g(`price_excess?isin=eq.${Q}&trade_date=gte.2023-11-01&select=trade_date,xret&order=trade_date.asc`),
      g(`dark_flow_summary?isin=eq.${Q}&trade_date=gte.${since}&select=trade_date,dark_side_bias,dark_impact_bps,dark_evidence&order=trade_date.asc`),
      g(`short_interest?isin=eq.${Q}&position_date=gte.${iso(day(todayIso) - 400)}&select=position_date,short_pct&order=position_date.asc`),
      g(`insider_trades?isin=eq.${Q}&trade_date=gte.${since}&side=in.(BUY,SELL)&select=trade_date,person_name,person_role,side,value,currency&order=trade_date.asc`),
    ]);
    const last = cons.length ? cons[0].snapshot_date : null;
    // the wires repeat themselves: one broker action comes as two or three headlines (Norwegian and English, often a
    // day apart), and the same headline under two prefixes ("HYDRO:", "HYDRO(NY):"). Shown once, the fullest copy.
    const fullness = (a) => (a.target != null) + (a.prior_target != null) + !!a.rating + !!a.prior_rating;
    const acts = [];
    for (const a of brokers) {
      const k = firmName(a.firm).toLowerCase().replace(/[^a-zæøå0-9]/g, ''), sg = Math.sign(brokerDir(a)), dd = day(a.published_at);
      const dup = acts.find((b) => b._k === k && b._s === sg && Math.abs(b._d - dd) <= 2);
      if (!dup) acts.push(Object.assign({ _k: k, _s: sg, _d: dd }, a));
      else if (fullness(a) > fullness(dup)) Object.assign(dup, a);
    }
    const seenT = new Set(), headlines = news.filter((n) => {
      const k = String(n.title || '').toLowerCase().replace(/^[^:]{1,24}:\s*/, '').replace(/[^a-zæøå0-9]+/g, ' ').trim().slice(0, 90);
      if (seenT.has(k)) return false; seenT.add(k); return true;
    });
    const d = { radar: radar[0] || null, calendar, history: history.filter((x) => x.report_session), graded, forward, snaps,
      cons: cons.filter((c) => c.snapshot_date === last), consPath, targets, ac, rec, brokers: acts, news: headlines, nowcast, ncAttr, ncHist, excess, dark, shorts, insiders };
    const step = (name, fn) => { try { fn(); } catch (e) { console.error('dossier-report', name, e); } };
    let next = null;
    step('ahead', () => { next = renderAhead(box.ahead, d); });
    if (next) d.consPath = d.consPath.filter((c) => String(c.period_end) >= iso(day(next.date) - 120));
    step('road', () => box.road && renderRoad(box.road, d, next));
    step('into', () => box.into && renderInto(box.into, d));
    step('history', () => box.history && renderHistory(box.history, d));
    step('news', () => box.news && renderNews(box.news, d));
    let t, w = window.innerWidth;
    window.addEventListener('resize', () => { if (window.innerWidth === w) return; w = window.innerWidth; clearTimeout(t);
      t = setTimeout(() => { step('road', () => box.road && renderRoad(box.road, d, next)); step('into', () => box.into && renderInto(box.into, d)); }, 180); });
    d.next = next;
    return d;
  }

  window.DossierReport = { render };
})();
