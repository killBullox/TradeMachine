import { useEffect, useState } from 'react'
import toast from 'react-hot-toast'
import { Brain, TrendingUp, AlertTriangle, Shield, Search, Lightbulb, FlaskConical, Activity, Zap } from 'lucide-react'

const PRIORITY_STYLE = {
  alta:  'bg-rose-600/25 text-rose-300',
  media: 'bg-amber-600/25 text-amber-300',
  bassa: 'bg-slate-600/40 text-slate-300',
}

const SIM_RULES = {
  exclude_hours:     { label: 'Escludi ore (Roma)',        hint: 'es. 9,10',                    build: v => ({ hours: v.split(',').map(s => parseInt(s.trim())).filter(n => !isNaN(n)) }) },
  exclude_sessions:  { label: 'Escludi sessioni',          hint: 'asia, londra, new_york, notte', build: v => ({ sessions: v.split(',').map(s => s.trim()).filter(Boolean) }) },
  exclude_weekdays:  { label: 'Escludi giorni',            hint: 'lun, mar, mer, gio, ven',     build: v => ({ weekdays: v.split(',').map(s => s.trim()).filter(Boolean) }) },
  exclude_direction: { label: 'Escludi direzione',         hint: 'buy oppure sell',             build: v => ({ direction: v.trim() }) },
  min_rr_tp1:        { label: 'R:R minimo su TP1',         hint: 'es. 0.5',                     build: v => ({ min_rr: parseFloat(v) }) },
  cap_loss_at_risk:  { label: 'Cap perdite al max-risk',   hint: 'nessun parametro',            build: () => ({}) },
  scale_risk:        { label: 'Scala rischio per trade',   hint: 'fattore, es. 0.5',            build: v => ({ factor: parseFloat(v) }) },
  exclude_near_news: { label: 'Escludi entrate vicino news', hint: 'minuti, es. 30',            build: v => ({ minutes: parseInt(v) || 30 }) },
}

function ImpactBox({ impact }) {
  if (!impact) return null
  if (!impact.ok) {
    return <p className="mt-2 text-xs text-amber-400">⚠ Simulazione non riuscita: {impact.error}</p>
  }
  const d = impact.delta_pnl
  const good = d > 0
  const v = impact.validation
  return (
    <div className={`mt-2 rounded-lg p-2.5 text-xs border ${good ? 'border-emerald-700/50 bg-emerald-900/20' : d < 0 ? 'border-rose-700/50 bg-rose-900/20' : 'border-slate-700 bg-slate-800/40'}`}>
      <p className="font-semibold mb-1 flex items-center gap-2 flex-wrap">
        <span className={good ? 'text-emerald-300' : d < 0 ? 'text-rose-300' : 'text-slate-300'}>
          Impatto simulato: {d >= 0 ? '+' : ''}{d}$ ({impact.verdict})
        </span>
        {v && v.passed && (
          <span className="text-[10px] px-1.5 py-0.5 rounded bg-emerald-600/30 text-emerald-300 font-semibold">
            ✓ VERIFICATA{v.bootstrap_confidence != null ? ` · confidenza ${Math.round(v.bootstrap_confidence * 100)}%` : ''}
          </span>
        )}
        {v && !v.passed && (
          <span className="text-[10px] px-1.5 py-0.5 rounded bg-amber-600/30 text-amber-300 font-semibold">
            ✗ NON VALIDATA
          </span>
        )}
      </p>
      <p className="text-slate-400">
        P&L: {impact.baseline.pnl}$ → <span className="text-slate-200">{impact.simulated.pnl}$</span>
        {' · '}Win rate: {impact.baseline.win_rate}% → {impact.simulated.win_rate}%
        {' · '}Trade: {impact.baseline.trades} → {impact.simulated.trades}
        {impact.trades_excluded > 0 && ` (${impact.trades_excluded} esclusi)`}
        {impact.trades_modified > 0 && ` (${impact.trades_modified} modificati)`}
      </p>
      {v && (
        <p className="text-slate-500 mt-1">
          Campione: {v.n_affected} trade toccati ({v.category})
          {v.top1_share != null && ` · miglior trade = ${Math.round(v.top1_share * 100)}% del delta`}
          {v.delta_without_top1 != null && ` · senza top-1: ${v.delta_without_top1 >= 0 ? '+' : ''}${v.delta_without_top1}$`}
        </p>
      )}
      {v && v.fail_reasons?.length > 0 && (
        <p className="text-amber-400 mt-1">Motivi: {v.fail_reasons.join('; ')}</p>
      )}
      <p className="text-slate-600 mt-1">Assunzioni: trade indipendenti, esclusioni non alterano i segnali successivi.</p>
    </div>
  )
}

function Simulator() {
  const [rule, setRule] = useState('min_rr_tp1')
  const [paramText, setParamText] = useState('')
  const [running, setRunning] = useState(false)
  const [result, setResult] = useState(null)

  const run = async () => {
    setRunning(true); setResult(null)
    try {
      const params = SIM_RULES[rule].build(paramText || '')
      const res = await fetch('/api/advisor/simulate', {
        method: 'POST', headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ type: rule, params }),
      }).then(r => r.json())
      setResult(res)
      if (!res.ok) toast.error(res.error || 'Simulazione fallita')
    } catch (e) { toast.error(`Errore: ${e.message}`) }
    finally { setRunning(false) }
  }

  return (
    <div className="card p-5">
      <h2 className="text-sm font-semibold text-violet-300 mb-3 uppercase tracking-wider flex items-center gap-2">
        <FlaskConical size={15} /> Simulatore what-if
      </h2>
      <div className="flex gap-2 flex-wrap items-end mb-2">
        <div>
          <label className="text-xs text-slate-400 block mb-1">Regola</label>
          <select value={rule} onChange={e => { setRule(e.target.value); setParamText(''); setResult(null) }}
            className="px-3 py-2 text-sm bg-slate-800 border border-slate-700 rounded-lg text-white">
            {Object.entries(SIM_RULES).map(([k, r]) => <option key={k} value={k}>{r.label}</option>)}
          </select>
        </div>
        <div>
          <label className="text-xs text-slate-400 block mb-1">Parametri ({SIM_RULES[rule].hint})</label>
          <input value={paramText} onChange={e => setParamText(e.target.value)}
            placeholder={SIM_RULES[rule].hint} disabled={rule === 'cap_loss_at_risk'}
            className="px-3 py-2 text-sm bg-slate-800 border border-slate-700 rounded-lg text-white w-52 disabled:opacity-40" />
        </div>
        <button onClick={run} disabled={running}
          className="px-4 py-2 text-sm bg-violet-600 hover:bg-violet-500 disabled:opacity-50 text-white rounded-lg font-medium">
          {running ? 'Simulo…' : '▶ Simula'}
        </button>
      </div>
      <p className="text-xs text-slate-500 mb-1">
        Calcolo esatto sui trade reali storici: cosa sarebbe successo applicando la regola dall'inizio.
      </p>
      <ImpactBox impact={result} />
    </div>
  )
}

function KPI({ label, value, sub, color }) {
  return (
    <div className="card flex flex-col gap-1">
      <p className="text-xs text-slate-400 uppercase tracking-wide">{label}</p>
      <p className={`font-bold text-2xl ${color || 'text-white'}`}>{value ?? '—'}</p>
      {sub && <p className="text-xs text-slate-500">{sub}</p>}
    </div>
  )
}

function Section({ icon: Icon, title, items, color }) {
  if (!items || items.length === 0) return null
  return (
    <div className="card p-5">
      <h2 className={`text-sm font-semibold mb-3 uppercase tracking-wider flex items-center gap-2 ${color || 'text-slate-300'}`}>
        <Icon size={15} /> {title}
      </h2>
      <ul className="space-y-2">
        {items.map((it, i) => (
          <li key={i} className="text-sm text-slate-300 flex gap-2">
            <span className="text-slate-600 flex-shrink-0">▸</span>
            <span>{it}</span>
          </li>
        ))}
      </ul>
    </div>
  )
}

function MonitorCard({ rule, onPromote, onRollback }) {
  const m = rule.monitor || {}
  const dObs = m.delta_osservato ?? m.pnl_evitato
  const dExp = rule.delta_promesso
  return (
    <div className="bg-slate-800/60 rounded-lg p-3">
      <div className="flex items-center justify-between gap-2 flex-wrap">
        <div>
          <p className="text-sm font-semibold text-white">{rule.title}</p>
          <p className="text-xs text-slate-500">
            {rule.sim_type} · attiva dal <span className="font-mono">{rule.attiva_dal_roma}</span>
          </p>
        </div>
        <div className="flex gap-2">
          {rule.mode === 'test' && (
            <button onClick={() => onPromote(rule)} className="px-3 py-1.5 text-xs bg-emerald-600 hover:bg-emerald-500 text-white rounded-lg font-medium">
              ⚡ Approva
            </button>
          )}
          <button onClick={() => onRollback(rule)} className="px-3 py-1.5 text-xs bg-slate-600 hover:bg-slate-500 text-white rounded-lg font-medium">
            ↩ Rollback
          </button>
        </div>
      </div>
      {m.ok ? (
        <div className="mt-2 text-xs text-slate-400 space-y-0.5">
          {m.tipo === 'reale_blocco' ? (
            <>
              <p>Trade bloccati: <span className="text-slate-200">{m.trade_bloccati}</span>
                {' · '}esiti simulati disponibili: {m.esiti_simulati_disponibili}</p>
              <p>P&L evitato: <span className={m.pnl_evitato > 0 ? 'text-emerald-300 font-semibold' : m.pnl_evitato < 0 ? 'text-rose-300 font-semibold' : 'text-slate-300'}>
                {m.pnl_evitato >= 0 ? '+' : ''}{m.pnl_evitato}$</span>
                {' '}(positivo = il blocco ha risparmiato)</p>
            </>
          ) : (
            <>
              <p>Trade nel periodo: <span className="text-slate-200">{m.trade_nel_periodo}</span>
                {' · '}toccati dalla regola: {m.trade_toccati ?? '—'}</p>
              {m.pnl_reale != null && (
                <p>P&L reale {m.pnl_reale}$ → con regola <span className="text-slate-200">{m.pnl_con_regola}$</span></p>
              )}
              {dObs != null && (
                <p>Delta osservato: <span className={dObs > 0 ? 'text-emerald-300 font-semibold' : dObs < 0 ? 'text-rose-300 font-semibold' : 'text-slate-300'}>
                  {dObs >= 0 ? '+' : ''}{dObs}$</span>
                  {dExp != null && <span className="text-slate-500"> · promesso: {dExp >= 0 ? '+' : ''}{dExp}$</span>}</p>
              )}
              {m.nota && <p className="text-amber-400">{m.nota}</p>}
            </>
          )}
        </div>
      ) : (
        <p className="mt-2 text-xs text-amber-400">Monitor non disponibile: {m.error}</p>
      )}
    </div>
  )
}

export default function Advisor() {
  const [report, setReport] = useState(null)
  const [rules, setRules] = useState({ test: [], real: [] })
  const [loading, setLoading] = useState(true)
  const [generating, setGenerating] = useState(false)

  const load = async () => {
    try {
      const [d, ru] = await Promise.all([
        fetch('/api/advisor').then(r => r.json()),
        fetch('/api/advisor/rules').then(r => r.json()).catch(() => ({ test: [], real: [] })),
      ])
      setReport(d.report)
      setRules(ru)
    } catch { toast.error('Errore caricamento advisor') }
    finally { setLoading(false) }
  }
  useEffect(() => { load() }, [])

  const recAction = async (rec, action) => {
    try {
      if (action === 'test' || action === 'real') {
        const res = await fetch('/api/advisor/rules', {
          method: 'POST', headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
            mode: action, sim_type: rec.sim_type,
            sim_params: JSON.parse(rec.sim_params || '{}'),
            title: rec.title, source_detail: rec.detail,
            expected: rec.impact || null,
          }),
        }).then(r => r.json())
        if (!res.ok) { toast.error(res.error || 'Errore'); return }
        toast.success(action === 'test' ? 'Regola in Monitor Test' : 'Regola APPROVATA (Monitor Reale)')
      } else {
        toast('Consiglio rifiutato (l\'advisor può riproporlo)', { icon: '✋' })
      }
      await fetch('/api/advisor/rec-action', {
        method: 'POST', headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ report_id: report.id, title: rec.title, action }),
      })
      load()
    } catch (e) { toast.error(`Errore: ${e.message}`) }
  }

  const promoteRule = async (rule) => {
    if (!confirm(`Approvare "${rule.title}"? La regola AGIRÀ sui trade futuri.`)) return
    const res = await fetch(`/api/advisor/rules/${rule.id}/promote`, { method: 'POST' }).then(r => r.json())
    if (res.ok) toast.success('Regola promossa a Monitor Reale')
    else toast.error(res.error || 'Errore')
    load()
  }

  const rollbackRule = async (rule) => {
    if (!confirm(`Rollback di "${rule.title}"? La regola si disattiva.`)) return
    const res = await fetch(`/api/advisor/rules/${rule.id}/rollback`, { method: 'POST' }).then(r => r.json())
    if (res.ok) toast.success('Rollback eseguito')
    else toast.error(res.error || 'Errore')
    load()
  }

  const generate = async () => {
    setGenerating(true)
    toast('Analisi in corso… (30-90 secondi)', { icon: '🧠' })
    try {
      const d = await fetch('/api/advisor/generate', { method: 'POST' }).then(r => r.json())
      if (d.report?.status === 'ok') {
        setReport(d.report)
        toast.success('Analisi completata')
      } else {
        toast.error(`Errore analisi: ${d.report?.error || 'sconosciuto'}`)
        if (d.report) setReport(d.report)
      }
    } catch (e) { toast.error(`Errore: ${e.message}`) }
    finally { setGenerating(false) }
  }

  if (loading) return <div className="p-6 text-slate-400">Caricamento…</div>

  const s = report?.sections
  const ov = report?.stats?.overall
  const ex = report?.stats?.execution

  return (
    <div className="p-6 space-y-6 max-w-5xl">
      <div className="flex items-center justify-between flex-wrap gap-3">
        <h1 className="text-xl font-bold text-white flex items-center gap-2">
          <Brain size={22} className="text-brand-400" /> AI Advisor
        </h1>
        <button onClick={generate} disabled={generating}
          className="px-4 py-2 text-sm bg-brand-600 hover:bg-brand-500 disabled:opacity-50 text-white rounded-lg font-medium">
          {generating ? 'Analizzo…' : '🧠 Rigenera analisi'}
        </button>
      </div>

      {report && (
        <p className="text-xs text-slate-500">
          Ultimo report: <span className="font-mono">{report.created_at_roma}</span>
          {' · '}{report.model}{' · '}{report.tokens_in + report.tokens_out} token
          {' · '}{report.duration_s}s
          {' · '}{report.stats?.trades_analyzed ?? '—'} trade reali analizzati
          {report.status === 'error' && <span className="text-rose-400"> · ERRORE: {report.error}</span>}
        </p>
      )}

      {!report && (
        <div className="card p-8 text-center text-slate-400">
          <Brain size={40} className="mx-auto mb-3 text-slate-600" />
          <p>Nessuna analisi ancora generata.</p>
          <p className="text-xs text-slate-500 mt-1">
            Il report viene generato automaticamente ogni sera dopo la chiusura NY,
            oppure premi "Rigenera analisi".
          </p>
        </div>
      )}

      {ov && (
        <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
          <KPI label="P&L totale" value={`${ov.total_pnl >= 0 ? '+' : ''}${ov.total_pnl}$`}
            color={ov.total_pnl >= 0 ? 'text-emerald-400' : 'text-rose-400'} />
          <KPI label="Win rate" value={`${ov.win_rate}%`} sub={`${ov.wins}W / ${ov.losses}L`} />
          <KPI label="Profit factor" value={ov.profit_factor ?? '∞'} />
          <KPI label="Max drawdown" value={`${ov.max_drawdown}$`} color="text-amber-400" />
        </div>
      )}

      {s?.executive_summary && (
        <div className="card p-5 border border-brand-600/30">
          <h2 className="text-sm font-semibold text-brand-300 mb-2 uppercase tracking-wider">Sintesi</h2>
          <p className="text-sm text-slate-200 whitespace-pre-wrap leading-relaxed">{s.executive_summary}</p>
        </div>
      )}

      <Section icon={Brain} title="Strategia del trader (contesto ICT)" items={s?.strategia_trader} color="text-brand-300" />
      <Section icon={TrendingUp} title="Edge del trader" items={s?.trader_edge} color="text-emerald-300" />
      <Section icon={AlertTriangle} title="Buchi di esecuzione" items={s?.execution_gaps} color="text-rose-300" />
      <Section icon={Shield} title="Profilo di rischio" items={s?.risk_profile} color="text-amber-300" />
      <Section icon={Search} title="Pattern rilevati" items={s?.patterns} color="text-sky-300" />

      {s?.recommendations?.length > 0 && (
        <div className="card p-5 border border-emerald-600/30">
          <h2 className="text-sm font-semibold text-emerald-300 mb-3 uppercase tracking-wider flex items-center gap-2">
            <Lightbulb size={15} /> Raccomandazioni
          </h2>
          <div className="space-y-3">
            {s.recommendations.filter(r => !r.user_action).map((r, i) => (
              <div key={i} className="bg-slate-800/60 rounded-lg p-3">
                <div className="flex items-center gap-2 mb-1 flex-wrap">
                  <span className={`text-[10px] px-1.5 py-0.5 rounded font-semibold uppercase ${PRIORITY_STYLE[r.priority] || PRIORITY_STYLE.bassa}`}>
                    {r.priority}
                  </span>
                  <span className="text-sm font-semibold text-white">{r.title}</span>
                  {r.sim_type && r.sim_type !== 'none' && (
                    <span className="text-[10px] px-1.5 py-0.5 rounded bg-violet-600/25 text-violet-300">simulata</span>
                  )}
                </div>
                <p className="text-sm text-slate-300">{r.detail}</p>
                <ImpactBox impact={r.impact} />
                <div className="mt-2 flex gap-2 flex-wrap">
                  <button onClick={() => recAction(r, 'rejected')}
                    className="px-3 py-1.5 text-xs bg-slate-700 hover:bg-slate-600 text-slate-300 rounded-lg">
                    ✋ Rifiuta
                  </button>
                  {r.sim_type && r.sim_type !== 'none' && (
                    <>
                      <button onClick={() => recAction(r, 'test')}
                        className="px-3 py-1.5 text-xs bg-violet-600 hover:bg-violet-500 text-white rounded-lg font-medium">
                        🧪 Monitora (test)
                      </button>
                      <button onClick={() => recAction(r, 'real')}
                        className="px-3 py-1.5 text-xs bg-emerald-600 hover:bg-emerald-500 text-white rounded-lg font-medium">
                        ⚡ Approva
                      </button>
                    </>
                  )}
                </div>
              </div>
            ))}
          </div>
        </div>
      )}

      {/* MONITOR TEST — nessun effetto reale, statistiche virtuali live */}
      <div className="card p-5 border border-violet-600/30">
        <h2 className="text-sm font-semibold text-violet-300 mb-1 uppercase tracking-wider flex items-center gap-2">
          <Activity size={15} /> Monitor Test
        </h2>
        <p className="text-xs text-slate-500 mb-3">
          Regole in osservazione: NESSUN effetto sui trade reali. Statistiche live di cosa
          sarebbe successo se la regola fosse attiva. Da qui: Approva o Rollback.
        </p>
        {rules.test.length === 0
          ? <p className="text-xs text-slate-600">Nessuna regola in test.</p>
          : <div className="space-y-2">{rules.test.map(r =>
              <MonitorCard key={r.id} rule={r} onPromote={promoteRule} onRollback={rollbackRule} />)}</div>}
      </div>

      {/* MONITOR REALE — la regola agisce davvero */}
      <div className="card p-5 border border-emerald-600/30">
        <h2 className="text-sm font-semibold text-emerald-300 mb-1 uppercase tracking-wider flex items-center gap-2">
          <Zap size={15} /> Monitor Reale
        </h2>
        <p className="text-xs text-slate-500 mb-3">
          Regole APPROVATE che agiscono sui trade futuri (i segnali bloccati diventano paper
          trade: vedi quanto il blocco rende o costa). Da qui: Rollback.
        </p>
        {rules.real.length === 0
          ? <p className="text-xs text-slate-600">Nessuna regola attiva.</p>
          : <div className="space-y-2">{rules.real.map(r =>
              <MonitorCard key={r.id} rule={r} onPromote={promoteRule} onRollback={rollbackRule} />)}</div>}
      </div>

      {s?.scartate_dalla_verifica?.length > 0 && (
        <div className="card p-5 border border-amber-700/30">
          <h2 className="text-sm font-semibold text-amber-300 mb-3 uppercase tracking-wider flex items-center gap-2">
            <AlertTriangle size={15} /> Proposte scartate dalla verifica statistica
          </h2>
          <p className="text-xs text-slate-500 mb-3">
            Idee con numeri apparentemente buoni ma bocciate dai gate (campione insufficiente,
            delta concentrato su pochi trade, o confidenza bootstrap bassa). Mostrate per trasparenza: NON eseguirle.
          </p>
          <div className="space-y-2">
            {s.scartate_dalla_verifica.map((r, i) => (
              <div key={i} className="bg-slate-800/40 rounded-lg p-3 opacity-80">
                <p className="text-sm text-slate-300 font-semibold line-through decoration-amber-600/60">{r.title}</p>
                <p className="text-xs text-amber-400 mt-0.5">✗ {r.demotion_reason}</p>
                <ImpactBox impact={r.impact} />
              </div>
            ))}
          </div>
        </div>
      )}

      {s?.confidence_note && (
        <p className="text-xs text-slate-500 italic">{s.confidence_note}</p>
      )}

      <Simulator />

      {ex && (
        <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
          <KPI label="Slippage medio oltre range" value={`${ex.avg_slippage_beyond_range}$`}
            sub={`${ex.fills_beyond_range} fill fuori range`} />
          <KPI label="Latenza fill media" value={ex.avg_fill_latency_s != null ? `${ex.avg_fill_latency_s}s` : '—'} />
          <KPI label="Durata media trade" value={ex.avg_hold_minutes != null ? `${ex.avg_hold_minutes} min` : '—'} />
          <KPI label="Violazioni max-risk" value={ex.max_risk_violations?.length ?? 0}
            color={(ex.max_risk_violations?.length ?? 0) > 0 ? 'text-rose-400' : 'text-emerald-400'} />
        </div>
      )}
    </div>
  )
}
