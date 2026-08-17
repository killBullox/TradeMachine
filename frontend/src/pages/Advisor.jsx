import { useEffect, useState } from 'react'
import toast from 'react-hot-toast'
import { Brain, TrendingUp, AlertTriangle, Shield, Search, Lightbulb } from 'lucide-react'

const PRIORITY_STYLE = {
  alta:  'bg-rose-600/25 text-rose-300',
  media: 'bg-amber-600/25 text-amber-300',
  bassa: 'bg-slate-600/40 text-slate-300',
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

export default function Advisor() {
  const [report, setReport] = useState(null)
  const [loading, setLoading] = useState(true)
  const [generating, setGenerating] = useState(false)

  const load = async () => {
    try {
      const d = await fetch('/api/advisor').then(r => r.json())
      setReport(d.report)
    } catch { toast.error('Errore caricamento advisor') }
    finally { setLoading(false) }
  }
  useEffect(() => { load() }, [])

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
            {s.recommendations.map((r, i) => (
              <div key={i} className="bg-slate-800/60 rounded-lg p-3">
                <div className="flex items-center gap-2 mb-1">
                  <span className={`text-[10px] px-1.5 py-0.5 rounded font-semibold uppercase ${PRIORITY_STYLE[r.priority] || PRIORITY_STYLE.bassa}`}>
                    {r.priority}
                  </span>
                  <span className="text-sm font-semibold text-white">{r.title}</span>
                </div>
                <p className="text-sm text-slate-300">{r.detail}</p>
              </div>
            ))}
          </div>
        </div>
      )}

      {s?.confidence_note && (
        <p className="text-xs text-slate-500 italic">{s.confidence_note}</p>
      )}

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
