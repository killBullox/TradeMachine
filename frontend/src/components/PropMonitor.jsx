import { useEffect, useState } from 'react'
import { AlertTriangle, Shield, TrendingDown, Layers, Activity } from 'lucide-react'

/**
 * Pannello prop_mode: visualizza in tempo reale le 4 guardie prop.
 *
 * SE prop_mode=False sull'account attivo → ritorna null (componente invisibile).
 *
 * Per Avatrade demo l'API restituisce sempre `enabled: false` e il componente
 * NON viene renderizzato — zero impatto sull'esperienza utente attuale.
 */
export default function PropMonitor() {
  const [data, setData] = useState(null)
  const [loading, setLoading] = useState(true)

  const load = () => {
    fetch('/api/prop/status')
      .then(r => r.json())
      .then(d => { setData(d); setLoading(false) })
      .catch(() => setLoading(false))
  }

  useEffect(() => {
    load()
    const t = setInterval(load, 15000)
    return () => clearInterval(t)
  }, [])

  if (loading || !data || !data.enabled) return null

  const dd = data.daily_dd
  const tr = data.trailing_dd
  const co = data.coerenza
  const mc = data.max_concurrent

  const fmt$ = (v) => v == null ? '—'
    : `${v >= 0 ? '+' : '-'}$${Math.abs(v).toFixed(2)}`

  return (
    <div className="card border border-violet-700 bg-violet-950/20 mb-3">
      <div className="flex items-center gap-2 mb-3">
        <Shield className="text-violet-400" size={18} />
        <span className="text-sm font-semibold text-violet-300">
          PROP MODE — {data.account_label}
        </span>
        {data.current_equity != null && (
          <span className="ml-auto text-xs text-slate-400">
            Equity: <span className="font-mono text-white">${data.current_equity.toFixed(2)}</span>
          </span>
        )}
      </div>

      <div className="grid grid-cols-1 md:grid-cols-4 gap-3">
        {/* Daily DD */}
        {dd && (
          <div className={`p-3 rounded border ${dd.blocked ? 'border-rose-600 bg-rose-950/30' : 'border-slate-700'}`}>
            <div className="flex items-center gap-1.5 mb-1">
              <TrendingDown size={14} className="text-amber-400" />
              <span className="text-xs uppercase text-slate-400 tracking-wide">Daily DD</span>
            </div>
            <p className={`text-lg font-bold ${dd.today_pnl < 0 ? 'text-rose-400' : 'text-emerald-400'}`}>
              {fmt$(dd.today_pnl)}
            </p>
            <p className="text-xs text-slate-500 mt-0.5">
              Limite: -${dd.limit}
            </p>
            {dd.blocked && (
              <div className="mt-2 flex items-start gap-1 text-xs text-rose-400">
                <AlertTriangle size={12} className="mt-0.5 shrink-0" />
                <span>BLOCCATO</span>
              </div>
            )}
          </div>
        )}

        {/* Trailing DD */}
        {tr && (
          <div className={`p-3 rounded border ${tr.breach ? 'border-rose-600 bg-rose-950/30' : tr.warning ? 'border-amber-600 bg-amber-950/20' : 'border-slate-700'}`}>
            <div className="flex items-center gap-1.5 mb-1">
              <Activity size={14} className="text-violet-400" />
              <span className="text-xs uppercase text-slate-400 tracking-wide">Trailing DD</span>
            </div>
            <p className="text-lg font-bold text-white">
              ${tr.remaining_buffer.toFixed(0)}
              <span className="text-xs text-slate-400 font-normal"> buffer</span>
            </p>
            <p className="text-xs text-slate-500 mt-0.5">
              Peak: ${tr.peak.toFixed(0)} · Limite: ${tr.max_total_dd.toFixed(0)}
            </p>
            {tr.breach && (
              <div className="mt-2 flex items-start gap-1 text-xs text-rose-400">
                <AlertTriangle size={12} className="mt-0.5 shrink-0" />
                <span>BREACH! ${tr.distance_from_peak.toFixed(0)} dal peak</span>
              </div>
            )}
            {!tr.breach && tr.warning && (
              <p className="mt-1 text-xs text-amber-400">⚠️ {(tr.distance_from_peak/tr.max_total_dd*100).toFixed(0)}% del limite</p>
            )}
          </div>
        )}

        {/* Coerenza */}
        {co && (
          <div className={`p-3 rounded border ${co.breach ? 'border-rose-600 bg-rose-950/30' : 'border-slate-700'}`}>
            <div className="flex items-center gap-1.5 mb-1">
              <Layers size={14} className="text-sky-400" />
              <span className="text-xs uppercase text-slate-400 tracking-wide">Coerenza</span>
            </div>
            <p className={`text-lg font-bold ${co.breach ? 'text-rose-400' : 'text-white'}`}>
              {co.max_day_pct.toFixed(1)}%
            </p>
            <p className="text-xs text-slate-500 mt-0.5">
              Max-day vs tot · Soglia {co.threshold_pct}%
            </p>
            {co.breach && (
              <p className="mt-2 text-xs text-amber-400">
                Payout safe a ${co.payout_safe_at.toFixed(0)}
              </p>
            )}
          </div>
        )}

        {/* Max concurrent */}
        {mc && (
          <div className={`p-3 rounded border ${mc.block_reason ? 'border-rose-600 bg-rose-950/30' : 'border-slate-700'}`}>
            <div className="flex items-center gap-1.5 mb-1">
              <Layers size={14} className="text-emerald-400" />
              <span className="text-xs uppercase text-slate-400 tracking-wide">Max trade</span>
            </div>
            <p className="text-lg font-bold text-white">
              max {mc.limit}
            </p>
            {mc.block_reason && (
              <div className="mt-2 flex items-start gap-1 text-xs text-rose-400">
                <AlertTriangle size={12} className="mt-0.5 shrink-0" />
                <span>BLOCCO ATTIVO</span>
              </div>
            )}
          </div>
        )}
      </div>
    </div>
  )
}
