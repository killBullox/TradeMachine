import { useEffect, useState } from 'react'
import { api } from '../api'
import { TrendingUp, TrendingDown, Target, ShieldAlert, Activity, DollarSign, Calendar, BarChart3 } from 'lucide-react'
import TradeCard from '../components/TradeCard'

function StatCard({ label, value, icon: Icon, color }) {
  return (
    <div className="card flex items-center gap-4">
      <div className={`p-3 rounded-lg ${color}`}>
        <Icon size={20} />
      </div>
      <div>
        <p className="text-slate-400 text-xs">{label}</p>
        <p className="text-2xl font-bold">{value ?? '—'}</p>
      </div>
    </div>
  )
}

function MT5Panel() {
  const [mt5, setMt5] = useState(null)
  const [toggling, setToggling] = useState(false)

  const load = () => fetch('/api/mt5/status').then(r => r.json()).then(setMt5).catch(() => {})
  useEffect(() => { load(); const t = setInterval(load, 15000); return () => clearInterval(t) }, [])

  if (!mt5) return null

  const toggle = async () => {
    setToggling(true)
    const url = mt5.auto_trade ? '/api/mt5/disable' : '/api/mt5/enable'
    await fetch(url, { method: 'POST' })
    await load()
    setToggling(false)
  }

  const acc = mt5.account || {}
  const positions = mt5.open_positions || []

  return (
    <div className={`card border ${mt5.auto_trade ? 'border-emerald-700/50 bg-emerald-950/20' : 'border-slate-700'}`}>
      <div className="flex items-center justify-between">
        <div className="flex items-center gap-3">
          <div className={`w-2.5 h-2.5 rounded-full ${mt5.auto_trade ? 'bg-emerald-400 animate-pulse' : 'bg-slate-600'}`} />
          <div>
            <span className="text-sm font-semibold text-white">MT5 Auto-Trade</span>
            {acc.name && (
              <span className="ml-2 text-xs text-slate-500">{acc.name} · {acc.server}</span>
            )}
          </div>
          {acc.demo && <span className="text-xs px-1.5 py-0.5 bg-amber-900/50 text-amber-400 rounded">DEMO</span>}
        </div>
        <div className="flex items-center gap-4">
          {acc.balance != null && (
            <div className="text-right">
              <div className="text-xs text-slate-500">Balance</div>
              <div className="text-sm font-mono text-white">${acc.balance?.toLocaleString('it-IT', {minimumFractionDigits: 2})}</div>
            </div>
          )}
          {acc.profit != null && (
            <div className="text-right">
              <div className="text-xs text-slate-500">P&L open</div>
              <div className={`text-sm font-mono font-semibold ${acc.profit >= 0 ? 'text-emerald-400' : 'text-rose-400'}`}>
                {acc.profit >= 0 ? '+' : ''}{acc.profit?.toFixed(2)}$
              </div>
            </div>
          )}
          <button onClick={toggle} disabled={toggling}
            className={`px-4 py-1.5 text-xs font-semibold rounded transition-colors ${
              mt5.auto_trade
                ? 'bg-rose-700 hover:bg-rose-600 text-white'
                : 'bg-emerald-700 hover:bg-emerald-600 text-white'
            } disabled:opacity-50`}>
            {toggling ? '...' : mt5.auto_trade ? 'DISATTIVA' : 'ATTIVA'}
          </button>
        </div>
      </div>
      {positions.length > 0 && (
        <div className="mt-3 pt-3 border-t border-slate-700/50 space-y-1">
          {positions.map(p => (
            <div key={p.ticket} className="flex items-center justify-between text-xs">
              <span className="text-slate-400">{p.symbol} {p.type.toUpperCase()} {p.volume}lot @ {p.price_open}</span>
              <span className={`font-mono font-semibold ${p.profit >= 0 ? 'text-emerald-400' : 'text-rose-400'}`}>
                {p.profit >= 0 ? '+' : ''}{p.profit?.toFixed(2)}$
              </span>
            </div>
          ))}
        </div>
      )}
    </div>
  )
}

export default function Dashboard({ wsEvents }) {
  const [signals, setSignals] = useState([])
  const [perf, setPerf] = useState(null)
  const [positions, setPositions] = useState([])
  const [prices, setPrices] = useState({})
  const [loading, setLoading] = useState(true)

  const load = async () => {
    const [sigs, p, mt5] = await Promise.all([
      api.getSignals({ limit: 20 }),
      api.getPerformance(),
      fetch('/api/mt5/status').then(r => r.json()).catch(() => ({})),
    ])
    setSignals(sigs)
    setPerf(p)
    setPositions(mt5.open_positions ?? [])
    setLoading(false)

    // Polling prezzi per i simboli con segnali attivi
    const active = sigs.filter(s => ['pending', 'open', 'tp1', 'tp2'].includes(s.status) && !s.closed_at)
    const syms = [...new Set(active.map(s => s.symbol))]
    const priceMap = {}
    await Promise.all(syms.map(async sym => {
      try {
        const r = await fetch(`/api/price/${sym}`).then(r => r.json())
        if (r.price) priceMap[sym] = r.price
      } catch {}
    }))
    setPrices(priceMap)
  }

  useEffect(() => {
    load()
    // Auto-refresh ogni 15s per tenere sincronizzati prezzi e P&L live
    const t = setInterval(load, 15000)
    return () => clearInterval(t)
  }, [])

  // Ricarica quando arriva un evento WS
  useEffect(() => {
    if (wsEvents.length > 0) load()
  }, [wsEvents])

  const active = signals.filter(s => ['pending', 'open', 'tp1', 'tp2'].includes(s.status) && !s.closed_at)

  return (
    <div className="p-6 space-y-6">
      <div className="flex items-center justify-between">
        <h1 className="text-xl font-bold text-white">Dashboard</h1>
        <button onClick={load} className="text-xs text-slate-400 hover:text-white transition-colors">
          ↻ Aggiorna
        </button>
      </div>

      <MT5Panel />

      {/* Stats */}
      {perf && (
        <div className="grid grid-cols-2 md:grid-cols-3 xl:grid-cols-6 gap-4">
          <StatCard
            label="Segnali totali"
            value={<>{perf.total_signals ?? '—'} <span className="text-slate-500 text-lg font-normal">({perf.managed_signals ?? 0})</span></>}
            icon={Activity}
            color="bg-brand-900/40 text-brand-400"
          />
          <StatCard label="Win rate" value={perf.win_rate_pct != null ? `${perf.win_rate_pct}%` : '—'} icon={Target} color="bg-emerald-900/40 text-emerald-400" />
          <StatCard label="SL hit" value={perf.sl_hits} icon={ShieldAlert} color="bg-rose-900/40 text-rose-400" />
          <StatCard label="Ultimi 7gg" value={perf.signals_last_7d} icon={TrendingUp} color="bg-violet-900/40 text-violet-400" />
          <StatCard
            label="P&L Oggi"
            value={<span className={perf.today_pnl >= 0 ? 'text-emerald-400' : 'text-rose-400'}>{perf.today_pnl >= 0 ? '+' : ''}{perf.today_pnl?.toFixed(2)}$</span>}
            icon={Calendar}
            color={perf.today_pnl >= 0 ? 'bg-emerald-900/40 text-emerald-400' : 'bg-rose-900/40 text-rose-400'}
          />
          <StatCard
            label="P&L Totale"
            value={<span className={perf.total_pnl_usd >= 0 ? 'text-emerald-400' : 'text-rose-400'}>{perf.total_pnl_usd >= 0 ? '+' : ''}{perf.total_pnl_usd?.toFixed(2)}$</span>}
            icon={DollarSign}
            color={perf.total_pnl_usd >= 0 ? 'bg-emerald-900/40 text-emerald-400' : 'bg-rose-900/40 text-rose-400'}
          />
        </div>
      )}

      {/* Segnali attivi */}
      <div>
        <h2 className="text-sm font-semibold text-slate-400 mb-3 uppercase tracking-wider">
          Segnali attivi ({active.length})
        </h2>
        {loading ? (
          <p className="text-slate-500 text-sm">Caricamento...</p>
        ) : active.length === 0 ? (
          <p className="text-slate-500 text-sm">Nessun segnale attivo</p>
        ) : (
          <div className="grid grid-cols-1 md:grid-cols-2 xl:grid-cols-3 gap-4">
            {active.map(s => (
              <TradeCard
                key={s.id}
                sig={s}
                positions={positions}
                currentPrice={prices[s.symbol]}
                onClose={load}
              />
            ))}
          </div>
        )}
      </div>

      {/* Ultimi eventi WS */}
      {wsEvents.length > 0 && (
        <div>
          <h2 className="text-sm font-semibold text-slate-400 mb-3 uppercase tracking-wider">
            Feed live
          </h2>
          <div className="card space-y-2 max-h-48 overflow-auto">
            {wsEvents.slice(0, 10).map((e, i) => (
              <div key={i} className="text-xs text-slate-400 flex gap-2">
                <span className={`font-mono ${
                  e.event === 'new_signal' ? 'text-emerald-400' :
                  e.event === 'trade_update' ? 'text-blue-400' : 'text-slate-500'
                }`}>
                  [{e.event}]
                </span>
                <span>{JSON.stringify(e.data).slice(0, 80)}...</span>
              </div>
            ))}
          </div>
        </div>
      )}
    </div>
  )
}
