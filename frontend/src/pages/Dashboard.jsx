import { useEffect, useState } from 'react'
import { api } from '../api'
import { formatDistanceToNow } from 'date-fns'
import { it } from 'date-fns/locale'
import { TrendingUp, TrendingDown, Target, ShieldAlert, Activity, DollarSign, Calendar, BarChart3 } from 'lucide-react'
import toast from 'react-hot-toast'

const STATUS_COLORS = {
  pending:   'bg-yellow-900/40 text-yellow-400 border-yellow-700',
  open:      'bg-blue-900/40 text-blue-400 border-blue-700',
  tp1:       'bg-emerald-900/40 text-emerald-400 border-emerald-700',
  tp2:       'bg-emerald-900/60 text-emerald-300 border-emerald-600',
  tp3:       'bg-emerald-800/60 text-emerald-200 border-emerald-500',
  closed:    'bg-slate-700/40 text-slate-400 border-slate-600',
  sl_hit:    'bg-rose-900/40 text-rose-400 border-rose-700',
  cancelled: 'bg-slate-800/40 text-slate-500 border-slate-700',
  cancelled_timing: 'bg-amber-900/20 text-amber-600 border-amber-800',
}

const STATUS_LABELS = {
  pending: 'In attesa', open: 'Aperto', tp1: 'TP1 ✓', tp2: 'TP2 ✓',
  tp3: 'TP3 ✓', closed: 'Chiuso', sl_hit: 'SL Hit', cancelled: 'Annullato', cancelled_timing: 'Timing mancato',
}

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

function SignalCard({ signal, onStatusChange }) {
  const [updating, setUpdating] = useState(false)
  const [closing, setClosing] = useState(false)
  const [notes, setNotes] = useState(signal.notes || '')
  const [price, setPrice] = useState(null)

  // Polling prezzo attuale ogni 10s
  useEffect(() => {
    const fetchPrice = () =>
      fetch(`/api/price/${signal.symbol}`).then(r => r.json()).then(d => setPrice(d.price)).catch(() => {})
    fetchPrice()
    const t = setInterval(fetchPrice, 10000)
    return () => clearInterval(t)
  }, [signal.symbol])

  const handleStatus = async (status) => {
    setUpdating(true)
    try {
      await api.updateSignal(signal.id, { status })
      onStatusChange()
    } finally {
      setUpdating(false)
    }
  }

  const handleCancel = async () => {
    if (!confirm(`Annullare il segnale #${signal.id} ${signal.symbol}?`)) return
    await handleStatus('cancelled')
  }

  const handleCloseTrade = async () => {
    if (!confirm(`Chiudere il trade MT5 #${signal.id} ${signal.symbol}?`)) return
    setClosing(true)
    try {
      const r = await fetch(`/api/mt5/close_signal/${signal.id}`, { method: 'POST' }).then(r => r.json())
      if (r.ok) {
        toast.success(`Trade #${signal.id} chiuso`)
        onStatusChange()
      } else {
        // Prova a sincronizzare con MT5 — le posizioni potrebbero essere già chiuse
        await fetch('/api/mt5/sync', { method: 'POST' })
        const updated = await fetch(`/api/signals/${signal.id}`).then(r => r.json())
        if (!['open','pending','tp1','tp2'].includes(updated.status)) {
          toast.success(`Trade #${signal.id} già chiuso su MT5 — stato aggiornato`)
          onStatusChange()
        } else {
          toast.error('Errore nella chiusura MT5')
        }
      }
    } catch {
      toast.error('Errore di rete')
    } finally {
      setClosing(false)
    }
  }

  const saveNotes = async () => {
    await api.updateSignal(signal.id, { notes })
    onStatusChange()
  }

  const isBuy = signal.direction === 'buy'
  const hasMt5 = signal.mt5_ticket || signal.mt5_tickets

  return (
    <div className="card space-y-3">
      <div className="flex items-start justify-between">
        <div className="flex items-center gap-2">
          <span className={isBuy ? 'badge-buy' : 'badge-sell'}>
            {isBuy ? '▲ BUY' : '▼ SELL'}
          </span>
          <span className="font-bold text-white">{signal.symbol}</span>
        </div>
        <span className={`badge-status border ${STATUS_COLORS[signal.status] || ''}`}>
          {STATUS_LABELS[signal.status] || signal.status}
        </span>
      </div>

      {/* Prezzo attuale + P&L */}
      {price != null && (() => {
        const entry = signal.actual_entry_price || signal.entry_price
        const inProfit = entry
          ? (isBuy ? price > entry : price < entry)
          : null
        const digits = price > 1000 ? 2 : 5
        // P&L: usa pnl_usd dal backend (MT5 reale) oppure stima dal prezzo
        const pnl = signal.pnl_usd
        return (
          <div className="flex items-center justify-between bg-slate-800/50 rounded px-3 py-1.5">
            <div>
              <span className="text-xs text-slate-500">Prezzo attuale</span>
              <span className={`ml-2 font-mono font-bold text-lg ${
                inProfit === null ? 'text-slate-300' : inProfit ? 'text-emerald-400' : 'text-rose-400'
              }`}>
                {price.toFixed(digits)}
              </span>
            </div>
            {pnl != null && (
              <div className="text-right">
                <span className="text-xs text-slate-500">P&L</span>
                <p className={`font-mono font-bold text-lg ${pnl >= 0 ? 'text-emerald-400' : 'text-rose-400'}`}>
                  {pnl >= 0 ? '+' : ''}{pnl.toFixed(2)}$
                </p>
              </div>
            )}
          </div>
        )
      })()}

      <div className="grid grid-cols-2 gap-2 text-sm">
        <div>
          <span className="text-slate-500">Entry</span>
          <p className="font-mono text-slate-200">
            {signal.entry_price ?? '?'}
            {signal.entry_price_high ? ` – ${signal.entry_price_high}` : ''}
          </p>
        </div>
        <div>
          <span className="text-slate-500">Stop Loss</span>
          <p className="font-mono text-rose-400">{signal.stoploss ?? '—'}</p>
        </div>
        <div>
          <span className="text-slate-500">TP1</span>
          <p className="font-mono text-emerald-400">{signal.tp1 ?? '—'}</p>
        </div>
        <div>
          <span className="text-slate-500">TP2 / TP3</span>
          <p className="font-mono text-emerald-300">
            {signal.tp2 ?? '—'} / {signal.tp3 ?? '—'}
          </p>
        </div>
      </div>

      {/* Aggiorna stato */}
      <div className="flex flex-wrap gap-1">
        {['open', 'tp1', 'tp2', 'tp3', 'closed', 'sl_hit'].map(s => (
          <button
            key={s}
            onClick={() => handleStatus(s)}
            disabled={updating || signal.status === s}
            className={`text-xs px-2 py-1 rounded border transition-colors ${
              signal.status === s
                ? 'opacity-50 cursor-default border-slate-700 text-slate-500'
                : 'border-slate-700 text-slate-300 hover:border-slate-500 hover:text-white'
            }`}
          >
            {STATUS_LABELS[s]}
          </button>
        ))}
      </div>

      {/* Azioni MT5 / Annulla */}
      <div className="flex gap-2">
        {hasMt5 ? (
          <button
            onClick={handleCloseTrade}
            disabled={closing}
            className="flex-1 text-xs px-2 py-1.5 rounded bg-rose-900/40 text-rose-400 hover:bg-rose-900/60 hover:text-rose-300 transition-colors disabled:opacity-50"
          >
            {closing ? 'Chiusura...' : 'Chiudi trade MT5'}
          </button>
        ) : (
          <button
            onClick={handleCancel}
            disabled={updating}
            className="flex-1 text-xs px-2 py-1.5 rounded bg-slate-800 text-slate-400 hover:bg-slate-700 hover:text-slate-200 transition-colors disabled:opacity-50"
          >
            Annulla segnale
          </button>
        )}
      </div>

      {/* Note */}
      <div className="flex gap-2">
        <input
          value={notes}
          onChange={e => setNotes(e.target.value)}
          placeholder="Note rapide..."
          className="flex-1 bg-slate-800 border border-slate-700 rounded px-2 py-1 text-xs text-slate-300 placeholder-slate-600 focus:outline-none focus:border-brand-500"
        />
        <button
          onClick={saveNotes}
          className="text-xs px-2 py-1 bg-brand-700 hover:bg-brand-600 rounded transition-colors"
        >
          Salva
        </button>
      </div>

      <p className="text-xs text-slate-600">
        {signal.created_at
          ? formatDistanceToNow(new Date(signal.created_at), { addSuffix: true, locale: it })
          : ''}
      </p>
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
  const [loading, setLoading] = useState(true)

  const load = async () => {
    const [sigs, p] = await Promise.all([
      api.getSignals({ limit: 20 }),
      api.getPerformance(),
    ])
    setSignals(sigs)
    setPerf(p)
    setLoading(false)
  }

  useEffect(() => { load() }, [])

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
          <StatCard label="Segnali totali" value={perf.total_signals} icon={Activity} color="bg-brand-900/40 text-brand-400" />
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
              <SignalCard key={s.id} signal={s} onStatusChange={load} />
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
