import { useEffect, useState } from 'react'
import { format } from 'date-fns'
import toast from 'react-hot-toast'
import TradeCard from '../components/TradeCard'

function pnlColor(v) {
  if (v == null) return 'text-slate-400'
  return v > 0 ? 'text-emerald-400' : v < 0 ? 'text-rose-400' : 'text-slate-400'
}

function fmtPnl(v) {
  if (v == null) return '—'
  return (v >= 0 ? '+' : '') + v.toFixed(2) + '$'
}

export default function OpenTrades() {
  const [signals, setSignals] = useState([])
  const [positions, setPositions] = useState([])
  const [prices, setPrices] = useState({})
  const [lastUpdate, setLastUpdate] = useState(null)
  const [closingAll, setClosingAll] = useState(false)

  const load = async () => {
    try {
      const [sigs, mt5] = await Promise.all([
        fetch('/api/signals?limit=200').then(r => r.json()),
        fetch('/api/mt5/status').then(r => r.json()),
      ])

      // Solo segnali con ticket MT5 effettivo (trade realmente aperti su MT5)
      const open = sigs.filter(s =>
        ['open', 'pending', 'tp1', 'tp2'].includes(s.status) &&
        !s.closed_at &&
        (s.mt5_ticket || s.mt5_tickets)
      )
      setSignals(open)
      setPositions(mt5.open_positions ?? [])

      const syms = [...new Set(open.map(s => s.symbol))]
      const priceMap = {}
      await Promise.all(syms.map(async sym => {
        try {
          const r = await fetch(`/api/price/${sym}`).then(r => r.json())
          if (r.price) priceMap[sym] = r.price
        } catch {}
      }))
      setPrices(priceMap)
      setLastUpdate(new Date())
    } catch {}
  }

  useEffect(() => {
    load()
    const t = setInterval(load, 15000)
    return () => clearInterval(t)
  }, [])

  const handleCloseAll = async () => {
    if (!confirm('Chiudere TUTTE le posizioni MT5 aperte?')) return
    setClosingAll(true)
    try {
      const r = await fetch('/api/mt5/close_all', { method: 'POST' }).then(r => r.json())
      if (r.ok) {
        toast.success(`Chiuse ${r.closed} posizioni`)
        await load()
      } else {
        toast.error('Errore nella chiusura massiva')
      }
    } catch {
      toast.error('Errore di rete')
    } finally {
      setClosingAll(false)
    }
  }

  const livePnl = positions.reduce((s, p) => s + (p.profit ?? 0), 0)
  const hasLive = positions.length > 0

  return (
    <div className="p-6 space-y-4">
      <div className="flex items-center justify-between">
        <h1 className="text-xl font-bold text-white">Trade Aperti</h1>
        <div className="flex items-center gap-4">
          {lastUpdate && (
            <span className="text-xs text-slate-500">
              Aggiornato {format(lastUpdate, 'HH:mm:ss')}
            </span>
          )}
          {hasLive && (
            <span className={`text-sm font-mono font-bold ${pnlColor(livePnl)}`}>
              P&L live: {fmtPnl(livePnl)}
            </span>
          )}
          <button onClick={load} className="text-xs text-slate-400 hover:text-white transition-colors">
            ↻ Aggiorna
          </button>
          {signals.length > 0 && (
            <button
              onClick={handleCloseAll}
              disabled={closingAll}
              className="text-xs px-3 py-1.5 rounded-lg bg-rose-900/40 text-rose-400 hover:bg-rose-900/70 hover:text-rose-300 transition-colors disabled:opacity-50"
            >
              {closingAll ? 'Chiusura...' : 'Close All'}
            </button>
          )}
        </div>
      </div>

      {signals.length === 0 ? (
        <div className="card p-10 text-center text-slate-500">
          Nessun trade aperto al momento
        </div>
      ) : (
        <div className="grid grid-cols-1 lg:grid-cols-2 xl:grid-cols-3 gap-4">
          {signals.map(s => (
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
  )
}
