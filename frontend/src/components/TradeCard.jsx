import { useState } from 'react'
import { format } from 'date-fns'
import { it } from 'date-fns/locale'
import toast from 'react-hot-toast'
import TradeProgress from './TradeProgress'

function fmtTs(ts) {
  if (!ts) return '—'
  try { return format(new Date(ts), 'dd/MM HH:mm:ss', { locale: it }) } catch { return ts }
}

function pnlColor(v) {
  if (v == null) return 'text-slate-400'
  return v > 0 ? 'text-emerald-400' : v < 0 ? 'text-rose-400' : 'text-slate-400'
}

function fmtPnl(v) {
  if (v == null) return '—'
  return (v >= 0 ? '+' : '') + v.toFixed(2) + '$'
}

function Row({ label, value, mono, cls, extra }) {
  return (
    <div className="flex justify-between items-baseline">
      <span className="text-[15px] text-slate-500">{label}</span>
      <span className={`text-[15px] ${mono ? 'font-mono' : ''} ${cls || 'text-slate-200'}`}>
        {value}{extra && <span className="text-[15px] text-slate-500 ml-1">{extra}</span>}
      </span>
    </div>
  )
}

/**
 * Card completa di un trade aperto/attivo. Usata in OpenTrades e Dashboard.
 * Props:
 *   sig:        signal object dal backend
 *   positions:  array di posizioni MT5 aperte (da /api/mt5/status)
 *   currentPrice: numero o null (se preso esternamente). Se omesso, la card non
 *                 fa polling autonomo — gli usi devono passare il prezzo.
 *   onClose:    callback dopo chiusura riuscita
 */
export default function TradeCard({ sig, positions, currentPrice, onClose }) {
  const isBuy = sig.direction === 'buy'
  const [closing, setClosing] = useState(false)

  let tickets = []
  try { tickets = sig.mt5_tickets ? JSON.parse(sig.mt5_tickets) : (sig.mt5_ticket ? [sig.mt5_ticket] : []) } catch {}
  const openPos = tickets.map(t => (positions || []).find(p => p.ticket === t)).filter(Boolean)

  const livePnl = openPos.length > 0 ? openPos.reduce((s, p) => s + (p.profit ?? 0), 0) : null
  const displayPnl = livePnl ?? sig.pnl_usd
  const lots = openPos.reduce((s, p) => s + (p.volume ?? 0), 0)

  const entry = sig.actual_entry_price ?? sig.entry_price
  const slDist = currentPrice && sig.stoploss
    ? Math.abs(currentPrice - sig.stoploss).toFixed(isBuy ? 0 : 5)
    : null

  const decimals = sig.symbol?.includes('BTC') ? 0 : sig.symbol?.includes('JPY') ? 3 : 5
  const fmtPrice = (v) => v != null ? Number(v).toFixed(decimals) : '—'

  const handleClose = async () => {
    if (!confirm(`Chiudere tutte le posizioni del trade #${sig.id} ${sig.symbol}?`)) return
    setClosing(true)
    try {
      const r = await fetch(`/api/mt5/close_signal/${sig.id}`, { method: 'POST' }).then(r => r.json())
      if (r.ok) {
        toast.success(`Trade #${sig.id} chiuso`)
        onClose?.()
      } else {
        await fetch('/api/mt5/sync', { method: 'POST' })
        const updated = await fetch(`/api/signals/${sig.id}`).then(r => r.json())
        if (!['open','pending','tp1','tp2'].includes(updated.status)) {
          toast.success(`Trade #${sig.id} già chiuso su MT5 — stato aggiornato`)
          onClose?.()
        } else {
          toast.error('Errore nella chiusura')
        }
      }
    } catch {
      toast.error('Errore di rete')
    } finally {
      setClosing(false)
    }
  }

  return (
    <div className="card p-5 space-y-4">
      {/* Header */}
      <div className="flex items-center justify-between">
        <div className="flex items-center gap-3">
          <span className="text-lg font-bold text-white">{sig.symbol}</span>
          <span className={`text-xs font-semibold px-2 py-0.5 rounded ${isBuy ? 'bg-emerald-900/50 text-emerald-400' : 'bg-rose-900/50 text-rose-400'}`}>
            {isBuy ? '▲ BUY' : '▼ SELL'}
          </span>
          {sig.is_risky && <span className="text-xs text-amber-400 px-2 py-0.5 bg-amber-900/30 rounded">⚠ Risky</span>}
          <span className="text-xs text-slate-500">#{sig.id}</span>
        </div>
        <span className={`text-xl font-mono font-bold ${pnlColor(displayPnl)}`}>
          {fmtPnl(displayPnl)}
          {livePnl != null && <span className="text-xs font-normal text-slate-500 ml-1">live</span>}
        </span>
      </div>

      {/* Prezzo attuale prominente */}
      {currentPrice != null && (
        <div className="bg-slate-800/50 rounded-lg px-4 py-2 flex items-center justify-between">
          <span className="text-xs text-slate-400">Prezzo attuale</span>
          <span className={`text-lg font-mono font-bold ${
            entry
              ? (isBuy
                  ? (currentPrice > entry ? 'text-emerald-400' : 'text-rose-400')
                  : (currentPrice < entry ? 'text-emerald-400' : 'text-rose-400'))
              : 'text-white'
          }`}>
            {fmtPrice(currentPrice)}
          </span>
        </div>
      )}

      {/* Griglia dati principali */}
      <div className="grid grid-cols-2 gap-x-8 gap-y-3 text-sm">
        <div className="space-y-3">
          <Row label="Entry time"  value={fmtTs(sig.entered_at ?? sig.created_at)} />
          <Row label="Entry price" value={fmtPrice(entry)} mono />
          <Row label="Lotti aperti" value={lots > 0 ? `${lots.toFixed(2)} lot` : '—'} />
        </div>
        <div className="space-y-3">
          <Row label="Stop Loss"  value={fmtPrice(sig.stoploss)} mono cls="text-rose-400" extra={slDist ? `(${slDist} pts)` : ''} />
          <Row label="TP1" value={fmtPrice(sig.tp1)} mono cls="text-emerald-400" />
          <Row label="TP2" value={fmtPrice(sig.tp2)} mono cls="text-emerald-300" />
          <Row label="TP3" value={fmtPrice(sig.tp3)} mono cls="text-emerald-200" />
        </div>
      </div>

      {/* Barra di progressione (SL/BE/Entry/TP1/TP2/TP3 + price) */}
      {currentPrice != null && <TradeProgress sig={sig} price={currentPrice} />}

      {/* Ticket MT5 */}
      {openPos.length > 0 && (
        <div className="border-t border-slate-700/50 pt-3 space-y-1">
          {openPos.map((p, i) => (
            <div key={p.ticket} className="flex justify-between text-xs text-slate-500">
              <span>Ticket {p.ticket} ({p.comment || `TP${i+1}`})</span>
              <span className={pnlColor(p.profit)}>{fmtPnl(p.profit)}</span>
            </div>
          ))}
        </div>
      )}

      {/* Close button */}
      {tickets.length > 0 && (
        <button
          onClick={handleClose}
          disabled={closing}
          className="w-full mt-1 px-3 py-1.5 rounded-lg text-xs font-medium bg-rose-900/40 text-rose-400 hover:bg-rose-900/70 hover:text-rose-300 transition-colors disabled:opacity-50"
        >
          {closing ? 'Chiusura...' : 'Chiudi trade'}
        </button>
      )}
    </div>
  )
}
