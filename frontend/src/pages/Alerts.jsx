import { useEffect, useState } from 'react'
import { api } from '../api'
import { formatDistanceToNow } from 'date-fns'
import { it } from 'date-fns/locale'

const STATUS_COLORS = {
  pending: 'bg-yellow-900/40 text-yellow-400 border-yellow-700',
  open:    'bg-blue-900/40 text-blue-400 border-blue-700',
  tp1:     'bg-emerald-900/40 text-emerald-400 border-emerald-700',
  tp2:     'bg-emerald-900/60 text-emerald-300 border-emerald-600',
  tp3:     'bg-emerald-800/60 text-emerald-200 border-emerald-500',
  closed:  'bg-slate-700/40 text-slate-400 border-slate-600',
  sl_hit:  'bg-rose-900/40 text-rose-400 border-rose-700',
}

const STATUS_LABELS = {
  pending: 'In attesa', open: 'Aperto', tp1: 'TP1', tp2: 'TP2',
  tp3: 'TP3', closed: 'Chiuso', sl_hit: 'SL Hit', cancelled: 'Annullato',
}

export default function Alerts() {
  const [signals, setSignals] = useState([])
  const [filter, setFilter] = useState('all')
  const [symbol, setSymbol] = useState('')

  const load = async () => {
    const params = {}
    if (filter !== 'all') params.status = filter
    if (symbol) params.symbol = symbol
    const data = await api.getSignals({ ...params, limit: 100 })
    setSignals(data)
  }

  useEffect(() => { load() }, [filter, symbol])

  const FILTERS = ['all', 'pending', 'open', 'tp1', 'tp2', 'tp3', 'sl_hit', 'closed']

  return (
    <div className="p-6 space-y-5">
      <h1 className="text-xl font-bold text-white">Alert & Segnali</h1>

      {/* Filtri */}
      <div className="flex flex-wrap gap-2 items-center">
        <div className="flex gap-1">
          {FILTERS.map(f => (
            <button
              key={f}
              onClick={() => setFilter(f)}
              className={`text-xs px-3 py-1 rounded-full border transition-colors ${
                filter === f
                  ? 'bg-brand-600 border-brand-500 text-white'
                  : 'border-slate-700 text-slate-400 hover:text-slate-200'
              }`}
            >
              {STATUS_LABELS[f] || 'Tutti'}
            </button>
          ))}
        </div>
        <input
          value={symbol}
          onChange={e => setSymbol(e.target.value.toUpperCase())}
          placeholder="Filtra simbolo..."
          className="ml-auto bg-slate-800 border border-slate-700 rounded px-3 py-1 text-xs text-slate-300 placeholder-slate-600 focus:outline-none focus:border-brand-500"
        />
      </div>

      {/* Lista */}
      <div className="space-y-3">
        {signals.length === 0 && (
          <p className="text-slate-500 text-sm">Nessun segnale trovato</p>
        )}
        {signals.map(sig => (
          <div key={sig.id} className="card flex items-start gap-4">
            <div className="flex-shrink-0 pt-1">
              <span className={sig.direction === 'buy' ? 'badge-buy' : 'badge-sell'}>
                {sig.direction === 'buy' ? '▲ BUY' : '▼ SELL'}
              </span>
            </div>
            <div className="flex-1 min-w-0">
              <div className="flex items-center gap-2 mb-1">
                <span className="font-bold text-white">{sig.symbol}</span>
                <span className={`badge-status border text-xs ${STATUS_COLORS[sig.status] || ''}`}>
                  {STATUS_LABELS[sig.status] || sig.status}
                </span>
              </div>
              <div className="flex gap-4 text-xs text-slate-400 font-mono">
                <span>Entry: <span className="text-slate-200">{sig.entry_price ?? '?'}{sig.entry_price_high ? `–${sig.entry_price_high}` : ''}</span></span>
                <span>SL: <span className="text-rose-400">{sig.stoploss ?? '—'}</span></span>
                <span>TP1: <span className="text-emerald-400">{sig.tp1 ?? '—'}</span></span>
                <span>TP2: <span className="text-emerald-300">{sig.tp2 ?? '—'}</span></span>
                <span>TP3: <span className="text-emerald-200">{sig.tp3 ?? '—'}</span></span>
              </div>
              {sig.notes && <p className="text-xs text-slate-500 mt-1 italic">{sig.notes}</p>}
            </div>
            <div className="flex-shrink-0 text-xs text-slate-600 text-right">
              {sig.created_at
                ? formatDistanceToNow(new Date(sig.created_at), { addSuffix: true, locale: it })
                : ''}
            </div>
          </div>
        ))}
      </div>
    </div>
  )
}
