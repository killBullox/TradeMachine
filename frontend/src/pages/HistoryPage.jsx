import { useEffect, useState } from 'react'
import { api } from '../api'
import { format } from 'date-fns'
import { it } from 'date-fns/locale'

const STATUS_COLORS = {
  pending: 'text-yellow-400', open: 'text-blue-400',
  tp1: 'text-emerald-400', tp2: 'text-emerald-300', tp3: 'text-emerald-200',
  closed: 'text-slate-400', sl_hit: 'text-rose-400', cancelled: 'text-slate-500', cancelled_timing: 'text-amber-600',
}
const STATUS_LABELS = {
  pending: 'In attesa', open: 'Aperto', tp1: 'TP1 ✓', tp2: 'TP2 ✓',
  tp3: 'TP3 ✓', closed: 'Chiuso', sl_hit: 'SL Hit', cancelled: 'Annullato', cancelled_timing: 'Timing mancato',
}
const EVENT_CFG = {
  // Trade events
  entry:   { label: 'Ingresso',   color: 'text-blue-400',    dot: 'bg-blue-400' },
  tp1:     { label: 'TP1',        color: 'text-emerald-400', dot: 'bg-emerald-400' },
  tp2:     { label: 'TP2',        color: 'text-emerald-300', dot: 'bg-emerald-300' },
  tp3:     { label: 'TP3',        color: 'text-emerald-200', dot: 'bg-emerald-200' },
  sl_hit:  { label: 'Stop Loss',  color: 'text-rose-400',    dot: 'bg-rose-400' },
  sl_move:    { label: 'Trail SL →',  color: 'text-amber-400',  dot: 'bg-amber-400' },
  breakeven:  { label: 'Breakeven ⇒', color: 'text-sky-400',    dot: 'bg-sky-400' },
  closed:     { label: 'Chiuso ✕',    color: 'text-slate-400',  dot: 'bg-slate-400' },
  // Process events
  received:        { label: '📥 TG ricevuto',   color: 'text-violet-400', dot: 'bg-violet-400' },
  signal_saved:    { label: '💾 Salvato DB',     color: 'text-slate-400',  dot: 'bg-slate-600' },
  cancelled:       { label: '🚫 Annullato',      color: 'text-slate-500',  dot: 'bg-slate-600' },
  mt5_placing:     { label: '⏳ MT5 invio...',   color: 'text-yellow-400', dot: 'bg-yellow-500' },
  mt5_preparing:   { label: '🔧 MT5 prepara',   color: 'text-yellow-300', dot: 'bg-yellow-400' },
  mt5_placed:      { label: '✅ MT5 ok',         color: 'text-emerald-400', dot: 'bg-emerald-500' },
  mt5_order_sent:  { label: '📤 Ordine inviato', color: 'text-emerald-300', dot: 'bg-emerald-400' },
  mt5_order_failed:{ label: '❌ Ordine fallito', color: 'text-rose-400',   dot: 'bg-rose-500' },
  mt5_failed:      { label: '❌ MT5 fallito',    color: 'text-rose-400',   dot: 'bg-rose-500' },
  mt5_skip:        { label: '⏭ MT5 skip',       color: 'text-amber-500',  dot: 'bg-amber-500' },
  mt5_tp_skip:     { label: '⏭ TP skip',        color: 'text-amber-400',  dot: 'bg-amber-400' },
}

function fmtTs(ts) {
  if (!ts) return '—'
  try {
    // Aggiunge Z se manca il timezone (stringhe UTC dal backend)
    const normalized = ts.endsWith('Z') || ts.includes('+') ? ts : ts + 'Z'
    const d = new Date(normalized)
    // Formatta in ora locale manualmente (date-fns non gestisce timezone)
    const dd = String(d.getDate()).padStart(2, '0')
    const mm = String(d.getMonth() + 1).padStart(2, '0')
    const hh = String(d.getHours()).padStart(2, '0')
    const mi = String(d.getMinutes()).padStart(2, '0')
    const ss = String(d.getSeconds()).padStart(2, '0')
    return `${dd}/${mm} ${hh}:${mi}:${ss}`
  }
  catch { return ts }
}

function pnlColor(v) {
  if (v == null) return 'text-slate-500'
  return v >= 0 ? 'text-emerald-400' : 'text-rose-400'
}
function fmtPnl(v) {
  if (v == null) return '—'
  return (v >= 0 ? '+' : '') + v.toFixed(2) + '$'
}

function TradeLog({ json }) {
  let events = []
  try { events = JSON.parse(json) } catch { return null }
  if (!events.length) return null
  return (
    <div className="flex flex-col gap-1 py-1 pl-1">
      {events.map((ev, i) => {
        const cfg = EVENT_CFG[ev.event] || { label: ev.event, color: 'text-slate-400', dot: 'bg-slate-400' }
        const isProcess = ['received','signal_saved','cancelled','mt5_placing','mt5_preparing','mt5_placed','mt5_order_sent','mt5_order_failed','mt5_failed','mt5_skip','mt5_tp_skip'].includes(ev.event)
        return (
          <div key={i} className={`flex items-start gap-2 text-xs ${isProcess ? 'py-0.5' : ''}`}>
            <span className={`w-2 h-2 rounded-full flex-shrink-0 mt-0.5 ${cfg.dot}`} />
            <span className={`font-medium w-28 flex-shrink-0 ${cfg.color}`}>{cfg.label}</span>
            {isProcess ? (
              <span className="text-slate-400 flex-1">{ev.detail}</span>
            ) : (
              <>
                <span className="font-mono text-slate-300 w-16">{ev.price ?? '—'}</span>
                {ev.pnl != null && (
                  <span className={`font-mono font-semibold ${pnlColor(ev.pnl)}`}>{fmtPnl(ev.pnl)}</span>
                )}
              </>
            )}
            <span className="text-slate-600 whitespace-nowrap ml-auto">{fmtTs(ev.ts)}</span>
          </div>
        )
      })}
    </div>
  )
}

function Mt5Tickets({ mt5_ticket, mt5_tickets }) {
  if (!mt5_ticket) return null
  let tickets = []
  try { tickets = mt5_tickets ? JSON.parse(mt5_tickets) : [mt5_ticket] } catch { tickets = [mt5_ticket] }
  return (
    <div className="flex flex-col gap-1 py-1 pl-1 border-t border-slate-700/50 mt-1">
      <div className="text-xs text-slate-500 mb-1">Ticket MT5</div>
      {tickets.map((t, i) => (
        <div key={t} className="flex items-center gap-2 text-xs">
          <span className="w-2 h-2 rounded-full flex-shrink-0 bg-blue-500" />
          <span className="text-slate-400 font-mono w-20">TP{i + 1}</span>
          <span className="font-mono text-blue-300">{t}</span>
          <span className="text-slate-500 text-xs">IC#{mt5_ticket} · ticket {t}</span>
        </div>
      ))}
    </div>
  )
}

function SignalRow({ s }) {
  const [open, setOpen] = useState(false)
  const hasLog = !!s.trade_log
  const hasDetail = hasLog || !!s.mt5_ticket
  return (
    <>
      <tr
        className={`hover:bg-slate-800/30 transition-colors ${hasDetail ? 'cursor-pointer' : ''}`}
        onClick={() => hasDetail && setOpen(o => !o)}
      >
        <td className="py-2 pr-3 font-medium text-white text-sm">
          {hasDetail
            ? <span className="text-slate-500 mr-1 text-xs">{open ? '▼' : '▶'}</span>
            : s.status !== 'pending' && <span className="text-rose-600 mr-1 text-xs" title="Dati tick non disponibili">✕</span>
          }
          {s.symbol}
          {s.is_risky && <span className="ml-1 text-xs text-amber-400" title="Risky trade — lotto dimezzato">⚠</span>}
        </td>
        <td className="py-2 pr-3">
          <span className={s.direction === 'buy' ? 'badge-buy' : 'badge-sell'}>
            {s.direction === 'buy' ? '▲ B' : '▼ S'}
          </span>
        </td>
        <td className="py-2 pr-3 font-mono text-slate-400 text-xs">
          {s.entry_price ?? '?'}{s.entry_price_high ? `–${s.entry_price_high}` : ''}
        </td>
        <td className="py-2 pr-3 font-mono text-slate-200 text-xs">{s.actual_entry_price ?? '—'}</td>
        <td className="py-2 pr-3 font-mono text-rose-400 text-xs">{s.stoploss ?? '—'}</td>
        <td className="py-2 pr-3 font-mono text-emerald-400 text-xs">{s.tp1 ?? '—'}</td>
        <td className="py-2 pr-3 font-mono text-emerald-300 text-xs">{s.tp2 ?? '—'}</td>
        <td className="py-2 pr-3 font-mono text-emerald-200 text-xs">{s.tp3 ?? '—'}</td>
        <td className="py-2 pr-3">
          <span className={`text-xs ${STATUS_COLORS[s.status] || 'text-slate-400'}`}>
            {STATUS_LABELS[s.status] || s.status}
          </span>
        </td>
        <td className="py-2 pr-3 text-slate-500 text-xs whitespace-nowrap">
          {s.position_size != null ? `${s.position_size} lot` : '—'}
        </td>
        <td className={`py-2 pr-3 font-mono text-xs font-semibold ${pnlColor(s.pnl_usd)}`}>
          {fmtPnl(s.pnl_usd)}
        </td>
        <td className={`py-2 pr-3 font-mono text-xs ${s.running_balance != null ? (s.running_balance >= (s.running_balance - (s.pnl_usd||0)) ? 'text-slate-200' : 'text-slate-400') : 'text-slate-600'}`}>
          {s.running_balance != null ? `$${s.running_balance.toLocaleString('it-IT', {minimumFractionDigits: 2, maximumFractionDigits: 2})}` : '—'}
        </td>
        <td className="py-2 pr-3 text-slate-400 text-xs whitespace-nowrap">
          {s.entered_at ? fmtTs(s.entered_at) : '—'}
        </td>
        <td className="py-2 text-slate-400 text-xs whitespace-nowrap">
          {s.closed_at ? fmtTs(s.closed_at) : '—'}
        </td>
      </tr>
      {open && hasDetail && (
        <tr className="bg-slate-900/60">
          <td colSpan={14} className="px-10 pb-3">
            {s.notes && (
              <div className="mb-2 px-3 py-2 bg-amber-900/20 border border-amber-700/30 rounded text-sm text-amber-300">
                {s.notes}
              </div>
            )}
            {hasLog && <TradeLog json={s.trade_log} />}
            <Mt5Tickets mt5_ticket={s.mt5_ticket} mt5_tickets={s.mt5_tickets} />
          </td>
        </tr>
      )}
    </>
  )
}

function RiskPanel({ onSaved }) {
  const [settings, setSettings] = useState(null)
  const [saving, setSaving] = useState(false)

  useEffect(() => {
    api.getRiskSettings().then(setSettings)
  }, [])

  if (!settings) return null

  async function save() {
    setSaving(true)
    try {
      await api.saveRiskSettings(settings)
    } finally {
      setSaving(false)
    }
    onSaved()
  }

  return (
    <div className="card p-4 space-y-3">
      <h3 className="text-sm font-semibold text-white">Risk Management</h3>
      <div className="flex flex-wrap gap-4 items-end">
        <div className="space-y-1">
          <label className="text-xs text-slate-400">Account ($)</label>
          <input type="number" value={settings.account_size}
            onChange={e => setSettings(s => ({...s, account_size: +e.target.value}))}
            className="w-28 px-2 py-1 text-xs bg-slate-800 border border-slate-700 rounded text-white" />
        </div>
        <div className="space-y-1">
          <label className="text-xs text-slate-400">Rischio % / trade</label>
          <input type="number" step="0.1" value={settings.risk_per_trade_pct}
            onChange={e => setSettings(s => ({...s, risk_per_trade_pct: +e.target.value}))}
            className="w-20 px-2 py-1 text-xs bg-slate-800 border border-slate-700 rounded text-white" />
        </div>
        <div className="flex items-center gap-2">
          <input type="checkbox" checked={settings.use_fixed_usd}
            onChange={e => setSettings(s => ({...s, use_fixed_usd: e.target.checked}))}
            className="w-3 h-3" id="use-fixed" />
          <label htmlFor="use-fixed" className="text-xs text-slate-400">Importo fisso ($)</label>
          <input type="number" value={settings.risk_per_trade_usd ?? ''}
            disabled={!settings.use_fixed_usd}
            onChange={e => setSettings(s => ({...s, risk_per_trade_usd: +e.target.value}))}
            className="w-20 px-2 py-1 text-xs bg-slate-800 border border-slate-700 rounded text-white disabled:opacity-40" />
        </div>
        <div className="text-xs text-slate-500">
          Max risk: <span className="text-white font-mono">
            {settings.use_fixed_usd && settings.risk_per_trade_usd
              ? `$${settings.risk_per_trade_usd}`
              : `$${((settings.account_size * settings.risk_per_trade_pct) / 100).toFixed(0)}`}
          </span>
        </div>
        <button onClick={save} disabled={saving}
          className="px-3 py-1 text-xs bg-brand-600 hover:bg-brand-500 rounded text-white disabled:opacity-50">
          {saving ? 'Salvando...' : 'Salva & Ricalcola'}
        </button>
      </div>
    </div>
  )
}

function ResetPanel({ onReset }) {
  const [since, setSince] = useState('')
  const [resetting, setResetting] = useState(false)
  const [open, setOpen] = useState(false)

  async function doReset() {
    if (!window.confirm(since
      ? `Eliminare tutti i segnali dal ${since} e riscaricare da Telegram?`
      : 'Eliminare TUTTI i segnali e riscaricare da Telegram? Questa operazione è irreversibile.'))
      return
    setResetting(true)
    try {
      const res = await fetch('/api/reset', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ since: since || null })
      })
      const d = await res.json()
      if (d.ok) {
        alert(`Reset completato: ${d.deleted} segnali eliminati. Ricarico da Telegram...`)
        setTimeout(onReset, 3000)
      } else {
        alert('Errore: ' + (d.error || 'sconosciuto'))
      }
    } finally {
      setResetting(false)
      setOpen(false)
    }
  }

  return (
    <div className="card p-4 border border-rose-900/40">
      <div className="flex items-center justify-between cursor-pointer" onClick={() => setOpen(o => !o)}>
        <h3 className="text-sm font-semibold text-rose-400">Reset & Ricarica da Telegram</h3>
        <span className="text-slate-400 text-xs">{open ? '▲' : '▼'}</span>
      </div>
      {open && (
        <div className="mt-3 flex flex-wrap gap-3 items-end border-t border-slate-700 pt-3">
          <div className="space-y-1">
            <label className="text-xs text-slate-400">Data inizio (opzionale)</label>
            <input type="date" value={since}
              onChange={e => setSince(e.target.value)}
              className="px-2 py-1 text-xs bg-slate-800 border border-slate-700 rounded text-white" />
          </div>
          <div className="text-xs text-slate-500 max-w-xs">
            {since
              ? `Verranno eliminati i segnali dal ${since} in poi e riscaricati da Telegram.`
              : 'Senza data: elimina TUTTI i segnali e riscarica tutto da Telegram.'}
          </div>
          <button onClick={doReset} disabled={resetting}
            className="px-3 py-1 text-xs bg-rose-700 hover:bg-rose-600 rounded text-white disabled:opacity-50">
            {resetting ? 'Reset in corso...' : '⟳ Reset & Ricarica'}
          </button>
        </div>
      )}
    </div>
  )
}

export default function HistoryPage() {
  const [allSignals, setAllSignals] = useState([])
  const [updates, setUpdates] = useState([])
  const [tab, setTab] = useState('signals')
  const [page, setPage] = useState(0)
  const [refreshKey, setRefreshKey] = useState(0)
  const [dateFrom, setDateFrom] = useState('')
  const [dateTo, setDateTo] = useState('')
  const [managedFilter, setManagedFilter] = useState('all') // 'all' | 'managed' | 'missed'
  const PAGE_SIZE = 50

  useEffect(() => {
    if (tab === 'signals') {
      api.getSignals({ limit: 2000, offset: 0 }).then(setAllSignals)
    } else {
      api.getUpdates({ limit: 2000 }).then(setUpdates)
    }
  }, [tab, refreshKey])

  // Filtra per data e per stato "gestito" (ha prodotto un ordine MT5)
  const isManaged = s => Boolean(s.mt5_ticket || s.mt5_tickets)
  const signals = allSignals.filter(s => {
    const ref = s.created_at ? new Date(s.created_at) : null
    if (ref) {
      if (dateFrom && ref < new Date(dateFrom)) return false
      if (dateTo   && ref > new Date(dateTo + 'T23:59:59')) return false
    }
    if (managedFilter === 'managed' && !isManaged(s)) return false
    if (managedFilter === 'missed'  &&  isManaged(s)) return false
    return true
  })
  const managedCount = allSignals.filter(isManaged).length

  // Paginazione sul filtrato
  const paginated = signals.slice(page * PAGE_SIZE, (page + 1) * PAGE_SIZE)

  // Totali P&L visibili (sul filtrato, non solo pagina)
  const closedWithPnl = signals.filter(s => s.pnl_usd != null)
  const totalPnl = closedWithPnl.reduce((acc, s) => acc + s.pnl_usd, 0)

  return (
    <div className="p-6 space-y-4">
      <h1 className="text-xl font-bold text-white">Storico</h1>

      <ResetPanel onReset={() => setRefreshKey(k => k + 1)} />

      <div className="flex flex-wrap gap-3 items-end">
        {['signals', 'updates'].map(t => (
          <button key={t} onClick={() => { setTab(t); setPage(0) }}
            className={`text-sm px-4 py-1.5 rounded border transition-colors ${
              tab === t ? 'bg-brand-600 border-brand-500 text-white' : 'border-slate-700 text-slate-400 hover:text-white'
            }`}>
            {t === 'signals' ? 'Segnali' : 'Aggiornamenti'}
          </button>
        ))}

        {tab === 'signals' && (
          <div className="flex items-center gap-2 ml-2 flex-wrap">
            {[
              { k: 'all',     label: 'Tutti',   count: allSignals.length },
              { k: 'managed', label: 'Gestiti', count: managedCount },
              { k: 'missed',  label: 'Mancati', count: allSignals.length - managedCount },
            ].map(f => (
              <button key={f.k} onClick={() => { setManagedFilter(f.k); setPage(0) }}
                className={`text-xs px-3 py-1 rounded border transition-colors ${
                  managedFilter === f.k
                    ? 'bg-brand-600 border-brand-500 text-white'
                    : 'border-slate-700 text-slate-400 hover:text-white'
                }`}>
                {f.label} <span className="opacity-70">({f.count})</span>
              </button>
            ))}
            <span className="text-xs text-slate-500 ml-2">Dal</span>
            <input type="date" value={dateFrom} onChange={e => { setDateFrom(e.target.value); setPage(0) }}
              className="text-xs px-2 py-1 bg-slate-800 border border-slate-700 rounded text-slate-200" />
            <span className="text-xs text-slate-500">Al</span>
            <input type="date" value={dateTo} onChange={e => { setDateTo(e.target.value); setPage(0) }}
              className="text-xs px-2 py-1 bg-slate-800 border border-slate-700 rounded text-slate-200" />
            {(dateFrom || dateTo) && (
              <button onClick={() => { setDateFrom(''); setDateTo(''); setPage(0) }}
                className="text-xs text-slate-500 hover:text-white px-2 py-1 border border-slate-700 rounded">
                ✕ Reset
              </button>
            )}
            <span className="text-xs text-slate-600">{signals.length} segnali</span>
          </div>
        )}

        {tab === 'signals' && closedWithPnl.length > 0 && (
          <span className={`ml-auto text-sm font-mono font-semibold self-center ${pnlColor(totalPnl)}`}>
            P&L filtro: {fmtPnl(totalPnl)}
          </span>
        )}
      </div>

      {tab === 'signals' && (
        <>
          <div className="overflow-x-auto">
            <table className="w-full text-sm">
              <thead>
                <tr className="text-left text-xs text-slate-500 border-b border-slate-800">
                  <th className="pb-2 pr-3">Simbolo</th>
                  <th className="pb-2 pr-3">Dir.</th>
                  <th className="pb-2 pr-3">Segnale</th>
                  <th className="pb-2 pr-3">Ingresso</th>
                  <th className="pb-2 pr-3">SL</th>
                  <th className="pb-2 pr-3">TP1</th>
                  <th className="pb-2 pr-3">TP2</th>
                  <th className="pb-2 pr-3">TP3</th>
                  <th className="pb-2 pr-3">Stato</th>
                  <th className="pb-2 pr-3">Size</th>
                  <th className="pb-2 pr-3">P&L</th>
                  <th className="pb-2 pr-3">Posizione</th>
                  <th className="pb-2 pr-3">Entrata</th>
                  <th className="pb-2">Uscita</th>
                </tr>
              </thead>
              <tbody className="divide-y divide-slate-800/50">
                {paginated.map(s => <SignalRow key={s.id} s={s} />)}
              </tbody>
            </table>
          </div>
          <div className="flex gap-2 justify-end">
            <button onClick={() => setPage(p => Math.max(0, p - 1))} disabled={page === 0}
              className="text-xs px-3 py-1 border border-slate-700 rounded disabled:opacity-40 text-slate-400 hover:text-white">← Prec</button>
            <span className="text-xs text-slate-500 px-2 self-center">Pagina {page + 1}</span>
            <button onClick={() => setPage(p => p + 1)} disabled={(page + 1) * PAGE_SIZE >= signals.length}
              className="text-xs px-3 py-1 border border-slate-700 rounded disabled:opacity-40 text-slate-400 hover:text-white">Succ →</button>
          </div>
        </>
      )}

      {tab === 'updates' && (
        <div className="space-y-2">
          {updates.map(u => (
            <div key={u.id} className="card flex items-center gap-4">
              <span className="font-bold text-white w-20 flex-shrink-0">{u.symbol}</span>
              <span className="font-mono text-sm text-slate-300">{u.price_from ?? '?'} → {u.price_to ?? '?'}</span>
              <span className="flex-1 text-sm text-slate-400">{u.update_text}</span>
              <span className="text-xs text-slate-600 whitespace-nowrap">
                {u.created_at ? fmtTs(u.created_at) : ''}
              </span>
            </div>
          ))}
        </div>
      )}
    </div>
  )
}
