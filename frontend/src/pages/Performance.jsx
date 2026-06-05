import { useEffect, useState, useCallback } from 'react'
import { api } from '../api'
import {
  LineChart, Line, BarChart, Bar, XAxis, YAxis, CartesianGrid,
  Tooltip, ResponsiveContainer, ReferenceLine, Cell, AreaChart, Area, Brush
} from 'recharts'

// ─── Helpers ─────────────────────────────────────────────────────────────────

const fmt$ = (v) => {
  if (v == null) return '—'
  const abs = Math.abs(v).toLocaleString('it-IT', { minimumFractionDigits: 2, maximumFractionDigits: 2 })
  return `${v >= 0 ? '+' : '-'}$${abs}`
}
const fmtPct = (v) => v != null ? `${v}%` : '—'
const clr = (v) => v == null ? 'text-slate-400' : v >= 0 ? 'text-emerald-400' : 'text-rose-400'

// ─── KPI card ─────────────────────────────────────────────────────────────────

function KPI({ label, value, sub, color, size = 'lg' }) {
  return (
    <div className="card flex flex-col gap-1">
      <p className="text-xs text-slate-400 uppercase tracking-wide">{label}</p>
      <p className={`font-bold ${size === 'xl' ? 'text-4xl' : 'text-2xl'} ${color || 'text-white'}`}>{value}</p>
      {sub && <p className="text-xs text-slate-500">{sub}</p>}
    </div>
  )
}

// ─── Risk settings panel ──────────────────────────────────────────────────────

function RiskPanel({ settings, onSaved }) {
  const [form, setForm] = useState(settings)
  const [saving, setSaving] = useState(false)
  const [open, setOpen] = useState(false)

  useEffect(() => setForm(settings), [settings])

  const save = async () => {
    setSaving(true)
    try {
      await api.saveRiskSettings(form)
      onSaved()
      setOpen(false)
    } finally {
      setSaving(false)
    }
  }

  const riskAmount = form.use_fixed_usd && form.risk_per_trade_usd
    ? form.risk_per_trade_usd
    : (form.account_size * form.risk_per_trade_pct / 100)

  return (
    <div className="card border border-slate-700">
      <div className="flex items-center justify-between cursor-pointer" onClick={() => setOpen(o => !o)}>
        <div>
          <p className="text-xs text-slate-400 uppercase tracking-wide">Risk Settings</p>
          <p className="text-white font-semibold mt-0.5">
            Account ${form.account_size?.toLocaleString()} &nbsp;·&nbsp;
            Rischio {form.use_fixed_usd ? `$${form.risk_per_trade_usd}` : `${form.risk_per_trade_pct}%`}/trade
            &nbsp;≈&nbsp;
            <span className="text-amber-400">${riskAmount?.toFixed(2)}</span>
          </p>
        </div>
        <span className="text-slate-400 text-lg">{open ? '▲' : '▼'}</span>
      </div>

      {open && (
        <div className="mt-4 grid grid-cols-2 md:grid-cols-5 gap-4 border-t border-slate-700 pt-4">
          {/* Account size */}
          <div>
            <label className="text-xs text-slate-400 block mb-1">Capital ($)</label>
            <input
              type="number" min="100"
              className="w-full bg-slate-800 border border-slate-600 rounded px-3 py-1.5 text-white text-sm"
              value={form.account_size}
              onChange={e => setForm(f => ({ ...f, account_size: +e.target.value }))}
            />
          </div>

          {/* Tipo rischio */}
          <div>
            <label className="text-xs text-slate-400 block mb-1">Tipo rischio</label>
            <select
              className="w-full bg-slate-800 border border-slate-600 rounded px-3 py-1.5 text-white text-sm"
              value={form.use_fixed_usd ? 'fixed' : 'pct'}
              onChange={e => setForm(f => ({ ...f, use_fixed_usd: e.target.value === 'fixed' }))}
            >
              <option value="pct">% del capitale</option>
              <option value="fixed">$ fisso</option>
            </select>
          </div>

          {/* Rischio */}
          {form.use_fixed_usd ? (
            <div>
              <label className="text-xs text-slate-400 block mb-1">Rischio fisso ($)</label>
              <input
                type="number" min="1"
                className="w-full bg-slate-800 border border-slate-600 rounded px-3 py-1.5 text-white text-sm"
                value={form.risk_per_trade_usd || ''}
                onChange={e => setForm(f => ({ ...f, risk_per_trade_usd: +e.target.value }))}
              />
            </div>
          ) : (
            <div>
              <label className="text-xs text-slate-400 block mb-1">Rischio per trade (%)</label>
              <input
                type="number" min="0.1" max="10" step="0.1"
                className="w-full bg-slate-800 border border-slate-600 rounded px-3 py-1.5 text-white text-sm"
                value={form.risk_per_trade_pct}
                onChange={e => setForm(f => ({ ...f, risk_per_trade_pct: +e.target.value }))}
              />
            </div>
          )}

          {/* Tolleranza entry */}
          <div>
            <label className="text-xs text-slate-400 block mb-1" title="Pip di tolleranza per entrare a mercato quando il prezzo è fuori dal range del segnale">
              Tolleranza entry (pip)
            </label>
            <input
              type="number" min="0" step="0.5"
              className="w-full bg-slate-800 border border-slate-600 rounded px-3 py-1.5 text-white text-sm"
              value={form.entry_tolerance_pips ?? 3}
              onChange={e => setForm(f => ({ ...f, entry_tolerance_pips: +e.target.value }))}
            />
            <p className="text-xs text-slate-500 mt-1">≈ ${((form.entry_tolerance_pips ?? 3) * 0.10).toFixed(2)} su gold</p>
          </div>

          {/* Rischio calcolato + salva */}
          <div className="flex flex-col justify-end gap-2">
            <p className="text-xs text-amber-400">
              Rischio/trade: <strong>${riskAmount?.toFixed(2)}</strong>
            </p>
            <button
              onClick={save} disabled={saving}
              className="bg-brand-600 hover:bg-brand-500 text-white text-sm px-4 py-1.5 rounded disabled:opacity-50"
            >
              {saving ? 'Salvo...' : 'Salva & Ricalcola'}
            </button>
          </div>
        </div>
      )}
    </div>
  )
}

// ─── Equity curve ─────────────────────────────────────────────────────────────

function EquityCurve({ bySymbol, totalPnl }) {
  // Creiamo una equity curve simulata per simbolo
  const data = bySymbol.map((s, i) => ({
    name: s.symbol,
    pnl: Math.round(s.pnl * 100) / 100,
    fill: s.pnl >= 0 ? '#10b981' : '#f43f5e',
  }))

  return (
    <div className="card">
      <h2 className="text-sm font-semibold text-slate-400 mb-4 uppercase tracking-wide">P&L per Simbolo</h2>
      <ResponsiveContainer width="100%" height={200}>
        <BarChart data={data} margin={{ top: 4, right: 4, left: 0, bottom: 0 }}>
          <CartesianGrid strokeDasharray="3 3" stroke="#1e293b" />
          <XAxis dataKey="name" tick={{ fill: '#94a3b8', fontSize: 11 }} axisLine={false} tickLine={false} />
          <YAxis tick={{ fill: '#94a3b8', fontSize: 11 }} axisLine={false} tickLine={false}
            tickFormatter={v => `$${v}`} />
          <Tooltip
            contentStyle={{ background: '#1e293b', border: '1px solid #334155', borderRadius: 8 }}
            formatter={(v) => [`$${v.toFixed(2)}`, 'P&L']}
          />
          <ReferenceLine y={0} stroke="#475569" />
          <Bar dataKey="pnl" radius={[4, 4, 0, 0]}>
            {data.map((entry, i) => <Cell key={i} fill={entry.fill} />)}
          </Bar>
        </BarChart>
      </ResponsiveContainer>
    </div>
  )
}

// ─── Tabella per simbolo ──────────────────────────────────────────────────────

function SymbolTable({ bySymbol, untradeableSymbols = [] }) {
  const untradeable = new Set((untradeableSymbols || []).map(s => s.toUpperCase()))
  const tradeable = bySymbol.filter(s => !untradeable.has(s.symbol.toUpperCase()))
  const excluded  = bySymbol.filter(s =>  untradeable.has(s.symbol.toUpperCase()))

  const rows = (list, dimmed = false) => list.map(s => {
    const total = s.wins + s.losses
    const wr = total > 0 ? Math.round(s.wins / total * 100) : null
    return (
      <tr key={s.symbol} className={`border-b border-slate-800 ${dimmed ? 'opacity-40' : 'hover:bg-slate-800/40'}`}>
        <td className="py-2 font-semibold text-white flex items-center gap-1.5">
          {dimmed && <span className="text-rose-600 text-xs" title="Non eseguibile su MT5">✕</span>}
          {s.symbol}
        </td>
        <td className="py-2 text-center text-slate-300">{s.count}</td>
        <td className="py-2 text-center text-emerald-400">{s.wins}</td>
        <td className="py-2 text-center text-rose-400">{s.losses}</td>
        <td className="py-2 text-center text-slate-300">{wr != null ? `${wr}%` : '—'}</td>
        <td className={`py-2 text-right font-semibold ${dimmed ? 'text-slate-500' : clr(s.pnl)}`}>
          {dimmed ? '—' : fmt$(s.pnl)}
        </td>
      </tr>
    )
  })

  return (
    <div className="card overflow-hidden">
      <h2 className="text-sm font-semibold text-slate-400 mb-3 uppercase tracking-wide">Dettaglio per simbolo</h2>
      <table className="w-full text-sm">
        <thead>
          <tr className="text-slate-500 text-xs uppercase border-b border-slate-700">
            <th className="text-left pb-2">Simbolo</th>
            <th className="text-center pb-2">Trade</th>
            <th className="text-center pb-2">W</th>
            <th className="text-center pb-2">L</th>
            <th className="text-center pb-2">Win%</th>
            <th className="text-right pb-2">P&L</th>
          </tr>
        </thead>
        <tbody>
          {rows(tradeable)}
          {excluded.length > 0 && (
            <>
              <tr><td colSpan={6} className="pt-3 pb-1 text-xs text-rose-600 uppercase tracking-wider">Non eseguibili su MT5</td></tr>
              {rows(excluded, true)}
            </>
          )}
        </tbody>
      </table>
    </div>
  )
}

// ─── Equity Curve storica zoomabile ───────────────────────────────────────────

function EquityCurveHistory({ dateFrom, dateTo }) {
  const [data, setData] = useState(null)
  const [loading, setLoading] = useState(true)

  useEffect(() => {
    setLoading(true)
    const params = new URLSearchParams()
    if (dateFrom) params.append('date_from', dateFrom)
    if (dateTo) params.append('date_to', dateTo)
    fetch(`/api/performance/equity-curve?${params}`)
      .then(r => r.json())
      .then(d => { setData(d); setLoading(false) })
      .catch(() => setLoading(false))
  }, [dateFrom, dateTo])

  if (loading) return <div className="card text-slate-400 text-sm">Caricamento equity curve...</div>
  if (!data || !data.points?.length) return <div className="card text-slate-400 text-sm">Nessun trade chiuso nel periodo.</div>

  // Formatta data per asse X in ora Roma
  const fmtDate = (ts_ms) => {
    const d = new Date(ts_ms)
    return d.toLocaleDateString('it-IT', { day: '2-digit', month: '2-digit', year: '2-digit' })
  }
  const fmtDateTime = (ts_ms) => {
    const d = new Date(ts_ms)
    return d.toLocaleString('it-IT', { day: '2-digit', month: '2-digit', year: '2-digit', hour: '2-digit', minute: '2-digit' })
  }

  const CustomTooltip = ({ active, payload }) => {
    if (!active || !payload?.length) return null
    const p = payload[0].payload
    return (
      <div className="bg-slate-900 border border-slate-700 rounded p-2 text-xs">
        <div className="text-slate-400">{fmtDateTime(p.ts_ms)}</div>
        <div className="text-white font-semibold">#{p.id} {p.symbol} {p.direction?.toUpperCase()} <span className="text-slate-400">({p.status})</span></div>
        <div className={p.pnl >= 0 ? 'text-emerald-400' : 'text-rose-400'}>
          Trade P&L: {fmt$(p.pnl)}
        </div>
        <div className={p.cum_pnl >= 0 ? 'text-emerald-400 font-bold' : 'text-rose-400 font-bold'}>
          Cumulativo: {fmt$(p.cum_pnl)}
        </div>
        {p.drawdown > 0 && (
          <div className="text-amber-400">Drawdown: -${p.drawdown.toFixed(2)}</div>
        )}
      </div>
    )
  }

  // Sommari KPI
  const finalCum = data.total_pnl
  const wins = data.points.filter(p => p.pnl > 0).length
  const losses = data.points.filter(p => p.pnl < 0).length

  return (
    <div className="card">
      <div className="flex items-center justify-between mb-3 flex-wrap gap-2">
        <h2 className="text-sm font-semibold text-slate-400 uppercase tracking-wide">
          Equity Curve — andamento storico P&L cumulativo
        </h2>
        <div className="text-xs text-slate-500">
          {data.count} trade · finale <span className={`font-semibold ${finalCum >= 0 ? 'text-emerald-400' : 'text-rose-400'}`}>{fmt$(finalCum)}</span> · max DD <span className="text-rose-400 font-semibold">-${data.max_drawdown.toFixed(2)}</span> · peak <span className="text-emerald-400">{fmt$(data.peak)}</span>
        </div>
      </div>

      <ResponsiveContainer width="100%" height={360}>
        <AreaChart data={data.points} margin={{ top: 10, right: 16, left: 0, bottom: 0 }}>
          <defs>
            <linearGradient id="eqGradPos" x1="0" y1="0" x2="0" y2="1">
              <stop offset="0%" stopColor="#10b981" stopOpacity={0.45} />
              <stop offset="100%" stopColor="#10b981" stopOpacity={0.02} />
            </linearGradient>
            <linearGradient id="eqGradNeg" x1="0" y1="0" x2="0" y2="1">
              <stop offset="0%" stopColor="#f43f5e" stopOpacity={0.45} />
              <stop offset="100%" stopColor="#f43f5e" stopOpacity={0.02} />
            </linearGradient>
          </defs>
          <CartesianGrid strokeDasharray="3 3" stroke="#1e293b" />
          <XAxis
            dataKey="ts_ms"
            type="number"
            domain={['dataMin', 'dataMax']}
            scale="time"
            tickFormatter={fmtDate}
            tick={{ fill: '#94a3b8', fontSize: 11 }}
            axisLine={false}
            tickLine={false}
          />
          <YAxis
            tick={{ fill: '#94a3b8', fontSize: 11 }}
            axisLine={false}
            tickLine={false}
            tickFormatter={v => `$${v}`}
          />
          <Tooltip content={<CustomTooltip />} />
          <ReferenceLine y={0} stroke="#475569" strokeDasharray="3 3" />
          <Area
            type="monotone"
            dataKey="cum_pnl"
            stroke={finalCum >= 0 ? '#10b981' : '#f43f5e'}
            strokeWidth={2}
            fill={finalCum >= 0 ? 'url(#eqGradPos)' : 'url(#eqGradNeg)'}
            dot={false}
            activeDot={{ r: 4 }}
          />
          <Brush
            dataKey="ts_ms"
            height={28}
            stroke="#475569"
            fill="#1e293b"
            tickFormatter={fmtDate}
            travellerWidth={10}
          />
        </AreaChart>
      </ResponsiveContainer>

      <p className="text-xs text-slate-500 mt-2">
        Trascina le maniglie del riquadro in basso per zoomare su un intervallo. Hover sui punti per il dettaglio trade. {wins} wins, {losses} losses.
      </p>
    </div>
  )
}

// ─── Heatmap Simbolo x Ora Roma ───────────────────────────────────────────────

function SymbolHourHeatmap({ dateFrom, dateTo }) {
  const [data, setData] = useState(null)
  const [loading, setLoading] = useState(true)
  const [sortBy, setSortBy] = useState('pnl') // 'pnl' | 'count' | 'wr'

  useEffect(() => {
    setLoading(true)
    const params = new URLSearchParams()
    if (dateFrom) params.append('date_from', dateFrom)
    if (dateTo) params.append('date_to', dateTo)
    fetch(`/api/performance/by-symbol-hour?${params}`)
      .then(r => r.json())
      .then(d => { setData(d); setLoading(false) })
      .catch(() => setLoading(false))
  }, [dateFrom, dateTo])

  if (loading) return <div className="card text-slate-400 text-sm">Caricamento heatmap...</div>
  if (!data || !data.rows?.length) return <div className="card text-slate-400 text-sm">Nessun trade chiuso nel periodo.</div>

  // Costruisci matrice [symbol][hour] = riga
  const cellMap = {}
  data.rows.forEach(r => {
    cellMap[r.symbol] = cellMap[r.symbol] || {}
    cellMap[r.symbol][r.hour] = r
  })

  // Ordina simboli (rows del heatmap)
  const symbolStats = data.by_symbol || []
  const sortedSymbols = [...symbolStats].sort((a, b) => {
    if (sortBy === 'pnl') return b.pnl - a.pnl
    if (sortBy === 'count') return b.count - a.count
    if (sortBy === 'wr') {
      const wrA = a.wins + a.losses > 0 ? a.wins / (a.wins + a.losses) : 0
      const wrB = b.wins + b.losses > 0 ? b.wins / (b.wins + b.losses) : 0
      return wrB - wrA
    }
    return 0
  })

  // Colore per WR: <50% rosso, >=50% verde, null slate
  const colorForWR = (wr) => {
    if (wr == null) return 'bg-slate-800/40'
    return wr >= 50 ? 'bg-emerald-700/70' : 'bg-rose-700/70'
  }
  // Colore riga totale per ora (basato su P&L)
  const colorForPnl = (pnl) => {
    if (pnl == null || pnl === 0) return 'bg-slate-800/40'
    return pnl > 0 ? 'bg-emerald-700/60' : 'bg-rose-700/60'
  }

  const hours = Array.from({length: 24}, (_, i) => i)

  return (
    <div className="card">
      <div className="flex items-center justify-between mb-4 flex-wrap gap-3">
        <div>
          <h2 className="text-sm font-semibold text-slate-400 uppercase tracking-wide">
            Performance per simbolo × ora ingresso (Roma)
          </h2>
          <p className="text-xs text-slate-500 mt-1">
            {data.total_trades} trade · P&L totale {fmt$(data.total_pnl)}
          </p>
        </div>
        <div className="flex items-center gap-2 text-xs">
          <span className="text-slate-500">Ordina simboli per:</span>
          {['pnl', 'count', 'wr'].map(opt => (
            <button key={opt} onClick={() => setSortBy(opt)}
              className={`px-2 py-1 rounded ${sortBy === opt ? 'bg-brand-700 text-white' : 'text-slate-400 hover:text-white border border-slate-700'}`}>
              {opt === 'pnl' ? 'P&L' : opt === 'count' ? 'N. trade' : 'Win rate'}
            </button>
          ))}
        </div>
      </div>

      <div className="overflow-x-auto">
        <table className="text-sm border-collapse">
          <thead>
            <tr>
              <th className="text-left p-1 text-slate-400 text-sm sticky left-0 bg-slate-900 z-10">Simbolo</th>
              {hours.map(h => (
                <th key={h} className="p-1 text-slate-400 font-semibold w-20 text-center text-sm" title={`${h}:00-${h}:59 Roma`}>
                  {String(h).padStart(2,'0')}
                </th>
              ))}
              <th className="p-1 text-slate-400 font-semibold text-right pl-3 text-sm">Tot</th>
            </tr>
          </thead>
          <tbody>
            {sortedSymbols.map(sym => {
              const symPnl = sym.pnl
              return (
                <tr key={sym.symbol} className="border-t border-slate-800">
                  <td className="p-1 text-white font-semibold pr-3 sticky left-0 bg-slate-900 z-10 whitespace-nowrap text-base">{sym.symbol}</td>
                  {hours.map(h => {
                    const cell = cellMap[sym.symbol]?.[h]
                    if (!cell) return <td key={h} className="p-0.5"><div className="h-20 w-20 rounded bg-slate-800/20" /></td>
                    const pnlCompact = (cell.pnl_usd >= 0 ? '+' : '-') + '$' + Math.abs(cell.pnl_usd).toFixed(0)
                    return (
                      <td key={h} className="p-0.5"
                          title={`${sym.symbol} ${String(h).padStart(2,'0')}:00 Roma — ${cell.count} trade, ${cell.wins}W ${cell.losses}L, WR ${cell.win_rate_pct ?? '—'}%, P&L ${fmt$(cell.pnl_usd)} (avg ${fmt$(cell.avg_pnl_per_trade)})`}>
                        <div className={`h-20 w-20 rounded flex flex-col items-center justify-center ${colorForWR(cell.win_rate_pct)}`}>
                          <span className="text-lg text-white font-bold leading-none">{cell.count}</span>
                          <span className="text-sm text-white/90 leading-none mt-0.5">{cell.win_rate_pct ?? '—'}%</span>
                          <span className="text-sm text-white font-semibold leading-none mt-1">{pnlCompact}</span>
                        </div>
                      </td>
                    )
                  })}
                  <td className={`p-1 text-right pl-3 font-semibold text-base ${clr(symPnl)}`}>
                    {fmt$(symPnl)}
                  </td>
                </tr>
              )
            })}
            {/* Totale per ora */}
            <tr className="border-t-2 border-slate-700 bg-slate-800/40">
              <td className="p-1 text-slate-200 font-semibold pr-3 sticky left-0 bg-slate-800 z-10 text-base">Totale</td>
              {hours.map(h => {
                const hb = data.by_hour.find(x => x.hour === h)
                if (!hb) return <td key={h} className="p-0.5"><div className="h-20 w-20" /></td>
                const pnlCompact = (hb.pnl >= 0 ? '+' : '-') + '$' + Math.abs(hb.pnl).toFixed(0)
                return (
                  <td key={h} className="p-0.5"
                      title={`Ora ${String(h).padStart(2,'0')}:00 Roma totale — ${hb.count} trade, P&L ${fmt$(hb.pnl)}`}>
                    <div className={`h-20 w-20 rounded flex flex-col items-center justify-center ${colorForPnl(hb.pnl)}`}>
                      <span className="text-lg text-white font-bold leading-none">{hb.count}</span>
                      <span className="text-sm text-white font-semibold leading-none mt-1">{pnlCompact}</span>
                    </div>
                  </td>
                )
              })}
              <td className={`p-1 text-right pl-3 font-bold text-base ${clr(data.total_pnl)}`}>{fmt$(data.total_pnl)}</td>
            </tr>
          </tbody>
        </table>
      </div>

      <p className="text-xs text-slate-500 mt-3">
        Cella (3 righe): numero trade · win-rate% · P&L USD assoluto. Sfondo verde se WR ≥ 50%, rosso se &lt; 50%. Hover per dettaglio completo.
      </p>
    </div>
  )
}

// ─── Trading Calendar ─────────────────────────────────────────────────────────

const MONTHS_IT = ['Gennaio','Febbraio','Marzo','Aprile','Maggio','Giugno',
                   'Luglio','Agosto','Settembre','Ottobre','Novembre','Dicembre']
const DAYS_IT   = ['DOM','LUN','MAR','MER','GIO','VEN','SAB']

function TradingCalendar() {
  const now = new Date()
  const [year,  setYear]  = useState(now.getFullYear())
  const [month, setMonth] = useState(now.getMonth() + 1)
  const [data,  setData]  = useState({})
  const [view,  setView]  = useState('pnl') // 'pnl' | 'events'

  useEffect(() => {
    fetch(`/api/performance/calendar?year=${year}&month=${month}`)
      .then(r => r.json()).then(d => setData(d.days || {}))
  }, [year, month])

  const prev = () => { if (month === 1) { setYear(y => y-1); setMonth(12) } else setMonth(m => m-1) }
  const next = () => { if (month === 12) { setYear(y => y+1); setMonth(1) } else setMonth(m => m+1) }

  // Costruisce griglia calendario
  const firstDay = new Date(year, month - 1, 1).getDay()
  const daysInMonth = new Date(year, month, 0).getDate()
  const cells = []
  for (let i = 0; i < firstDay; i++) cells.push(null)
  for (let d = 1; d <= daysInMonth; d++) cells.push(d)
  while (cells.length % 7 !== 0) cells.push(null)

  return (
    <div className="card">
      {/* Header */}
      <div className="flex items-center justify-between mb-4">
        <h2 className="text-sm font-semibold text-slate-400 uppercase tracking-wide">Trading Calendar</h2>
        <div className="flex items-center gap-3">
          <div className="flex rounded overflow-hidden border border-slate-700 text-xs">
            <button onClick={() => setView('pnl')}
              className={`px-3 py-1 ${view==='pnl' ? 'bg-emerald-700 text-white' : 'text-slate-400 hover:text-white'}`}>
              PNL
            </button>
            <button onClick={() => setView('events')}
              className={`px-3 py-1 ${view==='events' ? 'bg-brand-700 text-white' : 'text-slate-400 hover:text-white'}`}>
              Events
            </button>
          </div>
          <div className="flex items-center gap-2">
            <button onClick={prev} className="text-slate-400 hover:text-white px-2">←</button>
            <span className="text-white font-semibold text-sm w-36 text-center">
              {MONTHS_IT[month-1]} {year}
            </span>
            <button onClick={next} className="text-slate-400 hover:text-white px-2">→</button>
          </div>
        </div>
      </div>

      {/* Giorni settimana */}
      <div className="grid grid-cols-7 mb-1">
        {DAYS_IT.map(d => (
          <div key={d} className="text-center text-xs text-slate-500 uppercase py-1">{d}</div>
        ))}
      </div>

      {/* Celle */}
      <div className="grid grid-cols-7 gap-1">
        {cells.map((day, i) => {
          if (!day) return <div key={i} />
          const key = `${year}-${String(month).padStart(2,'0')}-${String(day).padStart(2,'0')}`
          const info = data[key]
          const pnl  = info?.pnl ?? null
          const isPos = pnl > 0
          const isNeg = pnl < 0

          let bg = 'bg-slate-800/30'
          if (pnl !== null) bg = isPos ? 'bg-emerald-900/50' : isNeg ? 'bg-rose-900/50' : 'bg-slate-700/30'
          const border = pnl !== null ? (isPos ? 'border-emerald-700/40' : isNeg ? 'border-rose-700/40' : 'border-slate-700') : 'border-slate-800'

          return (
            <div key={i} className={`${bg} border ${border} rounded-lg p-2 min-h-[80px] flex flex-col`}>
              <span className="text-xs text-slate-400 font-medium">{day}</span>
              {info && view === 'pnl' && (
                <>
                  <span className={`text-sm font-bold mt-auto ${isPos ? 'text-emerald-400' : 'text-rose-400'}`}>
                    {isPos ? '+' : ''}{pnl !== null ? `$${Math.abs(pnl).toFixed(2)}` : ''}
                  </span>
                  <span className="text-xs text-slate-500">{info.win_rate}%</span>
                </>
              )}
              {info && view === 'events' && (
                <span className="text-xs text-slate-300 mt-auto">{info.trades} trade{info.trades !== 1 ? 's' : ''}</span>
              )}
            </div>
          )
        })}
      </div>
    </div>
  )
}

// ─── Main ─────────────────────────────────────────────────────────────────────

export default function Performance() {
  const [perf, setPerf] = useState(null)
  const [loading, setLoading] = useState(true)
  const [dateFrom, setDateFrom] = useState('')
  const [dateTo, setDateTo] = useState('')

  const load = useCallback(() => {
    setLoading(true)
    const params = {}
    if (dateFrom) params.date_from = dateFrom
    if (dateTo) params.date_to = dateTo
    api.getPerformance(params).then(d => { setPerf(d); setLoading(false) })
  }, [dateFrom, dateTo])

  useEffect(() => { load() }, [load])

  if (loading || !perf) return <div className="p-6 text-slate-400">Caricamento...</div>

  const rs = perf.risk_settings || {}

  return (
    <div className="p-6 space-y-6">
      <div className="flex items-center justify-between flex-wrap gap-3">
        <h1 className="text-xl font-bold text-white">Performance</h1>
        <div className="flex items-center gap-2 flex-wrap">
          <label className="text-xs text-slate-400">Dal</label>
          <input
            type="datetime-local"
            value={dateFrom}
            onChange={e => setDateFrom(e.target.value)}
            className="bg-slate-800 border border-slate-600 rounded px-2 py-1 text-xs text-slate-200 focus:outline-none focus:border-brand-500"
          />
          <label className="text-xs text-slate-400">Al</label>
          <input
            type="datetime-local"
            value={dateTo}
            onChange={e => setDateTo(e.target.value)}
            className="bg-slate-800 border border-slate-600 rounded px-2 py-1 text-xs text-slate-200 focus:outline-none focus:border-brand-500"
          />
          {(dateFrom || dateTo) && (
            <button
              onClick={() => { setDateFrom(''); setDateTo('') }}
              className="text-xs text-slate-500 hover:text-slate-300 px-2 py-1 border border-slate-700 rounded"
            >
              Reset
            </button>
          )}
          <button onClick={load} className="text-xs text-slate-400 hover:text-white border border-slate-700 rounded px-3 py-1.5">
            Aggiorna
          </button>
        </div>
      </div>
      {(dateFrom || dateTo) && (
        <div className="text-xs text-amber-400 bg-amber-900/20 border border-amber-800/40 rounded px-3 py-2">
          Statistiche filtrate: {dateFrom ? `dal ${dateFrom.replace('T',' ')}` : ''}{dateTo ? ` al ${dateTo.replace('T',' ')}` : ''}
        </div>
      )}

      {/* Risk Settings */}
      <RiskPanel settings={rs} onSaved={load} />

      {/* KPI principali */}
      <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
        <KPI
          label="P&L Totale"
          value={fmt$(perf.total_pnl_usd)}
          color={clr(perf.total_pnl_usd)}
          sub={`${perf.closed_trades} trade chiusi`}
          size="xl"
        />
        <KPI
          label="Win Rate"
          value={fmtPct(perf.win_rate_pct)}
          color={perf.win_rate_pct >= 50 ? 'text-emerald-400' : 'text-rose-400'}
          sub={`${perf.tp_hits} TP · ${perf.sl_hits} SL`}
        />
        <KPI
          label="Profit Factor"
          value={perf.profit_factor ?? '—'}
          color={perf.profit_factor >= 1 ? 'text-emerald-400' : 'text-rose-400'}
          sub="wins / losses"
        />
        <KPI
          label="Max Drawdown"
          value={perf.max_drawdown_usd ? `$${perf.max_drawdown_usd.toLocaleString()}` : '—'}
          color="text-rose-400"
          sub={`Streak: +${perf.best_streak} / ${perf.worst_streak}`}
        />
      </div>

      {/* Seconda riga KPI */}
      <div className="grid grid-cols-2 md:grid-cols-5 gap-4">
        <KPI
          label="Segnali ricevuti"
          value={<>{perf.total_signals ?? '—'} <span className="text-slate-500 text-lg font-normal">({perf.managed_signals ?? 0})</span></>}
          color="text-white"
          sub="totali (gestiti)"
        />
        <KPI label="Totale Wins" value={fmt$(perf.total_wins_usd)} color="text-emerald-400" sub={`Media: ${fmt$(perf.avg_win_usd)}`} />
        <KPI label="Totale Loss" value={fmt$(perf.total_loss_usd)} color="text-rose-400" sub={`Media: ${fmt$(perf.avg_loss_usd)}`} />
        <KPI
          label="Ultimi 7gg (segnali)"
          value={<>{perf.signals_last_7d ?? '—'} <span className="text-slate-500 text-lg font-normal">({perf.managed_signals_last_7d ?? 0})</span></>}
          color="text-sky-400"
          sub="totali (gestiti)"
        />
        <KPI label="P&L Ultimi 7gg" value={fmt$(perf.pnl_last_7d)} color={clr(perf.pnl_last_7d)} />
      </div>

      {/* Terza riga KPI: Sharpe, Avg Win, Avg Loss */}
      <div className="grid grid-cols-3 gap-4">
        <KPI
          label="Sharpe Ratio"
          value={perf.sharpe_ratio != null ? perf.sharpe_ratio.toFixed(2) : '—'}
          color={
            perf.sharpe_ratio == null ? 'text-slate-400'
            : perf.sharpe_ratio >= 2 ? 'text-emerald-400'
            : perf.sharpe_ratio >= 1 ? 'text-sky-400'
            : 'text-amber-400'
          }
          sub="annualizzato su P&L giornalieri"
        />
        <KPI
          label="Avg Win"
          value={fmt$(perf.avg_win_usd)}
          color="text-emerald-400"
          sub={`${perf.tp_hits ?? 0} TP hit`}
        />
        <KPI
          label="Avg Loss"
          value={fmt$(perf.avg_loss_usd)}
          color="text-rose-400"
          sub={`${perf.sl_hits ?? 0} SL hit`}
        />
      </div>

      {/* Grafici */}
      <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
        <EquityCurve bySymbol={perf.by_symbol || []} totalPnl={perf.total_pnl_usd} />

        {/* Win/Loss donut */}
        <div className="card">
          <h2 className="text-sm font-semibold text-slate-400 mb-4 uppercase tracking-wide">Trade chiusi</h2>
          <div className="flex items-center justify-around h-[200px]">
            <div className="text-center">
              <p className="text-5xl font-bold text-emerald-400">{perf.tp_hits}</p>
              <p className="text-sm text-slate-400 mt-1">TP Hit</p>
            </div>
            <div className="text-center">
              <p className="text-5xl font-bold text-rose-400">{perf.sl_hits}</p>
              <p className="text-sm text-slate-400 mt-1">SL Hit</p>
            </div>
            <div className="text-center">
              <p className="text-5xl font-bold text-slate-300">{perf.total_signals - perf.closed_trades}</p>
              <p className="text-sm text-slate-400 mt-1">Aperti</p>
            </div>
          </div>
        </div>
      </div>

      {/* Equity curve storica zoomabile */}
      <EquityCurveHistory dateFrom={dateFrom} dateTo={dateTo} />

      {/* Tabella per simbolo */}
      <SymbolTable bySymbol={perf.by_symbol || []} untradeableSymbols={perf.untradeable_symbols || []} />

      {/* Heatmap simbolo x ora di ingresso */}
      <SymbolHourHeatmap dateFrom={dateFrom} dateTo={dateTo} />

      {/* Calendario P&L */}
      <TradingCalendar />
    </div>
  )
}
