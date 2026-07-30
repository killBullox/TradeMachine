import { useEffect, useState } from 'react'
import toast from 'react-hot-toast'

export default function NewsFilterPanel() {
  const [events, setEvents] = useState([])
  const [status, setStatus] = useState(null)
  const [cal, setCal] = useState(null)        // stato feed calendario
  const [name, setName] = useState('')
  const [when, setWhen] = useState('')       // "YYYY-MM-DD HH:MM" Roma
  const [flatten, setFlatten] = useState(true)
  const [showPast, setShowPast] = useState(false)
  const [refreshing, setRefreshing] = useState(false)

  const load = async () => {
    try {
      const [ev, st, cs] = await Promise.all([
        fetch('/api/news-events').then(r => r.json()),
        fetch('/api/news-filter/status').then(r => r.json()),
        fetch('/api/economic-calendar/status').then(r => r.json()).catch(() => null),
      ])
      setEvents(ev.events || [])
      setStatus(st)
      setCal(cs)
    } catch { toast.error('Errore caricamento news filter') }
  }
  useEffect(() => { load() }, [])

  const refreshCalendar = async () => {
    setRefreshing(true)
    try {
      const res = await fetch('/api/economic-calendar/refresh', { method: 'POST' })
      const d = await res.json()
      if (d.source_ok) toast.success(`Calendario aggiornato: +${d.added ?? 0} ~${d.updated ?? 0}`)
      else toast.error('Feed non raggiungibile')
      load()
    } catch (e) { toast.error(`Errore refresh: ${e.message}`) }
    finally { setRefreshing(false) }
  }

  const add = async () => {
    if (!name || !when) { toast.error('Nome e data/ora richiesti'); return }
    try {
      const res = await fetch('/api/news-events', {
        method: 'POST', headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ name, event_time_roma: when, flatten }),
      })
      if (!res.ok) { const d = await res.json(); throw new Error(d.detail || res.status) }
      toast.success('Evento aggiunto')
      setName(''); setWhen('')
      load()
    } catch (e) { toast.error(`Errore: ${e.message}`) }
  }

  const del = async (id) => {
    if (!confirm('Rimuovere questo evento?')) return
    await fetch(`/api/news-events/${id}`, { method: 'DELETE' })
    load()
  }

  const visible = events.filter(e => showPast || !e.past)

  return (
    <div className="card p-6 mb-8 border border-orange-600/30">
      <div className="flex items-center justify-between mb-2">
        <h2 className="text-lg font-semibold text-white">News Filter — Calendario economico</h2>
        {status && (
          <div className="flex gap-2 text-xs">
            <span className={`px-2 py-0.5 rounded-full ${status.enabled ? 'bg-emerald-600/30 text-emerald-300' : 'bg-red-600/30 text-red-300'}`}>
              {status.enabled ? 'ATTIVO' : 'DISATTIVATO'}
            </span>
            {status.entry_blocked && (
              <span className="px-2 py-0.5 rounded-full bg-red-600/40 text-red-200 font-semibold">FINESTRA NEWS IN CORSO</span>
            )}
          </div>
        )}
      </div>
      <p className="text-xs text-slate-400 mb-3">
        Eventi USD high-impact caricati automaticamente dal calendario (Forex Factory). Attorno agli eventi:
        blocco ingressi da -10 a +15 min · pending cancellati a -10 min · flatten posizioni a -5 min.
        Flatten weekend: venerdì 22:45 Roma. Toggle globali nel Risk Management.
      </p>

      <div className="flex items-center gap-3 mb-4 text-xs">
        <button onClick={refreshCalendar} disabled={refreshing}
          className="px-3 py-1.5 bg-brand-600 hover:bg-brand-500 disabled:opacity-50 text-white rounded-lg font-medium">
          {refreshing ? 'Aggiorno…' : '↻ Aggiorna calendario ora'}
        </button>
        {cal && (
          <span className="text-slate-400">
            {cal.source_ok
              ? <span className="text-emerald-400">● feed ok</span>
              : <span className="text-red-400">● feed KO</span>}
            {cal.last_refresh_roma ? ` · ultimo: ${cal.last_refresh_roma}` : ' · mai aggiornato'}
            {typeof cal.usd_high_upcoming === 'number' ? ` · ${cal.usd_high_upcoming} eventi USD-High futuri` : ''}
          </span>
        )}
      </div>

      <div className="flex gap-2 mb-4 flex-wrap items-end">
        <div>
          <label className="text-xs text-slate-400 block mb-1">Evento manuale</label>
          <input value={name} onChange={e => setName(e.target.value)} placeholder="Evento extra"
            className="px-3 py-2 text-sm bg-slate-800 border border-slate-700 rounded-lg text-white w-52" />
        </div>
        <div>
          <label className="text-xs text-slate-400 block mb-1">Data/ora (Roma)</label>
          <input value={when} onChange={e => setWhen(e.target.value)} placeholder="2026-07-29 20:00"
            className="px-3 py-2 text-sm bg-slate-800 border border-slate-700 rounded-lg text-white w-44 font-mono" />
        </div>
        <label className="flex items-center gap-1.5 text-xs text-slate-400 pb-2.5">
          <input type="checkbox" checked={flatten} onChange={e => setFlatten(e.target.checked)} className="w-4 h-4" />
          Flatten
        </label>
        <button onClick={add} className="px-4 py-2 text-sm bg-orange-600 hover:bg-orange-500 text-white rounded-lg font-medium">
          Aggiungi
        </button>
        <button onClick={() => setShowPast(s => !s)} className="px-3 py-2 text-xs text-slate-400 hover:text-slate-300">
          {showPast ? 'Nascondi passati' : 'Mostra passati'}
        </button>
      </div>

      <div className="space-y-1.5">
        {visible.length === 0 && <p className="text-xs text-slate-500">Nessun evento futuro in calendario.</p>}
        {visible.map(e => {
          const auto = (e.source || 'manual') === 'forexfactory'
          return (
            <div key={e.id} className={`flex items-center justify-between px-3 py-2 rounded-lg text-sm ${e.past ? 'bg-slate-800/30 text-slate-500' : 'bg-slate-800/70'}`}>
              <div className="flex items-center gap-3 flex-wrap">
                <span className="font-mono text-xs text-slate-400">{e.event_time_roma}</span>
                <span className={e.past ? '' : 'text-white'}>{e.name}</span>
                <span className="text-[10px] px-1.5 py-0.5 bg-slate-700/60 text-slate-300 rounded">{e.currency || 'USD'} · {e.impact || 'high'}</span>
                <span className={`text-[10px] px-1.5 py-0.5 rounded font-semibold ${auto ? 'bg-sky-600/25 text-sky-300' : 'bg-purple-600/25 text-purple-300'}`}>
                  {auto ? 'AUTO' : 'MANUALE'}
                </span>
                {e.flatten && <span className="text-[10px] px-1.5 py-0.5 bg-orange-600/25 text-orange-300 rounded font-semibold" title="A -5 min chiude tutte le posizioni aperte">FLATTEN</span>}
                {e.flatten_done && <span className="text-[10px] px-1.5 py-0.5 bg-slate-600/40 text-slate-400 rounded">eseguito</span>}
              </div>
              {!auto && <button onClick={() => del(e.id)} className="text-xs text-red-400 hover:text-red-300">Rimuovi</button>}
            </div>
          )
        })}
      </div>
    </div>
  )
}
