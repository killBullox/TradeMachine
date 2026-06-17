import { useEffect, useState } from 'react'
import toast from 'react-hot-toast'

const HOURS = Array.from({ length: 24 }, (_, i) => i)

export default function FilterPanel() {
  const [loading, setLoading] = useState(true)
  const [saving, setSaving] = useState(false)
  const [available, setAvailable] = useState([])
  const [excluded, setExcluded] = useState([])
  const [allowedHours, setAllowedHours] = useState(null) // null = tutte
  const [dirty, setDirty] = useState(false)

  const load = async () => {
    try {
      const res = await fetch('/api/filter-settings')
      const data = await res.json()
      setAvailable(data.available_symbols || [])
      setExcluded(data.excluded_symbols || [])
      setAllowedHours(data.allowed_hours)
    } catch (e) {
      toast.error('Errore caricamento filtri')
    } finally {
      setLoading(false)
    }
  }
  useEffect(() => { load() }, [])

  const toggleSymbol = (s) => {
    setExcluded(prev => prev.includes(s) ? prev.filter(x => x !== s) : [...prev, s])
    setDirty(true)
  }
  const toggleHour = (h) => {
    setDirty(true)
    setAllowedHours(prev => {
      if (prev === null) {
        // attivava tutte → ora una sola attiva diventa la lista
        return HOURS.filter(x => x !== h)
      }
      return prev.includes(h) ? prev.filter(x => x !== h) : [...prev, h].sort((a, b) => a - b)
    })
  }
  const allHours = () => { setAllowedHours(null); setDirty(true) }
  const noHours = () => { setAllowedHours([]); setDirty(true) }

  const save = async () => {
    setSaving(true)
    try {
      const res = await fetch('/api/filter-settings', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ excluded_symbols: excluded, allowed_hours: allowedHours })
      })
      if (!res.ok) throw new Error(`status ${res.status}`)
      toast.success('Filtri salvati')
      setDirty(false)
    } catch (e) {
      toast.error('Errore salvataggio filtri')
    } finally {
      setSaving(false)
    }
  }

  if (loading) return <div className="text-gray-400 text-sm">Caricamento filtri...</div>

  return (
    <div className="bg-gray-800 rounded-xl p-5 mb-6 border border-gray-700">
      <div className="flex items-center justify-between mb-4">
        <div>
          <h3 className="text-lg font-semibold text-white">Filtri Segnali</h3>
          <p className="text-xs text-gray-400 mt-1">
            I segnali esclusi vengono salvati nello storico con flag "filtered" e simulati (senza piazzare ordini su MT5). Stats reali e What-If sono separate.
          </p>
        </div>
        <button
          onClick={save}
          disabled={!dirty || saving}
          className="px-4 py-2 bg-violet-600 hover:bg-violet-500 disabled:bg-gray-600 disabled:cursor-not-allowed text-white rounded-lg text-sm font-medium"
        >
          {saving ? 'Salvataggio...' : 'Salva filtri'}
        </button>
      </div>

      <div className="mb-5">
        <div className="text-sm font-medium text-gray-300 mb-2">Simboli da escludere</div>
        {available.length === 0 ? (
          <div className="text-xs text-gray-500">Nessun simbolo tradato ancora.</div>
        ) : (
          <div className="flex flex-wrap gap-2">
            {available.map(s => {
              const on = excluded.includes(s)
              return (
                <button
                  key={s}
                  onClick={() => toggleSymbol(s)}
                  className={`px-3 py-1.5 rounded-lg text-xs font-medium border transition ${on
                    ? 'bg-red-600/20 border-red-500 text-red-300'
                    : 'bg-gray-700 border-gray-600 text-gray-300 hover:bg-gray-600'}`}
                >
                  {on && '✕ '}{s}
                </button>
              )
            })}
          </div>
        )}
      </div>

      <div>
        <div className="flex items-center justify-between mb-2">
          <div className="text-sm font-medium text-gray-300">
            Ore permesse (Roma) {allowedHours === null && <span className="text-gray-500 text-xs ml-1">— tutte attive</span>}
          </div>
          <div className="flex gap-2">
            <button onClick={allHours} className="text-xs text-violet-400 hover:text-violet-300">Tutte</button>
            <button onClick={noHours} className="text-xs text-gray-400 hover:text-gray-300">Nessuna</button>
          </div>
        </div>
        <div className="grid grid-cols-12 gap-1.5">
          {HOURS.map(h => {
            const on = allowedHours === null || allowedHours.includes(h)
            return (
              <button
                key={h}
                onClick={() => toggleHour(h)}
                className={`px-2 py-1.5 rounded text-xs font-medium border transition ${on
                  ? 'bg-violet-600/20 border-violet-500 text-violet-200'
                  : 'bg-gray-700 border-gray-600 text-gray-500 hover:bg-gray-600'}`}
              >
                {String(h).padStart(2, '0')}
              </button>
            )
          })}
        </div>
        <p className="text-xs text-gray-500 mt-2">
          Solo i segnali ricevuti nelle ore selezionate verranno tradati. Le ore non selezionate finiscono nelle stats What-If.
        </p>
      </div>
    </div>
  )
}
