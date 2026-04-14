import { useEffect, useState } from 'react'
import { api } from '../api'
import { format } from 'date-fns'
import { it } from 'date-fns/locale'
import toast from 'react-hot-toast'
import { Trash2 } from 'lucide-react'

const EMOTIONS = ['', '😎 Disciplinato', '😰 Ansioso', '🤑 Avido', '😤 Frustrato', '😌 Neutrale', '🎯 Focalizzato']

export default function Journal() {
  const [entries, setEntries] = useState([])
  const [form, setForm] = useState({ title: '', content: '', trade_result: '', emotion: '' })
  const [submitting, setSubmitting] = useState(false)

  const load = () => api.getJournal().then(setEntries)
  useEffect(() => { load() }, [])

  const handleSubmit = async (e) => {
    e.preventDefault()
    if (!form.content.trim()) return toast.error('Inserisci il contenuto')
    setSubmitting(true)
    try {
      await api.createJournal({
        ...form,
        trade_result: form.trade_result ? parseFloat(form.trade_result) : null,
      })
      setForm({ title: '', content: '', trade_result: '', emotion: '' })
      load()
      toast.success('Entry salvata!')
    } catch {
      toast.error('Errore nel salvataggio')
    } finally {
      setSubmitting(false)
    }
  }

  const handleDelete = async (id) => {
    if (!confirm('Eliminare questa entry?')) return
    await api.deleteJournal(id)
    load()
  }

  return (
    <div className="p-6 space-y-6">
      <h1 className="text-xl font-bold text-white">Journal</h1>

      {/* Form nuova entry */}
      <form onSubmit={handleSubmit} className="card space-y-3">
        <h2 className="text-sm font-semibold text-slate-300">Nuova entry</h2>
        <input
          value={form.title}
          onChange={e => setForm(p => ({ ...p, title: e.target.value }))}
          placeholder="Titolo (opzionale)"
          className="w-full bg-slate-800 border border-slate-700 rounded px-3 py-2 text-sm text-slate-300 placeholder-slate-600 focus:outline-none focus:border-brand-500"
        />
        <textarea
          value={form.content}
          onChange={e => setForm(p => ({ ...p, content: e.target.value }))}
          placeholder="Descrivi il trade, la tua analisi, cosa hai imparato..."
          rows={4}
          className="w-full bg-slate-800 border border-slate-700 rounded px-3 py-2 text-sm text-slate-300 placeholder-slate-600 focus:outline-none focus:border-brand-500 resize-none"
        />
        <div className="flex gap-3">
          <input
            value={form.trade_result}
            onChange={e => setForm(p => ({ ...p, trade_result: e.target.value }))}
            placeholder="P&L ($)"
            type="number"
            step="0.01"
            className="w-32 bg-slate-800 border border-slate-700 rounded px-3 py-2 text-sm text-slate-300 placeholder-slate-600 focus:outline-none focus:border-brand-500"
          />
          <select
            value={form.emotion}
            onChange={e => setForm(p => ({ ...p, emotion: e.target.value }))}
            className="flex-1 bg-slate-800 border border-slate-700 rounded px-3 py-2 text-sm text-slate-300 focus:outline-none focus:border-brand-500"
          >
            {EMOTIONS.map(em => <option key={em} value={em}>{em || '— Emozione —'}</option>)}
          </select>
          <button
            type="submit"
            disabled={submitting}
            className="px-5 py-2 bg-brand-600 hover:bg-brand-500 disabled:opacity-50 rounded text-sm font-medium transition-colors"
          >
            Salva
          </button>
        </div>
      </form>

      {/* Lista entries */}
      <div className="space-y-3">
        {entries.length === 0 && <p className="text-slate-500 text-sm">Nessuna entry nel journal</p>}
        {entries.map(entry => (
          <div key={entry.id} className="card space-y-2">
            <div className="flex items-start justify-between">
              <div>
                {entry.title && <p className="font-semibold text-white">{entry.title}</p>}
                <p className="text-xs text-slate-500">
                  {entry.created_at
                    ? format(new Date(entry.created_at), 'dd MMM yyyy HH:mm', { locale: it })
                    : ''}
                  {entry.emotion && <span className="ml-2">{entry.emotion}</span>}
                </p>
              </div>
              <div className="flex items-center gap-3">
                {entry.trade_result != null && (
                  <span className={`font-mono font-bold text-sm ${entry.trade_result >= 0 ? 'text-emerald-400' : 'text-rose-400'}`}>
                    {entry.trade_result >= 0 ? '+' : ''}{entry.trade_result.toFixed(2)}$
                  </span>
                )}
                <button onClick={() => handleDelete(entry.id)} className="text-slate-600 hover:text-rose-400 transition-colors">
                  <Trash2 size={14} />
                </button>
              </div>
            </div>
            <p className="text-sm text-slate-300 whitespace-pre-wrap">{entry.content}</p>
          </div>
        ))}
      </div>
    </div>
  )
}
