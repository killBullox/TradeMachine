import { useEffect, useState } from 'react'
import { api } from '../api'
import { format } from 'date-fns'
import { it } from 'date-fns/locale'

const TYPE_COLORS = {
  signal:    'bg-emerald-900/40 text-emerald-400 border-emerald-700',
  update:    'bg-blue-900/40 text-blue-400 border-blue-700',
  level:     'bg-violet-900/40 text-violet-400 border-violet-700',
  watchlist: 'bg-amber-900/40 text-amber-400 border-amber-700',
  other:     'bg-slate-800/40 text-slate-500 border-slate-700',
}

export default function Messages() {
  const [messages, setMessages] = useState([])
  const [filter, setFilter] = useState('all')

  useEffect(() => {
    api.getMessages(300).then(setMessages)
  }, [])

  const filtered = filter === 'all' ? messages : messages.filter(m => m.type === filter)

  return (
    <div className="p-6 space-y-5">
      <h1 className="text-xl font-bold text-white">Messaggi raw</h1>

      <div className="flex gap-2 flex-wrap">
        {['all', 'signal', 'update', 'level', 'watchlist', 'other'].map(f => (
          <button
            key={f}
            onClick={() => setFilter(f)}
            className={`text-xs px-3 py-1 rounded-full border transition-colors capitalize ${
              filter === f
                ? 'bg-brand-600 border-brand-500 text-white'
                : 'border-slate-700 text-slate-400 hover:text-white'
            }`}
          >
            {f === 'all' ? 'Tutti' : f}
          </button>
        ))}
        <span className="ml-auto text-xs text-slate-500 self-center">{filtered.length} messaggi</span>
      </div>

      <div className="space-y-2">
        {filtered.map(msg => (
          <div key={msg.id} className="card flex gap-3">
            <div className="flex-shrink-0 w-20">
              <span className={`badge-status border text-xs ${TYPE_COLORS[msg.type] || TYPE_COLORS.other}`}>
                {msg.type}
              </span>
            </div>
            <div className="flex-1 min-w-0">
              <p className="text-xs text-slate-300 whitespace-pre-wrap break-words">{msg.text}</p>
            </div>
            <div className="flex-shrink-0 text-xs text-slate-600 text-right whitespace-nowrap">
              {msg.created_at ? format(new Date(msg.created_at), 'dd/MM HH:mm', { locale: it }) : ''}
            </div>
          </div>
        ))}
        {filtered.length === 0 && <p className="text-slate-500 text-sm">Nessun messaggio</p>}
      </div>
    </div>
  )
}
