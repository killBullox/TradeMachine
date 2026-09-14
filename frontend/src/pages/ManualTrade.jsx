import { useState, useEffect, useCallback } from 'react'
import toast from 'react-hot-toast'
import { PlusCircle, AlertTriangle, TrendingUp, TrendingDown } from 'lucide-react'

/**
 * Apertura manuale di un trade: si scelgono direzione, stop e target, il
 * sistema calcola i lotti dal rischio configurato e apre a mercato passando
 * dallo stesso pipeline dei segnali Telegram (quindi eredita la guardia sul
 * rischio, il cap margine e le guardie prop).
 */
export default function ManualTrade() {
  const [form, setForm] = useState({
    symbol: 'XAUUSD', direction: 'buy', stoploss: '', tp1: '', tp2: '', tp3: '',
  })
  const [prev, setPrev] = useState(null)
  const [sending, setSending] = useState(false)

  const set = (k, v) => setForm(f => ({ ...f, [k]: v }))

  const payload = useCallback(() => ({
    symbol: form.symbol.trim().toUpperCase(),
    direction: form.direction,
    stoploss: parseFloat(form.stoploss),
    tp1: form.tp1 ? parseFloat(form.tp1) : null,
    tp2: form.tp2 ? parseFloat(form.tp2) : null,
    tp3: form.tp3 ? parseFloat(form.tp3) : null,
  }), [form])

  // Anteprima continua: appena stop e almeno un target sono validi, il
  // backend ricalcola lotti, rischio e problemi.
  useEffect(() => {
    const p = payload()
    if (!p.stoploss || !(p.tp1 || p.tp2 || p.tp3)) { setPrev(null); return }
    let vivo = true
    const t = setTimeout(() => {
      fetch('/api/manual-trade/preview', {
        method: 'POST', headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(p),
      }).then(r => r.json()).then(d => { if (vivo) setPrev(d) }).catch(() => {})
    }, 400)
    return () => { vivo = false; clearTimeout(t) }
  }, [payload])

  const apri = async () => {
    const p = payload()
    const riga = `${p.symbol} ${p.direction.toUpperCase()} a mercato\n` +
      `Stop ${p.stoploss} · Target ${[p.tp1, p.tp2, p.tp3].filter(Boolean).join(' / ')}\n` +
      `${prev?.lotti_per_ticket} lotti x ${prev?.n_ticket} ticket · rischio ~${prev?.rischio_stimato}$`
    if (!confirm(`Aprire questo trade?\n\n${riga}`)) return
    setSending(true)
    try {
      const r = await fetch('/api/manual-trade', {
        method: 'POST', headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(p),
      })
      const d = await r.json()
      if (r.ok && d.ok) {
        toast.success(`Trade #${d.signal_id} aperto: ${d.tickets?.length || 0} ticket`)
        setForm(f => ({ ...f, stoploss: '', tp1: '', tp2: '', tp3: '' }))
        setPrev(null)
      } else {
        toast.error(d.detail || 'Apertura fallita')
      }
    } catch (e) {
      toast.error(`Errore di rete: ${e.message}`)
    } finally {
      setSending(false)
    }
  }

  const buy = form.direction === 'buy'
  const campo = 'w-full bg-slate-800 border border-slate-700 rounded-lg px-3 py-2 text-sm text-white focus:outline-none focus:border-slate-500'

  return (
    <div className="p-6 space-y-6 max-w-3xl">
      <h1 className="text-xl font-bold text-white flex items-center gap-2">
        <PlusCircle size={20} /> Apri trade manuale
      </h1>
      <p className="text-xs text-slate-500 -mt-4">
        Ingresso a mercato al prezzo corrente. I lotti li calcola il sistema dal rischio
        configurato e dalla distanza dello stop, divisi fra i target indicati.
      </p>

      <div className="card p-5 space-y-4">
        <div className="grid grid-cols-2 gap-4">
          <div>
            <label className="block text-xs text-slate-400 mb-1">Simbolo</label>
            <input className={campo} value={form.symbol}
              onChange={e => set('symbol', e.target.value)} />
          </div>
          <div>
            <label className="block text-xs text-slate-400 mb-1">Operazione</label>
            <div className="flex gap-2">
              <button onClick={() => set('direction', 'buy')}
                className={`flex-1 px-3 py-2 rounded-lg text-sm font-semibold flex items-center justify-center gap-1 ${
                  buy ? 'bg-emerald-600 text-white' : 'bg-slate-800 text-slate-400 hover:bg-slate-700'}`}>
                <TrendingUp size={15} /> BUY
              </button>
              <button onClick={() => set('direction', 'sell')}
                className={`flex-1 px-3 py-2 rounded-lg text-sm font-semibold flex items-center justify-center gap-1 ${
                  !buy ? 'bg-rose-600 text-white' : 'bg-slate-800 text-slate-400 hover:bg-slate-700'}`}>
                <TrendingDown size={15} /> SELL
              </button>
            </div>
          </div>
        </div>

        <div>
          <label className="block text-xs text-slate-400 mb-1">
            Stop loss <span className="text-rose-400">*</span>
            <span className="text-slate-600 ml-1">
              ({buy ? 'sotto' : 'sopra'} il prezzo corrente)
            </span>
          </label>
          <input className={campo} type="number" step="any" value={form.stoploss}
            onChange={e => set('stoploss', e.target.value)} placeholder="es. 4402" />
        </div>

        <div className="grid grid-cols-3 gap-3">
          {['tp1', 'tp2', 'tp3'].map((k, i) => (
            <div key={k}>
              <label className="block text-xs text-slate-400 mb-1">
                Target {i + 1}{i === 0 && <span className="text-rose-400"> *</span>}
              </label>
              <input className={campo} type="number" step="any" value={form[k]}
                onChange={e => set(k, e.target.value)}
                placeholder={i === 0 ? 'obbligatorio' : 'opzionale'} />
            </div>
          ))}
        </div>
      </div>

      {prev && (
        <div className={`card p-5 border ${prev.ok ? 'border-emerald-600/30' : 'border-rose-600/40'}`}>
          <h2 className="text-sm font-semibold text-slate-300 mb-3 uppercase tracking-wider">
            Calcolo del lotto
          </h2>
          <div className="grid grid-cols-2 gap-x-6 gap-y-2 text-sm">
            <Riga k="Prezzo corrente" v={prev.prezzo_corrente} />
            <Riga k="Distanza dallo stop" v={prev.distanza_stop != null ? `${prev.distanza_stop}$` : null} />
            <Riga k="Lotti per ticket" v={prev.lotti_per_ticket} forte />
            <Riga k="Ticket" v={prev.n_ticket} />
            <Riga k="Lotti totali" v={prev.lotti_totali} forte />
            <Riga k="Lotto minimo broker" v={prev.lotto_minimo_broker} />
            <Riga k="Rischio stimato" v={prev.rischio_stimato != null ? `${prev.rischio_stimato}$` : null} forte />
            <Riga k="Rischio massimo" v={`${prev.rischio_massimo}$`} />
          </div>

          {prev.avvisi?.length > 0 && (
            <div className="mt-3 space-y-1">
              {prev.avvisi.map((a, i) => (
                <p key={i} className="text-xs text-amber-400 flex gap-1.5">
                  <AlertTriangle size={13} className="flex-shrink-0 mt-0.5" /><span>{a}</span>
                </p>
              ))}
            </div>
          )}
          {prev.errori?.length > 0 && (
            <div className="mt-3 space-y-1">
              {prev.errori.map((e, i) => (
                <p key={i} className="text-xs text-rose-400 flex gap-1.5">
                  <AlertTriangle size={13} className="flex-shrink-0 mt-0.5" /><span>{e}</span>
                </p>
              ))}
            </div>
          )}

          <button onClick={apri} disabled={!prev.ok || sending || !prev.lotti_per_ticket}
            className="mt-4 w-full px-4 py-2.5 rounded-lg text-sm font-semibold bg-emerald-600 hover:bg-emerald-500 text-white disabled:opacity-40 disabled:cursor-not-allowed">
            {sending ? 'Apertura in corso...' : `Apri ${form.direction.toUpperCase()} a mercato`}
          </button>
        </div>
      )}
    </div>
  )
}

function Riga({ k, v, forte }) {
  return (
    <div className="flex justify-between border-b border-slate-800 pb-1">
      <span className="text-slate-500">{k}</span>
      <span className={forte ? 'text-white font-semibold' : 'text-slate-300'}>{v ?? '—'}</span>
    </div>
  )
}
