import { useState, useEffect, useRef } from 'react'
import toast from 'react-hot-toast'
import { PlusCircle, AlertTriangle, TrendingUp, TrendingDown, Calculator, Activity } from 'lucide-react'

/**
 * Apertura manuale di un trade: si scelgono simbolo, direzione, stop e target,
 * si preme "Calcola lotti" e il sistema mostra quanti lotti userebbe e quanto
 * si rischia. L'apertura e' a mercato e passa dallo stesso pipeline dei segnali
 * Telegram, quindi eredita guardia sul rischio, cap margine e guardie prop.
 */
export default function ManualTrade() {
  const [form, setForm] = useState({
    symbol: 'XAUUSD', direction: 'buy', stoploss: '', tp1: '', tp2: '', tp3: '',
    paper: false,
  })
  const [symbols, setSymbols] = useState([])
  const [prev, setPrev] = useState(null)
  const [calcolando, setCalcolando] = useState(false)
  const [sending, setSending] = useState(false)
  // Prezzo live del simbolo selezionato
  const [live, setLive] = useState({ price: null, at: null, prev: null })
  const prezzoPrec = useRef(null)

  const set = (k, v) => {
    setForm(f => ({ ...f, [k]: v }))
    setPrev(null)          // un input cambiato invalida il calcolo precedente
  }

  useEffect(() => {
    fetch('/api/manual-trade/symbols').then(r => r.json())
      .then(d => setSymbols(d.symbols || [])).catch(() => {})
  }, [])

  // Indicatore di prezzo il piu' reattivo possibile: aggiornamento ogni secondo
  // sul simbolo selezionato, con evidenza del movimento (verde sale, rosso scende).
  useEffect(() => {
    let vivo = true
    const tick = () => {
      fetch(`/api/price/${form.symbol}`).then(r => r.ok ? r.json() : null)
        .then(d => {
          if (!vivo || !d?.price) return
          setLive({ price: d.price, at: new Date(), prev: prezzoPrec.current })
          prezzoPrec.current = d.price
        }).catch(() => {})
    }
    prezzoPrec.current = null
    setLive({ price: null, at: null, prev: null })
    tick()
    const t = setInterval(tick, 1000)
    return () => { vivo = false; clearInterval(t) }
  }, [form.symbol])

  const payload = () => ({
    symbol: form.symbol.trim().toUpperCase(),
    direction: form.direction,
    stoploss: form.stoploss ? parseFloat(form.stoploss) : null,
    tp1: form.tp1 ? parseFloat(form.tp1) : null,
    tp2: form.tp2 ? parseFloat(form.tp2) : null,
    tp3: form.tp3 ? parseFloat(form.tp3) : null,
    paper: form.paper,
  })

  const calcola = async () => {
    const p = payload()
    if (!p.stoploss) { toast.error('Inserisci lo stop loss'); return }
    if (!(p.tp1 || p.tp2 || p.tp3)) { toast.error('Inserisci almeno un target'); return }
    setCalcolando(true)
    try {
      const d = await fetch('/api/manual-trade/preview', {
        method: 'POST', headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(p),
      }).then(r => r.json())
      setPrev(d)
    } catch (e) {
      toast.error(`Errore di rete: ${e.message}`)
    } finally {
      setCalcolando(false)
    }
  }

  const apri = async () => {
    const p = payload()
    const riga = `${p.symbol} ${p.direction.toUpperCase()} a mercato\n` +
      `Stop ${p.stoploss} · Target ${[p.tp1, p.tp2, p.tp3].filter(Boolean).join(' / ')}\n` +
      `${prev?.lotti_per_ticket} lotti x ${prev?.n_ticket} ticket · rischio ~${prev?.rischio_stimato}$`
    const titolo = form.paper
      ? 'Aprire questo trade in PAPER MODE? (simulato, nessun ordine reale)'
      : 'Aprire questo trade con denaro REALE?'
    if (!confirm(`${titolo}\n\n${riga}`)) return
    setSending(true)
    try {
      const r = await fetch('/api/manual-trade', {
        method: 'POST', headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(p),
      })
      const d = await r.json()
      if (r.ok && d.ok) {
        toast.success(d.paper
          ? `Paper trade #${d.signal_id} avviato (simulato, nessun ordine reale)`
          : `Trade #${d.signal_id} aperto: ${d.tickets?.length || 0} ticket`)
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
  const salito = live.prev != null && live.price != null && live.price > live.prev
  const sceso = live.prev != null && live.price != null && live.price < live.prev
  const decimali = form.symbol.includes('JPY') ? 3 : (live.price > 1000 ? 2 : 5)

  return (
    <div className="p-6 space-y-6 max-w-3xl">
      <h1 className="text-xl font-bold text-white flex items-center gap-2">
        <PlusCircle size={20} /> Apri trade manuale
      </h1>
      <p className="text-xs text-slate-500 -mt-4">
        Ingresso a mercato al prezzo corrente. I lotti li calcola il sistema dal rischio
        configurato e dalla distanza dello stop, divisi fra i target indicati.
      </p>

      {/* Prezzo live */}
      <div className="card p-4 flex items-center justify-between">
        <div className="flex items-center gap-2 text-xs text-slate-400 uppercase tracking-wider">
          <Activity size={14} className={live.price ? 'text-emerald-400' : 'text-slate-600'} />
          {form.symbol} · prezzo corrente
        </div>
        <div className="text-right">
          <div className={`text-2xl font-bold font-mono tabular-nums transition-colors ${
            salito ? 'text-emerald-400' : sceso ? 'text-rose-400' : 'text-white'}`}>
            {live.price != null ? live.price.toFixed(decimali) : '—'}
            {salito && <span className="text-sm ml-1">▲</span>}
            {sceso && <span className="text-sm ml-1">▼</span>}
          </div>
          <div className="text-[10px] text-slate-600">
            {live.at ? `aggiornato ${live.at.toLocaleTimeString('it-IT')}` : 'in attesa...'}
          </div>
        </div>
      </div>

      <div className="card p-5 space-y-4">
        <div className="grid grid-cols-2 gap-4">
          <div>
            <label className="block text-xs text-slate-400 mb-1">Simbolo</label>
            <select className={campo} value={form.symbol}
              onChange={e => set('symbol', e.target.value)}>
              {symbols.length === 0 && <option value={form.symbol}>{form.symbol}</option>}
              {symbols.map(s => (
                <option key={s.broker_symbol} value={s.simbolo} disabled={!s.disponibile}>
                  {s.simbolo}{s.simbolo !== s.broker_symbol ? ` (${s.broker_symbol})` : ''}
                  {!s.disponibile ? ' — non quotato' : ''}
                </option>
              ))}
            </select>
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
            onChange={e => set('stoploss', e.target.value)}
            placeholder={live.price ? (buy ? (live.price - 5).toFixed(decimali) : (live.price + 5).toFixed(decimali)) : 'es. 4290'} />
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

        <label className={`flex items-center gap-2 px-3 py-2 rounded-lg cursor-pointer border ${
          form.paper ? 'bg-violet-900/25 border-violet-600/50' : 'bg-slate-800/40 border-slate-700'}`}>
          <input type="checkbox" className="w-4 h-4 rounded" checked={form.paper}
            onChange={e => set('paper', e.target.checked)} />
          <span className="text-sm text-slate-200 font-medium">Paper mode</span>
          <span className="text-xs text-slate-500">
            nessun ordine reale: il trade viene simulato sui prezzi veri ed escluso dalle statistiche
          </span>
        </label>

        <button onClick={calcola} disabled={calcolando}
          className="w-full px-4 py-2.5 rounded-lg text-sm font-semibold bg-sky-600 hover:bg-sky-500 text-white disabled:opacity-50 flex items-center justify-center gap-2">
          <Calculator size={16} />
          {calcolando ? 'Calcolo...' : 'Calcola lotti'}
        </button>
      </div>

      {prev && (
        <div className={`card p-5 border ${prev.ok ? 'border-emerald-600/30' : 'border-rose-600/40'}`}>
          <h2 className="text-sm font-semibold text-slate-300 mb-3 uppercase tracking-wider">
            Calcolo del lotto
          </h2>
          <div className="grid grid-cols-2 gap-x-6 gap-y-2 text-sm">
            <Riga k="Prezzo usato nel calcolo" v={prev.prezzo_corrente} />
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
            className={`mt-4 w-full px-4 py-2.5 rounded-lg text-sm font-semibold text-white disabled:opacity-40 disabled:cursor-not-allowed ${
              form.paper ? 'bg-violet-600 hover:bg-violet-500' : 'bg-emerald-600 hover:bg-emerald-500'}`}>
            {sending ? 'Apertura in corso...'
              : form.paper
                ? `Simula ${form.direction.toUpperCase()} (paper)`
                : `Apri ${form.direction.toUpperCase()} a mercato`}
          </button>
          <p className="mt-2 text-[10px] text-slate-600 text-center">
            Il prezzo si muove: all'apertura il sistema ricalcola i lotti sul fill reale.
          </p>
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
