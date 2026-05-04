/**
 * Barra di progressione di un trade aperto.
 * Mostra SL, BE (se applicato), Entry, TP1, TP2, TP3 + posizione del prezzo
 * corrente, e calcola lo stato testuale "<floor> → <target>" in base a:
 *   - direzione (BUY/SELL)
 *   - max TP raggiunto (dal sig.status)
 *   - BE applicato (vero se status >= tp1, perché il bot lo sposta in automatico)
 *   - posizione del prezzo rispetto alle barriere
 *
 * Esempi BUY:
 *   - status=open, price tra entry e TP1 → "SL → TP1"
 *   - status=tp1, price tra TP1 e TP2 → "TP1 → TP2"
 *   - status=tp1, price tra entry e TP1 (retrace) → "BE → TP1"
 *   - status=tp2, price > TP2 → "TP2 → TP3"
 */
export default function TradeProgress({ sig, price }) {
  const isBuy = (sig.direction || 'buy').toLowerCase() === 'buy'
  const entry = sig.actual_entry_price ?? sig.entry_price ?? sig.entry_price_high
  const sl = sig.stoploss
  const tp1 = sig.tp1
  const tp2 = sig.tp2
  const tp3 = sig.tp3

  const tpHit = sig.status === 'tp3' ? 3 : sig.status === 'tp2' ? 2 : sig.status === 'tp1' ? 1 : 0
  const beActive = tpHit >= 1 && entry != null

  // Costruisci elenco barriere con label e valore
  const barriers = []
  if (sl != null) barriers.push({ label: 'SL', value: sl, kind: 'sl' })
  if (beActive) barriers.push({ label: 'BE', value: entry, kind: 'be' })
  if (entry != null) barriers.push({ label: 'Entry', value: entry, kind: 'entry' })
  if (tp1 != null) barriers.push({ label: 'TP1', value: tp1, kind: tpHit >= 1 ? 'tp_hit' : 'tp' })
  if (tp2 != null) barriers.push({ label: 'TP2', value: tp2, kind: tpHit >= 2 ? 'tp_hit' : 'tp' })
  if (tp3 != null) barriers.push({ label: 'TP3', value: tp3, kind: tpHit >= 3 ? 'tp_hit' : 'tp' })

  // Ordina per valore lungo la direzione favorevole
  barriers.sort((a, b) => (isBuy ? a.value - b.value : b.value - a.value))

  // Calcola stato testuale (floor/target rispetto al prezzo corrente)
  let stateText = '—'
  if (price != null && barriers.length > 0) {
    let floor = null
    let target = null
    for (const b of barriers) {
      // Skip 'Entry' come barrier per il calcolo (è informativo, non si difende)
      if (b.kind === 'entry' && beActive) continue
      const isBelowOrEq = isBuy ? b.value <= price : b.value >= price
      if (isBelowOrEq) floor = b
      else if (target == null) { target = b; break }
    }
    if (target == null && floor) stateText = `${floor.label} ✓`
    else if (floor == null && target) stateText = `< ${target.label}`
    else if (floor && target) stateText = `${floor.label} → ${target.label}`
  }

  // Range della barra: dal SL al TP3 (o estremi disponibili)
  const allValues = barriers.map(b => b.value)
  if (price != null) allValues.push(price)
  const minV = Math.min(...allValues)
  const maxV = Math.max(...allValues)
  const span = maxV - minV || 1
  // pos% = 0 sulla parte UNFAVORITA (per BUY = SL a sinistra; per SELL = SL a destra)
  // Ovvero in tutti i casi: SL è a sinistra, TP3 è a destra → ordiniamo i marker per "lato favorevole verso destra"
  const toPct = (v) => isBuy
    ? ((v - minV) / span) * 100
    : ((maxV - v) / span) * 100

  const decimals = sig.symbol?.includes('JPY') ? 3 : sig.symbol?.includes('BTC') ? 0 : sig.symbol?.includes('XAU') ? 2 : 5
  const fmt = (v) => v != null ? Number(v).toFixed(decimals) : '—'

  const colorOfKind = (k) => {
    if (k === 'sl') return 'bg-rose-500'
    if (k === 'be') return 'bg-amber-400'
    if (k === 'entry') return 'bg-slate-400'
    if (k === 'tp_hit') return 'bg-emerald-500'
    if (k === 'tp') return 'bg-emerald-700'
    return 'bg-slate-500'
  }
  const labelColor = (k) => {
    if (k === 'sl') return 'text-rose-400'
    if (k === 'be') return 'text-amber-300'
    if (k === 'entry') return 'text-slate-400'
    if (k === 'tp_hit') return 'text-emerald-300'
    if (k === 'tp') return 'text-emerald-500'
    return 'text-slate-400'
  }

  return (
    <div className="space-y-2">
      <div className="flex items-center justify-between">
        <span className="text-xs uppercase tracking-wide text-slate-500">Stato</span>
        <span className="text-sm font-semibold text-white">{stateText}</span>
      </div>

      {/* Barra */}
      <div className="relative h-8 mt-3">
        {/* Linea base */}
        <div className="absolute top-1/2 left-0 right-0 h-0.5 bg-slate-700 -translate-y-1/2" />

        {/* Marker delle barriere */}
        {barriers.map((b, i) => {
          const pct = toPct(b.value)
          return (
            <div
              key={`${b.label}-${i}`}
              className="absolute top-1/2 -translate-y-1/2 -translate-x-1/2"
              style={{ left: `${pct}%` }}
            >
              <div className={`w-1 h-3 ${colorOfKind(b.kind)}`} />
              {b.kind === 'tp_hit' && (
                <span className="absolute -top-3 left-1/2 -translate-x-1/2 text-emerald-400 text-xs">✓</span>
              )}
            </div>
          )
        })}

        {/* Marker prezzo corrente */}
        {price != null && (
          <div
            className="absolute top-1/2 -translate-y-1/2 -translate-x-1/2 z-10"
            style={{ left: `${Math.min(100, Math.max(0, toPct(price)))}%` }}
            title={`Prezzo attuale: ${fmt(price)}`}
          >
            <div className="w-3 h-3 rounded-full bg-white border border-slate-900 shadow-md" />
          </div>
        )}
      </div>

      {/* Etichette delle barriere sotto */}
      <div className="relative h-5">
        {barriers.map((b, i) => {
          const pct = toPct(b.value)
          return (
            <div
              key={`label-${b.label}-${i}`}
              className={`absolute -translate-x-1/2 text-[10px] font-mono ${labelColor(b.kind)}`}
              style={{ left: `${pct}%` }}
            >
              <div className="text-center leading-none">{b.label}</div>
              <div className="text-center text-slate-500 text-[9px] leading-tight">{fmt(b.value)}</div>
            </div>
          )
        })}
      </div>
    </div>
  )
}
