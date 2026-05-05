/**
 * Numero di TP raggiunti dal trade. Lo status del signal diventa 'tp1/tp2/tp3'
 * solo quando TUTTI i ticket sono chiusi: per i trade ancora aperti con TP1
 * gia' colpito (e ticket TP2/TP3 ancora attivi) lo status resta 'open'. Per
 * essere accurati anche in quel caso leggiamo il trade_log e contiamo gli
 * eventi 'ticket_closed' con reason 'TP'. Fallback al signal status quando
 * il trade_log e' vuoto o non popolato (es. record legacy).
 */
export function getTpHitCount(sig) {
  if (!sig) return 0
  try {
    const log = sig.trade_log ? JSON.parse(sig.trade_log) : []
    const tpClosed = log.filter(e => e.event === 'ticket_closed' && e.reason === 'TP')
    if (tpClosed.length > 0) return Math.min(3, tpClosed.length)
  } catch {}
  return sig.status === 'tp3' ? 3 : sig.status === 'tp2' ? 2 : sig.status === 'tp1' ? 1 : 0
}

/**
 * Barra di progressione di un trade aperto.
 * Mostra SL, BE (se applicato), Entry, TP1, TP2, TP3 + posizione del prezzo
 * corrente, e calcola lo stato testuale "<floor> → <target>" in base a:
 *   - direzione (BUY/SELL)
 *   - max TP raggiunto (da getTpHitCount: trade_log -> status come fallback)
 *   - BE applicato (vero se TP1 raggiunto, perche' il bot lo sposta in automatico)
 *   - posizione del prezzo rispetto alle barriere
 *
 * Esempi BUY:
 *   - TP1 non raggiunto, price tra entry e TP1 → "SL → TP1"
 *   - TP1 raggiunto, price tra TP1 e TP2 → "TP1 → TP2"
 *   - TP1 raggiunto, price tra entry e TP1 (retrace) → "BE → TP1"
 *   - TP2 raggiunto, price > TP2 → "TP2 → TP3"
 */
export default function TradeProgress({ sig, price, currentSl }) {
  const isBuy = (sig.direction || 'buy').toLowerCase() === 'buy'
  const entry = sig.actual_entry_price ?? sig.entry_price ?? sig.entry_price_high
  // Usa lo SL effettivo MT5 se passato (riflette BE / lock profit / SL move),
  // altrimenti fallback all'SL originale del segnale dal DB.
  const sl = currentSl ?? sig.stoploss
  const tp1 = sig.tp1
  const tp2 = sig.tp2
  const tp3 = sig.tp3

  const tpHit = getTpHitCount(sig)
  const beActive = tpHit >= 1 && entry != null

  // Costruisci elenco barriere con label e valore.
  const rawBarriers = []
  if (sl != null) rawBarriers.push({ label: 'SL', value: sl, kind: 'sl' })
  if (beActive) {
    rawBarriers.push({ label: 'BE', value: entry, kind: 'be' })
  } else if (entry != null) {
    rawBarriers.push({ label: 'Entry', value: entry, kind: 'entry' })
  }
  if (tp1 != null) rawBarriers.push({ label: 'TP1', value: tp1, kind: tpHit >= 1 ? 'tp_hit' : 'tp' })
  if (tp2 != null) rawBarriers.push({ label: 'TP2', value: tp2, kind: tpHit >= 2 ? 'tp_hit' : 'tp' })
  if (tp3 != null) rawBarriers.push({ label: 'TP3', value: tp3, kind: tpHit >= 3 ? 'tp_hit' : 'tp' })

  // Fondi barriere con stesso valore (es. quando il live SL coincide col BE
  // dopo TP1 hit). Priorita' di rendering: tp_hit > be > tp > entry > sl.
  // Per la label visibile teniamo la combinata 'SL/BE' tipo, ma per il
  // colore/marker usiamo la priorita' piu' visibile.
  const kindPriority = { sl: 0, entry: 1, tp: 2, be: 3, tp_hit: 4 }
  const merged = new Map()
  for (const b of rawBarriers) {
    // arrotonda a 8 decimali per evitare problemi di float
    const k = Number(b.value).toFixed(8)
    const ex = merged.get(k)
    if (!ex) {
      merged.set(k, { ...b, labels: [b.label] })
    } else {
      ex.labels.push(b.label)
      if (kindPriority[b.kind] > kindPriority[ex.kind]) {
        ex.kind = b.kind
      }
    }
  }
  const barriers = Array.from(merged.values()).map(b => ({
    ...b,
    label: b.labels.length > 1 ? b.labels.join('/') : b.labels[0],
  }))

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
      <div className="relative h-12 mt-4">
        {/* Linea base */}
        <div className="absolute top-1/2 left-0 right-0 h-1 bg-slate-700 rounded -translate-y-1/2" />

        {/* Marker delle barriere */}
        {barriers.map((b, i) => {
          const pct = toPct(b.value)
          return (
            <div
              key={`${b.label}-${i}`}
              className="absolute top-1/2 -translate-y-1/2 -translate-x-1/2"
              style={{ left: `${pct}%` }}
            >
              <div className={`w-1.5 h-5 ${colorOfKind(b.kind)} rounded-sm`} />
              {b.kind === 'tp_hit' && (
                <span className="absolute -top-4 left-1/2 -translate-x-1/2 text-emerald-400 text-sm">✓</span>
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
            <div className="w-4 h-4 rounded-full bg-white border-2 border-slate-900 shadow-md" />
          </div>
        )}
      </div>

      {/* Etichette delle barriere sotto. Le etichette agli estremi (sinistra
          o destra) verrebbero tagliate dalla card se centrate sopra il marker:
          adjusto translateX in base alla posizione percentuale. */}
      <div className="relative h-9">
        {barriers.map((b, i) => {
          const pct = toPct(b.value)
          const tx = pct < 8 ? '0%' : pct > 92 ? '-100%' : '-50%'
          const align = pct < 8 ? 'text-left' : pct > 92 ? 'text-right' : 'text-center'
          return (
            <div
              key={`label-${b.label}-${i}`}
              className={`absolute text-[15px] font-mono font-semibold ${labelColor(b.kind)}`}
              style={{ left: `${pct}%`, transform: `translateX(${tx})` }}
            >
              <div className={`${align} leading-tight`}>{b.label}</div>
              <div className={`${align} text-slate-400 text-[14px] leading-tight font-normal`}>{fmt(b.value)}</div>
            </div>
          )
        })}
      </div>
    </div>
  )
}
