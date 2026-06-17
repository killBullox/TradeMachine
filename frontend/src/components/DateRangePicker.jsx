import { useState, useEffect, useRef } from 'react'
import { format, startOfMonth, endOfMonth, eachDayOfInterval, isSameDay, isSameMonth,
         addMonths, subMonths, startOfWeek, endOfWeek, isToday, isWithinInterval,
         subDays, startOfYear } from 'date-fns'
import { it } from 'date-fns/locale'
import { Calendar, X } from 'lucide-react'

/**
 * Date range picker custom: due click sul calendario selezionano dal/al.
 * Output: stringhe YYYY-MM-DD (date only, niente time). Il backend interpreta
 * come giorni in fuso Roma (start-of-day / end-of-day).
 */
export default function DateRangePicker({ dateFrom, dateTo, onChange }) {
  const [open, setOpen] = useState(false)
  const [viewMonth, setViewMonth] = useState(new Date())
  const [pickingEnd, setPickingEnd] = useState(false)
  const ref = useRef(null)

  // Chiudi al click esterno
  useEffect(() => {
    if (!open) return
    const onClick = (e) => { if (ref.current && !ref.current.contains(e.target)) setOpen(false) }
    document.addEventListener('mousedown', onClick)
    return () => document.removeEventListener('mousedown', onClick)
  }, [open])

  // Parsing string YYYY-MM-DD → Date (mezzanotte locale)
  const parseDate = (s) => {
    if (!s) return null
    const [y, m, d] = s.split('-').map(Number)
    if (!y || !m || !d) return null
    return new Date(y, m - 1, d)
  }
  const fromDate = parseDate(dateFrom)
  const toDate = parseDate(dateTo)

  const fmtDate = (d) => format(d, 'yyyy-MM-dd')
  const fmtLabel = (d) => format(d, 'dd MMM yy', { locale: it })

  const handleClickDay = (day) => {
    if (!fromDate || pickingEnd === false) {
      // Primo click → set from, prepara per end
      onChange(fmtDate(day), null)
      setPickingEnd(true)
    } else {
      // Secondo click → set to (se prima di from, swap)
      if (day < fromDate) {
        onChange(fmtDate(day), fmtDate(fromDate))
      } else {
        onChange(fmtDate(fromDate), fmtDate(day))
      }
      setPickingEnd(false)
      setOpen(false)
    }
  }

  const setPreset = (preset) => {
    const today = new Date()
    let from = null, to = null
    if (preset === 'today')       { from = today; to = today }
    else if (preset === 'yesterday') { from = subDays(today, 1); to = subDays(today, 1) }
    else if (preset === '7d')     { from = subDays(today, 6); to = today }
    else if (preset === '30d')    { from = subDays(today, 29); to = today }
    else if (preset === 'month')  { from = startOfMonth(today); to = today }
    else if (preset === 'lastMonth') { const lm = subMonths(today, 1); from = startOfMonth(lm); to = endOfMonth(lm) }
    else if (preset === 'year')   { from = startOfYear(today); to = today }
    if (from && to) {
      onChange(fmtDate(from), fmtDate(to))
      setOpen(false)
      setPickingEnd(false)
    }
  }

  const clear = () => {
    onChange(null, null)
    setOpen(false)
    setPickingEnd(false)
  }

  // Genera la grid del mese
  const monthStart = startOfMonth(viewMonth)
  const monthEnd = endOfMonth(viewMonth)
  const gridStart = startOfWeek(monthStart, { weekStartsOn: 1 })  // lunedì
  const gridEnd = endOfWeek(monthEnd, { weekStartsOn: 1 })
  const days = eachDayOfInterval({ start: gridStart, end: gridEnd })

  // Label trigger
  let triggerLabel = 'Seleziona periodo'
  if (fromDate && toDate) {
    triggerLabel = isSameDay(fromDate, toDate) ? fmtLabel(fromDate) : `${fmtLabel(fromDate)} → ${fmtLabel(toDate)}`
  } else if (fromDate) {
    triggerLabel = `Da ${fmtLabel(fromDate)} →`
  }

  return (
    <div ref={ref} className="relative">
      <button onClick={() => setOpen(!open)}
        className="flex items-center gap-2 bg-slate-800 hover:bg-slate-700 border border-slate-600 rounded px-3 py-1.5 text-sm text-slate-200">
        <Calendar size={14} className="text-brand-400" />
        <span>{triggerLabel}</span>
        {(fromDate || toDate) && (
          <span onClick={(e) => { e.stopPropagation(); clear() }}
            className="ml-1 text-slate-500 hover:text-rose-400 cursor-pointer">
            <X size={14} />
          </span>
        )}
      </button>

      {open && (
        <div className="absolute right-0 mt-2 bg-slate-900 border border-slate-700 rounded-lg shadow-2xl z-50 p-3 w-[340px]">
          {/* Preset chips */}
          <div className="flex flex-wrap gap-1 mb-3 pb-3 border-b border-slate-700">
            <button onClick={() => setPreset('today')} className="text-xs bg-slate-800 hover:bg-brand-700 text-slate-300 px-2 py-1 rounded">Oggi</button>
            <button onClick={() => setPreset('yesterday')} className="text-xs bg-slate-800 hover:bg-brand-700 text-slate-300 px-2 py-1 rounded">Ieri</button>
            <button onClick={() => setPreset('7d')} className="text-xs bg-slate-800 hover:bg-brand-700 text-slate-300 px-2 py-1 rounded">Ultimi 7gg</button>
            <button onClick={() => setPreset('30d')} className="text-xs bg-slate-800 hover:bg-brand-700 text-slate-300 px-2 py-1 rounded">Ultimi 30gg</button>
            <button onClick={() => setPreset('month')} className="text-xs bg-slate-800 hover:bg-brand-700 text-slate-300 px-2 py-1 rounded">Mese corrente</button>
            <button onClick={() => setPreset('lastMonth')} className="text-xs bg-slate-800 hover:bg-brand-700 text-slate-300 px-2 py-1 rounded">Mese scorso</button>
            <button onClick={() => setPreset('year')} className="text-xs bg-slate-800 hover:bg-brand-700 text-slate-300 px-2 py-1 rounded">YTD</button>
          </div>

          {/* Header mese */}
          <div className="flex items-center justify-between mb-2">
            <button onClick={() => setViewMonth(subMonths(viewMonth, 1))} className="text-slate-400 hover:text-white px-2">←</button>
            <span className="text-white font-semibold text-sm capitalize">
              {format(viewMonth, 'MMMM yyyy', { locale: it })}
            </span>
            <button onClick={() => setViewMonth(addMonths(viewMonth, 1))} className="text-slate-400 hover:text-white px-2">→</button>
          </div>

          {/* Giorni settimana */}
          <div className="grid grid-cols-7 gap-1 mb-1">
            {['L','M','M','G','V','S','D'].map((d,i) => (
              <div key={i} className="text-center text-xs text-slate-500 py-1">{d}</div>
            ))}
          </div>

          {/* Calendario */}
          <div className="grid grid-cols-7 gap-1">
            {days.map((day, i) => {
              const isCurMonth = isSameMonth(day, viewMonth)
              const isStart = fromDate && isSameDay(day, fromDate)
              const isEnd = toDate && isSameDay(day, toDate)
              const inRange = fromDate && toDate && isWithinInterval(day, { start: fromDate, end: toDate })
              const isPickingEndPreview = pickingEnd && fromDate && !toDate && day > fromDate
              return (
                <button key={i} onClick={() => handleClickDay(day)}
                  disabled={!isCurMonth}
                  className={`
                    aspect-square text-xs rounded transition-colors
                    ${!isCurMonth ? 'text-slate-700' : 'text-slate-200'}
                    ${isStart || isEnd ? 'bg-brand-600 text-white font-bold' : ''}
                    ${inRange && !isStart && !isEnd ? 'bg-brand-900/50' : ''}
                    ${isToday(day) && !isStart && !isEnd ? 'ring-1 ring-amber-400' : ''}
                    ${isCurMonth && !isStart && !isEnd && !inRange ? 'hover:bg-slate-700' : ''}
                  `}>
                  {format(day, 'd')}
                </button>
              )
            })}
          </div>

          {/* Footer */}
          <div className="mt-3 pt-3 border-t border-slate-700 flex items-center justify-between text-xs">
            <button onClick={clear} className="text-slate-500 hover:text-rose-400">Pulisci</button>
            <span className="text-slate-500">
              {pickingEnd ? 'Seleziona fine →' : 'Seleziona inizio'}
            </span>
          </div>
        </div>
      )}
    </div>
  )
}
