import { useEffect, useRef, useCallback } from 'react'
import toast from 'react-hot-toast'

export function useWebSocket(onMessage) {
  const ws = useRef(null)
  const onMessageRef = useRef(onMessage)
  onMessageRef.current = onMessage

  const connect = useCallback(() => {
    const protocol = window.location.protocol === 'https:' ? 'wss' : 'ws'
    const url = `${protocol}://${window.location.host}/ws`
    ws.current = new WebSocket(url)

    ws.current.onopen = () => console.log('[WS] Connesso')

    ws.current.onmessage = (e) => {
      try {
        const data = JSON.parse(e.data)
        onMessageRef.current?.(data)

        if (data.event === 'new_signal') {
          const s = data.data
          toast.success(
            `🎯 Nuovo segnale: ${s.direction?.toUpperCase()} ${s.symbol} @ ${s.entry_price ?? '?'}`,
            { duration: 8000 }
          )
        } else if (data.event === 'trade_update') {
          const u = data.data
          toast(`📊 ${u.symbol}: ${u.price_from} → ${u.price_to} — ${u.status_text}`, {
            icon: '📈',
            duration: 5000,
          })
        }
      } catch (err) {
        console.error('[WS] Parse error', err)
      }
    }

    ws.current.onerror = (e) => console.error('[WS] Errore', e)

    ws.current.onclose = () => {
      console.log('[WS] Disconnesso, riconnessione in 3s...')
      setTimeout(connect, 3000)
    }
  }, [])

  useEffect(() => {
    connect()
    return () => ws.current?.close()
  }, [connect])
}
