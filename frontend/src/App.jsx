import { Routes, Route, NavLink } from 'react-router-dom'
import { LayoutDashboard, Bell, BookOpen, History, BarChart2, MessageSquare, RefreshCw, TrendingUp, Settings } from 'lucide-react'
import { useWebSocket } from './useWebSocket'
import { useState, useEffect } from 'react'
import { api } from './api'
import toast from 'react-hot-toast'

import Dashboard from './pages/Dashboard'
import OpenTrades from './pages/OpenTrades'
import Alerts from './pages/Alerts'
import Journal from './pages/Journal'
import HistoryPage from './pages/HistoryPage'
import Performance from './pages/Performance'
import Messages from './pages/Messages'
import Backup from './pages/Backup'

const NAV = [
  { to: '/',            label: 'Dashboard',   icon: LayoutDashboard },
  { to: '/open',        label: 'Trade Aperti', icon: TrendingUp },
  { to: '/alerts',      label: 'Alert',       icon: Bell },
  { to: '/journal',     label: 'Journal',     icon: BookOpen },
  { to: '/history',     label: 'Storico',     icon: History },
  { to: '/performance', label: 'Performance', icon: BarChart2 },
  { to: '/messages',    label: 'Messaggi',    icon: MessageSquare },
  { to: '/backup',      label: 'Impostazioni', icon: Settings },
]

function TelegramStatus() {
  const [status, setStatus] = useState('unknown')
  const [showAuth, setShowAuth] = useState(false)
  const [code, setCode] = useState('')
  const [sending, setSending] = useState(false)

  useEffect(() => {
    const check = () => {
      fetch('/api/telegram/status')
        .then(r => r.json())
        .then(d => setStatus(d.status))
        .catch(() => setStatus('error'))
    }
    check()
    const id = setInterval(check, 30000)
    return () => clearInterval(id)
  }, [])

  const requestCode = async () => {
    setSending(true)
    try {
      const r = await fetch('/api/telegram/auth/request', { method: 'POST' })
      const d = await r.json()
      if (d.ok) {
        setShowAuth(true)
        toast.success('Codice inviato al telefono!')
      } else {
        toast.error(d.error || 'Errore')
      }
    } catch { toast.error('Errore di rete') }
    setSending(false)
  }

  const verifyCode = async () => {
    if (!code.trim()) return
    setSending(true)
    try {
      const r = await fetch('/api/telegram/auth/verify', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ code: code.trim() }),
      })
      const d = await r.json()
      if (d.ok) {
        setStatus('connected')
        setShowAuth(false)
        setCode('')
        toast.success('Telegram riconnesso!')
      } else {
        toast.error(d.error || 'Codice errato')
      }
    } catch { toast.error('Errore di rete') }
    setSending(false)
  }

  const dot = status === 'connected' ? 'bg-emerald-400' :
              status === 'auth_needed' ? 'bg-amber-400' : 'bg-rose-400'
  const label = status === 'connected' ? 'Telegram OK' :
                status === 'auth_needed' ? 'Auth necessaria' : 'TG disconnesso'

  return (
    <div className="px-3 py-2 rounded-lg text-xs">
      <div className="flex items-center gap-2">
        <span className={`w-2 h-2 rounded-full ${dot}`} />
        <span className="text-slate-400">{label}</span>
        {status !== 'connected' && (
          <button onClick={requestCode} disabled={sending}
            className="ml-auto text-brand-400 hover:text-brand-300 text-[10px] font-medium">
            {sending ? '...' : 'Riconnetti'}
          </button>
        )}
      </div>
      {showAuth && (
        <div className="mt-2 flex gap-1">
          <input type="text" value={code} onChange={e => setCode(e.target.value)}
            placeholder="Codice" maxLength={6}
            className="flex-1 bg-slate-800 border border-slate-700 rounded px-2 py-1 text-white text-xs"
            onKeyDown={e => e.key === 'Enter' && verifyCode()} />
          <button onClick={verifyCode} disabled={sending}
            className="bg-brand-600 text-white px-2 py-1 rounded text-xs hover:bg-brand-500">
            OK
          </button>
        </div>
      )}
    </div>
  )
}


export default function App() {
  const [wsEvents, setWsEvents] = useState([])

  useWebSocket((event) => {
    setWsEvents(prev => [event, ...prev].slice(0, 50))
  })

  const handleReload = async () => {
    try {
      await api.reloadHistory(500)
      toast.success('Caricamento storico avviato!')
    } catch {
      toast.error('Errore nel reload')
    }
  }

  return (
    <div className="flex h-screen overflow-hidden">
      {/* Sidebar */}
      <aside className="w-56 flex-shrink-0 bg-slate-900 border-r border-slate-800 flex flex-col">
        <div className="px-4 py-5 border-b border-slate-800">
          <div className="flex items-center gap-2">
            <span className="text-2xl">📈</span>
            <div>
              <p className="font-bold text-sm text-white">Inner Circle</p>
              <p className="text-xs text-slate-400">TradeMachine</p>
            </div>
          </div>
        </div>

        <nav className="flex-1 p-3 space-y-1">
          {NAV.map(({ to, label, icon: Icon }) => (
            <NavLink
              key={to}
              to={to}
              end={to === '/'}
              className={({ isActive }) =>
                `flex items-center gap-3 px-3 py-2 rounded-lg text-sm transition-colors ${
                  isActive
                    ? 'bg-brand-600 text-white font-medium'
                    : 'text-slate-400 hover:text-slate-100 hover:bg-slate-800'
                }`
              }
            >
              <Icon size={16} />
              {label}
            </NavLink>
          ))}
        </nav>

        <div className="p-3 border-t border-slate-800 space-y-2">
          <TelegramStatus />
          <button
            onClick={handleReload}
            className="flex items-center gap-2 w-full px-3 py-2 rounded-lg text-sm text-slate-400 hover:text-slate-100 hover:bg-slate-800 transition-colors"
          >
            <RefreshCw size={14} />
            Ricarica storico
          </button>
        </div>
      </aside>

      {/* Main */}
      <main className="flex-1 overflow-auto">
        <Routes>
          <Route path="/"            element={<Dashboard wsEvents={wsEvents} />} />
          <Route path="/open"        element={<OpenTrades />} />
          <Route path="/alerts"      element={<Alerts />} />
          <Route path="/journal"     element={<Journal />} />
          <Route path="/history"     element={<HistoryPage />} />
          <Route path="/performance" element={<Performance />} />
          <Route path="/messages"    element={<Messages />} />
          <Route path="/backup"     element={<Backup />} />
        </Routes>
      </main>
    </div>
  )
}
