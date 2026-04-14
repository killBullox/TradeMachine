import { useState, useEffect } from 'react'
import { api } from '../api'
import toast from 'react-hot-toast'
import { format } from 'date-fns'
import { it } from 'date-fns/locale'

function fmtTs(ts) {
  if (!ts) return '—'
  try {
    const normalized = ts.endsWith('Z') || ts.includes('+') ? ts : ts + 'Z'
    return format(new Date(normalized), 'dd/MM/yyyy HH:mm', { locale: it })
  } catch { return ts }
}

export default function Backup() {
  const [restorePoints, setRestorePoints] = useState([])
  const [loading, setLoading] = useState(true)
  const [pinModal, setPinModal] = useState(null) // { action, id?, label, extra? }
  const [pin, setPin] = useState('')
  const [busy, setBusy] = useState(false)
  const [mt5Accounts, setMt5Accounts] = useState(null)
  const [showAddAccount, setShowAddAccount] = useState(false)
  const [newAcc, setNewAcc] = useState({ login: '', server: 'XM.COM-MT5', label: '', isDemo: true })

  const load = async () => {
    try {
      const [rp, acc] = await Promise.all([
        api.getRestorePoints(),
        api.getMt5Accounts().catch(() => null),
      ])
      setRestorePoints(rp)
      setMt5Accounts(acc)
    } catch (e) {
      toast.error('Errore caricamento punti di ripristino')
    }
    setLoading(false)
  }

  useEffect(() => { load() }, [])

  const openPinModal = (action, id = null, label = '', extra = null) => {
    setPin('')
    setPinModal({ action, id, label, extra })
  }

  const executeAction = async () => {
    if (!pin) return
    setBusy(true)
    try {
      if (pinModal.action === 'archive') {
        const res = await api.archiveTrades(pin)
        if (res.ok) {
          toast.success(`${res.archived} trade archiviati!`)
        } else {
          toast.error(res.message || 'Errore')
        }
      } else if (pinModal.action === 'restore') {
        const res = await api.restoreTrades(pinModal.id, pin)
        if (res.ok) {
          toast.success(`${res.restored} trade ripristinati!`)
        }
      } else if (pinModal.action === 'delete') {
        const res = await api.deleteRestorePoint(pinModal.id, pin)
        if (res.ok) {
          toast.success('Punto di ripristino eliminato')
        }
      } else if (pinModal.action === 'switch_account') {
        const { login, server } = pinModal.extra
        const res = await api.switchMt5Account(login, server, pin)
        toast.success(`Account cambiato: ${res.login} (${res.name})`)
      } else if (pinModal.action === 'add_account') {
        const { login, server, label, isDemo } = pinModal.extra
        await api.addMt5Account(login, server, label, isDemo, pin)
        toast.success(`Account ${login} aggiunto!`)
        setShowAddAccount(false)
        setNewAcc({ login: '', server: 'XM.COM-MT5', label: '', isDemo: true })
      } else if (pinModal.action === 'remove_account') {
        await api.removeMt5Account(pinModal.id, pin)
        toast.success('Account rimosso')
      }
      setPinModal(null)
      setPin('')
      load()
    } catch (e) {
      if (e.message.includes('403')) {
        toast.error('PIN non valido!')
      } else {
        toast.error('Errore: ' + e.message)
      }
    }
    setBusy(false)
  }

  return (
    <div className="p-6 max-w-4xl mx-auto">
      <h1 className="text-2xl font-bold text-white mb-6">Impostazioni</h1>

      {/* Riavvio Server */}
      <div className="card p-6 mb-8 border border-amber-600/30">
        <h2 className="text-lg font-semibold text-white mb-2">Riavvio Server</h2>
        <p className="text-slate-400 text-sm mb-4">Riavvia il backend se l'app non risponde. Il server si riavvia automaticamente in ~10 secondi.</p>
        <button
          className="px-4 py-2 bg-amber-600 hover:bg-amber-500 text-white rounded-lg font-medium"
          onClick={async () => {
            if (!confirm('Riavviare il server?')) return
            try {
              await fetch('/api/restart', { method: 'POST' })
            } catch {}
            alert('Riavvio in corso. Ricarica la pagina tra 10 secondi.')
          }}
        >
          Riavvia Server
        </button>
      </div>

      {/* Account MT5 */}
      {mt5Accounts && (
        <div className="card p-6 mb-8">
          <h2 className="text-lg font-semibold text-white mb-4">Account MT5</h2>
          <div className="space-y-3">
            {mt5Accounts.available.map(acc => {
              const isActive = mt5Accounts.current && mt5Accounts.current.login === acc.login
              return (
                <div
                  key={acc.login}
                  className={`flex items-center justify-between p-4 rounded-lg border ${
                    isActive
                      ? 'bg-emerald-900/20 border-emerald-600/40'
                      : 'bg-slate-800/50 border-slate-700'
                  }`}
                >
                  <div>
                    <div className="flex items-center gap-2">
                      <span className="text-white font-medium">{acc.label}</span>
                      {acc.demo ? (
                        <span className="px-2 py-0.5 bg-blue-600/30 text-blue-300 text-xs rounded-full">DEMO</span>
                      ) : (
                        <span className="px-2 py-0.5 bg-amber-600/30 text-amber-300 text-xs rounded-full">REAL</span>
                      )}
                      {isActive && (
                        <span className="px-2 py-0.5 bg-emerald-600/30 text-emerald-300 text-xs rounded-full">ATTIVO</span>
                      )}
                    </div>
                    <p className="text-sm text-slate-400 mt-1">
                      Login: {acc.login} &middot; Server: {acc.server}
                      {isActive && mt5Accounts.current && ` \u00B7 Balance: $${mt5Accounts.current.balance?.toLocaleString()}`}
                    </p>
                  </div>
                  <div className="flex gap-2 ml-4 flex-shrink-0">
                    {!isActive && (
                      <button
                        onClick={() => openPinModal('switch_account', null, `Cambia a ${acc.label} (${acc.login})`, { login: acc.login, server: acc.server })}
                        className={`px-4 py-2 text-sm rounded-lg font-medium transition-colors ${
                          acc.demo
                            ? 'bg-blue-600 hover:bg-blue-500 text-white'
                            : 'bg-amber-600 hover:bg-amber-500 text-white'
                        }`}
                      >
                        Seleziona
                      </button>
                    )}
                    {!acc.is_default && !isActive && (
                      <button
                        onClick={() => openPinModal('remove_account', acc.id, `Rimuovi account ${acc.label} (${acc.login})`)}
                        className="px-3 py-2 text-sm rounded-lg bg-red-600/20 hover:bg-red-600/40 text-red-400 transition-colors"
                      >
                        Rimuovi
                      </button>
                    )}
                  </div>
                </div>
              )
            })}
          </div>

          {/* Aggiungi account */}
          {!showAddAccount ? (
            <button
              onClick={() => setShowAddAccount(true)}
              className="mt-4 px-4 py-2 text-sm bg-slate-700 hover:bg-slate-600 text-slate-300 rounded-lg transition-colors"
            >
              + Aggiungi account
            </button>
          ) : (
            <div className="mt-4 p-4 bg-slate-800/50 border border-slate-700 rounded-lg">
              <h3 className="text-white font-medium mb-3">Nuovo account MT5</h3>
              <div className="grid grid-cols-2 gap-3 mb-3">
                <div>
                  <label className="text-xs text-slate-400 block mb-1">Login (numero)</label>
                  <input
                    type="number"
                    value={newAcc.login}
                    onChange={e => setNewAcc({ ...newAcc, login: e.target.value })}
                    placeholder="12345678"
                    className="w-full px-3 py-2 bg-slate-900 border border-slate-600 rounded text-white text-sm focus:outline-none focus:border-brand-500"
                  />
                </div>
                <div>
                  <label className="text-xs text-slate-400 block mb-1">Server</label>
                  <input
                    type="text"
                    value={newAcc.server}
                    onChange={e => setNewAcc({ ...newAcc, server: e.target.value })}
                    placeholder="XM.COM-MT5"
                    className="w-full px-3 py-2 bg-slate-900 border border-slate-600 rounded text-white text-sm focus:outline-none focus:border-brand-500"
                  />
                </div>
                <div>
                  <label className="text-xs text-slate-400 block mb-1">Etichetta</label>
                  <input
                    type="text"
                    value={newAcc.label}
                    onChange={e => setNewAcc({ ...newAcc, label: e.target.value })}
                    placeholder="Il mio conto"
                    className="w-full px-3 py-2 bg-slate-900 border border-slate-600 rounded text-white text-sm focus:outline-none focus:border-brand-500"
                  />
                </div>
                <div>
                  <label className="text-xs text-slate-400 block mb-1">Tipo</label>
                  <select
                    value={newAcc.isDemo ? 'demo' : 'real'}
                    onChange={e => setNewAcc({ ...newAcc, isDemo: e.target.value === 'demo' })}
                    className="w-full px-3 py-2 bg-slate-900 border border-slate-600 rounded text-white text-sm focus:outline-none focus:border-brand-500"
                  >
                    <option value="demo">Demo</option>
                    <option value="real">Real</option>
                  </select>
                </div>
              </div>
              <div className="flex gap-2">
                <button
                  onClick={() => { setShowAddAccount(false); setNewAcc({ login: '', server: 'XM.COM-MT5', label: '', isDemo: true }) }}
                  className="px-4 py-2 text-sm bg-slate-700 hover:bg-slate-600 text-slate-300 rounded-lg transition-colors"
                >
                  Annulla
                </button>
                <button
                  onClick={() => {
                    if (!newAcc.login || !newAcc.label) { toast.error('Compila login e etichetta'); return }
                    openPinModal('add_account', null, `Aggiungi account ${newAcc.label} (${newAcc.login})`, {
                      login: parseInt(newAcc.login), server: newAcc.server, label: newAcc.label, isDemo: newAcc.isDemo,
                    })
                  }}
                  className="px-4 py-2 text-sm bg-brand-600 hover:bg-brand-500 text-white rounded-lg font-medium transition-colors"
                >
                  Aggiungi
                </button>
              </div>
            </div>
          )}
        </div>
      )}

      {/* Archivazione */}
      <div className="card p-6 mb-8">
        <h2 className="text-lg font-semibold text-white mb-2">Archivia Trade</h2>
        <p className="text-sm text-slate-400 mb-4">
          Archivia tutti i trade chiusi (TP, SL, annullati). I trade archiviati non compariranno
          nelle statistiche, nello storico e nella dashboard. Viene creato automaticamente un
          punto di ripristino.
        </p>
        <button
          onClick={() => openPinModal('archive', null, 'Archivia tutti i trade chiusi')}
          className="px-4 py-2 bg-amber-600 hover:bg-amber-500 text-white rounded-lg font-medium transition-colors"
        >
          Archivia trade chiusi
        </button>
      </div>

      {/* Punti di ripristino */}
      <div className="card p-6">
        <h2 className="text-lg font-semibold text-white mb-4">Punti di Ripristino</h2>

        {loading ? (
          <p className="text-slate-400">Caricamento...</p>
        ) : restorePoints.length === 0 ? (
          <p className="text-slate-500 text-sm">Nessun punto di ripristino disponibile.</p>
        ) : (
          <div className="space-y-3">
            {restorePoints.map(rp => (
              <div key={rp.id} className="bg-slate-800/50 border border-slate-700 rounded-lg p-4">
                <div className="flex items-start justify-between">
                  <div>
                    <h3 className="text-white font-medium">{rp.name}</h3>
                    <p className="text-sm text-slate-400 mt-1">{rp.description}</p>
                    <p className="text-xs text-slate-500 mt-1">
                      {fmtTs(rp.created_at)} — {rp.signals_count} segnali
                    </p>
                  </div>
                  <div className="flex gap-2 ml-4 flex-shrink-0">
                    <button
                      onClick={() => openPinModal('restore', rp.id, `Ripristina "${rp.name}"`)}
                      className="px-3 py-1.5 bg-emerald-600 hover:bg-emerald-500 text-white text-sm rounded-lg transition-colors"
                    >
                      Ripristina
                    </button>
                    <button
                      onClick={() => openPinModal('delete', rp.id, `Elimina "${rp.name}"`)}
                      className="px-3 py-1.5 bg-red-600/30 hover:bg-red-600/50 text-red-400 text-sm rounded-lg transition-colors"
                    >
                      Elimina
                    </button>
                  </div>
                </div>
              </div>
            ))}
          </div>
        )}
      </div>

      {/* Modal PIN */}
      {pinModal && (
        <div className="fixed inset-0 bg-black/60 flex items-center justify-center z-50">
          <div className="bg-slate-800 border border-slate-700 rounded-xl p-6 w-96 shadow-xl">
            <h3 className="text-white font-semibold text-lg mb-1">Conferma operazione</h3>
            <p className="text-sm text-slate-400 mb-4">{pinModal.label}</p>
            <label className="text-sm text-slate-300 block mb-1">Inserisci PIN di conferma</label>
            <input
              type="password"
              value={pin}
              onChange={e => setPin(e.target.value)}
              onKeyDown={e => e.key === 'Enter' && executeAction()}
              placeholder="PIN"
              autoFocus
              className="w-full px-3 py-2 bg-slate-900 border border-slate-600 rounded-lg text-white text-center text-lg tracking-widest mb-4 focus:outline-none focus:border-brand-500"
            />
            <div className="flex gap-3">
              <button
                onClick={() => { setPinModal(null); setPin('') }}
                className="flex-1 px-4 py-2 bg-slate-700 hover:bg-slate-600 text-slate-300 rounded-lg transition-colors"
              >
                Annulla
              </button>
              <button
                onClick={executeAction}
                disabled={!pin || busy}
                className="flex-1 px-4 py-2 bg-brand-600 hover:bg-brand-500 disabled:opacity-50 text-white rounded-lg font-medium transition-colors"
              >
                {busy ? 'Attendere...' : 'Conferma'}
              </button>
            </div>
          </div>
        </div>
      )}
    </div>
  )
}
