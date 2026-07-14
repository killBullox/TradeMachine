import { useState, useEffect, useCallback } from 'react'
import { api } from '../api'
import toast from 'react-hot-toast'
import { format } from 'date-fns'
import { it } from 'date-fns/locale'
import FilterPanel from '../components/FilterPanel'
import NewsFilterPanel from '../components/NewsFilterPanel'

function RiskPanel() {
  const [settings, setSettings] = useState(null)
  const [saving, setSaving] = useState(false)
  const [mt5Info, setMt5Info] = useState(null) // {balance, login, prop_mode}

  useEffect(() => {
    api.getRiskSettings().then(setSettings)
    // Balance MT5 live + info account attivo (per suggerimento sync)
    Promise.all([
      fetch('/api/mt5/status').then(r => r.json()).catch(() => null),
      fetch('/api/mt5/accounts').then(r => r.json()).catch(() => null),
    ]).then(([status, accounts]) => {
      const activeAcc = accounts?.available?.find(a => a.is_active)
      const balance = status?.account?.balance ?? null
      setMt5Info({
        balance,
        login: status?.account?.login ?? null,
        prop_mode: activeAcc?.prop_mode ?? false,
        label: activeAcc?.label ?? null,
      })
      // Sincronizza settings.account_size = balance live, cosi' il display
      // "Max risk per trade" e il save mandano il valore corretto
      if (balance && balance > 0) {
        setSettings(s => s ? {...s, account_size: Math.round(balance)} : s)
      }
    })
  }, [])

  if (!settings) return null

  async function save() {
    setSaving(true)
    try {
      await api.saveRiskSettings(settings)
      toast.success('Impostazioni rischio salvate e ricalcolate!')
    } catch {
      toast.error('Errore nel salvataggio')
    } finally {
      setSaving(false)
    }
  }

  return (
    <div className="card p-6 mb-8">
      <h2 className="text-lg font-semibold text-white mb-4">Risk Management</h2>
      <div className="grid grid-cols-1 sm:grid-cols-2 gap-4">
        <div className="space-y-1">
          <label className="text-xs text-slate-400">Account ($) - live MT5</label>
          <input type="number"
            value={mt5Info?.balance != null ? Math.round(mt5Info.balance) : settings.account_size}
            readOnly
            disabled
            className="w-full px-3 py-2 text-sm bg-slate-800/60 border border-slate-700 rounded-lg text-slate-300 cursor-not-allowed" />
          <p className="text-xs text-slate-500">
            {mt5Info?.balance != null
              ? `Balance MT5 live (${mt5Info.label}). Il calcolo del rischio segue automaticamente balance + rischio %.`
              : 'MT5 non raggiungibile: fallback su ultimo valore salvato.'}
          </p>
        </div>
        <div className="space-y-1">
          <label className="text-xs text-slate-400">Rischio % / trade</label>
          <input type="number" step="0.1" value={settings.risk_per_trade_pct}
            onChange={e => setSettings(s => ({...s, risk_per_trade_pct: +e.target.value}))}
            className="w-full px-3 py-2 text-sm bg-slate-800 border border-slate-700 rounded-lg text-white focus:outline-none focus:border-brand-500" />
        </div>
        <div className="space-y-1">
          <div className="flex items-center gap-2 mb-1">
            <input type="checkbox" checked={settings.use_fixed_usd}
              onChange={e => setSettings(s => ({...s, use_fixed_usd: e.target.checked}))}
              className="w-4 h-4 rounded" id="use-fixed-settings" />
            <label htmlFor="use-fixed-settings" className="text-xs text-slate-400">Importo fisso ($)</label>
          </div>
          <input type="number" value={settings.risk_per_trade_usd ?? ''}
            disabled={!settings.use_fixed_usd}
            onChange={e => setSettings(s => ({...s, risk_per_trade_usd: +e.target.value}))}
            className="w-full px-3 py-2 text-sm bg-slate-800 border border-slate-700 rounded-lg text-white disabled:opacity-40 focus:outline-none focus:border-brand-500" />
        </div>
        <div className="space-y-1">
          <label className="text-xs text-slate-400" title="Pip di tolleranza per entrare a mercato quando il prezzo è di poco fuori dal range del segnale (Buy Near è approssimativo). Default 3 pip = 0.30 su gold, 0.0003 su forex.">
            Tolleranza entry (pip)
          </label>
          <input type="number" step="0.5" min="0" value={settings.entry_tolerance_pips ?? 3}
            onChange={e => setSettings(s => ({...s, entry_tolerance_pips: +e.target.value}))}
            className="w-full px-3 py-2 text-sm bg-slate-800 border border-slate-700 rounded-lg text-white focus:outline-none focus:border-brand-500" />
          <p className="text-xs text-slate-500">≈ ${((settings.entry_tolerance_pips ?? 3) * 0.10).toFixed(2)} su gold</p>
        </div>
        <div className="space-y-1">
          <div className="flex items-center gap-2 mb-1">
            <input type="checkbox"
              checked={!!settings.trail_stop_enabled}
              onChange={e => setSettings(s => ({...s, trail_stop_enabled: e.target.checked}))}
              className="w-4 h-4 rounded" id="trail-stop-toggle" />
            <label htmlFor="trail-stop-toggle" className="text-xs text-slate-400"
              title="Default per i nuovi trade. ON: il bot sposta auto lo SL ai TP raggiunti (BE+1pip su TP1, TP1+1pip su TP2). OFF: il bot non muove lo SL in automatico, gestione manuale via Lock profit. Override per-trade dalla card.">
              Trail the stop (default)
            </label>
          </div>
          <p className="text-xs text-slate-500">
            {settings.trail_stop_enabled
              ? 'ON: TP1 hit → SL = BE+1pip · TP2 hit → SL = TP1+1pip (auto)'
              : 'OFF: il bot non muove lo SL in automatico, solo manuale'}
          </p>
        </div>
        <div className="space-y-1">
          <label className="text-xs text-slate-400" title="Cap di margine massimo che ogni nuovo trade puo' utilizzare, in % del free margin disponibile. Esempio 50%: se hai $10k liberi, ogni trade puo' bloccare al massimo $5k di margine. Se la position size calcolata dal rischio supera questo cap, viene ridotta (rischio reale < target). Default 50%.">
            Max margin per trade (%)
          </label>
          <input type="number" step="5" min="5" max="100" value={settings.max_margin_pct_per_trade ?? 50}
            onChange={e => setSettings(s => ({...s, max_margin_pct_per_trade: +e.target.value}))}
            className="w-full px-3 py-2 text-sm bg-slate-800 border border-slate-700 rounded-lg text-white focus:outline-none focus:border-brand-500" />
          <p className="text-xs text-slate-500">Cap del margine usato per trade (% del free margin)</p>
        </div>
        <div className="space-y-1">
          <div className="flex items-center gap-2 mb-1">
            <input type="checkbox" checked={settings.news_filter_enabled !== false}
              onChange={e => setSettings(s => ({...s, news_filter_enabled: e.target.checked}))}
              className="w-4 h-4 rounded" id="news-filter-toggle" />
            <label htmlFor="news-filter-toggle" className="text-xs text-slate-400"
              title="Blocca nuovi ingressi -10/+5 min attorno alle news high-impact, cancella i pending a -10 e chiude le posizioni aperte a -5 (eventi con flag Flatten). Post-mortem #570.">
              News filter (blocco + flatten)
            </label>
          </div>
          <div className="flex items-center gap-2">
            <input type="checkbox" checked={settings.friday_flatten_enabled !== false}
              onChange={e => setSettings(s => ({...s, friday_flatten_enabled: e.target.checked}))}
              className="w-4 h-4 rounded" id="friday-flatten-toggle" />
            <label htmlFor="friday-flatten-toggle" className="text-xs text-slate-400"
              title="Chiude tutte le posizioni il venerdì alle 22:45 Roma contro i gap di apertura del lunedì.">
              Weekend flatten (ven 22:45)
            </label>
          </div>
        </div>
        <div className="flex items-end">
          <div className="w-full">
            <div className="text-xs text-slate-500 mb-2">
              Max risk per trade: <span className="text-white font-mono text-sm font-semibold">
                {settings.use_fixed_usd && settings.risk_per_trade_usd
                  ? `$${settings.risk_per_trade_usd}`
                  : `$${((settings.account_size * settings.risk_per_trade_pct) / 100).toFixed(0)}`}
              </span>
            </div>
            <button onClick={save} disabled={saving}
              className="w-full px-4 py-2 text-sm bg-brand-600 hover:bg-brand-500 rounded-lg text-white font-medium disabled:opacity-50 transition-colors">
              {saving ? 'Salvando...' : 'Salva & Ricalcola'}
            </button>
          </div>
        </div>
      </div>
    </div>
  )
}

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
  const [newAcc, setNewAcc] = useState({ login: '', server: 'XM.COM-MT5', label: '', isDemo: true, mt5Path: '', broker: '' })
  const [editAcc, setEditAcc] = useState(null) // {id, label, server, mt5_path, broker, is_demo}
  const [propAcc, setPropAcc] = useState(null) // {id, label, prop_mode, daily_dd_limit_usd, ...}
  const [propSaving, setPropSaving] = useState(false)

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
        const { login, server, label, isDemo, mt5Path, broker } = pinModal.extra
        await api.addMt5Account(login, server, label, isDemo, pin, mt5Path, broker)
        toast.success(`Account ${login} aggiunto!`)
        setShowAddAccount(false)
        setNewAcc({ login: '', server: 'XM.COM-MT5', label: '', isDemo: true, mt5Path: '', broker: '' })
      } else if (pinModal.action === 'update_account') {
        await api.updateMt5Account(pinModal.id, pin, pinModal.extra)
        toast.success('Account aggiornato')
        setEditAcc(null)
      } else if (pinModal.action === 'remove_account') {
        await api.removeMt5Account(pinModal.id, pin)
        toast.success('Account rimosso')
      } else if (pinModal.action === 'update_prop') {
        setPropSaving(true)
        const params = new URLSearchParams({ pin })
        for (const [k, v] of Object.entries(pinModal.extra)) {
          if (v !== null && v !== undefined && v !== '') params.set(k, String(v))
        }
        const res = await fetch(`/api/mt5/prop-settings/${pinModal.id}?${params}`, { method: 'PATCH' })
        if (!res.ok) throw new Error(`status ${res.status}`)
        toast.success('Prop settings aggiornate')
        setPropAcc(null)
        setPropSaving(false)
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

      {/* Risk Management */}
      <RiskPanel />

      {/* Filtri Segnali */}
      <FilterPanel />

      {/* News Filter */}
      <NewsFilterPanel />

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
            {(() => {
              const activeAcc = mt5Accounts.available.find(a => a.is_active)
              const terminalLogin = mt5Accounts.current?.login
              const mismatch = activeAcc && terminalLogin && activeAcc.login !== terminalLogin
              return mismatch && (
                <div className="mb-3 p-3 bg-red-900/30 border border-red-600/50 rounded-lg text-sm">
                  <div className="font-semibold text-red-300">[!] Mismatch account MT5 vs TradeMachine</div>
                  <div className="text-red-200 mt-1">
                    TradeMachine sta usando <b>{activeAcc.label} ({activeAcc.login})</b> ma il terminale MT5 e loggato su <b>{terminalLogin}</b>.
                    Clicca <b>Seleziona</b> sul conto giusto per allineare tutto (switch effettivo + persistenza DB).
                  </div>
                </div>
              )
            })()}
            {mt5Accounts.available.map(acc => {
              // isActive = attivo nel DB TM (chi usera per piazzare i trade)
              const isActive = !!acc.is_active
              // Il terminale MT5 potrebbe essere loggato su un account diverso
              const isTerminalConnected = mt5Accounts.current && mt5Accounts.current.login === acc.login
              return (
                <div
                  key={acc.login}
                  className={`p-3 rounded-lg border ${
                    isActive
                      ? 'bg-emerald-900/20 border-emerald-600/40'
                      : 'bg-slate-800/50 border-slate-700'
                  }`}
                >
                  <div className="flex items-center justify-between gap-3 flex-wrap">
                    {/* LEFT: label + badges + login riga singola */}
                    <div className="min-w-0 flex-1">
                      <div className="flex items-center gap-2 flex-wrap">
                        <span className="text-white font-semibold text-sm">{acc.label}</span>
                        <span className="text-xs text-slate-500 font-mono">#{acc.login}</span>
                        {acc.demo
                          ? <span className="px-1.5 py-0.5 bg-blue-600/25 text-blue-300 text-[10px] font-semibold rounded">DEMO</span>
                          : <span className="px-1.5 py-0.5 bg-amber-600/25 text-amber-300 text-[10px] font-semibold rounded">REAL</span>}
                        {isActive && (
                          <span className="px-1.5 py-0.5 bg-emerald-600/30 text-emerald-300 text-[10px] font-semibold rounded" title="TradeMachine usa questo account per piazzare i trade">ATTIVO</span>
                        )}
                        {isTerminalConnected && !isActive && (
                          <span className="px-1.5 py-0.5 bg-amber-600/30 text-amber-300 text-[10px] font-semibold rounded" title="Loggato nel terminale MT5 ma non usato da TM">TERM</span>
                        )}
                        {acc.prop_mode && (
                          <span className="px-1.5 py-0.5 bg-violet-600/30 text-violet-300 text-[10px] font-semibold rounded" title="Prop mode (daily DD, trailing, coerenza)">PROP</span>
                        )}
                      </div>
                      <div className="text-xs text-slate-400 mt-1 truncate" title={`${acc.server}${acc.broker ? ' · ' + acc.broker : ''}${acc.mt5_path ? ' · ' + acc.mt5_path : ''}`}>
                        {acc.server}
                        {acc.broker && <> <span className="text-slate-600">·</span> {acc.broker}</>}
                        {isTerminalConnected && mt5Accounts.current && <> <span className="text-slate-600">·</span> <span className="text-emerald-400">${mt5Accounts.current.balance?.toLocaleString()}</span></>}
                      </div>
                    </div>
                    {/* RIGHT: actions compatte */}
                    <div className="flex gap-1.5 flex-shrink-0">
                      <button
                        onClick={() => openPinModal('switch_account', null, isActive ? `Ri-attiva ${acc.label} (${acc.login})` : `Cambia a ${acc.label} (${acc.login})`, { login: acc.login, server: acc.server })}
                        title={isActive ? "Ri-attiva: forza switch_account (aggiorna .env / ri-init MT5)" : "Attiva questo account"}
                        className={`px-3 py-1.5 text-xs rounded-md font-medium transition-colors ${
                          isActive
                            ? 'bg-slate-700 hover:bg-slate-600 text-slate-200'
                            : acc.demo
                              ? 'bg-blue-600 hover:bg-blue-500 text-white'
                              : 'bg-amber-600 hover:bg-amber-500 text-white'
                        }`}
                      >
                        {isActive ? 'Ri-attiva' : 'Seleziona'}
                      </button>
                      <button
                        onClick={() => setEditAcc({ id: acc.id, label: acc.label, server: acc.server, mt5_path: acc.mt5_path || '', broker: acc.broker || '', is_demo: acc.demo })}
                        className="px-3 py-1.5 text-xs rounded-md bg-slate-700 hover:bg-slate-600 text-slate-300"
                        title="Modifica label, server, path, broker"
                      >
                        Modifica
                      </button>
                      <button
                        onClick={() => setPropAcc({
                          id: acc.id, label: acc.label,
                          prop_mode: !!acc.prop_mode,
                          daily_dd_limit_usd: acc.daily_dd_limit_usd || '',
                          daily_dd_warning_usd: acc.daily_dd_warning_usd || '',
                          max_total_dd_usd: acc.max_total_dd_usd || '',
                          consistency_threshold_pct: acc.consistency_threshold_pct ?? 30,
                          max_concurrent_trades: acc.max_concurrent_trades || '',
                        })}
                        className={`px-3 py-1.5 text-xs rounded-md transition-colors ${
                          acc.prop_mode ? 'bg-violet-700 hover:bg-violet-600 text-white' : 'bg-slate-700 hover:bg-slate-600 text-slate-300'
                        }`}
                        title="Configura prop mode (Funded Elite / FTMO / ecc.)"
                      >
                        Prop
                      </button>
                      {!acc.is_default && !isActive && (
                        <button
                          onClick={() => openPinModal('remove_account', acc.id, `Rimuovi account ${acc.label} (${acc.login})`)}
                          className="px-3 py-1.5 text-xs rounded-md bg-red-600/20 hover:bg-red-600/40 text-red-400"
                          title="Rimuovi questo account dal DB"
                        >
                          Rimuovi
                        </button>
                      )}
                    </div>
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
                <div>
                  <label className="text-xs text-slate-400 block mb-1" title="Tag broker (es. xm, avatrade) — popolato in signals.broker per filtri storico">Broker tag</label>
                  <input
                    type="text"
                    value={newAcc.broker}
                    onChange={e => setNewAcc({ ...newAcc, broker: e.target.value })}
                    placeholder="xm / avatrade"
                    className="w-full px-3 py-2 bg-slate-900 border border-slate-600 rounded text-white text-sm focus:outline-none focus:border-brand-500"
                  />
                </div>
                <div className="col-span-2">
                  <label className="text-xs text-slate-400 block mb-1" title="Path completo a terminal64.exe del terminale dedicato a questo account. Vuoto = default da .env (XM TradeMachine).">Path terminale MT5 (opzionale)</label>
                  <input
                    type="text"
                    value={newAcc.mt5Path}
                    onChange={e => setNewAcc({ ...newAcc, mt5Path: e.target.value })}
                    placeholder="C:\\AvaTrade MT5\\terminal64.exe"
                    className="w-full px-3 py-2 bg-slate-900 border border-slate-600 rounded text-white text-sm focus:outline-none focus:border-brand-500 font-mono"
                  />
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
                      mt5Path: newAcc.mt5Path, broker: newAcc.broker,
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

      {/* Edit account modal */}
      {editAcc && (
        <div className="fixed inset-0 bg-black/60 flex items-center justify-center z-50">
          <div className="bg-slate-800 border border-slate-700 rounded-xl p-6 w-[480px] shadow-xl">
            <h3 className="text-white font-semibold text-lg mb-4">Modifica account MT5</h3>
            <div className="space-y-3">
              <div>
                <label className="text-xs text-slate-400 block mb-1">Etichetta</label>
                <input type="text" value={editAcc.label} onChange={e => setEditAcc({...editAcc, label: e.target.value})}
                  className="w-full px-3 py-2 bg-slate-900 border border-slate-600 rounded text-white text-sm" />
              </div>
              <div>
                <label className="text-xs text-slate-400 block mb-1">Server</label>
                <input type="text" value={editAcc.server} onChange={e => setEditAcc({...editAcc, server: e.target.value})}
                  className="w-full px-3 py-2 bg-slate-900 border border-slate-600 rounded text-white text-sm" />
              </div>
              <div>
                <label className="text-xs text-slate-400 block mb-1">Broker tag</label>
                <input type="text" value={editAcc.broker} onChange={e => setEditAcc({...editAcc, broker: e.target.value})}
                  placeholder="xm / avatrade"
                  className="w-full px-3 py-2 bg-slate-900 border border-slate-600 rounded text-white text-sm" />
              </div>
              <div>
                <label className="text-xs text-slate-400 block mb-1">Path terminale MT5</label>
                <input type="text" value={editAcc.mt5_path} onChange={e => setEditAcc({...editAcc, mt5_path: e.target.value})}
                  placeholder="C:\\AvaTrade MT5\\terminal64.exe"
                  className="w-full px-3 py-2 bg-slate-900 border border-slate-600 rounded text-white text-sm font-mono" />
              </div>
              <div>
                <label className="text-xs text-slate-400 block mb-1">Tipo</label>
                <select value={editAcc.is_demo ? 'demo' : 'real'} onChange={e => setEditAcc({...editAcc, is_demo: e.target.value === 'demo'})}
                  className="w-full px-3 py-2 bg-slate-900 border border-slate-600 rounded text-white text-sm">
                  <option value="demo">Demo</option>
                  <option value="real">Real</option>
                </select>
              </div>
            </div>
            <div className="flex gap-2 mt-5">
              <button onClick={() => setEditAcc(null)}
                className="flex-1 px-4 py-2 bg-slate-700 hover:bg-slate-600 text-slate-300 rounded-lg">Annulla</button>
              <button onClick={() => openPinModal('update_account', editAcc.id, `Aggiorna ${editAcc.label}`, {
                  label: editAcc.label, server: editAcc.server, mt5_path: editAcc.mt5_path,
                  broker: editAcc.broker, is_demo: editAcc.is_demo,
                })}
                className="flex-1 px-4 py-2 bg-brand-600 hover:bg-brand-500 text-white rounded-lg font-medium">Salva</button>
            </div>
          </div>
        </div>
      )}

      {/* Modal Prop Settings */}
      {propAcc && (
        <div className="fixed inset-0 bg-black/60 flex items-center justify-center z-50">
          <div className="bg-slate-800 border border-violet-700 rounded-xl p-6 w-[480px] max-w-[95vw] shadow-xl">
            <h3 className="text-white font-semibold text-lg mb-1 flex items-center gap-2">
              🛡️ Prop Mode — {propAcc.label}
            </h3>
            <p className="text-xs text-slate-400 mb-4">
              Attiva e configura le guardie per i conti prop (Funded Elite, FTMO, ecc.).
              Lascia OFF per account normali (es. Avatrade) — niente cambia.
            </p>
            <div className="space-y-3">
              <label className="flex items-center gap-2 text-sm">
                <input type="checkbox" checked={propAcc.prop_mode}
                  onChange={e => setPropAcc(p => ({...p, prop_mode: e.target.checked}))} />
                <span className="text-white">Attiva Prop Mode su questo account</span>
              </label>
              <div className={propAcc.prop_mode ? '' : 'opacity-40 pointer-events-none'}>
                <div className="grid grid-cols-2 gap-3">
                  <div>
                    <label className="text-xs text-slate-400 block mb-1" title="Soglia oltre la quale il bot smette di piazzare nuovi trade per la giornata (in $). Esempio Funded Elite 25K: $500 (= $250 buffer su limite hard $750).">
                      Daily DD limit ($)
                    </label>
                    <input type="number" value={propAcc.daily_dd_limit_usd}
                      onChange={e => setPropAcc(p => ({...p, daily_dd_limit_usd: e.target.value}))}
                      placeholder="es. 500"
                      className="w-full bg-slate-900 border border-slate-600 rounded px-2 py-1.5 text-white text-sm" />
                  </div>
                  <div>
                    <label className="text-xs text-slate-400 block mb-1" title="Soglia warning (solo log/UI, no block).">
                      Daily DD warning ($)
                    </label>
                    <input type="number" value={propAcc.daily_dd_warning_usd}
                      onChange={e => setPropAcc(p => ({...p, daily_dd_warning_usd: e.target.value}))}
                      placeholder="es. 300"
                      className="w-full bg-slate-900 border border-slate-600 rounded px-2 py-1.5 text-white text-sm" />
                  </div>
                  <div>
                    <label className="text-xs text-slate-400 block mb-1" title="Limite totale trailing equity (regola 'equita' inseguita' dei prop).">
                      Max total DD ($)
                    </label>
                    <input type="number" value={propAcc.max_total_dd_usd}
                      onChange={e => setPropAcc(p => ({...p, max_total_dd_usd: e.target.value}))}
                      placeholder="es. 2000 (8% di 25k)"
                      className="w-full bg-slate-900 border border-slate-600 rounded px-2 py-1.5 text-white text-sm" />
                  </div>
                  <div>
                    <label className="text-xs text-slate-400 block mb-1" title="Soglia regola coerenza: max-day P&L / total P&L. Tipico 30%.">
                      Coerenza %
                    </label>
                    <input type="number" value={propAcc.consistency_threshold_pct}
                      onChange={e => setPropAcc(p => ({...p, consistency_threshold_pct: e.target.value}))}
                      placeholder="30"
                      className="w-full bg-slate-900 border border-slate-600 rounded px-2 py-1.5 text-white text-sm" />
                  </div>
                  <div className="col-span-2">
                    <label className="text-xs text-slate-400 block mb-1" title="Numero massimo di trade contemporaneamente aperti.">
                      Max trade contemporanei
                    </label>
                    <input type="number" value={propAcc.max_concurrent_trades}
                      onChange={e => setPropAcc(p => ({...p, max_concurrent_trades: e.target.value}))}
                      placeholder="es. 3"
                      className="w-full bg-slate-900 border border-slate-600 rounded px-2 py-1.5 text-white text-sm" />
                  </div>
                </div>
                <p className="text-xs text-amber-400/70 mt-3">
                  💡 Suggerimento Funded Elite 25K: daily 500, warning 300, total DD 2000, coerenza 30%, max trade 3.
                </p>
              </div>
            </div>
            <div className="flex gap-2 mt-5">
              <button onClick={() => setPropAcc(null)}
                className="flex-1 px-4 py-2 bg-slate-700 hover:bg-slate-600 text-slate-300 rounded-lg">Annulla</button>
              <button onClick={() => openPinModal('update_prop', propAcc.id, `Aggiorna prop settings ${propAcc.label}`, {
                  prop_mode: propAcc.prop_mode,
                  daily_dd_limit_usd: propAcc.daily_dd_limit_usd === '' ? null : +propAcc.daily_dd_limit_usd,
                  daily_dd_warning_usd: propAcc.daily_dd_warning_usd === '' ? null : +propAcc.daily_dd_warning_usd,
                  max_total_dd_usd: propAcc.max_total_dd_usd === '' ? null : +propAcc.max_total_dd_usd,
                  consistency_threshold_pct: propAcc.consistency_threshold_pct === '' ? null : +propAcc.consistency_threshold_pct,
                  max_concurrent_trades: propAcc.max_concurrent_trades === '' ? null : +propAcc.max_concurrent_trades,
                })}
                disabled={propSaving}
                className="flex-1 px-4 py-2 bg-violet-600 hover:bg-violet-500 text-white rounded-lg font-medium disabled:opacity-50">
                {propSaving ? 'Salvo...' : 'Salva'}
              </button>
            </div>
          </div>
        </div>
      )}

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
