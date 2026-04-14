const BASE = '/api'

async function request(path, opts = {}) {
  const res = await fetch(`${BASE}${path}`, {
    headers: { 'Content-Type': 'application/json' },
    ...opts,
  })
  if (!res.ok) throw new Error(`HTTP ${res.status}`)
  return res.json()
}

export const api = {
  // Signals
  getSignals: (params = {}) => {
    const qs = new URLSearchParams(params).toString()
    return request(`/signals${qs ? '?' + qs : ''}`)
  },
  updateSignal: (id, body) => request(`/signals/${id}`, { method: 'PATCH', body: JSON.stringify(body) }),
  deleteSignal: (id) => request(`/signals/${id}`, { method: 'DELETE' }),

  // Updates
  getUpdates: (params = {}) => {
    const qs = new URLSearchParams(params).toString()
    return request(`/updates${qs ? '?' + qs : ''}`)
  },

  // Levels
  getLevels: (params = {}) => {
    const qs = new URLSearchParams(params).toString()
    return request(`/levels${qs ? '?' + qs : ''}`)
  },

  // Journal
  getJournal: () => request('/journal'),
  createJournal: (body) => request('/journal', { method: 'POST', body: JSON.stringify(body) }),
  deleteJournal: (id) => request(`/journal/${id}`, { method: 'DELETE' }),

  // Performance
  getPerformance: (params = {}) => {
    const q = new URLSearchParams()
    if (params.date_from) q.set('date_from', params.date_from)
    if (params.date_to) q.set('date_to', params.date_to)
    const qs = q.toString()
    return request(`/performance${qs ? '?' + qs : ''}`)
  },

  // Risk settings
  getRiskSettings: () => request('/risk-settings'),
  saveRiskSettings: (body) => request('/risk-settings', { method: 'POST', body: JSON.stringify(body) }),
  recalculate: () => request('/recalculate', { method: 'POST' }),
  getPrice: (symbol) => request(`/price/${symbol}`),
  triggerBackfill: () => request('/backfill', { method: 'POST' }),

  // Messages
  getMessages: (limit = 100) => request(`/messages?limit=${limit}`),

  // Reload
  reloadHistory: (limit = 500) => request(`/reload-history?limit=${limit}`, { method: 'POST' }),

  // MT5 Account
  getMt5Accounts: () => request('/mt5/accounts'),
  switchMt5Account: (login, server, pin) => request(`/mt5/switch-account?login=${login}&server=${server}&pin=${pin}`, { method: 'POST' }),
  addMt5Account: (login, server, label, isDemo, pin) =>
    request(`/mt5/add-account?login=${login}&server=${encodeURIComponent(server)}&label=${encodeURIComponent(label)}&is_demo=${isDemo}&pin=${pin}`, { method: 'POST' }),
  removeMt5Account: (id, pin) => request(`/mt5/remove-account/${id}?pin=${pin}`, { method: 'DELETE' }),

  // Backup & Ripristino
  archiveTrades: (pin) => request(`/backup/archive?pin=${pin}`, { method: 'POST' }),
  getRestorePoints: () => request('/backup/restore-points'),
  restoreTrades: (id, pin) => request(`/backup/restore/${id}?pin=${pin}`, { method: 'POST' }),
  deleteRestorePoint: (id, pin) => request(`/backup/restore-points/${id}?pin=${pin}`, { method: 'DELETE' }),
}
