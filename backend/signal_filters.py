"""Helper centrale per gestione filtri signal (symbol exclusion + hour inclusion).

Filtri applicati al SIGNAL TIME (created_at), non al fill. Quando un signal viene
filtrato:
- NON viene piazzato su MT5
- Viene comunque salvato in DB con is_filtered=True + filter_reason
- Tutta la pipeline (sl_move, target_done, edit) lo gestisce come trade normale
  per simulare l'esito ipotetico
- Le stats reali (performance/by-symbol-hour/equity-curve) ESCLUDONO is_filtered=True
- Una sezione separata what-if espone le stats sui filtered
"""
import json
from datetime import datetime
from typing import Optional


def _load_filter_config(db=None):
    """Carica excluded_symbols e allowed_hours dalla tabella risk_settings."""
    from database import SessionLocal, RiskSettings
    close_db = False
    if db is None:
        db = SessionLocal()
        close_db = True
    try:
        rs = db.query(RiskSettings).first()
        if rs is None:
            return [], None
        excluded = []
        if rs.excluded_symbols:
            try:
                excluded = json.loads(rs.excluded_symbols)
            except Exception:
                excluded = []
        allowed_hours = None
        if rs.allowed_hours:
            try:
                allowed_hours = [int(h) for h in json.loads(rs.allowed_hours)]
            except Exception:
                allowed_hours = None
        return [s.upper() for s in excluded], allowed_hours
    finally:
        if close_db:
            db.close()


def check_signal_filter(symbol: str, signal_created_at: datetime, db=None) -> Optional[str]:
    """Controlla se un signal va filtrato. Ritorna stringa motivazione se SI'
    (signal va marcato is_filtered=True), None se va processato normalmente.

    Logica:
    - Simbolo in excluded_symbols → filtrato
    - allowed_hours settato E ora signal (Roma) ∉ allowed_hours → filtrato
    - Altrimenti → None (procedi normalmente)
    """
    if not symbol:
        return None
    excluded, allowed_hours = _load_filter_config(db)
    if symbol.upper() in excluded:
        return f"Simbolo {symbol.upper()} escluso dai filtri utente"
    if allowed_hours is not None and signal_created_at:
        try:
            from zoneinfo import ZoneInfo
            rome = ZoneInfo("Europe/Rome")
            utc = ZoneInfo("UTC")
            ts = signal_created_at
            if ts.tzinfo is None:
                ts = ts.replace(tzinfo=utc)
            hour = ts.astimezone(rome).hour
            if hour not in allowed_hours:
                return f"Ora {hour:02d}:xx Roma non in fascia permessa {sorted(allowed_hours)}"
        except Exception:
            pass
    return None


def get_filter_config(db=None) -> dict:
    """Snapshot configurazione filtri per UI/API."""
    excluded, allowed_hours = _load_filter_config(db)
    return {
        "excluded_symbols": excluded,
        "allowed_hours": allowed_hours,
    }


def set_filter_config(excluded_symbols: list = None, allowed_hours: list = None, db=None):
    """Aggiorna i filtri in DB. Passa None per lasciare un campo invariato.
    Passa [] o lista vuota per pulire/azzerare."""
    from database import SessionLocal, RiskSettings
    close_db = False
    if db is None:
        db = SessionLocal()
        close_db = True
    try:
        rs = db.query(RiskSettings).first()
        if rs is None:
            rs = RiskSettings()
            db.add(rs)
        if excluded_symbols is not None:
            rs.excluded_symbols = json.dumps([s.upper() for s in excluded_symbols]) if excluded_symbols else None
        if allowed_hours is not None:
            rs.allowed_hours = json.dumps(sorted(set(int(h) for h in allowed_hours))) if allowed_hours else None
        db.add(rs)
        db.commit()
        db.refresh(rs)
        return {"excluded_symbols": json.loads(rs.excluded_symbols) if rs.excluded_symbols else [],
                "allowed_hours": json.loads(rs.allowed_hours) if rs.allowed_hours else None}
    finally:
        if close_db:
            db.close()
