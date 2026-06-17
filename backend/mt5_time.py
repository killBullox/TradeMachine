"""Helper unico per conversione orari MT5 ↔ Roma ↔ UTC.

Usa SEMPRE questi helper negli script di debug/analisi. Il bug ricorrente
era: detection manuale dell'offset server in ogni script, sbagliata 1 volta su 2.
Qui sotto la detection e' robusta e centralizzata.

Esempio uso script ad-hoc:
    from mt5_time import roma_to_mt5_server_range, mt5_epoch_to_roma
    srv_start, srv_end = roma_to_mt5_server_range("2026-06-17 20:02:00", "2026-06-17 20:03:40")
    ticks = m.copy_ticks_range("GOLD", srv_start, srv_end, m.COPY_TICKS_ALL)
    for t in ticks:
        rome_str = mt5_epoch_to_roma(t['time'])
        print(rome_str, t['bid'])
"""
from datetime import datetime, timedelta
from typing import Optional


_cached_offset_s: Optional[int] = None


def detect_mt5_server_offset(symbol_hint: str = "GOLD") -> int:
    """Rileva offset server MT5 vs UTC in secondi (positivo = server ahead UTC).
    Prova diverse varianti di symbol name perche' alcuni broker usano suffissi.
    Caches il risultato. NON usa il valore globale di mt5_trader (potrebbe non
    essere inizializzato negli script standalone).
    """
    global _cached_offset_s
    if _cached_offset_s is not None:
        return _cached_offset_s
    try:
        import MetaTrader5 as mt5
        # Prova varianti symbol fino a una valida
        for sym in (symbol_hint, symbol_hint + "#", symbol_hint + ".r",
                    "EURUSD", "EURUSD#", "GOLD", "GOLD#", "XAUUSD"):
            tick = mt5.symbol_info_tick(sym)
            if tick and tick.time > 0:
                srv = datetime.utcfromtimestamp(tick.time)
                utc = datetime.utcnow()
                diff_s = int((srv - utc).total_seconds())
                # Arrotonda alla mezz'ora (offset broker e' sempre multiplo di 30min)
                _cached_offset_s = round(diff_s / 1800) * 1800
                return _cached_offset_s
    except Exception:
        pass
    _cached_offset_s = 0
    return 0


def mt5_epoch_to_utc(mt5_epoch: int) -> datetime:
    """Converte un timestamp MT5 (server time) in datetime UTC reale."""
    offset_s = detect_mt5_server_offset()
    return datetime.utcfromtimestamp(mt5_epoch - offset_s)


def mt5_epoch_to_roma(mt5_epoch: int) -> datetime:
    """Converte un timestamp MT5 (server time) in datetime Roma (CEST/CET)."""
    utc = mt5_epoch_to_utc(mt5_epoch)
    try:
        from zoneinfo import ZoneInfo
        return utc.replace(tzinfo=ZoneInfo("UTC")).astimezone(ZoneInfo("Europe/Rome")).replace(tzinfo=None)
    except Exception:
        # Fallback rozzo +2h (CEST)
        return utc + timedelta(hours=2)


def roma_to_utc(roma_str_or_dt) -> datetime:
    """Converte una data/ora Roma a datetime UTC.
    Accetta str 'YYYY-MM-DD HH:MM:SS' o datetime naive (interpretato come Roma)."""
    if isinstance(roma_str_or_dt, str):
        dt = datetime.fromisoformat(roma_str_or_dt.replace("Z", "").strip())
    else:
        dt = roma_str_or_dt
    try:
        from zoneinfo import ZoneInfo
        return dt.replace(tzinfo=ZoneInfo("Europe/Rome")).astimezone(ZoneInfo("UTC")).replace(tzinfo=None)
    except Exception:
        return dt - timedelta(hours=2)


def roma_to_mt5_server_range(roma_start: str, roma_end: str) -> tuple:
    """Converte una finestra Roma → range datetime in server time MT5,
    utilizzabili direttamente con mt5.copy_ticks_range(..., start, end, ...).

    Esempio:
        srv_start, srv_end = roma_to_mt5_server_range("2026-06-17 20:02", "2026-06-17 20:04")
        ticks = m.copy_ticks_range("GOLD", srv_start, srv_end, m.COPY_TICKS_ALL)
    """
    utc_start = roma_to_utc(roma_start)
    utc_end = roma_to_utc(roma_end)
    offset_s = detect_mt5_server_offset()
    return utc_start + timedelta(seconds=offset_s), utc_end + timedelta(seconds=offset_s)
