"""News filter — protezione da gap su eventi macro high-impact.

Post-mortem #570 (14/07/2026): il trader ha mandato un SELL 56s prima del CPI;
il feed FTMO e' rimasto muto 32s e lo SL a 4034 ha fillato a 4086.95 (-8166$,
challenge bruciata). Un gap del genere e' possibile SOLO su release schedulate:
i market maker ritirano la liquidita' in anticipo, in modo coordinato.

Difese (tutte gated da RiskSettings.news_filter_enabled):
  Tier 1 — blocco NUOVI ingressi (market + pending) in [T-10min, T+5min]
  Tier 2 — cancellazione pending non fillati a partire da T-10min
  Tier 3 — flatten totale posizioni aperte a partire da T-5min (event.flatten)
  Weekend — flatten venerdi' sera (RiskSettings.friday_flatten_enabled),
            default 22:45 Roma, contro i gap di apertura del lunedi'.

Tutti i timestamp in DB sono UTC. Le finestre sono calcolate in UTC.
"""
from datetime import datetime, timedelta
from typing import Optional

# Finestre (minuti)
ENTRY_BLOCK_BEFORE_MIN = 10
ENTRY_BLOCK_AFTER_MIN = 5
PENDING_CANCEL_BEFORE_MIN = 10
FLATTEN_BEFORE_MIN = 5

# Flatten venerdi': orario Roma
FRIDAY_FLATTEN_HOUR_ROMA = 22
FRIDAY_FLATTEN_MINUTE_ROMA = 45


def _log(msg: str):
    try:
        from mt5_trader import log as _l
        _l(f"[NewsFilter] {msg}")
    except Exception:
        print(f"[NewsFilter] {msg}", flush=True)


def is_enabled(db=None) -> bool:
    from database import SessionLocal, RiskSettings
    close = False
    if db is None:
        db = SessionLocal(); close = True
    try:
        rs = db.query(RiskSettings).first()
        return bool(rs is None or getattr(rs, "news_filter_enabled", True))
    finally:
        if close: db.close()


def is_friday_flatten_enabled(db=None) -> bool:
    from database import SessionLocal, RiskSettings
    close = False
    if db is None:
        db = SessionLocal(); close = True
    try:
        rs = db.query(RiskSettings).first()
        return bool(rs is None or getattr(rs, "friday_flatten_enabled", True))
    finally:
        if close: db.close()


def _upcoming_events(db, now_utc: datetime, horizon_min: int = 30):
    """Eventi con event_time in [now - horizon, now + horizon]."""
    from database import NewsEvent
    lo = now_utc - timedelta(minutes=horizon_min)
    hi = now_utc + timedelta(minutes=horizon_min)
    return db.query(NewsEvent).filter(
        NewsEvent.event_time >= lo,
        NewsEvent.event_time <= hi,
    ).all()


def entry_blocked(now_utc: datetime = None, db=None) -> Optional[str]:
    """Ritorna motivazione se i NUOVI ingressi sono bloccati adesso, None se ok.
    Finestra: [event - 10min, event + 5min] per ogni evento in tabella."""
    from database import SessionLocal
    if now_utc is None:
        now_utc = datetime.utcnow()
    close = False
    if db is None:
        db = SessionLocal(); close = True
    try:
        if not is_enabled(db):
            return None
        for ev in _upcoming_events(db, now_utc):
            start = ev.event_time - timedelta(minutes=ENTRY_BLOCK_BEFORE_MIN)
            end = ev.event_time + timedelta(minutes=ENTRY_BLOCK_AFTER_MIN)
            if start <= now_utc <= end:
                return (f"News window attiva: '{ev.name}' alle "
                        f"{ev.event_time.strftime('%H:%M')} UTC "
                        f"(blocco da -{ENTRY_BLOCK_BEFORE_MIN}m a +{ENTRY_BLOCK_AFTER_MIN}m). "
                        f"Nuovi ingressi BLOCCATI.")
        return None
    finally:
        if close: db.close()


def flatten_due(now_utc: datetime = None, db=None):
    """Ritorna l'evento (non ancora processato) per cui adesso va eseguito il
    flatten [event-5min, event], None altrimenti. NON marca flatten_done."""
    from database import SessionLocal
    if now_utc is None:
        now_utc = datetime.utcnow()
    close = False
    if db is None:
        db = SessionLocal(); close = True
    try:
        if not is_enabled(db):
            return None
        for ev in _upcoming_events(db, now_utc):
            if not ev.flatten or ev.flatten_done:
                continue
            start = ev.event_time - timedelta(minutes=FLATTEN_BEFORE_MIN)
            if start <= now_utc <= ev.event_time:
                return ev
        return None
    finally:
        if close: db.close()


def pending_cancel_due(now_utc: datetime = None, db=None):
    """Evento per cui adesso vanno cancellati i pending non fillati
    [event-10min, event]. Riusa flatten_done come idempotenza leggera? No:
    la cancellazione pending puo' avvenire piu' volte senza danni (idempotente
    per natura: i pending gia' rimossi non ci sono piu')."""
    from database import SessionLocal
    if now_utc is None:
        now_utc = datetime.utcnow()
    close = False
    if db is None:
        db = SessionLocal(); close = True
    try:
        if not is_enabled(db):
            return None
        for ev in _upcoming_events(db, now_utc):
            start = ev.event_time - timedelta(minutes=PENDING_CANCEL_BEFORE_MIN)
            if start <= now_utc <= ev.event_time:
                return ev
        return None
    finally:
        if close: db.close()


def friday_flatten_due(now_utc: datetime = None, db=None) -> bool:
    """True se adesso e' venerdi' sera oltre l'orario di flatten weekend
    (22:45 Roma) e la feature e' attiva. Idempotente per natura: dopo il primo
    flatten non ci sono piu' posizioni."""
    if now_utc is None:
        now_utc = datetime.utcnow()
    try:
        from zoneinfo import ZoneInfo
        roma = now_utc.replace(tzinfo=ZoneInfo("UTC")).astimezone(ZoneInfo("Europe/Rome"))
    except Exception:
        return False
    if roma.weekday() != 4:  # 4 = venerdi'
        return False
    if (roma.hour, roma.minute) < (FRIDAY_FLATTEN_HOUR_ROMA, FRIDAY_FLATTEN_MINUTE_ROMA):
        return False
    return is_friday_flatten_enabled(db)


def mark_flatten_done(event_id: int, db=None):
    from database import SessionLocal, NewsEvent
    close = False
    if db is None:
        db = SessionLocal(); close = True
    try:
        ev = db.query(NewsEvent).filter(NewsEvent.id == event_id).first()
        if ev:
            ev.flatten_done = True
            db.commit()
    finally:
        if close: db.close()


def seed_default_events(db=None) -> int:
    """Inserisce gli eventi noti futuri se non gia' presenti (idempotente).
    - CPI USA: date ufficiali BLS (14:30 Roma estate = 12:30 UTC)
    - NFP: primo venerdi' del mese, 14:30 Roma
    FOMC/PPI: inserimento manuale da UI quando confermati."""
    from database import SessionLocal, NewsEvent
    import calendar as _cal
    close = False
    if db is None:
        db = SessionLocal(); close = True
    try:
        added = 0
        events = []
        # CPI 2026 H2 (BLS): 12 agosto confermato; i successivi tipicamente
        # seconda settimana del mese — verificare su bls.gov e aggiornare da UI.
        events.append(("US CPI (July data)", datetime(2026, 8, 12, 12, 30)))
        # NFP: primo venerdi' del mese alle 12:30 UTC (estate)
        for month in (8, 9, 10, 11, 12):
            cal = _cal.monthcalendar(2026, month)
            first_friday = next(w[4] for w in cal if w[4] != 0)
            # nov/dic: inverno CET -> 14:30 Roma = 13:30 UTC
            utc_hour = 13 if month >= 11 else 12
            events.append((f"US NFP ({_cal.month_name[month]})",
                           datetime(2026, month, first_friday, utc_hour, 30)))
        for name, ts in events:
            exists = db.query(NewsEvent).filter(
                NewsEvent.name == name, NewsEvent.event_time == ts).first()
            if not exists:
                db.add(NewsEvent(name=name, event_time=ts, currency="USD",
                                 impact="high", flatten=True))
                added += 1
        db.commit()
        if added:
            _log(f"seed: aggiunti {added} eventi")
        return added
    finally:
        if close: db.close()
