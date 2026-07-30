"""Feed calendario economico -> popola automaticamente la tabella NewsEvent.

Post-mortem FOMC 29/07 (#622/#623): la FOMC non era in tabella perche' gli
eventi erano solo manuali/hardcoded (CPI+NFP). Risultato: nessuna finestra di
blocco, due segnali entrati a 5 min dalla FOMC in pieno rischio-gap (come il
#570 col CPI). Questo modulo collega un feed reale cosi' la protezione news
(Tier 1/2/3, gia' cablata) scatti da sola.

Sorgente: Forex Factory / faireconomy (gratuito, senza API key).
Filtro: SOLO eventi country=USD e impact=High (l'oro e' guidato dal dollaro;
decisione utente). Ogni evento auto ha flatten=True -> Tier 3 flatten attivo.

Tutti i timestamp DB sono UTC. Il feed fornisce la data ISO8601 CON offset di
fuso, quindi la conversione a UTC e' deterministica (niente detection manuale).
"""
import json
import urllib.request
from datetime import datetime, timedelta, timezone
from typing import Optional

FEED_URLS = [
    "https://nfs.faireconomy.media/ff_calendar_thisweek.json",
    "https://nfs.faireconomy.media/ff_calendar_nextweek.json",
]
SOURCE = "forexfactory"
REFRESH_EVERY_MIN = 30
_HTTP_TIMEOUT = 20

# Stato modulo per il throttle e l'endpoint di stato
_last_refresh_utc: Optional[datetime] = None
_last_source_ok: bool = False
_last_counts = {"added": 0, "updated": 0, "pruned": 0, "total_usd_high": 0}


def _log(msg: str):
    try:
        from mt5_trader import log as _l
        _l(f"[EconCalendar] {msg}")
    except Exception:
        print(f"[EconCalendar] {msg}", flush=True)


def _to_utc(date_str: str) -> Optional[datetime]:
    """ISO8601 con offset (es. '2026-07-29T14:00:00-04:00') -> datetime UTC naive.
    Ritorna None se non parsabile."""
    if not date_str:
        return None
    s = date_str.strip()
    # Python <3.11 non gestisce la 'Z'; normalizziamo comunque.
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    try:
        dt = datetime.fromisoformat(s)
    except ValueError:
        return None
    if dt.tzinfo is None:
        # Senza offset: assumiamo gia' UTC (fallback prudente).
        return dt
    return dt.astimezone(timezone.utc).replace(tzinfo=None)


def fetch_feed() -> list:
    """Scarica e unisce gli eventi grezzi dai feed. Robusto: su errore di rete
    logga e ritorna lista vuota, MAI solleva (non deve rompere il monitor)."""
    events = []
    ok_any = False
    for url in FEED_URLS:
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "TradeMachine/1.0"})
            with urllib.request.urlopen(req, timeout=_HTTP_TIMEOUT) as resp:
                data = json.loads(resp.read().decode("utf-8", errors="replace"))
            if isinstance(data, list):
                events.extend(data)
                ok_any = True
        except Exception as e:
            _log(f"fetch errore {url}: {str(e)[:120]}")
    global _last_source_ok
    _last_source_ok = ok_any
    return events


def _usd_high(raw: dict) -> bool:
    return (str(raw.get("country", "")).upper() == "USD"
            and str(raw.get("impact", "")).lower() == "high")


def refresh_events(db=None) -> dict:
    """Upsert degli eventi USD High-impact dal feed in NewsEvent (source=forexfactory).
    Idempotente: chiave stabile external_key = 'title|iso_date'.
    - nuovo -> insert (flatten=True)
    - esistente con event_time diverso -> update (e reset flatten_done se futuro)
    - eventi manuali: MAI toccati
    - prune: rimuove SOLO eventi auto FUTURI spariti dal feed (cancellati)
    Ritorna dict con conteggi.
    """
    from database import SessionLocal, NewsEvent
    close = False
    if db is None:
        db = SessionLocal(); close = True
    added = updated = pruned = 0
    try:
        raw_events = fetch_feed()
        # (external_key, event_time_utc, name)
        parsed = []
        seen_keys = set()
        for r in raw_events:
            if not _usd_high(r):
                continue
            iso = r.get("date")
            et = _to_utc(iso)
            title = (r.get("title") or "").strip()
            if not et or not title:
                continue
            key = f"{title}|{iso}"
            if key in seen_keys:
                continue
            seen_keys.add(key)
            parsed.append((key, et, title))

        if not _last_source_ok:
            # Rete fallita: non tocchiamo nulla (eventi esistenti restano).
            _log("feed non raggiungibile: nessuna modifica")
            _finalize_counts(db, added, updated, pruned)
            return dict(_last_counts, source_ok=False)

        existing_auto = {e.external_key: e for e in db.query(NewsEvent).filter(
            NewsEvent.source == SOURCE).all() if e.external_key}

        now = datetime.utcnow()
        for key, et, title in parsed:
            ev = existing_auto.get(key)
            if ev is None:
                db.add(NewsEvent(name=title, event_time=et, currency="USD",
                                 impact="high", flatten=True, source=SOURCE,
                                 external_key=key))
                added += 1
            else:
                if ev.event_time != et:
                    ev.event_time = et
                    if et > now:
                        ev.flatten_done = False
                    updated += 1

        # Prune: eventi auto FUTURI non piu' nel feed = cancellati dal calendario
        feed_keys = {k for (k, _, _) in parsed}
        for key, ev in existing_auto.items():
            if key not in feed_keys and ev.event_time and ev.event_time > now:
                db.delete(ev)
                pruned += 1

        db.commit()
        _finalize_counts(db, added, updated, pruned)
        if added or updated or pruned:
            _log(f"refresh: +{added} ~{updated} -{pruned} (USD-High)")
        return dict(_last_counts, source_ok=True)
    except Exception as e:
        _log(f"refresh_events errore: {str(e)[:150]}")
        try:
            db.rollback()
        except Exception:
            pass
        return dict(_last_counts, source_ok=_last_source_ok)
    finally:
        if close:
            db.close()


def _finalize_counts(db, added, updated, pruned):
    from database import NewsEvent
    global _last_refresh_utc, _last_counts
    _last_refresh_utc = datetime.utcnow()
    try:
        total = db.query(NewsEvent).filter(
            NewsEvent.source == SOURCE,
            NewsEvent.event_time >= datetime.utcnow(),
        ).count()
    except Exception:
        total = _last_counts.get("total_usd_high", 0)
    _last_counts = {"added": added, "updated": updated, "pruned": pruned,
                    "total_usd_high": total}


def maybe_refresh(db=None) -> dict:
    """Esegue refresh_events al massimo ogni REFRESH_EVERY_MIN. Da chiamare dal
    loop 15s (dentro run_in_executor). Ritorna {refreshed, ...}."""
    global _last_refresh_utc
    now = datetime.utcnow()
    if _last_refresh_utc is not None and (now - _last_refresh_utc) < timedelta(minutes=REFRESH_EVERY_MIN):
        return {"refreshed": False}
    res = refresh_events(db)
    return dict(res, refreshed=True)


def last_refresh_info() -> dict:
    """Per l'endpoint di stato UI."""
    roma = None
    if _last_refresh_utc is not None:
        try:
            from zoneinfo import ZoneInfo
            roma = (_last_refresh_utc.replace(tzinfo=timezone.utc)
                    .astimezone(ZoneInfo("Europe/Rome")).strftime("%Y-%m-%d %H:%M:%S"))
        except Exception:
            roma = _last_refresh_utc.strftime("%Y-%m-%d %H:%M:%S") + " UTC"
    return {
        "last_refresh_roma": roma,
        "source_ok": _last_source_ok,
        "usd_high_upcoming": _last_counts.get("total_usd_high", 0),
    }
