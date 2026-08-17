"""Motore di simulazione what-if per l'AI Advisor.

Applica una REGOLA PARAMETRICA e DETERMINISTICA ai trade reali chiusi e calcola
la variazione ESATTA del risultato: P&L baseline vs simulato, delta, trade
toccati, nuovo win rate. Niente stime a occhio: solo cio' che e' computabile
dalle colonne del DB viene simulato; il resto e' dichiarato non simulabile.

Regole v1 (trade-level, esatte sotto le assunzioni dichiarate nei caveat):
  exclude_hours      {"hours": [9, 10]}          escludi ore Roma (created_at)
  exclude_sessions   {"sessions": ["asia"]}      escludi sessioni (asia/londra/new_york/notte)
  exclude_weekdays   {"weekdays": ["ven"]}       escludi giorni (lun..dom)
  exclude_direction  {"direction": "sell"}       escludi una direzione
  min_rr_tp1         {"min_rr": 0.5}             escludi trade con R:R pianificato su TP1 < soglia
  cap_loss_at_risk   {}                          taglia ogni perdita al max-risk del trade (enforcement #670)
  scale_risk         {"factor": 0.5}             scala il rischio per trade (P&L proporzionale)
  exclude_near_news  {"minutes": 30}             escludi trade entrati entro X min da una news in tabella

ASSUNZIONI (sempre nei caveat del risultato): trade indipendenti (no
compounding/margine), escludere un trade non altera i segnali successivi,
P&L storici presi cosi' come registrati.
"""
import json
from datetime import timezone
from typing import Optional

CAVEATS = [
    "Trade considerati indipendenti (no compounding ne' effetti margine).",
    "Escludere un trade non altera i segnali successivi del trader.",
    "P&L storici registrati, fill idealizzati come avvenuti.",
]

SUPPORTED_RULES = ("exclude_hours", "exclude_sessions", "exclude_weekdays",
                   "exclude_direction", "min_rr_tp1", "cap_loss_at_risk",
                   "scale_risk", "exclude_near_news")


def _roma(dt):
    from zoneinfo import ZoneInfo
    return dt.replace(tzinfo=timezone.utc).astimezone(ZoneInfo("Europe/Rome")) if dt else None


def _session_of(hour):
    if 1 <= hour < 9:
        return "asia"
    if 9 <= hour < 14:
        return "londra"
    if 14 <= hour < 23:
        return "new_york"
    return "notte"


def _planned_rr_tp1(t) -> Optional[float]:
    try:
        e0 = float(t.entry_price or t.actual_entry_price)
        risk = abs(e0 - float(t.stoploss))
        if risk <= 0:
            return None
        return abs(float(t.tp1) - e0) / risk
    except (TypeError, ValueError):
        return None


def _decide(rule_type, params, t, ctx):
    """Ritorna ("keep", pnl) | ("exclude", None) | ("modify", new_pnl).
    Solleva ValueError su parametri invalidi (validati al primo trade)."""
    pnl = float(t.pnl_usd)
    if rule_type == "exclude_hours":
        hours = [int(h) for h in params["hours"]]
        r = _roma(t.created_at)
        return ("exclude", None) if (r and r.hour in hours) else ("keep", pnl)
    if rule_type == "exclude_sessions":
        sessions = list(params["sessions"])
        r = _roma(t.created_at)
        return ("exclude", None) if (r and _session_of(r.hour) in sessions) else ("keep", pnl)
    if rule_type == "exclude_weekdays":
        weekdays = list(params["weekdays"])
        names = ["lun", "mar", "mer", "gio", "ven", "sab", "dom"]
        r = _roma(t.created_at)
        return ("exclude", None) if (r and names[r.weekday()] in weekdays) else ("keep", pnl)
    if rule_type == "exclude_direction":
        d = str(params["direction"]).lower()
        if d not in ("buy", "sell"):
            raise ValueError(f"direction invalida: {d}")
        return ("exclude", None) if (t.direction or "").lower() == d else ("keep", pnl)
    if rule_type == "min_rr_tp1":
        min_rr = float(params["min_rr"])
        rr = _planned_rr_tp1(t)
        # rr non calcolabile -> tieni (conservativo: non escludere alla cieca)
        return ("exclude", None) if (rr is not None and rr < min_rr) else ("keep", pnl)
    if rule_type == "cap_loss_at_risk":
        target = float(t.risk_usd) if t.risk_usd else 1000.0
        if pnl < -target:
            return ("modify", -target)
        return ("keep", pnl)
    if rule_type == "scale_risk":
        factor = float(params["factor"])
        if factor <= 0 or factor > 5:
            raise ValueError(f"factor fuori range (0, 5]: {factor}")
        return ("modify", round(pnl * factor, 2)) if factor != 1.0 else ("keep", pnl)
    if rule_type == "exclude_near_news":
        minutes = int(params.get("minutes", 30))
        ent = t.entered_at or t.created_at
        if ent and any(abs((ent - et).total_seconds()) <= minutes * 60 for et in ctx["news_times"]):
            return ("exclude", None)
        return ("keep", pnl)
    raise ValueError(f"regola non supportata: {rule_type}")


def _stats(pnls):
    n = len(pnls)
    wins = sum(1 for p in pnls if p > 0)
    return {
        "trades": n,
        "pnl": round(sum(pnls), 2),
        "win_rate": round(100.0 * wins / n, 1) if n else 0.0,
    }


def simulate(rule_type: str, params: dict, db=None) -> dict:
    """Applica la regola a TUTTI i trade reali chiusi. Ritorna il confronto
    esatto baseline vs simulato. Mai solleva: errori -> {"ok": False, ...}."""
    from database import SessionLocal, NewsEvent
    from ai_advisor import _real_closed_trades
    if rule_type not in SUPPORTED_RULES:
        return {"ok": False, "error": f"regola non supportata: {rule_type}",
                "supported": list(SUPPORTED_RULES)}
    close = False
    if db is None:
        db = SessionLocal(); close = True
    try:
        trades = _real_closed_trades(db)
        ctx = {"news_times": []}
        if rule_type == "exclude_near_news":
            try:
                ctx["news_times"] = [e.event_time for e in db.query(NewsEvent).all()
                                     if e.event_time]
            except Exception:
                pass
        base_pnls, sim_pnls = [], []
        excluded, modified = [], []
        for t in trades:
            pnl = float(t.pnl_usd)
            base_pnls.append(pnl)
            action, new_pnl = _decide(rule_type, params or {}, t, ctx)
            if action == "exclude":
                excluded.append({"id": t.id, "pnl": round(pnl, 2)})
            elif action == "modify":
                modified.append({"id": t.id, "pnl": round(pnl, 2), "new_pnl": round(new_pnl, 2)})
                sim_pnls.append(new_pnl)
            else:
                sim_pnls.append(pnl)
        baseline = _stats(base_pnls)
        simulated = _stats(sim_pnls)
        delta = round(simulated["pnl"] - baseline["pnl"], 2)
        verdict = "migliora" if delta > 0 else ("peggiora" if delta < 0 else "invariato")
        return {
            "ok": True,
            "rule": {"type": rule_type, "params": params or {}},
            "baseline": baseline,
            "simulated": simulated,
            "delta_pnl": delta,
            "verdict": verdict,
            "trades_excluded": len(excluded),
            "trades_modified": len(modified),
            "excluded_sample": sorted(excluded, key=lambda x: x["pnl"])[:15],
            "modified_sample": sorted(modified, key=lambda x: x["pnl"])[:15],
            "caveats": CAVEATS,
        }
    except (ValueError, KeyError, TypeError) as e:
        return {"ok": False, "error": f"parametri invalidi: {str(e)[:200]}"}
    except Exception as e:
        return {"ok": False, "error": str(e)[:200]}
    finally:
        if close:
            db.close()
