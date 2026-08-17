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

# Regole STRUTTURALI (policy di sicurezza deterministiche, es. enforcement del
# max-risk): esenti dai gate di campione/robustezza perche' non sono scommesse
# su un pattern storico. Le regole PATTERN (esclusioni) invece devono superare
# la validazione statistica per essere raccomandabili.
STRUCTURAL_RULES = ("cap_loss_at_risk", "scale_risk")

# Gate statistici per le regole pattern
MIN_SAMPLE = 10           # trade toccati minimi perche' il campione conti
TOP1_SHARE_MAX = 0.60     # il singolo trade migliore non puo' valere >60% del delta
BOOTSTRAP_N = 500         # ricampionamenti (seed fisso -> deterministico)
BOOTSTRAP_SEED = 42
CONFIDENCE_MIN = 0.90     # frazione minima di ricampionamenti con delta > 0


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


def _build_ctx(rule_type, db):
    ctx = {"news_times": []}
    if rule_type == "exclude_near_news":
        try:
            from database import NewsEvent
            ctx["news_times"] = [e.event_time for e in db.query(NewsEvent).all()
                                 if e.event_time]
        except Exception:
            pass
    return ctx


def _apply_decisions(rule_type, params, trades, ctx):
    """Applica la regola a ogni trade. Ritorna lista (trade, action, new_pnl)."""
    out = []
    for t in trades:
        action, new_pnl = _decide(rule_type, params or {}, t, ctx)
        out.append((t, action, new_pnl))
    return out


def _contributions(decisions):
    """Contributo di ogni trade al delta: exclude -> -pnl, modify -> new-old,
    keep -> 0. delta totale = somma dei contributi."""
    contribs = []
    for t, action, new_pnl in decisions:
        pnl = float(t.pnl_usd)
        if action == "exclude":
            contribs.append(-pnl)
        elif action == "modify":
            contribs.append(new_pnl - pnl)
        else:
            contribs.append(0.0)
    return contribs


def _bootstrap_confidence(contribs, n_resamples=BOOTSTRAP_N, seed=BOOTSTRAP_SEED):
    """Frazione di ricampionamenti (storia ricampionata con replacement) in cui
    la regola resta vantaggiosa (delta > 0). Seed fisso -> deterministico."""
    import random
    n = len(contribs)
    if n == 0:
        return 0.0
    rng = random.Random(seed)
    positive = 0
    for _ in range(n_resamples):
        s = 0.0
        for _ in range(n):
            s += contribs[rng.randrange(n)]
        if s > 0:
            positive += 1
    return round(positive / n_resamples, 3)


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
        ctx = _build_ctx(rule_type, db)
        base_pnls, sim_pnls = [], []
        excluded, modified = [], []
        for t, action, new_pnl in _apply_decisions(rule_type, params, trades, ctx):
            pnl = float(t.pnl_usd)
            base_pnls.append(pnl)
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


def validate(rule_type: str, params: dict, db=None) -> dict:
    """Simulazione + VALIDAZIONE STATISTICA. Una regola e' "promossa"
    (passed=True) solo se:
      - delta > 0 (migliora il risultato storico)
      - campione: >= MIN_SAMPLE trade toccati (regole pattern; le strutturali
        tipo cap_loss_at_risk sono policy deterministiche, esenti)
      - robustezza: il delta regge anche togliendo il singolo trade che
        contribuisce di piu' (niente conclusioni da 1-2 outlier), e il top
        contributore vale al massimo TOP1_SHARE_MAX del delta
      - confidenza: in >= CONFIDENCE_MIN dei ricampionamenti bootstrap della
        storia il delta resta positivo (seed fisso -> deterministico)
    Ritorna il risultato di simulate() + blocco "validation". Mai solleva."""
    from database import SessionLocal
    from ai_advisor import _real_closed_trades
    if rule_type not in SUPPORTED_RULES:
        return {"ok": False, "error": f"regola non supportata: {rule_type}"}
    close = False
    if db is None:
        db = SessionLocal(); close = True
    try:
        res = simulate(rule_type, params, db)
        if not res.get("ok"):
            return res
        trades = _real_closed_trades(db)
        ctx = _build_ctx(rule_type, db)
        decisions = _apply_decisions(rule_type, params, trades, ctx)
        contribs = _contributions(decisions)
        delta = res["delta_pnl"]
        n_affected = res["trades_excluded"] + res["trades_modified"]
        structural = rule_type in STRUCTURAL_RULES

        reasons = []
        if delta <= 0:
            reasons.append(f"non migliora il risultato (delta {delta}$)")
        sample_ok = structural or n_affected >= MIN_SAMPLE
        if not sample_ok:
            reasons.append(f"campione insufficiente ({n_affected} trade toccati, minimo {MIN_SAMPLE})")

        top1 = max(contribs) if contribs else 0.0
        top1_share = round(top1 / delta, 3) if delta > 0 and top1 > 0 else None
        delta_no_top1 = round(delta - top1, 2)
        robust_ok = True
        if not structural and delta > 0:
            if delta_no_top1 <= 0:
                robust_ok = False
                reasons.append(f"fragile: senza il miglior singolo trade il delta crolla a {delta_no_top1}$")
            elif top1_share is not None and top1_share > TOP1_SHARE_MAX:
                robust_ok = False
                reasons.append(f"concentrato: il singolo miglior trade vale il {round(top1_share*100)}% del delta")

        confidence = None
        conf_ok = True
        if delta > 0 and sample_ok and robust_ok:
            confidence = _bootstrap_confidence(contribs)
            if not structural and confidence < CONFIDENCE_MIN:
                conf_ok = False
                reasons.append(f"confidenza bootstrap {round(confidence*100)}% sotto il minimo {round(CONFIDENCE_MIN*100)}%")

        passed = (delta > 0) and sample_ok and robust_ok and conf_ok
        res["validation"] = {
            "passed": passed,
            "category": "strutturale" if structural else "pattern",
            "n_affected": n_affected,
            "top1_share": top1_share,
            "delta_without_top1": delta_no_top1,
            "bootstrap_confidence": confidence,
            "fail_reasons": reasons,
        }
        return res
    except Exception as e:
        return {"ok": False, "error": str(e)[:200]}
    finally:
        if close:
            db.close()


def sweep(db=None, max_entries: int = 25) -> dict:
    """Batteria sistematica PRE-LLM: testa e valida tutte le regole standard.
    Ritorna {promosse: [...], bocciate_interessanti: [...]} compatte per il
    dossier. Solo le PROMOSSE sono raccomandabili dall'LLM."""
    from database import SessionLocal
    close = False
    if db is None:
        db = SessionLocal(); close = True
    try:
        candidates = []
        for h in range(24):
            candidates.append(("exclude_hours", {"hours": [h]}))
        for s in ("asia", "londra", "new_york", "notte"):
            candidates.append(("exclude_sessions", {"sessions": [s]}))
        for w in ("lun", "mar", "mer", "gio", "ven"):
            candidates.append(("exclude_weekdays", {"weekdays": [w]}))
        for dirn in ("buy", "sell"):
            candidates.append(("exclude_direction", {"direction": dirn}))
        for rr in (0.3, 0.5, 0.8, 1.0):
            candidates.append(("min_rr_tp1", {"min_rr": rr}))
        candidates.append(("cap_loss_at_risk", {}))
        for f in (0.5, 0.75):
            candidates.append(("scale_risk", {"factor": f}))
        for m in (30, 60):
            candidates.append(("exclude_near_news", {"minutes": m}))

        promoted, rejected = [], []
        for rule_type, params in candidates:
            res = validate(rule_type, params, db)
            if not res.get("ok"):
                continue
            v = res["validation"]
            if v["n_affected"] == 0:
                continue  # regola che non tocca nulla: rumore
            entry = {
                "sim_type": rule_type,
                "sim_params": json.dumps(params),
                "delta_pnl": res["delta_pnl"],
                "n_affected": v["n_affected"],
                "bootstrap_confidence": v["bootstrap_confidence"],
                "top1_share": v["top1_share"],
                "category": v["category"],
            }
            if v["passed"]:
                promoted.append(entry)
            elif res["delta_pnl"] > 0:
                # sembrava buona ma bocciata: l'LLM DEVE saperlo (anti-illusione)
                entry["fail_reasons"] = v["fail_reasons"]
                rejected.append(entry)
        promoted.sort(key=lambda e: -e["delta_pnl"])
        rejected.sort(key=lambda e: -e["delta_pnl"])
        return {
            "note": ("Regole gia' testate e validate statisticamente sul campione "
                     "storico. Solo le PROMOSSE sono raccomandabili."),
            "gates": {"min_sample": MIN_SAMPLE, "top1_share_max": TOP1_SHARE_MAX,
                      "bootstrap_confidence_min": CONFIDENCE_MIN},
            "promosse": promoted[:max_entries],
            "bocciate_interessanti": rejected[:max_entries],
        }
    finally:
        if close:
            db.close()
