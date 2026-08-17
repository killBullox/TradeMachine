"""AI Advisor — motore di analisi dei trade reali + sintesi LLM.

Due strati:
  1. build_dossier(): statistiche DETERMINISTICHE calcolate in Python sui trade
     reali (is_filtered=0, con ticket MT5). L'LLM e' bravo quanto i dati che
     riceve: il dossier e' la fonte di verita', la sintesi non inventa numeri.
  2. generate_report(): 1 chiamata a Claude (structured output JSON) che
     trasforma il dossier in conclusioni leggibili: edge del trader, buchi di
     esecuzione nostri, rischio, pattern, raccomandazioni.

Cadenza: automatica 1x/giorno dopo la chiusura NY (23:10 Roma, guardia
persistita su ai_reports — restart-safe) + rigenerazione on-demand da UI.

Convenzioni: DB in UTC naive; output utente in ora Roma; win set coerente con
gli endpoint esistenti: {tp1,tp2,tp3,closed,trail_out} vs loss {sl_hit}.
"""
import json
import os
import time
from datetime import datetime, timedelta, timezone
from typing import Optional

ADVISOR_MODEL = "claude-opus-5"
DAILY_HOUR_ROMA = 23      # auto-run dalle 23:10 Roma (dopo chiusura NY)
DAILY_MINUTE_ROMA = 10
CHECK_EVERY_S = 600       # throttle modulo: check DB al massimo ogni 10 min
PER_TRADE_ROWS = 60       # righe della tabella per-trade passata all'LLM

WIN_STATUSES = ("tp1", "tp2", "tp3", "closed", "trail_out")
LOSS_STATUSES = ("sl_hit",)
TERMINAL_STATUSES = WIN_STATUSES + LOSS_STATUSES

_last_check_utc: Optional[datetime] = None
_client = None


def _log(msg: str):
    try:
        from mt5_trader import log as _l
        _l(f"[AIAdvisor] {msg}")
    except Exception:
        print(f"[AIAdvisor] {msg}", flush=True)


def _roma_now():
    from zoneinfo import ZoneInfo
    return datetime.now(ZoneInfo("Europe/Rome"))


def _utc_to_roma(dt):
    if dt is None:
        return None
    from zoneinfo import ZoneInfo
    return dt.replace(tzinfo=timezone.utc).astimezone(ZoneInfo("Europe/Rome"))


# ─── Strato 1: dossier deterministico ────────────────────────────────────────

def _real_closed_trades(db):
    """Trade REALI chiusi: no archiviati, no paper, con ticket MT5, entrati
    davvero, con esito terminale e P&L noto."""
    from database import Signal
    from sqlalchemy import or_
    return (db.query(Signal)
            .filter(Signal.is_archived == False,  # noqa: E712
                    Signal.is_filtered == False,  # noqa: E712
                    or_(Signal.mt5_ticket.isnot(None), Signal.mt5_tickets.isnot(None)),
                    Signal.actual_entry_price.isnot(None),
                    Signal.status.in_(TERMINAL_STATUSES),
                    Signal.pnl_usd.isnot(None))
            .order_by(Signal.created_at)
            .all())


def _bucket(stats, key):
    b = stats.setdefault(key, {"trades": 0, "wins": 0, "pnl": 0.0})
    return b


def _finalize_buckets(d):
    for b in d.values():
        b["pnl"] = round(b["pnl"], 2)
        b["win_rate"] = round(100.0 * b["wins"] / b["trades"], 1) if b["trades"] else 0.0
    return d


def _session_of(hour_roma: int) -> str:
    if 1 <= hour_roma < 9:
        return "asia"
    if 9 <= hour_roma < 14:
        return "londra"
    if 14 <= hour_roma < 23:
        return "new_york"
    return "notte"


def _survived_events(trade_log_json):
    """Conta gli eventi di interesse sopravvissuti nel trade_log. NB: per i
    trade MT5 chiusi il log ricco viene sovrascritto a fine trade, quindi
    questi conteggi sono un LOWER BOUND (documentato nel dossier)."""
    interesting = ("size_entry_fix", "size_entry_fill_fix", "duplicate_kept_old",
                   "duplicate_replaced", "news_blocked", "trader_news_block",
                   "be_at_tp1", "trail_applied", "sl_move_rejected_risk",
                   "mt5_tp_fix", "mt5_entry_fix", "late_catch", "reenter",
                   "enter_now_market", "manual_close")
    counts = {}
    try:
        for e in json.loads(trade_log_json or "[]"):
            ev = e.get("event")
            if ev in interesting:
                counts[ev] = counts.get(ev, 0) + 1
    except Exception:
        pass
    return counts


def build_dossier(db=None) -> dict:
    """Dossier statistico completo sui trade reali chiusi. Deterministico."""
    from database import SessionLocal, EmaCase, NewsEvent
    close = False
    if db is None:
        db = SessionLocal(); close = True
    try:
        trades = _real_closed_trades(db)
        n = len(trades)
        d = {
            "generated_at_roma": _roma_now().strftime("%Y-%m-%d %H:%M:%S"),
            "trades_analyzed": n,
            "symbol_focus": "XAUUSD",
            "caveats": [
                "Solo trade reali chiusi (no paper/filtrati, no archiviati).",
                "Conteggi eventi trade_log = lower bound (log sovrascritto alla chiusura).",
                "Orari in ora Europe/Rome.",
            ],
        }
        if n == 0:
            d["overall"] = {}
            return d

        wins = [t for t in trades if t.status in WIN_STATUSES]
        losses = [t for t in trades if t.status in LOSS_STATUSES]
        pnls = [float(t.pnl_usd) for t in trades]
        win_pnl = sum(float(t.pnl_usd) for t in wins)
        loss_pnl = sum(float(t.pnl_usd) for t in losses)

        # Equity/drawdown/streak sulla sequenza per closed_at
        seq = sorted(trades, key=lambda t: t.closed_at or t.created_at)
        equity = 0.0; peak = 0.0; max_dd = 0.0
        best_streak = 0; worst_streak = 0; cur_w = 0; cur_l = 0
        for t in seq:
            equity += float(t.pnl_usd)
            peak = max(peak, equity)
            max_dd = max(max_dd, peak - equity)
            if float(t.pnl_usd) > 0:
                cur_w += 1; cur_l = 0
            else:
                cur_l += 1; cur_w = 0
            best_streak = max(best_streak, cur_w)
            worst_streak = max(worst_streak, cur_l)

        d["overall"] = {
            "total_pnl": round(sum(pnls), 2),
            "wins": len(wins), "losses": len(losses),
            "win_rate": round(100.0 * len(wins) / n, 1),
            "avg_win": round(win_pnl / len(wins), 2) if wins else 0.0,
            "avg_loss": round(loss_pnl / len(losses), 2) if losses else 0.0,
            "profit_factor": round(win_pnl / abs(loss_pnl), 2) if loss_pnl else None,
            "max_drawdown": round(max_dd, 2),
            "best_win_streak": best_streak, "worst_loss_streak": worst_streak,
            "biggest_win": round(max(pnls), 2), "biggest_loss": round(min(pnls), 2),
        }

        # Edge del trader: direzione / ora / sessione / giorno / TP raggiunti
        by_dir, by_hour, by_session, by_weekday, by_status = {}, {}, {}, {}, {}
        rr_tp1_list = []
        weekdays = ["lun", "mar", "mer", "gio", "ven", "sab", "dom"]
        for t in trades:
            pnl = float(t.pnl_usd)
            is_win = t.status in WIN_STATUSES
            roma = _utc_to_roma(t.created_at)
            for key, dd in ((t.direction or "?", by_dir),
                            (f"{roma.hour:02d}" if roma else "?", by_hour),
                            (_session_of(roma.hour) if roma else "?", by_session),
                            (weekdays[roma.weekday()] if roma else "?", by_weekday)):
                b = _bucket(dd, key)
                b["trades"] += 1; b["pnl"] += pnl
                if is_win:
                    b["wins"] += 1
            by_status[t.status] = by_status.get(t.status, 0) + 1
            # R:R pianificato su TP1 (rischio = entry->SL sul prezzo segnale)
            try:
                e0 = float(t.entry_price or t.actual_entry_price)
                if t.stoploss and t.tp1:
                    risk = abs(e0 - float(t.stoploss))
                    if risk > 0:
                        rr_tp1_list.append(abs(float(t.tp1) - e0) / risk)
            except Exception:
                pass

        d["trader_edge"] = {
            "by_direction": _finalize_buckets(by_dir),
            "by_hour_roma": _finalize_buckets(dict(sorted(by_hour.items()))),
            "by_session": _finalize_buckets(by_session),
            "by_weekday": _finalize_buckets(by_weekday),
            "status_distribution": by_status,
            "tp_reach": {
                "tp1_or_better": sum(by_status.get(s, 0) for s in ("tp1", "tp2", "tp3")),
                "tp2_or_better": sum(by_status.get(s, 0) for s in ("tp2", "tp3")),
                "tp3": by_status.get("tp3", 0),
            },
            "planned_rr_tp1": {
                "avg": round(sum(rr_tp1_list) / len(rr_tp1_list), 2) if rr_tp1_list else None,
                "min": round(min(rr_tp1_list), 2) if rr_tp1_list else None,
                "max": round(max(rr_tp1_list), 2) if rr_tp1_list else None,
            },
        }

        # Esecuzione nostra: slippage / latenza / hold / eventi / violazioni risk
        slippages, latencies, holds, risk_violations = [], [], [], []
        event_totals = {}
        for t in trades:
            try:
                lo = float(t.entry_price) if t.entry_price else None
                hi = float(t.entry_price_high) if t.entry_price_high else lo
                ae = float(t.actual_entry_price)
                if lo is not None:
                    lo2, hi2 = min(lo, hi or lo), max(lo, hi or lo)
                    if ae > hi2:
                        slippages.append(round(ae - hi2, 2))
                    elif ae < lo2:
                        slippages.append(round(lo2 - ae, 2))
                    else:
                        slippages.append(0.0)
            except Exception:
                pass
            if t.entered_at and t.created_at:
                latencies.append((t.entered_at - t.created_at).total_seconds())
            if t.closed_at and t.entered_at:
                holds.append((t.closed_at - t.entered_at).total_seconds() / 60.0)
            for ev, c in _survived_events(t.trade_log).items():
                event_totals[ev] = event_totals.get(ev, 0) + c
            # Violazione max-risk (caso #670): perdita oltre il rischio definito
            try:
                target = float(t.risk_usd) if t.risk_usd else 1000.0
                if float(t.pnl_usd) < -target * 1.05:
                    risk_violations.append({
                        "id": t.id, "pnl": round(float(t.pnl_usd), 2),
                        "risk_target": round(target, 2),
                        "excess_pct": round(100.0 * (abs(float(t.pnl_usd)) / target - 1), 1),
                    })
            except Exception:
                pass

        out_of_range = [s for s in slippages if s > 0]
        d["execution"] = {
            "avg_slippage_beyond_range": round(sum(out_of_range) / len(out_of_range), 2) if out_of_range else 0.0,
            "max_slippage_beyond_range": round(max(out_of_range), 2) if out_of_range else 0.0,
            "fills_beyond_range": len(out_of_range),
            "avg_fill_latency_s": round(sum(latencies) / len(latencies), 1) if latencies else None,
            "avg_hold_minutes": round(sum(holds) / len(holds), 1) if holds else None,
            "trade_log_events_lower_bound": event_totals,
            "max_risk_violations": risk_violations,
        }

        # Rischio: giornate, news proximity
        daily = {}
        for t in seq:
            day = _utc_to_roma(t.closed_at or t.created_at).strftime("%Y-%m-%d")
            daily[day] = round(daily.get(day, 0.0) + float(t.pnl_usd), 2)
        neg_days = [v for v in daily.values() if v < 0]
        news_near = 0
        try:
            events = db.query(NewsEvent).all()
            ev_times = [e.event_time for e in events if e.event_time]
            for t in trades:
                ent = t.entered_at or t.created_at
                if ent and any(abs((ent - et).total_seconds()) <= 1800 for et in ev_times):
                    news_near += 1
        except Exception:
            pass
        d["risk"] = {
            "trading_days": len(daily),
            "negative_days": len(neg_days),
            "worst_day": round(min(daily.values()), 2) if daily else 0.0,
            "best_day": round(max(daily.values()), 2) if daily else 0.0,
            "trades_entered_within_30m_of_news": news_near,
            "daily_pnl_last_15": dict(list(sorted(daily.items()))[-15:]),
        }

        # Entrate mancate (EMA)
        try:
            cases = db.query(EmaCase).all()
            d["missed_entries_ema"] = {
                "cases": len(cases),
                "sim_pnl_total": round(sum(float(c.sim_pnl_usd or 0) for c in cases), 2),
                "by_outcome": {o: sum(1 for c in cases if c.sim_outcome == o)
                               for o in {c.sim_outcome for c in cases if c.sim_outcome}},
            }
        except Exception:
            d["missed_entries_ema"] = {"cases": 0}

        # Tabella per-trade (ultimi N) — il materiale citabile dall'LLM
        rows = []
        for t in seq[-PER_TRADE_ROWS:]:
            roma_c = _utc_to_roma(t.created_at)
            hold_m = round((t.closed_at - t.entered_at).total_seconds() / 60.0, 1) \
                if (t.closed_at and t.entered_at) else None
            rows.append({
                "id": t.id,
                "data_roma": roma_c.strftime("%Y-%m-%d %H:%M") if roma_c else None,
                "dir": t.direction, "entry": t.actual_entry_price,
                "sl": t.stoploss, "status": t.status,
                "pnl": round(float(t.pnl_usd), 2), "hold_min": hold_m,
            })
        d["recent_trades"] = rows
        return d
    finally:
        if close:
            db.close()


# ─── Strato 2: sintesi LLM ───────────────────────────────────────────────────

SYSTEM_PROMPT = """Sei l'AI Advisor di TradeMachine: un analista quantitativo di \
performance per un bot che copia su MT5 i segnali XAUUSD di un trader Telegram \
(conto prop FTMO, rischio fisso per trade). Ricevi un DOSSIER statistico \
deterministico calcolato sul database dei trade REALI eseguiti.

Il tuo compito: trasformare il dossier in conclusioni utili a MIGLIORARE I \
RISULTATI, distinguendo sempre due piani:
1. L'EDGE DEL TRADER (qualita' dei segnali: quando/come rende, quando no)
2. L'ESECUZIONE NOSTRA (dove il bot perde soldi rispetto al segnale: slippage, \
entrate mancate, violazioni di rischio, churn)

REGOLE FERREE:
- Cita SOLO numeri presenti nel dossier. MAI inventare o estrapolare cifre.
- Se un dato e' insufficiente per una conclusione, dillo (campione piccolo).
- Raccomandazioni CONCRETE e azionabili, ordinate per impatto atteso.
- Considera i caveat del dossier (es. conteggi eventi = lower bound).
- Rispondi in ITALIANO. Riferisci i trade come #id. Orari in ora Roma."""

REPORT_SCHEMA = {
    "type": "object",
    "properties": {
        "executive_summary": {"type": "string"},
        "trader_edge": {"type": "array", "items": {"type": "string"}},
        "execution_gaps": {"type": "array", "items": {"type": "string"}},
        "risk_profile": {"type": "array", "items": {"type": "string"}},
        "patterns": {"type": "array", "items": {"type": "string"}},
        "recommendations": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "title": {"type": "string"},
                    "detail": {"type": "string"},
                    "priority": {"type": "string", "enum": ["alta", "media", "bassa"]},
                },
                "required": ["title", "detail", "priority"],
                "additionalProperties": False,
            },
        },
        "confidence_note": {"type": "string"},
    },
    "required": ["executive_summary", "trader_edge", "execution_gaps",
                 "risk_profile", "patterns", "recommendations", "confidence_note"],
    "additionalProperties": False,
}


def _get_client():
    global _client
    if _client is None:
        api_key = os.getenv("ANTHROPIC_API_KEY")
        if not api_key:
            return None
        import anthropic
        _client = anthropic.Anthropic(api_key=api_key)
    return _client


def _call_llm(dossier: dict):
    """1 chiamata a Claude con structured output. Ritorna (sections, usage_in,
    usage_out) o solleva RuntimeError con motivo chiaro."""
    client = _get_client()
    if client is None:
        raise RuntimeError("ANTHROPIC_API_KEY assente")
    user_msg = ("Analizza questo dossier e produci il report secondo lo schema.\n\n"
                "DOSSIER:\n" + json.dumps(dossier, ensure_ascii=False, default=str))
    response = client.with_options(timeout=180.0).messages.create(
        model=ADVISOR_MODEL,
        max_tokens=16000,
        system=SYSTEM_PROMPT,
        messages=[{"role": "user", "content": user_msg}],
        output_config={"format": {"type": "json_schema", "schema": REPORT_SCHEMA}},
    )
    if response.stop_reason == "refusal":
        raise RuntimeError("richiesta rifiutata dal modello (refusal)")
    text = next((b.text for b in response.content if b.type == "text"), None)
    if not text:
        raise RuntimeError("risposta senza blocco testo")
    sections = json.loads(text)
    u = getattr(response, "usage", None)
    return sections, (u.input_tokens if u else 0), (u.output_tokens if u else 0)


def generate_report(db=None, trigger: str = "manual") -> dict:
    """Genera e salva un report completo. MAI solleva: errori -> status=error."""
    from database import SessionLocal, AiReport
    close = False
    if db is None:
        db = SessionLocal(); close = True
    t0 = time.monotonic()
    report_date = _roma_now().strftime("%Y-%m-%d")
    try:
        dossier = build_dossier(db)
        sections, tin, tout = _call_llm(dossier)
        rep = AiReport(report_date=report_date, model=ADVISOR_MODEL,
                       sections_json=json.dumps(sections, ensure_ascii=False),
                       stats_json=json.dumps(dossier, ensure_ascii=False, default=str),
                       tokens_in=tin, tokens_out=tout,
                       duration_s=round(time.monotonic() - t0, 1), status="ok")
        db.add(rep); db.commit(); db.refresh(rep)
        _log(f"report #{rep.id} ok ({trigger}): {dossier.get('trades_analyzed')} trade, "
             f"{tin}+{tout} tok, {rep.duration_s}s")
        return _report_to_dict(rep)
    except Exception as e:
        err = str(e)[:500]
        _log(f"report ERRORE ({trigger}): {err}")
        try:
            rep = AiReport(report_date=report_date, model=ADVISOR_MODEL,
                           sections_json=None,
                           stats_json=None,
                           duration_s=round(time.monotonic() - t0, 1),
                           status="error", error=err)
            db.add(rep); db.commit(); db.refresh(rep)
            return _report_to_dict(rep)
        except Exception:
            return {"status": "error", "error": err}
    finally:
        if close:
            db.close()


def _report_to_dict(rep) -> dict:
    return {
        "id": rep.id,
        "report_date": rep.report_date,
        "model": rep.model,
        "sections": json.loads(rep.sections_json) if rep.sections_json else None,
        "stats": json.loads(rep.stats_json) if rep.stats_json else None,
        "tokens_in": rep.tokens_in, "tokens_out": rep.tokens_out,
        "duration_s": rep.duration_s,
        "status": rep.status, "error": rep.error,
        "created_at_roma": _utc_to_roma(rep.created_at).strftime("%Y-%m-%d %H:%M:%S")
        if rep.created_at else None,
    }


def get_latest(db=None) -> Optional[dict]:
    from database import SessionLocal, AiReport
    close = False
    if db is None:
        db = SessionLocal(); close = True
    try:
        rep = db.query(AiReport).order_by(AiReport.id.desc()).first()
        return _report_to_dict(rep) if rep else None
    finally:
        if close:
            db.close()


def maybe_run_daily(now_roma=None) -> dict:
    """Auto-run giornaliero: dopo le 23:10 Roma, al massimo 1 report al giorno
    (guardia persistita su ai_reports -> restart-safe). Da chiamare dal loop
    15s dentro run_in_executor. Throttle modulo: check ogni 10 min."""
    global _last_check_utc
    now_utc = datetime.utcnow()
    if _last_check_utc is not None and (now_utc - _last_check_utc) < timedelta(seconds=CHECK_EVERY_S):
        return {"ran": False, "reason": "throttled"}
    _last_check_utc = now_utc

    if now_roma is None:
        now_roma = _roma_now()
    if (now_roma.hour, now_roma.minute) < (DAILY_HOUR_ROMA, DAILY_MINUTE_ROMA):
        return {"ran": False, "reason": "before_schedule"}

    from database import SessionLocal, AiReport
    today = now_roma.strftime("%Y-%m-%d")
    db = SessionLocal()
    try:
        existing = db.query(AiReport).filter(AiReport.report_date == today).first()
        if existing:
            return {"ran": False, "reason": "already_done"}
        _log(f"auto-run giornaliero per {today}")
        res = generate_report(db, trigger="daily")
        return {"ran": True, "status": res.get("status")}
    finally:
        db.close()
