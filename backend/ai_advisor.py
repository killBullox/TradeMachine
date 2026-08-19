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

ADVISOR_MODEL = "claude-fable-5"
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

        # Protezioni GIA' ATTIVE nel sistema, con data ed efficacia misurata:
        # l'advisor NON deve consigliare cio' che esiste gia', ma valutarne
        # l'effetto (problema residuo dopo l'attivazione vs storico precedente).
        try:
            import sim_engine as _se
            prot_block = []
            for p in _se.ACTIVE_PROTECTIONS:
                entry = {"nome": p["nome"], "attiva_dal": p["attiva_dal"]}
                if p.get("sim_type"):
                    full = _se.simulate(p["sim_type"], p.get("sim_params") or {}, db)
                    resid = _se.simulate(p["sim_type"], p.get("sim_params") or {}, db,
                                         since=p["attiva_dal"])
                    if full.get("ok") and resid.get("ok"):
                        entry["problema_storico_totale_delta"] = full["delta_pnl"]
                        entry["problema_residuo_dopo_attivazione"] = {
                            "delta": resid["delta_pnl"],
                            "trade_toccati": resid["trades_excluded"] + resid["trades_modified"],
                            "trade_nel_periodo": resid["baseline"]["trades"],
                        }
                prot_block.append(entry)
            d["protezioni_attive"] = prot_block
        except Exception as _e:
            _log(f"protezioni block err: {str(_e)[:120]}")
            d["protezioni_attive"] = []

        # Strategia del trader (contesto ICT): setup rilevati algoritmicamente
        # dalle candele attorno a ogni entry (ict_engine), con esiti per setup,
        # kill zone, premium/discount, with/counter trend.
        try:
            import ict_engine as _ict
            d["strategia_trader"] = _ict.strategy_stats(db)
        except Exception as _e:
            _log(f"strategy stats err: {str(_e)[:120]}")
            d["strategia_trader"] = {}

        # Regole AIA gia' in gestione utente (Monitor Test / Reale): l'advisor
        # non deve riproporle come consigli nuovi, ma puo' commentarne l'esito.
        try:
            import advisor_rules as _ar
            d["regole_aia_in_gestione"] = _ar.active_summary(db)
        except Exception:
            d["regole_aia_in_gestione"] = {"in_monitor_test": [], "attive_reali": []}

        # Registro consigli aperti (persistenza giorno per giorno): l'LLM deve
        # riusare le key esistenti quando ripropone lo stesso consiglio.
        try:
            import advisor_registry as _reg
            d["registro_consigli_aperti"] = _reg.open_for_dossier(db)
        except Exception:
            d["registro_consigli_aperti"] = []

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
- Rispondi in ITALIANO. Riferisci i trade come #id. Orari in ora Roma.

CONSIGLI GIA' VERIFICATI (regola fondamentale — MAI autosmentirsi):
Il dossier contiene "validated_rules_sweep": la batteria di regole GIA' testata
e validata statisticamente dal motore di simulazione (gate: campione minimo,
robustezza senza il miglior singolo trade, confidenza bootstrap).
- Le raccomandazioni SIMULABILI devono venire ESCLUSIVAMENTE dalla lista
  "promosse": copia sim_type e sim_params ESATTI dall'entry scelta. Cita nel
  detail il delta, il campione (n_affected) e la confidenza bootstrap.
- Le regole in "bocciate_interessanti" (delta positivo ma campione fragile o
  concentrato su pochi trade) NON vanno MAI raccomandate: se rilevanti, citale
  in patterns come "testato e scartato: <motivo del fail>". Questo e' il
  valore: distinguere segnale da illusione statistica.
- Le regole con delta negativo dimostrano che un'idea NON funziona: usale per
  smontare false intuizioni (in patterns o trader_edge), mai in recommendations.
- Se nessuna regola promossa esiste, dillo apertamente: meglio zero consigli
  quantificati che consigli non verificati.

REPLAY TICK (gestione e ingresso — da v2 SONO quantificati): il dossier
contiene "gestione_replay_sweep": ogni trade reale e' stato RIGIOCATO sui tick
veri di MT5 e la batteria confronta la gestione corrente (BE a TP1, trail off)
con policy alternative di GESTIONE (no_be, trail_progressive, close_all_tp1,
close_all_tp2) e di INGRESSO (market_immediate, market_if_near_2/5 = MARKET se
il prezzo e' entro 2/5$ dal range, altrimenti LIMIT come oggi).
- Anche qui vale la regola ferrea: raccomanda SOLO le policy in "promosse",
  con sim_type "mgmt_policy" o "entry_policy" e sim_params ESATTI, es.
  {"policy": "close_all_tp2"}. Le bocciate citale come testate-e-scartate.
- Cita sempre la coverage (trade replayati, esclusi per fidelity): il replay
  e' onesto sui propri limiti.
- I consigli di gestione NON coperti dalla batteria (es. processi operativi,
  latenza infrastrutturale) restano leciti con sim_type="none",
  dichiarando nel detail che l'impatto non e' quantificato.

PROTEZIONI GIA' ATTIVE (regola fondamentale — mai consigliare l'esistente):
Il dossier contiene "protezioni_attive" (difese gia' implementate nel sistema,
con data di attivazione ed efficacia misurata) e lo sweep contiene
"gia_coperte" (regole corrispondenti a protezioni esistenti, senza problema
residuo validato).
- MAI raccomandare una protezione gia' attiva o una regola in "gia_coperte":
  sarebbe consigliare cio' che e' gia' stato fatto.
- I delta STORICI precedenti all'attivazione di una protezione NON sono
  opportunita' future: sono danni del passato gia' curati. Non presentarli
  come guadagno ottenibile.
- Al posto del consiglio, VALUTA L'EFFICACIA della protezione: se il problema
  residuo dopo l'attivazione e' nullo o basso, di' che la difesa sta
  funzionando (in execution_gaps o patterns, coi numeri residui). Se una
  regola coperta compare comunque in "promosse" (problema residuo validato),
  allora la protezione NON basta: raccomanda il rafforzamento citando SOLO i
  numeri del periodo residuo.

REGOLE GIA' IN GESTIONE UTENTE: il dossier contiene "regole_aia_in_gestione"
(regole che l'utente ha gia' messo in Monitor Test o approvato in Monitor
Reale). NON riproporle come raccomandazioni nuove: l'utente le sta gia'
gestendo. Puoi commentarne l'andamento in patterns se rilevante.

STRATEGIA DEL TRADER (contesto ICT): il trader opera con concetti ICT/Smart
Money. Il dossier contiene "strategia_trader": per ogni trade il sistema ha
ricostruito il contesto dalle candele M5/M15 attorno all'entry e classificato
il SETUP: sweep_reversal (entry dopo liquidity sweep), ob_retest (retest di
order block), fvg_entry (entry dentro un fair value gap), bos_retest (break of
structure con retest), bos_no_retest (BOS inseguito senza conferma),
counter_trend (contro il bias M15), no_context. Con esiti per setup, kill zone
(London/NY), premium/discount, with/counter trend.
- La sezione "strategia_trader" del report e' il posto per la SINTESI: cosa fa
  sistematicamente il trader, quali setup gli rendono e quali lo danneggiano,
  cosa ignora o abusa (es. "entra spesso su BOS senza retest: X trade, WR Y%,
  Z$ — dovrebbe aspettare conferme"), sempre coi numeri del dossier.
- Un setup dannoso puo' diventare una raccomandazione SOLO se la regola
  exclude_setup corrispondente e' nelle "promosse" dello sweep (stessi gate
  statistici di tutto il resto). Copia sim_type/sim_params esatti.
- Considera coverage: se pochi trade hanno contesto, dillo.
- I setup sono approssimazioni algoritmiche consistenti di concetti in parte
  discrezionali: parlane come "rilevati dal sistema", non come verita' assolute.

RACCOMANDAZIONI = SOLO AZIONABILI (regola ferrea, richiesta esplicita del
trader): una raccomandazione esiste SOLO se porta con se' le soluzioni.
- Ogni raccomandazione DEVE avere "azioni": la lista di SOLUZIONI CONCRETE e
  implementabili. O la regola promossa (sim_type/params: l'azione e' la regola
  stessa), o interventi operativi SPECIFICI: cosa fare esattamente, dove, con
  quali numeri attesi (es. per la latenza: 'policy MARKET testata sul replay:
  bocciata, delta -X$' oppure 'misurare il ping VPS->server broker e valutare
  colocation', 'ridurre lo step parser->ordine da X a Y').
- VIETATO raccomandare 'monitorare', 'presidiare', 'tracciare', 'valutare',
  'prestare attenzione': non sono azioni. Quelle considerazioni vanno in
  execution_gaps / risk_profile / patterns, NON in recommendations.
- Un problema senza soluzione concreta NON genera una raccomandazione: lo
  descrivi nelle sezioni di analisi e basta. Meglio zero raccomandazioni che
  raccomandazioni-chiacchiera. Il codice sposta d'ufficio in 'osservazioni'
  qualsiasi raccomandazione senza azioni concrete.

REGISTRO CONSIGLI (persistenza giorno per giorno): il dossier contiene
"registro_consigli_aperti" — i consigli ancora aperti dai report precedenti,
ognuno con la sua chiave stabile "key", la data di prima apparizione e quante
volte e' stato riconfermato.
- Ogni raccomandazione DEVE avere il campo "key": se il consiglio e'
  sostanzialmente lo stesso di una voce del registro, RIUSA esattamente la sua
  key (anche se riformuli il testo). Se e' un consiglio nuovo, crea una key
  nuova, breve, in kebab-case (es. "ridurre-latenza-fill").
- MAI riusare una key del registro per un consiglio diverso.
- Un consiglio del registro che i dati odierni non supportano piu' NON va
  riproposto: il motore lo invalidera' da solo, col motivo tracciato."""

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
                    "key": {"type": "string"},
                    "title": {"type": "string"},
                    "detail": {"type": "string"},
                    "azioni": {"type": "array", "items": {"type": "string"},
                               "minItems": 1},
                    "priority": {"type": "string", "enum": ["alta", "media", "bassa"]},
                    "sim_type": {"type": "string",
                                 "enum": ["none", "exclude_hours", "exclude_sessions",
                                          "exclude_weekdays", "exclude_direction",
                                          "min_rr_tp1", "cap_loss_at_risk",
                                          "scale_risk", "exclude_near_news",
                                          "exclude_setup", "mgmt_policy",
                                          "entry_policy"]},
                    "sim_params": {"type": "string"},
                },
                "required": ["key", "title", "detail", "azioni", "priority",
                             "sim_type", "sim_params"],
                "additionalProperties": False,
            },
        },
        "confidence_note": {"type": "string"},
    },
    "required": ["executive_summary", "trader_edge", "execution_gaps",
                 "risk_profile", "patterns", "strategia_trader",
                 "recommendations", "confidence_note"],
    "additionalProperties": False,
}
REPORT_SCHEMA["properties"]["strategia_trader"] = {
    "type": "array", "items": {"type": "string"}}


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
    # output_config + thinking via extra_body: funziona su qualsiasi versione
    # dell'SDK (kwarg tipizzati solo nelle piu' recenti; sul wire e' identico).
    # Adaptive thinking: il modello ragiona quanto serve prima della sintesi
    # (budget_tokens NON supportato dai modelli Claude 5: solo adaptive).
    response = client.with_options(timeout=300.0).messages.create(
        model=ADVISOR_MODEL,
        max_tokens=32000,
        system=SYSTEM_PROMPT,
        messages=[{"role": "user", "content": user_msg}],
        extra_body={
            "output_config": {"format": {"type": "json_schema", "schema": REPORT_SCHEMA}},
            "thinking": {"type": "adaptive"},
        },
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
        import sim_engine
        # Contesti ICT: calcola (incrementale) il contesto dei trade nuovi
        # prima del dossier, cosi' strategia_trader e sweep li vedono.
        try:
            import ict_engine
            ict_engine.ensure_contexts(db)
        except Exception as _e:
            _log(f"ensure_contexts err: {str(_e)[:120]}")
        # Replay tick: cache incrementale (i trade nuovi si aggiungono alla
        # chiusura; qui il backfill di sicurezza) + sweep policy.
        try:
            import replay_engine
            replay_engine.ensure_replays(db)
        except Exception as _e:
            _log(f"ensure_replays err: {str(_e)[:120]}")
        dossier = build_dossier(db)
        # Sweep sistematico PRE-LLM: regole gia' testate/validate. L'LLM puo'
        # raccomandare SOLO le promosse (le bocciate le cita come scartate).
        try:
            dossier["validated_rules_sweep"] = sim_engine.sweep(db)
        except Exception as _e:
            _log(f"sweep errore: {str(_e)[:120]}")
            dossier["validated_rules_sweep"] = {"promosse": [], "bocciate_interessanti": []}
        try:
            import replay_engine
            dossier["gestione_replay_sweep"] = replay_engine.replay_sweep(db)
        except Exception as _e:
            _log(f"replay sweep errore: {str(_e)[:120]}")
            dossier["gestione_replay_sweep"] = {"promosse": [], "bocciate_interessanti": []}
        sections, tin, tout = _call_llm(dossier)
        _enforce_actionable(sections)
        _attach_impacts(sections, db)
        _ensure_promoted_actionable(sections, dossier.get("validated_rules_sweep") or {}, db)
        _ensure_promoted_actionable(sections, dossier.get("gestione_replay_sweep") or {}, db)
        # Registro persistente: upsert dei consigli odierni, riproposta delle
        # voci aperte omesse dall'LLM (standing), invalidazione motivata di
        # quelle che i dati non supportano piu'.
        try:
            import advisor_registry
            advisor_registry.reconcile(sections, db, report_date)
        except Exception as _e:
            _log(f"registry reconcile err: {str(_e)[:150]}")
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


_NON_AZIONI = ("monitorar", "presidiar", "tracciar", "osservar", "valutar",
               "prestare attenzione", "tenere d'occhio", "attenzionar")


def _enforce_actionable(sections: dict) -> None:
    """ENFORCEMENT 'solo azionabili' (richiesta esplicita del trader): una
    raccomandazione senza regola simulabile E senza azioni concrete non e' una
    raccomandazione — viene spostata d'ufficio in sections['osservazioni'].
    Un'azione fatta solo di verbi-osservazione (monitorare/presidiare/...)
    non conta come azione. Mai solleva."""
    try:
        recs = (sections or {}).get("recommendations", [])
        kept, observations = [], []
        for rec in recs:
            actionable_rule = rec.get("sim_type") and rec.get("sim_type") != "none"
            azioni = [a for a in (rec.get("azioni") or [])
                      if isinstance(a, str) and a.strip()
                      and not any(v in a.lower()[:40] for v in _NON_AZIONI)]
            if actionable_rule or azioni:
                rec["azioni"] = azioni
                kept.append(rec)
            else:
                observations.append(rec)
        sections["recommendations"] = kept
        if observations:
            sections.setdefault("osservazioni", []).extend(
                {"title": r.get("title"), "detail": r.get("detail")}
                for r in observations)
            _log(f"{len(observations)} consigli senza azioni -> osservazioni")
    except Exception as e:
        _log(f"_enforce_actionable err: {str(e)[:120]}")


def _attach_impacts(sections: dict, db) -> None:
    """ENFORCEMENT anti-autosmentita: ogni raccomandazione simulabile viene
    ri-validata dal motore (simulate + gate statistici). Se NON supera la
    validazione viene DEGRADATA d'ufficio in sections['scartate_dalla_verifica']
    — anche se l'LLM l'aveva proposta. Doppia cintura: prompt + codice."""
    import sim_engine
    recs = (sections or {}).get("recommendations", [])
    kept, demoted = [], []
    for rec in recs:
        st = rec.get("sim_type")
        if not st or st == "none":
            kept.append(rec)          # non quantificabile: lecito, dichiarato
            continue
        # Regola gia' in gestione utente (Monitor Test/Reale): non e' un
        # consiglio nuovo, l'utente la sta gia' gestendo -> demotion.
        try:
            import advisor_rules as _ar
            _params_chk = json.loads(rec.get("sim_params") or "{}")
            existing = _ar.find_active(db, st, _params_chk)
            if existing:
                rec["demotion_reason"] = (
                    f"gia' in gestione: Monitor {'Test' if existing.mode == 'test' else 'Reale'} "
                    f"(regola #{existing.id}, attiva dal {existing.activated_at:%Y-%m-%d %H:%M} UTC)")
                demoted.append(rec)
                continue
        except Exception:
            pass
        # Regola coperta da protezione gia' attiva: si valuta SOLO il problema
        # residuo dopo l'attivazione. Consigliare l'esistente e' vietato.
        prot = sim_engine.COVERED_RULES.get(st)
        since = prot["attiva_dal"] if prot else None
        try:
            params = json.loads(rec.get("sim_params") or "{}")
            rec["impact"] = sim_engine.validate(st, params, db, since=since)
            if prot and rec["impact"].get("ok"):
                rec["impact"]["covered_by"] = {"protezione": prot["nome"],
                                               "attiva_dal": prot["attiva_dal"]}
        except Exception as e:
            rec["impact"] = {"ok": False, "error": str(e)[:200]}
        v = (rec["impact"] or {}).get("validation") or {}
        if rec["impact"].get("ok") and v.get("passed"):
            kept.append(rec)
        else:
            base_reason = ("; ".join(v.get("fail_reasons", []))
                           or rec["impact"].get("error")
                           or "validazione statistica non superata")
            if prot:
                base_reason = (f"gia' coperta dalla protezione '{prot['nome']}' "
                               f"(attiva dal {prot['attiva_dal']}); nel periodo successivo "
                               f"il problema residuo non e' validato: {base_reason}")
            rec["demotion_reason"] = base_reason
            demoted.append(rec)
    if sections is not None:
        sections["recommendations"] = kept
        if demoted:
            sections["scartate_dalla_verifica"] = demoted


def _ensure_promoted_actionable(sections: dict, sweep: dict, db) -> None:
    """GARANZIA STRUTTURALE: ogni regola PROMOSSA dallo sweep deve comparire tra
    le raccomandazioni in forma AZIONABILE (sim_type+params corretti, quindi coi
    bottoni Rifiuta/Monitora/Approva in UI). Se l'LLM l'ha descritta solo a
    parole (sim_type='none') o l'ha omessa, il codice la aggiunge come
    raccomandazione sintetica coi numeri del motore. Mai solleva."""
    try:
        import sim_engine
        import advisor_rules as _ar
        recs = sections.setdefault("recommendations", [])

        def _canon(p):
            try:
                return json.dumps(json.loads(p) if isinstance(p, str) else (p or {}),
                                  sort_keys=True)
            except Exception:
                return "{}"

        claimed = {(r.get("sim_type"), _canon(r.get("sim_params")))
                   for r in recs if r.get("sim_type") and r.get("sim_type") != "none"}
        for e in sweep.get("promosse", []):
            key = (e["sim_type"], _canon(e["sim_params"]))
            if key in claimed:
                continue
            params = json.loads(e["sim_params"] or "{}")
            # gia' in gestione utente? allora non va riproposta
            if _ar.find_active(db, e["sim_type"], params):
                continue
            prot = sim_engine.COVERED_RULES.get(e["sim_type"])
            impact = sim_engine.validate(e["sim_type"], params, db,
                                         since=prot["attiva_dal"] if prot else None)
            if not (impact.get("ok") and (impact.get("validation") or {}).get("passed")):
                continue    # dev'essere ancora valida al momento del report
            conf = e.get("bootstrap_confidence")
            recs.append({
                "title": f"Regola validata dal motore: {e['sim_type']} {e['sim_params']}",
                "detail": (f"Promossa dallo sweep sistematico: delta {e['delta_pnl']}$ su "
                           f"{e['n_affected']} trade toccati"
                           + (f", confidenza bootstrap {round(conf * 100)}%" if conf is not None else "")
                           + ". Aggiunta automaticamente dal motore perche' il report "
                             "non la includeva in forma azionabile."),
                "priority": "alta" if abs(e.get("delta_pnl") or 0) >= 1000 else "media",
                "sim_type": e["sim_type"],
                "sim_params": e["sim_params"],
                "impact": impact,
                "synthetic": True,
            })
            _log(f"raccomandazione sintetica aggiunta: {e['sim_type']} {e['sim_params']}")
    except Exception as e:
        _log(f"ensure_promoted_actionable err: {str(e)[:150]}")


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
