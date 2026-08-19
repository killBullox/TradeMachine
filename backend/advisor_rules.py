"""Gestione operativa dei consigli AIA (decisa dall'utente, sempre).

Tre azioni su un consiglio:
  - Rifiuta: sparisce dal report corrente. Nessuna persistenza: l'advisor puo'
    riproporlo domani se i numeri lo ripromuovono.
  - Monitora (test): regola in MONITOR TEST. Zero effetto sui trade reali; per
    ogni trade chiuso dal momento dell'attivazione si calcola in tempo reale
    cosa sarebbe successo con la regola attiva (delta virtuale vs promesso).
    Da qui: Approva (promozione a reale) o Rollback.
  - Approva: regola in MONITOR REALE. La regola AGISCE sui trade futuri:
      * regole di esclusione -> il segnale bloccato diventa PAPER trade
        (is_filtered=True), cosi' il monitor mostra anche quanto e' costato o
        ha reso il blocco (P&L evitato, simulato dall'infrastruttura paper).
      * scale_risk -> fattore applicato al rischio per trade nel sizing.
      * regole modify-only (cap_loss_at_risk): nessun enforcement aggiuntivo
        possibile in sicurezza -> monitor-only, dichiarato.
    Da qui: Rollback (la regola si disattiva, lo storico resta).

Precisione anti-artefatto: i monitor partono dal SECONDO di attivazione.
"""
import json
from datetime import datetime, timezone
from typing import Optional

EXCLUSION_RULES = ("exclude_hours", "exclude_sessions", "exclude_weekdays",
                   "exclude_direction", "min_rr_tp1", "exclude_near_news")
MONITOR_ONLY_REAL = ("cap_loss_at_risk",)   # nessun enforcement reale sicuro

_FILTER_PREFIX = "Regola AIA #"   # marker nel filter_reason dei trade bloccati


def _real_enforceable(sim_type: str, sim_params) -> tuple:
    """(ok, motivo). Le policy replay hanno enforcement reale SOLO se mappate
    su un toggle del sistema (be_at_tp1/trail); le altre restano Monitor Test."""
    if sim_type == "entry_policy":
        return (False, "policy d'ingresso: enforcement reale non disponibile "
                       "in v1 — usa Monitor Test")
    if sim_type == "mgmt_policy":
        import replay_engine
        policy = (sim_params if isinstance(sim_params, dict)
                  else json.loads(sim_params or "{}")).get("policy")
        if policy not in replay_engine.MGMT_REAL_TOGGLES:
            return (False, f"policy '{policy}': enforcement reale non disponibile "
                           f"in v1 — usa Monitor Test")
    return (True, "")


def _apply_mgmt_toggle(db, rule, activate: bool) -> Optional[str]:
    """mgmt_policy mappata su un toggle reale (no_be -> be_at_tp1_enabled=False,
    trail_progressive -> trail_stop_enabled=True): applica al passaggio in
    Monitor Reale, ripristina al rollback. Mai solleva."""
    try:
        if rule.sim_type != "mgmt_policy":
            return None
        import replay_engine
        policy = json.loads(rule.sim_params or "{}").get("policy")
        tg = replay_engine.MGMT_REAL_TOGGLES.get(policy)
        if not tg:
            return None
        from database import RiskSettings
        rs = db.query(RiskSettings).first()
        if rs is None:
            return None
        val = tg["value"] if activate else tg["restore"]
        setattr(rs, tg["field"], val)
        rs.updated_at = datetime.utcnow()
        db.commit()
        _log(f"regola #{rule.id}: toggle {tg['field']} -> {val} "
             f"({'attivazione' if activate else 'ripristino'})")
        return tg["field"]
    except Exception as e:
        _log(f"_apply_mgmt_toggle err: {str(e)[:120]}")
        return None


def _log(msg: str):
    try:
        from mt5_trader import log as _l
        _l(f"[AIARules] {msg}")
    except Exception:
        print(f"[AIARules] {msg}", flush=True)


def _canonical(params) -> str:
    try:
        return json.dumps(params or {}, sort_keys=True)
    except Exception:
        return "{}"


def _roma_str(dt):
    if dt is None:
        return None
    from zoneinfo import ZoneInfo
    return dt.replace(tzinfo=timezone.utc).astimezone(
        ZoneInfo("Europe/Rome")).strftime("%Y-%m-%d %H:%M:%S")


# ─── CRUD ────────────────────────────────────────────────────────────────────

def create_rule(sim_type: str, sim_params: dict, title: str, mode: str,
                expected: Optional[dict] = None, source_detail: str = None,
                db=None) -> dict:
    """Crea una regola in Monitor Test o Monitor Reale. Rifiuta duplicati
    attivi della stessa regola (stesso sim_type+params)."""
    from database import SessionLocal, AdvisorRule
    import sim_engine
    if mode not in ("test", "real"):
        return {"ok": False, "error": f"mode invalido: {mode}"}
    if sim_type not in sim_engine.SUPPORTED_RULES:
        return {"ok": False, "error": f"regola non supportata: {sim_type}"}
    if mode == "real":
        ok_real, why = _real_enforceable(sim_type, sim_params)
        if not ok_real:
            return {"ok": False, "error": why}
    close = False
    if db is None:
        db = SessionLocal(); close = True
    try:
        canon = _canonical(sim_params)
        dup = (db.query(AdvisorRule)
               .filter(AdvisorRule.sim_type == sim_type,
                       AdvisorRule.sim_params == canon,
                       AdvisorRule.status == "active").first())
        if dup:
            return {"ok": False,
                    "error": f"regola gia' attiva in Monitor {'Test' if dup.mode == 'test' else 'Reale'} (#{dup.id})"}
        now = datetime.utcnow()
        rule = AdvisorRule(
            sim_type=sim_type, sim_params=canon, title=(title or sim_type)[:300],
            source_detail=source_detail, mode=mode, status="active",
            expected_json=json.dumps(expected, ensure_ascii=False) if expected else None,
            activated_at=now,
            test_started_at=now if mode == "test" else None,
            real_started_at=now if mode == "real" else None,
        )
        db.add(rule); db.commit(); db.refresh(rule)
        if mode == "real":
            _apply_mgmt_toggle(db, rule, activate=True)
        _log(f"regola #{rule.id} creata: {sim_type} {canon} mode={mode}")
        return {"ok": True, "rule": rule_to_dict(rule, db)}
    finally:
        if close:
            db.close()


def promote_rule(rule_id: int, db=None) -> dict:
    """Monitor Test -> Monitor Reale. Il monitor reale riparte da ORA."""
    from database import SessionLocal, AdvisorRule
    close = False
    if db is None:
        db = SessionLocal(); close = True
    try:
        rule = db.query(AdvisorRule).filter(AdvisorRule.id == rule_id).first()
        if not rule or rule.status != "active":
            return {"ok": False, "error": "regola non trovata o non attiva"}
        if rule.mode != "test":
            return {"ok": False, "error": "solo una regola in Monitor Test puo' essere approvata"}
        ok_real, why = _real_enforceable(rule.sim_type, rule.sim_params)
        if not ok_real:
            return {"ok": False, "error": why}
        now = datetime.utcnow()
        rule.mode = "real"
        rule.real_started_at = now
        rule.activated_at = now      # il monitor reale conta da adesso
        db.commit(); db.refresh(rule)
        _apply_mgmt_toggle(db, rule, activate=True)
        _log(f"regola #{rule.id} promossa a REALE")
        return {"ok": True, "rule": rule_to_dict(rule, db)}
    finally:
        if close:
            db.close()


def rollback_rule(rule_id: int, db=None) -> dict:
    from database import SessionLocal, AdvisorRule
    close = False
    if db is None:
        db = SessionLocal(); close = True
    try:
        rule = db.query(AdvisorRule).filter(AdvisorRule.id == rule_id).first()
        if not rule or rule.status != "active":
            return {"ok": False, "error": "regola non trovata o non attiva"}
        was_real = rule.mode == "real"
        rule.status = "rolled_back"
        rule.rolled_back_at = datetime.utcnow()
        db.commit()
        if was_real:
            _apply_mgmt_toggle(db, rule, activate=False)
        _log(f"regola #{rule.id} rollback ({rule.mode})")
        return {"ok": True}
    finally:
        if close:
            db.close()


def active_rules(db, mode: Optional[str] = None):
    from database import AdvisorRule
    q = db.query(AdvisorRule).filter(AdvisorRule.status == "active")
    if mode:
        q = q.filter(AdvisorRule.mode == mode)
    return q.order_by(AdvisorRule.id).all()


# ─── Enforcement reale ───────────────────────────────────────────────────────

def check_signal_block(sig, db) -> Optional[str]:
    """Valuta il nuovo segnale contro le regole di ESCLUSIONE approvate (Monitor
    Reale). Ritorna il motivo del blocco (-> paper trade) o None. Mai solleva.

    exclude_setup: il contesto ICT viene calcolato IN TEMPO REALE al momento
    del segnale (candele fino ad adesso) — solo se una regola simile e' attiva,
    per non aggiungere latenza quando non serve."""
    try:
        import sim_engine
        rules = [r for r in active_rules(db, mode="real")
                 if r.sim_type in EXCLUSION_RULES or r.sim_type == "exclude_setup"]
        if not rules:
            return None
        live_setup = None
        setup_rules = [r for r in rules if r.sim_type == "exclude_setup"]
        if setup_rules:
            try:
                import ict_engine
                ctx_live = ict_engine.build_context_for_signal(sig)
                live_setup = ctx_live.get("setup")
                _log(f"#{sig.id} contesto ICT live all'intake: {live_setup}")
            except Exception as e:
                _log(f"#{sig.id} contesto ICT live err: {str(e)[:100]}")
        for r in rules:
            params = json.loads(r.sim_params or "{}")
            if r.sim_type == "exclude_setup":
                if live_setup and live_setup not in ("no_data", "no_context") \
                        and live_setup == str(params.get("setup")):
                    return f"{_FILTER_PREFIX}{r.id}: {r.title} (setup live: {live_setup})"
                continue
            ctx = {"news_times": []}
            if r.sim_type == "exclude_near_news":
                from database import NewsEvent
                ctx["news_times"] = [e.event_time for e in db.query(NewsEvent).all()
                                     if e.event_time]
            action, _ = sim_engine._decide(r.sim_type, params, sig, ctx)
            if action == "exclude":
                return f"{_FILTER_PREFIX}{r.id}: {r.title}"
    except Exception as e:
        _log(f"check_signal_block err: {str(e)[:120]}")
    return None


def scale_risk_factor(db=None) -> float:
    """Fattore di rischio da regole scale_risk approvate (Monitor Reale).
    1.0 se nessuna. Mai solleva."""
    from database import SessionLocal
    close = False
    if db is None:
        db = SessionLocal(); close = True
    try:
        f = 1.0
        for r in active_rules(db, mode="real"):
            if r.sim_type == "scale_risk":
                try:
                    f *= float(json.loads(r.sim_params or "{}").get("factor", 1.0))
                except Exception:
                    pass
        return f
    except Exception:
        return 1.0
    finally:
        if close:
            db.close()


# ─── Monitor (statistiche live) ──────────────────────────────────────────────

def _since_str(rule) -> str:
    return (rule.activated_at or rule.created_at).strftime("%Y-%m-%d %H:%M:%S")


def _monitor_test(rule, db) -> dict:
    """Cosa SAREBBE successo con la regola attiva, dai trade chiusi dopo
    l'attivazione. Delta virtuale esatto (motore di simulazione)."""
    import sim_engine
    params = json.loads(rule.sim_params or "{}")
    res = sim_engine.simulate(rule.sim_type, params, db, since=_since_str(rule))
    if not res.get("ok"):
        return {"ok": False, "error": res.get("error")}
    return {
        "ok": True, "tipo": "virtuale",
        "trade_nel_periodo": res["baseline"]["trades"],
        "trade_toccati": res["trades_excluded"] + res["trades_modified"],
        "pnl_reale": res["baseline"]["pnl"],
        "pnl_con_regola": res["simulated"]["pnl"],
        "delta_osservato": res["delta_pnl"],
        "win_rate_reale": res["baseline"]["win_rate"],
        "win_rate_con_regola": res["simulated"]["win_rate"],
    }


def _monitor_real(rule, db) -> dict:
    """Effetti REALI della regola: trade bloccati (paper, col loro esito
    simulato) o rischio scalato."""
    from database import Signal
    params = json.loads(rule.sim_params or "{}")
    since_dt = rule.activated_at or rule.created_at
    if rule.sim_type in EXCLUSION_RULES:
        blocked = (db.query(Signal)
                   .filter(Signal.is_filtered == True,  # noqa: E712
                           Signal.filter_reason.like(f"{_FILTER_PREFIX}{rule.id}:%"),
                           Signal.created_at >= since_dt)
                   .all())
        settled = [b for b in blocked if b.pnl_usd is not None]
        avoided = -sum(float(b.pnl_usd) for b in settled)
        return {
            "ok": True, "tipo": "reale_blocco",
            "trade_bloccati": len(blocked),
            "esiti_simulati_disponibili": len(settled),
            "pnl_evitato": round(avoided, 2),   # >0 = il blocco ha risparmiato
            "bloccati_sample": [{"id": b.id, "pnl_simulato": b.pnl_usd}
                                for b in blocked[-10:]],
        }
    if rule.sim_type == "scale_risk":
        factor = float(params.get("factor", 1.0)) or 1.0
        from ai_advisor import _real_closed_trades
        import sim_engine
        trades = sim_engine._filter_since(_real_closed_trades(db), _since_str(rule))
        actual = sum(float(t.pnl_usd) for t in trades)
        unscaled = actual / factor if factor else actual
        return {
            "ok": True, "tipo": "reale_scala",
            "trade_nel_periodo": len(trades),
            "pnl_reale_scalato": round(actual, 2),
            "pnl_stimato_senza_scala": round(unscaled, 2),
            "delta_osservato": round(actual - unscaled, 2),
        }
    if rule.sim_type == "mgmt_policy":
        # toggle reale attivo: i trade nuovi seguono GIA' la nuova gestione ->
        # confronto reale osservato vs controfattuale (gestione precedente)
        import replay_engine
        policy = params.get("policy")
        tg = replay_engine.MGMT_REAL_TOGGLES.get(policy)
        if tg:
            out = replay_engine.monitor_real_toggle(db, _since_str(rule))
            out["nota"] = f"effetto reale attivo via impostazione {tg['field']}"
            return out
    # monitor-only (es. cap_loss_at_risk): stessa meccanica del test, dichiarata
    out = _monitor_test(rule, db)
    out["tipo"] = "monitor_only"
    out["nota"] = "regola senza enforcement automatico sicuro: monitoraggio virtuale"
    return out


def rule_to_dict(rule, db) -> dict:
    exp = None
    try:
        exp = json.loads(rule.expected_json) if rule.expected_json else None
    except Exception:
        pass
    monitor = _monitor_test(rule, db) if rule.mode == "test" else _monitor_real(rule, db)
    return {
        "id": rule.id,
        "sim_type": rule.sim_type,
        "sim_params": json.loads(rule.sim_params or "{}"),
        "title": rule.title,
        "mode": rule.mode,
        "status": rule.status,
        "attiva_dal_roma": _roma_str(rule.activated_at),
        "delta_promesso": (exp or {}).get("delta_pnl"),
        "expected": exp,
        "monitor": monitor,
    }


def list_rules(db=None) -> dict:
    """Monitor Test e Monitor Reale, con statistiche live. Sezioni DISTINTE."""
    from database import SessionLocal
    close = False
    if db is None:
        db = SessionLocal(); close = True
    try:
        return {
            "test": [rule_to_dict(r, db) for r in active_rules(db, mode="test")],
            "real": [rule_to_dict(r, db) for r in active_rules(db, mode="real")],
        }
    finally:
        if close:
            db.close()


def active_summary(db) -> dict:
    """Riassunto compatto per il dossier LLM: regole gia' in gestione (l'advisor
    NON deve riproporle come consigli nuovi)."""
    out = {"in_monitor_test": [], "attive_reali": []}
    for r in active_rules(db):
        entry = {"sim_type": r.sim_type, "sim_params": json.loads(r.sim_params or "{}"),
                 "title": r.title, "attiva_dal": _roma_str(r.activated_at)}
        (out["in_monitor_test"] if r.mode == "test" else out["attive_reali"]).append(entry)
    return out


def find_active(db, sim_type: str, sim_params) -> Optional[object]:
    from database import AdvisorRule
    return (db.query(AdvisorRule)
            .filter(AdvisorRule.sim_type == sim_type,
                    AdvisorRule.sim_params == _canonical(sim_params),
                    AdvisorRule.status == "active").first())
