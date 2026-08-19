"""Registro persistente dei consigli AIA.

Problema risolto: prima di questo modulo ogni report ricreava i consigli da
zero — un consiglio ignorato poteva ricomparire riformulato (irriconoscibile),
o sparire a capriccio della prosa dell'LLM; nessuna memoria di "aperto da N
giorni". Ora ogni consiglio e' un'ENTITA' persistita (tabella advisor_recs)
con chiave stabile e ciclo di vita:

  open         in attesa di decisione utente; riconfermato/aggiornato a ogni
               report (dall'LLM o, per i quantificati, dal motore anche se
               l'LLM lo dimentica)
  in_gestione  l'utente ha creato una regola AIA (Monitor Test/Reale): il
               consiglio esce dal giro; rollback della regola -> torna open
  invalidated  la rivalidazione statistica e' fallita: il consiglio sparisce
               dai report con il motivo registrato (se i dati tornano a
               supportarlo, si riapre da solo)

Chiavi stabili: consigli quantificati -> "sim_type|params_canonici" (derivata
dal codice, l'LLM non puo' sbagliarla); consigli non quantificati (sim_type
none) -> key kebab-case scelta dall'LLM, che riceve il registro aperto nel
dossier con l'istruzione di riusare le key esistenti.

Il Rifiuta resta NON permanente (modello utente): incrementa rejected_count e
il consiglio puo' ripresentarsi al report successivo, col contatore visibile.
"""
import json
import re
from datetime import datetime
from typing import Optional


def _log(msg: str):
    try:
        from mt5_trader import log as _l
        _l(f"[AIARegistry] {msg}")
    except Exception:
        print(f"[AIARegistry] {msg}", flush=True)


def canon_params(params) -> str:
    """JSON canonico (chiavi ordinate) per confronti stabili."""
    try:
        if isinstance(params, str):
            params = json.loads(params or "{}")
        return json.dumps(params or {}, sort_keys=True)
    except Exception:
        return "{}"


def _slugify(text: str) -> str:
    s = re.sub(r"[^a-z0-9]+", "-", (text or "").lower()).strip("-")
    return s[:80] or "consiglio"


def key_of(rec: dict) -> str:
    """Chiave stabile di una raccomandazione. Per le simulabili la deriva il
    CODICE (sim_type+params canonici): l'LLM non puo' sbagliarla. Per le
    none-type usa la key dell'LLM (o lo slug del titolo come fallback)."""
    st = rec.get("sim_type")
    if st and st != "none":
        return f"{st}|{canon_params(rec.get('sim_params'))}"
    return "none|" + _slugify(rec.get("key") or rec.get("title"))


def _find(db, rec_key: str):
    from database import AdvisorRec
    return db.query(AdvisorRec).filter(AdvisorRec.rec_key == rec_key).first()


def _meta(entry) -> dict:
    return {
        "key": entry.rec_key,
        "first_seen": entry.first_seen,
        "last_confirmed": entry.last_confirmed,
        "times_seen": entry.times_seen,
        "rejected_count": entry.rejected_count,
        "status": entry.status,
    }


def reconcile(sections: dict, db, report_date: str) -> None:
    """Aggiorna il registro col report appena generato e ARRICCHISCE le sections:
      1. upsert di ogni raccomandazione finale (bump 1x/giorno, first_seen
         conservato; una voce invalidated che ricompare validata si riapre);
      2. voci open NON presenti oggi:
         - quantificate: rivalidate dal motore -> ancora valide = riconfermate
           e RIAGGIUNTE al report (standing); fallite = invalidated col motivo;
         - none-type: RIAGGIUNTE al report (standing, mai riconfermate d'ufficio);
      3. voci con regola AIA attiva -> in_gestione (escono dal giro).
    Mai solleva: il report esce anche se il registro inciampa. Il commit e'
    del chiamante (stessa transazione del salvataggio report)."""
    try:
        _reconcile_inner(sections, db, report_date)
    except Exception as e:
        _log(f"reconcile err: {str(e)[:200]}")


def _reconcile_inner(sections: dict, db, report_date: str) -> None:
    from database import AdvisorRec
    import sim_engine
    import advisor_rules as _ar

    recs = sections.setdefault("recommendations", [])
    today = report_date
    present_keys = set()

    # ── 1. upsert delle raccomandazioni di oggi ──────────────────────────────
    for rec in recs:
        k = key_of(rec)
        present_keys.add(k)
        entry = _find(db, k)
        if entry is None:
            entry = AdvisorRec(
                rec_key=k, title=rec.get("title"), detail=rec.get("detail"),
                azioni_json=json.dumps(rec.get("azioni"), ensure_ascii=False)
                if rec.get("azioni") else None,
                priority=rec.get("priority") or "media",
                sim_type=rec.get("sim_type") or "none",
                sim_params=canon_params(rec.get("sim_params")),
                impact_json=json.dumps(rec.get("impact"), ensure_ascii=False)
                if rec.get("impact") else None,
                status="open", first_seen=today, last_confirmed=today,
                times_seen=1, source="engine" if rec.get("synthetic") else "llm")
            db.add(entry)
            db.flush()
        else:
            entry.title = rec.get("title") or entry.title
            entry.detail = rec.get("detail") or entry.detail
            if rec.get("azioni"):
                entry.azioni_json = json.dumps(rec["azioni"], ensure_ascii=False)
            entry.priority = rec.get("priority") or entry.priority
            if rec.get("impact"):
                entry.impact_json = json.dumps(rec["impact"], ensure_ascii=False)
            if entry.status == "invalidated":
                # i dati sono tornati a supportarlo: si riapre
                entry.status = "open"
                entry.invalid_reason = None
            if entry.last_confirmed != today:      # bump max 1x/giorno
                entry.times_seen = (entry.times_seen or 0) + 1
            entry.last_confirmed = today
            entry.updated_at = datetime.utcnow()
        rec["registry"] = _meta(entry)

    # ── 2/3. voci open non presenti nel report odierno ───────────────────────
    open_entries = (db.query(AdvisorRec)
                    .filter(AdvisorRec.status == "open")
                    .order_by(AdvisorRec.first_seen)
                    .all())
    for entry in open_entries:
        if entry.rec_key in present_keys:
            continue
        # regola AIA gia' creata dall'utente? -> in_gestione, esce dal giro
        if entry.sim_type and entry.sim_type != "none":
            try:
                rule = _ar.find_active(db, entry.sim_type,
                                       json.loads(entry.sim_params or "{}"))
            except Exception:
                rule = None
            if rule:
                entry.status = "in_gestione"
                entry.rule_id = rule.id
                entry.updated_at = datetime.utcnow()
                continue
            # quantificata: la riconferma non dipende dall'LLM ma dal MOTORE
            prot = sim_engine.COVERED_RULES.get(entry.sim_type)
            try:
                impact = sim_engine.validate(
                    entry.sim_type, json.loads(entry.sim_params or "{}"), db,
                    since=prot["attiva_dal"] if prot else None)
            except Exception as e:
                impact = {"ok": False, "error": str(e)[:200]}
            v = (impact or {}).get("validation") or {}
            if impact.get("ok") and v.get("passed"):
                entry.impact_json = json.dumps(impact, ensure_ascii=False)
                if entry.last_confirmed != today:
                    entry.times_seen = (entry.times_seen or 0) + 1
                entry.last_confirmed = today
                entry.updated_at = datetime.utcnow()
                recs.append(_standing_rec(entry, impact=impact))
                _log(f"standing (motore) riproposta: {entry.rec_key}")
            else:
                reason = ("; ".join(v.get("fail_reasons", []))
                          or impact.get("error")
                          or "validazione statistica non superata")
                entry.status = "invalidated"
                entry.invalid_reason = reason
                entry.updated_at = datetime.utcnow()
                sections.setdefault("registro_invalidati", []).append({
                    "key": entry.rec_key, "title": entry.title,
                    "first_seen": entry.first_seen,
                    "times_seen": entry.times_seen,
                    "motivo": reason,
                })
                _log(f"invalidata: {entry.rec_key} ({reason[:80]})")
        else:
            # none-type: nessuna rivalidazione possibile. Policy "solo
            # azionabili": senza soluzioni concrete salvate NON viene
            # riproposta (voci-chiacchiera pre-policy: invalidate d'ufficio;
            # se l'LLM la ripropone CON azioni, l'upsert la riapre).
            if not entry.azioni_json:
                entry.status = "invalidated"
                entry.invalid_reason = ("non azionabile: nessuna soluzione "
                                        "concreta (policy 'solo raccomandazioni azionabili')")
                entry.updated_at = datetime.utcnow()
                continue
            # azionabile: resta aperta e viene riproposta cosi' com'e'
            # (last_confirmed NON aggiornato: in UI si vede che il report
            # odierno non l'ha riconfermata)
            recs.append(_standing_rec(entry))


def _standing_rec(entry, impact: Optional[dict] = None) -> dict:
    rec = {
        "title": entry.title,
        "detail": entry.detail or "",
        "priority": entry.priority or "media",
        "sim_type": entry.sim_type or "none",
        "sim_params": entry.sim_params or "{}",
        "standing": True,           # riproposta dal registro, non dall'LLM di oggi
        "registry": _meta(entry),
    }
    if entry.azioni_json:
        try:
            rec["azioni"] = json.loads(entry.azioni_json)
        except Exception:
            pass
    if impact:
        rec["impact"] = impact
    elif entry.impact_json:
        try:
            rec["impact"] = json.loads(entry.impact_json)
        except Exception:
            pass
    return rec


# ─── Hook dagli endpoint (azioni utente e ciclo di vita regole) ──────────────

def on_rec_rejected(db, rec: dict, today: str) -> None:
    """Rifiuto (NON permanente): traccia il contatore. Mai solleva."""
    try:
        k = (rec.get("registry") or {}).get("key") or key_of(rec)
        entry = _find(db, k)
        if entry:
            entry.rejected_count = (entry.rejected_count or 0) + 1
            entry.last_rejected = today
            entry.updated_at = datetime.utcnow()
    except Exception as e:
        _log(f"on_rec_rejected err: {str(e)[:150]}")


def on_rule_created(db, sim_type: str, sim_params, rule_id: int) -> None:
    """Consiglio messo in gestione (Monitor Test o Reale). Mai solleva."""
    try:
        k = f"{sim_type}|{canon_params(sim_params)}"
        entry = _find(db, k)
        if entry:
            entry.status = "in_gestione"
            entry.rule_id = rule_id
            entry.updated_at = datetime.utcnow()
    except Exception as e:
        _log(f"on_rule_created err: {str(e)[:150]}")


def on_rule_rolled_back(db, rule_id: int) -> None:
    """Rollback della regola: il consiglio torna open (non piu' gestito) e
    verra' rivalutato dal prossimo report. Mai solleva."""
    try:
        from database import AdvisorRec
        for entry in db.query(AdvisorRec).filter(AdvisorRec.rule_id == rule_id).all():
            entry.status = "open"
            entry.rule_id = None
            entry.updated_at = datetime.utcnow()
    except Exception as e:
        _log(f"on_rule_rolled_back err: {str(e)[:150]}")


# ─── Viste ────────────────────────────────────────────────────────────────────

def open_for_dossier(db) -> list:
    """Registro aperto in forma compatta per il dossier LLM (riuso key)."""
    from database import AdvisorRec
    try:
        rows = (db.query(AdvisorRec).filter(AdvisorRec.status == "open")
                .order_by(AdvisorRec.first_seen).all())
        return [{
            "key": r.rec_key, "title": r.title, "sim_type": r.sim_type,
            "first_seen": r.first_seen, "last_confirmed": r.last_confirmed,
            "times_seen": r.times_seen, "rejected_count": r.rejected_count,
        } for r in rows]
    except Exception:
        return []


def list_registry(db) -> dict:
    """Vista completa per endpoint/UI."""
    from database import AdvisorRec
    rows = db.query(AdvisorRec).order_by(AdvisorRec.status, AdvisorRec.first_seen).all()
    out = {"open": [], "in_gestione": [], "invalidated": []}
    for r in rows:
        item = {
            "id": r.id, "key": r.rec_key, "title": r.title,
            "priority": r.priority, "sim_type": r.sim_type,
            "sim_params": r.sim_params, "status": r.status,
            "first_seen": r.first_seen, "last_confirmed": r.last_confirmed,
            "times_seen": r.times_seen, "rejected_count": r.rejected_count,
            "last_rejected": r.last_rejected, "rule_id": r.rule_id,
            "invalid_reason": r.invalid_reason, "source": r.source,
        }
        out.setdefault(r.status, []).append(item)
    return out
