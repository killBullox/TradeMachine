"""Registro persistente dei consigli AIA (advisor_registry):
chiavi stabili, upsert giorno per giorno, riconferma automatica dal motore,
standing dei none-type, invalidazione motivata, hook azioni utente."""
import json
import pytest
from datetime import datetime, timedelta


def _rec(title="Ridurre la latenza", key="ridurre-latenza", sim_type="none",
         sim_params="{}", **kw):
    r = {"key": key, "title": title, "detail": "d", "priority": "media",
         "sim_type": sim_type, "sim_params": sim_params,
         "azioni": ["misurare il ping VPS->broker e valutare colocation"]}
    r.update(kw)
    return r


def _passed_impact():
    return {"ok": True, "delta_pnl": 500.0,
            "validation": {"passed": True, "n_affected": 12,
                           "bootstrap_confidence": 0.95, "fail_reasons": []}}


def _failed_impact(reason="campione insufficiente (3 < 10)"):
    return {"ok": True, "delta_pnl": 500.0,
            "validation": {"passed": False, "fail_reasons": [reason]}}


class TestKeys:
    def test_key_sim_derivata_dal_codice(self, in_memory_db, fake_mt5):
        import advisor_registry as reg
        # per le simulabili la key dell'LLM viene IGNORATA: params canonici
        r1 = _rec(key="qualcosa-a-caso", sim_type="exclude_hours",
                  sim_params='{"hours": [15]}')
        r2 = _rec(key="altra-key", sim_type="exclude_hours",
                  sim_params='{"hours":[15]}')
        assert reg.key_of(r1) == reg.key_of(r2) == 'exclude_hours|{"hours": [15]}'

    def test_key_none_usa_llm_o_slug(self, in_memory_db, fake_mt5):
        import advisor_registry as reg
        assert reg.key_of(_rec(key="ridurre-latenza")) == "none|ridurre-latenza"
        # senza key -> slug del titolo
        r = _rec(title="Ridurre la LATENZA dei fill!")
        r.pop("key")
        assert reg.key_of(r) == "none|ridurre-la-latenza-dei-fill"


class TestReconcile:
    def test_nuovo_consiglio_crea_entry(self, in_memory_db, fake_mt5):
        import advisor_registry as reg
        from database import AdvisorRec
        db = in_memory_db()
        try:
            sections = {"recommendations": [_rec()]}
            reg.reconcile(sections, db, "2026-08-19")
            db.commit()
            e = db.query(AdvisorRec).one()
            assert e.rec_key == "none|ridurre-latenza"
            assert e.first_seen == "2026-08-19" and e.last_confirmed == "2026-08-19"
            assert e.times_seen == 1 and e.status == "open" and e.source == "llm"
            # metadata attaccata alla rec per la UI
            assert sections["recommendations"][0]["registry"]["times_seen"] == 1
        finally:
            db.close()

    def test_riconferma_bump_max_1_al_giorno(self, in_memory_db, fake_mt5):
        import advisor_registry as reg
        from database import AdvisorRec
        db = in_memory_db()
        try:
            reg.reconcile({"recommendations": [_rec()]}, db, "2026-08-19")
            # rigenerazione lo STESSO giorno: niente doppio bump
            reg.reconcile({"recommendations": [_rec(title="Riformulato")]}, db, "2026-08-19")
            e = db.query(AdvisorRec).one()
            assert e.times_seen == 1
            assert e.title == "Riformulato"          # testo aggiornato
            # giorno dopo: bump e first_seen conservato
            reg.reconcile({"recommendations": [_rec()]}, db, "2026-08-20")
            db.commit()
            e = db.query(AdvisorRec).one()
            assert e.times_seen == 2
            assert e.first_seen == "2026-08-19" and e.last_confirmed == "2026-08-20"
        finally:
            db.close()

    def test_none_type_omesso_decade_mai_zombie(self, in_memory_db, fake_mt5):
        """La validita' di un consiglio non quantificato la giudica SOLO l'LLM
        col dossier di oggi: se non lo riconferma, DECADE (niente standing a
        pappagallo con testo di ieri). Se lo ripropone in futuro, si riapre."""
        import advisor_registry as reg
        from database import AdvisorRec
        db = in_memory_db()
        try:
            reg.reconcile({"recommendations": [_rec()]}, db, "2026-08-19")
            # il giorno dopo l'LLM non lo riconferma -> decade, NON standing
            sections = {"recommendations": []}
            reg.reconcile(sections, db, "2026-08-20")
            db.commit()
            assert sections["recommendations"] == []
            inv = sections["registro_invalidati"]
            assert len(inv) == 1 and "non riconfermato" in inv[0]["motivo"]
            e = db.query(AdvisorRec).one()
            assert e.status == "invalidated"
            # riproposto dall'LLM -> si riapre con storico conservato
            reg.reconcile({"recommendations": [_rec()]}, db, "2026-08-21")
            db.commit()
            e = db.query(AdvisorRec).one()
            assert e.status == "open" and e.first_seen == "2026-08-19"
            assert e.times_seen == 2
        finally:
            db.close()

    def test_sim_omessa_ancora_valida_riconfermata_dal_motore(self, in_memory_db,
                                                              fake_mt5, monkeypatch):
        import advisor_registry as reg
        import sim_engine
        from database import AdvisorRec
        db = in_memory_db()
        try:
            monkeypatch.setattr(sim_engine, "validate",
                                lambda *a, **k: _passed_impact())
            rec = _rec(sim_type="exclude_hours", sim_params='{"hours": [15]}',
                       impact=_passed_impact())
            reg.reconcile({"recommendations": [rec]}, db, "2026-08-19")
            # LLM la omette il giorno dopo: il MOTORE la riconferma e la ripropone
            sections = {"recommendations": []}
            reg.reconcile(sections, db, "2026-08-20")
            db.commit()
            assert len(sections["recommendations"]) == 1
            st = sections["recommendations"][0]
            assert st["standing"] is True and st["sim_type"] == "exclude_hours"
            assert st["impact"]["validation"]["passed"] is True
            e = db.query(AdvisorRec).one()
            assert e.last_confirmed == "2026-08-20" and e.times_seen == 2
        finally:
            db.close()

    def test_sim_non_piu_valida_invalidata_con_motivo(self, in_memory_db,
                                                      fake_mt5, monkeypatch):
        import advisor_registry as reg
        import sim_engine
        from database import AdvisorRec
        db = in_memory_db()
        try:
            monkeypatch.setattr(sim_engine, "validate",
                                lambda *a, **k: _passed_impact())
            rec = _rec(sim_type="exclude_hours", sim_params='{"hours": [15]}')
            reg.reconcile({"recommendations": [rec]}, db, "2026-08-19")
            # i dati cambiano: la validazione fallisce
            monkeypatch.setattr(sim_engine, "validate",
                                lambda *a, **k: _failed_impact())
            sections = {"recommendations": []}
            reg.reconcile(sections, db, "2026-08-20")
            db.commit()
            assert sections["recommendations"] == []          # NON riproposta
            inv = sections["registro_invalidati"]
            assert len(inv) == 1 and "campione insufficiente" in inv[0]["motivo"]
            e = db.query(AdvisorRec).one()
            assert e.status == "invalidated"
            # se i numeri tornano, ricomparendo nel report si RIAPRE
            monkeypatch.setattr(sim_engine, "validate",
                                lambda *a, **k: _passed_impact())
            reg.reconcile({"recommendations": [rec]}, db, "2026-08-21")
            db.commit()
            e = db.query(AdvisorRec).one()
            assert e.status == "open" and e.invalid_reason is None
        finally:
            db.close()

    def test_regola_attiva_porta_in_gestione(self, in_memory_db, fake_mt5):
        import advisor_registry as reg
        import advisor_rules as ar
        from database import AdvisorRec
        db = in_memory_db()
        try:
            rec = _rec(sim_type="exclude_hours", sim_params='{"hours": [15]}')
            reg.reconcile({"recommendations": [rec]}, db, "2026-08-19")
            db.commit()
            res = ar.create_rule("exclude_hours", {"hours": [15]}, "Evita 15", "test", db=db)
            assert res["ok"]
            # al report successivo (LLM correttamente non la ripropone perche'
            # gia' in gestione): la voce esce dal giro senza standing
            sections = {"recommendations": []}
            reg.reconcile(sections, db, "2026-08-20")
            db.commit()
            assert sections["recommendations"] == []
            e = db.query(AdvisorRec).one()
            assert e.status == "in_gestione" and e.rule_id == res["rule"]["id"]
        finally:
            db.close()


class TestHooks:
    def test_rifiuto_contato_non_permanente(self, in_memory_db, fake_mt5):
        import advisor_registry as reg
        from database import AdvisorRec
        db = in_memory_db()
        try:
            sections = {"recommendations": [_rec()]}
            reg.reconcile(sections, db, "2026-08-19")
            reg.on_rec_rejected(db, sections["recommendations"][0], "2026-08-19")
            db.commit()
            e = db.query(AdvisorRec).one()
            assert e.rejected_count == 1 and e.last_rejected == "2026-08-19"
            assert e.status == "open"     # NON permanente: resta aperto
        finally:
            db.close()

    def test_rule_created_e_rollback(self, in_memory_db, fake_mt5):
        import advisor_registry as reg
        from database import AdvisorRec
        db = in_memory_db()
        try:
            rec = _rec(sim_type="scale_risk", sim_params='{"factor": 0.5}')
            reg.reconcile({"recommendations": [rec]}, db, "2026-08-19")
            db.commit()
            reg.on_rule_created(db, "scale_risk", {"factor": 0.5}, rule_id=77)
            db.commit()
            e = db.query(AdvisorRec).one()
            assert e.status == "in_gestione" and e.rule_id == 77
            reg.on_rule_rolled_back(db, 77)
            db.commit()
            e = db.query(AdvisorRec).one()
            assert e.status == "open" and e.rule_id is None
        finally:
            db.close()

    def test_open_for_dossier_compatto(self, in_memory_db, fake_mt5):
        import advisor_registry as reg
        db = in_memory_db()
        try:
            reg.reconcile({"recommendations": [_rec()]}, db, "2026-08-19")
            db.commit()
            rows = reg.open_for_dossier(db)
            assert rows == [{
                "key": "none|ridurre-latenza", "title": "Ridurre la latenza",
                "sim_type": "none", "first_seen": "2026-08-19",
                "last_confirmed": "2026-08-19", "times_seen": 1,
                "rejected_count": 0,
            }]
        finally:
            db.close()
