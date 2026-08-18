"""Gestione consigli AIA: Monitor Test / Monitor Reale, enforcement reale
(blocco intake -> paper, scale_risk), promote/rollback, demotion anti-riproposta."""
import json
import pytest
from datetime import datetime, timedelta


def _mk(db, **kw):
    from database import Signal
    now = kw.pop("created", datetime(2026, 8, 10, 8, 0, 0))
    base = dict(
        telegram_msg_id=kw.pop("msg_id"), symbol="XAUUSD", direction="buy",
        entry_price=4000.0, entry_price_high=4001.0, actual_entry_price=4000.5,
        stoploss=3994.0, tp1=4005.0, tp2=4010.0, tp3=4015.0,
        status="tp1", pnl_usd=100.0, risk_usd=1000.0,
        is_filtered=False, is_archived=False, mt5_tickets="[1]",
        raw_message="t", created_at=now,
        entered_at=now + timedelta(seconds=5), closed_at=now + timedelta(minutes=30),
    )
    base.update(kw)
    s = Signal(**base)
    db.add(s); db.commit(); db.refresh(s)
    return s


class TestLifecycle:
    def test_create_promote_rollback(self, in_memory_db, fake_mt5):
        import advisor_rules as ar
        db = in_memory_db()
        try:
            res = ar.create_rule("exclude_hours", {"hours": [15]}, "Evita le 15",
                                 "test", expected={"delta_pnl": 100.0}, db=db)
            assert res["ok"] is True
            rid = res["rule"]["id"]
            assert res["rule"]["mode"] == "test"
            # duplicato -> rifiutato
            dup = ar.create_rule("exclude_hours", {"hours": [15]}, "x", "real", db=db)
            assert dup["ok"] is False and "gia' attiva" in dup["error"]
            # promote
            pro = ar.promote_rule(rid, db)
            assert pro["ok"] is True and pro["rule"]["mode"] == "real"
            # rollback
            rb = ar.rollback_rule(rid, db)
            assert rb["ok"] is True
            assert ar.list_rules(db) == {"test": [], "real": []}
            # dopo il rollback la stessa regola puo' essere ricreata
            again = ar.create_rule("exclude_hours", {"hours": [15]}, "Evita le 15", "test", db=db)
            assert again["ok"] is True
        finally:
            db.close()

    def test_monitor_test_delta_virtuale(self, in_memory_db, fake_mt5):
        import advisor_rules as ar
        from database import AdvisorRule
        db = in_memory_db()
        try:
            res = ar.create_rule("exclude_hours", {"hours": [15]}, "Evita 15", "test", db=db)
            rid = res["rule"]["id"]
            # forza attivazione nel passato, poi trade dopo l'attivazione
            rule = db.query(AdvisorRule).get(rid)
            rule.activated_at = datetime(2026, 8, 19, 0, 0, 0); db.commit()
            _mk(db, msg_id=1, pnl_usd=-80.0, status="sl_hit",
                created=datetime(2026, 8, 20, 13, 0))   # 15:00 Roma -> toccato
            _mk(db, msg_id=2, pnl_usd=50.0, created=datetime(2026, 8, 20, 8, 0))
            _mk(db, msg_id=3, pnl_usd=999.0, created=datetime(2026, 8, 1, 13, 0))  # PRIMA: ignorato
            out = ar.list_rules(db)
            m = out["test"][0]["monitor"]
            assert m["trade_nel_periodo"] == 2
            assert m["trade_toccati"] == 1
            assert m["delta_osservato"] == 80.0     # evitata la perdita delle 15
            assert out["test"][0]["delta_promesso"] is None or isinstance(out["test"][0]["delta_promesso"], (int, float))
        finally:
            db.close()

    def test_monitor_test_precisione_al_secondo(self, in_memory_db, fake_mt5):
        # anti-artefatto: trade della mattina NON contati se attivata a mezzogiorno
        import advisor_rules as ar
        from database import AdvisorRule
        db = in_memory_db()
        try:
            res = ar.create_rule("exclude_hours", {"hours": [15]}, "x", "test", db=db)
            rule = db.query(AdvisorRule).get(res["rule"]["id"])
            rule.activated_at = datetime(2026, 8, 20, 12, 0, 0); db.commit()
            _mk(db, msg_id=10, pnl_usd=-500.0, status="sl_hit",
                created=datetime(2026, 8, 20, 11, 59))  # 1 min PRIMA dell'attivazione
            m = ar.list_rules(db)["test"][0]["monitor"]
            assert m["trade_nel_periodo"] == 0       # non conta il pre-attivazione
        finally:
            db.close()


class TestEnforcement:
    def test_blocco_intake_regola_reale(self, in_memory_db, fake_mt5):
        import advisor_rules as ar
        db = in_memory_db()
        try:
            ar.create_rule("exclude_direction", {"direction": "sell"}, "No sell", "real", db=db)
            sell = _mk(db, msg_id=20, direction="sell")
            buy = _mk(db, msg_id=21, direction="buy")
            assert ar.check_signal_block(sell, db) is not None
            assert "No sell" in ar.check_signal_block(sell, db)
            assert ar.check_signal_block(buy, db) is None
        finally:
            db.close()

    def test_regola_test_non_blocca(self, in_memory_db, fake_mt5):
        # Monitor Test = ZERO effetto reale
        import advisor_rules as ar
        db = in_memory_db()
        try:
            ar.create_rule("exclude_direction", {"direction": "sell"}, "No sell", "test", db=db)
            sell = _mk(db, msg_id=30, direction="sell")
            assert ar.check_signal_block(sell, db) is None
        finally:
            db.close()

    def test_scale_risk_factor(self, in_memory_db, fake_mt5):
        import advisor_rules as ar
        db = in_memory_db()
        try:
            assert ar.scale_risk_factor(db) == 1.0
            ar.create_rule("scale_risk", {"factor": 0.5}, "Meta' rischio", "real", db=db)
            assert ar.scale_risk_factor(db) == 0.5
            # in test NON scala
            db2_rules = ar.list_rules(db)
            assert len(db2_rules["real"]) == 1
        finally:
            db.close()

    def test_monitor_reale_blocco_pnl_evitato(self, in_memory_db, fake_mt5):
        import advisor_rules as ar
        db = in_memory_db()
        try:
            res = ar.create_rule("exclude_direction", {"direction": "sell"}, "No sell", "real", db=db)
            rid = res["rule"]["id"]
            # trade bloccato dalla regola -> paper con esito simulato negativo
            _mk(db, msg_id=40, direction="sell", is_filtered=True,
                filter_reason=f"Regola AIA #{rid}: No sell", pnl_usd=-300.0,
                created=datetime.utcnow() + timedelta(minutes=1))
            m = ar.list_rules(db)["real"][0]["monitor"]
            assert m["tipo"] == "reale_blocco"
            assert m["trade_bloccati"] == 1
            assert m["pnl_evitato"] == 300.0    # il blocco ha risparmiato 300
        finally:
            db.close()


class TestAdvisorIntegration:
    def test_demotion_regola_gia_in_gestione(self, in_memory_db, fake_mt5, monkeypatch):
        import ai_advisor
        import advisor_rules as ar
        db = in_memory_db()
        try:
            for i in range(15):
                _mk(db, msg_id=100 + i, pnl_usd=-90.0 - i, status="sl_hit",
                    created=datetime(2026, 8, 10, 13, i))
            for i in range(10):
                _mk(db, msg_id=130 + i, pnl_usd=140.0, created=datetime(2026, 8, 10, 8, i))
            # l'utente ha GIA' messo la regola in Monitor Test
            ar.create_rule("exclude_hours", {"hours": [15]}, "Evita 15", "test", db=db)
            sections = {
                "executive_summary": "s", "trader_edge": [], "execution_gaps": [],
                "risk_profile": [], "patterns": [], "confidence_note": "",
                "recommendations": [
                    {"title": "Evita le 15", "detail": "d", "priority": "alta",
                     "sim_type": "exclude_hours", "sim_params": json.dumps({"hours": [15]})},
                ],
            }
            monkeypatch.setattr(ai_advisor, "_call_llm", lambda d: (sections, 10, 10))
            rep = ai_advisor.generate_report(db)
            assert len(rep["sections"]["recommendations"]) == 0
            dem = rep["sections"]["scartate_dalla_verifica"]
            assert "gia' in gestione: Monitor Test" in dem[0]["demotion_reason"]
        finally:
            db.close()

    def test_dossier_contiene_regole_in_gestione(self, in_memory_db, fake_mt5):
        import ai_advisor
        import advisor_rules as ar
        db = in_memory_db()
        try:
            _mk(db, msg_id=200)
            ar.create_rule("exclude_direction", {"direction": "sell"}, "No sell", "real", db=db)
            d = ai_advisor.build_dossier(db)
            g = d["regole_aia_in_gestione"]
            assert len(g["attive_reali"]) == 1
            assert g["attive_reali"][0]["sim_type"] == "exclude_direction"
        finally:
            db.close()
