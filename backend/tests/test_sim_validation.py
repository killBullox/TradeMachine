"""Validazione statistica del motore di simulazione: gate campione/robustezza/
bootstrap, determinismo, sweep sistematico e demotion anti-autosmentita."""
import json
import pytest
from datetime import datetime, timedelta


def _mk(db, **kw):
    from database import Signal
    now = kw.pop("created", datetime(2026, 8, 10, 8, 0, 0))  # 10:00 Roma estate
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
    db.add(s); db.commit()
    return s


class TestValidazioneStatistica:
    def test_campione_insufficiente_bocciata(self, in_memory_db, fake_mt5):
        # 3 trade perdenti alle 15: delta positivo ma campione < 10 -> bocciata
        import sim_engine
        db = in_memory_db()
        try:
            for i in range(3):
                _mk(db, msg_id=100 + i, pnl_usd=-100.0, status="sl_hit",
                    created=datetime(2026, 8, 10, 13, i))
            _mk(db, msg_id=110, pnl_usd=500.0)  # fuori fascia
            res = sim_engine.validate("exclude_hours", {"hours": [15]}, db)
            v = res["validation"]
            assert res["delta_pnl"] == 300.0          # sembra ottima...
            assert v["passed"] is False               # ...ma campione = 3
            assert any("campione" in r for r in v["fail_reasons"])
        finally:
            db.close()

    def test_delta_concentrato_su_un_trade_bocciata(self, in_memory_db, fake_mt5):
        # 12 trade toccati ma il delta viene quasi tutto da UNA perdita enorme
        import sim_engine
        db = in_memory_db()
        try:
            _mk(db, msg_id=120, pnl_usd=-5000.0, status="sl_hit",
                created=datetime(2026, 8, 10, 13, 0))
            for i in range(11):
                _mk(db, msg_id=121 + i, pnl_usd=-10.0, status="sl_hit",
                    created=datetime(2026, 8, 10, 13, 5 + i))
            _mk(db, msg_id=140, pnl_usd=100.0)
            res = sim_engine.validate("exclude_hours", {"hours": [15]}, db)
            v = res["validation"]
            assert res["delta_pnl"] == 5110.0
            assert v["passed"] is False
            assert v["top1_share"] > 0.9
            assert any(("concentrato" in r) or ("fragile" in r) for r in v["fail_reasons"])
        finally:
            db.close()

    def test_pattern_robusto_promossa(self, in_memory_db, fake_mt5):
        # 15 perdite omogenee alle 15 + vincenti fuori: promossa
        import sim_engine
        db = in_memory_db()
        try:
            for i in range(15):
                _mk(db, msg_id=200 + i, pnl_usd=-100.0 - i, status="sl_hit",
                    created=datetime(2026, 8, 10, 13, i))
            for i in range(10):
                _mk(db, msg_id=230 + i, pnl_usd=150.0,
                    created=datetime(2026, 8, 10, 8, i))
            res = sim_engine.validate("exclude_hours", {"hours": [15]}, db)
            v = res["validation"]
            assert v["passed"] is True
            assert v["n_affected"] == 15
            assert v["bootstrap_confidence"] >= 0.9
        finally:
            db.close()

    def test_strutturale_esente_da_campione(self, in_memory_db, fake_mt5):
        # cap_loss_at_risk con 1 solo trade toccato: strutturale -> promossa
        import sim_engine
        db = in_memory_db()
        try:
            _mk(db, msg_id=300, status="sl_hit", pnl_usd=-1229.25, risk_usd=1000.0)
            _mk(db, msg_id=301, pnl_usd=100.0)
            res = sim_engine.validate("cap_loss_at_risk", {}, db)
            v = res["validation"]
            assert v["passed"] is True
            assert v["category"] == "strutturale"
            assert v["n_affected"] == 1
        finally:
            db.close()

    def test_determinismo(self, in_memory_db, fake_mt5):
        import sim_engine
        db = in_memory_db()
        try:
            for i in range(15):
                _mk(db, msg_id=400 + i, pnl_usd=-50.0 - i, status="sl_hit",
                    created=datetime(2026, 8, 10, 13, i))
            _mk(db, msg_id=420, pnl_usd=300.0)
            r1 = sim_engine.validate("exclude_hours", {"hours": [15]}, db)
            r2 = sim_engine.validate("exclude_hours", {"hours": [15]}, db)
            assert r1["validation"] == r2["validation"]  # seed fisso
        finally:
            db.close()


class TestSweepEDemotion:
    def test_sweep_promuove_regola_robusta(self, in_memory_db, fake_mt5):
        import sim_engine
        db = in_memory_db()
        try:
            for i in range(15):
                _mk(db, msg_id=500 + i, pnl_usd=-80.0 - i, status="sl_hit",
                    created=datetime(2026, 8, 10, 13, i))
            for i in range(12):
                _mk(db, msg_id=530 + i, pnl_usd=120.0,
                    created=datetime(2026, 8, 10, 8, i))
            sw = sim_engine.sweep(db)
            promoted = {(e["sim_type"], e["sim_params"]) for e in sw["promosse"]}
            assert ("exclude_hours", json.dumps({"hours": [15]})) in promoted
            for e in sw["promosse"]:
                if e["category"] == "pattern":
                    assert e["n_affected"] >= sim_engine.MIN_SAMPLE
                    assert e["delta_pnl"] > 0
        finally:
            db.close()

    def test_demotion_consiglio_non_validato(self, in_memory_db, fake_mt5, monkeypatch):
        # L'LLM prova a raccomandare una regola fragile -> il codice la DEGRADA
        import ai_advisor
        db = in_memory_db()
        try:
            _mk(db, msg_id=600, pnl_usd=-5000.0, status="sl_hit",
                created=datetime(2026, 8, 10, 13, 0))  # unica perdita, ora 15
            _mk(db, msg_id=601, pnl_usd=100.0)
            sections = {
                "executive_summary": "s", "trader_edge": [], "execution_gaps": [],
                "risk_profile": [], "patterns": [], "confidence_note": "",
                "recommendations": [
                    {"title": "Evita le 15 (fragile!)", "detail": "d", "priority": "alta",
                     "sim_type": "exclude_hours", "sim_params": json.dumps({"hours": [15]})},
                    {"title": "Consiglio operativo", "detail": "non quantificato",
                     "priority": "media", "sim_type": "none", "sim_params": "{}"},
                ],
            }
            monkeypatch.setattr(ai_advisor, "_call_llm", lambda d: (sections, 10, 10))
            rep = ai_advisor.generate_report(db)
            assert rep["status"] == "ok"
            secs = rep["sections"]
            # la fragile e' stata DEGRADATA, non presentata come consiglio
            non_synth = [r for r in secs["recommendations"] if not r.get("synthetic")]
            assert len(non_synth) == 1
            assert non_synth[0]["title"] == "Consiglio operativo"
            demoted = secs["scartate_dalla_verifica"]
            assert len(demoted) == 1
            assert "15" in demoted[0]["title"]
            assert demoted[0]["demotion_reason"]
            assert demoted[0]["impact"]["validation"]["passed"] is False
        finally:
            db.close()

    def test_consiglio_validato_resta(self, in_memory_db, fake_mt5, monkeypatch):
        import ai_advisor
        db = in_memory_db()
        try:
            for i in range(15):
                _mk(db, msg_id=700 + i, pnl_usd=-90.0 - i, status="sl_hit",
                    created=datetime(2026, 8, 10, 13, i))
            for i in range(10):
                _mk(db, msg_id=730 + i, pnl_usd=140.0,
                    created=datetime(2026, 8, 10, 8, i))
            sections = {
                "executive_summary": "s", "trader_edge": [], "execution_gaps": [],
                "risk_profile": [], "patterns": [], "confidence_note": "",
                "recommendations": [
                    {"title": "Evita le 15 (robusta)", "detail": "d", "priority": "alta",
                     "sim_type": "exclude_hours", "sim_params": json.dumps({"hours": [15]})},
                ],
            }
            monkeypatch.setattr(ai_advisor, "_call_llm", lambda d: (sections, 10, 10))
            rep = ai_advisor.generate_report(db)
            recs = [r for r in rep["sections"]["recommendations"] if not r.get("synthetic")]
            assert len(recs) == 1
            assert recs[0]["impact"]["validation"]["passed"] is True
            assert "scartate_dalla_verifica" not in rep["sections"]
        finally:
            db.close()

    def test_sweep_nel_dossier(self, in_memory_db, fake_mt5, monkeypatch):
        import ai_advisor
        db = in_memory_db()
        try:
            for i in range(12):
                _mk(db, msg_id=800 + i, pnl_usd=-60.0 - i, status="sl_hit",
                    created=datetime(2026, 8, 10, 13, i))
            _mk(db, msg_id=820, pnl_usd=900.0)
            captured = {}
            def fake_llm(dossier):
                captured["sweep"] = dossier.get("validated_rules_sweep")
                return ({"executive_summary": "s", "trader_edge": [], "execution_gaps": [],
                         "risk_profile": [], "patterns": [], "recommendations": [],
                         "confidence_note": ""}, 1, 1)
            monkeypatch.setattr(ai_advisor, "_call_llm", fake_llm)
            ai_advisor.generate_report(db)
            assert captured["sweep"] is not None
            assert "promosse" in captured["sweep"]
            assert "gates" in captured["sweep"]
        finally:
            db.close()
