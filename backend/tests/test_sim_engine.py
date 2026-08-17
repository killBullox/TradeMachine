"""Motore di simulazione what-if: ogni regola produce delta ESATTI sui trade
reali storici. Verifica matematica regola per regola + integrazione col report."""
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


class TestRegole:
    def test_exclude_hours_delta_esatto(self, in_memory_db, fake_mt5):
        import sim_engine
        db = in_memory_db()
        try:
            _mk(db, msg_id=1, pnl_usd=100.0)   # 10:00 Roma
            _mk(db, msg_id=2, pnl_usd=-50.0, created=datetime(2026, 8, 10, 13, 0))  # 15:00 Roma
            _mk(db, msg_id=3, pnl_usd=200.0, created=datetime(2026, 8, 10, 13, 30))  # 15:30 Roma
            res = sim_engine.simulate("exclude_hours", {"hours": [15]}, db)
            assert res["ok"] is True
            assert res["baseline"]["pnl"] == 250.0
            assert res["simulated"]["pnl"] == 100.0
            assert res["delta_pnl"] == -150.0       # esclusi -50 e +200
            assert res["trades_excluded"] == 2
            assert res["verdict"] == "peggiora"
        finally:
            db.close()

    def test_min_rr_tp1_gate(self, in_memory_db, fake_mt5):
        import sim_engine
        db = in_memory_db()
        try:
            # RR = |4005-4000|/|4000-3994| = 5/6 = 0.83 -> sotto 1.0, escluso
            _mk(db, msg_id=10, pnl_usd=300.0)
            # RR = |4012-4000|/6 = 2.0 -> tenuto
            _mk(db, msg_id=11, tp1=4012.0, pnl_usd=-100.0)
            res = sim_engine.simulate("min_rr_tp1", {"min_rr": 1.0}, db)
            assert res["ok"] is True
            assert res["delta_pnl"] == -300.0  # perso il vincente con RR basso
            assert res["trades_excluded"] == 1
            assert res["excluded_sample"][0]["pnl"] == 300.0
        finally:
            db.close()

    def test_cap_loss_at_risk_670(self, in_memory_db, fake_mt5):
        import sim_engine
        db = in_memory_db()
        try:
            # #670-like: perdita -1229 su risk 1000 -> capped a -1000 (+229.25)
            _mk(db, msg_id=20, status="sl_hit", pnl_usd=-1229.25, risk_usd=1000.0)
            _mk(db, msg_id=21, status="sl_hit", pnl_usd=-980.0)   # sotto il cap, invariato
            _mk(db, msg_id=22, pnl_usd=500.0)                     # vincente, invariato
            res = sim_engine.simulate("cap_loss_at_risk", {}, db)
            assert res["ok"] is True
            assert res["delta_pnl"] == 229.25
            assert res["trades_modified"] == 1
            assert res["modified_sample"][0]["new_pnl"] == -1000.0
            assert res["verdict"] == "migliora"
        finally:
            db.close()

    def test_scale_risk(self, in_memory_db, fake_mt5):
        import sim_engine
        db = in_memory_db()
        try:
            _mk(db, msg_id=30, pnl_usd=100.0)
            _mk(db, msg_id=31, pnl_usd=-60.0, status="sl_hit")
            res = sim_engine.simulate("scale_risk", {"factor": 0.5}, db)
            assert res["simulated"]["pnl"] == 20.0   # (100-60)*0.5
            assert res["delta_pnl"] == -20.0
            assert res["trades_modified"] == 2
        finally:
            db.close()

    def test_exclude_direction_e_weekday(self, in_memory_db, fake_mt5):
        import sim_engine
        db = in_memory_db()
        try:
            _mk(db, msg_id=40, direction="buy", pnl_usd=100.0)    # lun 10/08/2026
            _mk(db, msg_id=41, direction="sell", pnl_usd=-40.0)
            r1 = sim_engine.simulate("exclude_direction", {"direction": "sell"}, db)
            assert r1["delta_pnl"] == 40.0 and r1["verdict"] == "migliora"
            r2 = sim_engine.simulate("exclude_weekdays", {"weekdays": ["lun"]}, db)
            assert r2["simulated"]["trades"] == 0   # entrambi di lunedi'
        finally:
            db.close()

    def test_exclude_near_news(self, in_memory_db, fake_mt5):
        import sim_engine
        from database import NewsEvent
        db = in_memory_db()
        try:
            db.add(NewsEvent(name="CPI", event_time=datetime(2026, 8, 10, 8, 10),
                             currency="USD", impact="high", flatten=True))
            db.commit()
            _mk(db, msg_id=50, pnl_usd=-200.0)  # entrato 08:00:05, a 10 min dal CPI
            _mk(db, msg_id=51, pnl_usd=80.0, created=datetime(2026, 8, 10, 12, 0))  # lontano
            res = sim_engine.simulate("exclude_near_news", {"minutes": 30}, db)
            assert res["trades_excluded"] == 1
            assert res["delta_pnl"] == 200.0
        finally:
            db.close()

    def test_regola_sconosciuta_e_parametri_invalidi(self, in_memory_db, fake_mt5):
        import sim_engine
        db = in_memory_db()
        try:
            _mk(db, msg_id=60)
            assert sim_engine.simulate("magia", {}, db)["ok"] is False
            assert sim_engine.simulate("scale_risk", {"factor": 99}, db)["ok"] is False
            assert sim_engine.simulate("exclude_direction", {"direction": "boh"}, db)["ok"] is False
            assert sim_engine.simulate("min_rr_tp1", {}, db)["ok"] is False  # param mancante
        finally:
            db.close()


class TestIntegrazioneReport:
    def test_impatti_allegati_alle_raccomandazioni(self, in_memory_db, fake_mt5, monkeypatch):
        import ai_advisor
        db = in_memory_db()
        try:
            _mk(db, msg_id=70, pnl_usd=100.0)
            _mk(db, msg_id=71, pnl_usd=-50.0, status="sl_hit",
                created=datetime(2026, 8, 10, 13, 0))  # 15:00 Roma
            sections = {
                "executive_summary": "s", "trader_edge": [], "execution_gaps": [],
                "risk_profile": [], "patterns": [], "confidence_note": "",
                "recommendations": [
                    {"title": "Evita le 15", "detail": "d", "priority": "alta",
                     "sim_type": "exclude_hours", "sim_params": "{\"hours\": [15]}"},
                    {"title": "Trailing SL", "detail": "richiede tick", "priority": "media",
                     "sim_type": "none", "sim_params": "{}"},
                    {"title": "Rotta", "detail": "d", "priority": "bassa",
                     "sim_type": "scale_risk", "sim_params": "{\"factor\": 99}"},
                ],
            }
            monkeypatch.setattr(ai_advisor, "_call_llm", lambda d: (sections, 10, 10))
            rep = ai_advisor.generate_report(db)
            assert rep["status"] == "ok"
            recs = rep["sections"]["recommendations"]
            # 1: simulata con delta esatto (+50 escludendo il perdente delle 15)
            assert recs[0]["impact"]["ok"] is True
            assert recs[0]["impact"]["delta_pnl"] == 50.0
            # 2: non simulabile -> nessun impact
            assert "impact" not in recs[1]
            # 3: parametri invalidi -> impact con ok=False, report comunque ok
            assert recs[2]["impact"]["ok"] is False
        finally:
            db.close()
