"""AI Advisor: dossier deterministico sui trade reali, guardia daily
restart-safe, generate_report con LLM mockato (mai chiamate reali nei test)."""
import json
import pytest
from datetime import datetime, timedelta


def _mk_signal(db, **kw):
    from database import Signal
    now = datetime(2026, 8, 10, 8, 0, 0)  # UTC
    base = dict(
        telegram_msg_id=kw.pop("msg_id", 90000) ,
        symbol="XAUUSD", direction="buy",
        entry_price=4000.0, entry_price_high=4001.0,
        actual_entry_price=4000.5, stoploss=3994.0,
        tp1=4005.0, tp2=4010.0, tp3=4015.0,
        status="tp1", pnl_usd=100.0, risk_usd=1000.0,
        is_filtered=False, is_archived=False,
        mt5_tickets="[1, 2, 3]",
        raw_message="t",
        created_at=now, entered_at=now + timedelta(seconds=5),
        closed_at=now + timedelta(minutes=30),
    )
    base.update(kw)
    s = Signal(**base)
    db.add(s); db.commit(); db.refresh(s)
    return s


class TestDossier:
    def test_predicato_reali_e_statistiche(self, in_memory_db, fake_mt5):
        import ai_advisor
        db = in_memory_db()
        try:
            # 2 vincenti, 1 perdente
            _mk_signal(db, msg_id=1, status="tp3", pnl_usd=500.0)
            _mk_signal(db, msg_id=2, status="tp1", pnl_usd=100.0)
            _mk_signal(db, msg_id=3, status="sl_hit", pnl_usd=-300.0, direction="sell")
            # ESCLUSI: paper, cancelled, senza ticket, aperto
            _mk_signal(db, msg_id=4, is_filtered=True, pnl_usd=999.0)
            _mk_signal(db, msg_id=5, status="cancelled", pnl_usd=0.0)
            _mk_signal(db, msg_id=6, mt5_tickets=None, mt5_ticket=None)
            _mk_signal(db, msg_id=7, status="open", closed_at=None, pnl_usd=None)

            d = ai_advisor.build_dossier(db)
            assert d["trades_analyzed"] == 3
            ov = d["overall"]
            assert ov["total_pnl"] == 300.0
            assert ov["wins"] == 2 and ov["losses"] == 1
            assert ov["win_rate"] == 66.7
            assert ov["profit_factor"] == 2.0  # 600 / 300
            assert ov["biggest_loss"] == -300.0
            # direzione
            assert d["trader_edge"]["by_direction"]["buy"]["trades"] == 2
            assert d["trader_edge"]["by_direction"]["sell"]["trades"] == 1
            # ora Roma: created 08:00 UTC estate = 10:00 Roma
            assert "10" in d["trader_edge"]["by_hour_roma"]
        finally:
            db.close()

    def test_violazione_max_risk_e_slippage_670(self, in_memory_db, fake_mt5):
        import ai_advisor
        db = in_memory_db()
        try:
            # #670-like: range 4387-4388, fill 4389.35, perdita -1229 su risk 1000
            _mk_signal(db, msg_id=10, entry_price=4387.0, entry_price_high=4388.0,
                       actual_entry_price=4389.35, stoploss=4382.0,
                       status="sl_hit", pnl_usd=-1229.25, risk_usd=1000.0)
            _mk_signal(db, msg_id=11, pnl_usd=200.0)  # normale, dentro il range
            d = ai_advisor.build_dossier(db)
            ex = d["execution"]
            assert ex["fills_beyond_range"] == 1
            assert ex["max_slippage_beyond_range"] == 1.35
            assert len(ex["max_risk_violations"]) == 1
            v = ex["max_risk_violations"][0]
            assert v["pnl"] == -1229.25
            assert v["excess_pct"] == pytest.approx(22.9, abs=0.2)
        finally:
            db.close()

    def test_dossier_vuoto(self, in_memory_db, fake_mt5):
        import ai_advisor
        db = in_memory_db()
        try:
            d = ai_advisor.build_dossier(db)
            assert d["trades_analyzed"] == 0
        finally:
            db.close()


class TestGenerateReport:
    def test_llm_mockato_salva_report(self, in_memory_db, fake_mt5, monkeypatch):
        import ai_advisor
        from database import AiReport
        sections = {"executive_summary": "ok", "trader_edge": [], "execution_gaps": [],
                    "risk_profile": [], "patterns": [],
                    "recommendations": [{"title": "t", "detail": "d", "priority": "alta"}],
                    "confidence_note": "n"}
        monkeypatch.setattr(ai_advisor, "_call_llm", lambda d: (sections, 1000, 200))
        db = in_memory_db()
        try:
            _mk_signal(db, msg_id=20)
            rep = ai_advisor.generate_report(db)
            assert rep["status"] == "ok"
            assert rep["sections"]["executive_summary"] == "ok"
            assert rep["tokens_in"] == 1000
            assert db.query(AiReport).count() == 1
            # get_latest lo ritrova
            latest = ai_advisor.get_latest(db)
            assert latest["id"] == rep["id"]
        finally:
            db.close()

    def test_api_key_assente_status_error(self, in_memory_db, fake_mt5, monkeypatch):
        import ai_advisor
        from database import AiReport
        monkeypatch.setattr(ai_advisor, "_client", None)
        monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)
        db = in_memory_db()
        try:
            rep = ai_advisor.generate_report(db)
            assert rep["status"] == "error"
            assert "ANTHROPIC_API_KEY" in rep["error"]
            assert db.query(AiReport).filter(AiReport.status == "error").count() == 1
        finally:
            db.close()

    def test_llm_errore_non_solleva(self, in_memory_db, fake_mt5, monkeypatch):
        import ai_advisor
        def boom(d):
            raise RuntimeError("risposta malformata")
        monkeypatch.setattr(ai_advisor, "_call_llm", boom)
        db = in_memory_db()
        try:
            rep = ai_advisor.generate_report(db)
            assert rep["status"] == "error"
            assert "malformata" in rep["error"]
        finally:
            db.close()


class TestMaybeRunDaily:
    def _prep(self, monkeypatch):
        import ai_advisor
        monkeypatch.setattr(ai_advisor, "_last_check_utc", None)
        sections = {"executive_summary": "s", "trader_edge": [], "execution_gaps": [],
                    "risk_profile": [], "patterns": [], "recommendations": [],
                    "confidence_note": ""}
        monkeypatch.setattr(ai_advisor, "_call_llm", lambda d: (sections, 10, 10))
        return ai_advisor

    def test_prima_delle_2310_non_gira(self, in_memory_db, fake_mt5, monkeypatch):
        ai = self._prep(monkeypatch)
        from zoneinfo import ZoneInfo
        now = datetime(2026, 8, 10, 22, 59, tzinfo=ZoneInfo("Europe/Rome"))
        res = ai.maybe_run_daily(now_roma=now)
        assert res == {"ran": False, "reason": "before_schedule"}

    def test_dopo_le_2310_gira_una_sola_volta(self, in_memory_db, fake_mt5, monkeypatch):
        ai = self._prep(monkeypatch)
        from zoneinfo import ZoneInfo
        from database import AiReport, SessionLocal
        now = datetime(2026, 8, 10, 23, 30, tzinfo=ZoneInfo("Europe/Rome"))
        # forza report_date = data del now simulato
        monkeypatch.setattr(ai, "_roma_now", lambda: now)
        res1 = ai.maybe_run_daily(now_roma=now)
        assert res1["ran"] is True
        # secondo giro stesso giorno: guardia DB (reset throttle modulo)
        monkeypatch.setattr(ai, "_last_check_utc", None)
        res2 = ai.maybe_run_daily(now_roma=now)
        assert res2 == {"ran": False, "reason": "already_done"}
        db = SessionLocal()
        try:
            assert db.query(AiReport).count() == 1  # UN solo report (no doppio run)
        finally:
            db.close()

    def test_throttle_modulo(self, in_memory_db, fake_mt5, monkeypatch):
        import ai_advisor as ai
        monkeypatch.setattr(ai, "_last_check_utc", datetime.utcnow())
        res = ai.maybe_run_daily()
        assert res == {"ran": False, "reason": "throttled"}


class TestEnforceActionable:
    """Policy 'solo raccomandazioni azionabili' (richiesta trader): senza
    regola simulabile E senza soluzioni concrete -> osservazioni, non recs."""

    def test_chiacchiera_demolita_in_osservazioni(self, in_memory_db, fake_mt5):
        import ai_advisor
        sections = {"recommendations": [
            {"title": "Presidiare il drawdown", "detail": "d", "priority": "media",
             "sim_type": "none", "sim_params": "{}", "azioni": []},
            {"title": "Monitorare il payoff", "detail": "d", "priority": "media",
             "sim_type": "none", "sim_params": "{}",
             "azioni": ["monitorare il rapporto avg_win/avg_loss"]},  # verbo-osservazione
        ]}
        ai_advisor._enforce_actionable(sections)
        assert sections["recommendations"] == []
        assert len(sections["osservazioni"]) == 2
        assert sections["osservazioni"][0]["title"] == "Presidiare il drawdown"

    def test_azioni_concrete_tenute(self, in_memory_db, fake_mt5):
        import ai_advisor
        sections = {"recommendations": [
            {"title": "Ridurre latenza", "detail": "d", "priority": "alta",
             "sim_type": "none", "sim_params": "{}",
             "azioni": ["misurare ping VPS->broker (attuale ignoto) e valutare colocation",
                        "monitorare non conta come azione"]},
        ]}
        ai_advisor._enforce_actionable(sections)
        assert len(sections["recommendations"]) == 1
        # il verbo-osservazione viene filtrato dalle azioni, la rec resta
        assert sections["recommendations"][0]["azioni"] == [
            "misurare ping VPS->broker (attuale ignoto) e valutare colocation"]
        assert "osservazioni" not in sections

    def test_regola_simulabile_sempre_tenuta(self, in_memory_db, fake_mt5):
        import ai_advisor
        sections = {"recommendations": [
            {"title": "Escludi setup X", "detail": "d", "priority": "alta",
             "sim_type": "exclude_setup", "sim_params": '{"setup": "x"}',
             "azioni": []},
        ]}
        ai_advisor._enforce_actionable(sections)
        assert len(sections["recommendations"]) == 1
