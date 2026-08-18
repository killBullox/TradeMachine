"""Garanzia strutturale: ogni regola promossa dallo sweep compare tra le
raccomandazioni in forma azionabile, anche se l'LLM la omette o la descrive
solo a parole."""
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


class TestPromotedActionable:
    def _setup_pattern(self, db):
        # pattern robusto: 15 perdite alle 15 Roma + vincenti fuori
        for i in range(15):
            _mk(db, msg_id=500 + i, pnl_usd=-90.0 - i, status="sl_hit",
                created=datetime(2026, 8, 10, 13, i))
        for i in range(10):
            _mk(db, msg_id=530 + i, pnl_usd=140.0, created=datetime(2026, 8, 10, 8, i))

    def test_llm_omette_il_codice_aggiunge(self, in_memory_db, fake_mt5, monkeypatch):
        import ai_advisor
        db = in_memory_db()
        try:
            self._setup_pattern(db)
            # LLM parla della regola ma la marca 'none' (o la omette del tutto)
            sections = {
                "executive_summary": "s", "trader_edge": [], "execution_gaps": [],
                "risk_profile": [], "patterns": [], "strategia_trader": [],
                "confidence_note": "",
                "recommendations": [
                    {"title": "Evitare le 15 (a parole)", "detail": "d", "priority": "alta",
                     "sim_type": "none", "sim_params": "{}"},
                ],
            }
            monkeypatch.setattr(ai_advisor, "_call_llm", lambda d: (sections, 10, 10))
            rep = ai_advisor.generate_report(db)
            recs = rep["sections"]["recommendations"]
            synth = [r for r in recs if r.get("synthetic")]
            # lo sweep puo' promuovere piu' regole (ora, sessione, scale):
            # tutte azionabili, tutte validate. Quella chiave deve esserci.
            assert len(synth) >= 1
            hours15 = [r for r in synth if r["sim_type"] == "exclude_hours"
                       and json.loads(r["sim_params"]) == {"hours": [15]}]
            assert len(hours15) == 1
            assert all(r["impact"]["validation"]["passed"] for r in synth)
        finally:
            db.close()

    def test_se_llm_la_dichiara_bene_niente_duplicato(self, in_memory_db, fake_mt5, monkeypatch):
        import ai_advisor
        db = in_memory_db()
        try:
            self._setup_pattern(db)
            sections = {
                "executive_summary": "s", "trader_edge": [], "execution_gaps": [],
                "risk_profile": [], "patterns": [], "strategia_trader": [],
                "confidence_note": "",
                "recommendations": [
                    {"title": "Evita le 15", "detail": "d", "priority": "alta",
                     "sim_type": "exclude_hours", "sim_params": json.dumps({"hours": [15]})},
                ],
            }
            monkeypatch.setattr(ai_advisor, "_call_llm", lambda d: (sections, 10, 10))
            rep = ai_advisor.generate_report(db)
            recs = rep["sections"]["recommendations"]
            hours_recs = [r for r in recs if r.get("sim_type") == "exclude_hours"]
            assert len(hours_recs) == 1              # nessun duplicato sintetico
            assert not hours_recs[0].get("synthetic")  # e' quella dell'LLM
        finally:
            db.close()

    def test_gia_in_gestione_non_riproposta(self, in_memory_db, fake_mt5, monkeypatch):
        import ai_advisor
        import advisor_rules as ar
        db = in_memory_db()
        try:
            self._setup_pattern(db)
            ar.create_rule("exclude_hours", {"hours": [15]}, "Evita 15", "test", db=db)
            sections = {
                "executive_summary": "s", "trader_edge": [], "execution_gaps": [],
                "risk_profile": [], "patterns": [], "strategia_trader": [],
                "confidence_note": "", "recommendations": [],
            }
            monkeypatch.setattr(ai_advisor, "_call_llm", lambda d: (sections, 10, 10))
            rep = ai_advisor.generate_report(db)
            # la regola in gestione NON viene riproposta (le altre promosse si')
            assert not any(r.get("synthetic") and r.get("sim_type") == "exclude_hours"
                           for r in rep["sections"]["recommendations"])
        finally:
            db.close()
