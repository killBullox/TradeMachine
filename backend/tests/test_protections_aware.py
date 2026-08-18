"""Consapevolezza delle protezioni attive: le regole coperte vengono valutate
solo sul periodo DOPO l'attivazione (problema residuo); l'advisor non puo'
consigliare cio' che esiste gia'; i delta storici pre-fix non sono opportunita'."""
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
    db.add(s); db.commit()
    return s


class TestSince:
    def test_simulate_con_since_filtra_il_passato(self, in_memory_db, fake_mt5):
        import sim_engine
        db = in_memory_db()
        try:
            # violazione max-risk PRIMA del fix #670 (17/08) e trade pulito dopo
            _mk(db, msg_id=1, status="sl_hit", pnl_usd=-1500.0,
                created=datetime(2026, 8, 10, 8, 0))
            _mk(db, msg_id=2, pnl_usd=200.0, created=datetime(2026, 8, 20, 8, 0))
            full = sim_engine.simulate("cap_loss_at_risk", {}, db)
            resid = sim_engine.simulate("cap_loss_at_risk", {}, db, since="2026-08-17")
            assert full["delta_pnl"] == 500.0        # storico: violazione capped
            assert full["baseline"]["trades"] == 2
            assert resid["delta_pnl"] == 0.0         # residuo: nessuna violazione
            assert resid["baseline"]["trades"] == 1  # solo il trade post-fix
            assert resid["since"] == "2026-08-17"
        finally:
            db.close()


class TestSweepCoverage:
    def test_violazioni_solo_pre_fix_finiscono_in_gia_coperte(self, in_memory_db, fake_mt5):
        import sim_engine
        db = in_memory_db()
        try:
            # 3 violazioni max-risk TUTTE prima del 17/08 + trade puliti dopo
            for i in range(3):
                _mk(db, msg_id=10 + i, status="sl_hit", pnl_usd=-1400.0 - i,
                    created=datetime(2026, 8, 10, 8 + i, 0))
            for i in range(5):
                _mk(db, msg_id=20 + i, pnl_usd=150.0,
                    created=datetime(2026, 8, 20, 8 + i, 0))
            sw = sim_engine.sweep(db)
            promoted_types = {e["sim_type"] for e in sw["promosse"]}
            assert "cap_loss_at_risk" not in promoted_types  # NON riproposta!
            cov = [e for e in sw["gia_coperte"] if e["sim_type"] == "cap_loss_at_risk"]
            assert len(cov) == 1
            assert cov[0]["protezione_attiva_dal"] == "2026-08-17"
            # il danno storico e' attribuito al passato, non al futuro
            assert cov[0]["delta_storico_pre_protezione"] > 1000
            assert "NON raccomandare" in cov[0]["esito"]
        finally:
            db.close()

    def test_violazioni_persistenti_dopo_fix_promosse_sul_residuo(self, in_memory_db, fake_mt5):
        import sim_engine
        db = in_memory_db()
        try:
            # violazioni ANCHE dopo l'attivazione: la protezione non basta ->
            # promossa (strutturale, esente dal gate campione) sul solo residuo
            _mk(db, msg_id=30, status="sl_hit", pnl_usd=-1800.0,
                created=datetime(2026, 8, 10, 8, 0))   # pre-fix (esclusa dal calcolo)
            _mk(db, msg_id=31, status="sl_hit", pnl_usd=-1300.0,
                created=datetime(2026, 8, 20, 8, 0))   # POST-fix: residuo reale
            _mk(db, msg_id=32, pnl_usd=100.0, created=datetime(2026, 8, 21, 8, 0))
            sw = sim_engine.sweep(db)
            cap = [e for e in sw["promosse"] if e["sim_type"] == "cap_loss_at_risk"]
            assert len(cap) == 1
            assert cap[0]["delta_pnl"] == 300.0            # SOLO la violazione post-fix
            assert cap[0]["coperta_da"]
            assert cap[0]["delta_storico_pre_protezione"] == 800.0  # il resto e' passato
        finally:
            db.close()


class TestDossierEDemotion:
    def test_dossier_contiene_protezioni_con_efficacia(self, in_memory_db, fake_mt5):
        import ai_advisor
        db = in_memory_db()
        try:
            _mk(db, msg_id=40, status="sl_hit", pnl_usd=-1500.0,
                created=datetime(2026, 8, 10, 8, 0))
            _mk(db, msg_id=41, pnl_usd=200.0, created=datetime(2026, 8, 20, 8, 0))
            d = ai_advisor.build_dossier(db)
            prot = d["protezioni_attive"]
            assert len(prot) >= 5
            cap = next(p for p in prot if "670" in p["nome"])
            assert cap["attiva_dal"] == "2026-08-17"
            assert cap["problema_storico_totale_delta"] == 500.0
            assert cap["problema_residuo_dopo_attivazione"]["delta"] == 0.0
            assert cap["problema_residuo_dopo_attivazione"]["trade_toccati"] == 0
        finally:
            db.close()

    def test_llm_consiglia_protezione_esistente_demotion(self, in_memory_db, fake_mt5, monkeypatch):
        # L'LLM prova comunque a consigliare il cap (gia' attivo, residuo pulito)
        # -> il codice lo DEGRADA citando la protezione
        import ai_advisor
        db = in_memory_db()
        try:
            _mk(db, msg_id=50, status="sl_hit", pnl_usd=-1500.0,
                created=datetime(2026, 8, 10, 8, 0))    # violazione pre-fix
            _mk(db, msg_id=51, pnl_usd=200.0, created=datetime(2026, 8, 20, 8, 0))
            sections = {
                "executive_summary": "s", "trader_edge": [], "execution_gaps": [],
                "risk_profile": [], "patterns": [], "confidence_note": "",
                "recommendations": [
                    {"title": "Cap perdite al max-risk", "detail": "d", "priority": "alta",
                     "sim_type": "cap_loss_at_risk", "sim_params": "{}"},
                ],
            }
            monkeypatch.setattr(ai_advisor, "_call_llm", lambda d: (sections, 10, 10))
            rep = ai_advisor.generate_report(db)
            assert rep["status"] == "ok"
            assert len(rep["sections"]["recommendations"]) == 0
            dem = rep["sections"]["scartate_dalla_verifica"]
            assert len(dem) == 1
            assert "coperta dalla protezione" in dem[0]["demotion_reason"]
            assert "2026-08-17" in dem[0]["demotion_reason"]
        finally:
            db.close()

    def test_residuo_reale_il_consiglio_resta(self, in_memory_db, fake_mt5, monkeypatch):
        # Violazione ANCHE dopo il fix: consigliare il rafforzamento e' legittimo
        import ai_advisor
        db = in_memory_db()
        try:
            _mk(db, msg_id=60, status="sl_hit", pnl_usd=-1300.0,
                created=datetime(2026, 8, 20, 8, 0))    # POST-fix
            _mk(db, msg_id=61, pnl_usd=100.0, created=datetime(2026, 8, 21, 8, 0))
            sections = {
                "executive_summary": "s", "trader_edge": [], "execution_gaps": [],
                "risk_profile": [], "patterns": [], "confidence_note": "",
                "recommendations": [
                    {"title": "La protezione non basta: rafforzare", "detail": "d",
                     "priority": "alta", "sim_type": "cap_loss_at_risk", "sim_params": "{}"},
                ],
            }
            monkeypatch.setattr(ai_advisor, "_call_llm", lambda d: (sections, 10, 10))
            rep = ai_advisor.generate_report(db)
            recs = rep["sections"]["recommendations"]
            assert len(recs) == 1
            assert recs[0]["impact"]["validation"]["passed"] is True
            assert recs[0]["impact"]["delta_pnl"] == 300.0   # solo residuo post-fix
            assert recs[0]["impact"]["covered_by"]["attiva_dal"] == "2026-08-17"
        finally:
            db.close()
