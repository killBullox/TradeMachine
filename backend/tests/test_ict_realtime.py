"""Contesto ICT in tempo reale: enforcement exclude_setup all'intake (contesto
live), contesto salvato alla chiusura (monitor near-real-time), _decide
tollerante a pnl None (segnali nuovi)."""
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


class TestEnforcementIntake:
    def test_exclude_setup_blocca_col_contesto_live(self, in_memory_db, fake_mt5, monkeypatch):
        import advisor_rules as ar
        import ict_engine
        db = in_memory_db()
        try:
            ar.create_rule("exclude_setup", {"setup": "ob_retest"}, "No OB", "real", db=db)
            # segnale APPENA arrivato: pnl None, nessun contesto in tabella
            sig = _mk(db, msg_id=1, pnl_usd=None, status="pending",
                      actual_entry_price=None, closed_at=None, entered_at=None)
            monkeypatch.setattr(ict_engine, "build_context_for_signal",
                                lambda s: {"setup": "ob_retest", "candles_ok": True})
            reason = ar.check_signal_block(sig, db)
            assert reason is not None and "No OB" in reason and "ob_retest" in reason
        finally:
            db.close()

    def test_setup_diverso_non_blocca(self, in_memory_db, fake_mt5, monkeypatch):
        import advisor_rules as ar
        import ict_engine
        db = in_memory_db()
        try:
            ar.create_rule("exclude_setup", {"setup": "ob_retest"}, "No OB", "real", db=db)
            sig = _mk(db, msg_id=2, pnl_usd=None, status="pending",
                      actual_entry_price=None, closed_at=None, entered_at=None)
            monkeypatch.setattr(ict_engine, "build_context_for_signal",
                                lambda s: {"setup": "bos_retest", "candles_ok": True})
            assert ar.check_signal_block(sig, db) is None
        finally:
            db.close()

    def test_no_data_non_blocca(self, in_memory_db, fake_mt5, monkeypatch):
        # contesto non calcolabile -> conservativo: NON bloccare
        import advisor_rules as ar
        import ict_engine
        db = in_memory_db()
        try:
            ar.create_rule("exclude_setup", {"setup": "ob_retest"}, "No OB", "real", db=db)
            sig = _mk(db, msg_id=3, pnl_usd=None, status="pending",
                      actual_entry_price=None, closed_at=None, entered_at=None)
            monkeypatch.setattr(ict_engine, "build_context_for_signal",
                                lambda s: {"setup": "no_data", "candles_ok": False})
            assert ar.check_signal_block(sig, db) is None
        finally:
            db.close()

    def test_regole_classiche_bloccano_con_pnl_none(self, in_memory_db, fake_mt5):
        # il bug latente: float(None) uccideva il check per i segnali nuovi
        import advisor_rules as ar
        db = in_memory_db()
        try:
            ar.create_rule("exclude_direction", {"direction": "sell"}, "No sell", "real", db=db)
            sig = _mk(db, msg_id=4, direction="sell", pnl_usd=None, status="pending",
                      actual_entry_price=None, closed_at=None, entered_at=None)
            assert ar.check_signal_block(sig, db) is not None
        finally:
            db.close()


class TestContextAtClose:
    def test_save_context_if_missing(self, in_memory_db, fake_mt5, monkeypatch):
        import ict_engine
        from database import TradeContext
        db = in_memory_db()
        try:
            sig = _mk(db, msg_id=10)
            monkeypatch.setattr(ict_engine, "build_context_for_signal",
                                lambda s: {"setup": "sweep_reversal", "candles_ok": True})
            setup = ict_engine.save_context_if_missing(db, sig)
            db.commit()
            assert setup == "sweep_reversal"
            tc = db.query(TradeContext).filter(TradeContext.signal_id == sig.id).first()
            assert tc.setup == "sweep_reversal" and tc.candles_ok is True
            # idempotente
            assert ict_engine.save_context_if_missing(db, sig) is None
        finally:
            db.close()
