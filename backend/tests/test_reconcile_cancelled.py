"""Trade ENTRATI ma marcati 'cancelled': erano invisibili alle statistiche
(misurato 24/08: 6 trade, -668.34$, tutti in perdita -> stime ottimistiche).
Ora vengono riportati a 'closed', con backfill di exit_price e pnl."""
import json
import types
import pytest
from datetime import datetime, timedelta


def _deal(entry, price, profit=0.0, volume=0.83, commission=0.0, swap=0.0):
    return types.SimpleNamespace(entry=entry, price=price, profit=profit,
                                 volume=volume, commission=commission, swap=swap)


def _sig(db, **kw):
    from database import Signal
    now = datetime(2026, 8, 24, 7, 18, 17)
    base = dict(telegram_msg_id=9807, symbol="XAUUSD", direction="sell",
                entry_price=4643.0, entry_price_high=4644.0,
                actual_entry_price=4642.44, stoploss=4647.0,
                tp1=4639.0, status="cancelled", pnl_usd=-231.57,
                is_filtered=False, is_archived=False, mt5_tickets="[1]",
                raw_message="t", created_at=now, entered_at=now,
                closed_at=now + timedelta(seconds=28))
    base.update(kw)
    s = Signal(**base)
    db.add(s); db.commit(); db.refresh(s)
    return s


class TestReconcile:
    def test_trade_entrato_torna_closed(self, in_memory_db, fake_mt5, monkeypatch):
        import mt5_trader
        db = in_memory_db()
        try:
            s = _sig(db)
            monkeypatch.setattr(mt5_trader, "_get_mt5", lambda: fake_mt5)
            n = mt5_trader.reconcile_entered_but_cancelled()
            assert n == 1
            db.expire_all()
            from database import Signal
            r = db.query(Signal).filter(Signal.id == s.id).first()
            assert r.status == "closed"
            assert r.pnl_usd == -231.57          # pnl esistente non toccato
            assert any(e.get("event") == "status_reconciled"
                       for e in json.loads(r.trade_log or "[]"))
        finally:
            db.close()

    def test_backfill_exit_price_dai_deal(self, in_memory_db, fake_mt5, monkeypatch):
        import mt5_trader
        db = in_memory_db()
        try:
            s = _sig(db, exit_price=None, pnl_usd=None, mt5_tickets="[1, 2]")
            fake_mt5.history_deals_by_position[1] = [
                _deal(fake_mt5.DEAL_ENTRY_IN, 4642.44),
                _deal(fake_mt5.DEAL_ENTRY_OUT, 4643.00, profit=-50.0, volume=1.0),
            ]
            fake_mt5.history_deals_by_position[2] = [
                _deal(fake_mt5.DEAL_ENTRY_IN, 4642.44),
                _deal(fake_mt5.DEAL_ENTRY_OUT, 4645.00, profit=-100.0, volume=1.0),
            ]
            monkeypatch.setattr(mt5_trader, "_get_mt5", lambda: fake_mt5)
            assert mt5_trader.reconcile_entered_but_cancelled() == 1
            db.expire_all()
            from database import Signal
            r = db.query(Signal).filter(Signal.id == s.id).first()
            assert r.status == "closed"
            assert r.pnl_usd == -150.0
            assert r.exit_price == 4644.0        # media pesata (4643+4645)/2
        finally:
            db.close()

    def test_pending_mai_fillato_resta_cancelled(self, in_memory_db, fake_mt5, monkeypatch):
        """Un LIMIT mai riempito NON e' un trade: deve restare 'cancelled'."""
        import mt5_trader
        db = in_memory_db()
        try:
            s = _sig(db, actual_entry_price=None, pnl_usd=0.0, entered_at=None)
            monkeypatch.setattr(mt5_trader, "_get_mt5", lambda: fake_mt5)
            assert mt5_trader.reconcile_entered_but_cancelled() == 0
            db.expire_all()
            from database import Signal
            assert db.query(Signal).filter(Signal.id == s.id).first().status == "cancelled"
        finally:
            db.close()

    def test_idempotente(self, in_memory_db, fake_mt5, monkeypatch):
        import mt5_trader
        db = in_memory_db()
        try:
            _sig(db)
            monkeypatch.setattr(mt5_trader, "_get_mt5", lambda: fake_mt5)
            assert mt5_trader.reconcile_entered_but_cancelled() == 1
            assert mt5_trader.reconcile_entered_but_cancelled() == 0
        finally:
            db.close()

    def test_archiviati_ignorati(self, in_memory_db, fake_mt5, monkeypatch):
        import mt5_trader
        db = in_memory_db()
        try:
            _sig(db, is_archived=True)
            monkeypatch.setattr(mt5_trader, "_get_mt5", lambda: fake_mt5)
            assert mt5_trader.reconcile_entered_but_cancelled() == 0
        finally:
            db.close()

    def test_rientra_nelle_statistiche(self, in_memory_db, fake_mt5, monkeypatch):
        """Il punto di tutto: dopo la riconciliazione il trade e' contato."""
        import mt5_trader
        from ai_advisor import _real_closed_trades
        db = in_memory_db()
        try:
            _sig(db)
            assert len(_real_closed_trades(db)) == 0      # prima: invisibile
            monkeypatch.setattr(mt5_trader, "_get_mt5", lambda: fake_mt5)
            mt5_trader.reconcile_entered_but_cancelled()
            db.expire_all()
            trades = _real_closed_trades(db)
            assert len(trades) == 1
            assert float(trades[0].pnl_usd) == -231.57
        finally:
            db.close()
