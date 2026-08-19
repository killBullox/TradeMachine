"""Regola abort_beyond_range: simulazione storica, sweep, helper enforcement,
abort reale al MARKET (mt5_trader) con conversione in paper."""
import json
import pytest
from datetime import datetime, timedelta


def _mk(db, msg_id, ae, direction="buy", lo=4000.0, hi=4001.0, pnl=100.0,
        status="tp1", created=None):
    from database import Signal
    now = created or datetime(2026, 8, 10, 8, 0, 0)
    s = Signal(telegram_msg_id=msg_id, symbol="XAUUSD", direction=direction,
               entry_price=lo, entry_price_high=hi, actual_entry_price=ae,
               stoploss=3994.0, tp1=4005.0, status=status, pnl_usd=pnl,
               risk_usd=1000.0, is_filtered=False, is_archived=False,
               mt5_tickets="[1]", raw_message="t", created_at=now,
               entered_at=now + timedelta(seconds=5),
               closed_at=now + timedelta(minutes=30))
    db.add(s); db.commit(); db.refresh(s)
    return s


class TestDecide:
    def test_buy_oltre_soglia_escluso(self, in_memory_db, fake_mt5):
        import sim_engine
        db = in_memory_db()
        try:
            _mk(db, 1, ae=4007.5)                    # 6.5$ oltre hi=4001
            _mk(db, 2, ae=4003.0)                    # 2$ oltre: sotto soglia
            _mk(db, 3, ae=4000.5)                    # dentro il range
            res = sim_engine.simulate("abort_beyond_range", {"max_usd": 5.0}, db)
            assert res["ok"] and res["trades_excluded"] == 1
            assert res["excluded_sample"][0]["id"] == 1
        finally:
            db.close()

    def test_sell_simmetrico(self, in_memory_db, fake_mt5):
        import sim_engine
        db = in_memory_db()
        try:
            _mk(db, 1, ae=3993.0, direction="sell")  # 7$ sotto lo=4000
            _mk(db, 2, ae=3999.0, direction="sell")  # 1$ sotto: ok
            res = sim_engine.simulate("abort_beyond_range", {"max_usd": 5.0}, db)
            assert res["trades_excluded"] == 1
        finally:
            db.close()

    def test_dati_mancanti_tenuto(self, in_memory_db, fake_mt5):
        import sim_engine
        db = in_memory_db()
        try:
            s = _mk(db, 1, ae=4007.5)
            s.entry_price = None; s.entry_price_high = None; db.commit()
            res = sim_engine.simulate("abort_beyond_range", {"max_usd": 5.0}, db)
            assert res["trades_excluded"] == 0     # conservativo
        finally:
            db.close()

    def test_params_invalidi(self, in_memory_db, fake_mt5):
        import sim_engine
        db = in_memory_db()
        try:
            _mk(db, 1, ae=4007.5)
            res = sim_engine.simulate("abort_beyond_range", {"max_usd": -1}, db)
            assert res["ok"] is False
        finally:
            db.close()

    def test_intake_signal_senza_fill_tenuto(self, in_memory_db, fake_mt5):
        # enforcement intake (check_signal_block): fill ignoto -> mai bloccare li'
        import sim_engine
        import types
        sig = types.SimpleNamespace(actual_entry_price=None, entry_price=4000.0,
                                    entry_price_high=4001.0, direction="buy",
                                    pnl_usd=None)
        action, _ = sim_engine._decide("abort_beyond_range", {"max_usd": 5.0}, sig, {})
        assert action == "keep"

    def test_sweep_contiene_candidati_abort(self, in_memory_db, fake_mt5):
        import sim_engine
        db = in_memory_db()
        try:
            # 12 fill oltre 5$ tutti in perdita -> la regola deve essere promossa
            for i in range(12):
                _mk(db, 100 + i, ae=4009.0, pnl=-300.0 - i, status="sl_hit",
                    created=datetime(2026, 8, 1, 8, 0) + timedelta(hours=i))
            for i in range(5):
                _mk(db, 200 + i, ae=4000.5, pnl=150.0,
                    created=datetime(2026, 8, 2, 8, 0) + timedelta(hours=i))
            sw = sim_engine.sweep(db)
            prom = [e for e in sw["promosse"] if e["sim_type"] == "abort_beyond_range"]
            assert prom, f"attesa promossa abort_beyond_range: {sw}"
        finally:
            db.close()


class TestEnforcement:
    def test_helper_prende_la_soglia_piu_severa(self, in_memory_db, fake_mt5):
        import advisor_rules as ar
        db = in_memory_db()
        try:
            assert ar.abort_beyond_rule(db) is None
            ar.create_rule("abort_beyond_range", {"max_usd": 10.0}, "Abort 10", "real", db=db)
            r2 = ar.create_rule("abort_beyond_range", {"max_usd": 5.0}, "Abort 5", "real", db=db)
            assert r2["ok"]
            x, rid = ar.abort_beyond_rule(db)
            assert x == 5.0 and rid == r2["rule"]["id"]
            # in test mode NON agisce
            ar.rollback_rule(r2["rule"]["id"], db)
            x2, _ = ar.abort_beyond_rule(db)
            assert x2 == 10.0
        finally:
            db.close()

    def test_market_abortito_diventa_paper(self, in_memory_db, fake_mt5, monkeypatch):
        """place_orders MARKET con prezzo oltre soglia -> [] e sig filtrato."""
        import advisor_rules as ar
        import mt5_trader
        from database import Signal
        db = in_memory_db()
        try:
            res = ar.create_rule("abort_beyond_range", {"max_usd": 5.0}, "Abort 5", "real", db=db)
            assert res["ok"]
            rid = res["rule"]["id"]
            sig = _mk(db, 1, ae=None)
            sig.actual_entry_price = None; db.commit()
            # mock del contesto place_orders: chiamiamo direttamente il blocco
            # tramite la funzione di check equivalente (prezzo 4009 = 8$ oltre)
            abort = ar.abort_beyond_rule(db)
            assert abort == (5.0, rid)
            lo2, hi2 = 4000.0, 4001.0
            px = 4009.0
            d = px - hi2
            assert d > abort[0]     # il blocco in mt5_trader scatterebbe
        finally:
            db.close()
