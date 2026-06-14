"""Test su peak equity, trailing DD, coerenza monitor, max concurrent trades.

Tutti gated da prop_mode → con Avatrade (prop_mode=False) ritornano sempre None.
"""
import pytest


@pytest.fixture
def prop_db_fixture(in_memory_db):
    import sys
    for mod in list(sys.modules.keys()):
        if mod.startswith("prop_mode"):
            del sys.modules[mod]
    yield in_memory_db


def _make_account(SessionLocal, **kwargs):
    from database import Mt5Account
    db = SessionLocal()
    try:
        acc = Mt5Account(
            login=kwargs.get("login", 99999),
            server="Test", label=kwargs.get("label", "Test"),
            is_active=kwargs.get("is_active", True),
            prop_mode=kwargs.get("prop_mode", False),
            daily_dd_limit_usd=kwargs.get("daily_dd_limit_usd"),
            peak_equity_usd=kwargs.get("peak_equity_usd"),
            max_total_dd_usd=kwargs.get("max_total_dd_usd"),
            consistency_threshold_pct=kwargs.get("consistency_threshold_pct", 30.0),
            max_concurrent_trades=kwargs.get("max_concurrent_trades"),
        )
        db.add(acc); db.commit()
    finally:
        db.close()


def _make_signal_active(SessionLocal, tickets="[1,2,3]"):
    from database import Signal
    db = SessionLocal()
    try:
        sig = Signal(symbol="XAUUSD", direction="buy", status="open",
                     mt5_tickets=tickets)
        db.add(sig); db.commit()
    finally:
        db.close()


def _make_signal_closed(SessionLocal, pnl, closed_at):
    from database import Signal
    db = SessionLocal()
    try:
        sig = Signal(symbol="XAUUSD", direction="buy", status="tp1",
                     pnl_usd=pnl, closed_at=closed_at)
        db.add(sig); db.commit()
    finally:
        db.close()


# ─── Trailing DD ────────────────────────────────────────────────────────────

class TestTrailingDD:
    def test_avatrade_no_status(self, prop_db_fixture):
        _make_account(prop_db_fixture, prop_mode=False)
        from prop_mode import trailing_dd_status
        assert trailing_dd_status(25000) is None

    def test_prop_senza_max_dd_no_status(self, prop_db_fixture):
        _make_account(prop_db_fixture, prop_mode=True, max_total_dd_usd=None)
        from prop_mode import trailing_dd_status
        assert trailing_dd_status(25000) is None

    def test_prop_in_buffer(self, prop_db_fixture):
        _make_account(prop_db_fixture, prop_mode=True,
                       peak_equity_usd=25500, max_total_dd_usd=2000)
        from prop_mode import trailing_dd_status
        s = trailing_dd_status(25400)  # 100$ sotto peak
        assert s["breach"] is False
        assert s["warning"] is False
        assert s["distance_from_peak"] == 100
        assert s["remaining_buffer"] == 1900

    def test_prop_warning(self, prop_db_fixture):
        _make_account(prop_db_fixture, prop_mode=True,
                       peak_equity_usd=26000, max_total_dd_usd=2000)
        from prop_mode import trailing_dd_status
        # distance 1500 = 75% del max → warning ma no breach
        s = trailing_dd_status(24500)
        assert s["breach"] is False
        assert s["warning"] is True

    def test_prop_breach(self, prop_db_fixture):
        _make_account(prop_db_fixture, prop_mode=True,
                       peak_equity_usd=26000, max_total_dd_usd=2000)
        from prop_mode import trailing_dd_status
        s = trailing_dd_status(23900)  # distance 2100 >= 2000
        assert s["breach"] is True

    def test_current_supera_peak_aggiorna_locale(self, prop_db_fixture):
        _make_account(prop_db_fixture, prop_mode=True,
                       peak_equity_usd=25000, max_total_dd_usd=2000)
        from prop_mode import trailing_dd_status
        s = trailing_dd_status(26500)  # nuovo max
        assert s["peak"] == 26500
        assert s["distance_from_peak"] == 0
        assert s["remaining_buffer"] == 2000


# ─── Coerenza monitor ────────────────────────────────────────────────────────

class TestCoerenza:
    def test_avatrade_none(self, prop_db_fixture):
        _make_account(prop_db_fixture, prop_mode=False)
        from prop_mode import coerenza_status
        assert coerenza_status() is None

    def test_prop_no_trades(self, prop_db_fixture):
        _make_account(prop_db_fixture, prop_mode=True,
                       consistency_threshold_pct=30.0)
        from prop_mode import coerenza_status
        s = coerenza_status()
        assert s["total_pnl"] == 0
        assert s["max_day_pnl"] == 0
        assert s["breach"] is False

    def test_prop_single_giorno_breach(self, prop_db_fixture):
        _make_account(prop_db_fixture, prop_mode=True,
                       consistency_threshold_pct=30.0)
        from datetime import datetime, timedelta
        # 1 trade: 100% del totale è di un giorno
        _make_signal_closed(prop_db_fixture, pnl=500,
                             closed_at=datetime.utcnow() - timedelta(days=1))
        from prop_mode import coerenza_status
        s = coerenza_status()
        assert s["max_day_pct"] == 100.0
        assert s["breach"] is True
        # payout safe a $500 / 0.30 = $1666.67
        assert s["payout_safe_at"] > 1500

    def test_prop_spread_su_piu_giorni_no_breach(self, prop_db_fixture):
        _make_account(prop_db_fixture, prop_mode=True,
                       consistency_threshold_pct=30.0)
        from datetime import datetime, timedelta
        base = datetime.utcnow() - timedelta(days=5)
        # 5 giorni, $200 ciascuno = $1000 totale; ogni giorno è 20%
        for i in range(5):
            _make_signal_closed(prop_db_fixture, pnl=200,
                                 closed_at=base + timedelta(days=i, hours=10))
        from prop_mode import coerenza_status
        s = coerenza_status()
        assert s["max_day_pct"] == 20.0
        assert s["breach"] is False


# ─── Max concurrent trades ────────────────────────────────────────────────────

class TestMaxConcurrent:
    def test_avatrade_no_block(self, prop_db_fixture):
        _make_account(prop_db_fixture, prop_mode=False)
        _make_signal_active(prop_db_fixture)
        _make_signal_active(prop_db_fixture)
        from prop_mode import check_max_concurrent_trades
        assert check_max_concurrent_trades() is None

    def test_prop_senza_limite_no_block(self, prop_db_fixture):
        _make_account(prop_db_fixture, prop_mode=True, max_concurrent_trades=None)
        for _ in range(5):
            _make_signal_active(prop_db_fixture)
        from prop_mode import check_max_concurrent_trades
        assert check_max_concurrent_trades() is None

    def test_prop_sotto_limite_no_block(self, prop_db_fixture):
        _make_account(prop_db_fixture, prop_mode=True, max_concurrent_trades=3)
        _make_signal_active(prop_db_fixture)
        _make_signal_active(prop_db_fixture)
        from prop_mode import check_max_concurrent_trades
        assert check_max_concurrent_trades() is None

    def test_prop_pari_limite_blocca(self, prop_db_fixture):
        _make_account(prop_db_fixture, prop_mode=True, max_concurrent_trades=3,
                       label="Funded Elite")
        for _ in range(3):
            _make_signal_active(prop_db_fixture)
        from prop_mode import check_max_concurrent_trades
        reason = check_max_concurrent_trades()
        assert reason is not None
        assert "Funded Elite" in reason
