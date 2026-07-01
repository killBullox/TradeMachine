"""Test sul daily DD kill-switch (prop_mode.should_block_new_trades).

Garanzie:
- prop_mode=False (Avatrade-like) → NESSUN blocco, qualunque sia il P&L giornaliero
- prop_mode=True ma daily_dd_limit_usd=None → NESSUN blocco
- prop_mode=True + soglia + P&L sotto soglia → blocco con stringa motivazione
- P&L positivo → niente blocco
- P&L pari a -limite esatto → blocco (segno >= ma testato come <=)
"""
import pytest


@pytest.fixture
def prop_db_with_acc(in_memory_db, monkeypatch):
    """In-memory DB con SessionLocal patched."""
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
            server=kwargs.get("server", "Test"),
            label=kwargs.get("label", "TestAcc"),
            is_active=kwargs.get("is_active", True),
            prop_mode=kwargs.get("prop_mode", False),
            daily_dd_limit_usd=kwargs.get("daily_dd_limit_usd"),
        )
        db.add(acc)
        db.commit()
    finally:
        db.close()


def _make_signal_today(SessionLocal, pnl: float, mt5_account: int = 99999):
    """Crea un signal chiuso oggi con pnl_usd. Il default mt5_account=99999
    matcha _make_account default login=99999 (indispensabile per il filtro
    per-account del prop_mode)."""
    from database import Signal
    from datetime import datetime
    db = SessionLocal()
    try:
        sig = Signal(
            symbol="XAUUSD", direction="buy", status="sl_hit",
            pnl_usd=pnl, closed_at=datetime.utcnow(),
            mt5_account=mt5_account,
        )
        db.add(sig)
        db.commit()
    finally:
        db.close()


class TestKillSwitchAvatrade:
    """Account NON prop → MAI blocchi, qualunque sia il P&L giornaliero."""

    def test_avatrade_no_block_anche_con_grosso_loss(self, prop_db_with_acc):
        _make_account(prop_db_with_acc, prop_mode=False, daily_dd_limit_usd=None)
        _make_signal_today(prop_db_with_acc, pnl=-2000)  # perdita enorme
        from prop_mode import should_block_new_trades
        assert should_block_new_trades() is None

    def test_avatrade_no_block_anche_con_breakeven(self, prop_db_with_acc):
        _make_account(prop_db_with_acc, prop_mode=False)
        from prop_mode import should_block_new_trades
        assert should_block_new_trades() is None


class TestKillSwitchProp:
    def test_prop_senza_limite_no_block(self, prop_db_with_acc):
        """prop_mode=True ma daily_dd_limit_usd=None → niente blocco."""
        _make_account(prop_db_with_acc, prop_mode=True, daily_dd_limit_usd=None)
        _make_signal_today(prop_db_with_acc, pnl=-1500)
        from prop_mode import should_block_new_trades
        assert should_block_new_trades() is None

    def test_prop_pnl_zero_no_block(self, prop_db_with_acc):
        _make_account(prop_db_with_acc, prop_mode=True, daily_dd_limit_usd=500)
        from prop_mode import should_block_new_trades
        assert should_block_new_trades() is None

    def test_prop_pnl_positivo_no_block(self, prop_db_with_acc):
        _make_account(prop_db_with_acc, prop_mode=True, daily_dd_limit_usd=500)
        _make_signal_today(prop_db_with_acc, pnl=300)
        from prop_mode import should_block_new_trades
        assert should_block_new_trades() is None

    def test_prop_pnl_negativo_sopra_soglia_no_block(self, prop_db_with_acc):
        """P&L = -400 con limite 500 → ancora sopra soglia, no block."""
        _make_account(prop_db_with_acc, prop_mode=True, daily_dd_limit_usd=500)
        _make_signal_today(prop_db_with_acc, pnl=-400)
        from prop_mode import should_block_new_trades
        assert should_block_new_trades() is None

    def test_prop_pnl_pari_a_limite_blocca(self, prop_db_with_acc):
        """P&L = -500 con limite 500 → blocca (testato come <=)."""
        _make_account(prop_db_with_acc, prop_mode=True, daily_dd_limit_usd=500)
        _make_signal_today(prop_db_with_acc, pnl=-500)
        from prop_mode import should_block_new_trades
        reason = should_block_new_trades()
        assert reason is not None
        assert "kill-switch" in reason.lower()
        assert "500" in reason

    def test_prop_pnl_oltre_soglia_blocca(self, prop_db_with_acc):
        _make_account(prop_db_with_acc, prop_mode=True, daily_dd_limit_usd=500,
                       label="Funded Elite 25K")
        _make_signal_today(prop_db_with_acc, pnl=-200)
        _make_signal_today(prop_db_with_acc, pnl=-400)
        from prop_mode import should_block_new_trades
        reason = should_block_new_trades()
        assert reason is not None
        assert "Funded Elite 25K" in reason
        assert "-600" in reason

    def test_prop_pnl_misto_somma_blocca_se_negativa(self, prop_db_with_acc):
        """Trade misti win/loss: somma è quella che conta."""
        _make_account(prop_db_with_acc, prop_mode=True, daily_dd_limit_usd=300)
        _make_signal_today(prop_db_with_acc, pnl=-500)
        _make_signal_today(prop_db_with_acc, pnl=150)
        # Somma = -350, sotto limite -300 → blocca
        from prop_mode import should_block_new_trades
        assert should_block_new_trades() is not None


class TestGetTodayPnl:
    def test_no_trades_oggi_ritorna_zero(self, prop_db_with_acc):
        _make_account(prop_db_with_acc)
        from prop_mode import get_today_pnl_usd
        assert get_today_pnl_usd() == 0.0

    def test_somma_pnl_oggi(self, prop_db_with_acc):
        _make_account(prop_db_with_acc)
        _make_signal_today(prop_db_with_acc, pnl=100)
        _make_signal_today(prop_db_with_acc, pnl=-50)
        _make_signal_today(prop_db_with_acc, pnl=200)
        from prop_mode import get_today_pnl_usd
        assert get_today_pnl_usd() == 250.0
