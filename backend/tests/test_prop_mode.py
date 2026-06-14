"""Test su prop_mode.get_prop_settings / is_prop_mode / update_peak_equity.

Garantisce che:
- Account con prop_mode=False ritorna sempre None → NESSUNA logica prop attiva
- Account con prop_mode=True ritorna PropSettings con i campi opzionali
- update_peak_equity rispetta il flag prop_mode
- Schema DB additivo (nuove colonne nullable) non rompe nulla
"""
import pytest


@pytest.fixture
def prop_db(in_memory_db, monkeypatch):
    """In-memory DB con account di test."""
    SessionLocal = in_memory_db
    # Reimporta prop_mode con il nuovo SessionLocal patched
    import sys
    for mod in list(sys.modules.keys()):
        if mod.startswith("prop_mode"):
            del sys.modules[mod]
    yield SessionLocal


def _make_account(SessionLocal, **kwargs):
    from database import Mt5Account
    db = SessionLocal()
    try:
        acc = Mt5Account(
            login=kwargs.get("login", 12345),
            server=kwargs.get("server", "Test-Server"),
            label=kwargs.get("label", "Test Account"),
            is_active=kwargs.get("is_active", True),
            prop_mode=kwargs.get("prop_mode", False),
            daily_dd_limit_usd=kwargs.get("daily_dd_limit_usd"),
            peak_equity_usd=kwargs.get("peak_equity_usd"),
            max_total_dd_usd=kwargs.get("max_total_dd_usd"),
            consistency_threshold_pct=kwargs.get("consistency_threshold_pct"),
            max_concurrent_trades=kwargs.get("max_concurrent_trades"),
        )
        db.add(acc)
        db.commit()
        db.refresh(acc)
        return acc
    finally:
        db.close()


class TestGetPropSettings:
    def test_avatrade_prop_mode_false_ritorna_none(self, prop_db):
        """Caso fondamentale: account demo Avatrade-like → get_prop_settings = None.
        Garantisce che nessuna logica prop venga eseguita."""
        _make_account(prop_db, prop_mode=False, daily_dd_limit_usd=None)
        from prop_mode import get_prop_settings
        assert get_prop_settings() is None

    def test_account_prop_mode_true_ritorna_settings(self, prop_db):
        _make_account(prop_db, prop_mode=True, daily_dd_limit_usd=500,
                       max_total_dd_usd=2000)
        from prop_mode import get_prop_settings
        s = get_prop_settings()
        assert s is not None
        assert s.daily_dd_limit_usd == 500
        assert s.max_total_dd_usd == 2000

    def test_nessun_account_attivo_ritorna_none(self, prop_db):
        _make_account(prop_db, is_active=False, prop_mode=True)
        from prop_mode import get_prop_settings
        assert get_prop_settings() is None

    def test_settings_con_campi_nulli_e_consentito(self, prop_db):
        """Permettiamo prop_mode=True ma alcune guardie disattive (None)."""
        _make_account(prop_db, prop_mode=True,
                       daily_dd_limit_usd=500,  # attiva
                       max_total_dd_usd=None,  # disattiva
                       max_concurrent_trades=None)  # disattiva
        from prop_mode import get_prop_settings
        s = get_prop_settings()
        assert s is not None
        assert s.daily_dd_limit_usd == 500
        assert s.max_total_dd_usd is None
        assert s.max_concurrent_trades is None


class TestIsPropMode:
    def test_avatrade_is_prop_mode_false(self, prop_db):
        _make_account(prop_db, prop_mode=False)
        from prop_mode import is_prop_mode
        assert is_prop_mode() is False

    def test_funded_elite_is_prop_mode_true(self, prop_db):
        _make_account(prop_db, prop_mode=True, daily_dd_limit_usd=500)
        from prop_mode import is_prop_mode
        assert is_prop_mode() is True


class TestUpdatePeakEquity:
    def test_no_op_su_avatrade(self, prop_db):
        _make_account(prop_db, prop_mode=False, peak_equity_usd=None)
        from prop_mode import update_peak_equity
        # Avatrade: ritorna None, peak resta None
        assert update_peak_equity(25500.0) is None

    def test_aggiorna_se_nuovo_max(self, prop_db):
        _make_account(prop_db, prop_mode=True, peak_equity_usd=25000)
        from prop_mode import update_peak_equity
        new_peak = update_peak_equity(25500.0)
        assert new_peak == 25500.0
        # Re-check via DB
        from database import Mt5Account
        db = prop_db()
        try:
            acc = db.query(Mt5Account).first()
            assert acc.peak_equity_usd == 25500.0
        finally:
            db.close()

    def test_non_decremento_peak(self, prop_db):
        _make_account(prop_db, prop_mode=True, peak_equity_usd=26000)
        from prop_mode import update_peak_equity
        # Equity inferiore al peak → peak invariato
        result = update_peak_equity(25500.0)
        assert result == 26000.0


class TestSchemaAdditive:
    """Garantisce che lo schema additivo non rompa i campi pre-esistenti."""

    def test_account_creation_senza_prop_fields(self, prop_db):
        """Crea un account senza specificare alcun campo prop → defaults OK."""
        from database import Mt5Account
        db = prop_db()
        try:
            acc = Mt5Account(login=99999, server="X", label="Bare")
            db.add(acc)
            db.commit()
            db.refresh(acc)
            assert acc.prop_mode is False or acc.prop_mode == 0
            assert acc.daily_dd_limit_usd is None
            assert acc.peak_equity_usd is None
            assert acc.max_concurrent_trades is None
        finally:
            db.close()

    def test_account_pre_esistente_invariato_dopo_migrazione(self, prop_db):
        """Simula record pre-migrazione: campi base settati, prop a None.
        Comportamento atteso: caricabile senza errori."""
        from database import Mt5Account
        db = prop_db()
        try:
            acc = Mt5Account(login=11111, server="Avatrade", label="Avatrade Demo",
                              is_demo=True, is_active=True, broker="avatrade")
            db.add(acc)
            db.commit()
            db.refresh(acc)
            assert acc.is_active is True
            assert acc.broker == "avatrade"
            assert acc.prop_mode in (False, 0)
        finally:
            db.close()
