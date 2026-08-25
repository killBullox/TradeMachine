"""Guardie prop PROSPETTICHE (25/08) e modello di max loss STATICO.

1. Un trade non deve partire se il caso peggiore (stop pieno = max risk)
   porta la giornata oltre la soglia. Prima si bloccava solo a soglia GIA'
   sfondata: con -3.442$ di giornata, 1.000$ di rischio e limite 3.500$ il
   trade partiva e poteva chiudere a -4.442$.
2. FTMO Challenge 2-Step usa il max loss STATICO (soglia fissa al capitale
   iniziale meno il massimo), non il trailing dal picco. Il vecchio codice
   usava il trailing con peak_equity mai inizializzato -> buffer sempre pieno,
   guardia inerte."""
import pytest


def _acct(db, **kw):
    from database import Mt5Account, RiskSettings
    base = dict(login=531385773, label="FTMO Challenge 2", server="FTMO-Server3",
                is_active=True, prop_mode=True,
                daily_dd_limit_usd=3500.0, daily_dd_warning_usd=2500.0,
                max_total_dd_usd=10000.0, dd_model="static",
                initial_capital_usd=100000.0)
    base.update(kw)
    a = Mt5Account(**base)
    db.add(a)
    db.add(RiskSettings(account_size=100000.0, risk_per_trade_usd=1000.0,
                        use_fixed_usd=True, risk_per_trade_pct=1.0))
    db.commit()
    return a


class TestMaxRiskPerTrade:
    def test_legge_il_rischio_fisso(self, in_memory_db, fake_mt5):
        import prop_mode
        db = in_memory_db()
        try:
            _acct(db)
            assert prop_mode.max_risk_per_trade(db) == 1000.0
        finally:
            db.close()

    def test_percentuale_se_non_fisso(self, in_memory_db, fake_mt5):
        import prop_mode
        from database import RiskSettings
        db = in_memory_db()
        try:
            _acct(db)
            rs = db.query(RiskSettings).first()
            rs.use_fixed_usd = False
            rs.risk_per_trade_pct = 2.0
            db.commit()
            assert prop_mode.max_risk_per_trade(db) == 2000.0
        finally:
            db.close()


class TestDailyProspettico:
    def _pnl(self, monkeypatch, value):
        import prop_mode
        monkeypatch.setattr(prop_mode, "get_today_pnl_usd", lambda db=None: value)

    def test_caso_reale_25_08(self, in_memory_db, fake_mt5, monkeypatch):
        """-3.442,77$ con rischio 1.000$ e soglia 3.500$: deve BLOCCARE."""
        import prop_mode
        db = in_memory_db()
        try:
            _acct(db)
            self._pnl(monkeypatch, -3442.77)
            r = prop_mode.should_block_new_trades(db)
            assert r is not None and "prospettico" in r
        finally:
            db.close()

    def test_margine_ampio_non_blocca(self, in_memory_db, fake_mt5, monkeypatch):
        import prop_mode
        db = in_memory_db()
        try:
            _acct(db)
            self._pnl(monkeypatch, -1200.0)     # -1200 -1000 = -2200 < 3500
            assert prop_mode.should_block_new_trades(db) is None
        finally:
            db.close()

    def test_esattamente_al_limite_blocca(self, in_memory_db, fake_mt5, monkeypatch):
        import prop_mode
        db = in_memory_db()
        try:
            _acct(db)
            self._pnl(monkeypatch, -2500.0)     # -2500 -1000 = -3500 = soglia
            assert prop_mode.should_block_new_trades(db) is not None
        finally:
            db.close()

    def test_soglia_gia_sfondata_resta_bloccata(self, in_memory_db, fake_mt5, monkeypatch):
        import prop_mode
        db = in_memory_db()
        try:
            _acct(db)
            self._pnl(monkeypatch, -3600.0)
            r = prop_mode.should_block_new_trades(db)
            assert r is not None and "kill-switch" in r
        finally:
            db.close()

    def test_prop_mode_off_non_blocca_mai(self, in_memory_db, fake_mt5, monkeypatch):
        import prop_mode
        db = in_memory_db()
        try:
            _acct(db, prop_mode=False)
            self._pnl(monkeypatch, -9999.0)
            assert prop_mode.should_block_new_trades(db) is None
        finally:
            db.close()


class TestDrawdownStatico:
    def test_soglia_fissa_su_capitale_iniziale(self, in_memory_db, fake_mt5):
        import prop_mode
        db = in_memory_db()
        try:
            _acct(db)
            st = prop_mode.total_dd_status(98923.16, db)
            assert st["modello"] == "static"
            assert st["floor"] == 90000.0          # 100.000 - 10.000
            assert st["remaining_buffer"] == 8923.16
            assert st["breach"] is False
        finally:
            db.close()

    def test_il_buffer_non_e_piu_sempre_pieno(self, in_memory_db, fake_mt5):
        """Il difetto vecchio: peak = equity corrente -> buffer 10.000$ sempre."""
        import prop_mode
        db = in_memory_db()
        try:
            _acct(db, peak_equity_usd=None)
            st = prop_mode.total_dd_status(95000.0, db)
            assert st["remaining_buffer"] == 5000.0     # non 10.000
            assert st["distance_from_peak"] == 5000.0   # non 0
        finally:
            db.close()

    def test_breach_sotto_la_soglia(self, in_memory_db, fake_mt5):
        import prop_mode
        db = in_memory_db()
        try:
            _acct(db)
            assert prop_mode.total_dd_status(89999.0, db)["breach"] is True
        finally:
            db.close()

    def test_modello_trailing_ancora_disponibile(self, in_memory_db, fake_mt5):
        """FTMO 1-Step e altri prop usano il trailing: resta supportato."""
        import prop_mode
        db = in_memory_db()
        try:
            _acct(db, dd_model="trailing", peak_equity_usd=105000.0)
            st = prop_mode.total_dd_status(98000.0, db)
            assert st["modello"] == "trailing"
            assert st["floor"] == 95000.0          # 105.000 - 10.000
            assert st["remaining_buffer"] == 3000.0
        finally:
            db.close()

    def test_blocco_totale_prospettico(self, in_memory_db, fake_mt5):
        """Equity a 900$ dalla soglia con 1.000$ di rischio: non si parte."""
        import prop_mode
        db = in_memory_db()
        try:
            _acct(db)
            assert prop_mode.should_block_total_dd(90900.0, db) is not None
            assert prop_mode.should_block_total_dd(95000.0, db) is None
        finally:
            db.close()

    def test_equity_odierna_non_blocca_sul_totale(self, in_memory_db, fake_mt5):
        """98.923$ con soglia 90.000$: sul TOTALE c'e' ancora margine."""
        import prop_mode
        db = in_memory_db()
        try:
            _acct(db)
            assert prop_mode.should_block_total_dd(98923.16, db) is None
        finally:
            db.close()
