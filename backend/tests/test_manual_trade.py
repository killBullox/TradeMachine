"""Apertura manuale di un trade: l'utente indica direzione, stop e target,
il sistema calcola i lotti dal rischio configurato e apre a mercato.

Il calcolo del lotto e i controlli di coerenza sono testati sulla funzione
pura _manual_trade_preview: niente TestClient, che aprirebbe il DB reale."""
import types
import pytest


class _Body:
    """Sostituto di ManualTradeIn (evita di importare main a livello modulo)."""
    def __init__(self, direction="buy", stoploss=4402.0, tp1=4414.0,
                 tp2=None, tp3=None, symbol="XAUUSD"):
        self.symbol = symbol; self.direction = direction
        self.stoploss = stoploss; self.tp1 = tp1; self.tp2 = tp2; self.tp3 = tp3


def _mt5(bid=4408.44, ask=4408.89, min_vol=0.01, step=0.01, visible=True):
    info = types.SimpleNamespace(volume_min=min_vol, volume_step=step,
                                 visible=visible, digits=2, point=0.01,
                                 trade_stops_level=0)
    tick = types.SimpleNamespace(bid=bid, ask=ask)
    return types.SimpleNamespace(
        symbol_info=lambda s: info,
        symbol_info_tick=lambda s: tick,
        symbol_select=lambda s, e: True,
        account_info=lambda: types.SimpleNamespace(equity=98000.0),
    )


@pytest.fixture
def prep(in_memory_db, monkeypatch):
    """Ambiente minimo: MT5 finto, rischio 1000$, nessuna guardia che blocca."""
    import main, mt5_trader, risk as risk_module, news_filter, prop_mode
    monkeypatch.setattr(mt5_trader, "is_enabled", lambda: True)
    monkeypatch.setattr(mt5_trader, "_get_mt5", lambda: _mt5())
    monkeypatch.setattr(mt5_trader, "get_mt5_symbol", lambda s, default=None: "XAUUSD")
    monkeypatch.setattr(risk_module, "get_risk_settings", lambda: {})
    monkeypatch.setattr(risk_module, "calc_risk_amount", lambda s: 1000.0)
    monkeypatch.setattr(news_filter, "entry_blocked", lambda **k: None)
    monkeypatch.setattr(prop_mode, "should_block_new_trades", lambda db=None: None)
    monkeypatch.setattr(prop_mode, "should_block_total_dd", lambda eq, db=None: None)
    return main


class TestCalcoloLotto:
    def test_un_solo_target(self, prep, in_memory_db):
        """1000$ di rischio, stop a 6.89$ dal prezzo: 1000/(68.9 pip x 10 $/pip)."""
        db = in_memory_db()
        try:
            r = prep._manual_trade_preview(_Body(), db)
            assert r["ok"] is True
            assert r["prezzo_corrente"] == 4408.89
            assert r["distanza_stop"] == 6.89
            assert r["n_ticket"] == 1
            assert r["lotti_per_ticket"] == 1.45      # arrotondato per DIFETTO
            assert r["rischio_stimato"] <= 1000 * 1.02
        finally:
            db.close()

    def test_tre_target_dividono_i_lotti(self, prep, in_memory_db):
        db = in_memory_db()
        try:
            r = prep._manual_trade_preview(
                _Body(tp1=4414.0, tp2=4420.0, tp3=4430.0), db)
            assert r["n_ticket"] == 3
            assert r["lotti_per_ticket"] == 0.48      # 1.45/3 per difetto
            assert r["lotti_totali"] == 1.44
            assert r["rischio_stimato"] <= 1000 * 1.02
        finally:
            db.close()

    def test_stop_largo_riduce_i_lotti(self, prep, in_memory_db):
        db = in_memory_db()
        try:
            stretto = prep._manual_trade_preview(_Body(stoploss=4405.0), db)
            largo = prep._manual_trade_preview(_Body(stoploss=4380.0), db)
            assert largo["lotti_per_ticket"] < stretto["lotti_per_ticket"]
            for r in (stretto, largo):
                assert r["rischio_stimato"] <= 1000 * 1.02
        finally:
            db.close()

    def test_mai_sotto_il_lotto_minimo_del_broker(self, prep, in_memory_db, monkeypatch):
        """Stop enorme: i lotti calcolati sarebbero sotto il minimo -> si usa
        il minimo e si AVVISA che il rischio sfora."""
        import mt5_trader
        monkeypatch.setattr(mt5_trader, "_get_mt5", lambda: _mt5(min_vol=0.5, step=0.5))
        db = in_memory_db()
        try:
            r = prep._manual_trade_preview(_Body(stoploss=4000.0, tp1=4500.0), db)
            assert r["lotti_per_ticket"] == 0.5
            assert any("lotto minimo" in a for a in r["avvisi"])
        finally:
            db.close()


class TestCoerenzaLivelli:
    def test_buy_con_stop_sopra_il_prezzo(self, prep, in_memory_db):
        db = in_memory_db()
        try:
            r = prep._manual_trade_preview(_Body(direction="buy", stoploss=4500.0), db)
            assert r["ok"] is False
            assert any("deve stare SOTTO" in e for e in r["errori"])
        finally:
            db.close()

    def test_sell_con_stop_sotto_il_prezzo(self, prep, in_memory_db):
        db = in_memory_db()
        try:
            r = prep._manual_trade_preview(
                _Body(direction="sell", stoploss=4300.0, tp1=4390.0), db)
            assert r["ok"] is False
            assert any("deve stare SOPRA" in e for e in r["errori"])
        finally:
            db.close()

    def test_target_dal_lato_sbagliato(self, prep, in_memory_db):
        db = in_memory_db()
        try:
            r = prep._manual_trade_preview(_Body(tp1=4390.0), db)
            assert r["ok"] is False
            assert any("deve stare SOPRA" in e for e in r["errori"])
        finally:
            db.close()

    def test_senza_target_non_si_apre(self, prep, in_memory_db):
        db = in_memory_db()
        try:
            r = prep._manual_trade_preview(_Body(tp1=None), db)
            assert r["ok"] is False
            assert any("almeno un target" in e.lower() for e in r["errori"])
        finally:
            db.close()

    def test_direzione_invalida(self, prep, in_memory_db):
        db = in_memory_db()
        try:
            r = prep._manual_trade_preview(_Body(direction="long"), db)
            assert r["ok"] is False
        finally:
            db.close()

    def test_target_fuori_ordine_avvisa(self, prep, in_memory_db):
        """Il caso #734: TP2 oltre il TP3. Non blocca, ma lo dice."""
        db = in_memory_db()
        try:
            r = prep._manual_trade_preview(
                _Body(tp1=4414.0, tp2=4429.0, tp3=4427.0), db)
            assert r["ok"] is True
            assert any("ordine crescente" in a for a in r["avvisi"])
        finally:
            db.close()


class TestGuardie:
    def test_kill_switch_blocca(self, prep, in_memory_db, monkeypatch):
        import prop_mode
        monkeypatch.setattr(prop_mode, "should_block_new_trades",
                            lambda db=None: "Daily DD prospettico: soglia vicina")
        db = in_memory_db()
        try:
            r = prep._manual_trade_preview(_Body(), db)
            assert r["ok"] is False
            assert any("Daily DD" in e for e in r["errori"])
        finally:
            db.close()

    def test_finestra_news_blocca(self, prep, in_memory_db, monkeypatch):
        import news_filter
        monkeypatch.setattr(news_filter, "entry_blocked",
                            lambda **k: "News window attiva: Core PCE")
        db = in_memory_db()
        try:
            r = prep._manual_trade_preview(_Body(), db)
            assert r["ok"] is False
            assert any("News window" in e for e in r["errori"])
        finally:
            db.close()

    def test_mt5_spento_blocca(self, prep, in_memory_db, monkeypatch):
        import mt5_trader
        monkeypatch.setattr(mt5_trader, "is_enabled", lambda: False)
        db = in_memory_db()
        try:
            r = prep._manual_trade_preview(_Body(), db)
            assert r["ok"] is False
            assert any("MT5 non disponibile" in e for e in r["errori"])
        finally:
            db.close()


class TestListaSimboli:
    def test_alias_canonico(self):
        """Fra piu' nomi che puntano allo stesso simbolo del broker si sceglie
        quello piu' vicino al nome broker: US100.cash -> 'US100', non 'USTECH'."""
        from main import _canonical_alias
        assert _canonical_alias(
            ["USTECH", "US100", "NAS100", "NASDAQ"], "US100.cash") == "US100"
        assert _canonical_alias(["USOIL", "OIL", "WTI"], "USOIL.cash") == "USOIL"
        assert _canonical_alias(["UKOIL", "BRENT"], "UKOIL.cash") == "UKOIL"
        assert _canonical_alias(["XAUUSD"], "XAUUSD") == "XAUUSD"

    def test_alias_senza_prefisso_comune(self):
        """Su AvaTrade XAUUSD -> GOLD: nessun prefisso in comune, si tiene il
        nome logico (unico alias)."""
        from main import _canonical_alias
        assert _canonical_alias(["XAUUSD"], "GOLD") == "XAUUSD"
