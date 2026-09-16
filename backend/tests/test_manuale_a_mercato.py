"""Un trade manuale dice "apri a mercato": deve partire a mercato.

Caso #763 (16/09 14:37 Roma). Due difetti in fila su un BUY XAUUSD manuale:

1. L'ingresso, che era la quotazione viva del broker (ask 4340.41), e' stato
   riscritto a 4330.41 dall'auto-correzione "typo single-digit" pensata per i
   messaggi Telegram: il TP1 era vicino (4342) e lo stop lontano (4324), quindi
   il prezzo e' sembrato "asimmetrico" e quindi sbagliato.
2. Con l'ingresso 10$ sotto il mercato, il routing LIMIT/STOP dei segnali
   Telegram ha piazzato tre BUY LIMIT a 4330.41 invece di comprare subito.

Il prezzo e' salito, il TP1 e' stato raggiunto alle 14:39 e i pendenti non si
sono mai riempiti: trade perso pur avendo ragione."""
import json
import pytest
from datetime import datetime


def _sig(db, msg="[MANUALE] XAUUSD buy a mercato (4340.41) SL=4324.0 TP=4342.0/4348.0/4354.0",
         entry=4340.41, **kw):
    from database import Signal
    base = dict(symbol="XAUUSD", direction="buy", entry_price=entry,
                entry_price_high=entry, stoploss=4324.0,
                tp1=4342.0, tp2=4348.0, tp3=4354.0,
                status="pending", is_filtered=False, is_archived=False,
                risk_usd=250.0, raw_message=msg, created_at=datetime.utcnow())
    base.update(kw)
    s = Signal(**base)
    db.add(s); db.commit(); db.refresh(s)
    return s


def _mercato(fake, bid, ask):
    from tests.conftest import _SymbolInfo, _Tick
    fake.symbols_info["XAUUSD"] = _SymbolInfo(digits=2, point=0.01)
    fake.ticks["XAUUSD"] = _Tick(bid=bid, ask=ask)


def _eseguiti(fake):
    """Ordini finiti a mercato (posizioni aperte) e ordini rimasti pendenti."""
    return len(fake.positions), len(fake.orders)


@pytest.fixture
def mt5_pronto(fake_mt5, monkeypatch):
    import mt5_trader
    monkeypatch.setattr(mt5_trader, "_get_mt5", lambda: fake_mt5)
    monkeypatch.setattr(mt5_trader, "is_enabled", lambda: True)
    monkeypatch.setattr(mt5_trader, "_auto_trade_enabled", True, raising=False)
    monkeypatch.setattr(mt5_trader, "get_mt5_symbol", lambda s, default=None: "XAUUSD")
    monkeypatch.setattr(mt5_trader, "check_prop_guard", lambda *a, **k: None, raising=False)
    _mercato(fake_mt5, 4339.96, 4340.41)
    return mt5_trader


class TestIngressoAMercato:
    def test_caso_763_prezzo_salito_resta_market(self, in_memory_db, mt5_pronto,
                                                 monkeypatch):
        """Fra anteprima e invio il prezzo si e' mosso: si compra comunque ORA."""
        db = in_memory_db()
        try:
            s = _sig(db, entry=4330.41)        # ingresso ormai 10$ sotto il mercato
            tks = mt5_pronto.place_orders(s)
            assert tks, "nessun ordine inviato"
            aperte, pendenti = _eseguiti(mt5_pronto._get_mt5())
            assert (aperte, pendenti) == (3, 0)
            for p in mt5_pronto._get_mt5().positions.values():
                assert p.price_open == 4340.41      # comprato all'ask di adesso
        finally:
            db.close()

    def test_un_segnale_telegram_conserva_il_routing(self, in_memory_db, mt5_pronto):
        """Il forzare a mercato vale SOLO per i manuali: su Telegram il prezzo
        sopra la zona d'ingresso deve restare un LIMIT in attesa."""
        db = in_memory_db()
        try:
            s = _sig(db, msg="Buy Gold 4330-4331 SL 4324 TP 4342", entry=4330.41)
            mt5_pronto.place_orders(s)
            aperte, pendenti = _eseguiti(mt5_pronto._get_mt5())
            assert (aperte, pendenti) == (0, 3)     # restano in attesa
        finally:
            db.close()


class TestIngressoInAttesa:
    """L'altra scelta possibile: l'operatore indica il prezzo e aspetta."""

    def test_il_pendente_resta_pendente(self, in_memory_db, mt5_pronto):
        db = in_memory_db()
        try:
            s = _sig(db, msg="[MANUALE] XAUUSD buy in attesa a 4330.41 (mercato 4340.41) "
                             "SL=4324.0 TP=4342.0/4348.0/4354.0", entry=4330.41)
            mt5_pronto.place_orders(s)
            aperte, pendenti = _eseguiti(mt5_pronto._get_mt5())
            assert (aperte, pendenti) == (0, 3)
        finally:
            db.close()

    def test_anche_il_pendente_non_viene_autocorretto(self, in_memory_db, mt5_pronto):
        db = in_memory_db()
        try:
            s = _sig(db, msg="[MANUALE] XAUUSD buy in attesa a 4340.41 (mercato 4340.41) "
                             "SL=4324.0 TP=4342.0/4348.0/4354.0")
            mt5_pronto.place_orders(s)
            assert s.entry_price == 4340.41
        finally:
            db.close()


class TestFormDelTradeManuale:
    """L'anteprima deve calcolare sul prezzo scelto, non su quello di adesso."""

    def _body(self, **kw):
        import main
        base = dict(symbol="XAUUSD", direction="buy", stoploss=4324.0,
                    tp1=4342.0, tp2=4348.0, tp3=4354.0)
        base.update(kw)
        return main.ManualTradeIn(**base)

    def test_pendente_calcola_sul_prezzo_scelto(self, in_memory_db, fake_mt5, monkeypatch):
        import main, risk
        import price_service as ps
        db = in_memory_db()
        try:
            monkeypatch.setattr(ps, "get_current_price", lambda sym: 4340.41)
            monkeypatch.setattr(risk, "calc_risk_amount", lambda *a, **k: 1000.0)
            r = main._manual_trade_preview(
                self._body(tipo_ingresso="pendente", entry=4330.41, paper=True), db)
            assert r["tipo_ingresso"] == "pendente"
            assert r["prezzo_corrente"] == 4330.41      # quello scelto
            assert r["prezzo_mercato"] == 4340.41       # quello di adesso
            assert r["distanza_stop"] == 6.41           # 4330.41 - 4324
            assert any("BUY LIMIT" in a for a in r["avvisi"])
        finally:
            db.close()

    def test_pendente_senza_prezzo_rifiutato(self, in_memory_db, fake_mt5, monkeypatch):
        import main, risk
        import price_service as ps
        db = in_memory_db()
        try:
            monkeypatch.setattr(ps, "get_current_price", lambda sym: 4340.41)
            monkeypatch.setattr(risk, "calc_risk_amount", lambda *a, **k: 1000.0)
            r = main._manual_trade_preview(
                self._body(tipo_ingresso="pendente", paper=True), db)
            assert r["ok"] is False
            assert any("prezzo di ingresso" in e for e in r["errori"])
        finally:
            db.close()

    def test_a_mercato_usa_il_prezzo_corrente(self, in_memory_db, fake_mt5, monkeypatch):
        import main, risk
        import price_service as ps
        db = in_memory_db()
        try:
            monkeypatch.setattr(ps, "get_current_price", lambda sym: 4340.41)
            monkeypatch.setattr(risk, "calc_risk_amount", lambda *a, **k: 1000.0)
            r = main._manual_trade_preview(self._body(paper=True), db)
            assert r["tipo_ingresso"] == "mercato"
            assert r["prezzo_corrente"] == 4340.41
        finally:
            db.close()

    def test_tipo_sconosciuto_rifiutato(self, in_memory_db, fake_mt5, monkeypatch):
        import main, risk
        import price_service as ps
        db = in_memory_db()
        try:
            monkeypatch.setattr(ps, "get_current_price", lambda sym: 4340.41)
            monkeypatch.setattr(risk, "calc_risk_amount", lambda *a, **k: 1000.0)
            r = main._manual_trade_preview(self._body(tipo_ingresso="boh", paper=True), db)
            assert r["ok"] is False
        finally:
            db.close()


class TestNienteAutocorrezioniSuiManuali:
    def test_ingresso_manuale_non_viene_riscritto(self, in_memory_db, mt5_pronto):
        """Il prezzo del manuale e' la quotazione del broker: non e' un refuso."""
        db = in_memory_db()
        try:
            s = _sig(db)                       # entry 4340.41, TP1 vicino, SL lontano
            mt5_pronto.place_orders(s)
            assert s.entry_price == 4340.41    # non riscritto
        finally:
            db.close()

    def test_lo_stesso_schema_da_telegram_viene_corretto(self, in_memory_db, mt5_pronto):
        """Controprova: la correzione resta viva per i segnali del trader."""
        db = in_memory_db()
        try:
            s = _sig(db, msg="Buy Gold 4340.41 SL 4324 TP 4342")
            mt5_pronto.place_orders(s)
            assert s.entry_price == 4330.41    # correzione ancora viva
        finally:
            db.close()

    def test_il_riconoscimento_del_manuale_e_uno_solo(self):
        from pathlib import Path
        testo = (Path(__file__).resolve().parent.parent / "mt5_trader.py").read_text(
            encoding="utf-8", errors="replace")
        assert "def _e_manuale(sig)" in testo
        assert testo.count('startswith("[MANUALE")') == 1   # un solo punto di verita'
