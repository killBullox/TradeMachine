"""Un segnale rifiutato dalle guardie deve restare nello storico (25/09).

Tre segnali del trader (EURUSD 08:28, XAUUSD 09:27, XAUUSD 10:37) sono stati
bloccati dalla soglia FTMO: l'equity era a 14$ dal pavimento dei 90.000$. Il
blocco era giusto, ma il segnale spariva del tutto: nessuna riga nello storico,
e dall'app sembrava che il trader non avesse mandato niente. Uno di quei trade
ha poi fatto tutti e tre i target."""
import json
import asyncio
import pytest
from datetime import datetime


SEGNALE = ("#XAUUSD | Buy Near 4270-71  Target 1 : 4275 | Target 2 : 4279 "
           "| Target 3 : 4284  Stoploss : 4266")


@pytest.fixture
def ricevi(in_memory_db, fake_mt5, monkeypatch):
    import telegram_client as tc
    import llm_parser
    import prop_mode
    import mt5_trader
    monkeypatch.setattr(tc, "SessionLocal", in_memory_db)
    monkeypatch.setattr(mt5_trader, "_get_mt5", lambda: fake_mt5)   # serve l'equity
    monkeypatch.setattr(llm_parser, "parse_with_llm", lambda t: None)   # parser regex

    def esegui(**blocchi):
        monkeypatch.setattr(prop_mode, "should_block_new_trades",
                            lambda *a, **k: blocchi.get("daily"))
        monkeypatch.setattr(prop_mode, "check_max_concurrent_trades",
                            lambda *a, **k: blocchi.get("concorrenti"))
        monkeypatch.setattr(prop_mode, "should_block_total_dd",
                            lambda *a, **k: blocchi.get("totale"))
        asyncio.run(tc.process_message(11218, "trader", SEGNALE, use_llm=False))
        from database import Signal
        db = in_memory_db()
        try:
            return db.query(Signal).filter(Signal.telegram_msg_id == 11218).first()
        finally:
            db.close()
    return esegui


class TestSegnaleBloccatoVisibile:
    def test_soglia_totale(self, ricevi):
        motivo = ("Perdita totale prospettica: equity 90019.58$, restano 19.58$ "
                  "prima della soglia 90000.00$")
        s = ricevi(totale=motivo)
        assert s is not None, "il segnale bloccato non e' finito nello storico"
        assert s.status == "cancelled"
        assert s.symbol == "XAUUSD" and s.direction == "buy"
        assert s.stoploss == 4266.0 and s.tp1 == 4275.0
        assert motivo in (s.filter_reason or "")
        assert "non aperto" in (s.notes or "")
        eventi = [e.get("event") for e in json.loads(s.trade_log or "[]")]
        assert "prop_block" in eventi

    def test_perdita_giornaliera(self, ricevi):
        s = ricevi(daily="Kill-switch giornaliero: persi 3500$")
        assert s is not None and s.status == "cancelled"
        assert "3500" in (s.filter_reason or "")

    def test_troppi_trade_aperti(self, ricevi):
        s = ricevi(concorrenti="Gia' 3 trade aperti")
        assert s is not None and s.status == "cancelled"

    def test_niente_ordini_al_broker(self, ricevi, monkeypatch):
        """Registrarlo nello storico non deve farlo partire."""
        import mt5_trader
        inviati = []
        monkeypatch.setattr(mt5_trader, "place_orders",
                            lambda sig, **kw: inviati.append(sig.id) or [])
        ricevi(totale="soglia totale")
        assert inviati == []

    def test_senza_blocchi_il_segnale_e_normale(self, ricevi):
        s = ricevi()
        assert s is not None
        assert s.status != "cancelled" or (s.filter_reason or "") == ""
