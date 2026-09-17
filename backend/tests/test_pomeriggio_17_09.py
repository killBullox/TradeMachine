"""Pomeriggio del 17/09: #775 e #776.

#775 — segnale delle 16:10:39, "Trade Closed" del trader alle 16:12:38. Il bot,
appena ripartito, recupera il segnale dallo storico alle 16:14:29 e lo apre:
stop dopo 30 secondi, -51$. Il recupero non guardava i messaggi successivi.
Lo stesso "Trade Closed" era stato classificato "altro" dall'LLM, mentre il
testo identico delle 16:18:26 era stato letto come chiusura.

#776 — "Everyone Take Small Qty Only" arrivato insieme al segnale: la frase
non era fra quelle che dimezzano il rischio (solo risky/aggressive/high risk),
trade partito a rischio pieno, -215$."""
import types
import asyncio
import pytest
from datetime import datetime, timedelta


class TestAvvisoRischioRidotto:
    @pytest.mark.parametrize("testo", [
        "Everyone Take Small Qty Only",
        "#Risky, Keep limited qty.",
        "#HighRisky  Enter Now",
        "take only small risks from here",
        "Use small lot size",
        "Highly risky trade",
        "#RiskyTrade",
        "aggressive entry",
    ])
    def test_riconosciuto(self, testo):
        from parser import e_avviso_rischio_ridotto
        assert e_avviso_rischio_ridotto(testo)

    @pytest.mark.parametrize("testo", [
        "Near First Target",
        "Trade Closed",
        "We made a small profit",
        "lower levels soon",
        "Everyone close the Trade here, Will enter at lower levels",
        "Price is at a low level",
    ])
    def test_non_scatta_a_vuoto(self, testo):
        from parser import e_avviso_rischio_ridotto
        assert not e_avviso_rischio_ridotto(testo)

    def test_segnale_con_small_qty_nasce_risky(self):
        from parser import parse_message
        t, p = parse_message("#XAUUSD | Buy Near 4370-72 Target 1 : 4380 | Target 2 : 4390 "
                             "| Target 3 : 4400 Stoploss : 4366 Take small qty")
        assert t == "signal" and p.is_risky is True

    def test_segnale_da_llm_risky_anche_se_llm_non_lo_dice(self):
        from llm_parser import llm_to_parsed
        t, p = llm_to_parsed({"type": "signal", "symbol": "XAUUSD", "direction": "buy",
                              "entry_low": 4370, "entry_high": 4372, "tp1": 4380, "sl": 4366,
                              "is_risky": False,
                              "_raw": "Buy Near 4370-72 TP 4380 SL 4366 Keep limited qty"})
        assert t == "signal" and p.is_risky is True


def _sig(db, **kw):
    from database import Signal
    base = dict(symbol="XAUUSD", direction="buy", entry_price=4370.0,
                entry_price_high=4372.0, stoploss=4366.0, tp1=4380.0,
                status="open", is_filtered=False, is_archived=False,
                telegram_msg_id=10857, raw_message="Buy Near 4370-72",
                created_at=datetime.utcnow() - timedelta(seconds=1))
    base.update(kw)
    s = Signal(**base)
    db.add(s); db.commit(); db.refresh(s)
    return s


@pytest.fixture
def processa(in_memory_db, fake_mt5, monkeypatch):
    import telegram_client as tc
    import llm_parser
    monkeypatch.setattr(tc, "SessionLocal", in_memory_db)
    chiusure = []

    async def finto_close(db, parsed, reply_to_msg_id=None):
        chiusure.append((parsed.raw, reply_to_msg_id))
    monkeypatch.setattr(tc, "_handle_close", finto_close)

    def esegui(testo, msg_id=10900, collegato_a=None, llm=None):
        monkeypatch.setattr(llm_parser, "parse_with_llm",
                            lambda t: llm or {"type": "ignore"})
        asyncio.run(tc.process_message(msg_id, "trader", testo,
                                       reply_to_msg_id=collegato_a, use_llm=True))
        return chiusure
    return esegui


class TestSmallQtyApplicato:
    def test_caso_776_collegato(self, in_memory_db, processa):
        db = in_memory_db()
        try:
            sid = _sig(db).id
        finally:
            db.close()
        processa("Everyone Take Small Qty Only", collegato_a=10857)
        from database import Signal
        db2 = in_memory_db()
        try:
            assert db2.query(Signal).get(sid).is_risky is True
        finally:
            db2.close()

    def test_un_segnale_non_marca_quello_prima(self, in_memory_db, processa):
        """small qty dentro un NUOVO segnale vale per lui, non per il precedente."""
        db = in_memory_db()
        try:
            prima = _sig(db).id
        finally:
            db.close()
        processa("#XAUUSD | Buy Near 4380-82 Target 1 : 4390 | Target 2 : 4395 "
                 "| Target 3 : 4400 Stoploss : 4376 Take small qty", msg_id=10901)
        from database import Signal
        db2 = in_memory_db()
        try:
            assert db2.query(Signal).get(prima).is_risky is False
        finally:
            db2.close()


class TestTradeClosed:
    @pytest.mark.parametrize("testo", ["Trade Closed \U0001F64C", "**Trade Closed** ✅",
                                       "Trade closed here", "The trade is closed"])
    def test_annuncio_secco(self, testo):
        import telegram_client as tc
        assert tc.e_annuncio_trade_chiuso(testo)

    @pytest.mark.parametrize("testo", ["Trade Closed, now buy again at 4350 with sl 4340",
                                       "Near Last Target", "Trade is open"])
    def test_frasi_lunghe_restano_all_llm(self, testo):
        import telegram_client as tc
        assert not tc.e_annuncio_trade_chiuso(testo)

    def test_letto_come_chiusura_anche_se_llm_dice_altro(self, processa):
        chiusure = processa("Trade Closed \U0001F64C", msg_id=10855,
                            llm={"type": "ignore", "symbol": None})
        assert len(chiusure) == 1


class TestRecuperoInRitardo:
    """Il #775: il bot recupera un segnale dallo storico dopo un blocco."""

    SEGNALE = ("#XAUUSD | Buy Near 4377-74 Target 1 : 4380 | Target 2 : 4390 "
               "| Target 3 : 4400 Stoploss : 4372")

    def _storico(self, monkeypatch, messaggi):
        import telegram_client as tc
        ora = datetime.utcnow()

        class Client:
            async def iter_dialogs(self):
                yield types.SimpleNamespace(name=tc.GROUP_NAME, entity="g")

            async def iter_messages(self, target, **kw):
                for mid, testo, fa in sorted(messaggi, reverse=True):   # dal piu' nuovo
                    yield types.SimpleNamespace(id=mid, text=testo, sender=None,
                                                date=ora - timedelta(seconds=fa))

        async def get_client():
            return Client()
        monkeypatch.setattr(tc, "get_client", get_client)

    def _prepara(self, in_memory_db, monkeypatch):
        import telegram_client as tc
        import mt5_trader
        monkeypatch.setattr(tc, "SessionLocal", in_memory_db)
        monkeypatch.setattr(mt5_trader, "is_enabled", lambda: True)
        aperti = []
        monkeypatch.setattr(mt5_trader, "place_orders",
                            lambda sig, **kw: aperti.append(sig.id) or [1, 2, 3])
        return aperti

    def test_caso_775_trader_aveva_gia_chiuso(self, in_memory_db, fake_mt5, monkeypatch):
        import telegram_client as tc
        aperti = self._prepara(in_memory_db, monkeypatch)
        self._storico(monkeypatch, [(10852, self.SEGNALE, 230),
                                    (10854, "Everyone Modify SL 4369", 150),
                                    (10855, "Trade Closed \U0001F64C", 110)])
        asyncio.run(tc.load_history(limit=50))
        assert aperti == []
        from database import Signal
        db = in_memory_db()
        try:
            s = db.query(Signal).filter(Signal.telegram_msg_id == 10852).first()
            assert s.status == "cancelled"
            assert "replay_skip_chiuso" in (s.trade_log or "")
        finally:
            db.close()

    def test_senza_chiusure_il_recupero_funziona_ancora(self, in_memory_db, fake_mt5, monkeypatch):
        import telegram_client as tc
        aperti = self._prepara(in_memory_db, monkeypatch)
        self._storico(monkeypatch, [(10852, self.SEGNALE, 60),
                                    (10853, "Near First Target soon", 30)])
        asyncio.run(tc.load_history(limit=50))
        assert len(aperti) == 1

    def test_una_chiusura_prima_del_segnale_non_conta(self, in_memory_db, fake_mt5, monkeypatch):
        import telegram_client as tc
        aperti = self._prepara(in_memory_db, monkeypatch)
        self._storico(monkeypatch, [(10850, "Trade Closed", 400),
                                    (10852, self.SEGNALE, 60)])
        asyncio.run(tc.load_history(limit=50))
        assert len(aperti) == 1
