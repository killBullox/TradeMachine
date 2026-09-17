"""Mattina del 17/09: messaggi del trader non trattati.

1. "#HighRisky Enter Now" (09:25:42) non nomina il simbolo: il gestore
   cercava il segnale per simbolo, non trovava nulla e ignorava il messaggio.
   Il #771, piazzato 2 secondi prima come BUY LIMIT, non si e' mai riempito e
   il trader ha preso due target. Anche trovandolo, il ripiazzamento passava
   dal routing normale e avrebbe rimesso lo stesso LIMIT.
2. Il monitor prezzi faceva query e scritture sul thread dell'event loop:
   un lock SQLite fermava l'intero processo, il healthcheck (timeout 8s) lo
   uccideva — 5 riavvii in una mattina.
3. "2nd Target Done" scorreva tutti i trade del simbolo mai chiusi in DB,
   anche di settimane prima: 130 trade, 41 secondi di Telegram fermo."""
import json
import types
import asyncio
import pytest
from datetime import datetime, timedelta


def _sig(db, **kw):
    from database import Signal
    base = dict(symbol="XAUUSD", direction="buy", entry_price=4322.0,
                entry_price_high=4323.0, stoploss=4317.0,
                tp1=4330.0, tp2=4335.0, tp3=4380.0, status="pending",
                is_filtered=False, is_archived=False,
                mt5_tickets=json.dumps([184976205, 184976208, 184976211]),
                mt5_ticket=184976205, raw_message="Buy Near 4323-22",
                created_at=datetime.utcnow() - timedelta(seconds=2))
    base.update(kw)
    s = Signal(**base)
    db.add(s); db.commit(); db.refresh(s)
    return s


def _pendenti(fake, tickets):
    for t in tickets:
        fake.orders[t] = types.SimpleNamespace(ticket=t, symbol="XAUUSD", type=2)


@pytest.fixture
def enter_now(in_memory_db, fake_mt5, monkeypatch):
    """Esegue process_message su un 'Enter Now' classificato dall'LLM e
    registra come viene chiamato place_orders."""
    import telegram_client as tc
    import mt5_trader
    import llm_parser
    monkeypatch.setattr(tc, "SessionLocal", in_memory_db)
    monkeypatch.setattr(mt5_trader, "_get_mt5", lambda: fake_mt5)
    monkeypatch.setattr(mt5_trader, "is_enabled", lambda: True)
    chiamate = []

    def finto_place(sig, **kw):
        chiamate.append((sig.id, kw))
        return [999001, 999002, 999003]
    monkeypatch.setattr(mt5_trader, "place_orders", finto_place)
    fake_mt5.TRADE_ACTION_REMOVE = 8
    fake_mt5.order_send = lambda req: types.SimpleNamespace(retcode=10009)

    def esegui(simbolo=None, testo="#HighRisky  Enter Now"):
        monkeypatch.setattr(llm_parser, "parse_with_llm",
                            lambda t: {"type": "enter_now", "symbol": simbolo, "sl": None})
        asyncio.run(tc.process_message(10795, "trader", testo, use_llm=True))
        return chiamate
    return esegui


class TestEnterNowSenzaSimbolo:
    def test_caso_771_entra_a_mercato(self, in_memory_db, fake_mt5, enter_now):
        db = in_memory_db()
        try:
            s = _sig(db)
            sid = s.id
        finally:
            db.close()
        _pendenti(fake_mt5, (184976205, 184976208, 184976211))
        chiamate = enter_now()
        assert len(chiamate) == 1, "Enter Now ignorato"
        assert chiamate[0][0] == sid
        assert chiamate[0][1].get("force_market") is True

    def test_segnale_vecchio_non_viene_agganciato(self, in_memory_db, fake_mt5, enter_now):
        """Oltre la finestra un 'Enter Now' anonimo non tocca niente."""
        db = in_memory_db()
        try:
            _sig(db, created_at=datetime.utcnow() - timedelta(minutes=45))
        finally:
            db.close()
        _pendenti(fake_mt5, (184976205, 184976208, 184976211))
        assert enter_now() == []

    def test_col_simbolo_anche_a_mercato(self, in_memory_db, fake_mt5, enter_now):
        db = in_memory_db()
        try:
            _sig(db)
        finally:
            db.close()
        _pendenti(fake_mt5, (184976205, 184976208, 184976211))
        chiamate = enter_now(simbolo="XAUUSD", testo="#XAUUSD Enter Now")
        assert len(chiamate) == 1 and chiamate[0][1].get("force_market") is True

    def test_gia_dentro_non_raddoppia(self, in_memory_db, fake_mt5, enter_now):
        db = in_memory_db()
        try:
            _sig(db, status="open")
        finally:
            db.close()
        for t in (184976205, 184976208, 184976211):
            fake_mt5.positions[t] = types.SimpleNamespace(ticket=t, symbol="XAUUSD")
        assert enter_now() == []


class TestMonitorFuoriDalLoop:
    def test_il_giro_gira_in_un_thread(self):
        """Nessuna query sul thread dell'event loop."""
        import inspect
        import price_service as ps
        assert inspect.iscoroutinefunction(ps._check_open_signals)
        assert not inspect.iscoroutinefunction(ps._check_open_signals_sync)
        corpo = inspect.getsource(ps._check_open_signals)
        assert "run_in_executor(None, _check_open_signals_sync)" in corpo
        assert "SessionLocal" not in corpo.split('"""')[-1]

    def test_il_loop_resta_libero_durante_il_giro(self, monkeypatch):
        """Un giro lento non deve impedire all'event loop di rispondere."""
        import time
        import price_service as ps
        monkeypatch.setattr(ps, "_check_open_signals_sync", lambda: time.sleep(0.6))

        async def prova():
            battiti = []

            async def battito():
                for _ in range(10):
                    battiti.append(time.monotonic())
                    await asyncio.sleep(0.05)
            await asyncio.gather(ps._check_open_signals(), battito())
            return battiti
        battiti = asyncio.run(prova())
        pause = [b - a for a, b in zip(battiti, battiti[1:])]
        assert max(pause) < 0.3, pause


class TestTargetDoneSoloTradeRecenti:
    def test_la_query_ha_la_finestra_temporale(self):
        from pathlib import Path
        src = (Path(__file__).resolve().parent.parent / "telegram_client.py").read_text(
            encoding="utf-8", errors="replace")
        i = src.index("affected_sigs = db.query(Signal).filter(")
        assert "TARGET_DONE_FINESTRA_ORE" in src[i:i + 500]

    def test_finestra_ragionevole(self):
        import telegram_client as tc
        assert 6 <= tc.TARGET_DONE_FINESTRA_ORE <= 48
        assert 1 <= tc.ENTER_NOW_FINESTRA_MIN <= 30


class TestTargetDoneEseguito:
    def test_trade_vecchi_non_toccati_e_nessun_errore(self, in_memory_db, fake_mt5,
                                                     monkeypatch, capsys):
        import telegram_client as tc
        import mt5_trader
        import llm_parser
        monkeypatch.setattr(tc, "SessionLocal", in_memory_db)
        monkeypatch.setattr(mt5_trader, "_get_mt5", lambda: fake_mt5)
        monkeypatch.setattr(mt5_trader, "is_enabled", lambda: True)
        db = in_memory_db()
        try:
            vecchio = _sig(db, status="tp1", created_at=datetime.utcnow() - timedelta(days=20),
                           mt5_tickets=json.dumps([111]), mt5_ticket=111)
            recente = _sig(db, status="tp1", created_at=datetime.utcnow() - timedelta(minutes=30),
                           mt5_tickets=json.dumps([222]), mt5_ticket=222)
            vid, rid = vecchio.id, recente.id
        finally:
            db.close()
        monkeypatch.setattr(llm_parser, "parse_with_llm", lambda t: {
            "type": "update", "symbol": "XAUUSD", "status_text": "second_target_hit"})
        asyncio.run(tc.process_message(10808, "trader",
                                       "#XAUUSD | 4324.00 To 4335.00 2nd Target Done",
                                       use_llm=True))
        out = capsys.readouterr().out
        assert "Errore drop pending" not in out, out
        assert f"[TargetDone] #{vid} " not in out          # il vecchio non passa
        assert f"[TargetDone] #{rid} " in out              # il recente si'
