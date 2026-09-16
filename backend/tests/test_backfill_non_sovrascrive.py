"""Il backfill e' una RICOSTRUZIONE dai prezzi: non deve mai sovrascrivere il
registro di un trade i cui ordini sono finiti davvero sul broker.

Caso #761 (16/09): i ticket erano solo dentro il log (non erano stati salvati
sul segnale), backfill_all guardava solo i campi ticket, non ha saltato il
trade e ha riscritto il registro — cancellando l'unica prova di cosa fosse
stato mandato al mercato e rendendo cieco il riaggancio automatico."""
import json
import pytest
from datetime import datetime


def _sig(db, trade_log=None, **kw):
    from database import Signal
    base = dict(symbol="XAUUSD", direction="buy", entry_price=4333.98,
                entry_price_high=4333.98, stoploss=4325.0, tp1=4341.0,
                status="pending", is_filtered=False, is_archived=False,
                raw_message="[MANUALE] test", created_at=datetime.utcnow(),
                trade_log=trade_log)
    base.update(kw)
    s = Signal(**base)
    db.add(s); db.commit(); db.refresh(s)
    return s


def _log(*eventi):
    return json.dumps([{"ts": "2026-09-16T08:26:38Z", "event": e,
                        "detail": f"TP1: ticket=184575436 | BUY MARKET"}
                       for e in eventi])


class TestProtezioneRegistro:
    def test_riconosce_gli_ordini_reali(self, in_memory_db, fake_mt5):
        import price_service as ps
        db = in_memory_db()
        try:
            assert ps.ha_eventi_di_esecuzione_reale(_sig(db, _log("mt5_order_sent")))
            assert ps.ha_eventi_di_esecuzione_reale(_sig(db, _log("mt5_placed")))
            assert ps.ha_eventi_di_esecuzione_reale(_sig(db, _log("mt5_preparing")))
        finally:
            db.close()

    def test_un_registro_simulato_non_e_protetto(self, in_memory_db, fake_mt5):
        import price_service as ps
        db = in_memory_db()
        try:
            assert ps.ha_eventi_di_esecuzione_reale(_sig(db, _log("entry", "tp1"))) is False
            assert ps.ha_eventi_di_esecuzione_reale(_sig(db, None)) is False
            assert ps.ha_eventi_di_esecuzione_reale(_sig(db, "non-json")) is False
        finally:
            db.close()

    @pytest.mark.asyncio
    async def test_il_backfill_salta_i_trade_con_ordini_reali(self, in_memory_db,
                                                              fake_mt5, monkeypatch):
        """Il caso #761: senza ticket sul segnale ma con ordini nel log."""
        import price_service as ps
        monkeypatch.setattr(ps, "SessionLocal", in_memory_db)
        db = in_memory_db()
        try:
            originale = _log("mt5_preparing", "mt5_order_sent", "mt5_order_sent")
            s = _sig(db, trade_log=originale)
            sid, atteso = s.id, s.trade_log
        finally:
            db.close()
        chiamate = []
        monkeypatch.setattr(ps, "_backfill_signal",
                            lambda sig: chiamate.append(sig.id) or
                            {"trade_log": json.dumps([{"event": "entry"}]),
                             "status": "open"})
        await ps.backfill_all()
        assert sid not in chiamate     # nemmeno valutato
        from database import Signal
        db2 = in_memory_db()
        try:
            assert db2.query(Signal).filter(Signal.id == sid).first().trade_log == atteso
        finally:
            db2.close()

    @pytest.mark.asyncio
    async def test_un_segnale_senza_ordini_reali_viene_valutato(self, in_memory_db,
                                                                fake_mt5, monkeypatch):
        """Il backfill deve continuare a fare il suo lavoro sugli altri."""
        import price_service as ps
        monkeypatch.setattr(ps, "SessionLocal", in_memory_db)
        db = in_memory_db()
        try:
            sid = _sig(db, trade_log=None).id
        finally:
            db.close()
        chiamate = []
        monkeypatch.setattr(ps, "_backfill_signal",
                            lambda sig: chiamate.append(sig.id) or None)
        await ps.backfill_all()
        assert sid in chiamate
    
    @pytest.mark.asyncio
    async def test_i_trade_con_ticket_restano_esclusi(self, in_memory_db,
                                                      fake_mt5, monkeypatch):
        import price_service as ps
        monkeypatch.setattr(ps, "SessionLocal", in_memory_db)
        db = in_memory_db()
        try:
            sid = _sig(db, mt5_tickets=json.dumps([1, 2, 3]), status="open",
                       trade_log=None).id
        finally:
            db.close()
        chiamate = []
        monkeypatch.setattr(ps, "_backfill_signal",
                            lambda sig: chiamate.append(sig.id) or None)
        await ps.backfill_all()
        assert sid not in chiamate
