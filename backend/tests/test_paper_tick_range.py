"""Paper trade: i livelli sfiorati fra due campionamenti non devono sfuggire.

Caso #752 (14/09): TP2 4305 toccato a 4305.08 per 3 secondi, il monitor
campiona ogni 15s e la simulazione e' rimasta ferma a TP1. Ora, per i soli
paper, i confronti usano l'ESCURSIONE (minimo/massimo) dell'intervallo, come
farebbe un take-profit sul broker."""
import json
import pytest
from datetime import datetime, timedelta


def _sig(db, **kw):
    from database import Signal
    now = datetime(2026, 9, 14, 15, 57, 6)
    base = dict(symbol="XAUUSD", direction="buy",
                entry_price=4291.5, entry_price_high=4291.5,
                actual_entry_price=4291.5, stoploss=4284.0,
                tp1=4300.0, tp2=4305.0, tp3=None,
                status="open", is_filtered=True, is_archived=False,
                raw_message="[MANUALE PAPER] test", position_size=0.32,
                created_at=now, entered_at=now)
    base.update(kw)
    s = Signal(**base)
    db.add(s); db.commit(); db.refresh(s)
    return s


class TestEscursione:
    def test_tp_sfiorato_fra_due_campionamenti(self, in_memory_db, fake_mt5):
        """Prezzo puntuale 4301 (sotto il TP2) ma nell'intervallo ha toccato
        4305.08: il TP2 deve essere registrato."""
        import price_service as ps
        db = in_memory_db()
        try:
            s = _sig(db, status="tp1", stoploss=4291.5)
            ps._update_realtime(db, s, 4301.0, datetime.utcnow(),
                                price_range=(4299.0, 4305.08))
            db.refresh(s)
            assert s.status == "tp2"
            eventi = [e["event"] for e in json.loads(s.trade_log or "[]")]
            assert "tp2" in eventi
        finally:
            db.close()

    def test_senza_escursione_il_tp_sfugge(self, in_memory_db, fake_mt5):
        """Comportamento vecchio: col solo prezzo puntuale il TP non si vede."""
        import price_service as ps
        db = in_memory_db()
        try:
            s = _sig(db, status="tp1", stoploss=4291.5)
            ps._update_realtime(db, s, 4301.0, datetime.utcnow())
            db.refresh(s)
            assert s.status == "tp1"
        finally:
            db.close()

    def test_sell_simmetrico(self, in_memory_db, fake_mt5):
        import price_service as ps
        db = in_memory_db()
        try:
            s = _sig(db, direction="sell", actual_entry_price=4300.0,
                     stoploss=4310.0, tp1=4292.0, tp2=4285.0, status="open")
            ps._update_realtime(db, s, 4295.0, datetime.utcnow(),
                                price_range=(4284.9, 4301.0))
            db.refresh(s)
            assert s.status == "tp2"
        finally:
            db.close()

    def test_stop_sfiorato_viene_visto(self, in_memory_db, fake_mt5):
        """Vale anche a sfavore: uno stop toccato per un istante conta."""
        import price_service as ps
        db = in_memory_db()
        try:
            s = _sig(db, status="open")
            ps._update_realtime(db, s, 4295.0, datetime.utcnow(),
                                price_range=(4283.9, 4296.0))
            db.refresh(s)
            assert s.status == "sl_hit"
            assert s.exit_price == 4284.0
        finally:
            db.close()

    def test_stop_prima_del_target_nel_dubbio(self, in_memory_db, fake_mt5):
        """Se nell'intervallo sono stati toccati sia stop sia target non
        sappiamo l'ordine: si sceglie lo scenario peggiore (stop)."""
        import price_service as ps
        db = in_memory_db()
        try:
            s = _sig(db, status="open")
            ps._update_realtime(db, s, 4295.0, datetime.utcnow(),
                                price_range=(4283.0, 4306.0))
            db.refresh(s)
            assert s.status == "sl_hit"
        finally:
            db.close()

    def test_i_reali_restano_sul_prezzo_puntuale(self, in_memory_db, fake_mt5):
        """Sui trade reali decide il broker: l'escursione non va applicata.
        Un signal non filtrato senza ticket non viene comunque trackato qui."""
        import price_service as ps
        db = in_memory_db()
        try:
            s = _sig(db, is_filtered=False, status="tp1", stoploss=4291.5)
            ps._update_realtime(db, s, 4301.0, datetime.utcnow(),
                                price_range=(4299.0, 4305.08))
            db.refresh(s)
            assert s.status == "tp1"
        finally:
            db.close()


class TestFinestraCampionamento:
    def test_primo_giro_nessun_range(self, in_memory_db, fake_mt5, monkeypatch):
        """Al primo passaggio non c'e' un riferimento precedente."""
        import price_service as ps
        ps._ultimo_campionamento.clear()
        assert ps.range_dal_check_precedente("XAUUSD", datetime.utcnow()) is None

    def test_buco_troppo_lungo_ignorato(self, in_memory_db, fake_mt5, monkeypatch):
        """Dopo un riavvio lungo non si ricostruisce mezz'ora di storia."""
        import price_service as ps
        ora = datetime.utcnow()
        ps._ultimo_campionamento["XAUUSD"] = ora - timedelta(minutes=30)
        chiamate = []
        monkeypatch.setattr(ps, "get_ticks_mt5",
                            lambda *a, **k: chiamate.append(a) or None)
        assert ps.range_dal_check_precedente("XAUUSD", ora) is None
        assert chiamate == []       # non ha nemmeno interrogato MT5
