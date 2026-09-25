"""#808 (25/09): rischio massimo 9$, stop preso da 18,91$.

Sotto il lotto minimo del broker (0,01) non si scende, e il sistema apre un
ticket per target: su XAUUSD con lo stop a 6,30$ di distanza ogni ticket
rischia 6,30$ e tre ne rischiano 18,90$, il doppio del limite. Il bot lo
calcolava (nel log: risk=$9 lots=0.01x3) e apriva lo stesso.

Ora i ticket vengono ridotti finche' il rischio rientra; se nemmeno un ticket
al minimo rientra, il trade non parte e resta scritto nello storico."""
import json
import pytest
from datetime import datetime


def _sig(db, sl=4298.0, tp=(4289.0, 4278.5, 4280.0), **kw):
    from database import Signal
    base = dict(symbol="XAUUSD", direction="sell", entry_price=4293.0,
                entry_price_high=4294.0, stoploss=sl,
                tp1=tp[0], tp2=tp[1], tp3=tp[2],
                status="pending", is_filtered=False, is_archived=False,
                raw_message="#XAUUSD | Sell Near 4294-93", created_at=datetime.utcnow())
    base.update(kw)
    s = Signal(**base)
    db.add(s); db.commit(); db.refresh(s)
    return s


@pytest.fixture
def mercato(fake_mt5, monkeypatch):
    """Broker con lotto minimo 0.01 e XAUUSD a 4291.70/4292.15."""
    import mt5_trader
    import risk
    from tests.conftest import _SymbolInfo, _Tick
    fake_mt5.symbols_info["XAUUSD"] = _SymbolInfo(digits=2, point=0.01,
                                                  volume_min=0.01, volume_step=0.01)
    fake_mt5.ticks["XAUUSD"] = _Tick(bid=4291.70, ask=4292.15)
    monkeypatch.setattr(mt5_trader, "_get_mt5", lambda: fake_mt5)
    monkeypatch.setattr(mt5_trader, "is_enabled", lambda: True)
    monkeypatch.setattr(mt5_trader, "_auto_trade_enabled", True, raising=False)
    monkeypatch.setattr(mt5_trader, "get_mt5_symbol", lambda s, default=None: "XAUUSD")

    def rischio(valore):
        monkeypatch.setattr(risk, "calc_risk_amount", lambda *a, **k: valore)
        monkeypatch.setattr(mt5_trader, "calc_risk_amount", lambda *a, **k: valore,
                            raising=False)
    return rischio


def _eventi(sig):
    return [e.get("event") for e in json.loads(sig.trade_log or "[]")]


class TestRischioMaiOltreIlMassimo:
    def test_caso_808_ticket_ridotti(self, in_memory_db, mercato):
        """9$ di massimo, 6,30$ per ticket: un solo ticket, non tre."""
        import mt5_trader
        mercato(9.0)
        db = in_memory_db()
        try:
            s = _sig(db)
            tickets = mt5_trader.place_orders(s)
            assert len(tickets) == 1, f"aperti {len(tickets)} ticket invece di 1"
            assert "ticket_ridotti_per_rischio" in _eventi(s)
            posizioni = list(mt5_trader._get_mt5().positions.values())
            assert len(posizioni) == 1
            # rischio calcolato sul prezzo di riempimento reale, non sul bordo
            rischio = abs(posizioni[0].price_open - 4298.0) * 100 * posizioni[0].volume
            assert rischio <= 9.0, f"rischio reale {rischio:.2f}$ oltre il massimo di 9$"
        finally:
            db.close()

    def test_due_ticket_se_ci_stanno(self, in_memory_db, mercato):
        import mt5_trader
        mercato(11.0)                      # 5,00 x 2 = 10 <= 11, tre sarebbero 15
        db = in_memory_db()
        try:
            s = _sig(db)
            assert len(mt5_trader.place_orders(s)) == 2
        finally:
            db.close()

    def test_tre_ticket_se_il_limite_lo_consente(self, in_memory_db, mercato):
        import mt5_trader
        mercato(100.0)
        db = in_memory_db()
        try:
            s = _sig(db)
            assert len(mt5_trader.place_orders(s)) == 3
            assert "ticket_ridotti_per_rischio" not in _eventi(s)
        finally:
            db.close()

    def test_se_nemmeno_un_ticket_rientra_il_trade_non_parte(self, in_memory_db, mercato):
        """4$ di massimo contro i 5$ del lotto minimo: niente ordini."""
        import mt5_trader
        mercato(4.0)
        db = in_memory_db()
        try:
            s = _sig(db)
            assert mt5_trader.place_orders(s) == []
            assert mt5_trader._get_mt5().positions == {}
            assert s.status == "cancelled"
            assert "Lotto minimo" in (s.filter_reason or "")
            assert "rischio_minimo_superato" in _eventi(s)
        finally:
            db.close()

    def test_stop_largo_nessuna_riduzione(self, in_memory_db, mercato):
        """Con lo stop lontano il lotto minimo non e' un problema: tre ticket."""
        import mt5_trader
        mercato(60.0)
        db = in_memory_db()
        try:
            s = _sig(db, sl=4310.0)        # 18,30$ di distanza -> 18,30 x 3 = 54,90
            assert len(mt5_trader.place_orders(s)) == 3
        finally:
            db.close()
