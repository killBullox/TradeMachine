"""Posizioni aperte sul broker che il database non conosce (caso #761, 16/09).

Un trade manuale reale e' finito sul broker con tre posizioni aperte mentre a
database restava 'pending' senza ticket: place_orders RESTITUISCE i ticket ma
non li scrive, e il percorso del trade manuale si era dimenticato di salvarli.
Il bot non le gestiva: niente break-even, niente trail, fuori dal P&L.

Il riaggancio non indovina nulla: usa i ticket che il segnale stesso ha
registrato nel proprio trade_log con gli eventi mt5_order_sent."""
import json
import types
import pytest
from datetime import datetime, timedelta


def _pos(ticket, price_open=4333.92, tp=4341.0):
    return types.SimpleNamespace(ticket=ticket, symbol="XAUUSD", type=0,
                                 volume=0.01, price_open=price_open,
                                 sl=4325.0, tp=tp, profit=-0.74)


def _log_invii(tickets):
    eventi = [{"ts": "2026-09-16T08:26:37Z", "event": "mt5_preparing",
               "detail": "Tipo ordine: BUY MARKET | lots_each=0.01"}]
    for i, t in enumerate(tickets, 1):
        eventi.append({"ts": "2026-09-16T08:26:38Z", "event": "mt5_order_sent",
                       "detail": f"TP{i}: ticket={t} | BUY MARKET | entry=4333.98 "
                                 f"| sl=4325.0 | tp=4341.0 | lots=0.01"})
    return json.dumps(eventi)


def _sig(db, tickets=(184575436, 184575438, 184575441), **kw):
    from database import Signal
    now = datetime.utcnow() - timedelta(minutes=10)
    base = dict(symbol="XAUUSD", direction="buy",
                entry_price=4333.98, entry_price_high=4333.98,
                stoploss=4325.0, tp1=4341.0, tp2=4345.0, tp3=4350.0,
                status="pending", is_filtered=False, is_archived=False,
                mt5_ticket=None, mt5_tickets=None, position_size=0.03,
                raw_message="[MANUALE] XAUUSD buy a mercato",
                created_at=now, trade_log=_log_invii(tickets))
    base.update(kw)
    s = Signal(**base)
    db.add(s); db.commit(); db.refresh(s)
    return s


class TestRiaggancio:
    def test_caso_761(self, in_memory_db, fake_mt5, monkeypatch):
        import mt5_trader
        db = in_memory_db()
        try:
            s = _sig(db)
            sid = s.id
        finally:
            db.close()
        for t in (184575436, 184575438, 184575441):
            fake_mt5.positions[t] = _pos(t)
        monkeypatch.setattr(mt5_trader, "_get_mt5", lambda: fake_mt5)
        assert mt5_trader.aggancia_posizioni_orfane() == 3
        from database import SessionLocal, Signal
        db2 = SessionLocal()
        try:
            r = db2.query(Signal).filter(Signal.id == sid).first()
            assert r.status == "open"
            assert json.loads(r.mt5_tickets) == [184575436, 184575438, 184575441]
            assert r.mt5_ticket == 184575436
            assert r.actual_entry_price == 4333.92
            assert r.entered_at is not None
            assert any(e.get("event") == "posizioni_riagganciate"
                       for e in json.loads(r.trade_log or "[]"))
        finally:
            db2.close()

    def test_non_tocca_posizioni_di_altri_trade(self, in_memory_db, fake_mt5, monkeypatch):
        """Una posizione che il segnale non ha mai inviato non gli viene data."""
        import mt5_trader
        db = in_memory_db()
        try:
            s = _sig(db)
            sid = s.id
        finally:
            db.close()
        fake_mt5.positions[999999] = _pos(999999)      # estranea
        monkeypatch.setattr(mt5_trader, "_get_mt5", lambda: fake_mt5)
        assert mt5_trader.aggancia_posizioni_orfane() == 0
        from database import SessionLocal, Signal
        db2 = SessionLocal()
        try:
            assert db2.query(Signal).filter(Signal.id == sid).first().mt5_tickets is None
        finally:
            db2.close()

    def test_ticket_gia_noti_ignorati(self, in_memory_db, fake_mt5, monkeypatch):
        import mt5_trader
        db = in_memory_db()
        try:
            _sig(db, mt5_tickets=json.dumps([184575436]), mt5_ticket=184575436,
                 status="open")
        finally:
            db.close()
        fake_mt5.positions[184575436] = _pos(184575436)
        monkeypatch.setattr(mt5_trader, "_get_mt5", lambda: fake_mt5)
        assert mt5_trader.aggancia_posizioni_orfane() == 0

    def test_idempotente(self, in_memory_db, fake_mt5, monkeypatch):
        import mt5_trader
        db = in_memory_db()
        try:
            _sig(db)
        finally:
            db.close()
        for t in (184575436, 184575438, 184575441):
            fake_mt5.positions[t] = _pos(t)
        monkeypatch.setattr(mt5_trader, "_get_mt5", lambda: fake_mt5)
        assert mt5_trader.aggancia_posizioni_orfane() == 3
        assert mt5_trader.aggancia_posizioni_orfane() == 0

    def test_segnali_vecchi_non_vengono_pescati(self, in_memory_db, fake_mt5, monkeypatch):
        """Oltre 6 ore non si riaggancia: sarebbe un accostamento azzardato."""
        import mt5_trader
        db = in_memory_db()
        try:
            _sig(db, created_at=datetime.utcnow() - timedelta(hours=10))
        finally:
            db.close()
        for t in (184575436, 184575438, 184575441):
            fake_mt5.positions[t] = _pos(t)
        monkeypatch.setattr(mt5_trader, "_get_mt5", lambda: fake_mt5)
        assert mt5_trader.aggancia_posizioni_orfane() == 0

    def test_i_paper_non_c_entrano(self, in_memory_db, fake_mt5, monkeypatch):
        import mt5_trader
        db = in_memory_db()
        try:
            _sig(db, is_filtered=True)
        finally:
            db.close()
        for t in (184575436, 184575438, 184575441):
            fake_mt5.positions[t] = _pos(t)
        monkeypatch.setattr(mt5_trader, "_get_mt5", lambda: fake_mt5)
        assert mt5_trader.aggancia_posizioni_orfane() == 0


class TestSalvataggioTicketNelManuale:
    def test_il_percorso_manuale_scrive_i_ticket(self):
        """La causa radice: place_orders restituisce i ticket, il chiamante
        deve salvarli. Il trade manuale non lo faceva."""
        from pathlib import Path
        src = (Path(__file__).resolve().parent.parent / "main.py").read_text(
            encoding="utf-8", errors="replace")
        i_place = src.index("tks = mt5_trader.place_orders(ss) or []")
        coda = src[i_place:i_place + 1400]
        assert "ss.mt5_tickets = _json_mt.dumps(tks)" in coda
        assert "ss.mt5_ticket = tks[0]" in coda
        assert 'ss.status = "open"' in coda
        assert '"mt5_placed"' in coda

    def test_il_riaggancio_gira_nel_ciclo(self):
        from pathlib import Path
        src = (Path(__file__).resolve().parent.parent / "mt5_trader.py").read_text(
            encoding="utf-8", errors="replace")
        i_def = src.index("def sync_positions()")
        assert "aggancia_posizioni_orfane()" in src[i_def:i_def + 3000]


class TestRiaggancioTicketChiusi:
    """Il #761 e' stato scoperto DOPO che lo stop aveva gia' chiuso tutto:
    il riaggancio deve funzionare anche sui ticket non piu' aperti, leggendoli
    dallo storico dei deal."""

    def _deal(self, fake, ticket, entry_tipo, price, profit=0.0):
        d = types.SimpleNamespace(entry=entry_tipo, price=price, volume=0.01,
                                  profit=profit, commission=0.0, swap=0.0, time=0)
        fake.history_deals_by_position.setdefault(ticket, []).append(d)

    def test_ticket_gia_chiusi_vengono_riagganciati(self, in_memory_db, fake_mt5, monkeypatch):
        import mt5_trader
        db = in_memory_db()
        try:
            s = _sig(db, status="closed")
            sid = s.id
        finally:
            db.close()
        for t in (184575436, 184575438, 184575441):
            self._deal(fake_mt5, t, fake_mt5.DEAL_ENTRY_IN, 4333.92)
            self._deal(fake_mt5, t, fake_mt5.DEAL_ENTRY_OUT, 4324.99, profit=-8.93)
        monkeypatch.setattr(mt5_trader, "_get_mt5", lambda: fake_mt5)
        assert mt5_trader.aggancia_posizioni_orfane() == 3
        from database import SessionLocal, Signal
        db2 = SessionLocal()
        try:
            r = db2.query(Signal).filter(Signal.id == sid).first()
            assert json.loads(r.mt5_tickets) == [184575436, 184575438, 184575441]
            assert r.actual_entry_price == 4333.92
            assert r.status == "closed"      # nessuno aperto: lo stato resta
        finally:
            db2.close()

    def test_ticket_mai_esistiti_non_si_agganciano(self, in_memory_db, fake_mt5, monkeypatch):
        """Se il broker non sa nulla di quei ticket non si inventa niente."""
        import mt5_trader
        db = in_memory_db()
        try:
            s = _sig(db)
            sid = s.id
        finally:
            db.close()
        monkeypatch.setattr(mt5_trader, "_get_mt5", lambda: fake_mt5)
        assert mt5_trader.aggancia_posizioni_orfane() == 0
        from database import SessionLocal, Signal
        db2 = SessionLocal()
        try:
            assert db2.query(Signal).filter(Signal.id == sid).first().mt5_tickets is None
        finally:
            db2.close()

    def test_poi_il_finalizzatore_ricostruisce_il_pnl(self, in_memory_db, fake_mt5, monkeypatch):
        """Riaggancio + finalizzazione: il trade rientra con i numeri veri."""
        import mt5_trader
        db = in_memory_db()
        try:
            s = _sig(db, status="closed")
            sid = s.id
        finally:
            db.close()
        for t in (184575436, 184575438, 184575441):
            self._deal(fake_mt5, t, fake_mt5.DEAL_ENTRY_IN, 4333.92)
            self._deal(fake_mt5, t, fake_mt5.DEAL_ENTRY_OUT, 4324.99, profit=-8.93)
        monkeypatch.setattr(mt5_trader, "_get_mt5", lambda: fake_mt5)
        monkeypatch.setattr(mt5_trader, "_get_mt5_utc", lambda e: datetime(2026, 9, 16, 8, 38, 8))
        mt5_trader.aggancia_posizioni_orfane()
        assert mt5_trader.finalize_orphan_closed_trades() == 1
        from database import SessionLocal, Signal
        db2 = SessionLocal()
        try:
            r = db2.query(Signal).filter(Signal.id == sid).first()
            assert r.pnl_usd == -26.79          # 3 x -8.93
            assert r.exit_price == 4324.99
            assert r.closed_at is not None
        finally:
            db2.close()
