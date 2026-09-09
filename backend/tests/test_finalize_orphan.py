"""Trade chiusi sul broker ma mai finalizzati (caso #734, 09/09).

Il ticket del TP3 si e' chiuso PRIMA di quello del TP2 (target del trader
fuori ordine: TP2 4429, TP3 4427), lo status e' saltato a 'tp3' con un ticket
ancora aperto e il segnale e' uscito dalla query di sync_positions, che allora
non includeva 'tp3'. Risultato: closed_at vuoto -> P&L fuori dal totale di
giornata e dal kill-switch, e fermo a una stima intermedia."""
import json
import types
import pytest
from datetime import datetime, timedelta


def _deal(entry, price, profit=0.0, volume=0.35, commission=-1.08, tempo=0):
    return types.SimpleNamespace(entry=entry, price=price, profit=profit,
                                 volume=volume, commission=commission,
                                 swap=0.0, time=tempo)


def _sig(db, **kw):
    from database import Signal
    now = datetime(2026, 9, 9, 13, 7, 23)
    base = dict(telegram_msg_id=10383, symbol="XAUUSD", direction="buy",
                entry_price=4407.0, entry_price_high=4408.0,
                actual_entry_price=4408.81, stoploss=4408.91,
                tp1=4414.0, tp2=4429.0, tp3=4427.0,
                status="tp3", pnl_usd=1446.90, exit_price=None, closed_at=None,
                is_filtered=False, is_archived=False,
                mt5_tickets="[182735746, 182735750, 182735753]",
                mt5_account=531385773, raw_message="t",
                created_at=now, entered_at=now)
    base.update(kw)
    s = Signal(**base)
    db.add(s); db.commit(); db.refresh(s)
    return s


def _deals_reali(fake):
    """I deal veri del #734, letti da MT5."""
    IN, OUT = fake.DEAL_ENTRY_IN, fake.DEAL_ENTRY_OUT
    fake.history_deals_by_position[182735746] = [
        _deal(IN, 4408.81), _deal(OUT, 4414.02, profit=182.35, tempo=1000)]
    fake.history_deals_by_position[182735750] = [
        _deal(IN, 4408.78), _deal(OUT, 4429.07, profit=710.15, commission=-1.09, tempo=1368)]
    fake.history_deals_by_position[182735753] = [
        _deal(IN, 4408.83), _deal(OUT, 4427.03, profit=637.00, tempo=1338)]


class TestFinalizzazione:
    def test_caso_734_pnl_ed_exit_dai_deal(self, in_memory_db, fake_mt5, monkeypatch):
        import mt5_trader
        db = in_memory_db()
        try:
            s = _sig(db)
            _deals_reali(fake_mt5)
            monkeypatch.setattr(mt5_trader, "_get_mt5", lambda: fake_mt5)
            monkeypatch.setattr(mt5_trader, "_get_mt5_utc",
                                lambda e: datetime(2026, 9, 9, 13, 25, 12))
            assert mt5_trader.finalize_orphan_closed_trades() == 1
            db.expire_all()
            from database import Signal
            r = db.query(Signal).filter(Signal.id == s.id).first()
            # 182.35 + 710.15 + 637.00 - 1.08 - 1.09 - 1.08 = 1526.25
            assert r.pnl_usd == 1526.25
            assert r.closed_at is not None
            # uscita media pesata sui volumi (stesso volume: media semplice)
            assert r.exit_price == round((4414.02 + 4429.07 + 4427.03) / 3, 5)
            assert any(e.get("event") == "finalized_orphan"
                       for e in json.loads(r.trade_log or "[]"))
        finally:
            db.close()

    def test_rientra_nel_pnl_di_giornata(self, in_memory_db, fake_mt5, monkeypatch):
        """Il punto di tutto: prima non contava, adesso si'."""
        import mt5_trader
        from prop_mode import get_today_pnl_usd
        from database import Mt5Account
        db = in_memory_db()
        try:
            db.add(Mt5Account(login=531385773, label="FTMO", server="FTMO-Server3",
                              is_active=True, prop_mode=True,
                              daily_dd_limit_usd=3500.0))
            db.commit()
            oggi = datetime.utcnow().replace(hour=13, minute=7, second=0, microsecond=0)
            s = _sig(db, created_at=oggi, entered_at=oggi)
            _deals_reali(fake_mt5)
            monkeypatch.setattr(mt5_trader, "_get_mt5", lambda: fake_mt5)
            monkeypatch.setattr(mt5_trader, "_get_mt5_utc", lambda e: oggi)
            assert get_today_pnl_usd(db) == 0.0          # prima: invisibile
            mt5_trader.finalize_orphan_closed_trades()
            db.expire_all()
            assert get_today_pnl_usd(db) == 1526.25      # dopo: conteggiato
        finally:
            db.close()

    def test_ticket_ancora_aperto_non_si_tocca(self, in_memory_db, fake_mt5, monkeypatch):
        """Se una posizione e' ancora viva NON e' un orfano: guai a chiuderlo."""
        import mt5_trader
        db = in_memory_db()
        try:
            s = _sig(db)
            _deals_reali(fake_mt5)
            fake_mt5.positions[182735750] = types.SimpleNamespace(
                ticket=182735750, symbol="XAUUSD", type=0, volume=0.35,
                price_open=4408.78, sl=0.0, tp=4429.0, profit=100.0)
            monkeypatch.setattr(mt5_trader, "_get_mt5", lambda: fake_mt5)
            assert mt5_trader.finalize_orphan_closed_trades() == 0
            db.expire_all()
            from database import Signal
            assert db.query(Signal).filter(Signal.id == s.id).first().closed_at is None
        finally:
            db.close()

    def test_storico_incompleto_riprova_dopo(self, in_memory_db, fake_mt5, monkeypatch):
        """Race del #656: un deal non ancora propagato -> non finalizzare a meta'."""
        import mt5_trader
        db = in_memory_db()
        try:
            s = _sig(db)
            _deals_reali(fake_mt5)
            fake_mt5.history_deals_by_position[182735750] = [
                _deal(fake_mt5.DEAL_ENTRY_IN, 4408.78)]     # manca il deal OUT
            monkeypatch.setattr(mt5_trader, "_get_mt5", lambda: fake_mt5)
            assert mt5_trader.finalize_orphan_closed_trades() == 0
            db.expire_all()
            from database import Signal
            assert db.query(Signal).filter(Signal.id == s.id).first().closed_at is None
        finally:
            db.close()

    def test_idempotente(self, in_memory_db, fake_mt5, monkeypatch):
        import mt5_trader
        db = in_memory_db()
        try:
            _sig(db)
            _deals_reali(fake_mt5)
            monkeypatch.setattr(mt5_trader, "_get_mt5", lambda: fake_mt5)
            monkeypatch.setattr(mt5_trader, "_get_mt5_utc",
                                lambda e: datetime(2026, 9, 9, 13, 25, 12))
            assert mt5_trader.finalize_orphan_closed_trades() == 1
            assert mt5_trader.finalize_orphan_closed_trades() == 0
        finally:
            db.close()

    def test_paper_ignorati(self, in_memory_db, fake_mt5, monkeypatch):
        import mt5_trader
        db = in_memory_db()
        try:
            _sig(db, is_filtered=True)
            _deals_reali(fake_mt5)
            monkeypatch.setattr(mt5_trader, "_get_mt5", lambda: fake_mt5)
            assert mt5_trader.finalize_orphan_closed_trades() == 0
        finally:
            db.close()


class TestFiltroSync:
    def test_tp3_incluso_nella_query(self):
        """La causa radice: senza 'tp3' nel filtro il segnale usciva dal ciclo
        e non veniva mai finalizzato."""
        from pathlib import Path
        src = Path(__file__).resolve().parent.parent / "mt5_trader.py"
        testo = src.read_text(encoding="utf-8", errors="replace")
        assert 'Signal.status.in_(["open", "pending", "tp1", "tp2", "tp3"])' in testo


class TestBackfillAccount:
    def test_assegna_il_conto_attivo(self, in_memory_db, fake_mt5, monkeypatch):
        """#732: chiuso senza mt5_account -> fuori dal P&L di giornata."""
        import mt5_trader
        db = in_memory_db()
        try:
            s = _sig(db, mt5_account=None, status="tp1", pnl_usd=52.32,
                     closed_at=datetime.utcnow() - timedelta(hours=2))
            monkeypatch.setattr(mt5_trader, "MT5_ACCOUNT", 531385773)
            assert mt5_trader.backfill_mt5_account() == 1
            db.expire_all()
            from database import Signal
            assert db.query(Signal).filter(Signal.id == s.id).first().mt5_account == 531385773
        finally:
            db.close()

    def test_non_tocca_i_trade_aperti(self, in_memory_db, fake_mt5, monkeypatch):
        import mt5_trader
        db = in_memory_db()
        try:
            _sig(db, mt5_account=None, status="open", closed_at=None)
            monkeypatch.setattr(mt5_trader, "MT5_ACCOUNT", 531385773)
            assert mt5_trader.backfill_mt5_account() == 0
        finally:
            db.close()
