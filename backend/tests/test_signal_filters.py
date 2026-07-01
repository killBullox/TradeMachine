"""Test su signal_filters: symbol exclusion + hour inclusion."""
import json
import pytest
from datetime import datetime


@pytest.fixture
def filter_db(in_memory_db):
    import sys
    for mod in list(sys.modules.keys()):
        if mod.startswith("signal_filters"):
            del sys.modules[mod]
    yield in_memory_db


def _set_filters(SessionLocal, excluded=None, allowed_hours=None):
    from database import RiskSettings
    db = SessionLocal()
    try:
        rs = db.query(RiskSettings).first()
        if not rs:
            rs = RiskSettings()
            db.add(rs)
        rs.excluded_symbols = json.dumps(excluded) if excluded else None
        rs.allowed_hours = json.dumps(allowed_hours) if allowed_hours else None
        db.add(rs); db.commit()
    finally:
        db.close()


class TestNoFilter:
    def test_default_nessun_filtro(self, filter_db):
        # Nessuna riga RiskSettings → niente filtri attivi
        from signal_filters import check_signal_filter
        from datetime import datetime
        result = check_signal_filter("XAUUSD", datetime(2026, 6, 17, 14, 0))
        assert result is None

    def test_excluded_vuoto_no_block(self, filter_db):
        _set_filters(filter_db, excluded=[], allowed_hours=None)
        from signal_filters import check_signal_filter
        assert check_signal_filter("XAUUSD", datetime(2026, 6, 17, 14, 0)) is None


class TestExcludedSymbols:
    def test_simbolo_escluso_blocca(self, filter_db):
        _set_filters(filter_db, excluded=["EURJPY"])
        from signal_filters import check_signal_filter
        result = check_signal_filter("EURJPY", datetime(2026, 6, 17, 14, 0))
        assert result is not None
        assert "EURJPY" in result

    def test_case_insensitive(self, filter_db):
        _set_filters(filter_db, excluded=["xauusd"])
        from signal_filters import check_signal_filter
        result = check_signal_filter("XAUUSD", datetime(2026, 6, 17, 14, 0))
        assert result is not None

    def test_simbolo_non_in_lista_passa(self, filter_db):
        _set_filters(filter_db, excluded=["EURJPY"])
        from signal_filters import check_signal_filter
        assert check_signal_filter("XAUUSD", datetime(2026, 6, 17, 14, 0)) is None


class TestAllowedHours:
    def test_ora_in_lista_passa(self, filter_db):
        _set_filters(filter_db, allowed_hours=[8, 9, 10, 14, 15, 16])
        from signal_filters import check_signal_filter
        # 14:30 Roma = 12:30 UTC (CEST)
        utc_ts = datetime(2026, 6, 17, 12, 30)
        result = check_signal_filter("XAUUSD", utc_ts)
        assert result is None

    def test_ora_non_in_lista_blocca(self, filter_db):
        _set_filters(filter_db, allowed_hours=[8, 9])
        from signal_filters import check_signal_filter
        # 14:30 Roma = 12:30 UTC
        utc_ts = datetime(2026, 6, 17, 12, 30)
        result = check_signal_filter("XAUUSD", utc_ts)
        assert result is not None
        assert "14" in result  # ora Roma 14

    def test_allowed_hours_none_passa_sempre(self, filter_db):
        _set_filters(filter_db, allowed_hours=None)
        from signal_filters import check_signal_filter
        # qualunque ora
        for h in (0, 6, 12, 18, 23):
            result = check_signal_filter("XAUUSD", datetime(2026, 6, 17, h, 0))
            assert result is None


class TestCombinedFilters:
    def test_simbolo_escluso_prevale_su_ora_ok(self, filter_db):
        _set_filters(filter_db, excluded=["BTCUSD"], allowed_hours=[8, 9, 10])
        from signal_filters import check_signal_filter
        # BTC alle 10 Roma (= 8 UTC) → escluso anche se ora ok
        utc_ts = datetime(2026, 6, 17, 8, 30)
        result = check_signal_filter("BTCUSD", utc_ts)
        assert result is not None
        assert "BTCUSD" in result

    def test_ora_blocca_anche_se_simbolo_ok(self, filter_db):
        _set_filters(filter_db, excluded=["BTCUSD"], allowed_hours=[8, 9])
        from signal_filters import check_signal_filter
        # XAUUSD alle 14 Roma → ora non in lista
        utc_ts = datetime(2026, 6, 17, 12, 0)
        result = check_signal_filter("XAUUSD", utc_ts)
        assert result is not None
        assert "14" in result


class TestGetSetConfig:
    def test_get_config_default(self, filter_db):
        from signal_filters import get_filter_config
        cfg = get_filter_config()
        assert cfg["excluded_symbols"] == []
        assert cfg["allowed_hours"] is None

    def test_set_excluded(self, filter_db):
        from signal_filters import set_filter_config, get_filter_config
        set_filter_config(excluded_symbols=["EURJPY", "USOIL"])
        cfg = get_filter_config()
        assert "EURJPY" in cfg["excluded_symbols"]
        assert "USOIL" in cfg["excluded_symbols"]

    def test_set_hours(self, filter_db):
        from signal_filters import set_filter_config, get_filter_config
        set_filter_config(allowed_hours=[8, 10, 15])
        cfg = get_filter_config()
        assert cfg["allowed_hours"] == [8, 10, 15]

    def test_clear_filters(self, filter_db):
        from signal_filters import set_filter_config, get_filter_config
        set_filter_config(excluded_symbols=["EURJPY"], allowed_hours=[8])
        set_filter_config(excluded_symbols=[], allowed_hours=[])
        cfg = get_filter_config()
        assert cfg["excluded_symbols"] == []
        assert cfg["allowed_hours"] is None


class TestPaperLifecycle:
    """Verifica che i signal filtered abbiano vita completa come paper trade."""

    def _make_filtered_signal(self, SessionLocal, **overrides):
        from database import Signal
        from datetime import datetime
        db = SessionLocal()
        try:
            sig = Signal(
                telegram_msg_id=99001,
                symbol="EURJPY",
                direction="buy",
                entry_price=160.00,
                entry_price_high=160.05,
                stoploss=159.50,
                tp1=160.50,
                tp2=161.00,
                tp3=161.50,
                status="pending",
                is_filtered=True,
                filter_reason="test",
                raw_message="test signal",
                created_at=datetime.utcnow(),
                **overrides,
            )
            db.add(sig); db.commit(); db.refresh(sig)
            return sig.id
        finally:
            db.close()

    def test_recalculate_signal_calcola_pnl_su_tp3(self, filter_db, fake_mt5):
        sid = self._make_filtered_signal(filter_db)
        # Simula transizioni complete: tutte e 3 le TP raggiunte
        from database import Signal
        from price_service import _append_event
        from datetime import datetime
        import risk
        db = filter_db()
        try:
            sig = db.query(Signal).filter(Signal.id == sid).first()
            now = datetime.utcnow()
            sig.status = "open"
            sig.actual_entry_price = 160.0
            sig.entered_at = now
            _append_event(sig, "entry", 160.0, now)
            _append_event(sig, "tp1", 160.50, now)
            _append_event(sig, "tp2", 161.00, now)
            _append_event(sig, "tp3", 161.50, now)
            sig.status = "tp3"
            sig.exit_price = 161.50
            sig.closed_at = now
            risk.recalculate_signal(sig)
            db.add(sig); db.commit()
            db.refresh(sig)
            assert sig.position_size is not None and sig.position_size > 0
            assert sig.pnl_usd is not None
            assert sig.pnl_usd > 0  # 3 TP positivi → profit
        finally:
            db.close()

    def test_recalculate_signal_su_sl_hit_perdita(self, filter_db, fake_mt5):
        sid = self._make_filtered_signal(filter_db)
        from database import Signal
        from price_service import _append_event
        from datetime import datetime
        import risk
        db = filter_db()
        try:
            sig = db.query(Signal).filter(Signal.id == sid).first()
            now = datetime.utcnow()
            sig.status = "open"
            sig.actual_entry_price = 160.0
            sig.entered_at = now
            _append_event(sig, "entry", 160.0, now)
            _append_event(sig, "sl_hit", 159.50, now)
            sig.status = "sl_hit"
            sig.exit_price = 159.50
            sig.closed_at = now
            risk.recalculate_signal(sig)
            db.add(sig); db.commit()
            db.refresh(sig)
            assert sig.pnl_usd is not None
            assert sig.pnl_usd < 0
        finally:
            db.close()

    def test_sl_move_aggiorna_stoploss_paper(self, filter_db, fake_mt5):
        """Su un paper trade, lo SL move TG deve aggiornare sig.stoploss
        (per i reali lo fa il sync MT5)."""
        sid = self._make_filtered_signal(filter_db)
        from database import Signal
        from telegram_client import _save_sl_move
        from parser import ParsedSLMove
        db = filter_db()
        try:
            sig = db.query(Signal).filter(Signal.id == sid).first()
            sig.status = "open"
            sig.actual_entry_price = 160.0
            db.add(sig); db.commit()
            parsed = ParsedSLMove(symbol="EURJPY", new_sl=160.00, is_breakeven=True, raw="be")
            _save_sl_move(db, parsed, msg_id=99100)
            db.refresh(sig)
            assert sig.stoploss == 160.00
        finally:
            db.close()

    def test_append_event_costruisce_trade_log_json(self, filter_db, fake_mt5):
        from database import Signal
        from price_service import _append_event
        from datetime import datetime
        import json as _json
        sid = self._make_filtered_signal(filter_db)
        db = filter_db()
        try:
            sig = db.query(Signal).filter(Signal.id == sid).first()
            _append_event(sig, "entry", 160.0, datetime.utcnow())
            _append_event(sig, "tp1", 160.50, datetime.utcnow())
            evs = _json.loads(sig.trade_log)
            assert len(evs) == 2
            assert evs[0]["event"] == "entry" and evs[0]["price"] == 160.0
            assert evs[1]["event"] == "tp1"
        finally:
            db.close()


class TestPaperSellFillZone:
    """Regression: paper SELL con entry range deve entrare quando price <= entry_high (upper),
    non quando price <= entry_low (bug #521 USTECH)."""

    def test_paper_sell_fill_al_toccare_upper(self, filter_db, fake_mt5):
        from database import Signal
        from price_service import _update_realtime
        from datetime import datetime
        db = filter_db()
        try:
            sig = Signal(
                telegram_msg_id=99500, symbol="USTECH", direction="sell",
                entry_price=29900.0, entry_price_high=29950.0,
                stoploss=30050.0, tp1=29800.0, tp2=29700.0, tp3=29600.0,
                status="pending", is_filtered=True, filter_reason="test",
                raw_message="test", created_at=datetime.utcnow(),
            )
            db.add(sig); db.commit(); db.refresh(sig)
            # Prezzo 29940 (dentro il range, sotto upper) → SELL deve fillare
            _update_realtime(db, sig, price=29940.0, now=datetime.utcnow())
            db.refresh(sig)
            assert sig.status == "open", f"SELL non entrato al toccare upper: status={sig.status}"
            assert sig.actual_entry_price == 29940.0
        finally:
            db.close()

    def test_paper_sell_non_entra_sopra_range(self, filter_db, fake_mt5):
        from database import Signal
        from price_service import _update_realtime
        from datetime import datetime
        db = filter_db()
        try:
            sig = Signal(
                telegram_msg_id=99501, symbol="USTECH", direction="sell",
                entry_price=29900.0, entry_price_high=29950.0,
                stoploss=30050.0, tp1=29800.0, tp2=29700.0, tp3=29600.0,
                status="pending", is_filtered=True, filter_reason="test",
                raw_message="test", created_at=datetime.utcnow(),
            )
            db.add(sig); db.commit(); db.refresh(sig)
            _update_realtime(db, sig, price=29960.0, now=datetime.utcnow())
            db.refresh(sig)
            assert sig.status == "pending", "SELL non deve entrare finche' price > entry_high"
        finally:
            db.close()

    def test_paper_buy_fill_al_toccare_low(self, filter_db, fake_mt5):
        from database import Signal
        from price_service import _update_realtime
        from datetime import datetime
        db = filter_db()
        try:
            sig = Signal(
                telegram_msg_id=99502, symbol="USTECH", direction="buy",
                entry_price=29900.0, entry_price_high=29950.0,
                stoploss=29800.0, tp1=30000.0, tp2=30100.0, tp3=30200.0,
                status="pending", is_filtered=True, filter_reason="test",
                raw_message="test", created_at=datetime.utcnow(),
            )
            db.add(sig); db.commit(); db.refresh(sig)
            _update_realtime(db, sig, price=29910.0, now=datetime.utcnow())
            db.refresh(sig)
            assert sig.status == "open", f"BUY non entrato al toccare low: status={sig.status}"
        finally:
            db.close()
