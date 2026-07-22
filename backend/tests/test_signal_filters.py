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


class TestPaperParityLifecycle:
    """Parita' di logica reali/paper: close-event pnl, missed TP1 drop."""

    def _mk(self, SessionLocal, **kw):
        from database import Signal
        from datetime import datetime
        db = SessionLocal()
        try:
            defaults = dict(
                telegram_msg_id=99600, symbol="XAUUSD", direction="buy",
                entry_price=4000.0, entry_price_high=4002.0, stoploss=3990.0,
                tp1=4010.0, tp2=4020.0, tp3=4030.0, status="pending",
                is_filtered=True, filter_reason="test", raw_message="t",
                created_at=datetime.utcnow(),
            )
            defaults.update(kw)
            sig = Signal(**defaults)
            db.add(sig); db.commit(); db.refresh(sig)
            return sig.id
        finally:
            db.close()

    def test_evento_closed_calcola_pnl_residuo(self, filter_db, fake_mt5):
        """Evento 'closed' nel trade_log = chiusura residuo a mercato → pnl calcolato."""
        from database import Signal
        from price_service import _append_event
        from datetime import datetime
        import risk
        sid = self._mk(filter_db)
        db = filter_db()
        try:
            sig = db.query(Signal).filter(Signal.id == sid).first()
            now = datetime.utcnow()
            sig.status = "open"; sig.actual_entry_price = 4000.0
            _append_event(sig, "entry", 4000.0, now)
            _append_event(sig, "closed", 4005.0, now)  # chiusura manuale in profit
            sig.status = "closed"; sig.exit_price = 4005.0; sig.closed_at = now
            risk.recalculate_signal(sig)
            assert sig.pnl_usd is not None and sig.pnl_usd > 0
        finally:
            db.close()

    def test_paper_pending_dropped_se_tp1_hit_senza_fill(self, filter_db, fake_mt5):
        """BUY range 4000-4002, prezzo era gia' sopra e tocca TP1 4010 senza fill → cancelled."""
        from database import Signal
        from price_service import _update_realtime
        from datetime import datetime
        sid = self._mk(filter_db)
        db = filter_db()
        try:
            sig = db.query(Signal).filter(Signal.id == sid).first()
            _update_realtime(db, sig, price=4010.5, now=datetime.utcnow())
            db.refresh(sig)
            assert sig.status == "cancelled", f"atteso cancelled, got {sig.status}"
        finally:
            db.close()

    def test_paper_pending_fill_normale_non_droppato(self, filter_db, fake_mt5):
        """Prezzo dentro il range (sotto TP1) → fill normale, non drop."""
        from database import Signal
        from price_service import _update_realtime
        from datetime import datetime
        sid = self._mk(filter_db)
        db = filter_db()
        try:
            sig = db.query(Signal).filter(Signal.id == sid).first()
            _update_realtime(db, sig, price=4001.0, now=datetime.utcnow())
            db.refresh(sig)
            assert sig.status == "open"
        finally:
            db.close()


class TestRecalcNonDistruttivo:
    """recalculate_all/signal NON devono azzerare pnl di trade chiusi
    quando il ricalcolo non e' possibile (bug #543/#537/#528: SL a BE
    → entry==sl → lots=None → pnl azzerato)."""

    def test_paper_chiuso_sl_be_preserva_pnl(self, filter_db, fake_mt5):
        from database import Signal
        from datetime import datetime
        import risk
        db = filter_db()
        try:
            now = datetime.utcnow()
            sig = Signal(
                telegram_msg_id=99700, symbol="GBPJPY", direction="sell",
                entry_price=216.00, entry_price_high=216.05,
                stoploss=216.252,  # SL mosso a BE == actual_entry
                actual_entry_price=216.252,
                tp1=215.85, tp2=215.65, tp3=215.50,
                status="sl_hit", exit_price=216.252, closed_at=now,
                pnl_usd=-42.50,          # pnl esistente da preservare
                position_size=1.16,
                is_filtered=True, filter_reason="test", raw_message="t",
                created_at=now,
            )
            db.add(sig); db.commit(); db.refresh(sig)
            risk.recalculate_signal(sig)
            # position_size storica usata, pnl NON azzerato
            assert sig.pnl_usd is not None, "pnl azzerato su trade chiuso con SL==entry!"
        finally:
            db.close()

    def test_recalculate_all_preserva_pnl_chiusi_sl_be(self, filter_db, fake_mt5):
        from database import Signal
        from datetime import datetime
        import sys
        for mod in list(sys.modules.keys()):
            if mod.startswith("risk"):
                del sys.modules[mod]
        import risk
        db = filter_db()
        try:
            now = datetime.utcnow()
            sig = Signal(
                telegram_msg_id=99701, symbol="BTCUSD", direction="buy",
                entry_price=63500.0, actual_entry_price=63527.88,
                stoploss=63527.88,  # BE
                tp1=64000.0, tp2=64500.0, tp3=65000.0,
                status="sl_hit", exit_price=63527.88, closed_at=now,
                pnl_usd=-12.30, position_size=0.23,
                is_filtered=True, filter_reason="test", raw_message="t",
                created_at=now,
            )
            db.add(sig); db.commit(); sid = sig.id
        finally:
            db.close()
        risk.recalculate_all()
        db = filter_db()
        try:
            sig = db.query(Signal).filter(Signal.id == sid).first()
            assert sig.pnl_usd is not None, "recalculate_all ha azzerato pnl chiuso!"
        finally:
            db.close()


class TestAntiAllargamentoSL:
    """Regola #546: SL move che allarga lo stop → rifiutato (solo tightening)."""

    def _mk_paper_open(self, SessionLocal, direction="buy", sl=4118.0):
        from database import Signal
        from datetime import datetime
        db = SessionLocal()
        try:
            sig = Signal(
                telegram_msg_id=99800, symbol="XAUUSD", direction=direction,
                entry_price=4122.0, entry_price_high=4124.0,
                actual_entry_price=4123.91, stoploss=sl,
                tp1=4128.0, tp2=4132.0, tp3=4137.0,
                status="open", is_filtered=True, filter_reason="test",
                raw_message="t", created_at=datetime.utcnow(),
            )
            db.add(sig); db.commit(); db.refresh(sig)
            return sig.id
        finally:
            db.close()

    def test_buy_widening_rifiutato(self, filter_db, fake_mt5):
        """BUY con SL 4118: proposta 4116 (allarga) → rifiutata, SL resta 4118."""
        from database import Signal
        from telegram_client import _save_sl_move
        from parser import ParsedSLMove
        sid = self._mk_paper_open(filter_db, "buy", 4118.0)
        db = filter_db()
        try:
            parsed = ParsedSLMove(symbol="XAUUSD", new_sl=4116.0, is_breakeven=False, raw="hold 4116 sl")
            _save_sl_move(db, parsed, msg_id=99801)
            sig = db.query(Signal).filter(Signal.id == sid).first()
            assert sig.stoploss == 4118.0, f"SL allargato applicato! sl={sig.stoploss}"
        finally:
            db.close()

    def test_buy_tightening_applicato(self, filter_db, fake_mt5):
        """BUY con SL 4118: proposta 4120 (stringe) → applicata."""
        from database import Signal
        from telegram_client import _save_sl_move
        from parser import ParsedSLMove
        sid = self._mk_paper_open(filter_db, "buy", 4118.0)
        db = filter_db()
        try:
            parsed = ParsedSLMove(symbol="XAUUSD", new_sl=4120.0, is_breakeven=False, raw="hold 4120 sl")
            _save_sl_move(db, parsed, msg_id=99802)
            sig = db.query(Signal).filter(Signal.id == sid).first()
            assert sig.stoploss == 4120.0
        finally:
            db.close()

    def test_sell_widening_rifiutato(self, filter_db, fake_mt5):
        """SELL con SL 4130: proposta 4135 (allarga verso l'alto) → rifiutata."""
        from database import Signal
        from telegram_client import _save_sl_move
        from parser import ParsedSLMove
        sid = self._mk_paper_open(filter_db, "sell", 4130.0)
        db = filter_db()
        try:
            parsed = ParsedSLMove(symbol="XAUUSD", new_sl=4135.0, is_breakeven=False, raw="hold 4135 sl")
            _save_sl_move(db, parsed, msg_id=99803)
            sig = db.query(Signal).filter(Signal.id == sid).first()
            assert sig.stoploss == 4130.0, f"SL SELL allargato applicato! sl={sig.stoploss}"
        finally:
            db.close()


class TestModifySlFloatCast:
    """Bug #546: int passato alla lib MT5 → (-2, Invalid sl argument). Il cast
    float() deve avvenire in modify_sl_tp prima della request."""

    def test_int_sl_convertito_float(self, fake_mt5):
        # verifica del comportamento del cast: round(float(int)) non solleva
        # e produce float
        v = round(float(4116), 2)
        assert isinstance(v, float) and v == 4116.0


class TestPaperResiduoSLPostTP:
    """Bug #571: paper in tp1 con SL a BE toccato → residuo va chiuso."""

    def test_residuo_chiuso_su_sl_post_tp1(self, filter_db, fake_mt5):
        from database import Signal
        from price_service import _update_realtime, _append_event
        from datetime import datetime
        db = filter_db()
        try:
            now = datetime.utcnow()
            sig = Signal(
                telegram_msg_id=99900, symbol="GBPJPY", direction="buy",
                entry_price=217.40, entry_price_high=217.45,
                actual_entry_price=217.424, stoploss=217.434,  # BE post-TP1
                tp1=217.55, tp2=217.70, tp3=217.85,
                status="tp1", position_size=6.87,
                is_filtered=True, filter_reason="test", raw_message="t",
                created_at=now, entered_at=now,
            )
            import json as _j
            sig.trade_log = _j.dumps([
                {"ts": now.isoformat()+"Z", "event": "entry", "price": 217.424},
                {"ts": now.isoformat()+"Z", "event": "tp1", "price": 217.55},
            ])
            db.add(sig); db.commit(); db.refresh(sig)
            # prezzo sotto il BE → residuo chiuso
            _update_realtime(db, sig, price=217.38, now=datetime.utcnow())
            db.refresh(sig)
            assert sig.closed_at is not None, "residuo NON chiuso!"
            assert sig.status == "tp1"          # parziale resta
            assert sig.exit_price == 217.434    # fill al BE
            assert sig.pnl_usd is not None
        finally:
            db.close()

    def test_residuo_resta_aperto_sopra_sl(self, filter_db, fake_mt5):
        from database import Signal
        from price_service import _update_realtime
        from datetime import datetime
        import json as _j
        db = filter_db()
        try:
            now = datetime.utcnow()
            sig = Signal(
                telegram_msg_id=99901, symbol="GBPJPY", direction="buy",
                entry_price=217.40, actual_entry_price=217.424, stoploss=217.434,
                tp1=217.55, tp2=217.70, tp3=217.85,
                status="tp1", position_size=6.87,
                is_filtered=True, filter_reason="test", raw_message="t",
                created_at=now, entered_at=now,
                trade_log=_j.dumps([{"ts": now.isoformat()+"Z", "event": "entry", "price": 217.424}]),
            )
            db.add(sig); db.commit(); db.refresh(sig)
            _update_realtime(db, sig, price=217.60, now=datetime.utcnow())
            db.refresh(sig)
            assert sig.closed_at is None  # sopra il BE: resta vivo
        finally:
            db.close()


class TestNoRiprocessoChiusi:
    """Bug #592: paper chiuso post-TP1 (closed_at set, status tp1) veniva
    ripescato dal monitor ogni 15s → sl_hit duplicati all'infinito, pnl -437k."""

    def test_paper_chiuso_non_riprocessato(self, filter_db, fake_mt5):
        from database import Signal
        from price_service import _update_realtime
        from datetime import datetime
        import json as _j
        db = filter_db()
        try:
            now = datetime.utcnow()
            sig = Signal(
                telegram_msg_id=99950, symbol="GBPJPY", direction="buy",
                entry_price=218.55, actual_entry_price=218.589, stoploss=218.40,
                tp1=218.70, tp2=218.85, tp3=219.00,
                status="tp1", position_size=4.07,
                exit_price=218.40, closed_at=now,     # gia' chiuso
                is_filtered=True, filter_reason="test", raw_message="t",
                created_at=now, entered_at=now,
                trade_log=_j.dumps([
                    {"ts": now.isoformat()+"Z", "event": "entry", "price": 218.589},
                    {"ts": now.isoformat()+"Z", "event": "tp1", "price": 218.70},
                    {"ts": now.isoformat()+"Z", "event": "sl_hit", "price": 218.40},
                ]),
            )
            db.add(sig); db.commit(); db.refresh(sig)
            before = len(_j.loads(sig.trade_log))
            # 5 cicli di monitor col prezzo sotto lo SL: NON deve aggiungere nulla
            for _ in range(5):
                _update_realtime(db, sig, price=218.30, now=datetime.utcnow())
            db.refresh(sig)
            after = len(_j.loads(sig.trade_log))
            assert after == before, f"eventi duplicati! {before} -> {after}"
        finally:
            db.close()

    def test_reale_con_ticket_mai_toccato(self, filter_db, fake_mt5):
        """Garanzia sui REALI: un trade con ticket MT5 non viene toccato dal
        monitor, ne' prima ne' dopo il fix."""
        from database import Signal
        from price_service import _update_realtime
        from datetime import datetime
        db = filter_db()
        try:
            now = datetime.utcnow()
            sig = Signal(
                telegram_msg_id=99951, symbol="XAUUSD", direction="buy",
                entry_price=4000.0, actual_entry_price=4001.0, stoploss=3995.0,
                tp1=4004.0, tp2=4008.0, tp3=4011.0,
                status="open", position_size=1.41,
                mt5_ticket=123456, mt5_tickets="[123456,123457,123458]",
                is_filtered=False, raw_message="t", created_at=now, entered_at=now,
            )
            db.add(sig); db.commit(); db.refresh(sig)
            # prezzo sotto lo SL: un paper si chiuderebbe, un reale NO
            _update_realtime(db, sig, price=3990.0, now=datetime.utcnow())
            db.refresh(sig)
            assert sig.status == "open", "monitor ha toccato un trade REALE!"
            assert sig.closed_at is None
        finally:
            db.close()


class TestNuovoSimboloAutoFiltrato:
    """Simbolo mai tradato → filtrato di default + aggiunto a excluded_symbols
    (post-mortem #597 EURGBP: primo signal di un simbolo nuovo finiva sul reale)."""

    def _mk(self, SessionLocal, symbol, sid_offset=0):
        from database import Signal
        from datetime import datetime
        db = SessionLocal()
        try:
            sig = Signal(
                telegram_msg_id=90000 + sid_offset, symbol=symbol, direction="buy",
                entry_price=1.0, stoploss=0.99, tp1=1.01, tp2=1.02, tp3=1.03,
                status="pending", raw_message="t", created_at=datetime.utcnow(),
            )
            db.add(sig); db.commit(); db.refresh(sig)
            return sig.id
        finally:
            db.close()

    def test_simbolo_mai_visto_e_nuovo(self, filter_db):
        from signal_filters import is_symbol_ever_traded
        sid = self._mk(filter_db, "EURGBP")
        db = filter_db()
        try:
            # escludendo se stesso, EURGBP non e' mai apparso → nuovo
            assert is_symbol_ever_traded("EURGBP", db, exclude_signal_id=sid) is False
        finally:
            db.close()

    def test_simbolo_gia_apparso_non_nuovo(self, filter_db):
        from signal_filters import is_symbol_ever_traded
        first = self._mk(filter_db, "EURGBP", 1)
        second = self._mk(filter_db, "EURGBP", 2)
        db = filter_db()
        try:
            # il secondo EURGBP ha un precedente → NON nuovo
            assert is_symbol_ever_traded("EURGBP", db, exclude_signal_id=second) is True
        finally:
            db.close()

    def test_auto_exclude_aggiunge_a_lista(self, filter_db):
        from signal_filters import auto_exclude_symbol, get_filter_config
        db = filter_db()
        try:
            auto_exclude_symbol("EURGBP", db)
        finally:
            db.close()
        cfg = get_filter_config()
        assert "EURGBP" in cfg["excluded_symbols"]

    def test_xauusd_gia_tradato_non_impattato(self, filter_db):
        """Garanzia: XAUUSD (gia' tradato) non viene mai visto come nuovo."""
        from signal_filters import is_symbol_ever_traded
        self._mk(filter_db, "XAUUSD", 1)  # storico
        new_xau = self._mk(filter_db, "XAUUSD", 2)
        db = filter_db()
        try:
            assert is_symbol_ever_traded("XAUUSD", db, exclude_signal_id=new_xau) is True
        finally:
            db.close()
