"""Backup news dal trader (post-mortem FOMC #622/#623): un avviso del trader
blocca gli ingressi 30 min e cancella i pending. Classificazione + enforcement."""
import pytest
from datetime import datetime, timedelta


class TestClassificazione:
    @pytest.mark.parametrize("txt", [
        "High impact news soon, stay out",
        "No trades before the news",
        "Wait for the news guys",
        "Avoid trading now",
        "No entries until after the news",
        "Stay out now, big news",
        "Don't enter, news in 5 minutes",
        "Do not trade before CPI",
    ])
    def test_avvisi_news(self, fake_mt5, txt):
        from parser import parse_message, ParsedNewsWarning
        mt, parsed = parse_message(txt)
        assert mt == "news_warning", f"{txt!r} -> {mt}"
        assert isinstance(parsed, ParsedNewsWarning)

    @pytest.mark.parametrize("txt", [
        "XAUUSD Buy Near 4550-52, TP1 4560 TP2 4570 SL 4540",
        "Everyone close the trade now",
        "Move SL to 4550",
        "Good morning traders",
        # STRETTO (post-mortem BoE 30/07): bollettini informativi e commenti
        # generici NON devono bloccare — li gestisce il calendario, non il backup.
        "Big news today",
        "Careful today, volatile session",
        "GBP Bank of England (BoE) Interest Rate Decision is scheduled at 16:30 IST. The forecast is 3.75% compared to the previous 3.75%.",
        "US CPI due tomorrow at 14:30",
    ])
    def test_non_confuso(self, fake_mt5, txt):
        from parser import parse_message
        mt, _ = parse_message(txt)
        assert mt != "news_warning", f"{txt!r} erroneamente news_warning"

    def test_exit_now_resta_close(self, fake_mt5):
        # "exit now" esplicito deve restare close, non news_warning
        from parser import parse_message
        mt, _ = parse_message("Exit now, news coming")
        assert mt == "close"

    def test_llm_mapping(self, fake_mt5):
        from llm_parser import llm_to_parsed
        mt, data = llm_to_parsed({"type": "news_warning", "_raw": "stay out, news"})
        assert mt == "news_warning"


class TestEnforcement:
    def _rs(self, db, **kw):
        from database import RiskSettings
        rs = RiskSettings(account_size=100000, risk_per_trade_pct=1.0)
        for k, v in kw.items():
            setattr(rs, k, v)
        db.add(rs); db.commit()
        return rs

    def test_blocco_30min_e_scadenza(self, in_memory_db, fake_mt5):
        import telegram_client as tc
        import news_filter as nf
        db = in_memory_db()
        try:
            self._rs(db, trader_news_backup_enabled=True)
            until, cancelled = tc.apply_trader_news_block(db, minutes=30)
            assert until is not None
            # blocco attivo adesso
            assert nf.entry_blocked(db=db) is not None
            # scaduto: forzo trader_block_until nel passato
            from database import RiskSettings
            rs = db.query(RiskSettings).first()
            rs.trader_block_until = datetime.utcnow() - timedelta(minutes=1)
            db.commit()
            assert nf.entry_blocked(db=db) is None
        finally:
            db.close()

    def test_toggle_off_nessun_blocco(self, in_memory_db, fake_mt5):
        import telegram_client as tc
        import news_filter as nf
        db = in_memory_db()
        try:
            self._rs(db, trader_news_backup_enabled=False)
            until, cancelled = tc.apply_trader_news_block(db, minutes=30)
            assert until is None
            assert nf.entry_blocked(db=db) is None
        finally:
            db.close()

    def test_cancella_paper_pending(self, in_memory_db, fake_mt5):
        import telegram_client as tc
        from database import Signal
        db = in_memory_db()
        try:
            self._rs(db, trader_news_backup_enabled=True)
            now = datetime.utcnow()
            sig = Signal(telegram_msg_id=99500, symbol="XAUUSD", direction="buy",
                         entry_price=4000.0, stoploss=3990.0, tp1=4010.0,
                         status="pending", is_filtered=True, filter_reason="test",
                         raw_message="t", created_at=now)
            db.add(sig); db.commit(); db.refresh(sig)
            tc.apply_trader_news_block(db, minutes=30)
            db.refresh(sig)
            assert sig.status == "cancelled"
        finally:
            db.close()

    def test_non_accorcia_blocco_piu_lungo(self, in_memory_db, fake_mt5):
        import telegram_client as tc
        from database import RiskSettings
        db = in_memory_db()
        try:
            self._rs(db, trader_news_backup_enabled=True)
            far = datetime.utcnow() + timedelta(minutes=90)
            rs = db.query(RiskSettings).first()
            rs.trader_block_until = far; db.commit()
            tc.apply_trader_news_block(db, minutes=30)  # 30 < 90
            rs = db.query(RiskSettings).first()
            assert rs.trader_block_until == far  # non accorciato
        finally:
            db.close()
