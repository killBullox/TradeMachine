"""Regressione del buco reale (FOMC 29/07): con l'evento FOMC in tabella, i due
segnali #622 (19:55 Roma) e #623 (19:58 Roma) sarebbero stati BLOCCATI.
FOMC 14:00 ET = 18:00 UTC = 20:00 Roma. Finestra ingressi [T-10, T+15] =
[17:50, 18:15] UTC. Flatten [T-5, T] = [17:55, 18:00] UTC."""
import pytest
from datetime import datetime


def _add_fomc(db):
    from database import NewsEvent
    ev = NewsEvent(name="FOMC Statement", event_time=datetime(2026, 7, 29, 18, 0, 0),
                   currency="USD", impact="high", flatten=True, source="forexfactory",
                   external_key="FOMC Statement|2026-07-29T14:00:00-04:00")
    db.add(ev); db.commit()
    return ev


class TestFomcRegression:
    def test_622_bloccato_1955(self, in_memory_db, fake_mt5):
        import news_filter as nf
        db = in_memory_db()
        try:
            _add_fomc(db)
            # #622 creato 19:55 Roma = 17:55 UTC -> dentro [17:50, 18:05]
            reason = nf.entry_blocked(now_utc=datetime(2026, 7, 29, 17, 55, 0), db=db)
            assert reason is not None
            assert "FOMC" in reason
        finally:
            db.close()

    def test_623_bloccato_1958(self, in_memory_db, fake_mt5):
        import news_filter as nf
        db = in_memory_db()
        try:
            _add_fomc(db)
            reason = nf.entry_blocked(now_utc=datetime(2026, 7, 29, 17, 58, 0), db=db)
            assert reason is not None
        finally:
            db.close()

    def test_fuori_finestra_non_bloccato(self, in_memory_db, fake_mt5):
        import news_filter as nf
        db = in_memory_db()
        try:
            _add_fomc(db)
            # 17:40 UTC (19:40 Roma) = prima di T-10 -> non bloccato
            assert nf.entry_blocked(now_utc=datetime(2026, 7, 29, 17, 40, 0), db=db) is None
            # 18:10 UTC (20:10 Roma) = ancora dentro T+15 -> BLOCCATO
            assert nf.entry_blocked(now_utc=datetime(2026, 7, 29, 18, 10, 0), db=db) is not None
            # 18:20 UTC (20:20 Roma) = dopo T+15 -> non bloccato
            assert nf.entry_blocked(now_utc=datetime(2026, 7, 29, 18, 20, 0), db=db) is None
        finally:
            db.close()

    def test_flatten_attivo_nella_finestra(self, in_memory_db, fake_mt5):
        import news_filter as nf
        db = in_memory_db()
        try:
            _add_fomc(db)
            # 17:56 UTC dentro [17:55, 18:00] -> flatten dovuto
            ev = nf.flatten_due(now_utc=datetime(2026, 7, 29, 17, 56, 0), db=db)
            assert ev is not None
            assert ev.name == "FOMC Statement"
        finally:
            db.close()
