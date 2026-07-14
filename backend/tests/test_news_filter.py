"""Test news filter (post-mortem #570): finestre blocco/flatten + seed + friday."""
import pytest
from datetime import datetime, timedelta


@pytest.fixture
def nf_db(in_memory_db):
    import sys
    for mod in list(sys.modules.keys()):
        if mod.startswith("news_filter"):
            del sys.modules[mod]
    yield in_memory_db


def _add_event(SessionLocal, name, event_time, flatten=True):
    from database import NewsEvent
    db = SessionLocal()
    try:
        ev = NewsEvent(name=name, event_time=event_time, flatten=flatten)
        db.add(ev); db.commit(); db.refresh(ev)
        return ev.id
    finally:
        db.close()


class TestEntryBlock:
    def test_blocco_dentro_finestra(self, nf_db):
        import news_filter as nf
        ev_t = datetime(2026, 8, 12, 12, 30)
        _add_event(nf_db, "US CPI", ev_t)
        # 9 minuti prima → bloccato
        assert nf.entry_blocked(ev_t - timedelta(minutes=9)) is not None
        # 1 minuto dopo → bloccato
        assert nf.entry_blocked(ev_t + timedelta(minutes=1)) is not None
        # 4 min dopo → bloccato (fino a +5)
        assert nf.entry_blocked(ev_t + timedelta(minutes=4)) is not None

    def test_fuori_finestra_libero(self, nf_db):
        import news_filter as nf
        ev_t = datetime(2026, 8, 12, 12, 30)
        _add_event(nf_db, "US CPI", ev_t)
        assert nf.entry_blocked(ev_t - timedelta(minutes=11)) is None
        assert nf.entry_blocked(ev_t + timedelta(minutes=6)) is None
        assert nf.entry_blocked(ev_t - timedelta(hours=3)) is None

    def test_caso_570_sarebbe_bloccato(self, nf_db):
        """Il segnale delle 12:29:04 UTC con CPI alle 12:30 → bloccato."""
        import news_filter as nf
        ev_t = datetime(2026, 7, 14, 12, 30)
        _add_event(nf_db, "US CPI (June)", ev_t)
        assert nf.entry_blocked(datetime(2026, 7, 14, 12, 29, 4)) is not None

    def test_disabilitato_globale(self, nf_db):
        import news_filter as nf
        from database import RiskSettings
        db = nf_db()
        try:
            rs = RiskSettings(news_filter_enabled=False)
            db.add(rs); db.commit()
        finally:
            db.close()
        ev_t = datetime(2026, 8, 12, 12, 30)
        _add_event(nf_db, "US CPI", ev_t)
        assert nf.entry_blocked(ev_t - timedelta(minutes=5)) is None


class TestFlattenWindow:
    def test_flatten_due_a_meno_5(self, nf_db):
        import news_filter as nf
        ev_t = datetime(2026, 8, 12, 12, 30)
        _add_event(nf_db, "US CPI", ev_t, flatten=True)
        assert nf.flatten_due(ev_t - timedelta(minutes=4)) is not None
        assert nf.flatten_due(ev_t - timedelta(minutes=6)) is None

    def test_flatten_false_non_scatta(self, nf_db):
        import news_filter as nf
        ev_t = datetime(2026, 8, 12, 12, 30)
        _add_event(nf_db, "Minor news", ev_t, flatten=False)
        assert nf.flatten_due(ev_t - timedelta(minutes=3)) is None

    def test_flatten_done_idempotente(self, nf_db):
        import news_filter as nf
        ev_t = datetime(2026, 8, 12, 12, 30)
        eid = _add_event(nf_db, "US CPI", ev_t, flatten=True)
        nf.mark_flatten_done(eid)
        assert nf.flatten_due(ev_t - timedelta(minutes=3)) is None


class TestFriday:
    def test_venerdi_sera_due(self, nf_db):
        import news_filter as nf
        # 2026-08-14 e' un venerdi'. 22:50 Roma estate = 20:50 UTC
        assert nf.friday_flatten_due(datetime(2026, 8, 14, 20, 50)) is True

    def test_venerdi_prima_orario_no(self, nf_db):
        import news_filter as nf
        # 20:00 Roma = 18:00 UTC → prima delle 22:45
        assert nf.friday_flatten_due(datetime(2026, 8, 14, 18, 0)) is False

    def test_giovedi_no(self, nf_db):
        import news_filter as nf
        assert nf.friday_flatten_due(datetime(2026, 8, 13, 20, 50)) is False


class TestSeed:
    def test_seed_idempotente(self, nf_db):
        import news_filter as nf
        n1 = nf.seed_default_events()
        n2 = nf.seed_default_events()
        assert n1 > 0
        assert n2 == 0
