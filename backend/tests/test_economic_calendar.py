"""Feed calendario economico: conversione UTC, filtro USD-High, upsert
idempotente, prune, resilienza rete. (post-mortem FOMC #622/#623)"""
import pytest
from datetime import datetime


def _set_feed(monkeypatch, events, ok=True):
    import economic_calendar as ec
    def fake_fetch():
        ec._last_source_ok = ok
        return events
    monkeypatch.setattr(ec, "fetch_feed", fake_fetch)


FOMC = {"title": "FOMC Statement", "country": "USD",
        "date": "2027-03-18T14:00:00-04:00", "impact": "High"}
CPI = {"title": "CPI m/m", "country": "USD",
       "date": "2027-03-11T08:30:00-04:00", "impact": "High"}
USD_MED = {"title": "Unemployment Claims", "country": "USD",
           "date": "2027-03-12T08:30:00-04:00", "impact": "Medium"}
EUR_HIGH = {"title": "ECB Rate", "country": "EUR",
            "date": "2027-03-13T08:15:00-04:00", "impact": "High"}


class TestToUtc:
    def test_fomc_offset_a_utc(self, fake_mt5):
        # IL bug reale: 14:00 ET (-04:00) = 18:00 UTC = 20:00 Roma.
        import economic_calendar as ec
        assert ec._to_utc("2026-07-29T14:00:00-04:00") == datetime(2026, 7, 29, 18, 0, 0)

    def test_z_suffix(self, fake_mt5):
        import economic_calendar as ec
        assert ec._to_utc("2026-07-29T18:00:00Z") == datetime(2026, 7, 29, 18, 0, 0)

    def test_none_su_stringa_vuota(self, fake_mt5):
        import economic_calendar as ec
        assert ec._to_utc("") is None
        assert ec._to_utc("garbage") is None


class TestRefreshEvents:
    def test_inserisce_solo_usd_high(self, in_memory_db, fake_mt5, monkeypatch):
        from database import NewsEvent
        _set_feed(monkeypatch, [FOMC, CPI, USD_MED, EUR_HIGH])
        import economic_calendar as ec
        db = in_memory_db()
        try:
            res = ec.refresh_events(db)
            evs = db.query(NewsEvent).all()
            names = {e.name for e in evs}
            assert names == {"FOMC Statement", "CPI m/m"}  # scartati Medium + EUR
            assert res["added"] == 2
            fomc = db.query(NewsEvent).filter(NewsEvent.name == "FOMC Statement").first()
            assert fomc.event_time == datetime(2027, 3, 18, 18, 0, 0)  # 14:00-04:00
            assert fomc.source == "forexfactory"
            assert fomc.flatten is True
        finally:
            db.close()

    def test_idempotente_nessun_duplicato(self, in_memory_db, fake_mt5, monkeypatch):
        from database import NewsEvent
        _set_feed(monkeypatch, [FOMC, CPI])
        import economic_calendar as ec
        db = in_memory_db()
        try:
            ec.refresh_events(db)
            ec.refresh_events(db)  # secondo giro
            assert db.query(NewsEvent).count() == 2
        finally:
            db.close()

    def test_update_orario_non_duplica(self, in_memory_db, fake_mt5, monkeypatch):
        from database import NewsEvent
        _set_feed(monkeypatch, [FOMC])
        import economic_calendar as ec
        db = in_memory_db()
        try:
            ec.refresh_events(db)
            # stesso evento ma stesso external_key (title|date) -> nessun cambiamento;
            # per simulare uno spostamento reale, il feed cambia la data.
            moved = dict(FOMC, date="2027-03-18T14:30:00-04:00")
            _set_feed(monkeypatch, [moved])
            ec.refresh_events(db)
            evs = db.query(NewsEvent).filter(NewsEvent.name == "FOMC Statement").all()
            # external_key diverso (data diversa) -> il vecchio (futuro) viene prunato,
            # resta solo quello nuovo alle 18:30.
            assert len(evs) == 1
            assert evs[0].event_time == datetime(2027, 3, 18, 18, 30, 0)
        finally:
            db.close()

    def test_manuali_mai_toccati(self, in_memory_db, fake_mt5, monkeypatch):
        from database import NewsEvent
        db = in_memory_db()
        try:
            man = NewsEvent(name="Manuale mio", event_time=datetime(2027, 3, 20, 12, 0),
                            currency="USD", impact="high", flatten=True, source="manual")
            db.add(man); db.commit()
            _set_feed(monkeypatch, [FOMC])
            import economic_calendar as ec
            ec.refresh_events(db)
            assert db.query(NewsEvent).filter(NewsEvent.source == "manual").count() == 1
            assert db.query(NewsEvent).filter(NewsEvent.name == "Manuale mio").first() is not None
        finally:
            db.close()

    def test_prune_evento_auto_futuro_sparito(self, in_memory_db, fake_mt5, monkeypatch):
        from database import NewsEvent
        import economic_calendar as ec
        db = in_memory_db()
        try:
            _set_feed(monkeypatch, [FOMC, CPI])
            ec.refresh_events(db)
            assert db.query(NewsEvent).count() == 2
            # il feed ora non contiene piu' CPI (cancellato dal calendario)
            _set_feed(monkeypatch, [FOMC])
            ec.refresh_events(db)
            names = {e.name for e in db.query(NewsEvent).all()}
            assert names == {"FOMC Statement"}  # CPI (futuro) prunato
        finally:
            db.close()

    def test_rete_ko_non_tocca_nulla(self, in_memory_db, fake_mt5, monkeypatch):
        from database import NewsEvent
        import economic_calendar as ec
        db = in_memory_db()
        try:
            _set_feed(monkeypatch, [FOMC])
            ec.refresh_events(db)
            assert db.query(NewsEvent).count() == 1
            # rete giu': fetch ritorna [] e source_ok False -> nessuna modifica/prune
            _set_feed(monkeypatch, [], ok=False)
            res = ec.refresh_events(db)
            assert res.get("source_ok") is False
            assert db.query(NewsEvent).count() == 1  # evento preesistente intatto
        finally:
            db.close()
