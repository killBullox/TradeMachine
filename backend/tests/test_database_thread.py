"""Accesso al database da piu' thread insieme (17/09 pomeriggio).

Il backend usava UNA connessione SQLite condivisa da tutti i thread
(StaticPool). Spostato il monitor prezzi in un thread di lavoro, due thread la
usavano insieme: "cannot commit - no transaction is active" alle 14:42, "bad
parameter or other API misuse" alle 15:19, poi backend piantato e VPS
riavviata. Riprodotto in laboratorio: 4 thread, 312 errori e il 70% delle
scritture perse."""
import threading
import pytest
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool, NullPool


def _martella(engine, n_thread=4, n_giri=150):
    import database as db_mod
    db_mod.Base.metadata.create_all(bind=engine)
    S = sessionmaker(autocommit=False, autoflush=False, bind=engine)
    errori = []

    def lavora(k):
        for i in range(n_giri):
            s = S()
            try:
                s.add(db_mod.RawMessage(telegram_msg_id=k * 100000 + i, text=f"{k}-{i}"))
                s.commit()
                s.query(db_mod.RawMessage).filter(
                    db_mod.RawMessage.text.like(f"{k}-%")).count()
            except Exception as e:
                errori.append(type(e).__name__)
            finally:
                try:
                    s.close()
                except Exception as e:
                    errori.append(type(e).__name__)
    ts = [threading.Thread(target=lavora, args=(k,)) for k in range(n_thread)]
    [t.start() for t in ts]
    [t.join() for t in ts]
    s = S()
    try:
        righe = s.query(db_mod.RawMessage).count()
    finally:
        s.close()
    return righe, errori


class TestMotoreDelDatabase:
    def test_thread_in_parallelo_nessun_errore_nessuna_perdita(self, tmp_path):
        import database as db_mod
        eng = db_mod.crea_engine(f"sqlite:///{tmp_path / 'multi.db'}")
        try:
            righe, errori = _martella(eng)
        finally:
            eng.dispose()
        assert errori == []
        assert righe == 4 * 150

    def test_una_connessione_per_sessione(self, tmp_path):
        import database as db_mod
        eng = db_mod.crea_engine(f"sqlite:///{tmp_path / 'p.db'}")
        try:
            assert isinstance(eng.pool, NullPool)
            with eng.connect() as c:
                assert c.exec_driver_sql("PRAGMA journal_mode").scalar().lower() == "wal"
                assert c.exec_driver_sql("PRAGMA busy_timeout").scalar() == 30000
        finally:
            eng.dispose()

    def test_in_memoria_resta_condiviso(self):
        """Un DB in memoria esiste solo nella sua connessione."""
        import database as db_mod
        eng = db_mod.crea_engine("sqlite:///:memory:")
        try:
            assert isinstance(eng.pool, StaticPool)
        finally:
            eng.dispose()

    def test_il_backend_usa_il_motore_corretto(self):
        import database as db_mod
        assert not isinstance(db_mod.engine.pool, StaticPool) or \
            ":memory:" in str(db_mod.engine.url)
