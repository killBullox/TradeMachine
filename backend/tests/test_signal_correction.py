"""Correzione di un segnale (caso #687, 24/08, costo ~1.2k$):
il trader riposta lo stesso segnale con lo stop corretto e poi cancella il
messaggio sbagliato. Prima: repost scartato come "duplicato identico" e trade
chiuso alla cancellazione. Ora: correzione applicata, trade mantenuto."""
import json
import types
import pytest
from datetime import datetime, timedelta


def _pos(ticket, symbol, type, volume, price_open):
    """Posizione MT5 finta (il FakeMT5 di conftest non e' importabile qui)."""
    return types.SimpleNamespace(ticket=ticket, symbol=symbol, type=type,
                                 volume=volume, price_open=price_open,
                                 sl=0.0, tp=0.0, profit=0.0)


# Testo nel formato REALE del canale: il parser richiede questa struttura.
SIGNAL_TPL = ("\U0001F947 **#{sym}**** | {side} Near {e1}-{e2}**\n\n"
              "✅**Target 1 : {tp1} | Target 2 : {tp2} | Target 3 : {tp3}**\n\n"
              "**❕Stoploss : {sl}**")


def _parsed(sl=4647.0, tp1=4639.0, tp2=4634.0, tp3=4630.0, entry=4643.0,
            symbol="XAUUSD", direction="sell"):
    return types.SimpleNamespace(symbol=symbol, direction=direction,
                                 entry_price=entry, entry_price_high=entry + 1,
                                 stoploss=sl, tp1=tp1, tp2=tp2, tp3=tp3)


def _cand(db=None, sl=4647.0, created=None, **kw):
    from database import Signal
    base = dict(telegram_msg_id=9807, symbol="XAUUSD", direction="sell",
                entry_price=4643.0, entry_price_high=4644.0,
                actual_entry_price=4642.44, stoploss=sl,
                tp1=4639.0, tp2=4634.0, tp3=4630.0, status="open",
                is_filtered=False, is_archived=False, mt5_tickets="[1, 2, 3]",
                raw_message="t", position_size=2.49,
                created_at=created or datetime(2026, 8, 24, 7, 18, 17))
    base.update(kw)
    s = Signal(**base)
    if db is not None:
        db.add(s); db.commit(); db.refresh(s)
    return s


class TestClassifyRepost:
    def test_sl_diverso_di_due_dollari_e_correzione(self):
        """Il bug: 4647 -> 4649.50 su oro sta dentro lo 0.3% (14$) e passava
        per duplicato identico."""
        import telegram_client as tc
        now = datetime(2026, 8, 24, 7, 18, 43)
        assert tc.classify_repost(_parsed(sl=4649.50), _cand(), now=now) == "correction"

    def test_livelli_identici_e_duplicato(self):
        import telegram_client as tc
        now = datetime(2026, 8, 24, 7, 18, 43)
        assert tc.classify_repost(_parsed(sl=4647.0), _cand(), now=now) == "duplicate"

    def test_target_cambiato_e_correzione(self):
        import telegram_client as tc
        now = datetime(2026, 8, 24, 7, 18, 43)
        assert tc.classify_repost(_parsed(tp3=4620.0), _cand(), now=now) == "correction"

    def test_idea_diversa_e_different(self):
        import telegram_client as tc
        now = datetime(2026, 8, 24, 7, 18, 43)
        assert tc.classify_repost(_parsed(entry=4700.0), _cand(), now=now) == "different"

    def test_fuori_finestra_non_e_correzione(self):
        """Un repost con SL diverso 2 ore dopo e' un segnale NUOVO."""
        import telegram_client as tc
        now = datetime(2026, 8, 24, 9, 30, 0)
        assert tc.classify_repost(_parsed(sl=4649.50), _cand(), now=now) == "different"

    def test_fuori_finestra_livelli_uguali_resta_duplicato(self):
        import telegram_client as tc
        now = datetime(2026, 8, 24, 9, 30, 0)
        assert tc.classify_repost(_parsed(sl=4647.0), _cand(), now=now) == "duplicate"


class TestVolumePerStopCorretto:
    def test_stop_allargato_riduce_il_volume(self):
        import telegram_client as tc
        # entry 4642.44, stop 4647 (4.56) -> 4649.50 (7.06)
        v = tc.volume_for_corrected_sl(4642.44, 4647.0, 4649.50, 0.83)
        assert v == round(0.83 * 4.56 / 7.06, 2)
        assert v < 0.83

    def test_stop_stretto_lascia_il_volume(self):
        import telegram_client as tc
        assert tc.volume_for_corrected_sl(4642.44, 4647.0, 4645.0, 0.83) == 0.83

    def test_dati_invalidi(self):
        import telegram_client as tc
        assert tc.volume_for_corrected_sl(4642.44, 4642.44, 4649.5, 0.83) is None
        assert tc.volume_for_corrected_sl(None, 4647.0, 4649.5, 0.83) is None


class TestApplicaCorrezione:
    def test_stop_stretto_applicato_senza_ridurre(self, in_memory_db, fake_mt5, monkeypatch):
        import telegram_client as tc
        import mt5_trader
        db = in_memory_db()
        try:
            c = _cand(db)
            monkeypatch.setattr(mt5_trader, "is_enabled", lambda: True)
            monkeypatch.setattr(mt5_trader, "_get_mt5", lambda: fake_mt5)
            monkeypatch.setattr(mt5_trader, "modify_sl", lambda *a, **k: True)
            called = []
            monkeypatch.setattr(mt5_trader, "reduce_position",
                                lambda *a, **k: called.append(a) or True)
            ok = tc._apply_signal_correction(db, c, _parsed(sl=4645.0), 9808)
            assert ok is True
            assert c.stoploss == 4645.0
            assert called == []      # stop che stringe: nessun resize
            assert any(e.get("event") == "signal_corrected"
                       for e in json.loads(c.trade_log or "[]"))
        finally:
            db.close()

    def test_stop_allargato_riduce_la_size(self, in_memory_db, fake_mt5, monkeypatch):
        import telegram_client as tc
        import mt5_trader
        _Position = _pos
        db = in_memory_db()
        try:
            c = _cand(db)
            for t in (1, 2, 3):
                fake_mt5.positions[t] = _pos(t, "XAUUSD", 1, 0.83, 4642.44)
            monkeypatch.setattr(mt5_trader, "is_enabled", lambda: True)
            monkeypatch.setattr(mt5_trader, "_get_mt5", lambda: fake_mt5)
            monkeypatch.setattr(mt5_trader, "modify_sl", lambda *a, **k: True)
            reduced = []
            monkeypatch.setattr(mt5_trader, "reduce_position",
                                lambda tk, sym, vol: reduced.append((tk, vol)) or True)
            ok = tc._apply_signal_correction(db, c, _parsed(sl=4649.50), 9808)
            assert ok is True
            assert c.stoploss == 4649.50
            assert len(reduced) == 3                   # tutti e 3 i ticket
            assert all(v < 0.83 for _, v in reduced)   # size ridotta davvero
        finally:
            db.close()

    def test_resize_fallito_mantiene_lo_stop_vecchio(self, in_memory_db, fake_mt5, monkeypatch):
        """Invariante: mai allargare il rischio oltre il massimo d'ingresso."""
        import telegram_client as tc
        import mt5_trader
        _Position = _pos
        db = in_memory_db()
        try:
            c = _cand(db)
            fake_mt5.positions[1] = _pos(1, "XAUUSD", 1, 0.83, 4642.44)
            monkeypatch.setattr(mt5_trader, "is_enabled", lambda: True)
            monkeypatch.setattr(mt5_trader, "_get_mt5", lambda: fake_mt5)
            monkeypatch.setattr(mt5_trader, "reduce_position", lambda *a, **k: False)
            ok = tc._apply_signal_correction(db, c, _parsed(sl=4649.50), 9808)
            assert ok is False
            assert c.stoploss == 4647.0     # stop invariato
            assert any(e.get("event") == "correction_rejected"
                       for e in json.loads(c.trade_log or "[]"))
        finally:
            db.close()


class TestCancellazioneConSostituto:
    def _raw(self, db, msg_id, text, when):
        from database import RawMessage
        db.add(RawMessage(telegram_msg_id=msg_id, text=text, msg_type="signal",
                          created_at=when))
        db.commit()

    def test_trova_il_messaggio_sostitutivo(self, in_memory_db, fake_mt5):
        import telegram_client as tc
        db = in_memory_db()
        try:
            c = _cand(db)
            self._raw(db, 9808,
                      SIGNAL_TPL.format(sym="XAUUSD", side="Sell", e1=4643, e2=44,
                                        tp1=4639, tp2=4634, tp3=4630, sl=4649.50),
                      datetime(2026, 8, 24, 7, 18, 43))
            assert tc._find_replacement_message(db, c, 9807) == 9808
        finally:
            db.close()

    def test_nessun_sostituto_se_simbolo_diverso(self, in_memory_db, fake_mt5):
        import telegram_client as tc
        db = in_memory_db()
        try:
            c = _cand(db)
            self._raw(db, 9808,
                      SIGNAL_TPL.format(sym="GBPJPY", side="Buy", e1="217.100",
                                        e2="120", tp1="217.250", tp2="217.400",
                                        tp3="217.550", sl="216.950"),
                      datetime(2026, 8, 24, 7, 18, 43))
            assert tc._find_replacement_message(db, c, 9807) is None
        finally:
            db.close()

    def test_nessun_sostituto_se_precedente(self, in_memory_db, fake_mt5):
        """Un messaggio con id MINORE non e' un sostituto."""
        import telegram_client as tc
        db = in_memory_db()
        try:
            c = _cand(db)
            self._raw(db, 9700,
                      SIGNAL_TPL.format(sym="XAUUSD", side="Sell", e1=4643, e2=44,
                                        tp1=4639, tp2=4634, tp3=4630, sl=4649.50),
                      datetime(2026, 8, 24, 7, 18, 43))
            assert tc._find_replacement_message(db, c, 9807) is None
        finally:
            db.close()
