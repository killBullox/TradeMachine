"""Chiusura sugli annunci di target del trader (#734, 09/09) e pulsante
"chiudi prossimo ticket".

Nel #734 il trader ha scritto TP2 4429 (typo) e ha dichiarato il 2o target
raggiunto a 4418: il nostro ticket restava appeso a un livello che lui non
considerava piu' un obiettivo. Opzione OPT-IN (default OFF)."""
import json
import types
import pytest
from datetime import datetime


def _pos(ticket, tp, volume=0.35, price_open=4408.8, tipo=0):
    return types.SimpleNamespace(ticket=ticket, symbol="XAUUSD", type=tipo,
                                 volume=volume, price_open=price_open,
                                 sl=0.0, tp=tp, profit=0.0)


def _sig(db, **kw):
    from database import Signal
    now = datetime(2026, 9, 9, 13, 7, 23)
    base = dict(telegram_msg_id=10383, symbol="XAUUSD", direction="buy",
                entry_price=4407.0, entry_price_high=4408.0,
                actual_entry_price=4408.81, stoploss=4408.91,
                tp1=4414.0, tp2=4429.0, tp3=4427.0, status="tp1",
                is_filtered=False, is_archived=False,
                mt5_tickets="[182735746, 182735750, 182735753]",
                raw_message="t", created_at=now, entered_at=now)
    base.update(kw)
    s = Signal(**base)
    db.add(s); db.commit(); db.refresh(s)
    return s


class TestProssimoTicket:
    def test_ordina_per_target_piu_vicino(self, in_memory_db, fake_mt5, monkeypatch):
        """Coi target fuori ordine (TP2 4429, TP3 4427) il 'prossimo' e' il
        ticket del TP3, non quello del TP2."""
        import mt5_trader
        db = in_memory_db()
        try:
            s = _sig(db)
            fake_mt5.positions[182735750] = _pos(182735750, 4429.0)
            fake_mt5.positions[182735753] = _pos(182735753, 4427.0)
            fake_mt5.ticks["XAUUSD"] = types.SimpleNamespace(bid=4419.0, ask=4419.3, time=0, last=0)
            monkeypatch.setattr(mt5_trader, "_get_mt5", lambda: fake_mt5)
            monkeypatch.setattr(mt5_trader, "get_mt5_symbol", lambda x, default=None: "XAUUSD")
            aperti = mt5_trader.open_tickets_with_levels(s)
            assert [t for t, _, _ in aperti] == [182735753, 182735750]
            assert aperti[0][1] == 3 and aperti[0][2] == 4427.0   # livello TP3
        finally:
            db.close()

    def test_ignora_i_ticket_gia_chiusi(self, in_memory_db, fake_mt5, monkeypatch):
        import mt5_trader
        db = in_memory_db()
        try:
            s = _sig(db)
            fake_mt5.positions[182735750] = _pos(182735750, 4429.0)
            fake_mt5.ticks["XAUUSD"] = types.SimpleNamespace(bid=4419.0, ask=4419.3, time=0, last=0)
            monkeypatch.setattr(mt5_trader, "_get_mt5", lambda: fake_mt5)
            monkeypatch.setattr(mt5_trader, "get_mt5_symbol", lambda x, default=None: "XAUUSD")
            aperti = mt5_trader.open_tickets_with_levels(s)
            assert len(aperti) == 1 and aperti[0][0] == 182735750
        finally:
            db.close()


class TestChiusuraPerLivello:
    def _prepara(self, db, fake_mt5, monkeypatch):
        import mt5_trader
        s = _sig(db)
        fake_mt5.positions[182735750] = _pos(182735750, 4429.0)
        fake_mt5.positions[182735753] = _pos(182735753, 4427.0)
        fake_mt5.ticks["XAUUSD"] = types.SimpleNamespace(bid=4419.0, ask=4419.3, time=0, last=0)
        monkeypatch.setattr(mt5_trader, "_get_mt5", lambda: fake_mt5)
        monkeypatch.setattr(mt5_trader, "get_mt5_symbol", lambda x, default=None: "XAUUSD")
        chiusi = []
        monkeypatch.setattr(mt5_trader, "close_position",
                            lambda t, sym: chiusi.append(t) or True)
        return s, chiusi

    def test_chiude_solo_il_livello_dichiarato(self, in_memory_db, fake_mt5, monkeypatch):
        """'2nd Target Done' -> chiude il ticket TP2, non gli altri."""
        import mt5_trader
        db = in_memory_db()
        try:
            s, chiusi = self._prepara(db, fake_mt5, monkeypatch)
            r = mt5_trader.close_ticket_for_level(s, 2)
            assert r["chiusi"] == 1 and chiusi == [182735750]
        finally:
            db.close()

    def test_tutti_i_target(self, in_memory_db, fake_mt5, monkeypatch):
        import mt5_trader
        db = in_memory_db()
        try:
            s, chiusi = self._prepara(db, fake_mt5, monkeypatch)
            r = mt5_trader.close_ticket_for_level(s, 99)
            assert r["chiusi"] == 2 and set(chiusi) == {182735750, 182735753}
        finally:
            db.close()

    def test_livello_gia_chiuso_non_fa_nulla(self, in_memory_db, fake_mt5, monkeypatch):
        import mt5_trader
        db = in_memory_db()
        try:
            s, chiusi = self._prepara(db, fake_mt5, monkeypatch)
            r = mt5_trader.close_ticket_for_level(s, 1)     # TP1 gia' chiuso
            assert r["chiusi"] == 0 and chiusi == []
            assert "non e' piu' aperto" in r["motivo"]
        finally:
            db.close()

    def test_nessun_ticket_aperto(self, in_memory_db, fake_mt5, monkeypatch):
        import mt5_trader
        db = in_memory_db()
        try:
            s = _sig(db)
            monkeypatch.setattr(mt5_trader, "_get_mt5", lambda: fake_mt5)
            r = mt5_trader.close_ticket_for_level(s, 2)
            assert r["chiusi"] == 0 and r["ok"] is False
        finally:
            db.close()


class TestInterruttore:
    def test_default_spento(self, in_memory_db, fake_mt5):
        """Cambia il comportamento operativo: deve essere opt-in."""
        from database import RiskSettings
        db = in_memory_db()
        try:
            rs = RiskSettings()
            db.add(rs); db.commit(); db.refresh(rs)
            assert bool(rs.close_on_target_done_enabled) is False
        finally:
            db.close()

    def test_esposto_dalle_impostazioni(self):
        from pathlib import Path
        main_src = (Path(__file__).resolve().parent.parent / "main.py").read_text(
            encoding="utf-8", errors="replace")
        assert "close_on_target_done_enabled: bool = False" in main_src
        assert '"close_on_target_done_enabled":' in main_src
        assert "rs.close_on_target_done_enabled = body.close_on_target_done_enabled" in main_src

    def test_aggancio_dietro_interruttore(self):
        """La chiusura su annuncio deve stare dentro il check dell'opzione."""
        from pathlib import Path
        tg = (Path(__file__).resolve().parent.parent / "telegram_client.py").read_text(
            encoding="utf-8", errors="replace")
        assert "close_on_target_done_enabled" in tg
        assert "close_ticket_for_level" in tg
        i_flag = tg.index("close_on_target_done_enabled")
        i_call = tg.index("close_ticket_for_level")
        assert i_flag < i_call, "la chiamata deve venire DOPO il controllo dell'opzione"
