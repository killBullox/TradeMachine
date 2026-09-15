"""Gestione dei trade (15/09): rischio scelto sul trade manuale PRIMA di
aprirlo, modifica di stop e target sui ticket aperti, chiusura di un lotto
alla volta.

Meccanismo unico per reale e paper: tutto si riduce a una chiusura parziale a
mercato. Sui paper il consumo di lotti va scritto nel trade_log, perche'
recalculate_signal ricalcola position_size dallo stop a ogni giro e
sovrascriverebbe qualunque modifica diretta alla size."""
import json
import types
import pytest
from datetime import datetime


def _paper(db, **kw):
    from database import Signal
    now = datetime(2026, 9, 15, 9, 0, 0)
    base = dict(symbol="XAUUSD", direction="buy",
                entry_price=4290.0, entry_price_high=4290.0,
                actual_entry_price=4290.0, stoploss=4280.0,
                tp1=4300.0, tp2=4310.0, tp3=4320.0,
                status="open", is_filtered=True, is_archived=False,
                position_size=0.30, risk_usd=300.0,
                raw_message="[MANUALE PAPER] test", created_at=now, entered_at=now,
                trade_log=json.dumps([{"ts": now.isoformat() + "Z",
                                       "event": "entry", "price": 4290.0}]))
    base.update(kw)
    s = Signal(**base)
    db.add(s); db.commit(); db.refresh(s)
    return s


class TestPnlConChiusureParziali:
    def test_una_chiusura_parziale_riduce_il_residuo(self, in_memory_db, fake_mt5):
        """Il residuo per lo stop deve scendere dei lotti gia' incassati."""
        import risk
        db = in_memory_db()
        try:
            s = _paper(db)
            eventi = json.loads(s.trade_log)
            eventi.append({"ts": "2026-09-15T10:00:00Z", "event": "partial_close",
                           "price": 4295.0, "lots": 0.10})
            eventi.append({"ts": "2026-09-15T11:00:00Z", "event": "sl_hit",
                           "price": 4280.0})
            s.trade_log = json.dumps(eventi)
            pnl, _ = risk._calc_pnl_from_trade_log(s, 0.30, 4290.0)
            # +0.10 lotti da 4290 a 4295, poi 0.20 residui da 4290 a 4280
            atteso = (risk.calc_pnl("XAUUSD", "buy", 4290.0, 4295.0, 0.10)
                      + risk.calc_pnl("XAUUSD", "buy", 4290.0, 4280.0, 0.20))
            assert pnl == round(atteso, 2)
        finally:
            db.close()

    def test_senza_chiusure_parziali_nulla_cambia(self, in_memory_db, fake_mt5):
        """Garanzia di non regressione sui trade gia' in corso."""
        import risk
        db = in_memory_db()
        try:
            s = _paper(db)
            eventi = json.loads(s.trade_log)
            eventi.append({"ts": "2026-09-15T10:00:00Z", "event": "tp1", "price": 4300.0})
            eventi.append({"ts": "2026-09-15T11:00:00Z", "event": "sl_hit", "price": 4290.0})
            s.trade_log = json.dumps(eventi)
            pnl, _ = risk._calc_pnl_from_trade_log(s, 0.30, 4290.0)
            atteso = risk.calc_pnl("XAUUSD", "buy", 4290.0, 4300.0, 0.10)
            assert pnl == round(atteso, 2)
        finally:
            db.close()


class TestStatoPosizione:
    def test_paper_conta_tp_e_chiusure(self, in_memory_db, fake_mt5, monkeypatch):
        import mt5_trader
        import price_service as ps
        db = in_memory_db()
        try:
            s = _paper(db)
            eventi = json.loads(s.trade_log)
            eventi.append({"ts": "x", "event": "tp1", "price": 4300.0})
            eventi.append({"ts": "y", "event": "partial_close", "price": 4305.0, "lots": 0.05})
            s.trade_log = json.dumps(eventi); db.commit()
            monkeypatch.setattr(ps, "get_current_price", lambda sym: 4305.0)
            st = mt5_trader.stato_posizione(s)
            assert st["paper"] is True
            assert st["lotto_unitario"] == 0.10
            assert st["lotti_residui"] == 0.15      # 0.30 - 0.10 (tp1) - 0.05
            assert st["rischio_corrente"] == pytest.approx(150.0, abs=1.0)
        finally:
            db.close()


@pytest.fixture
def client(in_memory_db):
    from fastapi.testclient import TestClient
    import main
    from database import SessionLocal
    main.app.dependency_overrides[main.get_db] = lambda: SessionLocal()
    yield TestClient(main.app)
    main.app.dependency_overrides.clear()


class TestModificaLivelli:
    def test_stop_dal_lato_sbagliato_rifiutato(self, in_memory_db, fake_mt5,
                                               client, monkeypatch):
        import price_service as ps
        db = in_memory_db()
        try:
            s = _paper(db)
        finally:
            db.close()
        monkeypatch.setattr(ps, "get_current_price", lambda sym: 4295.0)
        r = client.post(f"/api/trades/{s.id}/modifica-livelli",
                        json={"stoploss": 4299.0}).json()
        assert r["ok"] is False and "sotto il prezzo" in r["error"]

    def test_stop_che_stringe_va_bene(self, in_memory_db, fake_mt5, client, monkeypatch):
        import price_service as ps
        db = in_memory_db()
        try:
            s = _paper(db)
        finally:
            db.close()
        monkeypatch.setattr(ps, "get_current_price", lambda sym: 4295.0)
        r = client.post(f"/api/trades/{s.id}/modifica-livelli",
                        json={"stoploss": 4288.0, "tp3": 4330.0}).json()
        assert r["ok"] is True
        assert r["stoploss"] == 4288.0 and r["tp3"] == 4330.0

    def test_stop_che_allarga_oltre_il_massimo_rifiutato(self, in_memory_db, fake_mt5,
                                                         client, monkeypatch):
        """Invariante: il rischio non supera mai il massimo definito."""
        import price_service as ps
        import risk
        db = in_memory_db()
        try:
            s = _paper(db, position_size=3.0)     # size grande: allargare sfora
        finally:
            db.close()
        monkeypatch.setattr(ps, "get_current_price", lambda sym: 4295.0)
        monkeypatch.setattr(risk, "calc_risk_amount", lambda *a, **k: 1000.0)
        r = client.post(f"/api/trades/{s.id}/modifica-livelli",
                        json={"stoploss": 4200.0}).json()
        assert r["ok"] is False and "sopra il massimo" in r["error"]


class TestChiudiProssimoLotto:
    def test_paper_chiude_un_lotto(self, in_memory_db, fake_mt5, client, monkeypatch):
        import price_service as ps
        db = in_memory_db()
        try:
            s = _paper(db)
        finally:
            db.close()
        monkeypatch.setattr(ps, "get_current_price", lambda sym: 4296.0)
        r = client.post(f"/api/trades/{s.id}/chiudi-prossimo-lotto").json()
        assert r["ok"] is True and r["paper"] is True
        assert r["lotti_chiusi"] == 0.10 and r["lotti_residui"] == 0.20

    def test_ultimo_lotto_chiude_il_trade(self, in_memory_db, fake_mt5, client, monkeypatch):
        import price_service as ps
        db = in_memory_db()
        try:
            s = _paper(db, position_size=0.10, tp2=None, tp3=None)
        finally:
            db.close()
        monkeypatch.setattr(ps, "get_current_price", lambda sym: 4296.0)
        r = client.post(f"/api/trades/{s.id}/chiudi-prossimo-lotto").json()
        assert r["ok"] is True and r["lotti_residui"] == 0.0
        from database import SessionLocal, Signal
        db2 = SessionLocal()
        try:
            assert db2.query(Signal).filter(Signal.id == s.id).first().status == "closed"
        finally:
            db2.close()


class TestRischioSulTradeManuale:
    """Il rischio si sceglie PRIMA di aprire, ed e' quello che determina i
    lotti. Solo in diminuzione rispetto al massimo delle Impostazioni."""

    def _body(self, **kw):
        import types
        base = dict(symbol="XAUUSD", direction="buy", stoploss=4280.0,
                    tp1=4300.0, tp2=None, tp3=None, paper=True, rischio_usd=None)
        base.update(kw)
        return types.SimpleNamespace(**base)

    def test_rischio_ridotto_da_meno_lotti(self, in_memory_db, fake_mt5, monkeypatch):
        import main, risk
        import price_service as ps
        db = in_memory_db()
        try:
            monkeypatch.setattr(ps, "get_current_price", lambda sym: 4290.0)
            monkeypatch.setattr(risk, "calc_risk_amount", lambda *a, **k: 1000.0)
            pieno = main._manual_trade_preview(self._body(), db)
            meta = main._manual_trade_preview(self._body(rischio_usd=500.0), db)
            assert pieno["ok"] and meta["ok"]
            assert meta["rischio_massimo"] == 500.0
            assert meta["rischio_configurato"] == 1000.0
            assert meta["lotti_totali"] < pieno["lotti_totali"]
        finally:
            db.close()

    def test_non_si_puo_aumentare(self, in_memory_db, fake_mt5, monkeypatch):
        import main, risk
        import price_service as ps
        db = in_memory_db()
        try:
            monkeypatch.setattr(ps, "get_current_price", lambda sym: 4290.0)
            monkeypatch.setattr(risk, "calc_risk_amount", lambda *a, **k: 1000.0)
            r = main._manual_trade_preview(self._body(rischio_usd=1500.0), db)
            assert r["ok"] is False
            assert any("solo ridurre" in e for e in r["errori"])
        finally:
            db.close()

    def test_rischio_non_positivo_rifiutato(self, in_memory_db, fake_mt5, monkeypatch):
        import main, risk
        import price_service as ps
        db = in_memory_db()
        try:
            monkeypatch.setattr(ps, "get_current_price", lambda sym: 4290.0)
            monkeypatch.setattr(risk, "calc_risk_amount", lambda *a, **k: 1000.0)
            r = main._manual_trade_preview(self._body(rischio_usd=0), db)
            assert r["ok"] is False
        finally:
            db.close()

    def test_place_orders_usa_il_rischio_del_segnale(self, in_memory_db, fake_mt5, monkeypatch):
        """Sul trade REALE il rischio ridotto deve arrivare fino al sizing."""
        from pathlib import Path
        src = Path(__file__).resolve().parent.parent / "mt5_trader.py"
        testo = src.read_text(encoding="utf-8", errors="replace")
        assert "rischio ridotto per questo trade" in testo
        i_calc = testo.index("risk_usd  = calc_risk_amount(settings)")
        i_scelto = testo.index('_scelto = getattr(sig, "risk_usd", None)')
        i_size = testo.index("lots_total_raw = calc_position_size")
        assert i_calc < i_scelto < i_size


class TestNessunDoppioDimezzamento:
    """Su un segnale Telegram "risky" il campo risk_usd puo' gia' valere meta'
    del massimo: rileggerlo come "scelta dell'utente" lo dimezzerebbe due
    volte. Il rischio per-trade vale SOLO per i trade manuali."""

    def test_solo_i_manuali_usano_il_rischio_del_segnale(self):
        from pathlib import Path
        testo = (Path(__file__).resolve().parent.parent / "mt5_trader.py").read_text(
            encoding="utf-8", errors="replace")
        assert '_manuale = str(getattr(sig, "raw_message", "") or "").startswith("[MANUALE")' in testo
        assert '_scelto = getattr(sig, "risk_usd", None) if _manuale else None' in testo
        # il controllo deve precedere il dimezzamento dei segnali risky
        i_scelto = testo.index("_scelto = getattr(sig")
        i_risky = testo.index("if getattr(sig, 'is_risky', False):\n        risk_usd *= 0.5")
        assert i_scelto < i_risky


class TestComandiSuiPaper:
    """Un trade manuale in paper non ha ticket MT5: prima i comandi della tile
    (lock profit, chiudi trade) non facevano nulla."""

    def test_lock_profit_sposta_lo_stop(self, in_memory_db, fake_mt5, client, monkeypatch):
        import price_service as ps
        db = in_memory_db()
        try:
            s = _paper(db)
        finally:
            db.close()
        monkeypatch.setattr(ps, "get_current_price", lambda sym: 4295.0)
        r = client.post(f"/api/mt5/lock-profit/{s.id}").json()
        assert r["ok"] is True and r["paper"] is True and r["rule"] == "BE+1pip"
        from database import SessionLocal, Signal
        db2 = SessionLocal()
        try:
            agg = db2.query(Signal).filter(Signal.id == s.id).first()
            assert agg.stoploss > 4290.0       # BE + 1 pip sopra l'ingresso
        finally:
            db2.close()

    def test_lock_profit_a_tp1_dopo_due_target(self, in_memory_db, fake_mt5,
                                               client, monkeypatch):
        import price_service as ps
        db = in_memory_db()
        try:
            s = _paper(db, status="tp2")
            eventi = json.loads(s.trade_log)
            eventi += [{"ts": "a", "event": "tp1", "price": 4300.0},
                       {"ts": "b", "event": "tp2", "price": 4310.0}]
            s.trade_log = json.dumps(eventi); db.commit()
            sid = s.id          # letto prima della chiusura della sessione
        finally:
            db.close()
        monkeypatch.setattr(ps, "get_current_price", lambda sym: 4312.0)
        r = client.post(f"/api/mt5/lock-profit/{sid}").json()
        assert r["ok"] is True and r["rule"] == "TP1" and r["new_sl"] == 4300.0

    def test_chiudi_trade_paper(self, in_memory_db, fake_mt5, client, monkeypatch):
        import price_service as ps
        db = in_memory_db()
        try:
            s = _paper(db)
        finally:
            db.close()
        monkeypatch.setattr(ps, "get_current_price", lambda sym: 4297.0)
        r = client.post(f"/api/mt5/close_signal/{s.id}").json()
        assert r["ok"] is True and r["paper"] is True and r["exit_price"] == 4297.0
        from database import SessionLocal, Signal
        db2 = SessionLocal()
        try:
            agg = db2.query(Signal).filter(Signal.id == s.id).first()
            assert agg.status == "closed" and agg.closed_at is not None
            assert agg.pnl_usd is not None and agg.pnl_usd > 0
        finally:
            db2.close()

    def test_la_tile_mostra_i_comandi_ai_paper(self):
        """Senza is_filtered nella condizione il blocco resta invisibile."""
        from pathlib import Path
        jsx = (Path(__file__).resolve().parents[2] / "frontend" / "src" /
               "components" / "TradeCard.jsx").read_text(encoding="utf-8", errors="replace")
        assert "{(tickets.length > 0 || sig.is_filtered || sig.status === 'pending') && (" in jsx
