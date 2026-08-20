"""ICT top-down: gate di copertura (mai classificare su finestre parziali,
caso #682) + bias/dealing range H1-H4 + regole exclude_counter_h4/setup_h4."""
import json
import pytest
from datetime import datetime, timedelta


def _candles(n, start, minutes, base=4000.0, step=1.0):
    return [{"t": start + timedelta(minutes=i * minutes),
             "o": base + i * step, "h": base + i * step + 2,
             "l": base + i * step - 2, "c": base + i * step}
            for i in range(n)]


def _zigzag(n, start, minutes, base=3500.0, leg=30.0, drift=20.0):
    """Serie con struttura rialzista VERA (massimi e minimi crescenti): una
    serie monotona non ha frattali, quindi structure_bias direbbe 'neutral'."""
    out = []
    for i in range(n):
        # onda: 3 su, 2 giu', con drift complessivo positivo
        phase = i % 5
        wave = leg if phase < 3 else -leg * 0.6
        c = base + drift * i + wave
        out.append({"t": start + timedelta(minutes=i * minutes),
                    "o": c - 1, "h": c + 3, "l": c - 3, "c": c})
    return out


ENTRY = datetime(2026, 8, 10, 12, 0, 0)


class TestCoverageGate:
    def test_poche_barre_prima_entry_no_data(self):
        import ict_engine as ict
        m5 = _candles(30, ENTRY - timedelta(minutes=5 * 30), 5)
        assert ict.coverage_ok(m5, ENTRY, "M5") is False
        res = ict.classify_entry(m5, [], ENTRY, 4000.0, "buy")
        assert res["setup"] == "no_data" and res["candles_ok"] is False
        assert "copertura" in res.get("motivo", "")

    def test_serie_che_non_arriva_all_entry_no_data(self):
        """Buco finale: dati che si fermano ore prima dell'entry."""
        import ict_engine as ict
        m5 = _candles(200, ENTRY - timedelta(hours=20), 5)   # finisce ~3h prima
        assert ict.coverage_ok(m5, ENTRY, "M5") is False

    def test_copertura_sufficiente_classifica(self):
        import ict_engine as ict
        m5 = _candles(150, ENTRY - timedelta(minutes=5 * 149), 5)
        assert ict.coverage_ok(m5, ENTRY, "M5") is True
        res = ict.classify_entry(m5, [], ENTRY, 4149.0, "buy")
        assert res["candles_ok"] is True and res["setup"] in ict.SETUP_LABELS


class TestHTF:
    def _base_m5(self):
        return _candles(150, ENTRY - timedelta(minutes=5 * 149), 5)

    def test_htf_none_se_mancano(self):
        import ict_engine as ict
        res = ict.classify_entry(self._base_m5(), [], ENTRY, 4149.0, "buy")
        assert res["bias_h4"] is None and res["with_trend_h4"] is None
        assert res["pd_bucket_h1"] is None

    def test_bias_h4_rialzista_e_allineamento(self):
        import ict_engine as ict
        h1 = _zigzag(60, ENTRY - timedelta(hours=60), 60, base=3800.0, drift=5.0)
        h4 = _zigzag(40, ENTRY - timedelta(hours=4 * 40), 240, base=3500.0, drift=20.0)
        res = ict.classify_entry(self._base_m5(), [], ENTRY, 4149.0, "buy",
                                 candles_h1=h1, candles_h4=h4)
        assert res["bias_h4"] == "bullish"
        assert res["with_trend_h4"] is True          # buy col trend H4
        assert res["pd_h1"] is not None
        res_sell = ict.classify_entry(self._base_m5(), [], ENTRY, 4149.0, "sell",
                                      candles_h1=h1, candles_h4=h4)
        assert res_sell["with_trend_h4"] is False    # sell contro H4

    def test_h4_parziale_ignorato(self):
        import ict_engine as ict
        h4_corto = _candles(4, ENTRY - timedelta(hours=16), 240)
        res = ict.classify_entry(self._base_m5(), [], ENTRY, 4149.0, "buy",
                                 candles_h4=h4_corto)
        assert res["bias_h4"] is None                # sotto MIN_BARS_BEFORE


def _mk(db, msg_id, pnl, created=None):
    from database import Signal
    now = created or datetime(2026, 8, 10, 8, 0, 0)
    s = Signal(telegram_msg_id=msg_id, symbol="XAUUSD", direction="buy",
               entry_price=4000.0, entry_price_high=4001.0,
               actual_entry_price=4000.5, stoploss=3994.0, tp1=4005.0,
               status="tp1" if pnl > 0 else "sl_hit", pnl_usd=pnl,
               risk_usd=1000.0, is_filtered=False, is_archived=False,
               mt5_tickets="[1]", raw_message="t", created_at=now,
               entered_at=now + timedelta(seconds=5),
               closed_at=now + timedelta(minutes=30))
    db.add(s); db.commit(); db.refresh(s)
    return s


def _ctx(db, sid, setup, with_trend_h4):
    from database import TradeContext
    db.add(TradeContext(signal_id=sid, setup=setup, candles_ok=True,
                        features_json=json.dumps({"with_trend_h4": with_trend_h4})))
    db.commit()


class TestRegoleH4:
    def test_exclude_counter_h4(self, in_memory_db, fake_mt5):
        import sim_engine
        db = in_memory_db()
        try:
            a = _mk(db, 1, -300.0); _ctx(db, a.id, "sweep_reversal", False)  # contro H4
            b = _mk(db, 2, 200.0);  _ctx(db, b.id, "sweep_reversal", True)   # con H4
            c = _mk(db, 3, 100.0)   # nessun contesto -> tenuto
            res = sim_engine.simulate("exclude_counter_h4", {}, db)
            assert res["ok"] and res["trades_excluded"] == 1
            assert res["delta_pnl"] == 300.0
        finally:
            db.close()

    def test_exclude_setup_h4_piu_chirurgica_di_exclude_setup(self, in_memory_db, fake_mt5):
        """Il setup perde solo contro H4: la regola mirata evita i perdenti
        SENZA buttare via i vincenti dello stesso setup."""
        import sim_engine
        db = in_memory_db()
        try:
            for i in range(3):
                s = _mk(db, 10 + i, -200.0); _ctx(db, s.id, "sweep_reversal", False)
            for i in range(3):
                s = _mk(db, 20 + i, 150.0); _ctx(db, s.id, "sweep_reversal", True)
            tutto = sim_engine.simulate("exclude_setup", {"setup": "sweep_reversal"}, db)
            mirato = sim_engine.simulate("exclude_setup_h4", {"setup": "sweep_reversal"}, db)
            assert tutto["trades_excluded"] == 6 and tutto["delta_pnl"] == 150.0
            assert mirato["trades_excluded"] == 3 and mirato["delta_pnl"] == 600.0
        finally:
            db.close()

    def test_sweep_include_i_candidati_h4(self, in_memory_db, fake_mt5):
        import sim_engine
        db = in_memory_db()
        try:
            for i in range(12):
                s = _mk(db, 100 + i, -200.0,
                        created=datetime(2026, 8, 1, 8, 0) + timedelta(hours=i))
                _ctx(db, s.id, "sweep_reversal", False)
            for i in range(4):
                s = _mk(db, 200 + i, 150.0,
                        created=datetime(2026, 8, 2, 8, 0) + timedelta(hours=i))
                _ctx(db, s.id, "sweep_reversal", True)
            sw = sim_engine.sweep(db)
            tipi = {e["sim_type"] for e in sw["promosse"]}
            assert "exclude_counter_h4" in tipi or "exclude_setup_h4" in tipi
        finally:
            db.close()
