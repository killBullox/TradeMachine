"""Motore ICT: feature pure su candele sintetiche (swing, BOS/retest, FVG, OB,
sweep, premium/discount, kill zone), classificazione setup, regola
exclude_setup, statistiche strategia e resilienza senza MT5."""
import json
import pytest
from datetime import datetime, timedelta


def _c(t, o, h, l, c):
    return {"t": t, "o": o, "h": h, "l": l, "c": c}


def _series(base_time, ohlc_list):
    return [_c(base_time + timedelta(minutes=5 * i), *v) for i, v in enumerate(ohlc_list)]


T0 = datetime(2026, 8, 20, 6, 0, 0)  # UTC (08:00 Roma estate)


class TestFeaturePure:
    def test_swings(self, fake_mt5):
        import ict_engine as ict
        # picco a idx 3, minimo a idx 7
        data = [(10, 11, 9, 10), (10, 12, 9, 11), (11, 13, 10, 12), (12, 15, 11, 14),
                (13, 14, 12, 13), (12, 13, 11, 12), (11, 12, 10, 11), (10, 11, 8, 9),
                (10, 12, 9, 11), (11, 13, 10, 12)]
        sw = ict.find_swings(_series(T0, data))
        kinds = {(i, k) for i, k, _ in sw}
        assert (3, "H") in kinds
        assert (7, "L") in kinds

    def test_structure_bias(self, fake_mt5):
        import ict_engine as ict
        bull = [(0, "L", 10), (1, "H", 15), (2, "L", 12), (3, "H", 18)]
        bear = [(0, "H", 18), (1, "L", 12), (2, "H", 15), (3, "L", 9)]
        assert ict.structure_bias(bull) == "bullish"
        assert ict.structure_bias(bear) == "bearish"

    def test_fvg_bullish_e_mitigazione(self, fake_mt5):
        import ict_engine as ict
        # candela 2: gap col high della 0 (10.5 < low 11 della 2) -> FVG bullish
        data = [(10, 10.5, 9.8, 10.3), (10.3, 11.2, 10.2, 11.1), (11.15, 12, 11, 11.9),
                (11.9, 12.2, 11.5, 12.0), (12.0, 12.3, 11.8, 12.1)]
        fvgs = ict.find_fvgs(_series(T0, data), upto_idx=5)
        assert any(f["type"] == "bullish" and not f["mitigated"] for f in fvgs)
        f = next(f for f in fvgs if f["type"] == "bullish")
        assert f["bottom"] == 10.5 and f["top"] == 11.0 or f["top"] == 11.0

    def test_bos_e_retest(self, fake_mt5):
        import ict_engine as ict
        # swing high a ~12, poi rottura con chiusura sopra, poi retest del livello
        data = [(10, 11, 9.5, 10.5), (10.5, 11.5, 10, 11), (11, 12, 10.8, 11.8),
                (11.8, 11.9, 11, 11.2), (11.2, 11.4, 10.9, 11.1), (11.1, 11.6, 11, 11.5),
                (11.5, 12.6, 11.4, 12.5),   # BOS: close 12.5 > swing high 12
                (12.5, 12.6, 11.95, 12.1),  # retest del 12
                (12.1, 12.8, 12.05, 12.7), (12.7, 13, 12.5, 12.9)]
        candles = _series(T0, data)
        sw = ict.find_swings(candles)
        bos = ict.last_bos(candles, sw, entry_idx=10, direction="buy")
        assert bos is not None
        assert bos["retest"] is True

    def test_sweep_sell_side(self, fake_mt5):
        import ict_engine as ict
        # minimi ~9.5, candela che spazza a 9.2 ma chiude 9.8 -> sweep sell-side
        data = [(10, 10.5, 9.5, 10)] * 6 + [(10, 10.2, 9.2, 9.9)] + [(9.9, 10.5, 9.8, 10.4)] * 3
        candles = _series(T0, data)
        sweep = ict.detect_sweep(candles, entry_idx=len(candles) - 1, direction="buy")
        assert sweep is not None and sweep["side"] == "sell_side"

    def test_premium_discount_e_killzone(self, fake_mt5):
        import ict_engine as ict
        m15 = _series(T0, [(10, 20, 10, 15)] * 5)   # range 10-20
        assert ict.premium_discount(m15, 12.5) == 0.25   # discount
        assert ict.premium_discount(m15, 17.5) == 0.75   # premium
        from zoneinfo import ZoneInfo
        assert ict.kill_zone(datetime(2026, 8, 20, 9, 30, tzinfo=ZoneInfo("Europe/Rome"))) == "london"
        assert ict.kill_zone(datetime(2026, 8, 20, 15, 0, tzinfo=ZoneInfo("Europe/Rome"))) == "new_york"
        assert ict.kill_zone(datetime(2026, 8, 20, 12, 0, tzinfo=ZoneInfo("Europe/Rome"))) is None

    def test_classify_bos_no_retest(self, fake_mt5):
        import ict_engine as ict
        # padding piatto (oltre MIN_BARS_BEFORE: il gate di copertura scarta
        # le finestre corte) + struttura che sale, BOS appena avvenuto, entry
        # SUBITO senza retest
        data = [(10, 10.2, 9.4, 10.1)] * 75 + \
               [(10, 11, 9.5, 10.5), (10.5, 11.5, 10, 11), (11, 12, 10.8, 11.8),
                (11.8, 11.9, 11, 11.2), (11.2, 11.4, 10.9, 11.1), (11.1, 11.6, 11, 11.5),
                (11.5, 12.6, 11.4, 12.5), (12.5, 13.1, 12.4, 13.0),
                (13.0, 13.4, 12.9, 13.3), (13.3, 13.6, 13.1, 13.5),
                (13.5, 13.8, 13.3, 13.7), (13.7, 14, 13.5, 13.9)]
        candles = _series(T0, data)
        entry_time = candles[-1]["t"] + timedelta(minutes=1)
        ctx = ict.classify_entry(candles, [], entry_time, 13.9, "buy")
        assert ctx["candles_ok"] is True
        assert ctx["setup"] in ("bos_no_retest", "bos_retest")  # BOS rilevato
        assert ctx["bos"] is not None

    def test_no_data(self, fake_mt5):
        import ict_engine as ict
        ctx = ict.classify_entry([], [], T0, 100.0, "buy")
        assert ctx["setup"] == "no_data" and ctx["candles_ok"] is False


def _mk(db, **kw):
    from database import Signal
    now = kw.pop("created", datetime(2026, 8, 10, 8, 0, 0))
    base = dict(
        telegram_msg_id=kw.pop("msg_id"), symbol="XAUUSD", direction="buy",
        entry_price=4000.0, entry_price_high=4001.0, actual_entry_price=4000.5,
        stoploss=3994.0, tp1=4005.0, tp2=4010.0, tp3=4015.0,
        status="tp1", pnl_usd=100.0, risk_usd=1000.0,
        is_filtered=False, is_archived=False, mt5_tickets="[1]",
        raw_message="t", created_at=now,
        entered_at=now + timedelta(seconds=5), closed_at=now + timedelta(minutes=30),
    )
    base.update(kw)
    s = Signal(**base)
    db.add(s); db.commit(); db.refresh(s)
    return s


def _ctx(db, signal_id, setup, **features):
    from database import TradeContext
    f = {"setup": setup, "candles_ok": True}
    f.update(features)
    db.add(TradeContext(signal_id=signal_id, setup=setup, candles_ok=True,
                        features_json=json.dumps(f)))
    db.commit()


class TestIntegrazione:
    def test_exclude_setup_rule(self, in_memory_db, fake_mt5):
        import sim_engine
        db = in_memory_db()
        try:
            t1 = _mk(db, msg_id=1, pnl_usd=-200.0, status="sl_hit")
            t2 = _mk(db, msg_id=2, pnl_usd=300.0)
            _ctx(db, t1.id, "bos_no_retest")
            _ctx(db, t2.id, "ob_retest")
            res = sim_engine.simulate("exclude_setup", {"setup": "bos_no_retest"}, db)
            assert res["ok"] is True
            assert res["trades_excluded"] == 1
            assert res["delta_pnl"] == 200.0
        finally:
            db.close()

    def test_strategy_stats(self, in_memory_db, fake_mt5):
        import ict_engine as ict
        db = in_memory_db()
        try:
            t1 = _mk(db, msg_id=10, pnl_usd=-200.0, status="sl_hit")
            t2 = _mk(db, msg_id=11, pnl_usd=300.0)
            t3 = _mk(db, msg_id=12, pnl_usd=150.0)
            _ctx(db, t1.id, "bos_no_retest", kill_zone="london", pd_bucket="premium",
                 with_trend=False, counter_trend=True)
            _ctx(db, t2.id, "ob_retest", kill_zone="new_york", pd_bucket="discount",
                 with_trend=True, counter_trend=False)
            # t3 senza contesto -> fuori coverage
            st = ict.strategy_stats(db)
            assert st["coverage"] == {"con_contesto": 2, "totale": 3}
            assert st["per_setup"]["bos_no_retest"]["pnl"] == -200.0
            assert st["per_setup"]["ob_retest"]["win_rate"] == 100.0
            assert st["per_kill_zone"]["london"]["trades"] == 1
            assert st["per_trend"]["counter_trend"]["pnl"] == -200.0
        finally:
            db.close()

    def test_ensure_contexts_senza_mt5_non_esplode(self, in_memory_db, fake_mt5):
        # FakeMT5 copy_rates_range -> [] : contesto no_data, nessun crash
        import ict_engine as ict
        from database import TradeContext
        db = in_memory_db()
        try:
            _mk(db, msg_id=20)
            res = ict.ensure_contexts(db)
            assert res["computed"] == 1
            tc = db.query(TradeContext).first()
            assert tc.setup == "no_data" and tc.candles_ok is False
            # i no_data restano RITENTABILI (storico MT5 on-demand) ma senza
            # duplicare righe: un solo TradeContext anche dopo il retry
            res2 = ict.ensure_contexts(db)
            assert res2["computed"] == 1 and res2["no_data"] == 1
            assert db.query(TradeContext).count() == 1
        finally:
            db.close()

    def test_dossier_contiene_strategia(self, in_memory_db, fake_mt5):
        import ai_advisor
        db = in_memory_db()
        try:
            t1 = _mk(db, msg_id=30, pnl_usd=100.0)
            _ctx(db, t1.id, "fvg_entry", kill_zone=None, pd_bucket="discount",
                 with_trend=True, counter_trend=False)
            d = ai_advisor.build_dossier(db)
            assert d["strategia_trader"]["coverage"]["con_contesto"] == 1
            assert "fvg_entry" in d["strategia_trader"]["per_setup"]
        finally:
            db.close()

    def test_sweep_include_exclude_setup(self, in_memory_db, fake_mt5):
        import sim_engine
        db = in_memory_db()
        try:
            # 12 perdite tutte con setup bos_no_retest -> candidato exclude_setup
            for i in range(12):
                t = _mk(db, msg_id=40 + i, pnl_usd=-100.0 - i, status="sl_hit")
                _ctx(db, t.id, "bos_no_retest")
            for i in range(8):
                t = _mk(db, msg_id=60 + i, pnl_usd=150.0)
                _ctx(db, t.id, "ob_retest")
            sw = sim_engine.sweep(db)
            promoted = {(e["sim_type"], e["sim_params"]) for e in sw["promosse"]}
            assert ("exclude_setup", json.dumps({"setup": "bos_no_retest"})) in promoted
        finally:
            db.close()
