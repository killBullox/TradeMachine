"""Test trail-a-BE (decisione 16/07): default BE, eccezione TP esplicito nel msg."""
import pytest


class TestDetectExplicitTP:
    def test_sl_to_tp2(self):
        from parser import detect_explicit_tp_trail
        assert detect_explicit_tp_trail("Everyone move SL to TP2 now") == 2

    def test_stop_to_target1(self):
        from parser import detect_explicit_tp_trail
        assert detect_explicit_tp_trail("shift stop to target 1") == 1

    def test_sl_at_tp3(self):
        from parser import detect_explicit_tp_trail
        assert detect_explicit_tp_trail("SL at TP3") == 3

    def test_generic_trail_none(self):
        from parser import detect_explicit_tp_trail
        assert detect_explicit_tp_trail("Everyone Trail In Profit") is None

    def test_target_done_none(self):
        from parser import detect_explicit_tp_trail
        assert detect_explicit_tp_trail("2nd Target Done") is None

    def test_safe_trails_none(self):
        from parser import detect_explicit_tp_trail
        assert detect_explicit_tp_trail("Safe trails in profits") is None


class TestComputeTrailSL:
    def _sig(self, fake_mt5, **kw):
        class S:
            direction = "sell"; tp1 = 3996.0; tp2 = 3992.0; tp3 = 3988.0
            actual_entry_price = 3998.86; entry_price = 4000.0
        s = S()
        for k, v in kw.items(): setattr(s, k, v)
        return s

    def test_default_be_sell(self, fake_mt5):
        import mt5_trader
        sig = self._sig(fake_mt5)
        sl, lbl = mt5_trader.compute_trail_sl(sig, pip_size=0.1, tp_level_hit=2, message="Everyone Trail In Profit")
        # SELL: BE - 1pip = 3998.86 - 0.1 = 3998.76, label BE
        assert lbl == "BE"
        assert abs(sl - 3998.76) < 1e-6

    def test_progressione_non_attiva(self, fake_mt5):
        """Anche con tp_level_hit=2 (TP2 colpito), NON va a TP1: resta BE."""
        import mt5_trader
        sig = self._sig(fake_mt5)
        sl, lbl = mt5_trader.compute_trail_sl(sig, pip_size=0.1, tp_level_hit=2, message="2nd target done trail")
        assert lbl == "BE"  # NON "TP1"

    def test_eccezione_tp_esplicito(self, fake_mt5):
        import mt5_trader
        sig = self._sig(fake_mt5)
        sl, lbl = mt5_trader.compute_trail_sl(sig, pip_size=0.1, tp_level_hit=2, message="move SL to TP2")
        # onora TP2: SELL 3992 - 0.1 = 3991.9
        assert lbl == "TP2(esplicito)"
        assert abs(sl - 3991.9) < 1e-6

    def test_buy_be(self, fake_mt5):
        import mt5_trader
        sig = self._sig(fake_mt5, direction="buy", actual_entry_price=4000.0,
                        tp1=4004.0, tp2=4008.0, tp3=4012.0)
        sl, lbl = mt5_trader.compute_trail_sl(sig, pip_size=0.1, tp_level_hit=2, message="trail in profit")
        # BUY: BE + 1pip = 4000.1
        assert lbl == "BE"
        assert abs(sl - 4000.1) < 1e-6

    def test_578_scenario(self, fake_mt5):
        """#578: dopo 'Trail In Profit' con tp2, SL a BE 3998.76 (non TP1 3995.9).
        Il rimbalzo a 3996.72 NON avrebbe toccato 3998.76 → residuo salvo."""
        import mt5_trader
        sig = self._sig(fake_mt5)
        sl, lbl = mt5_trader.compute_trail_sl(sig, pip_size=0.1, tp_level_hit=2, message="Everyone Trail In Profit")
        # SELL: SL a BE (3998.76) sta SOPRA il rimbalzo 3996.72 → non stoppato,
        # il residuo sopravvive e corre a TP3. Col vecchio trail (SL 3995.90,
        # sotto il rimbalzo) veniva stoppato.
        assert sl > 3996.72
