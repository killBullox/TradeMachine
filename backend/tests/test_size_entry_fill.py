"""Fix #670: clamp_size_entry_to_fill — il sizing usa il prezzo piu' lontano
dallo SL tra il bordo del range e il FILL reale, cosi' un MARKET entrato sopra
il range non fa superare il max risk (#670: -1229$ su max 1000$, +23%)."""
import pytest


def _f(*a):
    import mt5_trader
    return mt5_trader.clamp_size_entry_to_fill(*a)


class TestClampSizeEntryToFill:
    def test_670_market_sopra_range(self, fake_mt5):
        # #670: BUY, size_entry sul bordo 4388, ma fill reale MARKET 4389.35 (piu'
        # lontano dallo SL 4382) -> si dimensiona su 4389.35.
        se, changed = _f(4388.0, 4389.35, 4382.0, True)
        assert se == 4389.35
        assert changed is True

    def test_buy_fill_dentro_range_nessun_cambio(self, fake_mt5):
        # fill 4387 <= bordo 4388 -> nessun up-sizing, resta 4388
        se, changed = _f(4388.0, 4387.0, 4382.0, True)
        assert se == 4388.0
        assert changed is False

    def test_sell_fill_sotto_range(self, fake_mt5):
        # SELL, size_entry 4380 (bordo), fill 4378.5 piu' lontano dallo SL 4386
        # -> si dimensiona su 4378.5 (min).
        se, changed = _f(4380.0, 4378.5, 4386.0, False)
        assert se == 4378.5
        assert changed is True

    def test_sell_fill_migliore_nessun_cambio(self, fake_mt5):
        # SELL, fill 4381 piu' vicino allo SL del bordo 4380 -> resta 4380
        se, changed = _f(4380.0, 4381.0, 4386.0, False)
        assert se == 4380.0
        assert changed is False

    def test_entry_o_sl_none(self, fake_mt5):
        assert _f(4388.0, None, 4382.0, True) == (4388.0, False)
        assert _f(4388.0, 4389.0, None, True) == (4388.0, False)


class TestInvarianteRischioFill:
    def _risk(self, size_entry, sl, fill, risk_target=1000.0, pv=100.0):
        lots = risk_target / (abs(size_entry - sl) * pv)
        return abs(fill - sl) * pv * lots

    def test_670_rischio_rientra_nel_max(self, fake_mt5):
        # Senza fix: dimensiona su 4388 (dist 6) ma fill a 4389.35 -> rischio > max
        risk_bug = self._risk(4388.0, 4382.0, fill=4389.35)
        assert risk_bug > 1000.0  # dimostra il bug (+23%)
        # Con fix: size_entry allineato al fill -> rischio esatto = max
        se, _ = _f(4388.0, 4389.35, 4382.0, True)
        risk_fix = self._risk(se, 4382.0, fill=4389.35)
        assert risk_fix <= 1000.0 + 1e-6
