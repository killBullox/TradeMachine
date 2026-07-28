"""Fix #580: coherent_size_entry — il sizing non usa mai un bordo entry fuori
dalla zona valida (SL, TP1), E non sottostima mai la distanza dallo SL (cosi'
il rischio non supera MAI il max risk)."""
import pytest


def _f(*a):
    import mt5_trader
    return mt5_trader.coherent_size_entry(*a)


class TestCoherentSizeEntry:
    def test_580_buy_bordo_sopra_tp1(self, fake_mt5):
        # #580: BUY range 4000-4099, SL 3995, TP1 4004. max=4099 (>TP1) incoerente.
        # prezzo corrente 4001 -> si dimensiona su 4001 (il piu' lontano dallo SL
        # tra coerenti {4000} + corrente {4001}).
        se, fixed = _f(4000.0, 4099.0, 4004.0, 3995.0, True, 4001.0)
        assert se == 4001.0
        assert fixed == 4099.0

    def test_580_senza_prezzo_corrente_usa_bordo_coerente(self, fake_mt5):
        # senza prezzo corrente: ripiega sul bordo coerente 4000
        se, fixed = _f(4000.0, 4099.0, 4004.0, 3995.0, True, None)
        assert se == 4000.0
        assert fixed == 4099.0

    def test_614_buy_coerente_nessun_cambio(self, fake_mt5):
        # #614: BUY 4040-4041, SL 4034, TP1 4045. max=4041 coerente -> invariato
        se, fixed = _f(4040.0, 4041.0, 4045.0, 4034.0, True, 4041.5)
        assert se == 4041.0
        assert fixed is None

    def test_sell_bordo_sotto_tp1(self, fake_mt5):
        # SELL range 4100-4001, SL 4110, TP1 4095. min=4001 (<TP1) incoerente.
        # candidati coerenti {4100, corrente 4099} -> min=4099 (piu' lontano dallo
        # SL = worst-case fill). NON 4100, che col fill a 4099 sovra-rischierebbe.
        se, fixed = _f(4001.0, 4100.0, 4095.0, 4110.0, False, 4099.0)
        assert se == 4099.0
        assert fixed == 4001.0

    def test_sell_coerente(self, fake_mt5):
        se, fixed = _f(4100.0, 4102.0, 4095.0, 4110.0, False, 4099.0)
        assert se == 4100.0
        assert fixed is None

    def test_nessun_bordo_coerente_lascia_worstcase(self, fake_mt5):
        # Entrambi i bordi sopra TP1 (BUY) e prezzo corrente pure: lascio il max
        # (worst-case, sotto-size = sicuro), nessuna correzione.
        se, fixed = _f(4200.0, 4300.0, 4004.0, 3995.0, True, 4250.0)
        assert se == 4300.0
        assert fixed is None

    def test_tp1_o_sl_mancante_nessun_check(self, fake_mt5):
        se, fixed = _f(4000.0, 4099.0, None, 3995.0, True, 4001.0)
        assert se == 4099.0
        assert fixed is None

    def test_bordo_singolo(self, fake_mt5):
        se, fixed = _f(4000.0, None, 4004.0, 3995.0, True, 4001.0)
        assert se == 4000.0
        assert fixed is None


class TestInvarianteRischio:
    """Il size_entry scelto non deve MAI far superare il max risk: la distanza
    usata per il sizing deve essere >= distanza al fill reale."""

    def _risk_at_fill(self, size_entry, sl, fill, is_buy, risk_target=1000.0, pv=100.0):
        dist_size = abs(size_entry - sl)
        lots = risk_target / (dist_size * pv)
        dist_fill = abs(fill - sl)
        return dist_fill * pv * lots

    def test_580_rischio_non_supera_max(self, fake_mt5):
        # #580: fill reale ~4001. size_entry scelto = 4001 -> rischio esatto = max.
        se, _ = _f(4000.0, 4099.0, 4004.0, 3995.0, True, 4001.0)
        risk = self._risk_at_fill(se, 3995.0, fill=4001.0, is_buy=True)
        assert risk <= 1000.0 + 1e-6, f"rischio {risk} supera max"

    def test_bordo_vicino_avrebbe_sovra_rischiato(self, fake_mt5):
        # Controprova: dimensionare sul bordo VICINO (4000) col fill a 4001
        # avrebbe superato il max -> il fix NON deve scegliere 4000 se c'e' 4001.
        risk_near = self._risk_at_fill(4000.0, 3995.0, fill=4001.0, is_buy=True)
        assert risk_near > 1000.0
        se, _ = _f(4000.0, 4099.0, 4004.0, 3995.0, True, 4001.0)
        assert se != 4000.0

    def test_sell_rischio_non_supera_max(self, fake_mt5):
        se, _ = _f(4001.0, 4100.0, 4095.0, 4110.0, False, 4099.0)
        risk = self._risk_at_fill(se, 4110.0, fill=4099.0, is_buy=False)
        assert risk <= 1000.0 + 1e-6
