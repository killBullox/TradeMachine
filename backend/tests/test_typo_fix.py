"""Test del helper mt5_trader.fix_price_typo + logica typo correction in
place_orders. Garantiscono che i fix typo storici (#361, #431, ecc.) restino
gestiti correttamente.
"""
import sys
import importlib


def _import_mt5_trader_stub():
    """Importa mt5_trader saltando l'init MT5 reale (non disponibile in test)."""
    # Forza MT5_DISABLED affinche' _get_mt5 ritorni None
    import os
    os.environ["MT5_DISABLED"] = "1"
    if "mt5_trader" in sys.modules:
        return sys.modules["mt5_trader"]
    return importlib.import_module("mt5_trader")


class TestFixPriceTypoSubstitute:
    def test_value_gia_ok_nessuna_modifica(self):
        m = _import_mt5_trader_stub()
        # SL = 4525 con prezzo broker 4530, tolleranza default 2%
        v, fixed, reason = m.fix_price_typo(4525, anchor_price=4530, digits=2)
        assert fixed is False
        assert v == 4525
        assert reason == "ok"

    def test_single_digit_substitute_4335_to_4535(self):
        """Caso #361 XAUUSD: SL Move TG '4335' invece di '4535' per BUY @ 4543."""
        m = _import_mt5_trader_stub()
        # BUY: SL deve stare sotto il prezzo. Vincoli: max_allowed appena sotto.
        v, fixed, reason = m.fix_price_typo(
            4335, anchor_price=4543, digits=2,
            min_allowed=None, max_allowed=4543 * 0.9999,
        )
        assert fixed is True
        assert v == 4535
        assert "single_digit" in reason.lower() or "substitute" in reason.lower()

    def test_no_fix_se_valore_assurdo(self):
        m = _import_mt5_trader_stub()
        # Valore lontanissimo, nessun candidato single-digit entro tolleranza 2%.
        v, fixed, reason = m.fix_price_typo(100, anchor_price=4543, digits=2,
                                            max_allowed=4543 * 0.9999)
        assert fixed is False
        assert reason == "no_fix"


class TestFixPriceTypoInsert:
    def test_insert_digit_4480_to_4348_caso_431(self):
        """Caso #431: trader scrive 'Stoploss 448', parser normalizza a 4480.
        Insert digit deve trovare 4348 come unico candidato valido per SELL."""
        m = _import_mt5_trader_stub()
        # SELL XAUUSD entry 4343, TP1 4339 (sotto), SL deve stare sopra entry.
        # 4480 sopra entry ma troppo lontano (137 pt vs tp1_dist 4).
        # Insert digit di "448" (raw short di 4480) dovrebbe produrre 4348.
        v, fixed, reason = m.fix_price_typo(
            4480, anchor_price=4343, digits=2,
            min_allowed=4343 * 1.0001,  # SELL: SL sopra prezzo
            anchor_tol_pct=0.02,
        )
        assert fixed is True
        assert v == 4348

    def test_no_anchor_no_fix(self):
        m = _import_mt5_trader_stub()
        v, fixed, reason = m.fix_price_typo(4480, anchor_price=None, digits=2)
        assert fixed is False
        assert "no_anchor" in reason

    def test_zero_value_no_fix(self):
        m = _import_mt5_trader_stub()
        v, fixed, reason = m.fix_price_typo(0, anchor_price=4543, digits=2)
        assert fixed is False


class TestFixPriceTypoSideConstraint:
    def test_buy_sl_su_lato_sbagliato_corretto(self):
        """BUY: SL deve stare SOTTO prezzo broker. Valore typo sopra → fix."""
        m = _import_mt5_trader_stub()
        # BUY: max_allowed = prezzo - small buffer. value=4345 sopra prezzo 4340.
        v, fixed, reason = m.fix_price_typo(
            4345, anchor_price=4340, digits=2,
            max_allowed=4340 * 0.9999,
        )
        # 4345 sopra prezzo (max_allowed = 4339.57) → invalid bound
        # Single-digit fix dovrebbe cercare 4335, 4325, ... entro 2% (4253-4427)
        # 4335 in range e sotto max_allowed → fix
        assert fixed is True
        assert v < 4340  # sotto prezzo

    def test_sell_sl_su_lato_sbagliato_corretto(self):
        """SELL: SL deve stare SOPRA prezzo broker. Valore sotto → fix."""
        m = _import_mt5_trader_stub()
        v, fixed, reason = m.fix_price_typo(
            4335, anchor_price=4340, digits=2,
            min_allowed=4340 * 1.0001,
        )
        # 4335 sotto min_allowed (4340.43) → invalid.
        # Single-digit fix cerca 4345, 4355, ... → 4345 in range e sopra prezzo
        assert fixed is True
        assert v > 4340
