"""Test su llm_to_parsed: simula risposta LLM (dict JSON) e verifica mapping
in ParsedXxx. Non chiama l'API Anthropic reale.

Questi test garantiscono che ogni tipo di messaggio classificato dal LLM
(signal/sl_move/update/close/reenter/enter_now) venga correttamente convertito
in un oggetto Parsed e con i livelli/campi giusti.
"""
from llm_parser import llm_to_parsed
from parser import ParsedSignal, ParsedUpdate, ParsedSLMove, ParsedClose, ParsedReenter, ParsedEnterNow


def _make(data: dict, raw: str = "test_msg") -> dict:
    """Helper: aggiunge il campo _raw e ritorna dict."""
    out = dict(data)
    out["_raw"] = raw
    return out


class TestSignal:
    def test_signal_xauusd_buy_completo(self):
        d = _make({
            "type": "signal", "symbol": "XAUUSD", "direction": "buy",
            "entry_low": 4330, "entry_high": 4332,
            "tp1": 4337, "tp2": 4343, "tp3": 4350, "sl": 4325,
            "entry_type": "near",
        })
        mtype, p = llm_to_parsed(d)
        assert mtype == "signal"
        assert isinstance(p, ParsedSignal)
        assert p.symbol == "XAUUSD"
        assert p.direction == "buy"
        assert p.tp1 == 4337
        assert p.stoploss == 4325
        assert p.entry_type == "near"

    def test_signal_breakout_above(self):
        d = _make({
            "type": "signal", "symbol": "BTCUSD", "direction": "buy",
            "entry_low": 100000, "entry_high": None,
            "tp1": 105000, "sl": 95000,
            "entry_type": "breakout",
        }, raw="Buy above 100000")
        mtype, p = llm_to_parsed(d)
        assert mtype == "signal"
        assert p.entry_type == "breakout"

    def test_signal_solo_tp1_e_sl(self):
        # Signal reale: alcuni prop/trader danno solo TP1 + SL.
        d = _make({
            "type": "signal", "symbol": "GBPJPY", "direction": "sell",
            "entry_low": 213.15, "entry_high": 213.18,
            "tp1": 213.0, "tp2": None, "tp3": None, "sl": 213.32,
        })
        mtype, p = llm_to_parsed(d)
        assert mtype == "signal"
        assert p.tp1 == 213.0
        assert p.tp2 is None
        assert p.tp3 is None

    def test_signal_senza_direction_non_passa(self):
        d = _make({"type": "signal", "symbol": "XAUUSD", "direction": None,
                   "tp1": 4337, "sl": 4325})
        mtype, p = llm_to_parsed(d)
        assert mtype == "other"
        assert p is None


class TestSlMove:
    def test_sl_move_esplicito(self):
        d = _make({"type": "sl_move", "symbol": "XAUUSD",
                   "new_sl": 4521, "is_breakeven": False})
        mtype, p = llm_to_parsed(d)
        assert mtype == "sl_move"
        assert isinstance(p, ParsedSLMove)
        assert p.new_sl == 4521
        assert p.is_breakeven is False

    def test_sl_move_breakeven(self):
        d = _make({"type": "sl_move", "symbol": "XAUUSD",
                   "new_sl": None, "is_breakeven": True})
        mtype, p = llm_to_parsed(d)
        assert mtype == "sl_move"
        assert p.is_breakeven is True


class TestUpdateAndTrail:
    def test_update_first_target_hit(self):
        d = _make({
            "type": "update", "symbol": "XAUUSD",
            "price_from": 4527, "price_to": 4531,
            "status_text": "first_target_hit",
        })
        mtype, p = llm_to_parsed(d)
        assert mtype == "update"
        assert isinstance(p, ParsedUpdate)
        assert p.status_text == "first_target_hit"

    def test_update_trail_active(self):
        # "Hold Book Or Trail Accordingly" — il LLM deve classificare come
        # trail_active perche' e' istruzione esplicita di trail.
        d = _make({
            "type": "update", "symbol": None,
            "price_from": None, "price_to": None,
            "status_text": "trail_active",
        })
        mtype, p = llm_to_parsed(d)
        assert mtype == "update"
        assert p.status_text == "trail_active"

    def test_update_near_target(self):
        d = _make({
            "type": "update", "symbol": "GBPJPY",
            "price_from": 214.0, "price_to": 214.15,
            "status_text": "near_target",
        })
        mtype, p = llm_to_parsed(d)
        assert mtype == "update"
        assert p.status_text == "near_target"


class TestClose:
    def test_close_simple(self):
        d = _make({"type": "close", "symbol": "XAUUSD",
                   "price_from": None, "close_reason": "Trade Closed"})
        mtype, p = llm_to_parsed(d)
        assert mtype == "close"
        assert isinstance(p, ParsedClose)


class TestReenter:
    def test_reenter_explicit(self):
        d = _make({"type": "reenter", "symbol": "XAUUSD"},
                  raw="Re-enter at same levels")
        mtype, p = llm_to_parsed(d)
        assert mtype == "reenter"
        assert isinstance(p, ParsedReenter)


class TestEnterNow:
    """Tipo distinto: enter_now ≠ reenter. Vedi prompt LLM."""

    def test_enter_now_semplice(self):
        d = _make({"type": "enter_now", "symbol": "XAUUSD", "sl": None},
                  raw="Everyone Enter Now")
        mtype, p = llm_to_parsed(d)
        assert mtype == "enter_now"
        assert isinstance(p, ParsedEnterNow)
        assert p.sl is None

    def test_enter_now_con_sl(self):
        d = _make({"type": "enter_now", "symbol": "XAUUSD", "sl": 4084},
                  raw="Everyone Enter Now with 4084 SL")
        mtype, p = llm_to_parsed(d)
        assert mtype == "enter_now"
        assert p.sl == 4084

    def test_enter_now_cmp_price_info(self):
        # "Cmp 4341" — il prezzo e' info, non l'entry. enter_now non ha entry
        # fields nel ParsedEnterNow.
        d = _make({"type": "enter_now", "symbol": "XAUUSD", "sl": None},
                  raw="Everyone Enter Now Cmp 4341")
        mtype, p = llm_to_parsed(d)
        assert mtype == "enter_now"


class TestIgnore:
    def test_ignore_type(self):
        d = _make({"type": "ignore"}, raw="Add #XAUUSD to watchlist")
        mtype, p = llm_to_parsed(d)
        # llm_to_parsed potrebbe ritornare 'ignore' o 'other' o None: accetta
        assert mtype in ("ignore", "other") or p is None
