"""Test sul parser regex (parser.py) — fallback usato quando LLM non disponibile.

Copre i casi reali rilevati nello storico dei messaggi del trader. Snapshot del
comportamento corrente: questi test devono restare verdi anche dopo l'integrazione
del prop_mode (regola d'oro per garantire zero interferenza su Avatrade).
"""
from parser import parse_message, ParsedSignal, ParsedUpdate, ParsedSLMove, ParsedClose, ParsedReenter


class TestSignalParsing:
    def test_signal_xauusd_buy_completo(self):
        text = "**🥇** **#XAUUSD** | Buy Near 4332-30\n\n✅Target 1 : 4337 | Target 2 : 4343 | Target 3 : 4350\n\n❕Stoploss : 4325"
        mtype, p = parse_message(text)
        assert mtype == "signal"
        assert isinstance(p, ParsedSignal)
        assert p.symbol == "XAUUSD"
        assert p.direction == "buy"
        assert p.stoploss == 4325.0
        assert p.tp1 == 4337.0
        assert p.tp2 == 4343.0
        assert p.tp3 == 4350.0
        # entry: gli viene riconosciuto un range
        assert {p.entry_price, p.entry_price_high} == {4332.0, 4330.0}

    def test_signal_gbpjpy_sell_completo(self):
        text = "🇬🇧🇯🇵**#GBPJPY** | Sell near 213.150-213.180\n\nTarget 1 : 213.00 | Target 2 : 212.850 | Target 3 : 212.700\n\n❗️Stoploss : 213.320"
        mtype, p = parse_message(text)
        assert mtype == "signal"
        assert p.symbol == "GBPJPY"
        assert p.direction == "sell"
        assert p.stoploss == 213.32
        assert p.tp1 == 213.0

    def test_signal_btcusd_buy(self):
        text = "**💰** **#BTCUSD** | Buy Near 63000-63100\n\n✅ Target 1 63600: | Target 2 : 64000 | Target 3 : 64600\n\n❗️ Stoploss :62500"
        mtype, p = parse_message(text)
        assert mtype == "signal"
        assert p.symbol == "BTCUSD"
        assert p.direction == "buy"
        assert p.stoploss == 62500.0


class TestUpdateParsing:
    # NB: il regex parser e' un fallback minimo. La maggior parte degli "update"
    # va al LLM (vedi test_llm_parser.py). Qui verifichiamo solo che il regex
    # parser NON confonda questi messaggi con un signal.
    def test_update_first_target_done_not_signal(self):
        text = "🏆**#XAUUSD** | 4527.00 To 4531.00\n\n✅First Target Done"
        mtype, p = parse_message(text)
        assert mtype != "signal"

    def test_update_hold_book_or_trail_not_signal(self):
        text = "🙌🏻Everyone Hold Book Or Trail Accordingly"
        mtype, p = parse_message(text)
        assert mtype != "signal"


class TestSLMoveParsing:
    def test_sl_move_with_price(self):
        text = "🥷Everyone Hold With 4521 SL"
        mtype, p = parse_message(text)
        assert mtype == "sl_move"
        assert isinstance(p, ParsedSLMove)
        assert p.new_sl == 4521.0
        assert p.is_breakeven is False

    def test_sl_move_cost_to_cost(self):
        text = "🥷Everyone Hold With Cost To Cost"
        mtype, p = parse_message(text)
        assert mtype == "sl_move"
        assert p.is_breakeven is True


class TestCloseParsing:
    def test_close_trade_not_signal(self):
        # Il regex parser non riconosce 'Trade Closed' come close esplicito.
        # Verifichiamo solo che NON venga interpretato come signal nuovo.
        text = "🙌🏻Trade Closed"
        mtype, p = parse_message(text)
        assert mtype != "signal"


class TestReenterParsing:
    def test_reenter_again(self):
        text = "Everyone Enter Again Now"
        mtype, p = parse_message(text)
        assert mtype == "reenter"

    def test_reenter_explicit(self):
        text = "Re-enter at same levels"
        mtype, p = parse_message(text)
        assert mtype == "reenter"


class TestIgnore:
    def test_watchlist_or_other(self):
        text = "📈Add #GBPJPY to watchlist"
        mtype, p = parse_message(text)
        # accettabile sia "watchlist" che "other"/"ignore" — non è un signal
        assert mtype in ("watchlist", "other", "ignore")

    def test_market_levels(self):
        text = "🔔Today's Important Levels for #XAUUSD\n\nSupport: 4456-4445-4432\n\nResistance: 4479-4489-4500"
        mtype, p = parse_message(text)
        # accettiamo che venga riconosciuto come 'level' o ignorato
        assert mtype in ("level", "other", "ignore")
