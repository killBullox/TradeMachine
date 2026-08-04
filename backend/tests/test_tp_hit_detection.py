"""detect_tp_hits — riconoscimento TP robusto allo slippage (caso #636: TP1
4057.00 riempito a 4057.07 su un SELL -> l'auto-BE non scattava). Pilota sia
be_at_tp1 sia l'auto-trail."""
import pytest


def _f(*a):
    import mt5_trader
    return mt5_trader.detect_tp_hits(*a)


class TestDetectTpHits:
    def test_636_sell_tp1_slippage(self, fake_mt5):
        # #636 reale: SELL, TP1=4057.00, ticket TP1 chiuso a 4057.07 (7c oltre),
        # motivo broker "TP". Il confronto prezzo (4057.07 <= 4057.00) fallirebbe,
        # ma il motivo "TP" lo salva -> tp1_hit True.
        closed = [(172777987, 4057.07, 244.75, None)]
        reasons = {172777987: "TP"}
        order = [172777987, 172777990, 172777994]
        tp1_hit, levels = _f(closed, reasons, order, 4057.0, 4052.0, 4047.0, False)
        assert tp1_hit is True
        assert levels == 1

    def test_buy_tp1_slippage(self, fake_mt5):
        # BUY, TP1=4060.00, fill a 4059.95 (5c sotto), motivo "TP" -> riconosciuto
        closed = [(500, 4059.95, 100.0, None)]
        reasons = {500: "TP"}
        order = [500, 501, 502]
        tp1_hit, levels = _f(closed, reasons, order, 4060.0, 4070.0, 4080.0, True)
        assert tp1_hit is True
        assert levels == 1

    def test_tp2_chiuso_implica_tp1(self, fake_mt5):
        # Ticket TP2 chiuso in TP (livello 2) -> tp1_hit True, levels 2
        closed = [(991, 4051.9, 300.0, None)]  # SELL TP2 4052 fill 4051.9
        reasons = {991: "TP"}
        order = [990, 991, 992]
        tp1_hit, levels = _f(closed, reasons, order, 4057.0, 4052.0, 4047.0, False)
        assert tp1_hit is True
        assert levels == 2

    def test_sl_hit_non_e_tp(self, fake_mt5):
        # Ticket chiuso in SL -> nessun TP
        closed = [(700, 4067.0, -100.0, None)]
        reasons = {700: "SL"}
        order = [700, 701, 702]
        tp1_hit, levels = _f(closed, reasons, order, 4057.0, 4052.0, 4047.0, False)
        assert tp1_hit is False
        assert levels == 0

    def test_fallback_prezzo_senza_motivo(self, fake_mt5):
        # Motivo sconosciuto ma prezzo ha toccato TP1 -> fallback prezzo funziona
        closed = [(800, 4056.5, 100.0, None)]  # SELL, 4056.5 <= TP1 4057
        reasons = {800: "?"}
        order = [800, 801, 802]
        tp1_hit, levels = _f(closed, reasons, order, 4057.0, 4052.0, 4047.0, False)
        assert tp1_hit is True
        assert levels == 1

    def test_nessun_chiuso(self, fake_mt5):
        tp1_hit, levels = _f([], {}, [], 4057.0, 4052.0, 4047.0, False)
        assert tp1_hit is False
        assert levels == 0

    def test_ticket_non_in_ordine_conta_come_tp1(self, fake_mt5):
        # Ticket chiuso "TP" ma non presente in tickets_order -> almeno TP1
        closed = [(999, 4057.07, 200.0, None)]
        reasons = {999: "TP"}
        order = [1, 2, 3]
        tp1_hit, levels = _f(closed, reasons, order, 4057.0, 4052.0, 4047.0, False)
        assert tp1_hit is True
        assert levels == 1
