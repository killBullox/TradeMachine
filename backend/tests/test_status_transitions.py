"""Test sulla logica di determinazione status nel completed branch di
sync_positions. La logica e' replicata qui per testarla in isolamento (senza
chiamare l'intera sync_positions che richiede DB+MT5).

Coverage: TP1/TP2/TP3 hit, sl_hit, trail_out (caso #442).
"""


def determine_status(closed_tickets: list, sig_tp1, sig_tp2, sig_tp3,
                    sig_actual_entry, sig_pnl_total, is_buy: bool):
    """Replica esatta della logica in mt5_trader.sync_positions completed branch.

    closed_tickets: lista di (ticket, close_price, profit, close_time)
    Ritorna la string status (sl_hit / tp1 / tp2 / tp3 / trail_out).
    """
    # TP più alto raggiunto
    new_status = "sl_hit"
    for tp_num, tp_price in [(3, sig_tp3), (2, sig_tp2), (1, sig_tp1)]:
        if tp_price is None:
            continue
        if any((is_buy and cp >= tp_price) or (not is_buy and cp <= tp_price)
               for _, cp, _, _ in closed_tickets):
            new_status = f"tp{tp_num}"
            break
    # Trail-out detection: se sl_hit ma close favorevole vs entry o pnl > 0
    if new_status == "sl_hit" and sig_actual_entry and closed_tickets:
        favorable = sum(
            1 for _, cp, _, _ in closed_tickets
            if (is_buy and cp > sig_actual_entry) or (not is_buy and cp < sig_actual_entry)
        )
        if favorable >= len(closed_tickets) / 2 or sig_pnl_total > 0:
            new_status = "trail_out"
    return new_status


class TestStatusTransitions:
    def test_tp1_hit_buy(self):
        # 1 ticket chiuso al TP1 (4337), entry 4332.
        closed = [(1, 4337.0, 20.0, 0)]
        status = determine_status(closed, 4337, 4343, 4350, 4332, 20.0, is_buy=True)
        assert status == "tp1"

    def test_tp2_hit_buy(self):
        closed = [(1, 4337, 20, 0), (2, 4343, 40, 0)]
        status = determine_status(closed, 4337, 4343, 4350, 4332, 60, is_buy=True)
        assert status == "tp2"

    def test_tp3_hit_sell(self):
        # SELL: TP più "alto" = prezzo più basso. Tutti i 3 ticket chiusi sotto TP3.
        closed = [(1, 4283, 50, 0), (2, 4275, 70, 0), (3, 4270, 100, 0)]
        status = determine_status(closed, 4290, 4280, 4275, 4295, 220, is_buy=False)
        assert status == "tp3"

    def test_sl_hit_reale_buy(self):
        # Close prezzo SOTTO SL (4325), nessun TP toccato, profit negativo.
        closed = [(1, 4325, -10, 0), (2, 4325, -10, 0), (3, 4325, -10, 0)]
        status = determine_status(closed, 4337, 4343, 4350, 4332, -30, is_buy=True)
        assert status == "sl_hit"

    def test_trail_out_buy_caso_442(self):
        """Caso #442 GBPJPY BUY: entry 214.836, trail TG SL→214.845, close 214.845.
        Nessun TP toccato ma close FAVOREVOLE (sopra entry per BUY) e profit > 0
        → status deve essere 'trail_out'."""
        closed = [(1, 214.845, 1.56, 0), (2, 214.845, 1.87, 0), (3, 214.845, 2.02, 0)]
        status = determine_status(
            closed, 214.95, 215.1, 215.25, 214.836, 5.45, is_buy=True
        )
        assert status == "trail_out"

    def test_trail_out_sell(self):
        # SELL: entry 4543, trail SL 4540 (sotto entry per SELL = favorevole),
        # close 4540, profit positivo.
        closed = [(1, 4540, 10, 0), (2, 4540, 10, 0)]
        status = determine_status(closed, 4530, 4520, 4510, 4543, 20, is_buy=False)
        assert status == "trail_out"

    def test_sl_hit_se_close_negativo_anche_se_close_uguale_entry(self):
        # Caso edge: close = entry esatto, profit negativo (spread/commissioni).
        # Maggioranza non favorevole → sl_hit.
        closed = [(1, 4332.0, -2, 0), (2, 4332.0, -2, 0)]
        status = determine_status(closed, 4337, 4343, 4350, 4332.0, -4, is_buy=True)
        # close == entry, non strettamente > → 0 favorable → sl_hit
        # (anche se profit < 0)
        assert status == "sl_hit"

    def test_tp1_anche_se_un_solo_ticket_lo_raggiunge(self):
        # 3 ticket: 1 chiude a TP1, 2 chiudono a SL.
        # Il TP più alto raggiunto e' 1 → status='tp1' (status incorpora il max TP hit).
        closed = [(1, 4337, 20, 0), (2, 4325, -10, 0), (3, 4325, -10, 0)]
        status = determine_status(closed, 4337, 4343, 4350, 4332, 0, is_buy=True)
        assert status == "tp1"


class TestTpLevelHitPartialClose:
    """Logica avanzamento status durante partial close (TP1 chiude prima)."""

    def test_partial_close_avanza_tp1(self):
        """1 ticket chiuso a TP1, altri 2 ancora aperti."""
        closed_tickets = [(1, 4337.0, 20.0, 0)]
        # Replica logica in sync_positions: cerca TP più alto chiuso
        is_buy = True
        tp_levels_hit = 0
        for tp_num, tp_price in [(1, 4337), (2, 4343), (3, 4350)]:
            if any((is_buy and cp >= tp_price) or (not is_buy and cp <= tp_price)
                   for _, cp, _, _ in closed_tickets):
                tp_levels_hit = max(tp_levels_hit, tp_num)
        assert tp_levels_hit == 1

    def test_partial_close_avanza_tp2(self):
        """2 ticket chiusi: 1 al TP1, 1 al TP2."""
        closed_tickets = [(1, 4337, 20, 0), (2, 4343, 40, 0)]
        is_buy = True
        tp_levels_hit = 0
        for tp_num, tp_price in [(1, 4337), (2, 4343), (3, 4350)]:
            if any((is_buy and cp >= tp_price) or (not is_buy and cp <= tp_price)
                   for _, cp, _, _ in closed_tickets):
                tp_levels_hit = max(tp_levels_hit, tp_num)
        assert tp_levels_hit == 2
