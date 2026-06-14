"""Test su logiche pure dei handler (telegram_client) — non testano gli async
handler completi ma le funzioni di decisione (trail target, duplicate detection,
keyword parsing) che li compongono.
"""


# ─── Trail target calculation ────────────────────────────────────────────────

def trail_target(is_buy: bool, status: str, actual_entry, tp1, tp2, pip_size):
    """Replica logica trail target in telegram_client sl_move_trail_standalone /
    target_done_tg_action. Stesso schema di sync_positions auto-trail.

    Convenzione:
      status='open'  → SL = entry + 1 pip (BE+1pip)
      status='tp1'   → SL = entry + 1 pip (BE+1pip, lock break-even)
      status='tp2'   → SL = tp1 + 1 pip (lock TP1 profit)
    """
    if status == "tp2" and tp1:
        anchor = float(tp1)
        label = "TP1+1pip"
    else:
        anchor = actual_entry
        label = "BE+1pip"
    target = round(anchor + pip_size, 5) if is_buy else round(anchor - pip_size, 5)
    return target, label


class TestTrailTarget:
    def test_buy_status_open_be_plus_1pip(self):
        # GOLD: pip_size = 0.1
        t, label = trail_target(True, "open", 4332.0, 4337, 4343, 0.1)
        assert t == 4332.1
        assert label == "BE+1pip"

    def test_buy_status_tp1_be_plus_1pip(self):
        t, label = trail_target(True, "tp1", 4332.0, 4337, 4343, 0.1)
        assert t == 4332.1
        assert label == "BE+1pip"

    def test_buy_status_tp2_tp1_plus_1pip(self):
        t, label = trail_target(True, "tp2", 4332.0, 4337, 4343, 0.1)
        assert t == 4337.1
        assert label == "TP1+1pip"

    def test_sell_status_open_be_minus_1pip(self):
        t, label = trail_target(False, "open", 4543.0, 4538, 4533, 0.1)
        assert t == 4542.9
        assert label == "BE+1pip"

    def test_sell_status_tp2_tp1_minus_1pip(self):
        # Caso #366 SELL: tp2 hit, trail target deve essere TP1+1pip per SELL = TP1-pip
        t, label = trail_target(False, "tp2", 4543.0, 4538, 4533, 0.1)
        assert t == 4537.9
        assert label == "TP1+1pip"


# ─── Duplicate signal comparison ──────────────────────────────────────────────

def levels_match(parsed_entry, parsed_sl, parsed_tp1,
                 cand_entry_low, cand_entry_high, cand_sl, cand_tp1,
                 tol_pct=0.003):
    """Replica logica _close usata in duplicate detection di telegram_client."""

    def _close(a, b):
        if a is None or b is None:
            return False
        if a == 0:
            return abs(b) < tol_pct
        return abs(a - b) / abs(a) <= tol_pct

    same_sl = _close(parsed_sl, cand_sl)
    same_tp1 = _close(parsed_tp1, cand_tp1)
    same_entry = _close(parsed_entry, cand_entry_low) or _close(parsed_entry, cand_entry_high)
    return same_sl and same_tp1 and same_entry


class TestDuplicateDetection:
    def test_signal_identico_caso_446_447(self):
        """Caso #446/#447: stesso signal ripostato. Levels identici → match."""
        # Entry zone 63000-63100, SL 62500, TP1 63600 (entrambi)
        match = levels_match(
            parsed_entry=63000, parsed_sl=62500, parsed_tp1=63600,
            cand_entry_low=63000, cand_entry_high=63100,
            cand_sl=62500, cand_tp1=63600,
        )
        assert match is True

    def test_signal_diverso_tp1_no_match(self):
        # Stessi entry/SL ma TP1 diverso > 0.3% → no match
        match = levels_match(
            parsed_entry=4330, parsed_sl=4325, parsed_tp1=4337,
            cand_entry_low=4330, cand_entry_high=4332,
            cand_sl=4325, cand_tp1=4380,  # +1% diff
        )
        assert match is False

    def test_signal_simile_ma_oltre_tolleranza_no_match(self):
        # Tutto entro 0.5% (oltre 0.3%) → no match
        match = levels_match(
            parsed_entry=4332, parsed_sl=4327, parsed_tp1=4357,  # +0.6% diff
            cand_entry_low=4330, cand_entry_high=4332,
            cand_sl=4325, cand_tp1=4337,
        )
        assert match is False

    def test_signal_entry_high_match(self):
        """parsed_entry può matchare cand_entry_high (non solo entry_low)."""
        match = levels_match(
            parsed_entry=4332, parsed_sl=4325, parsed_tp1=4337,
            cand_entry_low=4330, cand_entry_high=4332,  # match su high
            cand_sl=4325, cand_tp1=4337,
        )
        assert match is True


# ─── Trail keyword detection ──────────────────────────────────────────────────

def detect_trail_keyword(text: str) -> bool:
    """Replica trail_explicit_regex in telegram_client."""
    import re
    normalized = re.sub(r'[^\w\s]+', ' ', text.lower())
    normalized = re.sub(r'\s+', ' ', normalized)
    return bool(re.search(r'\b(trail|trailing)\b', normalized))


class TestTrailKeyword:
    def test_safe_trail_in_profits(self):
        assert detect_trail_keyword("First Target Done, Safe Trail in Profits") is True

    def test_hold_book_or_trail_accordingly(self):
        assert detect_trail_keyword("Everyone Hold Book Or Trail Accordingly") is True

    def test_trailing_lowercase(self):
        assert detect_trail_keyword("everyone use trailing stop") is True

    def test_no_trail_in_simple_update(self):
        assert detect_trail_keyword("XAUUSD 4527 to 4530") is False

    def test_no_trail_in_signal(self):
        assert detect_trail_keyword("Buy Near 4527-30 TP1 4540 SL 4520") is False


# ─── Enter Now keyword detection (per anti-misclass guard) ────────────────────

def is_short_msg_without_levels(parsed_sl, parsed_tp1) -> bool:
    """Logica: signal reale richiede almeno SL + TP1. Se entrambi mancano,
    il LLM ha probabilmente mal-classificato un msg breve tipo 'Enter Now Cmp X'.
    """
    return parsed_sl is None and parsed_tp1 is None


class TestSignalGuard:
    def test_signal_completo_passa(self):
        # Signal valido: SL + TP1 entrambi presenti
        assert is_short_msg_without_levels(parsed_sl=4325, parsed_tp1=4337) is False

    def test_signal_solo_tp1_passa(self):
        # Edge case: solo TP1, no SL → tecnicamente non passa il guard
        # ma e' atteso che signal reali abbiano sempre SL.
        # Mappato dal flow attuale: passa solo se ENTRAMBI mancano.
        assert is_short_msg_without_levels(parsed_sl=None, parsed_tp1=4337) is False

    def test_signal_solo_sl_passa(self):
        # Edge case: solo SL, no TP — passa il guard (atteso che abbia almeno TP1)
        assert is_short_msg_without_levels(parsed_sl=4325, parsed_tp1=None) is False

    def test_msg_breve_senza_levels_blocca(self):
        # 'Everyone Enter Now Cmp X' — nessun SL, nessun TP
        assert is_short_msg_without_levels(parsed_sl=None, parsed_tp1=None) is True
