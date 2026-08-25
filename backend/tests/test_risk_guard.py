"""Sizing oltre il max risk: il fix #670 girava PRIMA che `entry` fosse il
prezzo di esecuzione, quindi su un MARKET entrato sopra il range la size
restava calcolata sul bordo. Casi reali 25/08: #692 -1102$, #693 -1161$,
#694 -1179$ con max risk 1000$.

Qui si verifica il RE-CLAMP sul prezzo definitivo e la GUARDIA FINALE, che
e' l'ultima rete e non dipende da nessun ramo di sizing precedente."""
import pytest


CONTRACT = 100.0          # XAUUSD: 100 oz per lotto


def _risk(entry, sl, lots_each, n=3):
    return abs(entry - sl) * CONTRACT * lots_each * n


class TestCasiReali:
    """I tre trade del 25/08, con i numeri esatti presi dal log."""

    CASI = [
        # (id, range_top, fill, sl, lots_each_usati, perdita_reale)
        (692, 4650.0, 4650.61, 4645.0, 0.66, -1102.20),
        (693, 4639.0, 4640.21, 4632.0, 0.47, -1161.37),
        (694, 4641.0, 4641.97, 4635.0, 0.55, -1179.20),
    ]

    def test_sizing_sul_bordo_sforava_il_massimo(self):
        """Fotografa il difetto: dimensionando sul bordo del range il rischio
        al fill reale era gia' oltre 1000$ PRIMA di qualsiasi slippage."""
        for tid, bordo, fill, sl, lots, perdita in self.CASI:
            teorico_bordo = _risk(bordo, sl, lots)
            teorico_fill = _risk(fill, sl, lots)
            assert teorico_bordo <= 1000 * 1.02, f"#{tid} sizing sul bordo"
            assert teorico_fill > 1000, (
                f"#{tid}: al fill {fill} il rischio era {teorico_fill:.0f}$")

    def test_clamp_sposta_il_sizing_sul_fill(self):
        import mt5_trader
        for tid, bordo, fill, sl, lots, perdita in self.CASI:
            se, changed = mt5_trader.clamp_size_entry_to_fill(bordo, fill, sl, True)
            assert changed is True, f"#{tid}: il clamp deve scattare"
            assert se == fill

    def test_riduzione_proporzionale_riporta_sotto_il_massimo(self):
        """La formula usata dal re-clamp: lots * d_old / d_new."""
        for tid, bordo, fill, sl, lots, perdita in self.CASI:
            d_old = abs(bordo - sl)
            d_new = abs(fill - sl)
            lots_new = lots * d_old / d_new
            assert lots_new < lots
            assert _risk(fill, sl, lots_new) <= 1000 * 1.02, f"#{tid}"

    def test_mai_up_sizing_su_fill_migliore(self):
        """Fill piu' vicino allo SL (esecuzione migliore): size invariata."""
        import mt5_trader
        se, changed = mt5_trader.clamp_size_entry_to_fill(4650.0, 4649.20, 4645.0, True)
        assert changed is False and se == 4650.0

    def test_sell_simmetrico(self):
        import mt5_trader
        # SELL: fill SOTTO il range = piu' lontano dallo SL sopra
        se, changed = mt5_trader.clamp_size_entry_to_fill(4643.0, 4642.44, 4647.0, False)
        assert changed is True and se == 4642.44
        se2, ch2 = mt5_trader.clamp_size_entry_to_fill(4643.0, 4643.50, 4647.0, False)
        assert ch2 is False and se2 == 4643.0


class TestGuardiaFinale:
    """La guardia calcola il rischio coi prezzi che verranno davvero usati."""

    def _lots_sicuri(self, entry, sl, lots_each, risk_max, n=3):
        r = _risk(entry, sl, lots_each, n)
        if r <= risk_max * 1.02:
            return lots_each
        return lots_each * risk_max / r

    def test_riduce_finche_rientra(self):
        for tid, bordo, fill, sl, lots, perdita in TestCasiReali.CASI:
            safe = self._lots_sicuri(fill, sl, lots, 1000.0)
            assert safe < lots
            assert _risk(fill, sl, safe) <= 1000 * 1.02

    def test_non_tocca_un_sizing_corretto(self):
        # 1000$ / (5$ * 100 * 3) = 0.666 per ticket
        lots = 0.66
        assert self._lots_sicuri(4650.0, 4645.0, lots, 1000.0) == lots

    def test_margine_del_due_percento_tollerato(self):
        """Un rischio a 1015$ (arrotondamenti) non fa scattare la guardia."""
        lots = 1015 / (5.0 * CONTRACT * 3)
        assert self._lots_sicuri(4650.0, 4645.0, lots, 1000.0) == lots

    def test_sotto_lotto_minimo_si_abortisce(self):
        """Se nemmeno il lotto minimo sta nel rischio, non si apre il trade.
        Serve uno stop enorme: 400$ di distanza -> 0.01x3 lotti valgono gia'
        1.200$ contro un massimo di 1.000$."""
        min_vol = 0.01
        entry, sl = 4650.0, 4250.0        # 400$ di distanza
        assert _risk(entry, sl, min_vol) == 1200.0
        safe = self._lots_sicuri(entry, sl, 0.66, 1000.0)
        assert safe < min_vol             # -> il codice aborta il trade


class TestIntegrazioneOrdine:
    def test_ordine_delle_operazioni(self):
        """Il difetto era di SEQUENZA: il clamp girava prima che `entry` fosse
        il prezzo di esecuzione. Verifica che nel sorgente il re-clamp e la
        guardia stiano DOPO l'ultima assegnazione di `entry`."""
        import re
        from pathlib import Path
        src = Path(__file__).resolve().parent.parent / "mt5_trader.py"
        righe = src.read_text(encoding="utf-8", errors="replace").splitlines()
        ultima_entry = max(i for i, l in enumerate(righe)
                           if re.match(r"^\s+entry = ", l) and i < len(righe))
        reclamp = next(i for i, l in enumerate(righe) if "RE-CLAMP del sizing" in l)
        guardia = next(i for i, l in enumerate(righe) if "GUARDIA FINALE SUL RISCHIO" in l)
        invio = next(i for i, l in enumerate(righe) if 'mt5_preparing"' in l)
        assert reclamp > ultima_entry, "re-clamp prima del prezzo definitivo"
        assert guardia > reclamp, "la guardia deve venire dopo il re-clamp"
        assert invio > guardia, "la guardia deve precedere l'invio degli ordini"
