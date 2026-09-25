"""#807 (24/09): tre ticket chiusi dallo stop, registrati come "tp1".

Il trader aveva scritto per sbaglio il target 1 a 4352 su una VENDITA partita
da 4258: un numero sopra l'ingresso, cioe' dalla parte della perdita. Il bot
lo aveva corretto a 4252 sui ticket veri, ma il messaggio rimandato dal trader
ha riportato 4352 nella scheda del trade.

Alla chiusura il bot usa due criteri: il motivo dato dal broker (qui "SL" su
tutti e tre) e, di riserva, il confronto fra prezzo di uscita e target. Il
secondo criterio ha ribaltato il primo: uscita a 4265, "sotto" 4352, quindi
target raggiunto. Due difetti in uno: il criterio di riserva non doveva
entrare in gioco con i motivi noti, e un target dalla parte della perdita non
e' un target."""
import pytest


def _chiusi(prezzo=4265.0, n=3):
    # (ticket, prezzo_chiusura, profitto, timestamp)
    return [(186979820 + i, prezzo, -6.65, 1000 + i) for i in range(n)]


def _motivi(motivo="SL", n=3):
    return {186979820 + i: motivo for i in range(n)}


def _tickets(n=3):
    return [186979820 + i for i in range(n)]


class TestTargetDallaParteSbagliata:
    """detect_tp_hits pilota anche l'auto-break-even: un target impossibile
    non deve far credere che il primo obiettivo sia stato preso."""

    def test_vendita_target_sopra_ingresso_ignorato(self, fake_mt5):
        import mt5_trader
        tp1_hit, livelli = mt5_trader.detect_tp_hits(
            _chiusi(), _motivi(), _tickets(),
            4352.0, 4245.0, 4240.0, is_buy=False, ingresso=4258.35)
        assert (tp1_hit, livelli) == (False, 0)

    def test_acquisto_target_sotto_ingresso_ignorato(self, fake_mt5):
        import mt5_trader
        tp1_hit, livelli = mt5_trader.detect_tp_hits(
            _chiusi(prezzo=4240.0), _motivi(), _tickets(),
            4230.0, 4290.0, 4300.0, is_buy=True, ingresso=4258.35)
        assert tp1_hit is False

    def test_target_veri_restano_riconosciuti(self, fake_mt5):
        import mt5_trader
        tp1_hit, livelli = mt5_trader.detect_tp_hits(
            _chiusi(prezzo=4251.0), _motivi(), _tickets(),
            4252.0, 4245.0, 4240.0, is_buy=False, ingresso=4258.35)
        assert tp1_hit is True and livelli == 1

    def test_senza_ingresso_si_comporta_come_prima(self, fake_mt5):
        import mt5_trader
        tp1_hit, _ = mt5_trader.detect_tp_hits(
            _chiusi(prezzo=4251.0), _motivi(), _tickets(),
            4252.0, 4245.0, 4240.0, is_buy=False)
        assert tp1_hit is True


class TestMotivoDelBrokerVince:
    """Il codice dello stato finale: quando il broker dice perche' ha chiuso
    ogni ticket, il confronto sui prezzi non deve poterlo ribaltare."""

    def test_lo_stato_si_legge_dai_ticket(self):
        from pathlib import Path
        src = (Path(__file__).resolve().parent.parent / "mt5_trader.py").read_text(
            encoding="utf-8", errors="replace")
        i = src.index("LO STATO SI LEGGE DAI TICKET")
        blocco = src[i:i + 2600]
        # (a) motivo del broker, (b) bersaglio scritto sull'ordine del ticket
        assert 'closed_reasons.get(_tk) == "TP"' in blocco
        assert "tp_sul_ticket[_tk]" in blocco
        # i target in scheda si usano SOLO senza motivi e senza bersagli dal broker
        i_guardia = blocco.index("elif not motivi_tutti_noti and not tp_sul_ticket:")
        i_scheda = blocco.index("(3, sig.tp3), (2, sig.tp2), (1, sig.tp1)")
        assert i_guardia < i_scheda
        assert "lato_giusto" in blocco      # e comunque niente target in perdita

    def test_il_bersaglio_del_ticket_viene_letto_dal_broker(self):
        from pathlib import Path
        src = (Path(__file__).resolve().parent.parent / "mt5_trader.py").read_text(
            encoding="utf-8", errors="replace")
        assert "tp_sul_ticket[ticket] = float(_o.tp)" in src
        assert "history_orders_get(position=ticket)" in src

    def test_detect_tp_hits_riceve_l_ingresso(self):
        from pathlib import Path
        src = (Path(__file__).resolve().parent.parent / "mt5_trader.py").read_text(
            encoding="utf-8", errors="replace")
        i = src.index("tp1_hit, tp_levels_hit = detect_tp_hits(")
        assert "ingresso=sig.actual_entry_price" in src[i:i + 300]
