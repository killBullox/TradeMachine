"""Fix #656: summarize_closed_deals — somma i deal OUT dopo una chiusura e
segnala se la chiusura e' completa (evita la race che lasciava il pnl in difetto,
mancava +497.74 del ticket chiuso a mano)."""
import pytest
from types import SimpleNamespace as NS

IN, OUT = 0, 1


def _f(*a):
    import mt5_trader
    return mt5_trader.summarize_closed_deals(*a)


def _d(entry, price, profit):
    return NS(entry=entry, price=price, profit=profit)


class TestSummarizeClosedDeals:
    def test_656_completo(self, fake_mt5):
        # #656 reale: 3 ticket BUY, tutti con IN+OUT. Totale 1250.50, complete.
        deals = {
            174442694: [_d(IN, 4337.77, 0.0), _d(OUT, 4344.76, 286.59)],
            174442696: [_d(IN, 4337.77, 0.0), _d(OUT, 4349.14, 466.17)],
            174442699: [_d(IN, 4337.77, 0.0), _d(OUT, 4349.91, 497.74)],
        }
        total, best_tp, complete, entry = _f(deals, 4345.0, 4349.0, 4354.0, True, IN, OUT)
        assert total == 1250.50
        assert complete is True
        assert best_tp == 2  # 4349.14/4349.91 >= tp2 4349
        assert entry == 4337.77

    def test_race_out_mancante_incompleto(self, fake_mt5):
        # Il ticket chiuso a mano ha solo il deal IN (OUT non ancora propagato):
        # complete=False -> il chiamante deve rileggere (retry).
        deals = {
            174442694: [_d(IN, 4337.77, 0.0), _d(OUT, 4344.76, 286.59)],
            174442696: [_d(IN, 4337.77, 0.0), _d(OUT, 4349.14, 466.17)],
            174442699: [_d(IN, 4337.77, 0.0)],  # OUT mancante
        }
        total, best_tp, complete, entry = _f(deals, 4345.0, 4349.0, 4354.0, True, IN, OUT)
        assert complete is False
        assert total == 752.76  # in difetto finche' non arriva l'OUT

    def test_ticket_mai_fillato_non_blocca(self, fake_mt5):
        # Ticket senza deal (pending mai fillato) -> non rende incompleto
        deals = {
            555: [_d(IN, 4000.0, 0.0), _d(OUT, 4010.0, 100.0)],
            556: [],  # mai fillato
        }
        total, best_tp, complete, entry = _f(deals, 4010.0, 4020.0, 4030.0, True, IN, OUT)
        assert complete is True
        assert total == 100.0

    def test_sell_best_tp(self, fake_mt5):
        deals = {
            1: [_d(IN, 4000.0, 0.0), _d(OUT, 3989.5, 200.0)],  # SELL, 3989.5 <= tp2 3990
        }
        total, best_tp, complete, entry = _f(deals, 3995.0, 3990.0, 3985.0, False, IN, OUT)
        assert total == 200.0
        assert best_tp == 2

    def test_vuoto(self, fake_mt5):
        total, best_tp, complete, entry = _f({}, 1.0, 2.0, 3.0, True, IN, OUT)
        assert total == 0.0 and best_tp == 0 and complete is True and entry is None
