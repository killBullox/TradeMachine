"""Fix #609: fix_tp_by_ordering — un TP dal lato sbagliato viene corretto con
un cambio single-digit ancorato all'ordinamento dei target, invece di essere
scartato. Il totale lotti non cambia (solo l'allocazione) -> rischio invariato."""
import pytest


def _f(*a):
    import mt5_trader
    return mt5_trader.fix_tp_by_ordering(*a)


class TestFixTpByOrdering:
    def test_609_tp2_typo_centinaia(self, fake_mt5):
        # #609: BUY entry ~4098, TP1=4101, TP2=4006 (voleva 4106), TP3=4112.
        # unico candidato single-digit tra TP1 e TP3 = 4106.
        tps = {1: 4101.0, 2: 4006.0, 3: 4112.0}
        assert _f(4006.0, 2, tps, 4098.0, True, 2) == 4106.0

    def test_609_niente_neighbor_superiore(self, fake_mt5):
        # TP3 mancante: bordo superiore assente. 4006 -> candidati >4101:
        # 4106,4206,...,4906,5006.. -> ambiguo -> None (scarta, come prima).
        tps = {1: 4101.0, 2: 4006.0, 3: None}
        assert _f(4006.0, 2, tps, 4098.0, True, 2) is None

    def test_sell_tp2_typo(self, fake_mt5):
        # SELL entry ~3902, TP1=3899, TP2=3994 (voleva 3894), TP3=3888.
        # candidati single-digit di 3994 tra TP3 3888 e TP1 3899 = 3894.
        tps = {1: 3899.0, 2: 3994.0, 3: 3888.0}
        assert _f(3994.0, 2, tps, 3902.0, False, 2) == 3894.0

    def test_nessun_candidato_valido_none(self, fake_mt5):
        # TP completamente fuori scala, nessun single-digit cade nell'intervallo.
        tps = {1: 4101.0, 2: 1234.0, 3: 4112.0}
        assert _f(1234.0, 2, tps, 4098.0, True, 2) is None

    def test_tp1_typo_con_bordo_solo_entry_e_next(self, fake_mt5):
        # TP1 dal lato sbagliato, bordo inferiore = entry, superiore = TP2.
        # BUY entry 4098, TP1=4001 (voleva 4101), TP2=4106.
        tps = {1: 4001.0, 2: 4106.0, 3: None}
        assert _f(4001.0, 1, tps, 4098.0, True, 2) == 4101.0

    def test_ambiguo_due_candidati_none(self, fake_mt5):
        # Intervallo largo con due candidati validi -> None (non indovina).
        # BUY entry 4000, TP1=4100 (typo 4X00), TP3=4900 -> candidati 4200..4800
        # multipli tra 4100 e 4900 -> ambiguo. (tp_bad 4100 e' > entry ma lo
        # forziamo come "bad" per testare la disambiguazione dell'intervallo)
        tps = {1: 4100.0, 2: 4001.0, 3: 4900.0}
        # correggo TP2=4001 (sotto entry): candidati tra TP1 4100 e TP3 4900:
        # 4201? no e' 4001->4201 (cambio 0->2 in pos1)=4201, 4301..4801, 4051? no.
        # 4001 -> pos1 0->N: 4101(gia' preso? no e' TP1 val ma non escluso),4201,..4901
        # tra 4100 e 4900: 4201,4301,4401,4501,4601,4701,4801 -> molti -> None
        assert _f(4001.0, 2, tps, 4000.0, True, 2) is None
