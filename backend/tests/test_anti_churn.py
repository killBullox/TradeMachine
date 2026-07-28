"""Fix #613/#616: keep_old_on_rapid_duplicate — un repost identico entro pochi
minuti da una posizione aperta fa TENERE il vecchio (skip nuovo), evitando di
chiudere in perdita e riaprire un trade equivalente."""
import pytest
from datetime import datetime, timedelta


def _f(*a, **k):
    import telegram_client
    return telegram_client.keep_old_on_rapid_duplicate(*a, **k)


class TestKeepOldOnRapidDuplicate:
    def test_613_repost_10s_tieni_vecchio(self, fake_mt5):
        now = datetime(2026, 7, 28, 5, 48, 59)
        opened = datetime(2026, 7, 28, 5, 48, 49)  # 10s prima
        assert _f(opened, now, has_open_pos=True) is True

    def test_616_repost_21s_tieni_vecchio(self, fake_mt5):
        now = datetime(2026, 7, 28, 12, 7, 3)
        opened = datetime(2026, 7, 28, 12, 6, 42)  # 21s prima
        assert _f(opened, now, has_open_pos=True) is True

    def test_oltre_finestra_non_tiene(self, fake_mt5):
        # #446/#447: repost 52min dopo -> fuori finestra, comportamento vecchio
        now = datetime(2026, 7, 28, 12, 52, 0)
        opened = datetime(2026, 7, 28, 12, 0, 0)
        assert _f(opened, now, has_open_pos=True) is False

    def test_nessuna_posizione_aperta_non_tiene(self, fake_mt5):
        # solo pending non fillato: nessun churn da evitare
        now = datetime(2026, 7, 28, 12, 7, 3)
        opened = datetime(2026, 7, 28, 12, 6, 42)
        assert _f(opened, now, has_open_pos=False) is False

    def test_opened_none_non_tiene(self, fake_mt5):
        assert _f(None, datetime(2026, 7, 28, 12, 0, 0), has_open_pos=True) is False

    def test_bordo_finestra_incluso(self, fake_mt5):
        now = datetime(2026, 7, 28, 12, 10, 0)
        opened = datetime(2026, 7, 28, 12, 0, 0)  # esattamente 10min
        assert _f(opened, now, has_open_pos=True) is True

    def test_appena_oltre_finestra_escluso(self, fake_mt5):
        now = datetime(2026, 7, 28, 12, 10, 1)
        opened = datetime(2026, 7, 28, 12, 0, 0)  # 10min 1s
        assert _f(opened, now, has_open_pos=True) is False

    def test_opened_nel_futuro_non_tiene(self, fake_mt5):
        # clock skew: apertura "nel futuro" -> non tenere (age negativo)
        now = datetime(2026, 7, 28, 12, 0, 0)
        opened = datetime(2026, 7, 28, 12, 1, 0)
        assert _f(opened, now, has_open_pos=True) is False
