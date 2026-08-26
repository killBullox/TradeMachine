"""Evento news IN CORSO (26/08): l'API marcava `past=True` allo scoccare
dell'ora e la UI lo nascondeva, quindi l'evento spariva proprio nei minuti in
cui stava bloccando gli ingressi. Ora `past` diventa True solo a finestra di
blocco CONCLUSA, e c'e' un flag `ongoing`.

Test sulla funzione PURA: niente TestClient, che aprirebbe il database reale."""
import pytest
from datetime import datetime, timedelta


T = datetime(2026, 8, 26, 12, 30, 0)      # 14:30 Roma, il Core PCE reale


class TestFinestraEvento:
    def test_caso_reale_evento_appena_uscito(self):
        """Evento delle 14:30, sono le 14:42, blocco fino alle 14:45:
        IN CORSO e NON passato (prima spariva dalla UI)."""
        from news_filter import event_window_state
        w = event_window_state(T, now=T + timedelta(minutes=12))
        assert w["ongoing"] is True
        assert w["past"] is False
        assert w["minuti_alla_fine_blocco"] == 3

    def test_prima_della_finestra(self):
        from news_filter import event_window_state
        w = event_window_state(T, now=T - timedelta(minutes=30))
        assert w["ongoing"] is False and w["past"] is False
        assert w["minuti_alla_fine_blocco"] is None

    def test_il_blocco_parte_dieci_minuti_prima(self):
        from news_filter import event_window_state
        assert event_window_state(T, now=T - timedelta(minutes=11))["ongoing"] is False
        assert event_window_state(T, now=T - timedelta(minutes=9))["ongoing"] is True

    def test_esattamente_all_ora_dell_evento(self):
        from news_filter import event_window_state
        w = event_window_state(T, now=T)
        assert w["ongoing"] is True and w["past"] is False
        assert w["minuti_alla_fine_blocco"] == 15

    def test_al_limite_dei_quindici_minuti(self):
        from news_filter import event_window_state
        assert event_window_state(T, now=T + timedelta(minutes=15))["ongoing"] is True
        assert event_window_state(T, now=T + timedelta(minutes=15, seconds=1))["past"] is True

    def test_a_blocco_finito(self):
        from news_filter import event_window_state
        w = event_window_state(T, now=T + timedelta(minutes=20))
        assert w["ongoing"] is False and w["past"] is True
        assert w["minuti_alla_fine_blocco"] is None

    def test_finestra_coerente_con_le_costanti_del_filtro(self):
        """La finestra mostrata in UI deve essere quella che blocca davvero."""
        import news_filter
        w = news_filter.event_window_state(T, now=T)
        assert w["start_utc"] == T - timedelta(minutes=news_filter.ENTRY_BLOCK_BEFORE_MIN)
        assert w["end_utc"] == T + timedelta(minutes=news_filter.ENTRY_BLOCK_AFTER_MIN)
