"""Replay engine v2: replay tick puro (gestione + ingresso), fidelity check,
validazione con gate statistici, dispatch da sim_engine, enforcement toggle."""
import json
import types
import pytest
from datetime import datetime, timedelta


T0 = datetime(2026, 8, 10, 8, 0, 0)


def _ticks_buy(prices, start=None, step_s=1):
    """Tick sintetici per un BUY: bid=p, ask=p+0.3."""
    start = start or T0
    return [{"t": start + timedelta(seconds=i * step_s),
             "bid": float(p), "ask": float(p) + 0.3}
            for i, p in enumerate(prices)]


def _ticks_sell(prices, start=None, step_s=1):
    """Tick sintetici per un SELL: ask=p, bid=p-0.3."""
    start = start or T0
    return [{"t": start + timedelta(seconds=i * step_s),
             "bid": float(p) - 0.3, "ask": float(p)}
            for i, p in enumerate(prices)]


# lotti: risk 900$, dist SL 6$, 3 TP -> 900/3/(6*100) = 0.5 a ticket
LOTS = 0.5
TPS = [4005.0, 4010.0, 4015.0]
SL = 3994.0
ENTRY = 4000.0


class TestReplayManage:
    def test_baseline_tp1_poi_be(self):
        """TP1 preso, BE+1pip, ritorno -> 2 ticket chiusi a BE+0.1."""
        import replay_engine as rp
        ticks = _ticks_buy([4000, 4003, 4005.2, 4002, 4000.1, 3993.9])
        res = rp.replay_manage(ticks, "buy", ENTRY, SL, TPS, LOTS, rp.BASELINE_MGMT)
        # tp1: 0.5*100*5 = 250; sl@4000.1 x2: 2*0.5*100*0.1 = 10
        assert res["pnl"] == 260.0
        assert any(e.startswith("tp1@") for e in res["events"])
        assert any(e.startswith("be+1pip@") for e in res["events"])

    def test_no_be_stesso_percorso_prende_lo_sl_pieno(self):
        import replay_engine as rp
        ticks = _ticks_buy([4000, 4003, 4005.2, 4002, 4000.1, 3993.9])
        res = rp.replay_manage(ticks, "buy", ENTRY, SL, TPS, LOTS,
                               rp.MGMT_POLICIES["no_be"])
        # tp1 250; SL pieno a 3994 x2: 2*0.5*100*(-6) = -600
        assert res["pnl"] == -350.0

    def test_close_all_tp2(self):
        import replay_engine as rp
        ticks = _ticks_buy([4000, 4005.2, 4010.3, 4015.5])
        base = rp.replay_manage(ticks, "buy", ENTRY, SL, TPS, LOTS, rp.BASELINE_MGMT)
        assert base["pnl"] == 1500.0          # 250 + 500 + 750
        res = rp.replay_manage(ticks, "buy", ENTRY, SL, TPS, LOTS,
                               rp.MGMT_POLICIES["close_all_tp2"])
        assert res["pnl"] == 1250.0           # 250 + 2 ticket chiusi a 4010
        assert any(e.startswith("close_all@4010") for e in res["events"])

    def test_trail_progressivo_protegge_il_tp3(self):
        import replay_engine as rp
        ticks = _ticks_buy([4000, 4005.2, 4010.3, 4005.0])
        base = rp.replay_manage(ticks, "buy", ENTRY, SL, TPS, LOTS, rp.BASELINE_MGMT)
        assert base["pnl"] == 1000.0          # 250+500+ orizzonte a 4005.0 (250)
        res = rp.replay_manage(ticks, "buy", ENTRY, SL, TPS, LOTS,
                               rp.MGMT_POLICIES["trail_progressive"])
        assert res["pnl"] == 1005.0           # tp3 agganciato dal trail a 4005.1
        assert any(e.startswith("trail_tp1+1pip@") for e in res["events"])

    def test_gap_sotto_sl_esce_al_prezzo_dello_stop(self):
        """Niente slippage simulato: SL fillato al livello (dichiarato)."""
        import replay_engine as rp
        ticks = _ticks_buy([4000, 3990.0])
        res = rp.replay_manage(ticks, "buy", ENTRY, SL, TPS, LOTS, rp.BASELINE_MGMT)
        assert res["pnl"] == -900.0           # 3*0.5*100*(3994-4000)

    def test_sell_simmetrico(self):
        import replay_engine as rp
        tps = [3995.0, 3990.0, 3985.0]
        ticks = _ticks_sell([4000, 3994.8, 4000.0])
        res = rp.replay_manage(ticks, "sell", 4000.0, 4006.0, tps, LOTS,
                               rp.BASELINE_MGMT)
        # tp1: 0.5*100*5 = 250; BE a 3999.9, ask 4000 >= 3999.9 -> x2 * 0.1*50
        assert res["pnl"] == 260.0

    def test_orizzonte_chiude_il_residuo(self):
        import replay_engine as rp
        ticks = _ticks_buy([4000, 4002.5])
        res = rp.replay_manage(ticks, "buy", ENTRY, SL, TPS, LOTS, rp.BASELINE_MGMT)
        # nessun TP/SL: 3 ticket chiusi all'ultimo bid 4002.5
        assert res["pnl"] == round(3 * LOTS * 100 * 2.5, 2)
        assert any(e.startswith("horizon@") for e in res["events"])


class TestReplayEntry:
    def test_market_immediate(self):
        import replay_engine as rp
        ticks = _ticks_buy([4002.7, 4005.2, 4010.3, 4015.5])  # ask0 = 4003.0
        res = rp.replay_entry(ticks, "buy", 4000.0, 4001.0, SL, TPS, 900.0,
                              rp.ENTRY_POLICIES["market_immediate"],
                              rp.BASELINE_MGMT)
        assert res["filled"] is True and res["entry"] == 4003.0

    def test_entry_oltre_tp1_non_entra(self):
        import replay_engine as rp
        ticks = _ticks_buy([4005.0, 4010.0])                  # ask0 = 4005.3 >= tp1
        res = rp.replay_entry(ticks, "buy", 4000.0, 4001.0, SL, TPS, 900.0,
                              rp.ENTRY_POLICIES["market_immediate"],
                              rp.BASELINE_MGMT)
        assert res["filled"] is False and "entry_beyond_tp1" in res["events"]
        assert res["pnl"] == 0.0

    def test_mixed_lontano_usa_limit_e_fills_sul_pullback(self):
        import replay_engine as rp
        # ask parte a 4004.5: dist dal bordo 4001 = 3.5 > 2 -> LIMIT
        ticks = _ticks_buy([4004.2, 4001.7, 4000.7, 4005.2, 4010.3, 4015.5])
        res = rp.replay_entry(ticks, "buy", 4000.0, 4001.0, SL, TPS, 900.0,
                              rp.ENTRY_POLICIES["market_if_near_2"],
                              rp.BASELINE_MGMT)
        assert res["filled"] is True and res["entry"] == 4001.0

    def test_mixed_vicino_entra_market(self):
        import replay_engine as rp
        ticks = _ticks_buy([4002.2, 4005.2])                  # ask0 4002.5, dist 1.5 <= 2
        res = rp.replay_entry(ticks, "buy", 4000.0, 4001.0, SL, TPS, 900.0,
                              rp.ENTRY_POLICIES["market_if_near_2"],
                              rp.BASELINE_MGMT)
        assert res["filled"] is True and res["entry"] == 4002.5

    def test_limit_mai_fillato(self):
        import replay_engine as rp
        ticks = _ticks_buy([4004.2, 4004.6, 4005.2])
        res = rp.replay_entry(ticks, "buy", 4000.0, 4001.0, SL, TPS, 900.0,
                              rp.ENTRY_POLICIES["market_if_near_2"],
                              rp.BASELINE_MGMT)
        assert res["filled"] is False and "limit_never_filled" in res["events"]

    def test_cap_lotti_su_entry_vicina_allo_sl(self):
        """Entry alternativa a ridosso dello SL: senza cap i lotti esplodono."""
        import replay_engine as rp
        # entry market a 3994.8 con SL 3994: dist 0.8 -> lots 900/3/80 = 3.75
        ticks = _ticks_buy([3994.5, 4005.2, 4010.3, 4015.5])
        res = rp.replay_entry(ticks, "buy", 4000.0, 4001.0, SL, TPS, 900.0,
                              rp.ENTRY_POLICIES["market_immediate"],
                              rp.BASELINE_MGMT, max_lots_each=1.0)
        assert res["filled"] is True
        # pnl coerente con lots cappati a 1.0 (non 3.75): tp1+tp2+tp3 da 3994.8
        expected = round(sum(1.0 * 100 * (tp - 3994.8) for tp in TPS), 2)
        assert res["pnl"] == expected

    def test_scope_solo_xauusd(self):
        """Forex/BTC fuori scope v1: contract size e pip sono calibrati oro."""
        import replay_engine as rp
        sig = _fake_sig(symbol="GBPUSD")
        assert rp.compute_for_signal(sig) is None


def _fake_sig(**kw):
    base = dict(id=1, symbol="XAUUSD", direction="buy",
                entry_price=4000.0, entry_price_high=4001.0,
                actual_entry_price=4000.0, stoploss=SL,
                tp1=TPS[0], tp2=TPS[1], tp3=TPS[2],
                risk_usd=900.0, position_size=1.5, pnl_usd=260.0,
                created_at=T0 - timedelta(seconds=30),
                entered_at=T0, closed_at=T0 + timedelta(minutes=10))
    base.update(kw)
    return types.SimpleNamespace(**base)


class TestBattery:
    def test_fidelity_ok_quando_replay_coincide(self):
        import replay_engine as rp
        ticks = _ticks_buy([4000, 4003, 4005.2, 4002, 4000.1, 3993.9])
        sig = _fake_sig(pnl_usd=260.0)
        res = rp.run_battery(sig, ticks, ticks)
        assert res["baseline_replay"] == 260.0
        assert res["fidelity"]["ok"] is True and res["fidelity"]["err"] == 0.0
        assert res["mgmt"]["no_be"] == -350.0

    def test_fidelity_ko_quando_diverge(self):
        """Chiusura esogena non ricostruibile -> escluso, MAI silenziato."""
        import replay_engine as rp
        ticks = _ticks_buy([4000, 4003, 4005.2, 4002, 4000.1, 3993.9])
        sig = _fake_sig(pnl_usd=1500.0)     # realta' lontana dal replay (err 1240)
        res = rp.run_battery(sig, ticks, ticks)
        assert res["fidelity"]["ok"] is False


def _seed_replay_rows(db, n=12, contrib=50.0, fid_fail_ids=(), no_replay_ids=()):
    """n trade reali chiusi + righe TradeReplay: no_be = baseline+contrib."""
    from database import Signal, TradeReplay
    import replay_engine as rp
    for i in range(1, n + 1):
        now = datetime(2026, 8, 1, 8, 0) + timedelta(hours=i)
        db.add(Signal(telegram_msg_id=i, symbol="XAUUSD", direction="buy",
                      entry_price=4000.0, entry_price_high=4001.0,
                      actual_entry_price=4000.5, stoploss=3994.0,
                      tp1=4005.0, status="tp1", pnl_usd=100.0, risk_usd=900.0,
                      is_filtered=False, is_archived=False, mt5_tickets="[1]",
                      raw_message="t", created_at=now,
                      entered_at=now + timedelta(seconds=5),
                      closed_at=now + timedelta(minutes=30)))
    db.commit()
    sigs = db.query(Signal).order_by(Signal.id).all()
    for s in sigs:
        if s.id in no_replay_ids:
            continue
        base = 100.0
        res = {"version": rp.BATTERY_VERSION, "baseline_replay": base,
               "actual_pnl": 100.0,
               "fidelity": {"err": 0.0, "tol": 225.0,
                            "ok": s.id not in fid_fail_ids},
               "mgmt": {"no_be": base + contrib, "trail_progressive": base,
                        "close_all_tp1": base - 10.0, "close_all_tp2": base},
               "entry": {"market_immediate": {"pnl": base, "filled": True},
                         "market_if_near_2": {"pnl": base, "filled": True},
                         "market_if_near_5": {"pnl": base, "filled": True}}}
        db.add(TradeReplay(signal_id=s.id, version=rp.BATTERY_VERSION,
                           ticks_ok=True, fidelity_ok=s.id not in fid_fail_ids,
                           results_json=json.dumps(res)))
    db.commit()
    return sigs


class TestValidatePolicy:
    def test_policy_consistente_promossa_con_coverage(self, in_memory_db, fake_mt5):
        import replay_engine as rp
        db = in_memory_db()
        try:
            _seed_replay_rows(db, n=12, contrib=50.0,
                              fid_fail_ids=(3,), no_replay_ids=(7,))
            res = rp.validate_policy("mgmt", {"policy": "no_be"}, db)
            assert res["ok"] is True
            cov = res["coverage"]
            assert cov["trade_reali"] == 12
            assert cov["esclusi_fidelity"] == 1 and cov["senza_replay"] == 1
            assert cov["replay_validi"] == 10
            assert res["delta_pnl"] == 500.0            # 10 x +50
            assert res["validation"]["passed"] is True
            assert res["validation"]["n_affected"] == 10
        finally:
            db.close()

    def test_policy_peggiorativa_bocciata(self, in_memory_db, fake_mt5):
        import replay_engine as rp
        db = in_memory_db()
        try:
            _seed_replay_rows(db, n=12, contrib=50.0)
            res = rp.validate_policy("mgmt", {"policy": "close_all_tp1"}, db)
            assert res["ok"] is True and res["delta_pnl"] == -120.0
            assert res["validation"]["passed"] is False
        finally:
            db.close()

    def test_policy_sconosciuta(self, in_memory_db, fake_mt5):
        import replay_engine as rp
        db = in_memory_db()
        try:
            res = rp.validate_policy("mgmt", {"policy": "boh"}, db)
            assert res["ok"] is False and "sconosciuta" in res["error"]
        finally:
            db.close()

    def test_dispatch_da_sim_engine(self, in_memory_db, fake_mt5):
        import sim_engine
        db = in_memory_db()
        try:
            _seed_replay_rows(db, n=12, contrib=50.0)
            res = sim_engine.validate("mgmt_policy", {"policy": "no_be"}, db)
            assert res["ok"] is True and res["rule"]["type"] == "mgmt_policy"
            assert res["delta_pnl"] == 600.0
            res2 = sim_engine.simulate("entry_policy", {"policy": "market_immediate"}, db)
            assert res2["ok"] is True and res2["rule"]["type"] == "entry_policy"
        finally:
            db.close()

    def test_replay_sweep_shape(self, in_memory_db, fake_mt5):
        import replay_engine as rp
        db = in_memory_db()
        try:
            _seed_replay_rows(db, n=12, contrib=50.0)
            sw = rp.replay_sweep(db)
            promoted = {(e["sim_type"], e["sim_params"]) for e in sw["promosse"]}
            assert ("mgmt_policy", '{"policy": "no_be"}') in promoted
            assert sw["coverage"]["replay_validi"] == 12
            # la promossa mappabile su toggle e' marcata
            e = next(x for x in sw["promosse"] if '"no_be"' in x["sim_params"])
            assert e["toggle_reale"] is True
        finally:
            db.close()


class TestRealEnforcementToggle:
    def _mk_settings(self, db):
        from database import RiskSettings
        rs = RiskSettings(be_at_tp1_enabled=True, trail_stop_enabled=False)
        db.add(rs); db.commit()
        return rs

    def test_entry_policy_real_rifiutata(self, in_memory_db, fake_mt5):
        import advisor_rules as ar
        db = in_memory_db()
        try:
            res = ar.create_rule("entry_policy", {"policy": "market_immediate"},
                                 "Market subito", "real", db=db)
            assert res["ok"] is False and "Monitor Test" in res["error"]
            # in test invece va
            res2 = ar.create_rule("entry_policy", {"policy": "market_immediate"},
                                  "Market subito", "test", db=db)
            assert res2["ok"] is True
        finally:
            db.close()

    def test_mgmt_senza_toggle_real_rifiutata(self, in_memory_db, fake_mt5):
        import advisor_rules as ar
        db = in_memory_db()
        try:
            res = ar.create_rule("mgmt_policy", {"policy": "close_all_tp2"},
                                 "Chiudi a TP2", "real", db=db)
            assert res["ok"] is False and "Monitor Test" in res["error"]
        finally:
            db.close()

    def test_no_be_real_flips_toggle_e_rollback_ripristina(self, in_memory_db, fake_mt5):
        import advisor_rules as ar
        from database import RiskSettings
        db = in_memory_db()
        try:
            self._mk_settings(db)
            res = ar.create_rule("mgmt_policy", {"policy": "no_be"},
                                 "Niente BE a TP1", "real", db=db)
            assert res["ok"] is True
            assert db.query(RiskSettings).first().be_at_tp1_enabled is False
            ar.rollback_rule(res["rule"]["id"], db)
            assert db.query(RiskSettings).first().be_at_tp1_enabled is True
        finally:
            db.close()

    def test_promote_trail_attiva_toggle(self, in_memory_db, fake_mt5):
        import advisor_rules as ar
        from database import RiskSettings
        db = in_memory_db()
        try:
            self._mk_settings(db)
            res = ar.create_rule("mgmt_policy", {"policy": "trail_progressive"},
                                 "Trail progressivo", "test", db=db)
            assert res["ok"] is True
            assert db.query(RiskSettings).first().trail_stop_enabled is False
            pro = ar.promote_rule(res["rule"]["id"], db)
            assert pro["ok"] is True
            assert db.query(RiskSettings).first().trail_stop_enabled is True
            ar.rollback_rule(res["rule"]["id"], db)
            assert db.query(RiskSettings).first().trail_stop_enabled is False
        finally:
            db.close()


class TestNoTp1Policies:
    """Policy 'niente TP1': il terzo destinato a 1.25R va piu' lontano.
    Rischio totale invariato (stessi 3 ticket, stessi lotti)."""

    def test_riallocazione_su_tp2_quando_il_prezzo_arriva(self):
        import replay_engine as rp
        # sale fino a TP2 e poi torna: baseline prende TP1+TP2 e BE sul resto,
        # la variante prende DUE volte TP2
        ticks = _ticks_buy([4000, 4005.2, 4010.3, 4000.1])
        base = rp.replay_manage(ticks, "buy", ENTRY, SL, TPS, LOTS, rp.BASELINE_MGMT)
        alt = rp.replay_manage(ticks, "buy", ENTRY, SL, TPS, LOTS,
                               rp.MGMT_POLICIES["no_tp1_to_tp2"])
        # baseline: tp1 250 + tp2 500 + BE 5 = 755
        assert base["pnl"] == 755.0
        # variante: tp2 x2 = 1000 + BE 5 = 1005
        assert alt["pnl"] == 1005.0

    def test_costa_quando_il_prezzo_non_arriva_a_tp2(self):
        """Il rovescio della medaglia: se tocca TP1 e torna indietro, la
        variante rinuncia all'incasso di TP1."""
        import replay_engine as rp
        ticks = _ticks_buy([4000, 4005.2, 4002, 4000.1])
        base = rp.replay_manage(ticks, "buy", ENTRY, SL, TPS, LOTS, rp.BASELINE_MGMT)
        alt = rp.replay_manage(ticks, "buy", ENTRY, SL, TPS, LOTS,
                               rp.MGMT_POLICIES["no_tp1_to_tp2"])
        assert base["pnl"] > alt["pnl"]      # qui il TP1 protegge
        assert base["pnl"] == 260.0          # 250 + BE su 2 ticket
        assert alt["pnl"] == 15.0            # solo BE su 3 ticket

    def test_be_scatta_al_tocco_del_livello_anche_senza_ticket_tp1(self):
        import replay_engine as rp
        ticks = _ticks_buy([4000, 4005.2, 3993.9])
        alt = rp.replay_manage(ticks, "buy", ENTRY, SL, TPS, LOTS,
                               rp.MGMT_POLICIES["no_tp1_to_tp2"])
        assert any(e.startswith("be+1pip@") for e in alt["events"])
        assert alt["pnl"] == 15.0            # 3 ticket a BE+1pip, NON lo SL pieno

    def test_no_tp1_no_be_prende_lo_sl_pieno(self):
        import replay_engine as rp
        ticks = _ticks_buy([4000, 4005.2, 3993.9])
        alt = rp.replay_manage(ticks, "buy", ENTRY, SL, TPS, LOTS,
                               rp.MGMT_POLICIES["no_tp1_no_be"])
        assert alt["pnl"] == -900.0
        assert not any(e.startswith("be+1pip@") for e in alt["events"])

    def test_no_tp1_to_tp3(self):
        import replay_engine as rp
        ticks = _ticks_buy([4000, 4005.2, 4010.3, 4015.5])
        alt = rp.replay_manage(ticks, "buy", ENTRY, SL, TPS, LOTS,
                               rp.MGMT_POLICIES["no_tp1_to_tp3"])
        # tp3 x2 (750x2) + tp2 (500) = 2000
        assert alt["pnl"] == 2000.0

    def test_rischio_totale_invariato(self):
        """Garanzia: le varianti NON aumentano il rischio (stesso SL pieno)."""
        import replay_engine as rp
        ticks = _ticks_buy([4000, 3990.0])
        base = rp.replay_manage(ticks, "buy", ENTRY, SL, TPS, LOTS, rp.BASELINE_MGMT)
        for name in ("no_tp1_to_tp2", "no_tp1_to_tp3", "no_tp1_no_be"):
            alt = rp.replay_manage(ticks, "buy", ENTRY, SL, TPS, LOTS,
                                   rp.MGMT_POLICIES[name])
            assert alt["pnl"] == base["pnl"] == -900.0, name


class TestDoppioGateFedelta:
    """Una policy che 'funziona' solo sui trade dove il replay diverge dal
    reale e' rumore: deve essere bocciata anche se supera i gate classici."""

    def _seed(self, db, n_good=12, contrib_good=60.0, n_noisy=12, contrib_noisy=400.0):
        from database import Signal, TradeReplay
        import replay_engine as rp
        from datetime import datetime, timedelta
        import json as _j
        for i in range(n_good + n_noisy):
            now = datetime(2026, 8, 1, 8, 0) + timedelta(hours=i)
            db.add(Signal(telegram_msg_id=i + 1, symbol="XAUUSD", direction="buy",
                          entry_price=4000.0, entry_price_high=4001.0,
                          actual_entry_price=4000.5, stoploss=3994.0, tp1=4005.0,
                          status="tp1", pnl_usd=100.0, risk_usd=900.0,
                          is_filtered=False, is_archived=False, mt5_tickets="[1]",
                          raw_message="t", created_at=now,
                          entered_at=now + timedelta(seconds=5),
                          closed_at=now + timedelta(minutes=30)))
        db.commit()
        sigs = db.query(Signal).order_by(Signal.id).all()
        for k, s in enumerate(sigs):
            noisy = k >= n_good
            # err entro tolleranza (225$) ma FUORI dal sottoinsieme stretto (50$)
            err = 200.0 if noisy else 5.0
            c = contrib_noisy if noisy else contrib_good
            res = {"version": rp.BATTERY_VERSION, "baseline_replay": 100.0,
                   "actual_pnl": 100.0,
                   "fidelity": {"err": err, "tol": 225.0, "ok": True},
                   "mgmt": {"no_be": 100.0 + c, "trail_progressive": 100.0,
                            "close_all_tp1": 100.0, "close_all_tp2": 100.0},
                   "entry": {}}
            db.add(TradeReplay(signal_id=s.id, version=rp.BATTERY_VERSION,
                               ticks_ok=True, fidelity_ok=True,
                               results_json=_j.dumps(res)))
        db.commit()

    def test_policy_confermata_sul_pulito_passa(self, in_memory_db, fake_mt5):
        import replay_engine as rp
        db = in_memory_db()
        try:
            self._seed(db, n_good=12, contrib_good=60.0, n_noisy=0)
            res = rp.validate_policy("mgmt", {"policy": "no_be"}, db)
            v = res["validation"]
            assert v["passed"] is True
            assert v["alta_fedelta"]["conferma"] is True
            assert v["alta_fedelta"]["trade"] == 12
        finally:
            db.close()

    def test_policy_solo_sui_rumorosi_bocciata(self, in_memory_db, fake_mt5):
        """Delta grande sul totale ma NEGATIVO sui trade ad alta fedelta'."""
        import replay_engine as rp
        db = in_memory_db()
        try:
            self._seed(db, n_good=12, contrib_good=-20.0, n_noisy=12,
                       contrib_noisy=400.0)
            res = rp.validate_policy("mgmt", {"policy": "no_be"}, db)
            v = res["validation"]
            assert res["delta_pnl"] > 0          # sul totale sembra ottima
            assert v["passed"] is False          # ma e' rumore di replay
            assert v["alta_fedelta"]["conferma"] is False
            assert any("alta fedelta" in r for r in v["fail_reasons"])
        finally:
            db.close()

    def test_policy_che_dipende_da_un_solo_trade_pulito_bocciata(self, in_memory_db, fake_mt5):
        import replay_engine as rp
        from database import TradeReplay
        import json as _j
        db = in_memory_db()
        try:
            self._seed(db, n_good=12, contrib_good=-10.0, n_noisy=12, contrib_noisy=400.0)
            # un solo trade pulito con contributo enorme
            row = db.query(TradeReplay).first()
            res = _j.loads(row.results_json)
            res["mgmt"]["no_be"] = 100.0 + 500.0
            row.results_json = _j.dumps(res); db.commit()
            out = rp.validate_policy("mgmt", {"policy": "no_be"}, db)
            v = out["validation"]
            assert v["alta_fedelta"]["delta"] > 0
            assert v["alta_fedelta"]["delta_senza_top1"] < 0   # regge su 1 solo
            assert v["passed"] is False
        finally:
            db.close()
