"""Replay engine v2 — rigioca i trade reali sui TICK veri di MT5.

Sblocca la categoria di consigli prima dichiarata "non simulabile": la
GESTIONE del trade (BE a TP1 si'/no, trail progressivo, chiusure anticipate)
e la POLICY D'INGRESSO (LIMIT attuale vs MARKET immediato vs mista), perche'
per quantificarle serve il percorso del prezzo DENTRO il trade, non solo
l'esito finale.

Architettura (stessa filosofia di sim_engine: numeri esatti + gate statistici):
  1. fetch_ticks(): tick bid/ask dal terminale MT5 (offset server centralizzato
     in mt5_time). Verificato: storico tick FTMO copre TUTTI i trade reali.
  2. replay_manage(): funzione PURA che rigioca un trade (3 ticket TP1/2/3,
     SL comune) sotto una policy di gestione parametrica.
  3. FIDELITY CHECK (onesta' prima di tutto): per ogni trade si rigioca anche
     la gestione CORRENTE del sistema (BE a TP1 ON, trail OFF) e la si
     confronta col P&L reale registrato. I trade dove il replay diverge oltre
     tolleranza sono ESCLUSI dagli aggregati e CONTATI (mai silenziati):
     chiusure manuali/da messaggio TG a meta' vita sono gestite chiudendo il
     residuo del replay all'orario di chiusura reale (orizzonte esogeno).
  4. Delta di una policy = replay(policy) - replay(gestione corrente), MAI
     replay(policy) - P&L reale: cosi' il bias di replay (spread, slippage,
     ordine dei tick) si cancella nel confronto.
  5. Cache su DB (trade_replays), incrementale come i contesti ICT: batch al
     report + hook alla chiusura di ogni trade nuovo.
  6. validate_policy(): stessi gate statistici di sim_engine (campione minimo,
     robustezza top-1, bootstrap deterministico).

Approssimazioni dichiarate (v1): TP fillato al prezzo esatto del livello,
SL al prezzo dello stop (niente slippage simulato), nell'ambiguita' intra-tick
vince lo scenario PEGGIORE (SL prima dei TP). BE = +1 pip come il sistema vivo.
"""
import json
from datetime import datetime, timedelta
from typing import Optional

BATTERY_VERSION = "v1"
CONTRACT_SIZE = 100.0        # XAUUSD: 100 oz per lotto
PIP = 0.1                    # 1 pip oro (allineato al BE+1pip del sistema)
EPS = 0.01                   # contributo minimo per contare un trade "toccato"
MAX_HOLD_HOURS = 48          # oltre: replay skippato (dichiarato in coverage)
FIDELITY_TOL_USD = 150.0     # tolleranza fissa fidelity...
FIDELITY_TOL_RISK = 0.25     # ...oppure 25% del rischio, il maggiore dei due

# Policy di GESTIONE (stessa entry reale, gestione alternativa).
# baseline = gestione corrente del sistema: BE a TP1 ON, trail OFF.
BASELINE_MGMT = {"be_at_tp1": True, "trail_progressive": False, "close_all_at": None}
MGMT_POLICIES = {
    "no_be":             {"be_at_tp1": False, "trail_progressive": False, "close_all_at": None},
    "trail_progressive": {"be_at_tp1": True,  "trail_progressive": True,  "close_all_at": None},
    "close_all_tp1":     {"be_at_tp1": True,  "trail_progressive": False, "close_all_at": "tp1"},
    "close_all_tp2":     {"be_at_tp1": True,  "trail_progressive": False, "close_all_at": "tp2"},
}
# Policy mappabili su toggle reali del sistema (Approva = flip impostazione):
MGMT_REAL_TOGGLES = {
    "no_be":             {"field": "be_at_tp1_enabled", "value": False, "restore": True},
    "trail_progressive": {"field": "trail_stop_enabled", "value": True, "restore": False},
}

# Policy d'INGRESSO (entry alternativa dal momento del segnale, gestione baseline).
ENTRY_POLICIES = {
    "market_immediate":  {"mode": "market", "near_usd": None},
    "market_if_near_2":  {"mode": "mixed", "near_usd": 2.0},
    "market_if_near_5":  {"mode": "mixed", "near_usd": 5.0},
}


def _log(msg: str):
    try:
        from mt5_trader import log as _l
        _l(f"[Replay] {msg}")
    except Exception:
        print(f"[Replay] {msg}", flush=True)


# ─── 1. Fetch tick ────────────────────────────────────────────────────────────

def _get_mt5():
    try:
        import mt5_trader
        if not mt5_trader.is_enabled():
            return None
        return mt5_trader._get_mt5()
    except Exception:
        return None


def fetch_ticks(symbol: str, utc_from, utc_to, attempts: int = 3):
    """Tick [{t(utc), bid, ask}] via MT5. Offset server centralizzato (mt5_time).
    Lo storico tick viene scaricato ON-DEMAND dal terminale: la prima richiesta
    su un range vecchio puo' tornare vuota mentre il download parte -> retry
    con pausa. Ritorna [] su qualsiasi problema."""
    mt5 = _get_mt5()
    if mt5 is None:
        return []
    try:
        import time as _time
        from mt5_time import detect_mt5_server_offset, mt5_epoch_to_utc
        off = detect_mt5_server_offset(symbol)
        srv_from = utc_from + timedelta(seconds=off)
        srv_to = utc_to + timedelta(seconds=off)
        raw = None
        for att in range(attempts):
            raw = mt5.copy_ticks_range(symbol, srv_from, srv_to, mt5.COPY_TICKS_ALL)
            if raw is not None and len(raw) >= 20:
                break
            _time.sleep(1.5)      # il terminale sta scaricando lo storico
        if raw is None:
            return []
        out = []
        last_bid = last_ask = None
        for r in raw:
            bid = float(r[1]) or (last_bid or 0.0)
            ask = float(r[2]) or (last_ask or bid)
            if bid <= 0:
                continue
            if ask <= 0:
                ask = bid
            last_bid, last_ask = bid, ask
            out.append({"t": mt5_epoch_to_utc(int(r[0])), "bid": bid, "ask": ask})
        return out
    except Exception as e:
        _log(f"fetch_ticks err {symbol}: {str(e)[:100]}")
        return []


# ─── 2. Replay puro ──────────────────────────────────────────────────────────

def _lots_each(risk_usd: float, entry: float, sl: float, n_tp: int) -> float:
    dist = abs(entry - sl)
    if dist <= 0 or n_tp <= 0:
        return 0.0
    return max(0.01, round(risk_usd / n_tp / (dist * CONTRACT_SIZE), 2))


def replay_manage(ticks, direction: str, entry_price: float, sl: float,
                  tps, lots_each: float, mgmt: dict, entry_from_idx: int = 0):
    """Rigioca la GESTIONE di una posizione gia' aperta a entry_price.
    ticks: lista [{t, bid, ask}] dall'apertura all'orizzonte (chiusura reale).
    tps: lista dei TP definiti (1..3). Ritorna {pnl, events, exits}.
    Convenzioni: buy esce sul bid, sell sull'ask. Nell'ambiguita' intra-tick
    vince lo scenario peggiore: lo SL si controlla PRIMA dei TP."""
    is_buy = (direction or "").lower() == "buy"
    sign = 1.0 if is_buy else -1.0
    cur_sl = float(sl)
    open_tk = [{"tp": float(tp), "idx": i} for i, tp in enumerate(tps) if tp]
    n0 = len(open_tk)
    if n0 == 0 or lots_each <= 0:
        return {"pnl": 0.0, "events": ["no_tickets"], "exits": []}
    exits = []      # (prezzo_uscita, n_ticket)
    events = []
    close_all_level = None
    if mgmt.get("close_all_at") == "tp1" and len(tps) >= 1 and tps[0]:
        close_all_level = float(tps[0])
    elif mgmt.get("close_all_at") == "tp2" and len(tps) >= 2 and tps[1]:
        close_all_level = float(tps[1])

    def px(tick):
        return tick["bid"] if is_buy else tick["ask"]

    tp1_val = float(tps[0]) if tps and tps[0] else None
    i = entry_from_idx
    while i < len(ticks) and open_tk:
        p = px(ticks[i])
        # 1. SL (scenario peggiore prima)
        if (is_buy and p <= cur_sl) or (not is_buy and p >= cur_sl):
            exits.append((cur_sl, len(open_tk)))
            events.append(f"sl@{round(cur_sl, 2)}x{len(open_tk)}")
            open_tk = []
            break
        # 2. chiusura anticipata totale
        if close_all_level is not None and (
                (is_buy and p >= close_all_level) or (not is_buy and p <= close_all_level)):
            exits.append((close_all_level, len(open_tk)))
            events.append(f"close_all@{round(close_all_level, 2)}x{len(open_tk)}")
            open_tk = []
            break
        # 3. TP individuali (dal piu' vicino)
        hit = [tk for tk in open_tk
               if (is_buy and p >= tk["tp"]) or (not is_buy and p <= tk["tp"])]
        for tk in hit:
            exits.append((tk["tp"], 1))
            events.append(f"tp{tk['idx'] + 1}@{round(tk['tp'], 2)}")
            open_tk.remove(tk)
            # gestione post-TP
            if tk["idx"] == 0 and mgmt.get("be_at_tp1"):
                cur_sl = entry_price + sign * PIP
                events.append(f"be+1pip@{round(cur_sl, 2)}")
            if tk["idx"] == 1 and mgmt.get("trail_progressive") and tp1_val:
                cur_sl = tp1_val + sign * PIP
                events.append(f"trail_tp1+1pip@{round(cur_sl, 2)}")
        i += 1
    # orizzonte raggiunto: residuo chiuso all'ultimo tick (chiusura esogena reale)
    if open_tk and ticks:
        p = px(ticks[-1])
        exits.append((p, len(open_tk)))
        events.append(f"horizon@{round(p, 2)}x{len(open_tk)}")
    pnl = sum(sign * (xp - entry_price) * CONTRACT_SIZE * lots_each * cnt
              for xp, cnt in exits)
    return {"pnl": round(pnl, 2), "events": events, "exits": exits}


def replay_entry(ticks, direction: str, range_lo: float, range_hi: float,
                 sl: float, tps, risk_usd: float, policy: dict, mgmt: dict,
                 max_lots_each: Optional[float] = None):
    """Rigioca l'INGRESSO alternativo dal momento del segnale (ticks partono
    da created_at) e poi la gestione baseline. LIMIT al bordo vicino del range
    (comportamento reale del bot); MARKET al primo tick. mixed: MARKET se il
    prezzo e' entro near_usd dal bordo, altrimenti LIMIT.
    Ritorna {pnl, filled, entry, events}."""
    if not ticks:
        return {"pnl": 0.0, "filled": False, "entry": None, "events": ["no_ticks"]}
    is_buy = (direction or "").lower() == "buy"
    lo, hi = min(range_lo, range_hi), max(range_lo, range_hi)
    near_edge = hi if is_buy else lo          # bordo vicino al mercato
    first = ticks[0]
    mkt0 = first["ask"] if is_buy else first["bid"]

    mode = policy.get("mode")
    use_market = (mode == "market")
    if mode == "mixed":
        dist = (mkt0 - near_edge) if is_buy else (near_edge - mkt0)
        use_market = dist <= float(policy.get("near_usd") or 0)

    entry_idx = None
    entry_price = None
    if use_market:
        entry_idx = 0
        entry_price = mkt0
    else:
        # LIMIT al bordo vicino: fill quando il prezzo torna sul livello
        for i, tk in enumerate(ticks):
            p = tk["ask"] if is_buy else tk["bid"]
            if (is_buy and p <= near_edge) or (not is_buy and p >= near_edge):
                entry_idx = i
                entry_price = near_edge
                break
        if entry_idx is None:
            return {"pnl": 0.0, "filled": False, "entry": None, "events": ["limit_never_filled"]}

    # sanity: entry oltre il TP piu' vicino = trade non sensato -> non entrare
    if tps and tps[0]:
        if (is_buy and entry_price >= float(tps[0])) or \
           (not is_buy and entry_price <= float(tps[0])):
            return {"pnl": 0.0, "filled": False, "entry": round(entry_price, 2),
                    "events": ["entry_beyond_tp1"]}
    n_tp = len([tp for tp in tps if tp])
    lots = _lots_each(risk_usd, entry_price, sl, n_tp)
    if max_lots_each:
        # cap prudenziale: un'entry alternativa vicinissima allo SL non deve
        # gonfiare la size oltre ogni realismo (margine/volume massimo broker)
        lots = min(lots, max_lots_each)
    res = replay_manage(ticks, direction, entry_price, sl, tps, lots, mgmt,
                        entry_from_idx=entry_idx)
    return {"pnl": res["pnl"], "filled": True, "entry": round(entry_price, 2),
            "events": res["events"]}


# ─── 3. Batteria per trade + cache ───────────────────────────────────────────

def _tps_of(sig):
    return [sig.tp1, sig.tp2, sig.tp3]


def run_battery(sig, ticks_entry, ticks_signal) -> Optional[dict]:
    """Esegue la batteria completa su un trade: fidelity (baseline vs reale),
    policy di gestione, policy d'ingresso. ticks_entry parte da entered_at,
    ticks_signal da created_at (per le policy d'ingresso)."""
    try:
        tps = _tps_of(sig)
        n_tp = len([tp for tp in tps if tp])
        if n_tp == 0 or not sig.stoploss or not sig.actual_entry_price:
            return None
        risk = float(sig.risk_usd) if sig.risk_usd else 1000.0
        # lots reali se noti, altrimenti ricostruiti dalla formula del sizing
        if sig.position_size:
            lots = round(float(sig.position_size) / n_tp, 2)
        else:
            lots = _lots_each(risk, float(sig.actual_entry_price),
                              float(sig.stoploss), n_tp)
        entry = float(sig.actual_entry_price)
        sl = float(sig.stoploss)

        base = replay_manage(ticks_entry, sig.direction, entry, sl, tps, lots,
                             BASELINE_MGMT)
        actual = float(sig.pnl_usd)
        err = round(base["pnl"] - actual, 2)
        tol = max(FIDELITY_TOL_USD, FIDELITY_TOL_RISK * risk)
        fid_ok = abs(err) <= tol

        mgmt_res = {}
        for name, pol in MGMT_POLICIES.items():
            r = replay_manage(ticks_entry, sig.direction, entry, sl, tps, lots, pol)
            mgmt_res[name] = r["pnl"]

        entry_res = {}
        lo = float(sig.entry_price) if sig.entry_price else entry
        hi = float(sig.entry_price_high) if sig.entry_price_high else lo
        for name, pol in ENTRY_POLICIES.items():
            r = replay_entry(ticks_signal, sig.direction, lo, hi, sl, tps,
                             risk, pol, BASELINE_MGMT,
                             max_lots_each=round(lots * 3, 2))
            entry_res[name] = {"pnl": r["pnl"], "filled": r["filled"]}

        return {
            "version": BATTERY_VERSION,
            "baseline_replay": base["pnl"],
            "actual_pnl": round(actual, 2),
            "fidelity": {"err": err, "tol": round(tol, 2), "ok": fid_ok},
            "mgmt": mgmt_res,
            "entry": entry_res,
        }
    except Exception as e:
        _log(f"battery err #{getattr(sig, 'id', '?')}: {str(e)[:120]}")
        return None


def compute_for_signal(sig) -> Optional[dict]:
    """Fetch tick + batteria per un trade reale chiuso. None se non replayabile.
    SCOPE v1: solo XAUUSD — pip, contract size e sizing sono calibrati oro;
    replayare forex/BTC con questi parametri produce numeri privi di senso
    (visto sul campo: GBPUSD con contract oro -> lotti x1000)."""
    if (sig.symbol or "XAUUSD") != "XAUUSD":
        return None
    if not (sig.entered_at and sig.closed_at and sig.created_at):
        return None
    hold_h = (sig.closed_at - sig.entered_at).total_seconds() / 3600.0
    if hold_h > MAX_HOLD_HOURS or hold_h < 0:
        return None
    t_from = min(sig.created_at, sig.entered_at) - timedelta(seconds=30)
    t_to = sig.closed_at + timedelta(seconds=30)
    ticks = fetch_ticks(sig.symbol or "XAUUSD", t_from, t_to)
    if len(ticks) < 20:
        return None
    ticks_entry = [tk for tk in ticks if tk["t"] >= sig.entered_at]
    ticks_signal = [tk for tk in ticks if tk["t"] >= sig.created_at]
    if len(ticks_entry) < 5:
        return None
    return run_battery(sig, ticks_entry, ticks_signal)


def save_replay_if_missing(db, sig) -> bool:
    """Hook alla chiusura del trade (sync_positions) + batch: calcola e salva
    il replay se manca o se la versione batteria e' cambiata. Mai solleva."""
    try:
        from database import TradeReplay
        row = db.query(TradeReplay).filter(TradeReplay.signal_id == sig.id).first()
        if row is not None and row.version == BATTERY_VERSION and row.ticks_ok:
            return False   # gia' calcolato; ticks_ok=False resta ritentabile
                           # (lo storico tick MT5 arriva on-demand)
        res = compute_for_signal(sig)
        if row is None:
            row = TradeReplay(signal_id=sig.id)
            db.add(row)
        row.version = BATTERY_VERSION
        row.ticks_ok = res is not None
        row.fidelity_ok = bool(res and res["fidelity"]["ok"])
        row.results_json = json.dumps(res, ensure_ascii=False, default=str) if res else None
        row.computed_at = datetime.utcnow()
        db.commit()
        return True
    except Exception as e:
        _log(f"save_replay_if_missing #{getattr(sig, 'id', '?')}: {str(e)[:120]}")
        try:
            db.rollback()
        except Exception:
            pass
        return False


def ensure_replays(db=None, max_new: int = 300) -> dict:
    """Backfill incrementale: replay per i trade reali chiusi che non lo hanno
    (o con versione batteria vecchia). Chiamato al report giornaliero."""
    from database import SessionLocal, TradeReplay
    from ai_advisor import _real_closed_trades
    close = False
    if db is None:
        db = SessionLocal(); close = True
    try:
        rows = {r.signal_id: r for r in db.query(TradeReplay).all()}

        def _needs(t):
            r = rows.get(t.id)
            if r is None or r.version != BATTERY_VERSION:
                return True
            # retry dei soli XAUUSD senza tick: lo storico arriva on-demand
            return (not r.ticks_ok) and (t.symbol or "XAUUSD") == "XAUUSD"

        todo = [t for t in _real_closed_trades(db) if _needs(t)]
        done = 0
        for sig in todo[:max_new]:
            if save_replay_if_missing(db, sig):
                done += 1
        if done:
            _log(f"ensure_replays: {done} replay calcolati ({len(todo)} da fare)")
        return {"computed": done, "pending": max(0, len(todo) - done)}
    except Exception as e:
        _log(f"ensure_replays err: {str(e)[:150]}")
        return {"computed": 0, "error": str(e)[:150]}
    finally:
        if close:
            db.close()


# ─── 4. Aggregazione + validazione (stessi gate di sim_engine) ───────────────

def _covered_rows(db, since: Optional[str] = None):
    """(trade, results) per i trade con replay valido e fidelity ok +
    coverage onesta (quanti trade sono fuori e perche')."""
    from database import TradeReplay
    import sim_engine
    from ai_advisor import _real_closed_trades
    trades = sim_engine._filter_since(_real_closed_trades(db), since)
    replays = {r.signal_id: r for r in db.query(TradeReplay)
               .filter(TradeReplay.version == BATTERY_VERSION).all()}
    rows, no_replay, no_ticks, low_fidelity, off_scope = [], 0, 0, 0, 0
    errs = []
    for t in trades:
        if (t.symbol or "XAUUSD") != "XAUUSD":
            off_scope += 1        # scope v1: replay calibrato solo oro
            continue
        r = replays.get(t.id)
        if r is None:
            no_replay += 1
            continue
        if not r.ticks_ok or not r.results_json:
            no_ticks += 1
            continue
        res = json.loads(r.results_json)
        errs.append(abs(res["fidelity"]["err"]))
        if not r.fidelity_ok:
            low_fidelity += 1
            continue
        rows.append((t, res))
    errs.sort()
    coverage = {
        "trade_reali": len(trades),
        "fuori_scope_simbolo": off_scope,
        "replay_validi": len(rows),
        "senza_replay": no_replay,
        "tick_mancanti_o_skip": no_ticks,
        "esclusi_fidelity": low_fidelity,
        "fidelity_median_abs_err": round(errs[len(errs) // 2], 2) if errs else None,
    }
    return rows, coverage


def _policy_contrib(res: dict, kind: str, policy: str) -> Optional[float]:
    """Contributo del trade al delta della policy: policy - baseline replay
    (il bias di replay si cancella nel confronto)."""
    base = res.get("baseline_replay")
    if base is None:
        return None
    if kind == "mgmt":
        p = res.get("mgmt", {}).get(policy)
        return None if p is None else round(p - base, 2)
    if kind == "entry":
        e = res.get("entry", {}).get(policy)
        if e is None:
            return None
        return round(float(e["pnl"]) - base, 2)
    return None


def validate_policy(kind: str, params: dict, db=None,
                    since: Optional[str] = None) -> dict:
    """Valida una policy di gestione/ingresso con gli STESSI gate statistici
    di sim_engine.validate. Shape del risultato compatibile (impact/monitor).
    kind: 'mgmt' | 'entry'; params: {"policy": "<nome>"}. Mai solleva."""
    from database import SessionLocal
    import sim_engine
    close = False
    if db is None:
        db = SessionLocal(); close = True
    try:
        policy = str((params or {}).get("policy") or "")
        valid = MGMT_POLICIES if kind == "mgmt" else ENTRY_POLICIES
        if policy not in valid:
            return {"ok": False,
                    "error": f"policy '{policy}' sconosciuta ({kind}); valide: {sorted(valid)}"}
        rows, coverage = _covered_rows(db, since)
        contribs, base_pnls, sim_pnls = [], [], []
        touched = 0
        for t, res in rows:
            c = _policy_contrib(res, kind, policy)
            if c is None:
                continue
            contribs.append(c)
            base_pnls.append(res["baseline_replay"])
            sim_pnls.append(round(res["baseline_replay"] + c, 2))
            if abs(c) > EPS:
                touched += 1
        delta = round(sum(contribs), 2)
        baseline = sim_engine._stats(base_pnls)
        simulated = sim_engine._stats(sim_pnls)

        reasons = []
        if delta <= 0:
            reasons.append(f"non migliora il risultato (delta {delta}$)")
        sample_ok = touched >= sim_engine.MIN_SAMPLE
        if not sample_ok:
            reasons.append(f"campione insufficiente ({touched} trade toccati, "
                           f"minimo {sim_engine.MIN_SAMPLE})")
        top1 = max(contribs) if contribs else 0.0
        top1_share = round(top1 / delta, 3) if delta > 0 and top1 > 0 else None
        delta_no_top1 = round(delta - top1, 2)
        robust_ok = True
        if delta > 0:
            if delta_no_top1 <= 0:
                robust_ok = False
                reasons.append(f"fragile: senza il miglior singolo trade il delta "
                               f"crolla a {delta_no_top1}$")
            elif top1_share is not None and top1_share > sim_engine.TOP1_SHARE_MAX:
                robust_ok = False
                reasons.append(f"concentrato: il singolo miglior trade vale il "
                               f"{round(top1_share * 100)}% del delta")
        confidence = None
        conf_ok = True
        if delta > 0 and sample_ok and robust_ok:
            confidence = sim_engine._bootstrap_confidence(contribs)
            if confidence < sim_engine.CONFIDENCE_MIN:
                conf_ok = False
                reasons.append(f"confidenza bootstrap {round(confidence * 100)}% "
                               f"sotto il minimo {round(sim_engine.CONFIDENCE_MIN * 100)}%")
        passed = (delta > 0) and sample_ok and robust_ok and conf_ok
        return {
            "ok": True,
            "rule": {"type": f"{kind}_policy", "params": {"policy": policy}},
            "since": since,
            "baseline": baseline,
            "simulated": simulated,
            "delta_pnl": delta,
            "verdict": "migliora" if delta > 0 else ("peggiora" if delta < 0 else "invariato"),
            "trades_excluded": 0,
            "trades_modified": touched,
            "coverage": coverage,
            "caveats": [
                "Replay su tick reali: TP al prezzo del livello, SL senza slippage, "
                "ambiguita' intra-tick risolta a sfavore (SL prima dei TP).",
                "Delta = replay(policy) - replay(gestione corrente): il bias di replay si cancella.",
                "Trade con fidelity oltre tolleranza esclusi e CONTATI in coverage.",
            ],
            "validation": {
                "passed": passed,
                "category": "replay",
                "n_affected": touched,
                "top1_share": top1_share,
                "delta_without_top1": delta_no_top1,
                "bootstrap_confidence": confidence,
                "fail_reasons": reasons,
            },
        }
    except Exception as e:
        return {"ok": False, "error": str(e)[:200]}
    finally:
        if close:
            db.close()


def monitor_real_toggle(db, since: Optional[str]) -> dict:
    """Monitor REALE di una mgmt_policy attivata via toggle: i trade nuovi
    seguono GIA' la nuova gestione, quindi il confronto giusto e'
    P&L reale osservato vs replay della GESTIONE PRECEDENTE (controfattuale
    'senza la modifica'). NB: fidelity_ok non e' applicabile qui (la realta'
    segue la nuova policy, e' NORMALE che diverga dal replay baseline)."""
    from database import TradeReplay
    import sim_engine
    from ai_advisor import _real_closed_trades
    try:
        trades = sim_engine._filter_since(_real_closed_trades(db), since)
        replays = {r.signal_id: r for r in db.query(TradeReplay)
                   .filter(TradeReplay.version == BATTERY_VERSION,
                           TradeReplay.ticks_ok == True).all()}  # noqa: E712
        actual_sum, counter_sum, n = 0.0, 0.0, 0
        for t in trades:
            r = replays.get(t.id)
            if r is None or not r.results_json:
                continue
            res = json.loads(r.results_json)
            actual_sum += float(t.pnl_usd)
            counter_sum += float(res["baseline_replay"])
            n += 1
        return {
            "ok": True, "tipo": "reale_toggle",
            "trade_nel_periodo": len(trades),
            "trade_con_replay": n,
            "pnl_reale_osservato": round(actual_sum, 2),
            "pnl_controfattuale_gestione_precedente": round(counter_sum, 2),
            "delta_osservato": round(actual_sum - counter_sum, 2),
        }
    except Exception as e:
        return {"ok": False, "error": str(e)[:150]}


def replay_sweep(db=None) -> dict:
    """Batteria completa (tutte le policy di gestione e d'ingresso) con
    validazione: promosse / bocciate_interessanti + coverage. Per il dossier."""
    from database import SessionLocal
    close = False
    if db is None:
        db = SessionLocal(); close = True
    try:
        promosse, bocciate = [], []
        coverage = None
        for kind, pool in (("mgmt", MGMT_POLICIES), ("entry", ENTRY_POLICIES)):
            for name in pool:
                res = validate_policy(kind, {"policy": name}, db)
                if not res.get("ok"):
                    continue
                coverage = res.get("coverage") or coverage
                v = res.get("validation") or {}
                entry = {
                    "sim_type": f"{kind}_policy",
                    "sim_params": json.dumps({"policy": name}),
                    "delta_pnl": res["delta_pnl"],
                    "n_affected": v.get("n_affected"),
                    "bootstrap_confidence": v.get("bootstrap_confidence"),
                    "toggle_reale": name in MGMT_REAL_TOGGLES if kind == "mgmt" else False,
                }
                if v.get("passed"):
                    promosse.append(entry)
                elif (res["delta_pnl"] or 0) > 0:
                    entry["fail_reasons"] = v.get("fail_reasons", [])
                    bocciate.append(entry)
        promosse.sort(key=lambda e: -(e["delta_pnl"] or 0))
        bocciate.sort(key=lambda e: -(e["delta_pnl"] or 0))
        return {"promosse": promosse, "bocciate_interessanti": bocciate,
                "coverage": coverage or {}}
    except Exception as e:
        _log(f"replay_sweep err: {str(e)[:150]}")
        return {"promosse": [], "bocciate_interessanti": [], "coverage": {}}
    finally:
        if close:
            db.close()
