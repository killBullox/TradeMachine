"""Motore di contesto ICT — ricostruisce il contesto di mercato in cui il
trader ha lanciato ogni trade e lo classifica in un setup.

Filosofia identica al resto dell'AIA: feature calcolate DETERMINISTICAMENTE
in Python su candele M5/M15 reali (MT5); l'LLM riceve numeri, non opinioni.

Concetti ICT rilevati (approssimazioni algoritmiche CONSISTENTI di concetti in
parte discrezionali — due analisti non tracciano gli stessi OB; qui la stessa
definizione vale per tutti i trade, e le conclusioni passano comunque i gate
statistici):
  - Swing H/L (frattali k=2) e struttura M15 (HH/HL vs LH/LL -> bias)
  - BOS (break of structure) direzionale su M5 + eventuale RETEST del livello
  - CHoCH implicito (BOS contro il bias precedente)
  - FVG (fair value gap a 3 candele) freschi/mitigati; entry dentro un FVG
  - Order Block (ultima candela contraria prima dell'impulso che rompe
    struttura); entry su retest dell'OB
  - Liquidity sweep (spazzata di minimi/massimi con rientro) prima dell'entry
  - Premium/Discount (posizione dell'entry nel dealing range M15)
  - Kill zones (London 08-11 Roma, NY 14:30-17 Roma)

Setup primario (priorita' decrescente):
  sweep_reversal > ob_retest > fvg_entry > bos_retest > bos_no_retest >
  counter_trend > no_context
"""
import json
from datetime import datetime, timedelta, timezone
from typing import Optional

SWING_K = 2                 # frattale: k candele per lato
BOS_LOOKBACK_M5 = 60        # candele M5 in cui cercare il BOS prima dell'entry
SWEEP_LOOKBACK = 24         # candele M5 per la ricerca dello sweep
FVG_LOOKBACK = 60
OB_LOOKBACK = 60
M5_WINDOW_H = 24            # ore di M5 scaricate prima dell'entry
M15_WINDOW_H = 72           # ore di M15 per bias e dealing range
ZONE_TOL = 0.0005           # tolleranza relativa (0.05%) per zone/retest

SETUP_LABELS = ("sweep_reversal", "ob_retest", "fvg_entry", "bos_retest",
                "bos_no_retest", "counter_trend", "no_context", "no_data")


def _log(msg: str):
    try:
        from mt5_trader import log as _l
        _l(f"[ICT] {msg}")
    except Exception:
        print(f"[ICT] {msg}", flush=True)


# ─── Feature pure (testabili senza MT5) ──────────────────────────────────────

def find_swings(candles, k: int = SWING_K):
    """Frattali: (idx, 'H'|'L', prezzo). candles = [{t,o,h,l,c}, ...]."""
    out = []
    n = len(candles)
    for i in range(k, n - k):
        hi = candles[i]["h"]; lo = candles[i]["l"]
        if all(hi > candles[j]["h"] for j in range(i - k, i + k + 1) if j != i):
            out.append((i, "H", hi))
        elif all(lo < candles[j]["l"] for j in range(i - k, i + k + 1) if j != i):
            out.append((i, "L", lo))
    return out


def structure_bias(swings) -> str:
    """Bias dalla sequenza degli ultimi swing: bullish (HH+HL), bearish (LH+LL),
    altrimenti neutral."""
    highs = [s for s in swings if s[1] == "H"][-2:]
    lows = [s for s in swings if s[1] == "L"][-2:]
    if len(highs) == 2 and len(lows) == 2:
        hh = highs[1][2] > highs[0][2]
        hl = lows[1][2] > lows[0][2]
        lh = highs[1][2] < highs[0][2]
        ll = lows[1][2] < lows[0][2]
        if hh and hl:
            return "bullish"
        if lh and ll:
            return "bearish"
    return "neutral"


def last_bos(candles, swings, entry_idx: int, direction: str):
    """Ultimo BOS nella direzione del trade prima dell'entry: la CHIUSURA di una
    candela oltre l'ultimo swing opposto. Ritorna dict o None.
    direction 'buy': close > ultimo swing High precedente. 'sell': close < swing Low."""
    want = "H" if direction == "buy" else "L"
    start = max(0, entry_idx - BOS_LOOKBACK_M5)
    best = None
    broken = set()
    for i in range(start, entry_idx):
        prior = [s for s in swings if s[0] < i and s[1] == want]
        if not prior:
            continue
        level = prior[-1][2]
        c = candles[i]["c"]
        prev_c = candles[i - 1]["c"] if i > 0 else None
        # BOS = il PRIMO attraversamento in chiusura del livello (transizione),
        # non ogni candela che resta oltre il livello gia' rotto.
        if direction == "buy":
            crossed = c > level and (prev_c is None or prev_c <= level)
        else:
            crossed = c < level and (prev_c is None or prev_c >= level)
        if crossed and level not in broken:
            broken.add(level)
            best = {"idx": i, "level": level}
    if best is None:
        return None
    # retest: dopo il BOS, il prezzo e' tornato a toccare il livello rotto?
    tol = best["level"] * ZONE_TOL
    retest = False
    for j in range(best["idx"] + 1, entry_idx):
        if direction == "buy" and candles[j]["l"] <= best["level"] + tol:
            retest = True; break
        if direction == "sell" and candles[j]["h"] >= best["level"] - tol:
            retest = True; break
    best["retest"] = retest
    best["bars_before_entry"] = entry_idx - best["idx"]
    return best


def find_fvgs(candles, upto_idx: int):
    """FVG a 3 candele fino a upto_idx (escluso). bullish: high[i-2] < low[i].
    Mitigato se una candela successiva rientra nella zona."""
    fvgs = []
    start = max(2, upto_idx - FVG_LOOKBACK)
    for i in range(start, upto_idx):
        a, b = candles[i - 2], candles[i]
        if a["h"] < b["l"]:
            fvgs.append({"idx": i, "type": "bullish", "bottom": a["h"], "top": b["l"], "mitigated": False})
        elif a["l"] > b["h"]:
            fvgs.append({"idx": i, "type": "bearish", "bottom": b["h"], "top": a["l"], "mitigated": False})
    for f in fvgs:
        for j in range(f["idx"] + 1, upto_idx):
            if f["type"] == "bullish" and candles[j]["l"] <= f["top"]:
                f["mitigated"] = True; break
            if f["type"] == "bearish" and candles[j]["h"] >= f["bottom"]:
                f["mitigated"] = True; break
    return fvgs


def entry_in_fvg(fvgs, entry_price: float, direction: str):
    """Entry dentro un FVG allineato (bullish per buy) e non mitigato."""
    want = "bullish" if direction == "buy" else "bearish"
    for f in reversed(fvgs):
        if f["type"] == want and not f["mitigated"] \
                and f["bottom"] <= entry_price <= f["top"]:
            return f
    return None


def find_order_blocks(candles, swings, entry_idx: int, direction: str):
    """OB allineato: ultima candela CONTRARIA prima di un BOS nella direzione
    del trade (bullish OB per buy = ultima candela rossa prima dell'impulso)."""
    bos = last_bos(candles, swings, entry_idx, direction)
    if not bos:
        return None
    i = bos["idx"]
    lookback_start = max(0, i - OB_LOOKBACK)
    for j in range(i - 1, lookback_start, -1):
        c = candles[j]
        contrary = (c["c"] < c["o"]) if direction == "buy" else (c["c"] > c["o"])
        if contrary:
            return {"idx": j, "bottom": c["l"], "top": c["h"], "bos_idx": i}
    return None


def entry_at_ob(ob, entry_price: float) -> bool:
    if not ob:
        return False
    tol = (ob["top"] - ob["bottom"]) * 0.1 + ob["top"] * ZONE_TOL
    return (ob["bottom"] - tol) <= entry_price <= (ob["top"] + tol)


def detect_sweep(candles, entry_idx: int, direction: str):
    """Liquidity sweep a favore del trade: per un BUY, una candela recente
    spazza i minimi (low < min dei precedenti) ma CHIUDE sopra (grab di
    sell-side liquidity), e l'entry avviene dopo. Simmetrico per SELL."""
    start = max(SWING_K, entry_idx - SWEEP_LOOKBACK)
    for i in range(entry_idx - 1, start, -1):
        window = candles[max(0, i - SWEEP_LOOKBACK):i]
        if not window:
            continue
        c = candles[i]
        if direction == "buy":
            prior_min = min(w["l"] for w in window)
            if c["l"] < prior_min and c["c"] > prior_min:
                return {"idx": i, "level": prior_min, "side": "sell_side"}
        else:
            prior_max = max(w["h"] for w in window)
            if c["h"] > prior_max and c["c"] < prior_max:
                return {"idx": i, "level": prior_max, "side": "buy_side"}
    return None


def premium_discount(candles_m15, entry_price: float) -> Optional[float]:
    """Posizione dell'entry nel dealing range M15 (0=minimo, 1=massimo).
    ICT: buy 'corretto' in discount (<0.5), sell in premium (>0.5)."""
    if not candles_m15:
        return None
    hi = max(c["h"] for c in candles_m15)
    lo = min(c["l"] for c in candles_m15)
    if hi <= lo:
        return None
    return round((entry_price - lo) / (hi - lo), 3)


def kill_zone(roma_dt) -> Optional[str]:
    hm = roma_dt.hour * 60 + roma_dt.minute
    if 8 * 60 <= hm < 11 * 60:
        return "london"
    if 14 * 60 + 30 <= hm < 17 * 60:
        return "new_york"
    return None


def classify_entry(candles_m5, candles_m15, entry_time_utc, entry_price: float,
                   direction: str) -> dict:
    """Contesto completo + setup primario. candles gia' in UTC, entry compresa
    nell'ultimo tratto delle M5."""
    if not candles_m5 or len(candles_m5) < 20:
        return {"setup": "no_data", "candles_ok": False}
    # indice dell'ultima candela PRIMA dell'entry
    entry_idx = len(candles_m5)
    for i, c in enumerate(candles_m5):
        if c["t"] > entry_time_utc:
            entry_idx = i
            break
    if entry_idx < 10:
        return {"setup": "no_data", "candles_ok": False}

    swings5 = find_swings(candles_m5[:entry_idx])
    swings15 = find_swings(candles_m15) if candles_m15 else []
    bias = structure_bias(swings15)
    bos = last_bos(candles_m5, swings5, entry_idx, direction)
    fvgs = find_fvgs(candles_m5, entry_idx)
    fvg = entry_in_fvg(fvgs, entry_price, direction)
    ob = find_order_blocks(candles_m5, swings5, entry_idx, direction)
    at_ob = entry_at_ob(ob, entry_price)
    sweep = detect_sweep(candles_m5, entry_idx, direction)
    pd_pos = premium_discount(candles_m15, entry_price)

    from zoneinfo import ZoneInfo
    roma = entry_time_utc.replace(tzinfo=timezone.utc).astimezone(ZoneInfo("Europe/Rome"))
    kz = kill_zone(roma)
    with_trend = (bias == "bullish" and direction == "buy") or \
                 (bias == "bearish" and direction == "sell")
    counter = (bias == "bullish" and direction == "sell") or \
              (bias == "bearish" and direction == "buy")

    # setup primario per priorita'
    if sweep is not None:
        setup = "sweep_reversal"
    elif at_ob:
        setup = "ob_retest"
    elif fvg is not None:
        setup = "fvg_entry"
    elif bos and bos["retest"]:
        setup = "bos_retest"
    elif bos and not bos["retest"]:
        setup = "bos_no_retest"
    elif counter:
        setup = "counter_trend"
    else:
        setup = "no_context"

    pd_bucket = None
    if pd_pos is not None:
        pd_bucket = "discount" if pd_pos < 0.45 else ("premium" if pd_pos > 0.55 else "equilibrium")

    return {
        "setup": setup,
        "candles_ok": True,
        "bias_m15": bias,
        "with_trend": with_trend,
        "counter_trend": counter,
        "bos": ({"bars_before_entry": bos["bars_before_entry"], "retest": bos["retest"]}
                if bos else None),
        "in_fvg": fvg is not None,
        "at_order_block": at_ob,
        "sweep": sweep["side"] if sweep else None,
        "premium_discount": pd_pos,
        "pd_bucket": pd_bucket,
        "kill_zone": kz,
    }


# ─── MT5 fetch + persistenza contesti ────────────────────────────────────────

def _get_mt5():
    try:
        import mt5_trader
        if not mt5_trader.is_enabled():
            return None
        return mt5_trader._get_mt5()
    except Exception:
        return None


def fetch_candles(symbol: str, timeframe_key: str, utc_from, utc_to):
    """Candele [{t(utc),o,h,l,c}] via MT5. Offset server centralizzato
    (mt5_time). Ritorna [] su qualsiasi problema."""
    mt5 = _get_mt5()
    if mt5 is None:
        return []
    try:
        from datetime import timezone as _tz
        from mt5_time import detect_mt5_server_offset, mt5_epoch_to_utc
        off = detect_mt5_server_offset(symbol)
        tf = {"M5": mt5.TIMEFRAME_M5, "M15": mt5.TIMEFRAME_M15}[timeframe_key]
        # AWARE UTC obbligatorio: il wrapper MT5 converte i naive col fuso
        # LOCALE della macchina -> sul VPS (tz Roma) la finestra arrivava
        # spostata di 2h e il contesto era calcolato PRIMA dell'entry.
        srv_from = (utc_from + timedelta(seconds=off)).replace(tzinfo=_tz.utc)
        srv_to = (utc_to + timedelta(seconds=off)).replace(tzinfo=_tz.utc)
        rates = mt5.copy_rates_range(symbol, tf, srv_from, srv_to)
        if rates is None:
            return []
        out = []
        for r in rates:
            out.append({"t": mt5_epoch_to_utc(int(r[0])),
                        "o": float(r[1]), "h": float(r[2]),
                        "l": float(r[3]), "c": float(r[4])})
        return out
    except Exception as e:
        _log(f"fetch_candles err {symbol} {timeframe_key}: {str(e)[:100]}")
        return []


def build_context_for_signal(sig) -> dict:
    """Contesto ICT per un trade (fetch M5+M15 e classifica).
    Funziona sia POST-fill (actual_entry_price noto: dato definitivo) sia
    ALL'INTAKE per l'enforcement (fallback: prezzo corrente o entry segnale)."""
    try:
        from mt5_trader import get_mt5_symbol
        symbol = get_mt5_symbol(sig.symbol)
    except Exception:
        symbol = sig.symbol
    entry_time = sig.entered_at or sig.created_at or datetime.utcnow()
    entry_price = None
    if sig.actual_entry_price:
        entry_price = float(sig.actual_entry_price)
    else:
        # intake: usa il prezzo corrente (approssimazione dichiarata) o l'entry
        mt5 = _get_mt5()
        try:
            if mt5:
                tick = mt5.symbol_info_tick(symbol)
                if tick:
                    entry_price = float(tick.ask if (sig.direction or "buy").lower() == "buy"
                                        else tick.bid)
        except Exception:
            pass
        if not entry_price and sig.entry_price:
            entry_price = float(sig.entry_price)
    if not entry_price:
        return {"setup": "no_data", "candles_ok": False}
    m5 = fetch_candles(symbol, "M5", entry_time - timedelta(hours=M5_WINDOW_H),
                       entry_time + timedelta(minutes=10))
    m15 = fetch_candles(symbol, "M15", entry_time - timedelta(hours=M15_WINDOW_H),
                        entry_time)
    return classify_entry(m5, m15, entry_time, entry_price,
                          (sig.direction or "buy").lower())


def save_context_if_missing(db, sig) -> Optional[str]:
    """Calcola e salva il contesto ICT per un trade appena CHIUSO (dati di fill
    definitivi) se non esiste gia'. Cosi' il Monitor Test si aggiorna subito,
    senza aspettare il batch giornaliero. Ritorna il setup o None. Mai solleva."""
    try:
        from database import TradeContext
        if db.query(TradeContext).filter(TradeContext.signal_id == sig.id).first():
            return None
        ctx = build_context_for_signal(sig)
        db.add(TradeContext(signal_id=sig.id, setup=ctx.get("setup", "no_data"),
                            candles_ok=bool(ctx.get("candles_ok")),
                            features_json=json.dumps(ctx, ensure_ascii=False, default=str)))
        _log(f"contesto ICT #{sig.id} alla chiusura: {ctx.get('setup')}")
        return ctx.get("setup")
    except Exception as e:
        _log(f"save_context_if_missing #{getattr(sig, 'id', '?')} err: {str(e)[:100]}")
        return None


def ensure_contexts(db=None, max_new: int = 400) -> dict:
    """Calcola e salva il contesto ICT per i trade reali chiusi che non lo hanno
    ancora. Incrementale e idempotente. Mai solleva."""
    from database import SessionLocal, TradeContext
    from ai_advisor import _real_closed_trades
    close = False
    if db is None:
        db = SessionLocal(); close = True
    done = 0; failed = 0
    try:
        have = {tc.signal_id for tc in db.query(TradeContext).all()}
        todo = [t for t in _real_closed_trades(db) if t.id not in have][:max_new]
        for t in todo:
            try:
                ctx = build_context_for_signal(t)
            except Exception as e:
                ctx = {"setup": "no_data", "candles_ok": False, "err": str(e)[:100]}
            db.add(TradeContext(signal_id=t.id, setup=ctx.get("setup", "no_data"),
                                candles_ok=bool(ctx.get("candles_ok")),
                                features_json=json.dumps(ctx, ensure_ascii=False, default=str)))
            done += 1
            if not ctx.get("candles_ok"):
                failed += 1
        if done:
            db.commit()
            _log(f"contesti ICT: +{done} ({failed} senza candele)")
        return {"computed": done, "no_data": failed}
    except Exception as e:
        _log(f"ensure_contexts err: {str(e)[:150]}")
        try:
            db.rollback()
        except Exception:
            pass
        return {"computed": done, "no_data": failed, "error": str(e)[:150]}
    finally:
        if close:
            db.close()


def strategy_stats(db) -> dict:
    """Statistiche 'strategia del trader' per il dossier: esiti per setup ICT,
    per kill zone, premium/discount, with/counter trend."""
    from database import TradeContext
    from ai_advisor import _real_closed_trades, WIN_STATUSES
    ctxs = {tc.signal_id: tc for tc in db.query(TradeContext).all()}
    trades = _real_closed_trades(db)
    per_setup, per_kz, per_pd, per_trend = {}, {}, {}, {}
    covered = 0
    for t in trades:
        tc = ctxs.get(t.id)
        if not tc or not tc.candles_ok:
            continue
        covered += 1
        try:
            f = json.loads(tc.features_json or "{}")
        except Exception:
            f = {}
        pnl = float(t.pnl_usd)
        win = t.status in WIN_STATUSES
        def acc(dct, key):
            if key is None:
                key = "n/d"
            b = dct.setdefault(key, {"trades": 0, "wins": 0, "pnl": 0.0})
            b["trades"] += 1; b["pnl"] += pnl
            if win:
                b["wins"] += 1
        acc(per_setup, tc.setup)
        acc(per_kz, f.get("kill_zone") or "fuori_kz")
        acc(per_pd, f.get("pd_bucket"))
        acc(per_trend, "with_trend" if f.get("with_trend")
            else ("counter_trend" if f.get("counter_trend") else "neutral"))
    for dct in (per_setup, per_kz, per_pd, per_trend):
        for b in dct.values():
            b["pnl"] = round(b["pnl"], 2)
            b["win_rate"] = round(100.0 * b["wins"] / b["trades"], 1) if b["trades"] else 0.0
    return {
        "coverage": {"con_contesto": covered, "totale": len(trades)},
        "per_setup": per_setup,
        "per_kill_zone": per_kz,
        "per_premium_discount": per_pd,
        "per_trend": per_trend,
        "nota": ("Setup rilevati algoritmicamente (approssimazione consistente di "
                 "concetti ICT in parte discrezionali). I pattern per setup possono "
                 "diventare regole exclude_setup, validate dai gate statistici."),
    }
