"""
MT5 Auto-Trader — esegue ordini su MT5 in base ai segnali del DB.

Flusso:
  1. place_order(sig) → apre un ordine pending (limit/stop) o market su MT5
  2. sync_positions()  → background task ogni 30s: controlla posizioni MT5 aperte
                         e aggiorna stato segnali nel DB (tp/sl hit, pnl reale)
  3. modify_sl(ticket, new_sl) → aggiorna lo SL di una posizione aperta
  4. close_position(ticket)    → chiude una posizione manualmente
"""
import asyncio
import math
from datetime import datetime
from typing import Optional

# Mapping simboli DB → simboli MT5 (stesso di price_service.py)
MT5_SYMBOL_MAP = {
    "XAUUSD":  "GOLD#",
    "XAGUSD":  "SILVER#",
    "EURUSD":  "EURUSD#",
    "GBPUSD":  "GBPUSD#",
    "USDJPY":  "USDJPY#",
    "USDCHF":  "USDCHF#",
    "USDCAD":  "USDCAD#",
    "AUDUSD":  "AUDUSD#",
    "NZDUSD":  "NZDUSD#",
    "GBPJPY":  "GBPJPY#",
    "EURJPY":  "EURJPY#",
    "AUDJPY":  "AUDJPY#",
    "GBPCHF":  "GBPCHF#",
    "EURGBP":  "EURGBP#",
}

_auto_trade_enabled = False   # toggle globale

import logging
_logger = logging.getLogger("trademachine")

def log(msg: str):
    safe = ''.join(c if ord(c) < 128 else '?' for c in str(msg))
    line = f"[MT5Trader] {safe}"
    print(line, flush=True)
    _logger.info(line)


def _append_trade_log_mt5(sig, event: str, detail: str, extra: dict = None):
    """Appende un evento al trade_log del segnale (chiamato da mt5_trader senza DB session)."""
    import json as _json
    from datetime import datetime as _dt
    now_str = _dt.utcnow().isoformat() + "Z"
    entry = {"ts": now_str, "event": event, "detail": detail}
    if extra:
        entry.update(extra)
    try:
        log_list = _json.loads(sig.trade_log) if sig.trade_log else []
    except Exception:
        log_list = []
    log_list.append(entry)
    sig.trade_log = _json.dumps(log_list)


def is_enabled() -> bool:
    return _auto_trade_enabled


def enable():
    global _auto_trade_enabled
    _auto_trade_enabled = True
    log("Auto-trading ATTIVATO")


def disable():
    global _auto_trade_enabled
    _auto_trade_enabled = False
    log("Auto-trading DISATTIVATO")


# Account demo XM — specificato per evitare di operare sul conto reale per errore
import os
from dotenv import load_dotenv
load_dotenv()

MT5_ACCOUNT = int(os.getenv("MT5_ACCOUNT", "27640489"))
MT5_SERVER = os.getenv("MT5_SERVER", "XM.COM-MT5")
MT5_PATH = os.getenv("MT5_PATH", "") or None  # Path seconda installazione MT5


_mt5_server_offset = None  # secondi di offset tra server MT5 e UTC

def _get_mt5_utc(mt5_epoch: int) -> datetime:
    """Converte un timestamp MT5 (server time) in datetime UTC."""
    global _mt5_server_offset
    if _mt5_server_offset is None:
        _detect_server_offset()
    return datetime.utcfromtimestamp(mt5_epoch - (_mt5_server_offset or 0))


def _detect_server_offset():
    """Rileva l'offset tra il server MT5 e UTC.
    MT5 deal.time è un pseudo-epoch in server timezone, non UTC reale.
    """
    global _mt5_server_offset
    try:
        import MetaTrader5 as mt5
        tick = mt5.symbol_info_tick("GOLD#") or mt5.symbol_info_tick("EURUSD")
        if tick:
            # utcfromtimestamp tratta l'epoch come UTC → dà il "server time" grezzo
            server_time = datetime.utcfromtimestamp(tick.time)
            utc_now = datetime.utcnow()
            _mt5_server_offset = int((server_time - utc_now).total_seconds())
            _mt5_server_offset = round(_mt5_server_offset / 1800) * 1800
            log(f"Server offset rilevato: {_mt5_server_offset}s ({_mt5_server_offset//3600}h)")
    except Exception as e:
        log(f"Errore rilevamento offset: {e}")
        _mt5_server_offset = 0


def _get_mt5():
    import os
    if os.getenv("MT5_DISABLED", "").strip() in ("1", "true", "yes"):
        return None
    try:
        import MetaTrader5 as mt5
        import time as _time
        init_kwargs = {"login": MT5_ACCOUNT, "server": MT5_SERVER}
        if MT5_PATH:
            init_kwargs["path"] = MT5_PATH
        if not mt5.initialize(**init_kwargs):
            err = mt5.last_error()
            log(f"MT5 init fallito (account {MT5_ACCOUNT}): {err} — retry in 3s...")
            mt5.shutdown()
            _time.sleep(3)
            # Secondo tentativo: MT5 potrebbe aver bisogno di più tempo per avviarsi
            if not mt5.initialize(**init_kwargs):
                log(f"MT5 init fallito al 2° tentativo: {mt5.last_error()}")
                return None
        # Verifica di sicurezza: assicurati di essere sul conto giusto
        info = mt5.account_info()
        if info and info.login != MT5_ACCOUNT:
            log(f"ATTENZIONE: connesso ad account {info.login} invece di {MT5_ACCOUNT} — blocco operazioni")
            mt5.shutdown()
            return None
        return mt5
    except Exception as e:
        log(f"MT5 non disponibile: {e}")
        return None


def _round_price(price: float, digits: int) -> float:
    return round(price, digits)


def _round_volume(vol: float, step: float, min_vol: float, max_vol: float) -> float:
    """Arrotonda il volume al step del broker, rispettando min/max."""
    if vol < min_vol:
        return min_vol
    if vol > max_vol:
        return max_vol
    steps = math.floor(vol / step)
    return round(steps * step, 10)


def _send_single_order(mt5, mt5_sym, order_type, action, entry, sl, tp, lots, sig_id, tp_num, digits, is_buy=True) -> Optional[int]:
    """Invia un singolo ordine e ritorna il ticket."""
    direction = "B" if is_buy else "S"
    request = {
        "action":       action,
        "symbol":       mt5_sym,
        "volume":       lots,
        "type":         order_type,
        "price":        entry,
        "sl":           sl,
        "tp":           tp,
        "deviation":    20,
        "magic":        20250326,
        "comment":      f"IC#{sig_id} {mt5_sym[:6]} {direction}/TP{tp_num}"[:31],
        "type_time":    mt5.ORDER_TIME_GTC,
        "type_filling": mt5.ORDER_FILLING_IOC,
    }
    result = mt5.order_send(request)
    if result is None or result.retcode != mt5.TRADE_RETCODE_DONE:
        code = result.retcode if result else "N/A"
        comment = result.comment if result else "N/A"
        log(f"#{sig_id} TP{tp_num} order_send fallito: retcode={code} {comment}")
        return None
    ticket = result.order
    # Verifica che il ticket esista: posizione aperta, ordine pendente, o già in storia
    import time as _time
    _time.sleep(0.3)
    exists = (mt5.positions_get(ticket=ticket) or
              mt5.orders_get(ticket=ticket) or
              mt5.history_orders_get(ticket=ticket) or
              mt5.history_deals_get(position=ticket))
    if not exists:
        log(f"#{sig_id} TP{tp_num} ticket={ticket} NON confermato in MT5 — ordine potenzialmente non eseguito")
        # Non blocchiamo: il ticket è stato accettato da MT5 (retcode=DONE), teniamolo
    else:
        log(f"#{sig_id} TP{tp_num} ticket={ticket} confermato in MT5")
    return ticket


def _analyze_late_catch_ticks(mt5, mt5_sym, sig, signal_ts, now_ts, pip_size):
    """
    Analizza i ticks nell'intervallo [signal_ts, now_ts] per determinare:
      - 'in_range'      → prezzo sempre dentro il range
      - 'out_range'     → prezzo sempre fuori dal range
      - 'crossed_above' → prezzo ha oltrepassato entry_high durante il ritardo
      - 'crossed_below' → prezzo ha oltrepassato entry_low durante il ritardo
      - 'no_data'       → impossibile recuperare ticks (fallback: no verdetto)
    Range:
      - se entry_high esiste → [entry_low, entry_high]
      - se solo entry_price → [entry - pip_size, entry + pip_size]
    """
    ep_low = float(sig.entry_price) if sig.entry_price else None
    ep_high = float(sig.entry_price_high) if sig.entry_price_high else None
    if ep_low is None and ep_high is None:
        return ("no_data", None, None, None)
    if ep_high is None:
        rng_low = ep_low - pip_size
        rng_high = ep_low + pip_size
    elif ep_low is None:
        rng_low = ep_high - pip_size
        rng_high = ep_high + pip_size
    else:
        rng_low = min(ep_low, ep_high)
        rng_high = max(ep_low, ep_high)

    try:
        ticks = mt5.copy_ticks_range(mt5_sym, signal_ts, now_ts, mt5.COPY_TICKS_ALL)
    except Exception as e:
        log(f"#{sig.id} copy_ticks_range errore: {e}")
        return ("no_data", rng_low, rng_high, None)
    if ticks is None or len(ticks) == 0:
        return ("no_data", rng_low, rng_high, None)

    saw_in = False
    saw_above = False
    saw_below = False
    for t in ticks:
        mid = (t['bid'] + t['ask']) / 2.0 if t['bid'] and t['ask'] else (t['bid'] or t['ask'])
        if mid < rng_low:
            saw_below = True
        elif mid > rng_high:
            saw_above = True
        else:
            saw_in = True

    n = len(ticks)
    if saw_in and not saw_above and not saw_below:
        return ("in_range", rng_low, rng_high, n)
    if (saw_above or saw_below) and not saw_in:
        return ("out_range", rng_low, rng_high, n)
    # Transizione
    if saw_above and saw_below:
        return ("crossed_above", rng_low, rng_high, n)  # è comunque "crossed"; log verrà generico
    if saw_above:
        return ("crossed_above", rng_low, rng_high, n)
    return ("crossed_below", rng_low, rng_high, n)


def place_orders(sig, catch_origin: str = "realtime", catch_reason: Optional[str] = None,
                 signal_ts: Optional[datetime] = None) -> list:
    """
    Piazza 2-3 ordini separati (uno per ogni TP) con lotti divisi equamente.
    Ritorna lista di ticket piazzati con successo.

    catch_origin: 'realtime' | 'delayed' | 'edited' | 'replay'
      Se != 'realtime', esegue la pre-check late-catch analizzando i tick
      nell'intervallo [signal_ts, now] e può annullare il segnale se il
      prezzo ha attraversato il range durante il ritardo.
    """
    if not _auto_trade_enabled:
        return []

    mt5 = _get_mt5()
    if mt5 is None:
        return []

    mt5_sym = MT5_SYMBOL_MAP.get(sig.symbol.upper())
    if not mt5_sym:
        log(f"Simbolo non supportato per trading: {sig.symbol}")
        return []

    mt5.symbol_select(mt5_sym, True)
    sym_info = mt5.symbol_info(mt5_sym)
    if sym_info is None:
        log(f"Symbol info non disponibile per {mt5_sym}")
        return []

    # Quando un simbolo viene appena aggiunto al Market Watch, il broker può
    # impiegare qualche istante a inviare il primo tick. Senza tick valido
    # mt5.order_send() risponde retcode=10015 Invalid price. Retry breve.
    import time as _time
    tick = None
    for _attempt in range(8):
        tick = mt5.symbol_info_tick(mt5_sym)
        if tick and tick.bid > 0 and tick.ask > 0:
            break
        _time.sleep(0.25)
    if tick is None or tick.bid <= 0 or tick.ask <= 0:
        log(f"#{sig.id} tick non disponibile per {mt5_sym} (bid/ask=0 dopo retry) — skip")
        _append_trade_log_mt5(sig, "mt5_skip", f"Tick non disponibile per {mt5_sym} (simbolo non in Market Watch?), ordini non inviati")
        return []

    digits      = sym_info.digits
    min_vol     = sym_info.volume_min
    max_vol     = sym_info.volume_max
    vol_step    = sym_info.volume_step
    point       = sym_info.point
    stops_level = sym_info.trade_stops_level
    min_dist    = stops_level * point
    pip_size    = point * 10  # 1 pip = 10 points (convenzione broker standard)

    # ── Pre-check late catch ─────────────────────────────────────────────────
    # Se il segnale NON è realtime, analizza i tick nell'intervallo di ritardo
    # per decidere se l'ingresso è ancora valido.
    if catch_origin != "realtime" and signal_ts is not None:
        now_ts = datetime.utcnow()
        verdict, rng_low, rng_high, n_ticks = _analyze_late_catch_ticks(
            mt5, mt5_sym, sig, signal_ts, now_ts, pip_size
        )
        log(f"#{sig.id} late-catch pre-check: origin={catch_origin} verdict={verdict} "
            f"range=[{rng_low},{rng_high}] ticks={n_ticks} reason='{catch_reason}'")
        if verdict in ("crossed_above", "crossed_below"):
            direction_txt = "sopra" if verdict == "crossed_above" else "sotto"
            cancel_msg = (f"Late catch ({catch_origin}) {catch_reason or ''}: prezzo ha "
                          f"attraversato il range [{rng_low:.5f},{rng_high:.5f}] "
                          f"andando {direction_txt} durante il ritardo "
                          f"({signal_ts.strftime('%H:%M:%S')}→{now_ts.strftime('%H:%M:%S')})")
            sig._late_catch_cancel_reason = cancel_msg
            log(f"#{sig.id} ANNULLATO: {cancel_msg}")
            return []
        # in_range / out_range / no_data → procede con la logica normale sotto.
    # ─────────────────────────────────────────────────────────────────────────

    is_buy = (sig.direction or "buy").lower() == "buy"
    entry  = sig.entry_price or sig.entry_price_high
    entry_high = sig.entry_price_high or sig.entry_price
    sl_raw = sig.stoploss

    # Auto-correzione entry: se SL e TP1 sono coerenti tra loro ma l'entry è dall'altra parte,
    # l'admin ha fatto un typo sull'entry → usa il prezzo corrente come entry
    tp1 = sig.tp1
    if entry and sl_raw and tp1:
        e, s, t = float(entry), float(sl_raw), float(tp1)
        if is_buy and s < t and (e > t or e < s * 0.99):
            # BUY: SL < TP1 è coerente, ma entry è fuori range → usa ask corrente
            tick = mt5.symbol_info_tick(mt5_sym)
            if tick and tick.ask > 0:
                old_entry = entry
                entry = tick.ask
                entry_high = entry
                sig.entry_price = entry
                sig.entry_price_high = entry
                log(f"#{sig.id} Entry {old_entry} incoerente con SL={s}/TP1={t} — corretto a prezzo corrente {entry}")
                _append_trade_log_mt5(sig, "mt5_entry_fix", f"Entry corretto da {old_entry} a {entry} (prezzo corrente, typo admin)")
        elif not is_buy and s > t and (e < t or e > s * 1.01):
            # SELL: SL > TP1 è coerente, ma entry è fuori range → usa bid corrente
            tick = mt5.symbol_info_tick(mt5_sym)
            if tick and tick.bid > 0:
                old_entry = entry
                entry = tick.bid
                entry_high = entry
                sig.entry_price = entry
                sig.entry_price_high = entry
                log(f"#{sig.id} Entry {old_entry} incoerente con SL={s}/TP1={t} — corretto a prezzo corrente {entry}")
                _append_trade_log_mt5(sig, "mt5_entry_fix", f"Entry corretto da {old_entry} a {entry} (prezzo corrente, typo admin)")

    if not entry:
        log(f"#{sig.id} nessun entry price, skip")
        _append_trade_log_mt5(sig, "mt5_skip", "Nessun entry price definito, ordini non inviati")
        return []

    # Auto-correzione TP con typo (zero di troppo/meno): confronta con gli altri TP e l'entry
    valid_tps = [float(getattr(sig, f'tp{i}')) for i in range(1, 4) if getattr(sig, f'tp{i}', None)]
    e_f = float(entry)
    for i in range(1, 4):
        tp_val = getattr(sig, f'tp{i}', None)
        if tp_val is None:
            continue
        tp_f = float(tp_val)
        # TP palesemente fuori scala rispetto all'entry e agli altri TP
        others = [t for t in valid_tps if t != tp_f]
        if others:
            avg_dist = sum(abs(t - e_f) for t in others) / len(others)
            dist = abs(tp_f - e_f)
            # Se la distanza è 10x la media, è un typo con zero di troppo
            if avg_dist > 0 and dist > avg_dist * 5:
                # Prova a correggere: dividi o moltiplica per 10
                for fix in [tp_f / 10, tp_f * 10]:
                    fix_dist = abs(fix - e_f)
                    if fix_dist < avg_dist * 3 and fix_dist > 0:
                        log(f"#{sig.id} TP{i}={tp_f} typo (zero di troppo?) - corretto a {fix}")
                        _append_trade_log_mt5(sig, "mt5_tp_fix", f"TP{i} corretto da {tp_f} a {fix} (typo admin)")
                        setattr(sig, f'tp{i}', fix)
                        tp_f = fix
                        break

    # Auto-correzione TP single-digit (typo di una sola cifra). Caso #280:
    # "Buy 4534-36 TP1 4542 TP2 4645 TP3 4650" — voleva 4545/4550 (5->6 typo).
    # Algoritmo: per ogni TP che è ben fuori scala rispetto a TP1, genera tutti
    # i candidati ottenuti sostituendo UNA cifra; tieni solo quelli dal lato
    # giusto, ordinati progressivamente (TP_i > TP_{i-1} per BUY, viceversa per
    # SELL) e con distanza plausibile (entro 1.5x..6x di TP1_dist per TP2,
    # 2x..10x per TP3). Se uno solo passa -> applica.
    tp1_val = getattr(sig, 'tp1', None)
    if tp1_val is not None:
        tp1_f = float(tp1_val)
        tp1_dist = abs(tp1_f - e_f)
        if tp1_dist > 0:
            # Bound dei multipli ragionevoli per TP2/TP3 vs TP1_dist
            tp_range = {2: (1.2, 6.0), 3: (1.8, 12.0)}
            for i in (2, 3):
                tp_val = getattr(sig, f'tp{i}', None)
                if tp_val is None:
                    continue
                tp_f = float(tp_val)
                dist = abs(tp_f - e_f)
                lo, hi = tp_range[i]
                # Skippa se il TP è già nella scala plausibile
                if lo * tp1_dist <= dist <= hi * tp1_dist:
                    continue
                # Prev TP (per garantire l'ordine progressivo)
                prev_tp = float(getattr(sig, f'tp{i-1}'))
                # Genera candidati single-digit del valore originale
                tp_str_full = f"{tp_f:.{digits}f}" if digits else f"{int(tp_f)}"
                seen = set()
                candidates = []
                for pos_idx, ch in enumerate(tp_str_full):
                    if not ch.isdigit():
                        continue
                    for d in "0123456789":
                        if d == ch:
                            continue
                        cand_str = tp_str_full[:pos_idx] + d + tp_str_full[pos_idx+1:]
                        try:
                            cand = float(cand_str)
                        except ValueError:
                            continue
                        if cand in seen or cand <= 0:
                            continue
                        seen.add(cand)
                        # Lato giusto rispetto a entry
                        side_ok = (is_buy and cand > e_f) or (not is_buy and cand < e_f)
                        if not side_ok:
                            continue
                        # Ordine progressivo rispetto al TP precedente
                        order_ok = (is_buy and cand > prev_tp) or (not is_buy and cand < prev_tp)
                        if not order_ok:
                            continue
                        # Distanza plausibile in scala con TP1
                        cand_dist = abs(cand - e_f)
                        if not (lo * tp1_dist <= cand_dist <= hi * tp1_dist):
                            continue
                        candidates.append(cand)

                if len(candidates) == 1:
                    fix = round(candidates[0], digits)
                    log(f"#{sig.id} TP{i}={tp_f} typo single-digit - corretto a {fix} (TP1_dist={tp1_dist:.1f})")
                    _append_trade_log_mt5(sig, "mt5_tp_fix", f"TP{i} corretto da {tp_f} a {fix} (typo single-digit)")
                    setattr(sig, f'tp{i}', fix)
                    sig.notes = (sig.notes or "") + f" [TP{i} auto-corretto: {tp_f} -> {fix}]"
                elif len(candidates) > 1:
                    log(f"#{sig.id} TP{i}={tp_f} fuori scala ma {len(candidates)} candidati ambigui - lascio com'e'")

    # Validazione coerenza direzione: scarta singoli TP ancora invalidi
    for i in range(1, 4):
        tp_val = getattr(sig, f'tp{i}', None)
        if tp_val is None:
            continue
        tp_f = float(tp_val)
        if is_buy and tp_f <= float(entry):
            log(f"#{sig.id} TP{i}={tp_f} sotto entry {entry} (BUY) — TP{i} scartato")
            _append_trade_log_mt5(sig, "mt5_tp_skip", f"TP{i}={tp_f} sotto entry {entry} — scartato")
            setattr(sig, f'tp{i}', None)
        elif not is_buy and tp_f >= float(entry):
            log(f"#{sig.id} TP{i}={tp_f} sopra entry {entry} (SELL) — TP{i} scartato")
            _append_trade_log_mt5(sig, "mt5_tp_skip", f"TP{i}={tp_f} sopra entry {entry} — scartato")
            setattr(sig, f'tp{i}', None)

    entry = _round_price(float(entry), digits)
    sl    = _round_price(float(sl_raw), digits) if sl_raw else 0.0

    # TP disponibili (dopo la pulizia)
    tps_raw = [(i+1, getattr(sig, f'tp{i+1}')) for i in range(3) if getattr(sig, f'tp{i+1}', None)]
    if not tps_raw:
        log(f"#{sig.id} nessun TP definito, skip")
        return []

    # Sanity check R/R: se TP1 e SL sono sproporzionati di oltre 5x in qualunque
    # direzione il segnale è probabilmente sbagliato (typo grave del trader).
    # Caso A (TP1 troppo lontano): "Sell 4734 SL 4742 TP1 4624" — voleva entry
    # 4634, R/R 14:1.
    # Caso B (SL troppo largo): "Sell 4567 TP1 4562 SL 4673" — voleva SL 4573,
    # R/R 0.05:1 (lotto microscopico, $5 di profit dove dovevi farne $100+).
    # NON tentare auto-correzione speculativa: annulla e aspetta l'edit.
    if sl_raw and tps_raw:
        sl_dist = abs(float(entry) - float(sl_raw))
        tp1_dist = abs(float(entry) - float(tps_raw[0][1]))
        if sl_dist > 0 and tp1_dist > 0:
            ratio = max(tp1_dist / sl_dist, sl_dist / tp1_dist)
            if ratio > 5:
                from database import SessionLocal as _SL
                if tp1_dist > sl_dist:
                    direction = f"TP1 {tp1_dist:.1f}pt ≫ SL {sl_dist:.1f}pt"
                else:
                    direction = f"SL {sl_dist:.1f}pt ≫ TP1 {tp1_dist:.1f}pt"
                msg = (f"R/R sproporzionato {ratio:.1f}:1 ({direction}) — "
                       f"segnale probabilmente sbagliato, in attesa di edit del trader")
                log(f"#{sig.id} {msg}")
                sig.status = "cancelled"
                sig.notes = (sig.notes or "") + f" [Sospeso R/R: {msg}]"
                _append_trade_log_mt5(sig, "rr_suspect", msg)
                _db = _SL()
                try:
                    _db.merge(sig)
                    _db.commit()
                finally:
                    _db.close()
                return []

    # Position size totale
    from risk import get_risk_settings, calc_risk_amount, calc_position_size, get_spec
    settings  = get_risk_settings()
    risk_usd  = calc_risk_amount(settings)
    if getattr(sig, 'is_risky', False):
        risk_usd *= 0.5
        log(f"#{sig.id} segnale RISKY → rischio dimezzato a ${risk_usd:.2f}")
    # Calcola lotti totali sull'intero rischio, poi dividi per n ordini
    n = len(tps_raw)
    lots_total_raw = calc_position_size(sig.symbol, entry, sl_raw, risk_usd) if sl_raw else min_vol
    lots_total = _round_volume(lots_total_raw or min_vol, vol_step, min_vol, max_vol)
    # Arrotonda per eccesso al vol_step per minimizzare la perdita
    lots_each_raw = lots_total / n
    lots_each_floor = _round_volume(lots_each_raw, vol_step, min_vol, max_vol)
    lots_each_ceil = round(lots_each_floor + vol_step, 10)
    if lots_each_ceil <= max_vol:
        # Scegli ceil se il rischio effettivo non supera il target di troppo (max +10%)
        spec = get_spec(sig.symbol)
        sl_pips = abs(float(entry) - float(sl_raw)) / spec["pip"] if sl_raw else 0
        risk_floor = sl_pips * spec["pv"] * lots_each_floor * n
        risk_ceil = sl_pips * spec["pv"] * lots_each_ceil * n
        lots_each = lots_each_ceil if abs(risk_ceil - risk_usd) < abs(risk_floor - risk_usd) else lots_each_floor
    else:
        lots_each = lots_each_floor
    lots_total = _round_volume(lots_each * n, vol_step, min_vol, max_vol)
    spec = get_spec(sig.symbol)
    sl_pips = abs(float(entry) - float(sl_raw)) / spec["pip"] if sl_raw else 0
    effective_risk = sl_pips * spec["pv"] * lots_total
    log(f"#{sig.id} risk=${risk_usd:.0f} lots={lots_each}x{n}={lots_total} sl_pips={sl_pips:.0f} eff_risk=${effective_risk:.2f}")

    # Determina tipo ordine
    current_bid = tick.bid
    current_ask = tick.ask

    ep1 = float(sig.entry_price) if sig.entry_price else None
    ep2 = float(sig.entry_price_high) if sig.entry_price_high else None

    # Strategia: il segnale Telegram è un'istruzione di entrare ORA.
    # Il "Buy Near 4682-83" indica il prezzo approssimativo, non un livello da aspettare.
    # Se il prezzo è entro la distanza SL dal range → entra a mercato.
    # Solo se il prezzo è oltre lo SL (trade già perso) → non entrare.
    sl_distance = abs(float(entry) - float(sl_raw)) if sl_raw else 0

    entry_lower = min(ep1, ep2) if ep1 and ep2 else (ep1 or ep2 or entry)
    entry_upper = max(ep1, ep2) if ep1 and ep2 else (ep1 or ep2 or entry)
    max_entry = entry_upper + sl_distance
    min_entry = entry_lower - sl_distance

    # Soglia MARKET: per i catch realtime niente tolleranza dal lato sfavorevole
    # (BUY sopra range / SELL sotto range) → in quel caso sempre LIMIT al bordo
    # del range, aspettando il pullback. Per i late-catch (delayed/edited/replay)
    # restiamo invece sulla tolleranza simmetrica originale, perché il pre-check
    # _analyze_late_catch_ticks ha già validato che il prezzo non ha attraversato
    # il range durante il ritardo.
    is_realtime = (catch_origin == "realtime")
    upper_threshold = entry_upper if is_realtime else max_entry
    lower_threshold = entry_lower if is_realtime else min_entry

    if is_buy:
        # BUY: favorevole = prezzo SOTTO range, sfavorevole = prezzo SOPRA.
        if current_ask < min_entry:
            # Troppo sotto (oltre tolleranza) → BUY STOP a entry_lower
            order_type = mt5.ORDER_TYPE_BUY_STOP
            entry = _round_price(float(entry_lower), digits)
            log(f"#{sig.id} BUY STOP: ask={current_ask} < range-SLdist, stop a {entry}")
        elif current_ask <= upper_threshold:
            # Nel range, oppure (solo late-catch) sopra entro tolleranza → MARKET
            order_type = mt5.ORDER_TYPE_BUY
            entry = current_ask
            log(f"#{sig.id} BUY MARKET: ask={current_ask} range={ep1}-{ep2} origin={catch_origin}")
        else:
            # Sopra range → BUY LIMIT a entry_upper, aspetta pullback
            order_type = mt5.ORDER_TYPE_BUY_LIMIT
            entry = _round_price(float(entry_upper), digits)
            log(f"#{sig.id} BUY LIMIT: ask={current_ask} > range, limit a {entry} origin={catch_origin}")
    else:
        # SELL: favorevole = prezzo SOPRA range, sfavorevole = prezzo SOTTO.
        if current_bid > max_entry:
            # Troppo sopra (oltre tolleranza) → SELL STOP a entry_upper
            order_type = mt5.ORDER_TYPE_SELL_STOP
            entry = _round_price(float(entry_upper), digits)
            log(f"#{sig.id} SELL STOP: bid={current_bid} > range+SLdist, stop a {entry}")
        elif current_bid >= lower_threshold:
            # Nel range, oppure (solo late-catch) sotto entro tolleranza → MARKET
            order_type = mt5.ORDER_TYPE_SELL
            entry = current_bid
            log(f"#{sig.id} SELL MARKET: bid={current_bid} range={ep1}-{ep2} origin={catch_origin}")
        else:
            # Sotto range → SELL LIMIT a entry_lower, aspetta pullback
            order_type = mt5.ORDER_TYPE_SELL_LIMIT
            entry = _round_price(float(entry_lower), digits)
            log(f"#{sig.id} SELL LIMIT: bid={current_bid} < range, limit a {entry} origin={catch_origin}")

    if sl and abs(entry - sl) < min_dist:
        log(f"#{sig.id} SL troppo vicino al prezzo, skip")
        _append_trade_log_mt5(sig, "mt5_skip", f"SL={sl} troppo vicino a entry={entry} (min_dist={min_dist:.5f}), ordini non inviati")
        return []

    # Validazione direzione SL: per BUY sl deve essere sotto entry, per SELL sopra.
    # Se l'SL del segnale è dal lato sbagliato è un typo del trader. Per evitare
    # auto-correzioni speculative (vedi #270 dove il typo era distribuito su più
    # campi e non si poteva indovinare), proviamo l'auto-correzione SOLO se è
    # spiegabile cambiando una singola cifra dell'SL e il candidato risultante è
    # univoco. In caso contrario annulliamo e aspettiamo l'edit del trader.
    #
    # Algoritmo (single-digit candidate):
    #   - genera tutti i candidati ottenuti sostituendo UNA cifra dell'SL
    #   - filtra: lato giusto rispetto all'entry e SL_dist in [0.3x, 3x] di TP1_dist
    #   - se esattamente 1 candidato passa → auto-correzione (ambiguità nulla)
    #   - se ≥2 o 0 → cancel + aspetta edit (ambiguità reale)
    if sl:
        sl_side_wrong = (is_buy and sl >= entry) or (not is_buy and sl <= entry)
        if sl_side_wrong:
            tp1_for_check = sig.tp1
            tp1_dist = abs(float(tp1_for_check) - float(entry)) if tp1_for_check else 0
            candidates = []
            if tp1_for_check and tp1_dist > 0:
                # Genera candidati: per ogni cifra di sl_str (escluso il punto
                # decimale), sostituiscila con 0..9 e valuta.
                sl_str = f"{sl:.{digits}f}".rstrip("0").rstrip(".") if digits else f"{int(sl)}"
                # Lavoriamo sulla rappresentazione intera/decimale completa
                sl_str_full = f"{sl:.{digits}f}" if digits else f"{int(sl)}"
                seen = set()
                for i, ch in enumerate(sl_str_full):
                    if not ch.isdigit():
                        continue
                    for d in "0123456789":
                        if d == ch:
                            continue
                        cand_str = sl_str_full[:i] + d + sl_str_full[i+1:]
                        try:
                            cand = float(cand_str)
                        except ValueError:
                            continue
                        if cand in seen or cand <= 0:
                            continue
                        seen.add(cand)
                        # Lato giusto?
                        side_ok = (is_buy and cand < float(entry)) or (not is_buy and cand > float(entry))
                        if not side_ok:
                            continue
                        # Distanza plausibile? R/R fra 0.3 e 3 rispetto a TP1
                        cand_dist = abs(cand - float(entry))
                        if cand_dist < tp1_dist * 0.3 or cand_dist > tp1_dist * 3:
                            continue
                        candidates.append(cand)

            if len(candidates) == 1:
                sl_corrected = round(candidates[0], digits)
                msg = (f"SL={sl} dal lato sbagliato per {'BUY' if is_buy else 'SELL'} "
                       f"entry={entry}: typo isolato a singola cifra, candidato univoco → "
                       f"SL={sl_corrected} (TP1={tp1_for_check})")
                log(f"#{sig.id} {msg}")
                _append_trade_log_mt5(sig, "mt5_sl_autocorrect", msg)
                sig.notes = (sig.notes or "") + f" [SL auto-corretto: {sl} → {sl_corrected}]"
                sl = sl_corrected
            else:
                # Ambiguo (≥2 candidati) o nessun candidato plausibile (0):
                # tipicamente errore distribuito su più campi → cancel + aspetta edit.
                from database import SessionLocal as _SL
                if len(candidates) == 0:
                    detail = "nessun candidato single-digit plausibile"
                else:
                    detail = f"{len(candidates)} candidati ambigui ({sorted(candidates)[:5]})"
                msg = (f"SL={sl} dal lato sbagliato per {'BUY' if is_buy else 'SELL'} "
                       f"entry={entry}: {detail}, in attesa di edit del trader")
                log(f"#{sig.id} {msg}")
                sig.status = "cancelled"
                sig.notes = (sig.notes or "") + f" [Sospeso SL: {msg}]"
                _append_trade_log_mt5(sig, "sl_suspect", msg)
                _db = _SL()
                try:
                    _db.merge(sig)
                    _db.commit()
                finally:
                    _db.close()
                return []

    is_market = order_type in (mt5.ORDER_TYPE_BUY, mt5.ORDER_TYPE_SELL)
    action = mt5.TRADE_ACTION_DEAL if is_market else mt5.TRADE_ACTION_PENDING

    order_type_names = {
        mt5.ORDER_TYPE_BUY: "BUY MARKET", mt5.ORDER_TYPE_SELL: "SELL MARKET",
        mt5.ORDER_TYPE_BUY_LIMIT: "BUY LIMIT", mt5.ORDER_TYPE_SELL_LIMIT: "SELL LIMIT",
        mt5.ORDER_TYPE_BUY_STOP: "BUY STOP", mt5.ORDER_TYPE_SELL_STOP: "SELL STOP",
    }
    order_type_str = order_type_names.get(order_type, str(order_type))
    _append_trade_log_mt5(sig, "mt5_preparing", f"Tipo ordine: {order_type_str} | symbol={mt5_sym} | entry={entry} | sl={sl} | lots_each={lots_each} | ask={current_ask} | bid={current_bid}")

    tickets = []
    for tp_num, tp_price in tps_raw:
        tp = _round_price(float(tp_price), digits)
        # Verifica distanza TP
        if tp and abs(entry - tp) < min_dist:
            log(f"#{sig.id} TP{tp_num} troppo vicino, skip questo TP")
            _append_trade_log_mt5(sig, "mt5_tp_skip", f"TP{tp_num}={tp} troppo vicino a entry={entry}")
            continue
        ticket = _send_single_order(mt5, mt5_sym, order_type, action,
                                    entry, sl, tp, lots_each, sig.id, tp_num, digits, is_buy)
        if ticket:
            tickets.append(ticket)
            _append_trade_log_mt5(sig, "mt5_order_sent", f"TP{tp_num}: ticket={ticket} | {order_type_str} | entry={entry} | sl={sl} | tp={tp} | lots={lots_each}", {"ticket": ticket, "tp_num": tp_num})
            log(f"#{sig.id} TP{tp_num} ticket={ticket} lots={lots_each} entry={entry} sl={sl} tp={tp}")
        else:
            _append_trade_log_mt5(sig, "mt5_order_failed", f"TP{tp_num}: ordine FALLITO | {order_type_str} | entry={entry} | sl={sl} | tp={tp}")
            log(f"#{sig.id} TP{tp_num} FALLITO entry={entry} sl={sl} tp={tp}")

    # Salva sul signal i lotti totali EFFETTIVAMENTE piazzati su MT5,
    # così il frontend mostra il valore reale e non un ricalcolo teorico.
    if tickets:
        sig.position_size = round(lots_each * len(tickets), 2)

    return tickets


def place_order(sig) -> Optional[int]:
    """Compatibilità: piazza ordini e ritorna il primo ticket."""
    tickets = place_orders(sig)
    return tickets[0] if tickets else None


def backfill_position_size() -> list:
    """
    One-shot: per ogni segnale con MT5 ticket attivo (non cancelled), ricalcola
    position_size dai deal di entrata (DEAL_ENTRY_IN) reali su MT5.
    Utile per sistemare i record salvati con il vecchio ricalcolo teorico.
    """
    from database import SessionLocal, Signal
    import json as jsonlib

    mt5 = _get_mt5()
    if mt5 is None:
        return []

    db = SessionLocal()
    try:
        sigs = db.query(Signal).filter(
            (Signal.mt5_ticket.isnot(None)) | (Signal.mt5_tickets.isnot(None)),
            Signal.status != "cancelled",
        ).all()

        updated = []
        for sig in sigs:
            tickets = jsonlib.loads(sig.mt5_tickets) if sig.mt5_tickets else [sig.mt5_ticket]
            total_vol = 0.0
            for t in tickets:
                deals = mt5.history_deals_get(position=t)
                if not deals:
                    continue
                for d in deals:
                    if d.entry == mt5.DEAL_ENTRY_IN:
                        total_vol += d.volume
                        break
            if total_vol > 0:
                new_size = round(total_vol, 2)
                if sig.position_size != new_size:
                    log(f"#{sig.id} backfill position_size: {sig.position_size} -> {new_size}")
                    sig.position_size = new_size
                    db.add(sig)
                    updated.append({"id": sig.id, "old": None, "new": new_size, "tickets": tickets})
        db.commit()
        return updated
    finally:
        db.close()


def _send_with_retry(mt5, request: dict, label: str, attempts: int = 3, delay: float = 0.5):
    """Invia un order_send con retry breve. Restituisce (ok, result, last_err)."""
    import time as _time
    last_result = None
    last_err = None
    for i in range(attempts):
        result = mt5.order_send(request)
        last_result = result
        if result is None:
            last_err = mt5.last_error()
            log(f"{label} tentativo {i+1}/{attempts}: result=None last_error={last_err}")
        elif result.retcode == mt5.TRADE_RETCODE_DONE:
            return True, result, None
        else:
            last_err = (result.retcode, getattr(result, 'comment', ''))
            log(f"{label} tentativo {i+1}/{attempts}: retcode={result.retcode} comment='{last_err[1]}'")
        if i < attempts - 1:
            _time.sleep(delay)
    return False, last_result, last_err


def modify_sl(ticket: int, new_sl: float, symbol: str) -> bool:
    """Modifica lo SL di una posizione aperta. Skip silenzioso se è già al valore
    richiesto. Retry breve (3 tentativi) se MT5 risponde None o errore transitorio."""
    return modify_sl_tp(ticket, new_sl, None, symbol)


def modify_sl_tp(ticket: int, new_sl: Optional[float], new_tp: Optional[float], symbol: str) -> bool:
    """Modifica SL e/o TP di una posizione aperta o un ordine pendente.
    Passa None per lasciare invariato uno dei due. Retry su errore transitorio."""
    mt5 = _get_mt5()
    if mt5 is None:
        return False

    mt5_sym = MT5_SYMBOL_MAP.get(symbol.upper(), symbol)
    sym_info = mt5.symbol_info(mt5_sym)
    digits = sym_info.digits if sym_info else 5
    new_sl_rounded = round(new_sl, digits) if new_sl is not None else None
    new_tp_rounded = round(new_tp, digits) if new_tp is not None else None

    positions = mt5.positions_get(ticket=ticket)
    if not positions:
        # Potrebbe essere un ordine pending
        orders = mt5.orders_get(ticket=ticket)
        if not orders:
            log(f"Ticket {ticket} non trovato")
            return False
        order = orders[0]
        target_sl = new_sl_rounded if new_sl_rounded is not None else order.sl
        target_tp = new_tp_rounded if new_tp_rounded is not None else order.tp
        # Skip se entrambi già al valore richiesto (evita retcode=10025 in loop)
        if (round(order.sl, digits) == target_sl and round(order.tp, digits) == target_tp):
            return True
        request = {
            "action": mt5.TRADE_ACTION_MODIFY,
            "order": ticket,
            "price": order.price_open,
            "sl": target_sl,
            "tp": target_tp,
            "type_time": mt5.ORDER_TIME_GTC,
        }
    else:
        pos = positions[0]
        target_sl = new_sl_rounded if new_sl_rounded is not None else pos.sl
        target_tp = new_tp_rounded if new_tp_rounded is not None else pos.tp
        if (round(pos.sl, digits) == target_sl and round(pos.tp, digits) == target_tp):
            return True
        request = {
            "action": mt5.TRADE_ACTION_SLTP,
            "position": ticket,
            "symbol": pos.symbol,
            "sl": target_sl,
            "tp": target_tp,
        }

    label = f"modify_sl_tp ticket={ticket} new_sl={new_sl_rounded} new_tp={new_tp_rounded}"
    ok, result, err = _send_with_retry(mt5, request, label, attempts=3, delay=0.5)
    status = "OK" if ok else f"FAIL last_err={err}"
    log(f"{label} -> {status}")
    return ok


def modify_order(ticket: int, symbol: str, new_entry: float = None, new_sl: float = None, new_tp: float = None) -> bool:
    """Modifica entry/sl/tp di un ordine pendente."""
    mt5 = _get_mt5()
    if mt5 is None:
        return False

    mt5_sym = MT5_SYMBOL_MAP.get(symbol.upper(), symbol)
    sym_info = mt5.symbol_info(mt5_sym)
    digits = sym_info.digits if sym_info else 5

    orders = mt5.orders_get(ticket=ticket)
    if not orders:
        log(f"modify_order: ticket {ticket} non è un ordine pendente")
        return False
    order = orders[0]
    request = {
        "action": mt5.TRADE_ACTION_MODIFY,
        "order": ticket,
        "price": round(new_entry, digits) if new_entry else order.price_open,
        "sl": round(new_sl, digits) if new_sl else order.sl,
        "tp": round(new_tp, digits) if new_tp else order.tp,
        "type_time": mt5.ORDER_TIME_GTC,
    }
    result = mt5.order_send(request)
    ok = result and result.retcode == mt5.TRADE_RETCODE_DONE
    status = "OK" if ok else f"FAIL retcode={result.retcode if result else '?'}"
    log(f"modify_order ticket={ticket} entry={request['price']} sl={request['sl']} tp={request['tp']} -> {status}")
    return ok


def close_position(ticket: int, symbol: str) -> bool:
    """Chiude una posizione aperta o cancella un ordine pendente."""
    mt5 = _get_mt5()
    if mt5 is None:
        return False

    # Prova prima come posizione aperta
    positions = mt5.positions_get(ticket=ticket)
    if positions:
        pos = positions[0]
        close_type = mt5.ORDER_TYPE_SELL if pos.type == mt5.POSITION_TYPE_BUY else mt5.ORDER_TYPE_BUY
        tick = mt5.symbol_info_tick(pos.symbol)
        price = tick.bid if close_type == mt5.ORDER_TYPE_SELL else tick.ask

        request = {
            "action":    mt5.TRADE_ACTION_DEAL,
            "position":  ticket,
            "symbol":    pos.symbol,
            "volume":    pos.volume,
            "type":      close_type,
            "price":     price,
            "deviation": 20,
            "magic":     20250326,
            "comment":   "IC-close",
            "type_filling": mt5.ORDER_FILLING_IOC,
        }
        result = mt5.order_send(request)
        ok = result and result.retcode == mt5.TRADE_RETCODE_DONE
        status = "OK" if ok else f"FAIL {result.retcode if result else '?'}"
        log(f"close_position ticket={ticket} -> {status}")
        return ok

    # Prova come ordine pendente (BUY_LIMIT / SELL_LIMIT / ecc.)
    orders = mt5.orders_get(ticket=ticket)
    if orders:
        request = {
            "action": mt5.TRADE_ACTION_REMOVE,
            "order":  ticket,
        }
        result = mt5.order_send(request)
        ok = result and result.retcode == mt5.TRADE_RETCODE_DONE
        status = "OK" if ok else f"FAIL {result.retcode if result else '?'}"
        log(f"cancel_pending ticket={ticket} -> {status}")
        return ok

    log(f"close_position: ticket {ticket} non trovato (ne posizione ne ordine pendente)")
    return False


def cancel_expired_signals():
    """
    1. Cancella segnali pending il cui prezzo è andato oltre lo SL.
    2. Cancella segnali attivi senza mt5_ticket (non eseguiti su MT5).
    """
    from database import SessionLocal, Signal
    import json as jsonlib

    mt5 = _get_mt5()
    if mt5 is None:
        return []

    db = SessionLocal()
    try:
        # Segnali attivi senza ticket MT5 → mai eseguiti, annulla
        orphans = db.query(Signal).filter(
            Signal.status.in_(["open", "tp1", "tp2"]),
            Signal.mt5_ticket.is_(None),
        ).all()
        cancelled = []
        for sig in orphans:
            sig.status = "cancelled"
            sig.updated_at = datetime.utcnow()
            sig.notes = (sig.notes or "") + " [Non eseguito su MT5]"
            db.add(sig)
            cancelled.append(sig.id)
            log(f"#{sig.id} {sig.symbol} annullato: attivo senza ticket MT5")

        # Segnali pending oltre SL
        pending = db.query(Signal).filter(Signal.status == "pending").all()

        for sig in pending:
            mt5_sym = MT5_SYMBOL_MAP.get(sig.symbol.upper(), sig.symbol)
            tick = mt5.symbol_info_tick(mt5_sym)
            if not tick:
                continue

            is_buy = (sig.direction or "buy").lower() == "buy"
            sl = sig.stoploss
            if not sl:
                continue

            price = tick.ask if is_buy else tick.bid
            # Se il prezzo è andato oltre SL → annulla
            past_sl = (is_buy and price <= sl) or (not is_buy and price >= sl)
            if not past_sl:
                continue

            log(f"#{sig.id} {sig.symbol} prezzo={price} oltre SL={sl} → annullo segnale pending")

            # Cancella ordini MT5 pending associati
            if sig.mt5_tickets:
                for ticket in jsonlib.loads(sig.mt5_tickets):
                    orders = mt5.orders_get(ticket=ticket)
                    if orders:
                        mt5.order_send({
                            "action": mt5.TRADE_ACTION_REMOVE,
                            "order": ticket,
                        })
                        log(f"  Rimosso ordine pending MT5 ticket={ticket}")

            sig.status = "cancelled"
            sig.updated_at = datetime.utcnow()
            db.add(sig)
            cancelled.append(sig.id)

        if cancelled:
            db.commit()
            log(f"Annullati {len(cancelled)} segnali expired: {cancelled}")
        return cancelled
    finally:
        db.close()


def get_account_info() -> dict:
    """Ritorna le info dell'account MT5."""
    mt5 = _get_mt5()
    if mt5 is None:
        return {}
    info = mt5.account_info()
    if not info:
        return {}
    return {
        "login":   info.login,
        "name":    info.name,
        "balance": info.balance,
        "equity":  info.equity,
        "margin":  info.margin,
        "free_margin": info.margin_free,
        "profit":  info.profit,
        "server":  info.server,
        "demo":    info.trade_mode == 0,
    }


def switch_account(login: int, server: str) -> dict:
    """Cambia l'account MT5 attivo."""
    global MT5_ACCOUNT, MT5_SERVER
    import MetaTrader5 as mt5
    mt5.shutdown()
    if not mt5.initialize(login=login, server=server):
        err = mt5.last_error()
        log(f"Switch account fallito ({login}@{server}): {err}")
        # Ripristina il precedente
        mt5.initialize(login=MT5_ACCOUNT, server=MT5_SERVER)
        return {"ok": False, "error": f"Connessione fallita: {err}"}
    info = mt5.account_info()
    if not info or info.login != login:
        log(f"Switch account: login mismatch {info.login if info else '?'} != {login}")
        mt5.shutdown()
        mt5.initialize(login=MT5_ACCOUNT, server=MT5_SERVER)
        return {"ok": False, "error": "Account non trovato nel terminale MT5"}
    MT5_ACCOUNT = login
    MT5_SERVER = server
    log(f"Account cambiato: {login}@{server} ({info.name}) balance={info.balance}")
    return {
        "ok": True,
        "login": info.login,
        "name": info.name,
        "balance": info.balance,
        "server": info.server,
        "demo": info.trade_mode == 0,
    }


def get_open_positions() -> list:
    """Lista posizioni aperte dai nostri ordini (magic=20250326)."""
    mt5 = _get_mt5()
    if mt5 is None:
        return []
    positions = mt5.positions_get()
    if positions is None:
        return []
    result = []
    for p in positions:
        if p.magic == 20250326:
            result.append({
                "ticket":  p.ticket,
                "symbol":  p.symbol,
                "type":    "buy" if p.type == 0 else "sell",
                "volume":  p.volume,
                "price_open": p.price_open,
                "sl":      p.sl,
                "tp":      p.tp,
                "profit":  p.profit,
                "comment": p.comment,
            })
    return result


def _find_close_deal(mt5, ticket: int, sig, hist_from):
    """Cerca il deal di chiusura per un ticket nello storico MT5.

    USA history_deals_get(position=ticket) — query diretta per position_id,
    non dipende da range di date o cache del server.
    """
    # Metodo diretto: interroga per position_id senza range date
    deals = mt5.history_deals_get(position=ticket)
    if deals:
        for d in sorted(deals, key=lambda x: x.time, reverse=True):
            if d.entry == mt5.DEAL_ENTRY_OUT:
                return d
    # Fallback: range date ampio
    deals = mt5.history_deals_get(hist_from, datetime.utcnow())
    if deals:
        for d in sorted(deals, key=lambda x: x.time, reverse=True):
            if d.position_id == ticket and d.entry == mt5.DEAL_ENTRY_OUT:
                return d
    return None


def _build_mt5_trade_log(sig, closed_tickets, is_buy, new_status) -> str:
    """Costruisce un trade_log JSON dagli eventi MT5 reali.
    closed_tickets: list of (ticket, close_price, profit, close_ts_utc)
    """
    import json as jsonlib
    events = []
    entry_ts = sig.entered_at.isoformat() if sig.entered_at else None
    if sig.actual_entry_price:
        events.append({"event": "entry", "price": round(sig.actual_entry_price, 5), "ts": entry_ts})

    for tp_num in [1, 2, 3]:
        tp_price = getattr(sig, f'tp{tp_num}', None)
        if tp_price is None:
            continue
        matching = [(cp, profit, ts) for _, cp, profit, ts in closed_tickets
                    if (is_buy and cp >= tp_price) or (not is_buy and cp <= tp_price)]
        if matching:
            cp_ev, profit_ev, ts_ev = matching[0]
            events.append({"event": f"tp{tp_num}", "price": round(cp_ev, 5),
                           "pnl": round(profit_ev, 2),
                           "ts": ts_ev.isoformat() if ts_ev else None})

    # Ticket chiusi al breakeven o SL (non matchati con nessun TP)
    tp_set = set()
    for tp_num in [1, 2, 3]:
        tp_price = getattr(sig, f'tp{tp_num}', None)
        if tp_price is None:
            continue
        for ticket, cp, profit, ts in closed_tickets:
            if (is_buy and cp >= tp_price) or (not is_buy and cp <= tp_price):
                tp_set.add(ticket)

    for ticket, cp, profit, ts in closed_tickets:
        if ticket in tp_set:
            continue
        # Chiuso senza raggiungere TP → breakeven o SL
        is_be = sig.actual_entry_price and abs(cp - sig.actual_entry_price) < abs(cp) * 0.001
        event_name = "breakeven" if is_be else "sl_hit"
        events.append({"event": event_name, "price": round(cp, 5),
                       "pnl": round(profit, 2),
                       "ts": ts.isoformat() if ts else None})

    return jsonlib.dumps(events)


def sync_positions() -> list:
    """
    Confronta posizioni/ordini MT5 con segnali 'open' nel DB.
    Gestisce multipli ticket per segnale (TP1/TP2/TP3 separati).
    Quando TP1 chiude → sposta SL a breakeven sugli altri.
    """
    from database import SessionLocal, Signal
    import json as jsonlib
    from datetime import timedelta

    mt5 = _get_mt5()
    if mt5 is None:
        return []

    db = SessionLocal()
    try:
        open_sigs = db.query(Signal).filter(
            Signal.mt5_ticket.isnot(None),
            Signal.closed_at.is_(None),
            Signal.status.in_(["open", "pending", "tp1", "tp2"])
        ).all()

        if not open_sigs:
            return []

        open_positions  = {p.ticket: p for p in (mt5.positions_get() or [])}
        pending_orders  = {o.ticket: o for o in (mt5.orders_get() or [])}

        updated = []
        for sig in open_sigs:
            is_buy   = sig.direction.lower() == "buy"
            # Cerca deal dalla creazione del segnale (max 7 giorni fa)
            sig_date = sig.created_at or (datetime.utcnow() - timedelta(days=7))
            hist_from = max(sig_date - timedelta(hours=1), datetime.utcnow() - timedelta(days=7))

            # Recupera lista ticket (nuovi segnali) o usa il singolo ticket (vecchi)
            if sig.mt5_tickets:
                tickets = jsonlib.loads(sig.mt5_tickets)
            else:
                tickets = [sig.mt5_ticket]

            total_profit   = 0.0
            total_volume   = 0.0   # somma dei lotti effettivi su MT5 (per popolare position_size)
            open_count     = 0
            positions_count = 0
            pendings_count = 0
            closed_tickets = []
            tp1_hit        = False

            for ticket in tickets:
                if ticket in open_positions:
                    pos = open_positions[ticket]
                    total_profit += pos.profit
                    total_volume += pos.volume
                    open_count += 1
                    positions_count += 1
                    # Cattura actual_entry_price quando un BUY/SELL LIMIT viene riempito
                    if not sig.actual_entry_price and pos.price_open:
                        sig.actual_entry_price = pos.price_open
                        log(f"#{sig.id} actual_entry_price={pos.price_open} (da posizione aperta)")
                    continue

                if ticket in pending_orders:
                    total_volume += pending_orders[ticket].volume_initial
                    open_count += 1
                    pendings_count += 1
                    continue

                # Ticket chiuso — cerca deal
                close_deal = _find_close_deal(mt5, ticket, sig, hist_from)
                if close_deal:
                    profit      = close_deal.profit
                    close_price = close_deal.price
                    close_ts    = _get_mt5_utc(close_deal.time)
                    total_profit += profit
                    closed_tickets.append((ticket, close_price, profit, close_ts))
                    # Recupera deal di entrata: serve sia per actual_entry_price sia per volume iniziale
                    entry_deals = mt5.history_deals_get(position=ticket)
                    if entry_deals:
                        for ed in entry_deals:
                            if ed.entry == mt5.DEAL_ENTRY_IN:
                                total_volume += ed.volume
                                if not sig.actual_entry_price:
                                    sig.actual_entry_price = ed.price
                                    sig.entered_at = _get_mt5_utc(ed.time)
                                    log(f"#{sig.id} actual_entry_price={ed.price} entered_at={sig.entered_at} (da deal)")
                                break

                    # Determina se è TP1
                    if sig.tp1 and ((is_buy and close_price >= sig.tp1) or
                                    (not is_buy and close_price <= sig.tp1)):
                        tp1_hit = True
                else:
                    # Nessun deal trovato — controlla se l'ordine è stato cancellato dal broker
                    hist_orders = mt5.history_orders_get(ticket=ticket)
                    if hist_orders and hist_orders[0].state == 2:  # ORDER_STATE_CANCELED
                        log(f"#{sig.id} ticket={ticket} ordine CANCELLATO dal broker (reason={hist_orders[0].reason})")
                        # Non incrementa open_count → ticket morto
                    else:
                        sig_age = (datetime.utcnow() - sig.created_at).total_seconds() if sig.created_at else 9999
                        if sig_age < 1800:
                            open_count += 1
                        else:
                            log(f"#{sig.id} ticket={ticket} non trovato in MT5 dopo {int(sig_age/60)}min — orfano")

            # Se TP1 è stato raggiunto → sposta SL a breakeven sugli ordini ancora aperti
            if tp1_hit and open_count > 0 and sig.actual_entry_price:
                be_price = round(sig.actual_entry_price, 5)
                for ticket in tickets:
                    if ticket in open_positions:
                        modify_sl(ticket, be_price, sig.symbol)
                        log(f"#{sig.id} TP1 hit → breakeven SL={be_price} su ticket={ticket}")
                    elif ticket in pending_orders:
                        modify_sl(ticket, be_price, sig.symbol)

            # Aggiorna P&L live (somma profit aperte + chiuse parziali)
            sig.pnl_usd = round(total_profit, 2)

            # Aggiorna position_size con la somma reale dei lotti su MT5,
            # così il valore mostrato nel frontend riflette quello effettivo
            # (e non un ricalcolo teorico che spesso differisce dalla realtà).
            if total_volume > 0:
                new_size = round(total_volume, 2)
                if sig.position_size != new_size:
                    sig.position_size = new_size

            # Allinea lo status con la realtà MT5:
            # - solo pending → 'pending' (ordini non ancora fillati)
            # - almeno una posizione aperta → 'open' (se non è già tp1/tp2)
            if sig.status in ("open", "pending") and not closed_tickets:
                if positions_count == 0 and pendings_count > 0 and sig.status != "pending":
                    sig.status = "pending"
                    log(f"#{sig.id} status allineato: pending ({pendings_count} ordini pending, 0 posizioni aperte)")
                elif positions_count > 0 and sig.status == "pending":
                    sig.status = "open"
                    log(f"#{sig.id} status allineato: open ({positions_count} posizioni aperte)")

            # Tutti i ticket orfani (non trovati in MT5 dopo 30min) → marca cancelled
            if open_count == 0 and not closed_tickets:
                sig_age = (datetime.utcnow() - sig.created_at).total_seconds() if sig.created_at else 0
                if sig_age > 1800:
                    log(f"#{sig.id} tutti i ticket orfani dopo {int(sig_age/60)}min → cancelled")
                    sig.status = "cancelled"
                    sig.notes = (sig.notes or "") + " [Ticket MT5 non trovati in storia — ordini mai eseguiti o account resettato]"
                    sig.updated_at = datetime.utcnow()
                    db.add(sig)
                    updated.append(sig.id)
                    continue

            if open_count == 0 and closed_tickets:
                # Tutti i ticket chiusi → segnale completato
                last_close = max(closed_tickets, key=lambda x: x[3])  # più recente per timestamp
                close_price = last_close[1]
                close_time  = last_close[3]  # timestamp UTC reale del deal

                # Determina status dal TP più alto raggiunto
                new_status = "sl_hit"
                for tp_num, tp_price in [(3, sig.tp3), (2, sig.tp2), (1, sig.tp1)]:
                    if tp_price is None:
                        continue
                    if any((is_buy and cp >= tp_price) or (not is_buy and cp <= tp_price)
                           for _, cp, _, _ in closed_tickets):
                        new_status = f"tp{tp_num}"
                        break

                sig.status     = new_status
                sig.exit_price = close_price
                sig.closed_at  = close_time
                sig.updated_at = datetime.utcnow()
                # Scrivi trade_log MT5 (per la freccia dettaglio in frontend)
                if not sig.trade_log:
                    sig.trade_log = _build_mt5_trade_log(sig, closed_tickets, is_buy, new_status)
                db.add(sig)
                updated.append(sig.id)
                log(f"#{sig.id} {sig.symbol} completato: {new_status} profit={total_profit:.2f}")
            else:
                db.add(sig)

        db.commit()
        return updated
    finally:
        db.close()
