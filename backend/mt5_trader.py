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
MT5_SYMBOL_MAP_XM = {
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
    "USTECH":  "US100Cash#",
    "US100":   "US100Cash#",
    "NAS100":  "US100Cash#",
    "NASDAQ":  "US100Cash#",
}

MT5_SYMBOL_MAP_FTMO = {
    "XAUUSD":  "XAUUSD",
    "XAGUSD":  "XAGUSD",
    "EURUSD":  "EURUSD",
    "GBPUSD":  "GBPUSD",
    "USDJPY":  "USDJPY",
    "USDCHF":  "USDCHF",
    "USDCAD":  "USDCAD",
    "AUDUSD":  "AUDUSD",
    "NZDUSD":  "NZDUSD",
    "GBPJPY":  "GBPJPY",
    "EURJPY":  "EURJPY",
    "AUDJPY":  "AUDJPY",
    "GBPCHF":  "GBPCHF",
    "EURGBP":  "EURGBP",
    "USTECH":  "US100.cash",
    "US100":   "US100.cash",
    "NAS100":  "US100.cash",
    "NASDAQ":  "US100.cash",
    "BTCUSD":  "BTCUSD",
    "USOIL":   "USOIL.cash",
    "OIL":     "USOIL.cash",
    "WTI":     "USOIL.cash",
    "UKOIL":   "UKOIL.cash",
    "BRENT":   "UKOIL.cash",
}

MT5_SYMBOL_MAP_AVATRADE = {
    "XAUUSD":  "GOLD",
    "XAGUSD":  "SILVER",
    "EURUSD":  "EURUSD",
    "GBPUSD":  "GBPUSD",
    "USDJPY":  "USDJPY",
    "USDCHF":  "USDCHF",
    "USDCAD":  "USDCAD",
    "AUDUSD":  "AUDUSD",
    "NZDUSD":  "NZDUSD",
    "GBPJPY":  "GBPJPY",
    "EURJPY":  "EURJPY",
    "AUDJPY":  "AUDJPY",
    "GBPCHF":  "GBPCHF",
    "EURGBP":  "EURGBP",
    "USTECH":  "US_TECH100",
    "US100":   "US_TECH100",
    "NAS100":  "US_TECH100",
    "NASDAQ":  "US_TECH100",
    "BTCUSD":  "BTCUSD",
    "ETHUSD":  "ETHUSD",
    "USOIL":   "CrudeOIL",
    "OIL":     "CrudeOIL",
    "WTI":     "CrudeOIL",
    "UKOIL":   "BRENT_OIL",
    "BRENT":   "BRENT_OIL",
}


def get_mt5_symbol(symbol: str, default=None):
    """Mappa symbol logico -> simbolo MT5 specifico del broker attivo."""
    s = symbol.upper()
    broker = (MT5_BROKER or "").lower()
    if broker == "avatrade":
        return MT5_SYMBOL_MAP_AVATRADE.get(s, default if default is not None else symbol)
    if broker == "ftmo":
        return MT5_SYMBOL_MAP_FTMO.get(s, default if default is not None else symbol)
    return MT5_SYMBOL_MAP_XM.get(s, default if default is not None else symbol)


# Compat alias: alcuni call-site fanno MT5_SYMBOL_MAP.get(...)
class _BrokerSymbolMap:
    def _active(self):
        broker = (MT5_BROKER or "").lower()
        if broker == "avatrade":
            return MT5_SYMBOL_MAP_AVATRADE
        if broker == "ftmo":
            return MT5_SYMBOL_MAP_FTMO
        return MT5_SYMBOL_MAP_XM
    def get(self, symbol, default=None):
        return get_mt5_symbol(symbol, default)
    def __getitem__(self, symbol):
        v = get_mt5_symbol(symbol, None)
        if v is None:
            raise KeyError(symbol)
        return v
    def __contains__(self, symbol):
        return get_mt5_symbol(symbol, None) is not None
    def keys(self):
        return self._active().keys()
    def values(self):
        return self._active().values()
    def items(self):
        return self._active().items()
    def __iter__(self):
        return iter(self._active())
    def __len__(self):
        return len(self._active())

MT5_SYMBOL_MAP = _BrokerSymbolMap()

_auto_trade_enabled = False   # toggle globale

# Ultimo errore di modify_sl_tp (tuple (code, message) o None se ok).
# Esposto per i chiamanti che vogliono distinguere "Invalid sl" da altri fail.
_last_modify_error = None

# Pending SL requests: signal_id -> dict.
# Registrate quando modify_sl_tp fallisce con 'Invalid sl' (SL troppo vicino al
# prezzo per il broker). sync_positions le riprocessa ogni 30s.
_pending_sl_requests = {}

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
MT5_BROKER = "xm"  # tag broker corrente; aggiornato da _load_active_account()


def _load_active_account():
    """Legge l'account marcato is_active=True dal DB e aggiorna le globali.
    Se nessuno e' attivo, usa is_default. Se nessun record, lascia i valori da .env.
    Va chiamato all'avvio e dopo ogni switch_account.
    """
    global MT5_ACCOUNT, MT5_SERVER, MT5_PATH, MT5_BROKER
    try:
        from database import SessionLocal as _SL, Mt5Account as _Acc
        db = _SL()
        try:
            acc = db.query(_Acc).filter(_Acc.is_active == True).first()
            if not acc:
                acc = db.query(_Acc).filter(_Acc.is_default == True).first()
            if acc:
                MT5_ACCOUNT = int(acc.login)
                MT5_SERVER = acc.server
                if acc.mt5_path:
                    MT5_PATH = acc.mt5_path
                if acc.broker:
                    MT5_BROKER = acc.broker
                log(f"Account attivo dal DB: {MT5_ACCOUNT}@{MT5_SERVER} broker={MT5_BROKER} path={MT5_PATH}")
        finally:
            db.close()
    except Exception as e:
        log(f"_load_active_account errore: {e} — uso valori .env")


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
        log(f"_get_mt5: MT5_DISABLED env set")
        return None
    try:
        import MetaTrader5 as mt5
        import time as _time
        init_kwargs = {"login": MT5_ACCOUNT, "server": MT5_SERVER}
        if MT5_PATH:
            init_kwargs["path"] = MT5_PATH
        if not mt5.initialize(**init_kwargs):
            err = mt5.last_error()
            log(f"MT5 init fallito (account {MT5_ACCOUNT}@{MT5_SERVER}): {err} — retry in 3s...")
            mt5.shutdown()
            _time.sleep(3)
            if not mt5.initialize(**init_kwargs):
                log(f"MT5 init fallito al 2° tentativo (account {MT5_ACCOUNT}): {mt5.last_error()}")
                return None
        info = mt5.account_info()
        if info and info.login != MT5_ACCOUNT:
            log(f"ATTENZIONE: connesso ad account {info.login} invece di {MT5_ACCOUNT} — blocco operazioni")
            mt5.shutdown()
            return None
        if info is None:
            log(f"_get_mt5: account_info() None dopo init — MT5_ACCOUNT={MT5_ACCOUNT}")
            return None
        # Check critico: AutoTrading abilitato sul terminale (pulsante verde).
        # Se rosso, ogni order_send tornera' retcode 10027 'AutoTrading disabled'.
        try:
            term_info = mt5.terminal_info()
            if term_info and not term_info.trade_allowed:
                log(f"ATTENZIONE: AutoTrading DISABILITATO sul terminale MT5 (pulsante rosso). Abilitalo per piazzare ordini.")
        except Exception:
            pass
        return mt5
    except Exception as e:
        log(f"_get_mt5 EXCEPTION: {e}")
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


def _pick_filling_mode(mt5, mt5_sym):
    """Sceglie il filling mode supportato dal simbolo del broker.
    bitfield: 1=FOK, 2=IOC, 4=Return. XM accetta IOC, Avatrade richiede FOK.
    Preferenza: IOC (legacy XM) > FOK > Return > fallback FOK."""
    si = mt5.symbol_info(mt5_sym)
    fm = (getattr(si, 'filling_mode', 0) or 0) if si else 0
    if fm & 2:
        return mt5.ORDER_FILLING_IOC
    if fm & 1:
        return mt5.ORDER_FILLING_FOK
    if fm & 4:
        return mt5.ORDER_FILLING_RETURN
    return mt5.ORDER_FILLING_FOK


_last_send_error = {}  # sig_id -> (retcode, comment) per ultimo fallimento, letto da place_orders


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
        "type_filling": _pick_filling_mode(mt5, mt5_sym),
    }
    expected_comment = request["comment"]  # IC#xxx ...
    result = mt5.order_send(request)
    if result is None or result.retcode != mt5.TRADE_RETCODE_DONE:
        code = result.retcode if result else "N/A"
        comment = result.comment if result else "N/A"
        try: last_err = mt5.last_error()
        except: last_err = "?"
        log(f"#{sig_id} TP{tp_num} order_send fallito: retcode={code} comment='{comment}' last_error={last_err}")
        # Memorizza errore per il chiamante (place_orders) che lo logga sulla sua session
        _last_send_error[(sig_id, tp_num)] = {"retcode": code, "comment": comment, "last_err": str(last_err)}
        # SAFETY NET: order_send puo' rispondere None per timeout/risposta lenta
        # MA l'ordine puo' essere comunque andato a mercato (caso #327 USDJPY).
        # Cerca sul broker ordini recenti con il nostro comment esatto e usalo.
        import time as _time
        _time.sleep(1.5)  # da' tempo al broker di propagare
        try:
            from datetime import datetime as _dt, timedelta as _td, timezone as _tz
            recent_from = _dt.now(_tz.utc) - _td(seconds=30)
            recent_to = _dt.now(_tz.utc) + _td(seconds=5)
            recent = mt5.history_orders_get(recent_from, recent_to) or []
            for ho in recent:
                if (ho.magic == 20250326 and ho.symbol == mt5_sym
                        and ho.comment == expected_comment and ho.state in (1, 4)):  # PLACED or FILLED
                    log(f"#{sig_id} TP{tp_num} SAFETY NET: trovato ticket={ho.ticket} sul broker (state={ho.state}) — uso questo invece di fallire")
                    return int(ho.ticket)
            # Anche tra le posizioni aperte
            open_pos = mt5.positions_get(symbol=mt5_sym) or []
            for p in open_pos:
                if p.magic == 20250326 and p.comment == expected_comment:
                    log(f"#{sig_id} TP{tp_num} SAFETY NET: trovata posizione aperta ticket={p.ticket} — uso questo")
                    return int(p.ticket)
        except Exception as _e:
            log(f"#{sig_id} TP{tp_num} SAFETY NET errore: {_e}")
        # Annotazione al trade_log: serve sapere PERCHE' su test/diagnostica
        try:
            from database import SessionLocal as _SL, Signal as _SG
            _db = _SL()
            try:
                _ss = _db.query(_SG).get(sig_id)
                if _ss:
                    _append_trade_log_mt5(_ss, "mt5_send_fail",
                        f"TP{tp_num} retcode={code} '{comment}' last_err={last_err}",
                        {"retcode": int(code) if isinstance(code, int) else None, "tp_num": tp_num})
                    _db.merge(_ss); _db.commit()
            finally:
                _db.close()
        except Exception:
            pass
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
                 signal_ts: Optional[datetime] = None, force_market: bool = False) -> list:
    """
    Piazza 2-3 ordini separati (uno per ogni TP) con lotti divisi equamente.
    Ritorna lista di ticket piazzati con successo.

    catch_origin: 'realtime' | 'delayed' | 'edited' | 'replay'
      Se != 'realtime', esegue la pre-check late-catch analizzando i tick
      nell'intervallo [signal_ts, now] e può annullare il segnale se il
      prezzo ha attraversato il range durante il ritardo.
    """
    if not _auto_trade_enabled:
        log(f"#{sig.id} place_orders: auto_trade DISABLED (_auto_trade_enabled=False)")
        _append_trade_log_mt5(sig, "mt5_skip", "auto_trade disabled")
        return []

    # ─── NEWS FILTER Tier 1 (post-mortem #570) ───
    # Nessun nuovo ordine (market o pending) nella finestra di una news
    # high-impact. Copre TUTTE le vie: signal, reenter, replay, enter_now,
    # retry manuale — tutte passano da qui.
    try:
        import news_filter as _nf
        _news_reason = _nf.entry_blocked()
        if _news_reason:
            log(f"#{sig.id} place_orders: BLOCCATO da news filter: {_news_reason}")
            _append_trade_log_mt5(sig, "news_blocked", _news_reason)
            return []
    except Exception as _nf_e:
        log(f"#{sig.id} news filter check err (procedo): {_nf_e}")

    mt5 = _get_mt5()
    if mt5 is None:
        log(f"#{sig.id} place_orders: _get_mt5() returned None (account={MT5_ACCOUNT}, server={MT5_SERVER}, path={MT5_PATH})")
        _append_trade_log_mt5(sig, "mt5_unavailable", f"_get_mt5() None — MT5_ACCOUNT={MT5_ACCOUNT} server={MT5_SERVER}")
        return []

    # Pre-check: AutoTrading abilitato sul terminale. Se disabilitato, NON inviare
    # nulla — risparmiamo round-trip a Avatrade e logghiamo motivazione chiara.
    try:
        term_info = mt5.terminal_info()
        if term_info and not term_info.trade_allowed:
            msg = ("AutoTrading disabilitato sul terminale MT5 (pulsante rosso). "
                   "Abilitarlo manualmente nel terminale per piazzare ordini.")
            log(f"#{sig.id} {msg}")
            from database import SessionLocal as _SL
            sig.status = "cancelled"
            sig.notes = (sig.notes or "") + f" [{msg}]"
            _append_trade_log_mt5(sig, "autotrading_disabled", msg)
            _db = _SL()
            try:
                _db.merge(sig); _db.commit()
            finally:
                _db.close()
            return []
    except Exception:
        pass

    mt5_sym = MT5_SYMBOL_MAP.get(sig.symbol.upper())
    if not mt5_sym:
        log(f"#{sig.id} Simbolo non supportato per trading: {sig.symbol} (broker={MT5_BROKER})")
        _append_trade_log_mt5(sig, "mt5_skip", f"Simbolo {sig.symbol} non in symbol_map (broker={MT5_BROKER})")
        return []

    mt5.symbol_select(mt5_sym, True)
    sym_info = mt5.symbol_info(mt5_sym)
    if sym_info is None:
        log(f"#{sig.id} Symbol info non disponibile per {mt5_sym} (broker={MT5_BROKER})")
        _append_trade_log_mt5(sig, "mt5_skip", f"symbol_info None per {mt5_sym}")
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

    # Sicurezza: niente SL = niente trade. Senza SL la position si dimensiona a
    # min_vol e resta esposta a perdita illimitata. Caso #297: il parser ha
    # estratto un signal da un reminder TG senza SL nel testo e il bot l'ha
    # piazzato comunque con sl=0. Meglio cancellare in attesa di un edit del
    # trader o di un segnale completo.
    if not sl_raw:
        from database import SessionLocal as _SL
        msg = "Segnale senza SL definito - skip per sicurezza, in attesa di edit"
        log(f"#{sig.id} {msg}")
        sig.status = "cancelled"
        sig.notes = (sig.notes or "") + f" [Skip: {msg}]"
        _append_trade_log_mt5(sig, "mt5_skip_no_sl", msg)
        _db = _SL()
        try:
            _db.merge(sig)
            _db.commit()
        finally:
            _db.close()
        return []

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

    # ────────────────────────────────────────────────────────────────────────
    # AUTO-CORREZIONE PRICE-ANCHORED (pipeline unificata): ancora al prezzo
    # broker corrente, classifica ogni livello come OK/ANOMALO, genera
    # candidati single-digit per gli ANOMALI scegliendo quello piu' vicino al
    # prezzo, valida che il pattern complessivo (SL/entry/TP) sia semanticamente
    # coerente per il side del trade. Copre:
    #   - tipi misti (#356: entry/TP off ma SL corretto)
    #   - typo sistematico uniforme (#348: tutti i livelli off di stessa cifra)
    #   - typo isolato TP1/TP2/TP3 (#351 e simmetrici)
    # Se applica fix uniforme + monotono, prosegue. Altrimenti fall-through
    # ai check legacy (sistematico, sl_autocorrect, tp_fix) che gestiscono
    # casi che la pipeline non puo' risolvere.
    try:
        cur_price = (tick.bid + tick.ask) / 2.0 if tick and tick.bid > 0 and tick.ask > 0 else None
        if cur_price:
            raw_levels = {}
            if sig.entry_price: raw_levels['entry_low'] = float(sig.entry_price)
            if sig.entry_price_high and sig.entry_price_high != sig.entry_price:
                raw_levels['entry_high'] = float(sig.entry_price_high)
            if sl_raw: raw_levels['sl'] = float(sl_raw)
            for n in (1, 2, 3):
                v = getattr(sig, f'tp{n}', None)
                if v: raw_levels[f'tp{n}'] = float(v)
            if len(raw_levels) >= 3:
                # OK = entro 1% prezzo; ANOMALO = oltre 1%
                ok_tol_pct = 0.01
                ANOMALO_set = set()
                for label, val in raw_levels.items():
                    if abs(val - cur_price) > cur_price * ok_tol_pct:
                        ANOMALO_set.add(label)
                if ANOMALO_set:
                    # Per ogni anomalo: cerca single-digit candidato piu' vicino
                    # al prezzo entro 5% (filtro contro candidati assurdi)
                    fixes = {}
                    feasible = True
                    for label in ANOMALO_set:
                        val = raw_levels[label]
                        val_str = f"{val:.{digits}f}" if digits else f"{int(val)}"
                        cands = []
                        seen = set()
                        for ip, ch in enumerate(val_str):
                            if not ch.isdigit():
                                continue
                            for d in "0123456789":
                                if d == ch:
                                    continue
                                cs = val_str[:ip] + d + val_str[ip+1:]
                                try:
                                    cv = float(cs)
                                except ValueError:
                                    continue
                                if cv in seen or cv <= 0:
                                    continue
                                seen.add(cv)
                                if abs(cv - cur_price) <= cur_price * 0.05:
                                    cands.append(cv)
                        if not cands:
                            feasible = False
                            break
                        fixes[label] = min(cands, key=lambda c: abs(c - cur_price))
                    if feasible:
                        # Valida monotonia del pattern dopo i fix
                        new_levels = {l: fixes.get(l, raw_levels[l]) for l in raw_levels}
                        order = [l for l in ('sl', 'entry_low', 'entry_high', 'tp1', 'tp2', 'tp3') if l in new_levels]
                        seq = [new_levels[l] for l in order]
                        if is_buy:
                            monotone = all(seq[i] <= seq[i+1] for i in range(len(seq)-1))
                        else:
                            monotone = all(seq[i] >= seq[i+1] for i in range(len(seq)-1))
                        if monotone and fixes:
                            # Applica i fix
                            if 'entry_low' in fixes:
                                sig.entry_price = round(fixes['entry_low'], digits)
                                entry = sig.entry_price
                            if 'entry_high' in fixes:
                                sig.entry_price_high = round(fixes['entry_high'], digits)
                                entry_high = sig.entry_price_high
                            if 'sl' in fixes:
                                sig.stoploss = round(fixes['sl'], digits)
                                sl_raw = sig.stoploss
                                sl = sl_raw
                            for n in (1, 2, 3):
                                if f'tp{n}' in fixes:
                                    setattr(sig, f'tp{n}', round(fixes[f'tp{n}'], digits))
                            tps_raw = [(i+1, getattr(sig, f'tp{i+1}')) for i in range(3) if getattr(sig, f'tp{i+1}', None)]
                            desc = "; ".join(f"{l}: {raw_levels[l]} → {round(fixes[l], digits)}" for l in fixes)
                            log(f"#{sig.id} typo anchored fix (prezzo {cur_price:.2f}): {desc}")
                            _append_trade_log_mt5(sig, "mt5_typo_anchored_fix",
                                f"Typo anchored al prezzo {cur_price:.2f}: {desc}",
                                {"price": cur_price, "fixes": {k: float(v) for k, v in fixes.items()}})
                            sig.notes = (sig.notes or "") + f" [Typo anchored fix: {desc}]"
    except Exception as _e:
        log(f"#{sig.id} typo anchored pipeline errore: {_e}")
    # ────────────────────────────────────────────────────────────────────────

    # AUTO-CORREZIONE TYPO SISTEMATICO (legacy, fallback): il trader scrive una cifra sbagliata
    # nello stesso slot per TUTTI i livelli (es. #348: "4653-54 SL 4658 TP1 4650"
    # invece di "4553-54 SL 4558 TP1 4550" — ha messo "6" invece di "5" nella
    # posizione delle centinaia in tutti i numeri). Detection:
    #   1. Confronta la media dei livelli signal col prezzo broker corrente.
    #   2. Se la media e' significativamente off (>=1%), prova a cambiare
    #      una stessa cifra in una stessa posizione su tutti i livelli.
    #   3. Se esiste un cambio uniforme che porta la media entro 0.5%, applicalo.
    # Conservativo: richiede che tutti i livelli abbiano stessa cifra in quella
    # posizione (origine comune del typo), niente over-correzione.
    try:
        levels = []
        if sig.entry_price: levels.append(float(sig.entry_price))
        if sig.entry_price_high and sig.entry_price_high != sig.entry_price:
            levels.append(float(sig.entry_price_high))
        if sl_raw: levels.append(float(sl_raw))
        for tp_n in (1, 2, 3):
            v = getattr(sig, f'tp{tp_n}', None)
            if v: levels.append(float(v))
        cur_price = (tick.bid + tick.ask) / 2 if tick and tick.bid > 0 and tick.ask > 0 else None
        if cur_price and len(levels) >= 3:
            avg = sum(levels) / len(levels)
            offset_pct = abs(avg - cur_price) / cur_price
            if offset_pct >= 0.01:
                # Tenta digit replacement uniforme sull'INTERO part
                int_levels = [int(round(l)) for l in levels]
                str_int = [str(l) for l in int_levels]
                if len(set(len(s) for s in str_int)) == 1:
                    n_digits = len(str_int[0])
                    best = None
                    for pos in range(n_digits):
                        digits_at_pos = [s[pos] for s in str_int]
                        if len(set(digits_at_pos)) != 1:
                            continue
                        original = digits_at_pos[0]
                        for repl in "0123456789":
                            if repl == original:
                                continue
                            try:
                                # Applica replacement preservando i decimali
                                new_levels = []
                                for orig, l_int in zip(levels, int_levels):
                                    decimal_part = orig - l_int
                                    new_int = int(str(l_int)[:pos] + repl + str(l_int)[pos+1:])
                                    new_levels.append(new_int + decimal_part)
                                new_avg = sum(new_levels) / len(new_levels)
                                new_off = abs(new_avg - cur_price) / cur_price
                                if new_off < 0.005:
                                    if best is None or new_off < best[0]:
                                        best = (new_off, pos, original, repl, new_levels)
                            except Exception:
                                continue
                    if best:
                        new_off, pos, original, repl, new_levels = best
                        log(f"#{sig.id} TYPO SISTEMATICO rilevato: cifra '{original}' in pos {pos} → '{repl}' su tutti i livelli (signal_avg ${avg:.2f} vs broker ${cur_price:.2f})")
                        # Applica nuovi valori in ordine: entry_low, entry_high (se presente), sl, tp1, tp2, tp3
                        idx = 0
                        if sig.entry_price:
                            sig.entry_price = round(new_levels[idx], digits); entry = sig.entry_price; idx += 1
                        if sig.entry_price_high and sig.entry_price_high != (sig.entry_price if idx == 0 else None):
                            sig.entry_price_high = round(new_levels[idx], digits); entry_high = sig.entry_price_high; idx += 1
                        if sl_raw:
                            sl_raw = round(new_levels[idx], digits); sl = sl_raw; idx += 1
                        for tp_n in (1, 2, 3):
                            if getattr(sig, f'tp{tp_n}', None):
                                setattr(sig, f'tp{tp_n}', round(new_levels[idx], digits)); idx += 1
                        # Aggiorna anche tps_raw e altre derivate
                        tps_raw = [(i+1, getattr(sig, f'tp{i+1}')) for i in range(3) if getattr(sig, f'tp{i+1}', None)]
                        sig.notes = (sig.notes or "") + f" [Typo sistematico corretto: '{original}' pos {pos} → '{repl}']"
                        _append_trade_log_mt5(sig, "mt5_systematic_typo_fix",
                            f"Typo sistematico: cifra '{original}' in posizione {pos} sostituita con '{repl}' su tutti i livelli. "
                            f"Signal avg era ${avg:.2f}, broker ${cur_price:.2f}, ora avg ${sum(new_levels)/len(new_levels):.2f}.",
                            {"position": pos, "old": original, "new": repl, "new_levels": new_levels})
    except Exception as _e:
        log(f"#{sig.id} typo detection errore: {_e}")
    # ────────────────────────────────────────────────────────────────────────

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

    # Auto-correzione TP1 dal lato sbagliato (single-digit).
    # Caso #295: SELL 4573-75, TP1=4669 (typo: voleva 4569). Senza correggere
    # TP1, anche la scala usata per validare TP2/TP3 viene calcolata su un
    # valore sbagliato e tutto il resto della logica TP fallisce a cascata.
    # Vincoli: lato giusto rispetto a entry + sandwich con TP2 (deve stare
    # TRA entry e TP2 in direzione del trade).
    tp1_val = getattr(sig, 'tp1', None)
    tp2_val = getattr(sig, 'tp2', None)
    if tp1_val is not None and tp2_val is not None:
        tp1_f = float(tp1_val)
        tp2_f = float(tp2_val)
        side_wrong_tp1 = (is_buy and tp1_f <= e_f) or (not is_buy and tp1_f >= e_f)
        if side_wrong_tp1:
            tp_str_full = f"{tp1_f:.{digits}f}" if digits else f"{int(tp1_f)}"
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
                    # Sandwich: TP1 deve stare TRA entry e TP2
                    sandwich_ok = (is_buy and cand < tp2_f) or (not is_buy and cand > tp2_f)
                    if not sandwich_ok:
                        continue
                    candidates.append(cand)
            if len(candidates) == 1:
                fix = round(candidates[0], digits)
                log(f"#{sig.id} TP1={tp1_f} lato sbagliato - corretto single-digit a {fix}")
                _append_trade_log_mt5(sig, "mt5_tp_fix", f"TP1 corretto da {tp1_f} a {fix} (typo single-digit, lato sbagliato)")
                sig.tp1 = fix
                sig.notes = (sig.notes or "") + f" [TP1 auto-corretto: {tp1_f} -> {fix}]"
            elif len(candidates) > 1:
                log(f"#{sig.id} TP1={tp1_f} lato sbagliato ma {len(candidates)} candidati ambigui - lascio com'e'")

    # Auto-correzione TP2/TP3 single-digit fuori scala (caso #280, #292).
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
                # Prev TP (per ordine progressivo); per i TP "interni" (es. TP2)
                # serve anche il vincolo del TP successivo: TP2 deve stare TRA
                # TP1 e TP3 (caso #292: TP2=4501 -> candidati 4601 e 4591;
                # 4591 < TP3=4595 viola la progressione, va escluso, lasciando
                # 4601 come unico candidato corretto).
                prev_tp = float(getattr(sig, f'tp{i-1}'))
                next_tp_val = getattr(sig, f'tp{i+1}', None) if i < 3 else None
                next_tp = float(next_tp_val) if next_tp_val is not None else None
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
                        # Ordine progressivo rispetto al TP precedente:
                        # BUY: cand > prev_tp; SELL: cand < prev_tp
                        order_ok = (is_buy and cand > prev_tp) or (not is_buy and cand < prev_tp)
                        if not order_ok:
                            continue
                        # Se esiste anche un TP successivo, il candidato deve
                        # rispettare la progressione anche da quel lato:
                        # BUY: cand < next_tp; SELL: cand > next_tp
                        if next_tp is not None:
                            sandwich_ok = (is_buy and cand < next_tp) or (not is_buy and cand > next_tp)
                            if not sandwich_ok:
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
                else:
                    # Disambiguazione via PROIEZIONE: se i TP precedenti formano
                    # una progressione coerente, calcola il TP{i} atteso e scegli
                    # il candidato (anche 2-digit) piu' vicino alla proiezione.
                    # Caso #328: TP1=4660 TP2=4665 → TP3 atteso 4670 (gap 5).
                    # Ricevuto 4772 → single-digit candidati [4672, 4702, 4712,
                    # 4722, 4732] ambigui. 4670 (2-digit: 7→6, 2→0) e' il match
                    # esatto della proiezione.
                    projection = None
                    if i == 3 and tp1_val is not None:
                        tp2_val_p = getattr(sig, 'tp2', None)
                        if tp2_val_p is not None:
                            t1, t2 = float(tp1_val), float(tp2_val_p)
                            projection = t2 + (t2 - t1)  # progressione lineare
                    if projection is not None:
                        # Tolleranza: 50% del gap TP1→TP2 oppure 2 punti
                        tol = max(abs(float(tp2_val_p) - float(tp1_val)) * 0.5, 2 * point)
                        # 1) PREFERISCI candidati SINGLE-DIGIT vicini alla proiezione.
                        sd_with_dist = sorted(((c, abs(c - projection)) for c in candidates), key=lambda x: x[1])
                        best_sd, best_sd_dist = sd_with_dist[0]
                        if best_sd_dist <= tol:
                            fix = round(best_sd, digits)
                            log(f"#{sig.id} TP{i}={tp_f} ambiguo → scelto {fix} (single-digit) piu' vicino a proiezione {projection:.2f} (dist {best_sd_dist:.2f}, tol {tol:.2f})")
                            _append_trade_log_mt5(sig, "mt5_tp_fix",
                                f"TP{i} corretto da {tp_f} a {fix} (single-digit via proiezione TP1→TP2 = {projection:.2f})")
                            setattr(sig, f'tp{i}', fix)
                            sig.notes = (sig.notes or "") + f" [TP{i} auto-corretto: {tp_f} -> {fix}]"
                        else:
                            # 2) Single-digit non bastano: prova 2-digit variants
                            two_digit = set()
                            for p1, c1 in enumerate(tp_str_full):
                                if not c1.isdigit():
                                    continue
                                for d1 in "0123456789":
                                    if d1 == c1:
                                        continue
                                    s1 = tp_str_full[:p1] + d1 + tp_str_full[p1+1:]
                                    for p2 in range(p1+1, len(s1)):
                                        if not s1[p2].isdigit():
                                            continue
                                        for d2 in "0123456789":
                                            if d2 == s1[p2]:
                                                continue
                                            s2 = s1[:p2] + d2 + s1[p2+1:]
                                            try:
                                                two_digit.add(float(s2))
                                            except ValueError:
                                                continue
                            valid2 = []
                            for c in two_digit:
                                if c <= 0:
                                    continue
                                side_ok = (is_buy and c > e_f) or (not is_buy and c < e_f)
                                if not side_ok: continue
                                if not ((is_buy and c > prev_tp) or (not is_buy and c < prev_tp)):
                                    continue
                                if next_tp is not None:
                                    if not ((is_buy and c < next_tp) or (not is_buy and c > next_tp)):
                                        continue
                                cd = abs(c - e_f)
                                if not (lo * tp1_dist <= cd <= hi * tp1_dist):
                                    continue
                                valid2.append((c, abs(c - projection)))
                            if valid2:
                                valid2.sort(key=lambda x: x[1])
                                best2, best2_dist = valid2[0]
                                if best2_dist <= tol:
                                    fix = round(best2, digits)
                                    log(f"#{sig.id} TP{i}={tp_f} typo 2-digit via proiezione {projection:.2f} → corretto a {fix} (dist {best2_dist:.2f})")
                                    _append_trade_log_mt5(sig, "mt5_tp_fix",
                                        f"TP{i} corretto da {tp_f} a {fix} (2-digit via proiezione TP1→TP2 = {projection:.2f})")
                                    setattr(sig, f'tp{i}', fix)
                                    sig.notes = (sig.notes or "") + f" [TP{i} auto-corretto: {tp_f} -> {fix}]"
                                else:
                                    log(f"#{sig.id} TP{i}={tp_f} ambiguo, best 1-digit={best_sd}(d={best_sd_dist:.2f}) 2-digit={best2}(d={best2_dist:.2f}) tutti fuori tolleranza {tol:.2f} - lascio com'e'")
                            else:
                                log(f"#{sig.id} TP{i}={tp_f} fuori scala, single-digit fuori tolleranza, 2-digit nessuno valido - lascio com'e'")
                    else:
                        log(f"#{sig.id} TP{i}={tp_f} fuori scala ma {len(candidates)} candidati ambigui senza proiezione - lascio com'e'")

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

    # Pre-pass: se SL è dal lato sbagliato, prova l'auto-correzione single-digit
    # PRIMA della R/R guard (altrimenti la distance calcolata su un SL invalido
    # è priva di senso e cancella segnali correggibili — caso #310: SELL 4665
    # SL 4571, candidato univoco 4671).
    if sl_raw:
        _sl_check = _round_price(float(sl_raw), digits)
        sl_side_wrong = (is_buy and _sl_check >= float(entry)) or (not is_buy and _sl_check <= float(entry))
        if sl_side_wrong and sig.tp1:
            tp1_dist_pre = abs(float(sig.tp1) - float(entry))
            candidates_pre = []
            if tp1_dist_pre > 0:
                sl_str_full = f"{_sl_check:.{digits}f}" if digits else f"{int(_sl_check)}"
                seen_pre = set()
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
                        if cand in seen_pre or cand <= 0:
                            continue
                        seen_pre.add(cand)
                        side_ok = (is_buy and cand < float(entry)) or (not is_buy and cand > float(entry))
                        if not side_ok:
                            continue
                        cand_dist = abs(cand - float(entry))
                        if cand_dist < tp1_dist_pre * 0.3 or cand_dist > tp1_dist_pre * 3:
                            continue
                        candidates_pre.append(cand)
            if len(candidates_pre) == 1:
                sl_corrected = round(candidates_pre[0], digits)
                msg = (f"SL={_sl_check} dal lato sbagliato per {'BUY' if is_buy else 'SELL'} "
                       f"entry={entry}: typo single-digit, candidato univoco → SL={sl_corrected} "
                       f"(TP1={sig.tp1})")
                log(f"#{sig.id} {msg}")
                _append_trade_log_mt5(sig, "mt5_sl_autocorrect", msg)
                sig.notes = (sig.notes or "") + f" [SL auto-corretto: {_sl_check} → {sl_corrected}]"
                sl_raw = sl_corrected
                sl = sl_corrected

    # PRE-R/R: TP_n fuori scala vs gli altri TP → typo single-digit isolato.
    # Casi:
    #  - #351: entry 4550, TP1 4654 (typo), TP2 4558, TP3 4564 → fix TP1 4554
    #  - speculare: entry 4550, TP1 4554, TP2 4658 (typo), TP3 4564 → fix TP2 4558
    #  - speculare: entry 4550, TP1 4554, TP2 4558, TP3 4664 (typo) → fix TP3 4564
    # Ogni TP_n viene controllato vs MEDIA degli altri due. Se fuori scala >5x,
    # cerca single-digit replacement che soddisfi il vincolo sandwich corretto:
    #   BUY:  entry < TP1 < TP2 < TP3
    #   SELL: entry > TP1 > TP2 > TP3
    if tps_raw and len(tps_raw) >= 2:
        e_f0 = float(entry)
        # Snapshot dei TP attuali per usarli come reference (i fix successivi
        # vengono applicati a sig ma la lista locale resta come baseline).
        tp_vals = {n: float(v) for n, v in tps_raw}
        for target_n in sorted(tp_vals.keys()):
            target_v = tp_vals[target_n]
            others = [tp_vals[n] for n in tp_vals if n != target_n]
            if not others:
                continue
            target_dist = abs(target_v - e_f0)
            other_dists = [abs(o - e_f0) for o in others]
            other_avg = sum(other_dists) / len(other_dists)
            if other_avg <= 0 or target_dist <= other_avg * 5:
                continue
            # Calcola bounds del sandwich per il target_n
            # Per BUY: TP_n deve stare TRA TP_{n-1} (o entry) e TP_{n+1} (o +inf)
            # Per SELL: simmetrico
            prev_v = tp_vals.get(target_n - 1, e_f0) if target_n - 1 >= 1 else e_f0
            next_v = tp_vals.get(target_n + 1) if target_n + 1 in tp_vals else None
            tgt_str = f"{target_v:.{digits}f}" if digits else f"{int(target_v)}"
            cands_pre = []
            seen_pre = set()
            for ip, ch in enumerate(tgt_str):
                if not ch.isdigit():
                    continue
                for d in "0123456789":
                    if d == ch:
                        continue
                    cs = tgt_str[:ip] + d + tgt_str[ip+1:]
                    try:
                        cv = float(cs)
                    except ValueError:
                        continue
                    if cv in seen_pre or cv <= 0:
                        continue
                    seen_pre.add(cv)
                    # Vincolo sandwich
                    if is_buy:
                        if cv > prev_v and (next_v is None or cv < next_v):
                            cands_pre.append(cv)
                    else:
                        if cv < prev_v and (next_v is None or cv > next_v):
                            cands_pre.append(cv)
            if len(cands_pre) == 1:
                fix = round(cands_pre[0], digits)
                log(f"#{sig.id} TP{target_n}={target_v} typo single-digit (fuori scala vs altri TP) → corretto a {fix}")
                _append_trade_log_mt5(sig, "mt5_tp_fix",
                    f"TP{target_n} corretto da {target_v} a {fix} (single-digit, sandwich constraint)")
                setattr(sig, f'tp{target_n}', fix)
                sig.notes = (sig.notes or "") + f" [TP{target_n} auto-corretto: {target_v} -> {fix}]"
                tp_vals[target_n] = fix
        # aggiorna tps_raw dopo eventuali fix
        tps_raw = [(i+1, getattr(sig, f'tp{i+1}')) for i in range(3) if getattr(sig, f'tp{i+1}', None)]

    # PRE-R/R: SL single-digit fix (oltre il lato-sbagliato gia' coperto sopra).
    # Caso: SL valore numerico anomalo (distanza da entry >5x o <0.2x della
    # distanza tipica TP1). Esempio: BUY entry=4550 TP1=4555 SL=4575 (typo:
    # voleva 4545). SL_dist=25 vs TP1_dist=5 → ratio 5 → fuori scala.
    # Vincoli: candidato deve essere su lato giusto (sotto entry per BUY),
    # con SL_dist nuovo in (0.3x, 3x) della TP1_dist.
    # Variante 2: INSERZIONE di una cifra (caso #431: trader scrive "448" invece
    # di "4348", parser normalizza a 4480 perdendo una posizione). Si prova a
    # inserire ogni cifra in ogni posizione del raw "short" (4480 → 448 + insert).
    if sl_raw and tps_raw:
        e_f0 = float(entry)
        sl_f = float(sl_raw)
        tp1_v = float(tps_raw[0][1])
        sl_dist0 = abs(e_f0 - sl_f)
        tp1_dist0 = abs(e_f0 - tp1_v)
        # Lato corretto?
        side_ok_now = (is_buy and sl_f < e_f0) or (not is_buy and sl_f > e_f0)
        if side_ok_now and tp1_dist0 > 0:
            ratio = sl_dist0 / tp1_dist0
            # Anomalo se SL distance >5x TP1 distance o <0.2x
            if ratio > 5 or ratio < 0.2:
                cands_sl = []
                seen_sl = set()
                # Genera tutti i candidati (sostituzione + inserzione) e filtra
                # sul lato giusto + ratio plausibile.
                def _add_cand(cv):
                    if cv <= 0 or cv in seen_sl:
                        return
                    seen_sl.add(cv)
                    if (is_buy and cv >= e_f0) or (not is_buy and cv <= e_f0):
                        return  # lato sbagliato
                    new_dist = abs(e_f0 - cv)
                    if new_dist <= 0:
                        return
                    new_ratio = new_dist / tp1_dist0
                    if 0.3 <= new_ratio <= 3:
                        cands_sl.append(cv)
                # Variante A: sostituzione single-digit
                sl_str = f"{sl_f:.{digits}f}" if digits else f"{int(sl_f)}"
                for ip, ch in enumerate(sl_str):
                    if not ch.isdigit(): continue
                    for d in "0123456789":
                        if d == ch: continue
                        try: cv = float(sl_str[:ip] + d + sl_str[ip+1:])
                        except ValueError: continue
                        _add_cand(cv)
                # Variante B: inserzione di una cifra in mancante (#431)
                # Lavora sulla parte intera, prova anche rimuovendo zeri finali
                int_str = str(int(sl_f))
                shorts = {int_str}
                if int_str.endswith("0") and len(int_str) > 1:
                    shorts.add(int_str[:-1])
                for short in shorts:
                    if len(short) < 2: continue
                    for ip in range(len(short) + 1):
                        for d in "0123456789":
                            cs = short[:ip] + d + short[ip:]
                            if cs.startswith("0"): continue
                            try: cv = float(cs)
                            except ValueError: continue
                            _add_cand(cv)
                if len(cands_sl) == 1:
                    fix = round(cands_sl[0], digits)
                    log(f"#{sig.id} SL={sl_f} typo (dist anomala vs TP1) → corretto a {fix}")
                    _append_trade_log_mt5(sig, "mt5_sl_autocorrect",
                        f"SL corretto da {sl_f} a {fix} (single-digit / insert-digit, dist anomala vs TP1)")
                    sig.notes = (sig.notes or "") + f" [SL auto-corretto: {sl_f} -> {fix}]"
                    sl_raw = fix
                    sl = fix

    # PRE-R/R: Entry single-digit fix (oltre il "wrong side" gia' coperto da
    # mt5_entry_fix che usa prezzo corrente). Caso: entry coerente con SL/TP1
    # in lato ma valore "lontano dal pattern" formato da SL+TP. Es: BUY signal
    # SL=4545, TP1=4554, entry=4548 (typo: voleva 4550). Entry_dist da SL=3,
    # da TP1=6 — asimmetrico. Cerca single-digit fix che renda entry vicino
    # al midpoint di SL e TP1, e dentro al range broker.
    if sl_raw and tps_raw:
        e_f0 = float(entry)
        sl_f = float(sl_raw)
        tp1_v = float(tps_raw[0][1])
        # Verifica sandwich + asymmetric distance
        if is_buy:
            in_sandwich = sl_f < e_f0 < tp1_v
        else:
            in_sandwich = tp1_v < e_f0 < sl_f
        if in_sandwich:
            d_to_sl = abs(e_f0 - sl_f)
            d_to_tp1 = abs(e_f0 - tp1_v)
            total_range = d_to_sl + d_to_tp1
            if total_range > 0:
                # Posizione dell'entry nel range SL-TP1 (0=SL, 1=TP1)
                pos_in_range = d_to_sl / total_range
                # Anomalo se entry occupa <10% o >90% del range
                if pos_in_range < 0.10 or pos_in_range > 0.90:
                    e_str = f"{e_f0:.{digits}f}" if digits else f"{int(e_f0)}"
                    cands_e = []
                    seen_e = set()
                    for ip, ch in enumerate(e_str):
                        if not ch.isdigit():
                            continue
                        for d in "0123456789":
                            if d == ch:
                                continue
                            cs = e_str[:ip] + d + e_str[ip+1:]
                            try:
                                cv = float(cs)
                            except ValueError:
                                continue
                            if cv in seen_e or cv <= 0:
                                continue
                            seen_e.add(cv)
                            # Sandwich + posizione in range plausibile
                            if is_buy:
                                ok = sl_f < cv < tp1_v
                            else:
                                ok = tp1_v < cv < sl_f
                            if not ok:
                                continue
                            new_d_to_sl = abs(cv - sl_f)
                            new_pos = new_d_to_sl / (new_d_to_sl + abs(cv - tp1_v))
                            if 0.20 <= new_pos <= 0.80:
                                cands_e.append(cv)
                    if len(cands_e) == 1:
                        fix = round(cands_e[0], digits)
                        log(f"#{sig.id} Entry={e_f0} typo single-digit (asimmetrico in sandwich SL-TP1) → corretto a {fix}")
                        _append_trade_log_mt5(sig, "mt5_entry_fix",
                            f"Entry corretto da {e_f0} a {fix} (single-digit, asimmetrico vs SL/TP1)")
                        sig.entry_price = fix
                        if not sig.entry_price_high or sig.entry_price_high == e_f0:
                            sig.entry_price_high = fix
                        sig.notes = (sig.notes or "") + f" [Entry auto-corretto: {e_f0} -> {fix}]"
                        entry = fix
                        entry_high = sig.entry_price_high

    # Sanity check R/R: blocchiamo SOLO il caso SL >> TP1 (perdita potenziale
    # >> profitto potenziale = vero typo grave). Esempio bloccato:
    # "Sell 4567 TP1 4562 SL 4673" — voleva SL 4573, ma cosi' rischia 100pt
    # per guadagnarne 5: lotto microscopico, R/R rovinato.
    # NON blocchiamo il caso opposto TP1 >> SL (es. #374: TP1 11pt vs SL 2pt,
    # ratio 5.5:1 a favore): e' un trade aggressivo con SL stretto, legittimo
    # come scelta di rischio. Se il SL e' troppo vicino al prezzo, sara' il
    # broker a rifiutare con INVALID_STOPS — non sta a noi pre-bloccare.
    if sl_raw and tps_raw:
        sl_dist = abs(float(entry) - float(sl_raw))
        tp1_dist = abs(float(entry) - float(tps_raw[0][1]))
        if sl_dist > 0 and tp1_dist > 0 and sl_dist > tp1_dist * 5:
            from database import SessionLocal as _SL
            msg = (f"R/R sproporzionato {sl_dist/tp1_dist:.1f}:1 sfavorevole "
                   f"(SL {sl_dist:.1f}pt >> TP1 {tp1_dist:.1f}pt) — "
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
    # Calcola lotti totali sull'intero rischio, poi dividi per n ordini.
    # Per il dimensionamento usiamo il bordo del range PIÙ LONTANO dallo SL
    # come prezzo di riferimento, perché un BUY LIMIT/STOP nel range puo'
    # riempirsi a quel bordo e in quel caso la SL distance reale e' la
    # massima possibile. Usare il bordo piu' vicino allo SL (entry_price
    # per un BUY) sottostima la distance e produce lotti troppo grandi
    # (caso #284: entry_low 0.713, entry_high 0.7135, SL 0.712 -> il bot
    # calcolava su 10 pip ma il LIMIT si e' riempito a 0.7135 dove la
    # distance reale era 15 pip → risk reale 49% sopra target).
    ep_low_f  = float(sig.entry_price)      if sig.entry_price      else None
    ep_high_f = float(sig.entry_price_high) if sig.entry_price_high else None
    if ep_low_f and ep_high_f:
        size_entry = max(ep_low_f, ep_high_f) if is_buy else min(ep_low_f, ep_high_f)
    else:
        size_entry = ep_low_f or ep_high_f or float(entry)
    n = len(tps_raw)
    lots_total_raw = calc_position_size(sig.symbol, size_entry, sl_raw, risk_usd) if sl_raw else min_vol
    lots_total = _round_volume(lots_total_raw or min_vol, vol_step, min_vol, max_vol)
    # Arrotonda al vol_step (mai per eccesso oltre il target di rischio)
    lots_each_raw = lots_total / n
    lots_each_floor = _round_volume(lots_each_raw, vol_step, min_vol, max_vol)
    lots_each_ceil = round(lots_each_floor + vol_step, 10)
    spec = get_spec(sig.symbol)
    sl_pips = abs(float(size_entry) - float(sl_raw)) / spec["pip"] if sl_raw else 0
    if lots_each_ceil <= max_vol:
        risk_floor = sl_pips * spec["pv"] * lots_each_floor * n
        risk_ceil = sl_pips * spec["pv"] * lots_each_ceil * n
        # Scegli ceil SOLO se non sfora il target di rischio (era +10% prima:
        # ma con SL stretti puo' diventare +30/+50%, vedi #284). Ora ceil
        # consentito solo se risk_ceil <= risk_usd.
        if risk_ceil <= risk_usd and abs(risk_ceil - risk_usd) < abs(risk_floor - risk_usd):
            lots_each = lots_each_ceil
        else:
            lots_each = lots_each_floor
    else:
        lots_each = lots_each_floor
    lots_total = _round_volume(lots_each * n, vol_step, min_vol, max_vol)
    effective_risk = sl_pips * spec["pv"] * lots_total
    log(f"#{sig.id} risk=${risk_usd:.0f} lots={lots_each}x{n}={lots_total} size_entry={size_entry} sl_pips={sl_pips:.0f} eff_risk=${effective_risk:.2f}")

    # MARGIN CAP: ogni trade puo' usare al massimo X% del free margin (default 50%).
    # Calcola il margin necessario per lots_total e, se eccede il cap, riduce i lotti.
    # Cosi' i trade su simboli ad alta richiesta margin (es. BTC con leva 1:2 su Avatrade)
    # non saturano il conto.
    try:
        max_margin_pct = float(settings.get("max_margin_pct_per_trade", 50.0))
    except Exception:
        max_margin_pct = 50.0
    try:
        acc_info = mt5.account_info()
        free_margin = float(acc_info.margin_free) if acc_info else 0.0
    except Exception:
        free_margin = 0.0
    margin_cap_usd = free_margin * (max_margin_pct / 100.0)
    # Margin per 1 lot del simbolo al prezzo di riferimento
    direction_action = mt5.ORDER_TYPE_BUY if is_buy else mt5.ORDER_TYPE_SELL
    try:
        margin_one_lot = mt5.order_calc_margin(direction_action, mt5_sym, 1.0, float(size_entry))
    except Exception:
        margin_one_lot = None
    if margin_one_lot and margin_one_lot > 0 and margin_cap_usd > 0:
        max_lots_by_margin = margin_cap_usd / margin_one_lot
        if lots_total > max_lots_by_margin:
            log(f"#{sig.id} MARGIN CAP: lots {lots_total} eccede cap {max_lots_by_margin:.4f} ({max_margin_pct}% di free ${free_margin:.0f}). Riduco.")
            _append_trade_log_mt5(sig, "margin_cap_applied",
                f"Lotti ridotti per cap margin {max_margin_pct}%: {lots_total} → cap {max_lots_by_margin:.4f}. "
                f"Free margin ${free_margin:.0f}, margin/lot ${margin_one_lot:.0f}.",
                {"original_lots": lots_total, "cap_lots": round(max_lots_by_margin, 4),
                 "free_margin": free_margin, "max_margin_pct": max_margin_pct})
            # Distribuisci il cap fra n ticket
            lots_each_capped = max_lots_by_margin / n
            lots_each = _round_volume(lots_each_capped, vol_step, min_vol, max_vol)
            # Se anche il vol_min eccede il cap → cancella il signal
            margin_for_min = margin_one_lot * min_vol * n
            if margin_for_min > margin_cap_usd:
                msg = (f"Margin insufficiente: anche vol_min {min_vol}×{n}={min_vol*n:.4f} richiede "
                       f"${margin_for_min:.0f} > cap ${margin_cap_usd:.0f} ({max_margin_pct}% di ${free_margin:.0f}). "
                       f"Margin/lot {mt5_sym}=${margin_one_lot:.0f}.")
                log(f"#{sig.id} {msg}")
                from database import SessionLocal as _SL
                sig.status = "cancelled"
                sig.notes = (sig.notes or "") + f" [Margin insufficiente: {msg}]"
                _append_trade_log_mt5(sig, "insufficient_margin", msg)
                _db = _SL()
                try:
                    _db.merge(sig); _db.commit()
                finally:
                    _db.close()
                return []
            lots_total = _round_volume(lots_each * n, vol_step, min_vol, max_vol)
            effective_risk = sl_pips * spec["pv"] * lots_total
            log(f"#{sig.id} dopo cap: lots={lots_each}x{n}={lots_total} eff_risk=${effective_risk:.2f}")

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

    # Soglia MARKET: per i catch realtime applichiamo una piccola tolleranza
    # configurabile (default 3 pip) sopra/sotto il bordo del range. Il segnale
    # è un "Near", quindi se il prezzo è di poco fuori range vogliamo comunque
    # entrare a mercato invece di mettere un LIMIT esatto che potrebbe non
    # riempirsi mai (caso #282: ask 4562.26 vs entry_high 4562 → mancato per
    # 26 cent). Per i late-catch (delayed/edited/replay) manteniamo la
    # tolleranza simmetrica grande (sl_distance), perché il pre-check
    # _analyze_late_catch_ticks ha già validato che il prezzo non ha
    # attraversato il range durante il ritardo.
    is_realtime = (catch_origin == "realtime")
    realtime_tol = settings.get("entry_tolerance_pips", 3.0) * pip_size
    # Soglia minima broker: LIMIT/STOP devono essere >= stops_level dal prezzo corrente.
    # Su XM stops_level=0 → no impatto. Su Avatrade GOLD=0.50, BTC=$190 ecc.
    broker_floor = (stops_level + 2) * point if stops_level else 0.0
    # Tolleranza MARKET PROPORZIONALE alla SL distance del segnale: se il prezzo
    # e' entro 30% della SL_distance dal range, il setup e' ancora valido e
    # andiamo a MARKET (caso #325: bid 0.6 sotto range, SL_dist 5 → 12% → MARKET
    # invece di LIMIT pendente che si e' attivato 27min dopo al picco UP per SL).
    proportional_tol = sl_distance * 0.30 if sl_distance else 0.0
    market_tol = max(realtime_tol, broker_floor, proportional_tol)
    upper_threshold = (entry_upper + market_tol) if is_realtime else (entry_upper + max(sl_distance, broker_floor))
    lower_threshold = (entry_lower - market_tol) if is_realtime else (entry_lower - max(sl_distance, broker_floor))

    def _safe_limit_or_market(buy_side: bool, target_entry: float, current: float):
        """Ritorna (order_type, price) per LIMIT/STOP, ma fa fallback a MARKET
        se la distanza dal prezzo corrente e' < stops_level del broker."""
        dist = abs(current - target_entry)
        if broker_floor and dist < broker_floor:
            log(f"#{sig.id} LIMIT/STOP a {target_entry} troppo vicino al prezzo {current} (dist {dist:.2f} < broker stops_level {broker_floor:.2f}) → fallback MARKET")
            return (mt5.ORDER_TYPE_BUY if buy_side else mt5.ORDER_TYPE_SELL, current)
        return (None, target_entry)

    # BREAKOUT entry ("Buy Above X" / "Sell Below X"): attesa breakout.
    # Per BUY: se ask >= entry il breakout e' gia' avvenuto → MARKET; altrimenti
    #          BUY STOP a entry (aspetta che il prezzo salga oltre il livello).
    # Per SELL: se bid <= entry il breakdown e' gia' avvenuto → MARKET; altrimenti
    #           SELL STOP a entry (aspetta che il prezzo scenda sotto il livello).
    is_breakout = (getattr(sig, 'entry_type', None) == 'breakout')
    if is_breakout and not force_market:
        breakout_level = float(sig.entry_price or sig.entry_price_high or entry)
        if is_buy:
            if current_ask >= breakout_level:
                order_type = mt5.ORDER_TYPE_BUY
                entry = current_ask
                log(f"#{sig.id} BREAKOUT BUY MARKET: ask={current_ask} >= level={breakout_level}, breakout in corso")
            else:
                # BUY STOP — verifica stops_level minimum
                target_entry = breakout_level
                if broker_floor and abs(current_ask - target_entry) < broker_floor:
                    target_entry = round(current_ask + broker_floor, digits)
                    log(f"#{sig.id} BREAKOUT BUY STOP: livello {breakout_level} troppo vicino, alzo a {target_entry}")
                order_type = mt5.ORDER_TYPE_BUY_STOP
                entry = round(target_entry, digits)
                log(f"#{sig.id} BREAKOUT BUY STOP a {entry} (livello breakout {breakout_level}, ask={current_ask})")
        else:
            if current_bid <= breakout_level:
                order_type = mt5.ORDER_TYPE_SELL
                entry = current_bid
                log(f"#{sig.id} BREAKOUT SELL MARKET: bid={current_bid} <= level={breakout_level}, breakdown in corso")
            else:
                target_entry = breakout_level
                if broker_floor and abs(current_bid - target_entry) < broker_floor:
                    target_entry = round(current_bid - broker_floor, digits)
                    log(f"#{sig.id} BREAKOUT SELL STOP: livello {breakout_level} troppo vicino, abbasso a {target_entry}")
                order_type = mt5.ORDER_TYPE_SELL_STOP
                entry = round(target_entry, digits)
                log(f"#{sig.id} BREAKOUT SELL STOP a {entry} (livello breakdown {breakout_level}, bid={current_bid})")
    # force_market: bypassa tutta la routing e va a mercato al prezzo corrente.
    # Usato dal retry manuale quando il LIMIT/STOP precedente e' stato rifiutato.
    elif force_market:
        if is_buy:
            order_type = mt5.ORDER_TYPE_BUY
            entry = current_ask
        else:
            order_type = mt5.ORDER_TYPE_SELL
            entry = current_bid
        log(f"#{sig.id} FORCE MARKET: prezzo {entry} (was force_market)")
    elif is_buy:
        # BUY: favorevole = prezzo SOTTO range, sfavorevole = prezzo SOPRA.
        if current_ask < min_entry:
            order_type, entry_candidate = _safe_limit_or_market(True, entry_lower, current_ask)
            if order_type is None:
                order_type = mt5.ORDER_TYPE_BUY_STOP
                entry = _round_price(float(entry_candidate), digits)
                log(f"#{sig.id} BUY STOP: ask={current_ask} < range-SLdist, stop a {entry}")
            else:
                entry = _round_price(float(entry_candidate), digits)
                log(f"#{sig.id} BUY MARKET (fallback): ask={current_ask}")
        elif current_ask <= upper_threshold:
            order_type = mt5.ORDER_TYPE_BUY
            entry = current_ask
            log(f"#{sig.id} BUY MARKET: ask={current_ask} range={ep1}-{ep2} origin={catch_origin}")
        else:
            order_type, entry_candidate = _safe_limit_or_market(True, entry_upper, current_ask)
            if order_type is None:
                order_type = mt5.ORDER_TYPE_BUY_LIMIT
                entry = _round_price(float(entry_candidate), digits)
                log(f"#{sig.id} BUY LIMIT: ask={current_ask} > range, limit a {entry} origin={catch_origin}")
            else:
                entry = _round_price(float(entry_candidate), digits)
                log(f"#{sig.id} BUY MARKET (fallback): ask={current_ask}")
    else:
        if current_bid > max_entry:
            order_type, entry_candidate = _safe_limit_or_market(False, entry_upper, current_bid)
            if order_type is None:
                order_type = mt5.ORDER_TYPE_SELL_STOP
                entry = _round_price(float(entry_candidate), digits)
                log(f"#{sig.id} SELL STOP: bid={current_bid} > range+SLdist, stop a {entry}")
            else:
                entry = _round_price(float(entry_candidate), digits)
                log(f"#{sig.id} SELL MARKET (fallback): bid={current_bid}")
        elif current_bid >= lower_threshold:
            order_type = mt5.ORDER_TYPE_SELL
            entry = current_bid
            log(f"#{sig.id} SELL MARKET: bid={current_bid} range={ep1}-{ep2} origin={catch_origin}")
        else:
            order_type, entry_candidate = _safe_limit_or_market(False, entry_lower, current_bid)
            if order_type is None:
                order_type = mt5.ORDER_TYPE_SELL_LIMIT
                entry = _round_price(float(entry_candidate), digits)
                log(f"#{sig.id} SELL LIMIT: bid={current_bid} < range, limit a {entry} origin={catch_origin}")
            else:
                entry = _round_price(float(entry_candidate), digits)
                log(f"#{sig.id} SELL MARKET (fallback): bid={current_bid}")

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
            err = _last_send_error.pop((sig.id, tp_num), None)
            err_str = f" | retcode={err['retcode']} '{err['comment']}'" if err else ""
            _append_trade_log_mt5(sig, "mt5_order_failed",
                f"TP{tp_num}: ordine FALLITO | {order_type_str} | entry={entry} | sl={sl} | tp={tp}{err_str}",
                {"retcode": err["retcode"] if err else None, "comment": err["comment"] if err else None, "tp_num": tp_num})
            log(f"#{sig.id} TP{tp_num} FALLITO entry={entry} sl={sl} tp={tp}")

    # Salva sul signal i lotti totali EFFETTIVAMENTE piazzati su MT5,
    # così il frontend mostra il valore reale e non un ricalcolo teorico.
    if tickets:
        sig.position_size = round(lots_each * len(tickets), 2)
        # Popola broker e mt5_account effettivi al momento dell'apertura.
        # Il broker e' hardcoded 'xm' per ora; quando attiveremo il selector
        # multi-broker leggera' da risk_settings. mt5_account viene letto
        # direttamente dall'account info del terminale loggato.
        if not sig.broker:
            sig.broker = MT5_BROKER or "xm"
        if not sig.mt5_account:
            try:
                acc_info = mt5.account_info()
                if acc_info and acc_info.login:
                    sig.mt5_account = int(acc_info.login)
            except Exception:
                pass

    return tickets


def place_order(sig) -> Optional[int]:
    """Compatibilità: piazza ordini e ritorna il primo ticket."""
    tickets = place_orders(sig)
    return tickets[0] if tickets else None


def backfill_trade_log(only_today: bool = False) -> list:
    """
    One-shot: ricostruisce gli eventi mancanti nel trade_log dei segnali gia'
    chiusi a partire dai deal MT5. Utile per i trade chiusi PRIMA del fix che
    ha aggiunto ticket_closed/be_applied/completed in sync_positions.
    """
    from database import SessionLocal, Signal
    from datetime import datetime as _dt, timedelta as _td
    import json as jsonlib

    mt5 = _get_mt5()
    if mt5 is None:
        return []

    db = SessionLocal()
    try:
        q = db.query(Signal).filter(
            (Signal.mt5_ticket.isnot(None)) | (Signal.mt5_tickets.isnot(None)),
            Signal.status != "cancelled",
        )
        if only_today:
            today_start = _dt.utcnow().replace(hour=0, minute=0, second=0, microsecond=0)
            q = q.filter(Signal.created_at >= today_start)
        sigs = q.all()

        updated = []
        for sig in sigs:
            tickets = jsonlib.loads(sig.mt5_tickets) if sig.mt5_tickets else [sig.mt5_ticket]
            try:
                log_list = jsonlib.loads(sig.trade_log) if sig.trade_log else []
            except Exception:
                log_list = []

            existing_closed_tickets = {e.get("ticket") for e in log_list if e.get("event") == "ticket_closed"}
            has_be      = any(e.get("event") == "be_applied" for e in log_list)
            has_completed = any(e.get("event") == "completed" for e in log_list)

            is_buy = (sig.direction or "buy").lower() == "buy"
            added_events = []
            close_deals = []  # (ticket, price, profit, time, reason)
            for t in tickets:
                deals = mt5.history_deals_get(position=t) or []
                for d in deals:
                    if d.entry != mt5.DEAL_ENTRY_OUT:
                        continue
                    profit, price, ts = d.profit, d.price, _get_mt5_utc(d.time)
                    comment = (d.comment or "").lower()
                    if 'tp' in comment:
                        reason = "TP"
                    elif 'sl' in comment:
                        if sig.actual_entry_price and abs(price - sig.actual_entry_price) < abs(price) * 0.001:
                            reason = "SL@BE"
                        else:
                            reason = "SL"
                    elif 'close' in comment or 'ic-close' in comment:
                        reason = "manuale/Close"
                    else:
                        reason = "?"
                    close_deals.append((t, price, profit, ts, reason))
                    if t not in existing_closed_tickets:
                        ev = {
                            "ts": (ts or _dt.utcnow()).isoformat() + "Z",
                            "event": "ticket_closed",
                            "detail": f"ticket={t} chiuso a {price} (motivo: {reason}, profit {profit:+.2f}$)",
                            "ticket": t, "price": price, "profit": round(profit, 2), "reason": reason,
                        }
                        log_list.append(ev)
                        added_events.append("ticket_closed")
                        existing_closed_tickets.add(t)

            # be_applied: se status >= tp1 e c'e' actual_entry_price + non era gia' loggato
            tp_levels_hit = sig.status in ("tp1", "tp2", "tp3", "closed")
            if tp_levels_hit and sig.actual_entry_price and not has_be:
                # Stima ts: subito dopo il primo close di tipo TP
                tp_close = next((cd for cd in close_deals if cd[4] == "TP"), None)
                ts_be = (tp_close[3] + _td(seconds=1)) if tp_close and tp_close[3] else _dt.utcnow()
                ev = {
                    "ts": ts_be.isoformat() + "Z",
                    "event": "be_applied",
                    "detail": f"TP1 raggiunto: SL spostato a breakeven {round(sig.actual_entry_price, 5)} (backfill)",
                    "price": round(sig.actual_entry_price, 5),
                    "tickets": tickets[1:] if len(tickets) > 1 else [],
                }
                log_list.append(ev)
                added_events.append("be_applied")

            # completed: se status finale chiuso e non gia' loggato
            if sig.status in ("tp1", "tp2", "tp3", "closed", "sl_hit") and not has_completed:
                last_close = max(close_deals, key=lambda x: x[3]) if close_deals else None
                ts_done = (last_close[3] if last_close and last_close[3] else (sig.closed_at or _dt.utcnow()))
                ev = {
                    "ts": ts_done.isoformat() + "Z",
                    "event": "completed",
                    "detail": f"Trade chiuso: status={sig.status}, P&L totale {(sig.pnl_usd or 0):+.2f}$ (backfill)",
                    "status": sig.status,
                    "pnl": sig.pnl_usd,
                }
                log_list.append(ev)
                added_events.append("completed")

            if added_events:
                # Riordina per timestamp
                log_list.sort(key=lambda e: e.get("ts", ""))
                sig.trade_log = jsonlib.dumps(log_list)
                db.add(sig)
                updated.append({"id": sig.id, "added": added_events})

        db.commit()
        return updated
    finally:
        db.close()


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
            first_entry_ts = None  # earliest DEAL_ENTRY_IN time fra i ticket
            for t in tickets:
                deals = mt5.history_deals_get(position=t)
                if not deals:
                    continue
                for d in deals:
                    if d.entry == mt5.DEAL_ENTRY_IN:
                        total_vol += d.volume
                        if first_entry_ts is None or d.time < first_entry_ts:
                            first_entry_ts = d.time
                        break
            changes = []
            if total_vol > 0:
                new_size = round(total_vol, 2)
                if sig.position_size != new_size:
                    sig.position_size = new_size
                    changes.append(f"position_size->{new_size}")
            if first_entry_ts is not None and not sig.entered_at:
                sig.entered_at = _get_mt5_utc(first_entry_ts)
                changes.append(f"entered_at->{sig.entered_at}")
            if changes:
                log(f"#{sig.id} backfill: {', '.join(changes)}")
                db.add(sig)
                updated.append({"id": sig.id, "changes": changes, "tickets": tickets})
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


def get_current_price(symbol: str) -> Optional[float]:
    """Ritorna il prezzo medio (bid+ask)/2 corrente del simbolo, o None se non disponibile."""
    mt5 = _get_mt5()
    if mt5 is None:
        return None
    try:
        mt5_sym = MT5_SYMBOL_MAP.get(symbol.upper(), symbol)
        if not mt5.symbol_select(mt5_sym, True):
            return None
        tick = mt5.symbol_info_tick(mt5_sym)
        if tick and tick.bid > 0 and tick.ask > 0:
            return (tick.bid + tick.ask) / 2.0
    except Exception:
        pass
    return None


def fix_price_typo(value: float, anchor_price: float, digits: int = 2,
                   min_allowed: Optional[float] = None,
                   max_allowed: Optional[float] = None,
                   anchor_tol_pct: float = 0.02) -> tuple:
    """Validazione/correzione single-digit typo per un singolo prezzo, ancorato
    al prezzo broker corrente con vincoli opzionali min/max.

    Ritorna (corrected_value, was_corrected, reason).
    Se value e' gia' coerente (entro anchor_tol_pct dal prezzo e dentro min/max),
    ritorna (value, False, "ok"). Altrimenti tenta tutte le variazioni single-digit
    e sceglie quella entro anchor_tol_pct dal prezzo E che rispetta min/max,
    preferendo la piu' vicina al prezzo. Se nessun candidato e' valido,
    ritorna (value, False, "no_fix").
    """
    if anchor_price is None or anchor_price <= 0 or value is None or value <= 0:
        return value, False, "no_anchor"

    def _in_bounds(v):
        if min_allowed is not None and v < min_allowed:
            return False
        if max_allowed is not None and v > max_allowed:
            return False
        return True

    near_price = abs(value - anchor_price) <= anchor_price * anchor_tol_pct
    if near_price and _in_bounds(value):
        return value, False, "ok"

    val_str = f"{value:.{digits}f}" if digits else f"{int(value)}"
    seen = set()
    cands = []  # (candidate_value, fix_kind)
    # Variante 1: SOSTITUZIONE single-digit (un carattere → un altro)
    for ip, ch in enumerate(val_str):
        if not ch.isdigit():
            continue
        for d in "0123456789":
            if d == ch:
                continue
            cs = val_str[:ip] + d + val_str[ip+1:]
            try:
                cv = float(cs)
            except ValueError:
                continue
            if cv <= 0 or cv in seen:
                continue
            seen.add(cv)
            if abs(cv - anchor_price) <= anchor_price * anchor_tol_pct and _in_bounds(cv):
                cands.append((cv, "substitute"))
    # Variante 2: INSERZIONE digit (caso #431: trader scrive "448" invece di "4348",
    # parser normalizza a 4480 perdendo una posizione). Si genera il valore "raw
    # short" rimuovendo eventuali zeri trailing e si prova a inserire ogni cifra
    # in ogni posizione. Funziona quando il valore parsato e' stato gonfiato di
    # un ordine di magnitudine da una normalizzazione automatica.
    # Usa la parte intera per evitare di interferire con i decimali.
    int_part_str = str(int(value))
    # Genera anche le versioni "ridotte" rimuovendo 1 zero finale (la normalizzazione tipica)
    candidates_short = {int_part_str}
    if int_part_str.endswith("0") and len(int_part_str) > 1:
        candidates_short.add(int_part_str[:-1])
    for short_str in candidates_short:
        if len(short_str) < 2:
            continue
        for ip in range(len(short_str) + 1):
            for d in "0123456789":
                cs = short_str[:ip] + d + short_str[ip:]
                if cs.startswith("0"):  # niente leading zero
                    continue
                try:
                    cv = float(cs)
                except ValueError:
                    continue
                if cv <= 0 or cv in seen:
                    continue
                seen.add(cv)
                if abs(cv - anchor_price) <= anchor_price * anchor_tol_pct and _in_bounds(cv):
                    cands.append((cv, "insert"))
    if not cands:
        return value, False, "no_fix"
    best, kind = min(cands, key=lambda x: abs(x[0] - anchor_price))
    return round(best, digits), True, f"{kind}_digit_fix: {value} -> {best}"


def modify_sl(ticket: int, new_sl: float, symbol: str) -> bool:
    """Modifica lo SL di una posizione aperta. Skip silenzioso se è già al valore
    richiesto. Retry breve (3 tentativi) se MT5 risponde None o errore transitorio."""
    return modify_sl_tp(ticket, new_sl, None, symbol)


def modify_sl_tp(ticket: int, new_sl: Optional[float], new_tp: Optional[float], symbol: str) -> bool:
    """Modifica SL e/o TP di una posizione aperta o un ordine pendente.
    Passa None per lasciare invariato uno dei due. Retry su errore transitorio.
    Espone l'ultimo errore in _last_modify_error per i chiamanti."""
    global _last_modify_error
    _last_modify_error = None
    mt5 = _get_mt5()
    if mt5 is None:
        _last_modify_error = (None, "MT5 not initialized")
        return False

    mt5_sym = MT5_SYMBOL_MAP.get(symbol.upper(), symbol)
    sym_info = mt5.symbol_info(mt5_sym)
    digits = sym_info.digits if sym_info else 5
    # float() OBBLIGATORIO: la libreria MT5 rifiuta int con (-2, 'Invalid "sl"
    # argument') SENZA arrivare al broker (bug #546: "Hold With 4116 SL" →
    # parser estrae int 4116 → 9 modify falliti in silenzio → SL fantasma).
    new_sl_rounded = round(float(new_sl), digits) if new_sl is not None else None
    new_tp_rounded = round(float(new_tp), digits) if new_tp is not None else None

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
    if not ok:
        _last_modify_error = err
    status = "OK" if ok else f"FAIL last_err={err}"
    log(f"{label} -> {status}")
    return ok


def register_pending_sl(sig_id: int, sl: float, tickets: list, symbol: str, direction: str):
    """Registra una pending SL request (modify rifiutato per 'Invalid sl').
    Sostituisce eventuale richiesta precedente sullo stesso signal."""
    from datetime import datetime as _dt
    _pending_sl_requests[sig_id] = {
        "sl": sl,
        "tickets": tickets,
        "symbol": symbol,
        "direction": direction,
        "requested_at": _dt.utcnow(),
    }
    log(f"#{sig_id} pending SL queued: SL={sl} su {len(tickets)} ticket")


def clear_pending_sl(sig_id: int):
    _pending_sl_requests.pop(sig_id, None)


def process_pending_sl_requests():
    """Riprocessa tutte le pending SL requests:
    - Se il prezzo ha toccato/superato lo SL nella direzione sfavorevole -> chiusura forzata.
    - Altrimenti tenta di nuovo modify_sl. Se va a buon fine -> rimuove dalla queue.
    Chiamata all'inizio di sync_positions ogni 30s.
    """
    if not _pending_sl_requests:
        return
    mt5 = _get_mt5()
    if mt5 is None:
        return
    from database import SessionLocal, Signal
    db = SessionLocal()
    try:
        for sig_id, req in list(_pending_sl_requests.items()):
            sig = db.query(Signal).filter(Signal.id == sig_id).first()
            if sig is None:
                _pending_sl_requests.pop(sig_id, None)
                continue
            # Se il segnale e' chiuso/cancelled -> pulisci
            if sig.status not in ("open", "tp1", "tp2"):
                log(f"#{sig_id} pending SL: signal status={sig.status}, scarto pending")
                _pending_sl_requests.pop(sig_id, None)
                continue

            mt5_sym = MT5_SYMBOL_MAP.get(req["symbol"].upper(), req["symbol"])
            mt5.symbol_select(mt5_sym, True)
            tick = mt5.symbol_info_tick(mt5_sym)
            if tick is None or tick.bid <= 0 or tick.ask <= 0:
                continue

            sl = req["sl"]
            is_buy = req["direction"].lower() == "buy"
            # Per BUY chiusura su bid; per SELL su ask. SL "toccato" se prezzo
            # chiusura ha superato lo SL nella direzione sfavorevole.
            sl_touched = (is_buy and tick.bid <= sl) or (not is_buy and tick.ask >= sl)
            if sl_touched:
                log(f"#{sig_id} pending SL {sl} TOCCATO (bid={tick.bid} ask={tick.ask}) - chiusura forzata")
                # Filtra ticket ancora aperti
                still_open = []
                for t in req["tickets"]:
                    if mt5.positions_get(ticket=t) or mt5.orders_get(ticket=t):
                        still_open.append(t)
                closed_count = 0
                for t in still_open:
                    if close_position(t, req["symbol"]):
                        closed_count += 1
                _append_trade_log_mt5(sig, "sl_force_close",
                    f"SL pending {sl} toccato dal prezzo (bid={tick.bid} ask={tick.ask}) - "
                    f"chiusura forzata di {closed_count}/{len(still_open)} ticket")
                # Aggiorna nota
                _strip_pending_note(sig)
                sig.notes = (sig.notes or "") + f" [SL pending {sl}: chiusura forzata a {tick.bid if is_buy else tick.ask}]"
                db.merge(sig)
                db.commit()
                _pending_sl_requests.pop(sig_id, None)
                continue

            # Retry modify_sl_tp su ogni ticket
            all_ok = True
            for t in req["tickets"]:
                # Verifica che il ticket esista ancora
                if not (mt5.positions_get(ticket=t) or mt5.orders_get(ticket=t)):
                    continue
                if not modify_sl_tp(t, sl, None, req["symbol"]):
                    all_ok = False
                    break
            if all_ok:
                log(f"#{sig_id} pending SL {sl} APPLICATO (prezzo si e' allontanato: bid={tick.bid} ask={tick.ask})")
                _append_trade_log_mt5(sig, "sl_applied_delayed",
                    f"SL pending {sl} applicato (bid={tick.bid} ask={tick.ask})")
                _strip_pending_note(sig)
                sig.notes = (sig.notes or "") + f" [SL pending {sl} applicato]"
                sig.stoploss = sl
                db.merge(sig)
                db.commit()
                _pending_sl_requests.pop(sig_id, None)
    finally:
        db.close()


def _strip_pending_note(sig):
    """Rimuove dalla nota la stringa '[SL pending: X]' precedente per evitare ridondanza."""
    import re as _re
    if sig.notes:
        sig.notes = _re.sub(r'\s*\[SL pending[^\]]*\]', '', sig.notes).strip() or None


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
            "type_filling": _pick_filling_mode(mt5, pos.symbol),
        }
        result = mt5.order_send(request)
        ok = result and result.retcode == mt5.TRADE_RETCODE_DONE
        if not ok:
            try: last_err = mt5.last_error()
            except: last_err = "?"
            comm = result.comment if result else "?"
            rc = result.retcode if result else "?"
            log(f"close_position ticket={ticket} FAIL retcode={rc} comment='{comm}' last_err={last_err}")
        else:
            log(f"close_position ticket={ticket} OK")
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


def analyze_ema_case(signal_id: int, cancel_reason: str) -> Optional[int]:
    """Entry Market Assessment: dato un signal di cui il pending STOP non è
    mai stato filled e che è stato droppato, simula cosa sarebbe successo
    entrando a MARKET al momento del segnale. Usa i tick MT5 storici.
    Ritorna l'id della riga ema_cases creata, o None se non applicabile.
    """
    from database import SessionLocal as _SL, Signal as _Sig, EmaCase as _EC
    import json as _json

    db = _SL()
    try:
        sig = db.query(_Sig).filter(_Sig.id == signal_id).first()
        if not sig:
            return None
        # Solo casi con STOP order mai filled
        if sig.actual_entry_price is not None:
            return None
        # Estrai info dal trade_log
        was_stop = False
        ask_at_signal = None
        bid_at_signal = None
        order_type_str = ""
        try:
            for ev in _json.loads(sig.trade_log or "[]"):
                if ev.get("event") == "mt5_preparing":
                    detail = ev.get("detail", "")
                    order_type_str = detail
                    if "STOP" in detail.upper():
                        was_stop = True
                    # parse "ask=X bid=Y" dal detail
                    import re
                    a = re.search(r'ask=([0-9.]+)', detail)
                    b = re.search(r'bid=([0-9.]+)', detail)
                    if a: ask_at_signal = float(a.group(1))
                    if b: bid_at_signal = float(b.group(1))
                    break
        except Exception:
            pass
        if not was_stop or ask_at_signal is None or bid_at_signal is None:
            return None
        # Evita duplicati
        existing = db.query(_EC).filter(_EC.signal_id == signal_id).first()
        if existing:
            return existing.id

        is_buy = (sig.direction or "buy").lower() == "buy"
        entry_market = ask_at_signal if is_buy else bid_at_signal
        sl = sig.stoploss
        tp1, tp2, tp3 = sig.tp1, sig.tp2, sig.tp3
        # SL valido solo se sul lato corretto: BUY → sl < entry, SELL → sl > entry.
        # Se entry_market e' gia' "oltre" l'SL del signal originale, l'SL e' inapplicabile
        # con un'entrata MARKET (broker la rifiuterebbe). Skippa SL check ma continua TP.
        sl_feasible = sl is not None and (
            (is_buy and sl < entry_market) or
            (not is_buy and sl > entry_market)
        )

        # Recupera tick fra signal_ts e cancel_ts
        signal_ts = sig.created_at or datetime.utcnow()
        cancel_ts = datetime.utcnow()
        sim_outcome = "no_hit"
        sim_close_time = None
        max_fav = 0.0
        max_adv = 0.0
        try:
            mt5 = _get_mt5()
            if mt5 is None:
                raise RuntimeError("MT5 unavailable")
            mt5_sym = MT5_SYMBOL_MAP.get(sig.symbol.upper(), sig.symbol)
            # Aggiungi 30min di buffer dopo cancel per vedere se TP/SL sarebbero stati hit dopo
            # IMPORTANTE: copy_ticks_range deve ricevere datetime tz-aware UTC, altrimenti
            # su Windows interpreta naive come local time (Rome UTC+2) e sfasa la finestra di 2h.
            from datetime import timedelta as _td, timezone as _tz
            def _to_utc_aware(dt):
                if dt is None:
                    return None
                if dt.tzinfo is None:
                    return dt.replace(tzinfo=_tz.utc)
                return dt.astimezone(_tz.utc)
            sig_utc = _to_utc_aware(signal_ts)
            cancel_utc = _to_utc_aware(cancel_ts)
            ticks = mt5.copy_ticks_range(mt5_sym, sig_utc, cancel_utc + _td(minutes=30), mt5.COPY_TICKS_ALL)
            if ticks is not None and len(ticks) > 0:
                for t in ticks:
                    bid = float(t['bid']) if t['bid'] else 0
                    ask = float(t['ask']) if t['ask'] else 0
                    if bid <= 0 or ask <= 0:
                        continue
                    # Per BUY: prezzo di chiusura sarebbe bid (vendiamo per chiudere). SL/TP sul bid.
                    # Per SELL: chiusura su ask (compriamo per chiudere). SL/TP sull'ask.
                    p_close = bid if is_buy else ask
                    # Distanze massime
                    if is_buy:
                        fav = p_close - entry_market
                        adv = entry_market - p_close
                    else:
                        fav = entry_market - p_close
                        adv = p_close - entry_market
                    if fav > max_fav: max_fav = fav
                    if adv > max_adv: max_adv = adv
                    def _t_to_utc(epoch):
                        return datetime.fromtimestamp(int(epoch), tz=_tz.utc).replace(tzinfo=None)
                    # Hit detection (TP3 prima, in ordine di livello favorevole)
                    if is_buy:
                        if sl_feasible and p_close <= sl:
                            sim_outcome = "sl_hit"; sim_close_time = _t_to_utc(t['time']); break
                        if tp3 is not None and p_close >= tp3:
                            sim_outcome = "tp3"; sim_close_time = _t_to_utc(t['time']); break
                        if tp2 is not None and p_close >= tp2:
                            sim_outcome = "tp2"; sim_close_time = _t_to_utc(t['time'])
                            continue
                        if tp1 is not None and p_close >= tp1:
                            sim_outcome = "tp1"; sim_close_time = _t_to_utc(t['time'])
                            continue
                    else:
                        if sl_feasible and p_close >= sl:
                            sim_outcome = "sl_hit"; sim_close_time = _t_to_utc(t['time']); break
                        if tp3 is not None and p_close <= tp3:
                            sim_outcome = "tp3"; sim_close_time = _t_to_utc(t['time']); break
                        if tp2 is not None and p_close <= tp2:
                            sim_outcome = "tp2"; sim_close_time = _t_to_utc(t['time']); continue
                        if tp1 is not None and p_close <= tp1:
                            sim_outcome = "tp1"; sim_close_time = _t_to_utc(t['time']); continue
        except Exception as e:
            log(f"EMA #{signal_id} tick analysis error: {e}")

        # Se SL inapplicabile e nessun TP raggiunto, segna outcome speciale
        if not sl_feasible and sim_outcome == "no_hit":
            sim_outcome = "sl_infeasible"
        elif not sl_feasible and sim_outcome != "no_hit":
            # TP raggiunto comunque, ma SL era inapplicabile — annota
            pass

        # Calcolo P&L simulato normalizzato a max risk del signal originale ($100 default).
        # Position size ricalcolata: lots = max_risk / (orig_sl_distance_pips * pv).
        # Cosi' il P&L riflette quello che AVREMMO realmente fatto entrando a MARKET
        # col rischio massimo identico al trade originale.
        from risk import get_spec, get_risk_settings, calc_risk_amount
        spec = get_spec(sig.symbol)
        # Risk amount: usa il rischio originale del trade se disponibile, altrimenti settings
        if sig.risk_usd and sig.risk_usd > 0:
            max_risk = float(sig.risk_usd)
        else:
            try:
                rs = get_risk_settings()
                max_risk = calc_risk_amount(rs)
            except Exception:
                max_risk = 100.0
        # SL distance: usa la distanza originale del signal (entry-SL), preservata
        # rispetto al nuovo entry_market.
        orig_entry = float(sig.entry_price_high or sig.entry_price or entry_market)
        orig_sl_dist = abs(orig_entry - sl) if sl else None
        # Lots normalizzati su max_risk con quella distanza
        if orig_sl_dist and orig_sl_dist > 0:
            sl_dist_pips = orig_sl_dist / spec["pip"]
            sim_lots = max_risk / (sl_dist_pips * spec["pv"]) if sl_dist_pips > 0 else 0.01
        else:
            sim_lots = sig.position_size or 0.01
        sim_pnl = 0.0
        target_price = None
        if sim_outcome == "tp1" and tp1: target_price = tp1
        elif sim_outcome == "tp2" and tp2: target_price = tp2
        elif sim_outcome == "tp3" and tp3: target_price = tp3
        elif sim_outcome == "sl_hit": sim_pnl = -max_risk  # per definizione
        if target_price is not None:
            dist = abs(target_price - entry_market)
            sim_pnl = (dist / spec["pip"]) * spec["pv"] * sim_lots
        # Pct relativo
        max_fav_pct = (max_fav / entry_market * 100) if entry_market else 0.0
        max_adv_pct = (max_adv / entry_market * 100) if entry_market else 0.0

        ec = _EC(
            signal_id=sig.id, symbol=sig.symbol, direction=sig.direction,
            signal_time=signal_ts, cancel_time=cancel_ts,
            cancel_reason=cancel_reason,
            entry_signal=sig.entry_price, entry_market=entry_market,
            stoploss=sl, tp1=tp1, tp2=tp2, tp3=tp3,
            sim_outcome=sim_outcome, sim_pnl_usd=round(sim_pnl, 2),
            sim_close_time=sim_close_time,
            sim_max_favorable_pct=round(max_fav_pct, 4),
            sim_max_adverse_pct=round(max_adv_pct, 4),
            notes=order_type_str[:200],
        )
        db.add(ec)
        db.commit()
        log(f"EMA case #{ec.id} per signal #{sig.id}: outcome={sim_outcome} pnl={sim_pnl:.2f}")
        return ec.id
    except Exception as e:
        log(f"analyze_ema_case error: {e}")
        return None
    finally:
        db.close()


def drop_pending_missed_tp():
    """Per ogni signal in stato 'pending' con ticket broker, verifica se il
    prezzo corrente del nostro broker ha raggiunto/superato TP1 in direzione
    favorevole. Se si', il trade e' un 'missed' (LIMIT/STOP mai filled mentre
    il prezzo ha gia' fatto target). Drop i pending order broker + marca
    signal cancelled. Indipendente da TG e parser.
    """
    from database import SessionLocal, Signal
    import json as _json

    mt5 = _get_mt5()
    if mt5 is None:
        return []

    db = SessionLocal()
    dropped = []
    try:
        pending = db.query(Signal).filter(
            Signal.status == "pending",
            Signal.mt5_ticket.isnot(None),
            Signal.tp1.isnot(None),
        ).all()
        for sig in pending:
            mt5_sym = MT5_SYMBOL_MAP.get(sig.symbol.upper(), sig.symbol)
            tick = mt5.symbol_info_tick(mt5_sym)
            if not tick or tick.bid <= 0 or tick.ask <= 0:
                continue
            is_buy = (sig.direction or "buy").lower() == "buy"
            tp1 = float(sig.tp1)
            # Per BUY: TP1 raggiunto quando bid >= tp1 (prezzo a cui chiuderemmo)
            # Per SELL: TP1 raggiunto quando ask <= tp1
            tp_reached = (is_buy and tick.bid >= tp1) or (not is_buy and tick.ask <= tp1)
            if not tp_reached:
                continue
            # Drop: cancella pending broker + marca signal cancelled
            tickets = _json.loads(sig.mt5_tickets) if sig.mt5_tickets else [sig.mt5_ticket]
            cancelled_count = 0
            for t in tickets:
                orders = mt5.orders_get(ticket=t)
                if orders:
                    mt5.order_send({"action": mt5.TRADE_ACTION_REMOVE, "order": t})
                    cancelled_count += 1
            reason = (f"TP1 {tp1} raggiunto dal prezzo broker "
                      f"(bid={tick.bid} ask={tick.ask}) ma LIMIT/STOP mai filled. "
                      f"Drop di {cancelled_count} pending broker.")
            log(f"#{sig.id} {sig.symbol} {reason}")
            _append_trade_log_mt5(sig, "pending_dropped_missed_tp", reason,
                {"tp1": tp1, "bid": tick.bid, "ask": tick.ask, "cancelled": cancelled_count,
                 "trigger": "price_check"})
            sig.status = "cancelled"
            sig.notes = (sig.notes or "") + (
                f" [Pending mai filled: prezzo ha raggiunto TP1 {tp1} ma LIMIT/STOP non attivato]"
            )
            from datetime import datetime as _dt
            sig.updated_at = _dt.utcnow()
            sig.closed_at = _dt.utcnow()
            db.add(sig)
            dropped.append(sig.id)
            # EMA: registra caso missed
            try:
                analyze_ema_case(sig.id, "price_missed_tp")
            except Exception:
                pass
        if dropped:
            db.commit()
            log(f"drop_pending_missed_tp: cancellati {len(dropped)} signals: {dropped}")
        return dropped
    finally:
        db.close()


def cancel_expired_signals():
    """
    1. Cancella segnali attivi (open/tp1/tp2) senza mt5_ticket: orphan, mai
       eseguiti su MT5.
    2. Cancella segnali pending SENZA ticket MT5 il cui prezzo e' andato oltre
       SL: il LIMIT/STOP non e' mai stato piazzato e ora il trade e' perso.
       NOTA: i pending CON ticket MT5 (broker ha gli ordini) NON vengono
       toccati qui — il broker gestisce l'attivazione, e per un SELL_STOP
       a 91.6 con SL 91.8, il prezzo a 94.2 non significa "trade perso", solo
       "ordine non ancora attivato". Lasciar scadere via timeout o close TG.
    """
    from database import SessionLocal, Signal
    import json as jsonlib

    mt5 = _get_mt5()
    if mt5 is None:
        return []

    db = SessionLocal()
    try:
        # Block 1: orphan (status attivo senza ticket MT5)
        # ESCLUDIAMO i paper trade (is_filtered=True): non hanno ticket per design,
        # non sono orfani ma simulazioni.
        orphans = db.query(Signal).filter(
            Signal.status.in_(["open", "tp1", "tp2"]),
            Signal.mt5_ticket.is_(None),
            Signal.is_filtered == False,
        ).all()
        cancelled = []
        for sig in orphans:
            reason = f"orphan: status={sig.status} senza ticket MT5 (place_orders fallito o race)"
            sig.status = "cancelled"
            sig.updated_at = datetime.utcnow()
            sig.notes = (sig.notes or "") + " [Non eseguito su MT5]"
            _append_trade_log_mt5(sig, "expired_cancel", reason, {"kind": "orphan"})
            db.add(sig)
            cancelled.append(sig.id)
            log(f"#{sig.id} {sig.symbol} expired_cancel: {reason}")

        # Block 2: pending SENZA ticket MT5 + prezzo oltre SL.
        # ESCLUDIAMO i pending con mt5_tickets popolati: in quel caso gli ordini
        # sono nelle mani del broker e non e' nostro compito cancellarli per
        # "prezzo oltre SL" mentre il pending non si e' ancora attivato.
        pending = db.query(Signal).filter(
            Signal.status == "pending",
            Signal.mt5_ticket.is_(None),
            Signal.is_filtered == False,
        ).all()

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
            past_sl = (is_buy and price <= sl) or (not is_buy and price >= sl)
            if not past_sl:
                continue

            reason = f"pending senza ticket MT5 e prezzo {price} oltre SL {sl} ({sig.direction})"
            log(f"#{sig.id} {sig.symbol} expired_cancel: {reason}")
            sig.status = "cancelled"
            sig.updated_at = datetime.utcnow()
            sig.notes = (sig.notes or "") + f" [Pending oltre SL: {price} vs {sl}]"
            _append_trade_log_mt5(sig, "expired_cancel", reason,
                                  {"kind": "pending_past_sl", "price": price, "sl": sl})
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
    """Cambia l'account MT5 attivo. Legge path/broker dal record DB."""
    global MT5_ACCOUNT, MT5_SERVER, MT5_PATH, MT5_BROKER
    import MetaTrader5 as mt5
    # Carica path/broker per il nuovo account dal DB
    new_path = MT5_PATH
    new_broker = MT5_BROKER
    try:
        from database import SessionLocal as _SL, Mt5Account as _Acc
        db = _SL()
        try:
            acc = db.query(_Acc).filter(_Acc.login == login).first()
            if acc:
                if acc.mt5_path:
                    new_path = acc.mt5_path
                if acc.broker:
                    new_broker = acc.broker
        finally:
            db.close()
    except Exception as e:
        log(f"switch_account: errore lettura DB account: {e}")
    mt5.shutdown()
    init_kwargs = {"login": login, "server": server}
    if new_path:
        init_kwargs["path"] = new_path
    if not mt5.initialize(**init_kwargs):
        err = mt5.last_error()
        log(f"Switch account fallito ({login}@{server} path={new_path}): {err}")
        # Ripristina il precedente
        prev_kwargs = {"login": MT5_ACCOUNT, "server": MT5_SERVER}
        if MT5_PATH:
            prev_kwargs["path"] = MT5_PATH
        mt5.initialize(**prev_kwargs)
        return {"ok": False, "error": f"Connessione fallita: {err}"}
    info = mt5.account_info()
    if not info or info.login != login:
        log(f"Switch account: login mismatch {info.login if info else '?'} != {login}")
        mt5.shutdown()
        prev_kwargs = {"login": MT5_ACCOUNT, "server": MT5_SERVER}
        if MT5_PATH:
            prev_kwargs["path"] = MT5_PATH
        mt5.initialize(**prev_kwargs)
        return {"ok": False, "error": "Account non trovato nel terminale MT5"}
    MT5_ACCOUNT = login
    MT5_SERVER = server
    MT5_PATH = new_path
    MT5_BROKER = new_broker
    # Persisti is_active=True sul nuovo account (e False su tutti gli altri)
    try:
        from database import SessionLocal as _SL, Mt5Account as _Acc
        db = _SL()
        try:
            db.query(_Acc).update({_Acc.is_active: False})
            acc = db.query(_Acc).filter(_Acc.login == login).first()
            if acc:
                acc.is_active = True
            db.commit()
        finally:
            db.close()
    except Exception as e:
        log(f"switch_account: errore persistenza is_active: {e}")
    # Aggiorna .env cosi' al prossimo restart TM parte gia' sull'account corretto
    try:
        _update_env_mt5(login, server, new_path, new_broker)
    except Exception as e:
        log(f"switch_account: errore update .env: {e}")
    log(f"Account cambiato: {login}@{server} ({info.name}) balance={info.balance}")
    return {
        "ok": True,
        "login": info.login,
        "name": info.name,
        "balance": info.balance,
        "server": info.server,
        "demo": info.trade_mode == 0,
    }


def _update_env_mt5(login: int, server: str, path: str, broker: str) -> None:
    """Aggiorna le variabili MT5_* nel .env in modo idempotente.
    Se una chiave esiste la sostituisce, altrimenti la appende.
    Salva sempre in UTF-8 con line-ending nativo del file esistente."""
    import os as _os
    env_path = _os.path.join(_os.path.dirname(__file__), ".env")
    if not _os.path.exists(env_path):
        log(f"_update_env_mt5: .env non trovato in {env_path}")
        return
    with open(env_path, "r", encoding="utf-8") as f:
        lines = f.readlines()
    updates = {
        "MT5_ACCOUNT": str(login),
        "MT5_SERVER": server,
        "MT5_PATH": path or "",
        "MT5_BROKER": broker or "",
    }
    keys_seen = set()
    new_lines = []
    for line in lines:
        stripped = line.lstrip()
        matched = False
        for k, v in updates.items():
            if stripped.startswith(k + "="):
                new_lines.append(f"{k}={v}\n")
                keys_seen.add(k)
                matched = True
                break
        if not matched:
            new_lines.append(line)
    # Append missing keys
    for k, v in updates.items():
        if k not in keys_seen:
            new_lines.append(f"{k}={v}\n")
    with open(env_path, "w", encoding="utf-8", newline="") as f:
        f.writelines(new_lines)
    log(f"_update_env_mt5: aggiornato .env -> account={login} server={server} broker={broker}")


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


def backfill_missing_pnl() -> int:
    """Backfill pnl_usd per signal chiusi (closed_at settato) ma con pnl_usd=None.
    Tipicamente price_service.py marca status=sl_hit + closed_at via monitor
    prezzo senza interrogare MT5, lasciando pnl_usd vuoto. sync_positions
    li ignora perche' closed_at e' set. Qui li riprendiamo: leggiamo i deal
    di chiusura da MT5 history e sommiamo i profit per ogni ticket."""
    from database import SessionLocal, Signal
    import json as jsonlib
    from datetime import timedelta
    mt5 = _get_mt5()
    if mt5 is None:
        return 0
    db = SessionLocal()
    fixed = 0
    try:
        from sqlalchemy import or_
        # Cattura NULL e 0.0 (bug storici del recalculate_all che azzerava
        # pnl invece di lasciarlo intatto).
        candidates = db.query(Signal).filter(
            or_(Signal.pnl_usd.is_(None), Signal.pnl_usd == 0.0),
            Signal.closed_at.isnot(None),
            Signal.mt5_ticket.isnot(None),
            Signal.status.in_(("sl_hit", "tp1", "tp2", "tp3", "closed")),
        ).all()
        for sig in candidates:
            try:
                tickets = jsonlib.loads(sig.mt5_tickets) if sig.mt5_tickets else [sig.mt5_ticket]
                total = 0.0
                found_any = False
                for t in tickets:
                    deals = mt5.history_deals_get(position=t)
                    if not deals:
                        continue
                    for d in deals:
                        if d.entry == mt5.DEAL_ENTRY_OUT:
                            total += float(d.profit) + float(getattr(d, 'commission', 0) or 0) + float(getattr(d, 'swap', 0) or 0)
                            found_any = True
                if found_any:
                    sig.pnl_usd = round(total, 2)
                    db.add(sig)
                    fixed += 1
                    log(f"[BackfillPnL] #{sig.id} {sig.symbol} pnl_usd={sig.pnl_usd}")
            except Exception as e:
                log(f"[BackfillPnL] #{sig.id} errore: {str(e)[:80]}")
        if fixed:
            db.commit()
    finally:
        db.close()
    return fixed


def sync_positions() -> list:
    """
    Confronta posizioni/ordini MT5 con segnali 'open' nel DB.
    Gestisce multipli ticket per segnale (TP1/TP2/TP3 separati).
    Quando TP1 chiude → sposta SL a breakeven sugli altri.
    """
    from database import SessionLocal, Signal
    import json as jsonlib
    from datetime import timedelta

    # Riprocessa eventuali pending SL requests (modify rifiutati per 'Invalid sl')
    process_pending_sl_requests()

    # ROBUSTEZZA: drop pending signals il cui TP1 e' stato raggiunto/superato
    # dal prezzo broker mentre il LIMIT/STOP non si e' mai fillato.
    # Indipendente da TG, parser, markdown — usa solo prezzi reali Avatrade.
    drop_pending_missed_tp()

    # Backfill pnl_usd per signal chiusi via price_service senza P&L
    try:
        backfill_missing_pnl()
    except Exception as _e:
        log(f"[BackfillPnL] errore globale: {str(_e)[:80]}")

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
            closed_reasons = {}  # ticket -> "TP" | "SL" | "SL@BE" | "manuale/Close" | "?"
            tp1_hit        = False

            for ticket in tickets:
                if ticket in open_positions:
                    pos = open_positions[ticket]
                    total_profit += pos.profit
                    total_volume += pos.volume
                    open_count += 1
                    positions_count += 1
                    # Cattura actual_entry_price + entered_at quando un
                    # BUY/SELL LIMIT viene riempito. pos.time è l'epoch
                    # (server time) di apertura della posizione.
                    if not sig.actual_entry_price and pos.price_open:
                        sig.actual_entry_price = pos.price_open
                        log(f"#{sig.id} actual_entry_price={pos.price_open} (da posizione aperta)")
                    if not sig.entered_at and getattr(pos, 'time', None):
                        sig.entered_at = _get_mt5_utc(pos.time)
                        log(f"#{sig.id} entered_at={sig.entered_at} (da posizione aperta)")
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
                    # Determina motivo dal commento del deal (es. '[tp 4530.00]',
                    # '[sl 4525.40]', 'IC-close'). Calcolato SEMPRE (non solo per il
                    # log): serve alla determinazione dello status TP robusta a
                    # slippage (caso #538: TP2 fill a 4164.81 con TP a 4165.0).
                    comment = (close_deal.comment or "").lower()
                    if 'tp' in comment:
                        reason = "TP"
                    elif 'sl' in comment:
                        # Distingui SL originale vs SL @ BE
                        if sig.actual_entry_price and abs(close_price - sig.actual_entry_price) < abs(close_price) * 0.001:
                            reason = "SL@BE"
                        else:
                            reason = "SL"
                    elif 'close' in comment or 'ic-close' in comment:
                        reason = "manuale/Close"
                    else:
                        reason = "?"
                    closed_reasons[ticket] = reason
                    # Log evento di chiusura nel trade_log (idempotente: skip se gia' loggato per quel ticket)
                    existing_log = sig.trade_log or "[]"
                    if f'"ticket": {ticket}' not in existing_log or '"event": "ticket_closed"' not in existing_log or f'{{"ticket": {ticket}' not in existing_log.replace('"event": "ticket_closed"', ''):
                        # Idempotenza: cerca evento gia' presente per questo ticket
                        import json as _jsonlib
                        try:
                            log_list = _jsonlib.loads(sig.trade_log) if sig.trade_log else []
                        except Exception:
                            log_list = []
                        already = any(e.get("event") == "ticket_closed" and e.get("ticket") == ticket for e in log_list)
                        if not already:
                            _append_trade_log_mt5(sig, "ticket_closed",
                                f"ticket={ticket} chiuso a {close_price} (motivo: {reason}, profit {profit:+.2f} $)",
                                {"ticket": ticket, "price": close_price, "profit": round(profit, 2), "reason": reason})
                    # Recupera deal di entrata: serve per actual_entry_price,
                    # entered_at e volume iniziale. Popola entered_at e
                    # actual_entry_price indipendentemente, perche' uno dei
                    # due puo' essere stato gia' settato dal branch
                    # 'posizione aperta' senza l'altro.
                    entry_deals = mt5.history_deals_get(position=ticket)
                    if entry_deals:
                        for ed in entry_deals:
                            if ed.entry == mt5.DEAL_ENTRY_IN:
                                total_volume += ed.volume
                                if not sig.actual_entry_price:
                                    sig.actual_entry_price = ed.price
                                    log(f"#{sig.id} actual_entry_price={ed.price} (da deal di entrata)")
                                if not sig.entered_at:
                                    sig.entered_at = _get_mt5_utc(ed.time)
                                    log(f"#{sig.id} entered_at={sig.entered_at} (da deal di entrata)")
                                break

                    # Determina se è TP1
                    if sig.tp1 and ((is_buy and close_price >= sig.tp1) or
                                    (not is_buy and close_price <= sig.tp1)):
                        tp1_hit = True
                else:
                    # Nessun deal trovato — controlla lo stato dell'ordine in history.
                    # Avatrade puo' rejectare ordini con comment 'deleted [no money]'
                    # quando un fill multiplo eccede il margin (sig.318 case).
                    hist_orders = mt5.history_orders_get(ticket=ticket)
                    if hist_orders:
                        ord_state = hist_orders[0].state
                        ord_comment = (hist_orders[0].comment or "")
                        # ORDER_STATE_REJECTED = 5, ORDER_STATE_CANCELED = 2,
                        # ORDER_STATE_EXPIRED = 6
                        if ord_state in (2, 5, 6):
                            state_label = {2: "CANCELED", 5: "REJECTED", 6: "EXPIRED"}[ord_state]
                            log(f"#{sig.id} ticket={ticket} ordine {state_label} dal broker (comment='{ord_comment}')")
                            # Idempotenza: log evento solo se non gia' presente
                            import json as _jsonlib
                            try:
                                _ll = _jsonlib.loads(sig.trade_log) if sig.trade_log else []
                            except Exception:
                                _ll = []
                            already = any(
                                e.get("event") == "ticket_rejected" and e.get("ticket") == ticket
                                for e in _ll
                            )
                            if not already:
                                _append_trade_log_mt5(sig, "ticket_rejected",
                                    f"ticket={ticket} {state_label} dal broker: '{ord_comment or 'no detail'}'",
                                    {"ticket": ticket, "state": state_label, "broker_comment": ord_comment})
                            # Non incrementa open_count, non somma volume
                        else:
                            sig_age = (datetime.utcnow() - sig.created_at).total_seconds() if sig.created_at else 9999
                            if sig_age < 1800:
                                open_count += 1
                            else:
                                log(f"#{sig.id} ticket={ticket} state={ord_state} dopo {int(sig_age/60)}min — orfano")
                    else:
                        sig_age = (datetime.utcnow() - sig.created_at).total_seconds() if sig.created_at else 9999
                        if sig_age < 1800:
                            open_count += 1
                        else:
                            log(f"#{sig.id} ticket={ticket} non trovato in MT5 dopo {int(sig_age/60)}min — orfano")

            # ─── SL trail logic ─────────────────────────────────────────────────
            # Se trail_stop_enabled e' attivo (per-trade override o default
            # globale), il bot muove auto lo SL al raggiungimento dei TP:
            #   - TP1 hit → SL su residui = BE +/- 1 pip
            #   - TP2 hit → SL su residui = TP1 +/- 1 pip
            # Se trail_stop_enabled e' OFF, sync_positions NON muove lo SL
            # automaticamente: la gestione e' lasciata all'utente (lock
            # profit manuale via UI o SLMove dal trader TG).
            if tp1_hit and open_count > 0 and sig.actual_entry_price:
                # Trail stop: override per-trade se valorizzato, altrimenti
                # default globale da risk_settings.
                if sig.trail_stop_enabled is not None:
                    trail_enabled = bool(sig.trail_stop_enabled)
                else:
                    try:
                        from risk import get_risk_settings as _grs
                        _settings = _grs()
                        trail_enabled = bool(_settings.get("trail_stop_enabled", False))
                    except Exception:
                        trail_enabled = False

                if trail_enabled:
                    # Determina il TP raggiunto piu' alto fra i ticket gia' chiusi
                    tp_levels_hit = 0
                    for tp_num, tp_price in [(1, sig.tp1), (2, sig.tp2), (3, sig.tp3)]:
                        if tp_price is None:
                            continue
                        if any((is_buy and cp >= tp_price) or (not is_buy and cp <= tp_price)
                               for _, cp, _, _ in closed_tickets):
                            tp_levels_hit = max(tp_levels_hit, tp_num)

                    # Calcola pip_size del simbolo
                    pip_size = 0.0
                    try:
                        sym_info_local = mt5.symbol_info(MT5_SYMBOL_MAP.get(sig.symbol.upper(), sig.symbol))
                        if sym_info_local:
                            pip_size = sym_info_local.point * 10
                    except Exception:
                        pass

                    if pip_size > 0:
                        if tp_levels_hit >= 2 and sig.tp1:
                            target_sl = round(float(sig.tp1) + pip_size, 5) if is_buy else round(float(sig.tp1) - pip_size, 5)
                            trail_label = "TP1+1pip"
                        else:
                            target_sl = round(sig.actual_entry_price + pip_size, 5) if is_buy else round(sig.actual_entry_price - pip_size, 5)
                            trail_label = "BE+1pip"

                        import json as _jsonlib
                        try:
                            log_list = _jsonlib.loads(sig.trade_log) if sig.trade_log else []
                        except Exception:
                            log_list = []
                        last_trail_event = None
                        for ev in log_list:
                            if ev.get("event") == "trail_applied":
                                last_trail_event = ev
                        already_at_target = (
                            last_trail_event is not None
                            and last_trail_event.get("rule") == trail_label
                            and abs(last_trail_event.get("price", 0) - target_sl) < 1e-9
                        )

                        affected_tickets = []
                        for ticket in tickets:
                            pos_or_ord = None
                            if ticket in open_positions:
                                pos_or_ord = open_positions[ticket]
                            elif ticket in pending_orders:
                                pos_or_ord = pending_orders[ticket]
                            if pos_or_ord is None:
                                continue
                            current_sl = getattr(pos_or_ord, 'sl', None)
                            # Skip se SL corrente e' gia' uguale o piu' favorevole del target
                            if current_sl is not None and current_sl > 0:
                                if is_buy and current_sl >= target_sl:
                                    continue
                                if not is_buy and current_sl <= target_sl:
                                    continue
                            modify_sl(ticket, target_sl, sig.symbol)
                            log(f"#{sig.id} trail SL={target_sl} ({trail_label}) su ticket={ticket}")
                            affected_tickets.append(ticket)
                        if affected_tickets and not already_at_target:
                            _append_trade_log_mt5(sig, "trail_applied",
                                f"TP{tp_levels_hit} raggiunto: SL spostato a {target_sl} ({trail_label}) sui {len(affected_tickets)} ticket residui",
                                {"price": target_sl, "rule": trail_label, "tp_hit": tp_levels_hit, "tickets": affected_tickets})
                            # Aggiorna sig.stoploss con il SL reale applicato sul broker,
                            # cosi' la UI mostra il valore corrente invece dell'originale del segnale.
                            sig.stoploss = target_sl

            # Aggiorna P&L live (somma profit aperte + chiuse parziali).
            # GUARD: per signal gia' completed (closed_at settato), NON sovrascrivere
            # pnl_usd. Su cicli sync successivi total_profit puo' tornare 0 perche'
            # i deal cadono fuori dalla hist_from window (1h-7d), e sovrascriverebbe
            # il valore corretto salvato al momento del completed (caso #355).
            if sig.closed_at is None:
                sig.pnl_usd = round(total_profit, 2)

            # Avanza status durante i partial close: tp1 -> tp2 -> tp3 in base
            # ai ticket gia' chiusi che hanno raggiunto i livelli TP. Cosi' la
            # tile in UI riflette lo stato reale anche durante il trade.
            if open_count > 0 and closed_tickets:
                tp_levels_hit_status = 0
                # Ticket-based (robusto a slippage, caso #538)
                for _idx, _tk in enumerate(tickets):
                    if _idx < 3 and closed_reasons.get(_tk) == "TP":
                        tp_levels_hit_status = max(tp_levels_hit_status, _idx + 1)
                # Fallback prezzo
                for _tp_num, _tp_price in [(1, sig.tp1), (2, sig.tp2), (3, sig.tp3)]:
                    if _tp_price is None:
                        continue
                    if any((is_buy and cp >= _tp_price) or (not is_buy and cp <= _tp_price)
                           for _, cp, _, _ in closed_tickets):
                        tp_levels_hit_status = max(tp_levels_hit_status, _tp_num)
                if tp_levels_hit_status > 0:
                    new_status_partial = f"tp{tp_levels_hit_status}"
                    cur_lvl = {"tp1": 1, "tp2": 2, "tp3": 3}.get(sig.status or "", 0)
                    if tp_levels_hit_status > cur_lvl:
                        sig.status = new_status_partial
                        log(f"#{sig.id} status avanzato: {new_status_partial} (partial close)")

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

                # Determina status dal TP più alto raggiunto.
                # Distinzione "sl_hit" vs "trail_out": se nessun TP e' stato raggiunto
                # ma il prezzo di chiusura e' sul lato FAVOREVOLE rispetto all'entry
                # reale (per BUY: close > entry, per SELL: close < entry), significa
                # che lo SL e' stato spostato dal trail (BE+1pip / TP1+1pip ecc.) e
                # ha agganciato in profitto: "trail_out". Altrimenti perdita vera =
                # "sl_hit". Caso #442 GBPJPY 11/06: entry 214.836, trail TG sposta SL
                # a 214.845, close 214.845 → profit +5.45$ ma status era "sl_hit".
                new_status = "sl_hit"
                # 1) Metodo ticket-based (robusto a slippage, caso #538): l'ordine
                #    dei ticket in sig.mt5_tickets e' [TP1, TP2, TP3]. Se il deal
                #    di chiusura del ticket i-esimo ha comment TP, quel livello e'
                #    stato raggiunto indipendentemente dal prezzo di fill.
                tp_from_tickets = 0
                for _idx, _tk in enumerate(tickets):
                    if _idx < 3 and closed_reasons.get(_tk) == "TP":
                        tp_from_tickets = max(tp_from_tickets, _idx + 1)
                if tp_from_tickets > 0:
                    new_status = f"tp{tp_from_tickets}"
                else:
                    # 2) Fallback prezzo (trade legacy / chiusure manuali)
                    for tp_num, tp_price in [(3, sig.tp3), (2, sig.tp2), (1, sig.tp1)]:
                        if tp_price is None:
                            continue
                        if any((is_buy and cp >= tp_price) or (not is_buy and cp <= tp_price)
                               for _, cp, _, _ in closed_tickets):
                            new_status = f"tp{tp_num}"
                            break
                if new_status == "sl_hit" and sig.actual_entry_price and closed_tickets:
                    entry_anchor = float(sig.actual_entry_price)
                    favorable_closes = sum(
                        1 for _, cp, _, _ in closed_tickets
                        if (is_buy and cp > entry_anchor) or (not is_buy and cp < entry_anchor)
                    )
                    # Se la MAGGIORANZA dei ticket si e' chiusa in zona favorevole
                    # o il P&L totale e' positivo, e' un trail_out non un sl_hit.
                    if favorable_closes >= len(closed_tickets) / 2 or total_profit > 0:
                        new_status = "trail_out"

                # Log dell'evento "completato" nel trade_log (idempotente)
                import json as _jsonlib
                try:
                    log_list = _jsonlib.loads(sig.trade_log) if sig.trade_log else []
                except Exception:
                    log_list = []
                if not any(e.get("event") == "completed" for e in log_list):
                    _append_trade_log_mt5(sig, "completed",
                        f"Trade chiuso: status={new_status}, P&L totale {total_profit:+.2f}$",
                        {"status": new_status, "exit_price": close_price, "pnl": round(total_profit, 2)})

                # Non degradare lo status: se gia' avanzato a tp_n da TG action o
                # da partial close, mantieni il livello massimo raggiunto (caso #366:
                # TG ha annunciato TP2 e abbiamo force-chiuso a 4479.26, ma il
                # match cp<=tp non riconosce 4479 per la noise di tick -> tp1).
                lvl_map = {"tp1": 1, "tp2": 2, "tp3": 3}
                cur_lvl = lvl_map.get(sig.status or "", 0)
                new_lvl = lvl_map.get(new_status, 0)
                if new_lvl >= cur_lvl:
                    sig.status = new_status
                else:
                    log(f"#{sig.id} status preservato: {sig.status} (calc {new_status} sarebbe regressione)")
                sig.exit_price = close_price
                sig.closed_at  = close_time
                sig.pnl_usd    = round(total_profit, 2)
                sig.updated_at = datetime.utcnow()
                # Build_mt5_trade_log solo se trade_log proprio vuoto (legacy)
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
