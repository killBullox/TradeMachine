"""
Client Telegram via Telethon.
- Legge i messaggi storici del gruppo
- Si mette in ascolto per i nuovi messaggi in real-time
- Notifica via WebSocket i client connessi
"""
import asyncio
import json
import os
import re
import sys
from datetime import datetime, timezone
from typing import Set, Optional

# Forza UTF-8 su stdout/stderr (necessario su Windows con terminali cp1252)
if hasattr(sys.stdout, 'reconfigure'):
    sys.stdout.reconfigure(encoding='utf-8', errors='replace')
if hasattr(sys.stderr, 'reconfigure'):
    sys.stderr.reconfigure(encoding='utf-8', errors='replace')


import logging
_logger = logging.getLogger("trademachine")

def log(msg: str):
    # Rimuove caratteri non-ASCII (emoji, ecc.) per compatibilità Windows cp1252
    safe = ''.join(c if ord(c) < 128 else '?' for c in msg)
    print(safe, flush=True)
    _logger.info(safe)


from dotenv import load_dotenv
from telethon import TelegramClient, events
from telethon.tl.types import Channel, Chat

from database import SessionLocal, Signal, TradeUpdate, MarketLevel, RawMessage, SLMove, init_db
from parser import parse_message, ParsedSignal, ParsedUpdate, ParsedLevel, ParsedSLMove, ParsedClose

load_dotenv()

API_ID = int(os.getenv("TELEGRAM_API_ID", "0"))
API_HASH = os.getenv("TELEGRAM_API_HASH", "")
PHONE = os.getenv("TELEGRAM_PHONE", "")
GROUP_NAME = os.getenv("TELEGRAM_GROUP", "Inner Circle Trader")

SESSION_FILE = "trading_session"

# Set di WebSocket connessi (gestiti da main.py)
connected_ws: Set = set()

client: TelegramClient = None
_tg_status = "disconnected"   # "connected" | "disconnected" | "auth_needed"
_auth_phone_hash = None        # hash per completare l'autenticazione


def get_tg_status() -> str:
    return _tg_status


async def get_client() -> TelegramClient:
    global client, _tg_status
    if client is None:
        client = TelegramClient(SESSION_FILE, API_ID, API_HASH,
                                connection_retries=-1,
                                retry_delay=5,
                                auto_reconnect=True)
    if not client.is_connected():
        try:
            await client.connect()
        except Exception as e:
            log(f"[Telegram] Errore connessione: {str(e)[:100]}")
            _tg_status = "disconnected"
            raise

    if not await client.is_user_authorized():
        _tg_status = "auth_needed"
        raise RuntimeError("Sessione Telegram non valida — autenticazione necessaria via /api/telegram/auth")

    _tg_status = "connected"
    return client


async def request_auth_code() -> bool:
    """Invia il codice di autenticazione al telefono. Ritorna True se inviato."""
    global client, _auth_phone_hash, _tg_status
    import os as _os

    # Cancella sessione corrotta se esiste
    session_path = SESSION_FILE + ".session"
    if _os.path.exists(session_path):
        try:
            if client and client.is_connected():
                await client.disconnect()
            client = None
        except Exception:
            pass
        _os.remove(session_path)
        log("[Telegram] Sessione corrotta rimossa")

    client = TelegramClient(SESSION_FILE, API_ID, API_HASH,
                            connection_retries=-1, retry_delay=5, auto_reconnect=True)
    await client.connect()

    result = await client.send_code_request(PHONE)
    _auth_phone_hash = result.phone_code_hash
    _tg_status = "auth_needed"
    log(f"[Telegram] Codice di verifica inviato a {PHONE}")
    return True


async def complete_auth(code: str) -> bool:
    """Completa l'autenticazione con il codice ricevuto via SMS/Telegram."""
    global _auth_phone_hash, _tg_status
    if not client or not _auth_phone_hash:
        raise RuntimeError("Prima richiedi il codice con /api/telegram/auth/request")

    try:
        await client.sign_in(PHONE, code, phone_code_hash=_auth_phone_hash)
        _tg_status = "connected"
        _auth_phone_hash = None
        log("[Telegram] Autenticazione completata!")
        return True
    except Exception as e:
        log(f"[Telegram] Errore autenticazione: {str(e)[:100]}")
        raise


async def disconnect_client():
    """Disconnette il client Telegram in modo pulito."""
    global client, _tg_status
    if client and client.is_connected():
        try:
            await client.disconnect()
        except Exception:
            pass
    _tg_status = "disconnected"


async def broadcast_ws(data: dict):
    """Invia un messaggio a tutti i WebSocket connessi."""
    global connected_ws
    if not connected_ws:
        return
    msg = json.dumps(data, default=str)
    dead = set()
    for ws in connected_ws:
        try:
            await ws.send_text(msg)
        except Exception:
            dead.add(ws)
    connected_ws -= dead


def _save_raw(db, msg_id: int, sender: str, text: str, msg_type: str):
    existing = db.query(RawMessage).filter(RawMessage.telegram_msg_id == msg_id).first()
    if existing:
        return
    raw = RawMessage(
        telegram_msg_id=msg_id,
        sender=sender,
        text=text,
        msg_type=msg_type,
    )
    db.add(raw)
    db.commit()


def _append_trade_log(sig, event: str, detail: str, extra: dict = None):
    """Appende un evento al trade_log del segnale (JSON list)."""
    import json as _json
    now_str = datetime.utcnow().isoformat() + "Z"
    entry = {"ts": now_str, "event": event, "detail": detail}
    if extra:
        entry.update(extra)
    try:
        log_list = _json.loads(sig.trade_log) if sig.trade_log else []
    except Exception:
        log_list = []
    log_list.append(entry)
    sig.trade_log = _json.dumps(log_list)


def _save_signal(db, parsed: ParsedSignal, msg_id: int):
    # Duplicato per stesso msg_id
    existing = db.query(Signal).filter(Signal.telegram_msg_id == msg_id).first()
    if existing:
        return

    # Dedup: due segnali sullo stesso simbolo+direzione entro 15 min sono
    # tipicamente un reinoltro/reminder dello stesso trade, NON un nuovo trade.
    # NON filtrare per status: il caso #297 era un reminder arrivato 45s dopo
    # che #296 era andato in SL — col vecchio filtro status=pending/open non
    # matchava #296 e veniva creato un duplicato senza SL. Includi tutti i
    # segnali recenti, poi distingui per cambi nei parametri.
    from datetime import timedelta
    cutoff = datetime.utcnow() - timedelta(minutes=15)
    existing_sig = db.query(Signal).filter(
        Signal.symbol == parsed.symbol,
        Signal.direction == parsed.direction,
        Signal.created_at >= cutoff,
    ).order_by(Signal.created_at.desc()).first()
    if existing_sig:
        # Confronta valori: se qualcosa è diverso, è una modifica
        changes = {}
        if parsed.entry_price and parsed.entry_price != existing_sig.entry_price:
            changes["entry_price"] = (existing_sig.entry_price, parsed.entry_price)
        if parsed.entry_price_high and parsed.entry_price_high != existing_sig.entry_price_high:
            changes["entry_price_high"] = (existing_sig.entry_price_high, parsed.entry_price_high)
        if parsed.stoploss and parsed.stoploss != existing_sig.stoploss:
            changes["stoploss"] = (existing_sig.stoploss, parsed.stoploss)
        if parsed.tp1 and parsed.tp1 != existing_sig.tp1:
            changes["tp1"] = (existing_sig.tp1, parsed.tp1)
        if parsed.tp2 and parsed.tp2 != existing_sig.tp2:
            changes["tp2"] = (existing_sig.tp2, parsed.tp2)
        if parsed.tp3 and parsed.tp3 != existing_sig.tp3:
            changes["tp3"] = (existing_sig.tp3, parsed.tp3)

        if not changes:
            log(f"[Dedup] Segnale {parsed.symbol} {parsed.direction} identico a #{existing_sig.id} (status={existing_sig.status}) - reinoltro scartato")
            return None

        # Cambiamenti: applica modifica solo se il precedente e' ancora attivo.
        # Se gia' chiuso (sl_hit/tp/closed/cancelled), un set di valori diversi
        # significa che e' un nuovo trade legittimo -> cade fuori al codice di
        # creazione signal sotto.
        if existing_sig.status not in ("pending", "open"):
            log(f"[Nuovo] Segnale {parsed.symbol} {parsed.direction} con parametri diversi da #{existing_sig.id} (status={existing_sig.status}) - tratto come nuovo trade")
            # Forza la creazione di un signal nuovo (skip blocco modifica)
            existing_sig = None

    if existing_sig and changes:
        # È una modifica: aggiorna il segnale esistente nel DB
        log(f"[Modifica] Segnale {parsed.symbol} {parsed.direction} modifica #{existing_sig.id}: {changes}")
        for field, (old_val, new_val) in changes.items():
            setattr(existing_sig, field, new_val)
        _append_trade_log(existing_sig, "modified",
            f"Segnale modificato da TG (msg {msg_id}): {', '.join(f'{k}: {v[0]}->{v[1]}' for k, v in changes.items())}")

        # Se ha ordini pendenti su MT5, aggiorna anche quelli
        if existing_sig.mt5_tickets and existing_sig.status == "pending":
            import json as _json
            import mt5_trader
            tickets = _json.loads(existing_sig.mt5_tickets)
            tp_values = [parsed.tp1, parsed.tp2, parsed.tp3]
            new_entry = parsed.entry_price_high or parsed.entry_price
            for i, ticket in enumerate(tickets):
                tp_val = tp_values[i] if i < len(tp_values) else None
                mt5_trader.modify_order(
                    ticket, existing_sig.symbol,
                    new_entry=new_entry if "entry_price" in changes or "entry_price_high" in changes else None,
                    new_sl=parsed.stoploss if "stoploss" in changes else None,
                    new_tp=tp_val if f"tp{i+1}" in changes else None,
                )

        db.add(existing_sig)
        db.commit()
        db.refresh(existing_sig)
        return existing_sig
    sig = Signal(
        telegram_msg_id=msg_id,
        symbol=parsed.symbol,
        direction=parsed.direction,
        entry_price=parsed.entry_price,
        entry_price_high=parsed.entry_price_high,
        tp1=parsed.tp1,
        tp2=parsed.tp2,
        tp3=parsed.tp3,
        stoploss=parsed.stoploss,
        status="pending",
        raw_message=parsed.raw,
        is_risky=getattr(parsed, 'is_risky', False),
    )
    _append_trade_log(sig, "received", f"Segnale Telegram ricevuto: {parsed.symbol} {parsed.direction} entry={parsed.entry_price}-{parsed.entry_price_high} sl={parsed.stoploss} tp1={parsed.tp1}", {"msg_id": msg_id})
    db.add(sig)
    db.commit()
    db.refresh(sig)
    return sig


def _save_update(db, parsed: ParsedUpdate, msg_id: int):
    upd = TradeUpdate(
        telegram_msg_id=msg_id,
        symbol=parsed.symbol,
        price_from=parsed.price_from,
        price_to=parsed.price_to,
        update_text=parsed.status_text,
        raw_message=parsed.raw,
    )
    db.add(upd)
    db.commit()


def _find_active_signal(db, symbol: Optional[str], new_sl: Optional[float]) -> Optional[int]:
    """
    Trova il signal_id più recente aperto/pending che corrisponde al simbolo
    (o, se nessun simbolo, cerca per range di prezzo).
    """
    from sqlalchemy import or_
    q = db.query(Signal).filter(Signal.status.in_(["open", "pending", "tp1", "tp2"]))
    if symbol:
        q = q.filter(Signal.symbol == symbol)
    elif new_sl is not None:
        # Nessun simbolo nel messaggio: cerca segnale il cui entry/SL è vicino al nuovo SL (±5%)
        q = q.filter(
            or_(
                (Signal.entry_price.isnot(None)) & (Signal.entry_price.between(new_sl * 0.95, new_sl * 1.05)),
                (Signal.stoploss.isnot(None)) & (Signal.stoploss.between(new_sl * 0.95, new_sl * 1.05)),
            )
        )
    sig = q.order_by(Signal.created_at.desc()).first()
    return sig.id if sig else None


def _save_sl_move(db, parsed: ParsedSLMove, msg_id: int):
    signal_id = _find_active_signal(db, parsed.symbol, parsed.new_sl)
    move = SLMove(
        signal_id=signal_id,
        telegram_msg_id=msg_id,
        new_sl=parsed.new_sl,
        is_breakeven=parsed.is_breakeven,
        raw_message=parsed.raw,
    )
    db.add(move)
    db.commit()
    log(f"[SLMove] msg={msg_id} sig={signal_id} new_sl={parsed.new_sl} be={parsed.is_breakeven}")


async def _handle_close(db, parsed: ParsedClose, reply_to_msg_id: int = None):
    """
    Gestisce un messaggio di chiusura immediata del trade.
    Logica di abbinamento (in ordine di priorità):
      1. Reply a un messaggio originale → cerca il segnale per telegram_msg_id
      2. Simbolo esplicito nel messaggio → cerca segnale aperto per quel simbolo
      3. Nessun simbolo → chiude TUTTI i segnali aperti con mt5_ticket
    """
    import mt5_trader
    import json as jsonlib
    from datetime import datetime as _dt

    # La chiusura va eseguita SEMPRE, indipendentemente da auto_trade
    # auto_trade controlla l'apertura di nuovi trade, non la chiusura di quelli esistenti

    # Trova i segnali da chiudere
    targets = []

    if reply_to_msg_id:
        sig = db.query(Signal).filter(Signal.telegram_msg_id == reply_to_msg_id).first()
        if sig and sig.status in ("open", "pending", "tp1", "tp2"):
            targets = [sig]
        elif sig:
            # Il reply punta a un segnale ESISTENTE ma già chiuso (SL hit, TP3,
            # cancelled, closed). Il messaggio era diretto a quel trade
            # specifico, e quello è gia' a buon fine: NON fare fallback su
            # altri trade (come e' successo il 30/04 con #284 AUDUSD chiuso
            # erroneamente perche' #287 XAUUSD era andato in SL 34s prima).
            log(f"[Close] reply a msg={reply_to_msg_id} -> #{sig.id} {sig.symbol} status={sig.status} (gia' chiuso) -> skip")
            return

    if not targets and parsed.symbol:
        sigs = db.query(Signal).filter(
            Signal.symbol == parsed.symbol,
            Signal.status.in_(["open", "pending", "tp1", "tp2"]),
            Signal.mt5_ticket.isnot(None),
        ).order_by(Signal.created_at.desc()).all()
        targets = sigs

    if not targets:
        # Nessun simbolo → chiudi tutto
        targets = db.query(Signal).filter(
            Signal.status.in_(["open", "pending", "tp1", "tp2"]),
            Signal.mt5_ticket.isnot(None),
        ).all()

    if not targets:
        log(f"[Close] Nessun segnale aperto da chiudere (sym={parsed.symbol})")
        return

    now = _dt.utcnow()
    raw_lower = (parsed.raw or "").lower()

    # "Book profit" / "take profit" = chiudi il ticket TP1 (consolidare guadagno) + SL a BE sugli altri
    is_book_profit = bool(re.search(r'book\s+(?:\w+\s+)?profit|take\s+profit|secure\s+profit|lock\s+profit',
                                    raw_lower))
    # "Close trade" / "closing trade" = chiusura totale esplicita → chiudi tutto
    is_hard_close = any(k in raw_lower for k in ["close trade", "closing trade", "close the trade",
                                                    "closing the trade", "close here", "close immediately",
                                                    "everyone close", "exit now", "exit trade",
                                                    "exit the trade"])

    for sig in targets:
        tickets = jsonlib.loads(sig.mt5_tickets) if sig.mt5_tickets else [sig.mt5_ticket]

        if is_book_profit and not is_hard_close and len(tickets) > 1:
            # Book profit: chiudi solo il primo ticket (TP1), sposta SL a BE sugli altri
            tp1_ticket = tickets[0]
            ok = mt5_trader.close_position(tp1_ticket, sig.symbol)
            if ok:
                log(f"[BookProfit] #{sig.id} {sig.symbol} ticket TP1={tp1_ticket} chiuso a mercato")
                # Sposta SL a breakeven (entry price) sui ticket rimanenti
                be_price = sig.actual_entry_price or sig.entry_price
                if be_price:
                    for remaining_ticket in tickets[1:]:
                        mt5_trader.modify_sl(remaining_ticket, float(be_price), sig.symbol)
                        log(f"[BookProfit] #{sig.id} {sig.symbol} ticket={remaining_ticket} SL → BE={be_price}")
                # Aggiorna status: almeno TP1 parziale
                if sig.status == "open":
                    sig.status = "tp1"
                sig.updated_at = now
                db.add(sig)
        else:
            # Chiusura totale: chiudi tutti i ticket. Distinguiamo fra
            # posizioni effettivamente aperte (chiusura → 'closed') e pending
            # mai eseguiti (cancellazione → 'cancelled' se nessuna posizione
            # è mai stata riempita).
            position_closed_any = False
            pending_cancelled_any = False
            for ticket in tickets:
                ok = mt5_trader.close_position(ticket, sig.symbol)
                if ok:
                    position_closed_any = True
                    log(f"[Close] #{sig.id} {sig.symbol} ticket={ticket} chiuso (motivo: {parsed.reason or 'manuale'})")
                else:
                    # Potrebbe essere un ordine pending → cancella
                    mt5 = mt5_trader._get_mt5()
                    if mt5:
                        orders = mt5.orders_get(ticket=ticket)
                        if orders:
                            cancel_req = {"action": mt5.TRADE_ACTION_REMOVE, "order": ticket}
                            mt5.order_send(cancel_req)
                            pending_cancelled_any = True
                            log(f"[Close] #{sig.id} {sig.symbol} pending ticket={ticket} cancellato")

            if position_closed_any:
                # Almeno una posizione era effettivamente aperta → trade chiuso
                sig.status = "closed"
                sig.closed_at = now
                sig.updated_at = now
                db.add(sig)
            elif pending_cancelled_any:
                # Solo pending mai riempiti: il trade non si è mai aperto.
                # Se non abbiamo un actual_entry_price il trade è "mancato" → cancelled.
                if not sig.actual_entry_price:
                    sig.status = "cancelled"
                    sig.notes = (sig.notes or "") + " [Close ricevuto: pending mai eseguiti]"
                    sig.closed_at = now
                    sig.updated_at = now
                    db.add(sig)
                else:
                    sig.status = "closed"
                    sig.closed_at = now
                    sig.updated_at = now
                    db.add(sig)

    db.commit()


def _save_level(db, parsed: ParsedLevel, msg_id: int):
    import json as jsonlib
    lvl = MarketLevel(
        symbol=parsed.symbol,
        support_levels=jsonlib.dumps(parsed.support_levels),
        resistance_levels=jsonlib.dumps(parsed.resistance_levels),
        raw_message=parsed.raw,
    )
    db.add(lvl)
    db.commit()


async def process_message(msg_id: int, sender: str, text: str, reply_to_msg_id: int = None, use_llm: bool = False,
                          msg_date: datetime = None, origin: str = "realtime"):
    """Parsa e salva un messaggio, poi notifica i client WS.

    origin: 'realtime' (listener live) | 'edited' (msg editato dal trader) | 'replay' (history replay)
    msg_date: timestamp originale del messaggio TG (UTC naive). Se None usa datetime.utcnow().
    Se il ritardo tra msg_date e ora è > 60s, anche un 'realtime' diventa 'delayed'.
    """
    if not text:
        return

    # Gestione reply "Highly Risky" — marca il segnale originale
    import re as _re
    if reply_to_msg_id and _re.search(r'\b(risky|highly.?risky|high.?risk|aggressive)\b', text, _re.IGNORECASE):
        db = SessionLocal()
        try:
            sig = db.query(Signal).filter(Signal.telegram_msg_id == reply_to_msg_id).first()
            if sig:
                sig.is_risky = True
                db.add(sig)
                db.commit()
                log(f"[Risky] Segnale #{sig.id} marcato come RISKY (reply a msg {reply_to_msg_id})")
        finally:
            db.close()

    # Parser LLM (solo messaggi live) → fallback regex per history
    try:
        from llm_parser import parse_with_llm, llm_to_parsed
        llm_data = await asyncio.get_event_loop().run_in_executor(None, parse_with_llm, text) if use_llm else None
        if llm_data:
            llm_data["_raw"] = text
            msg_type, parsed = llm_to_parsed(llm_data)
            log(f"[LLMParser] type={msg_type} sym={llm_data.get('symbol')} dir={llm_data.get('direction')}")
            if msg_type == "risky_flag":
                sym = llm_data.get("symbol")
                if sym:
                    db2 = SessionLocal()
                    try:
                        sig = db2.query(Signal).filter(
                            Signal.symbol == sym,
                            Signal.status.in_(["open","pending","tp1","tp2"])
                        ).order_by(Signal.created_at.desc()).first()
                        if sig:
                            sig.is_risky = True
                            db2.commit()
                            log(f"[Risky] #{sig.id} marcato RISKY via LLM")
                    finally:
                        db2.close()
                msg_type, parsed = "other", None
        else:
            msg_type, parsed = parse_message(text)
            log(f"[RegexParser] type={msg_type}")
    except Exception as e:
        log(f"[Parser] Errore LLM, uso regex: {str(e)[:80]}")
        msg_type, parsed = parse_message(text)

    db = SessionLocal()
    try:
        _save_raw(db, msg_id, sender, text, msg_type)

        if msg_type == "signal" and parsed:
            sig = _save_signal(db, parsed, msg_id)
            # Auto-trading: piazza ordine MT5 se abilitato
            if sig:
                try:
                    import mt5_trader
                    from mt5_trader import MT5_SYMBOL_MAP
                    import json as _json
                    symbol_supported = sig.symbol.upper() in MT5_SYMBOL_MAP
                    if not symbol_supported:
                        # Simbolo non gestibile su MT5 → annulla subito
                        _append_trade_log(sig, "cancelled", f"Simbolo {sig.symbol} non supportato su MT5, segnale annullato")
                        sig.status = "cancelled"
                        sig.notes = (sig.notes or "") + " [Non gestibile su MT5]"
                        db.add(sig)
                        db.commit()
                        log(f"[AutoTrade] #{sig.id} {sig.symbol} annullato: simbolo non supportato su MT5")
                    elif mt5_trader.is_enabled():
                        # Guard anti-duplicazione: se il segnale ha già ticket MT5, non piazzare altri ordini
                        _already_has_tickets = bool(sig.mt5_ticket or sig.mt5_tickets)
                        if _already_has_tickets:
                            log(f"[AutoTrade] #{sig.id} {sig.symbol} ha gia ticket MT5 - skip place_orders")
                        # Determina origine effettiva: realtime se msg appena arrivato, altrimenti late catch
                        from datetime import timedelta as _td
                        signal_ts = msg_date or sig.created_at or datetime.utcnow()
                        delay_sec = (datetime.utcnow() - signal_ts).total_seconds()
                        if origin == "edited":
                            effective_origin = "edited"
                            catch_reason = "Messaggio TG editato dal trader"
                        elif origin == "replay":
                            effective_origin = "replay"
                            catch_reason = "Segnale da history replay (TM riavviato)"
                        elif delay_sec > 60:
                            effective_origin = "delayed"
                            catch_reason = f"Listener TM ritardato di {int(delay_sec)}s rispetto al messaggio TG"
                        else:
                            effective_origin = "realtime"
                            catch_reason = None
                        if _already_has_tickets:
                            pass
                        else:
                            if effective_origin != "realtime":
                                log(f"[AutoTrade] #{sig.id} {sig.symbol} LATE CATCH ({effective_origin}): {catch_reason}")
                                _append_trade_log(sig, "late_catch", catch_reason)
                            _append_trade_log(sig, "mt5_placing", f"Invio ordini a MT5 per {sig.symbol} {sig.direction}")
                            db.add(sig)
                            db.commit()
                            tickets = mt5_trader.place_orders(sig, catch_origin=effective_origin,
                                                              catch_reason=catch_reason, signal_ts=signal_ts)
                            if tickets:
                                _append_trade_log(sig, "mt5_placed", f"Ordini MT5 piazzati con successo: tickets={tickets}")
                                sig.mt5_ticket = tickets[0]
                                sig.mt5_tickets = _json.dumps(tickets)
                                sig.status = "open"
                                db.add(sig)
                                db.commit()
                                log(f"[AutoTrade] #{sig.id} {sig.symbol} → tickets={tickets}")
                            else:
                                # Late catch: pre-check ha annullato → motivazione pronta
                                _late_reason = getattr(sig, '_late_catch_cancel_reason', None)
                                # Ordine fallito: verifica se il trade è già partito (prezzo oltre entry)
                                _cancel_reason = _late_reason
                                if not _cancel_reason:
                                    try:
                                        mt5 = mt5_trader._get_mt5()
                                        if mt5:
                                            mt5_sym = mt5_trader.MT5_SYMBOL_MAP.get(sig.symbol.upper(), sig.symbol)
                                            mt5.symbol_select(mt5_sym, True)
                                            tick = mt5.symbol_info_tick(mt5_sym)
                                            if tick and tick.ask > 0:
                                                is_buy = sig.direction.lower() == "buy"
                                                entry_upper = max(sig.entry_price or 0, sig.entry_price_high or 0)
                                                entry_lower = min(sig.entry_price or entry_upper, sig.entry_price_high or entry_upper)
                                                if is_buy and tick.ask > entry_upper * 1.002:
                                                    _cancel_reason = f"Prezzo {tick.ask} oltre entry {entry_upper} - timing mancato"
                                                elif not is_buy and tick.bid < entry_lower * 0.998:
                                                    _cancel_reason = f"Prezzo {tick.bid} oltre entry {entry_lower} - timing mancato"
                                    except Exception:
                                        pass

                                if _cancel_reason:
                                    sig.status = "cancelled"
                                    sig.notes = (sig.notes or "") + f" [{_cancel_reason}]"
                                    _tag = "late_catch_cancel" if _late_reason else "mt5_failed"
                                    _append_trade_log(sig, _tag, _cancel_reason)
                                    log(f"[AutoTrade] #{sig.id} {sig.symbol} ANNULLATO: {_cancel_reason}")
                                else:
                                    _append_trade_log(sig, "mt5_failed", "Nessun ticket MT5 ottenuto - controllare log MT5 per dettaglio errore")
                                    log(f"[AutoTrade] #{sig.id} {sig.symbol} nessun ticket: segnale rimane in pending")
                                db.add(sig)
                                db.commit()
                except Exception as e:
                    log(f"[AutoTrade] Errore place_order: {str(e)[:100]}")
            await broadcast_ws({
                "event": "new_signal",
                "data": {
                    "id": sig.id if sig else None,
                    "symbol": parsed.symbol,
                    "direction": parsed.direction,
                    "entry_price": parsed.entry_price,
                    "entry_price_high": parsed.entry_price_high,
                    "tp1": parsed.tp1,
                    "tp2": parsed.tp2,
                    "tp3": parsed.tp3,
                    "stoploss": parsed.stoploss,
                    "created_at": datetime.utcnow().isoformat(),
                }
            })

        elif msg_type == "update" and parsed:
            _save_update(db, parsed, msg_id)
            await broadcast_ws({
                "event": "trade_update",
                "data": {
                    "symbol": parsed.symbol,
                    "price_from": parsed.price_from,
                    "price_to": parsed.price_to,
                    "status_text": parsed.status_text,
                }
            })

        elif msg_type == "sl_move" and parsed:
            _save_sl_move(db, parsed, msg_id)
            # Auto-trading: modifica SL su MT5 per tutti i segnali aperti del simbolo
            try:
                import mt5_trader
                import json as _json
                if mt5_trader.is_enabled():
                    # Routing: prima prova via reply (se il messaggio è reply al
                    # segnale originale → applica SOLO a quel trade, anche se
                    # parsed.symbol è None). Se il segnale puntato è già chiuso,
                    # skip senza fallback. Se non è reply, fallback al simbolo
                    # (richiede parsed.symbol valorizzato per evitare di toccare
                    # trade di simboli diversi — vedi bug #284 del 30/04).
                    open_sigs = []
                    skip = False
                    if reply_to_msg_id:
                        target_sig = db.query(Signal).filter(
                            Signal.telegram_msg_id == reply_to_msg_id
                        ).first()
                        if target_sig and target_sig.status in ("open", "tp1", "tp2") \
                                and target_sig.mt5_ticket and target_sig.closed_at is None:
                            open_sigs = [target_sig]
                        elif target_sig:
                            log(f"[SLMove] reply a #{target_sig.id} {target_sig.symbol} status={target_sig.status} (gia' chiuso) -> skip")
                            skip = True
                    if not skip and not open_sigs:
                        if not parsed.symbol:
                            log(f"[SLMove] msg={msg_id} senza simbolo e senza reply utile -> skip per evitare di toccare trade sbagliati")
                        else:
                            open_sigs = db.query(Signal).filter(
                                Signal.status.in_(["open", "tp1", "tp2"]),
                                Signal.mt5_ticket.isnot(None),
                                Signal.closed_at.is_(None),
                                Signal.symbol == parsed.symbol,
                            ).all()

                    for sig in open_sigs:
                        new_sl = parsed.new_sl
                        if parsed.is_breakeven and sig.actual_entry_price:
                            new_sl = sig.actual_entry_price
                        if new_sl:
                            tickets = _json.loads(sig.mt5_tickets) if sig.mt5_tickets else [sig.mt5_ticket]
                            for ticket in tickets:
                                mt5_trader.modify_sl(ticket, new_sl, sig.symbol)
                            # NON sovrascrivere sig.stoploss (serve per dedup).
                            # Lo SL effettivo è su MT5, il DB tiene l'originale.
                            db.add(sig)
                            log(f"[SLMove] #{sig.id} {sig.symbol} SL→{new_sl} su {len(tickets)} ticket (DB stoploss invariato={sig.stoploss})")
                    db.commit()
            except Exception as e:
                log(f"[AutoTrade] Errore modify_sl: {str(e)[:100]}")
            await broadcast_ws({
                "event": "sl_move",
                "data": {"new_sl": parsed.new_sl, "is_breakeven": parsed.is_breakeven}
            })

        elif msg_type == "reenter" and parsed:
            try:
                import mt5_trader
                import json as _json
                # Routing: se il messaggio è reply al segnale originale, usa
                # ESATTAMENTE quel segnale come template per il reenter, anche
                # se non è il "più recente". Senza reply, fallback al simbolo;
                # se manca pure quello, skip (non scegliere a caso fra simboli
                # diversi — stesso bug visto sul Close di #284 il 30/04).
                from mt5_trader import MT5_SYMBOL_MAP
                tradeable_syms = list(MT5_SYMBOL_MAP.keys())
                last_sig = None
                if reply_to_msg_id:
                    candidate = db.query(Signal).filter(
                        Signal.telegram_msg_id == reply_to_msg_id,
                        Signal.symbol.in_(tradeable_syms),
                    ).first()
                    if candidate:
                        last_sig = candidate
                if last_sig is None:
                    if not parsed.symbol:
                        log(f"[Reenter] msg={msg_id} senza simbolo e senza reply utile -> skip per evitare di clonare il trade sbagliato")
                    else:
                        last_sig = db.query(Signal).filter(
                            Signal.status.in_(["sl_hit", "closed", "tp1", "tp2", "tp3"]),
                            Signal.symbol.in_(tradeable_syms),
                            Signal.symbol == parsed.symbol,
                        ).order_by(Signal.created_at.desc()).first()

                if last_sig:
                    # Clona come nuovo segnale
                    new_sig = Signal(
                        symbol=last_sig.symbol,
                        direction=last_sig.direction,
                        entry_price=last_sig.entry_price,
                        entry_price_high=last_sig.entry_price_high,
                        tp1=last_sig.tp1, tp2=last_sig.tp2, tp3=last_sig.tp3,
                        stoploss=last_sig.stoploss,
                        is_risky=last_sig.is_risky,
                        raw_message=f"[REENTER] {parsed.raw}",
                        telegram_msg_id=msg_id,
                    )
                    db.add(new_sig)
                    db.flush()  # get new_sig.id

                    _append_trade_log(new_sig, "reenter", f"Rientro nel trade #{last_sig.id} {last_sig.symbol} {last_sig.direction}")
                    log(f"[Reenter] Nuovo segnale #{new_sig.id} clonato da #{last_sig.id} {last_sig.symbol}")

                    if mt5_trader.is_enabled():
                        _append_trade_log(new_sig, "mt5_placing", f"Invio ordini a MT5 per {new_sig.symbol} {new_sig.direction}")
                        db.add(new_sig)
                        db.commit()
                        tickets = mt5_trader.place_orders(new_sig)
                        if tickets:
                            _append_trade_log(new_sig, "mt5_placed", f"Ordini MT5 piazzati: tickets={tickets}")
                            new_sig.mt5_ticket = tickets[0]
                            new_sig.mt5_tickets = _json.dumps(tickets)
                            new_sig.status = "open"
                            log(f"[Reenter] #{new_sig.id} → tickets={tickets}")
                        else:
                            # Simbolo non supportato o ordine fallito → annulla
                            from mt5_trader import MT5_SYMBOL_MAP
                            if new_sig.symbol.upper() not in MT5_SYMBOL_MAP:
                                _append_trade_log(new_sig, "cancelled", f"Simbolo {new_sig.symbol} non supportato su MT5, segnale annullato")
                                new_sig.status = "cancelled"
                                new_sig.notes = (new_sig.notes or "") + " [Non gestibile su MT5]"
                            else:
                                _append_trade_log(new_sig, "mt5_failed", "Nessun ticket ottenuto")
                    db.add(new_sig)
                    db.commit()

                    await broadcast_ws({
                        "event": "new_signal",
                        "data": {"id": new_sig.id, "symbol": new_sig.symbol,
                                 "direction": new_sig.direction, "reenter": True}
                    })
                else:
                    log(f"[Reenter] Nessun segnale chiuso trovato per {parsed.symbol}")
            except Exception as e:
                log(f"[Reenter] Errore: {str(e)[:100]}")

        elif msg_type == "close" and parsed:
            await _handle_close(db, parsed, reply_to_msg_id)
            await broadcast_ws({
                "event": "trade_close",
                "data": {"symbol": parsed.symbol, "reason": parsed.reason}
            })


        elif msg_type == "level" and parsed:
            await broadcast_ws({
                "event": "market_levels",
                "data": {
                    "symbol": parsed.symbol,
                    "support": parsed.support_levels,
                    "resistance": parsed.resistance_levels,
                }
            })

        else:
            await broadcast_ws({
                "event": "raw_message",
                "data": {"text": text[:200], "type": msg_type}
            })

    finally:
        db.close()


async def load_history(limit: int = 500, since: datetime = None):
    """Carica i messaggi storici del gruppo. Se since è specificato, carica solo da quella data."""
    tg = await get_client()
    target = None

    async for dialog in tg.iter_dialogs():
        if GROUP_NAME.lower() in dialog.name.lower():
            target = dialog.entity
            break

    if not target:
        log(f"[Telegram] Gruppo '{GROUP_NAME}' non trovato!")
        return

    date_str = f" dal {since.strftime('%d/%m/%Y')}" if since else ""
    log(f"[Telegram] Caricamento messaggi{date_str} da '{dialog.name}'...")
    count = 0
    errors = 0

    # Raccoglie tutti i messaggi prima di scrivere nel DB (evita lock SQLite)
    messages_batch = []
    iter_kwargs = {"limit": limit}
    if since:
        # offset_date con reverse=False scarica i messaggi PIÙ VECCHI di quella data
        # Per avere i messaggi DA quella data usiamo un filtro post-fetch
        iter_kwargs["limit"] = 5000  # aumenta il limite per compensare
    async for message in tg.iter_messages(target, **iter_kwargs):
        # Salva il timestamp originale del messaggio Telegram
        msg_date = message.date
        if msg_date and msg_date.tzinfo:
            msg_date = msg_date.replace(tzinfo=None)  # UTC naive
        # Filtra per data se specificata
        if since and msg_date and msg_date < since:
            continue
        messages_batch.append((message.id, message.sender, message.text or "", msg_date))

    log(f"[Telegram] Recuperati {len(messages_batch)} messaggi, salvataggio in DB...")

    # Usa una singola sessione DB per tutto il batch
    db = SessionLocal()
    try:
        for msg_id, sender_obj, text, msg_date in messages_batch:
            try:
                sender_name = ""
                if sender_obj:
                    sender_name = getattr(sender_obj, 'username', '') or \
                                  getattr(sender_obj, 'first_name', '') or ''

                if not text:
                    continue

                # Controlla duplicati
                existing = db.query(RawMessage).filter(RawMessage.telegram_msg_id == msg_id).first()
                if existing:
                    continue

                msg_type, parsed = parse_message(text)

                ts = msg_date or datetime.utcnow()
                raw = RawMessage(telegram_msg_id=msg_id, sender=sender_name, text=text,
                                 msg_type=msg_type, created_at=ts)
                db.add(raw)

                if msg_type == "signal" and parsed:
                    if not db.query(Signal).filter(Signal.telegram_msg_id == msg_id).first():
                        sig = Signal(
                            telegram_msg_id=msg_id, symbol=parsed.symbol, direction=parsed.direction,
                            entry_price=parsed.entry_price, entry_price_high=parsed.entry_price_high,
                            tp1=parsed.tp1, tp2=parsed.tp2, tp3=parsed.tp3, stoploss=parsed.stoploss,
                            status="pending", raw_message=parsed.raw,
                            created_at=ts, updated_at=ts,
                        )
                        db.add(sig)

                elif msg_type == "update" and parsed:
                    upd = TradeUpdate(
                        telegram_msg_id=msg_id, symbol=parsed.symbol,
                        price_from=parsed.price_from, price_to=parsed.price_to,
                        update_text=parsed.status_text, raw_message=parsed.raw,
                        created_at=ts,
                    )
                    db.add(upd)

                elif msg_type == "level" and parsed:
                    import json as jsonlib
                    lvl = MarketLevel(
                        symbol=parsed.symbol,
                        support_levels=jsonlib.dumps(parsed.support_levels),
                        resistance_levels=jsonlib.dumps(parsed.resistance_levels),
                        raw_message=parsed.raw, date=ts,
                    )
                    db.add(lvl)

                count += 1

                # Commit ogni 50 messaggi
                if count % 50 == 0:
                    db.commit()
                    log(f"[Telegram] Salvati {count} messaggi...")

            except Exception as e:
                errors += 1
                db.rollback()
                log(f"[Telegram] Errore msg {msg_id}: {str(e)[:80]}")

        db.commit()
    finally:
        db.close()

    log(f"[Telegram] Caricati {count} messaggi storici ({errors} errori).")


async def start_listener():
    """Avvia il listener real-time per nuovi messaggi."""
    tg = await get_client()
    target = None

    async for dialog in tg.iter_dialogs():
        if GROUP_NAME.lower() in dialog.name.lower():
            target = dialog.entity
            break

    if not target:
        log(f"[Telegram] Gruppo '{GROUP_NAME}' non trovato per il listener!")
        return

    @tg.on(events.NewMessage(chats=target))
    async def handler(event):
        sender_name = ""
        if event.sender:
            sender_name = getattr(event.sender, 'username', '') or \
                          getattr(event.sender, 'first_name', '') or ''
        reply_to = event.message.reply_to_msg_id if event.message.reply_to else None
        msg_date = event.message.date
        if msg_date and msg_date.tzinfo:
            msg_date = msg_date.replace(tzinfo=None)
        await process_message(event.message.id, sender_name, event.message.text or "",
                              reply_to_msg_id=reply_to, use_llm=True,
                              msg_date=msg_date, origin="realtime")

    @tg.on(events.MessageEdited(chats=target))
    async def on_edited(event):
        """Se un messaggio viene editato e corrisponde a un segnale pending/scartato, riprocessalo."""
        msg_id = event.message.id
        text = event.message.text or ""
        db = SessionLocal()
        try:
            # Accetta sia 'pending' sia 'cancelled' senza ticket: in particolare
            # i segnali sospesi per R/R sproporzionato vengono annullati in
            # attesa proprio dell'edit del trader.
            sig = db.query(Signal).filter(
                Signal.telegram_msg_id == msg_id,
                Signal.status.in_(["pending", "cancelled"]),
            ).first()
            if sig and not sig.mt5_ticket:
                # Se era cancellato in attesa di edit, lo riportiamo a pending
                # prima di ritentare il piazzamento.
                if sig.status == "cancelled":
                    sig.status = "pending"
                    log(f"[Edit] #{sig.id} riapro segnale cancellato (in attesa di edit)")
                log(f"[Edit] Messaggio TG {msg_id} editato — riprocesso segnale #{sig.id} {sig.symbol}")
                # Riparsa il messaggio editato
                from llm_parser import parse_message
                parsed = parse_message(text, use_llm=True)
                if parsed and hasattr(parsed, 'symbol') and parsed.symbol:
                    changed = []
                    for field in ['entry_price', 'entry_price_high', 'tp1', 'tp2', 'tp3', 'stoploss', 'direction']:
                        new_val = getattr(parsed, field, None)
                        old_val = getattr(sig, field, None)
                        if new_val and new_val != old_val:
                            setattr(sig, field, new_val)
                            changed.append(f"{field}: {old_val}->{new_val}")
                    if changed:
                        _append_trade_log(sig, "edited", f"Messaggio TG editato: {', '.join(changed)}")
                        sig.raw_message = text
                        db.add(sig)
                        db.commit()
                        db.refresh(sig)
                        log(f"[Edit] #{sig.id} aggiornato: {changed}")
                        # Ritenta piazzamento MT5 se auto-trade attivo (origine: edit TG → late catch)
                        import mt5_trader
                        if mt5_trader.is_enabled():
                            import json as _json
                            log(f"[Edit] Ritento piazzamento MT5 per #{sig.id} (origin=edited)")
                            _edit_reason = "Messaggio TG editato dal trader"
                            _append_trade_log(sig, "late_catch", _edit_reason)
                            tickets = mt5_trader.place_orders(sig, catch_origin="edited",
                                                              catch_reason=_edit_reason,
                                                              signal_ts=sig.created_at)
                            if tickets:
                                _append_trade_log(sig, "mt5_placed", f"Ordini MT5 piazzati dopo edit: tickets={tickets}")
                                sig.mt5_ticket = tickets[0]
                                sig.mt5_tickets = _json.dumps(tickets)
                                sig.status = "open"
                                db.add(sig)
                                db.commit()
                                log(f"[Edit] #{sig.id} piazzato: tickets={tickets}")
                            else:
                                _late_reason = getattr(sig, '_late_catch_cancel_reason', None)
                                if _late_reason:
                                    sig.status = "cancelled"
                                    sig.notes = (sig.notes or "") + f" [{_late_reason}]"
                                    _append_trade_log(sig, "late_catch_cancel", _late_reason)
                                    db.add(sig)
                                    db.commit()
                                    log(f"[Edit] #{sig.id} ANNULLATO: {_late_reason}")
                    else:
                        log(f"[Edit] #{sig.id} nessuna modifica rilevata nell'edit")
        except Exception as e:
            log(f"[Edit] Errore: {str(e)[:100]}")
        finally:
            db.close()

    @tg.on(events.MessageDeleted(chats=target))
    async def on_deleted(event):
        """Se un segnale viene cancellato entro pochi secondi → marca come cancelled."""
        from datetime import datetime as _dt, timedelta
        deleted_ids = event.deleted_ids or []
        if not deleted_ids:
            return
        db = SessionLocal()
        try:
            cutoff = _dt.utcnow() - timedelta(minutes=5)
            for tg_id in deleted_ids:
                sig = db.query(Signal).filter(
                    Signal.telegram_msg_id == tg_id,
                    Signal.created_at >= cutoff,
                    Signal.status == "pending",
                ).first()
                if sig:
                    sig.status = "cancelled"
                    sig.notes = (sig.notes or "") + " [Messaggio Telegram cancellato dall'admin]"
                    db.add(sig)
                    log(f"[Deleted] Segnale #{sig.id} annullato perché il msg TG è stato cancellato")
            db.commit()
        except Exception as e:
            log(f"[Deleted] Errore: {str(e)[:80]}")
        finally:
            db.close()

    log(f"[Telegram] Listener attivo su '{dialog.name}' — in attesa di nuovi messaggi...")
    # Keepalive loop: mantiene il task vivo e riconnette se necessario
    while True:
        try:
            if not tg.is_connected():
                log("[Telegram] Disconnesso — tentativo di riconnessione...")
                await tg.connect()
            await asyncio.sleep(30)
        except Exception as e:
            log(f"[Telegram] Errore keepalive: {str(e)[:80]}")
            await asyncio.sleep(10)
