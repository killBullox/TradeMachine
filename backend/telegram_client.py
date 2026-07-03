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
        entry_type=getattr(parsed, 'entry_type', None),
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
    # PAPER TRADE (filtered): per i reali il sync MT5 aggiorna sig.stoploss;
    # per i filtered nessun sync esiste, quindi lo aggiorniamo qui direttamente.
    if signal_id:
        tgt = db.query(Signal).filter(Signal.id == signal_id).first()
        if tgt and getattr(tgt, "is_filtered", False):
            new_sl = parsed.new_sl
            if new_sl is None and parsed.is_breakeven:
                new_sl = tgt.actual_entry_price or tgt.entry_price or tgt.entry_price_high
            if new_sl is not None:
                tgt.stoploss = new_sl
                db.add(tgt); db.commit()
                log(f"[SLMove] paper #{tgt.id} stoploss aggiornato a {new_sl}")


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

    log(f"[Close] handler START: symbol={parsed.symbol} reason='{(parsed.reason or '')[:80]}' reply_to={reply_to_msg_id}")

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

    log(f"[Close] {len(targets)} target(s): {[(t.id, t.symbol, t.status) for t in targets]}")

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
        # PAPER TRADE (is_filtered): nessun ticket MT5. Chiudi come simulazione:
        # marca status closed, prendi prezzo mercato corrente come exit, calcola
        # pnl teorico da entry->exit sui lots del paper.
        if getattr(sig, "is_filtered", False):
            if sig.status not in ("open", "tp1", "tp2"):
                log(f"[Close] paper #{sig.id} status={sig.status} → nulla da chiudere")
                continue
            reason_txt = parsed.reason or "Close da TG"
            close_price = None
            try:
                import mt5_trader as _mt5t
                mt5 = _mt5t._get_mt5()
                bsym = _mt5t.get_mt5_symbol(sig.symbol)
                if mt5 and bsym:
                    tick = mt5.symbol_info_tick(bsym)
                    if tick:
                        is_buy_p = (sig.direction or "buy").lower() == "buy"
                        close_price = tick.bid if is_buy_p else tick.ask
            except Exception as _e:
                log(f"[Close] paper #{sig.id} tick err: {_e}")
            if close_price is None:
                close_price = sig.actual_entry_price or sig.entry_price
            sig.status = "closed"
            sig.exit_price = close_price
            sig.closed_at = now
            sig.updated_at = now
            # Ricalcola pnl teorico: entry -> close_price su lots del paper
            try:
                from risk import calc_pnl
                entry = sig.actual_entry_price or sig.entry_price
                if entry and sig.position_size:
                    sig.pnl_usd = round(calc_pnl(sig.symbol, sig.direction or "buy",
                                                  entry, close_price, sig.position_size), 2)
            except Exception as _e:
                log(f"[Close] paper #{sig.id} calc pnl err: {_e}")
            _append_trade_log(sig, "tg_close",
                f"Close TG applicato (paper): exit @ {close_price}, pnl={sig.pnl_usd}$, motivo={reason_txt}",
                {"reason": reason_txt, "close_price": close_price})
            db.add(sig); db.commit()
            log(f"[Close] paper #{sig.id} {sig.symbol} chiuso @ {close_price} pnl={sig.pnl_usd}")
            continue
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

            reason_txt = parsed.reason or "Close da TG"
            if position_closed_any:
                # Almeno una posizione era effettivamente aperta → trade chiuso
                sig.status = "closed"
                sig.closed_at = now
                sig.updated_at = now
                # Calcola P&L reale dai deal MT5 (profit + commission + swap sui deal OUT).
                # Necessario perche' sync_positions dopo il close skippa l'update
                # (closed_at gia' settato), lasciando pnl_usd al valore floating vecchio.
                try:
                    import mt5_trader as _mt5t
                    import time as _time
                    _time.sleep(0.8)  # attendi che i deal siano in history
                    mt5 = _mt5t._get_mt5()
                    if mt5:
                        total_real = 0.0
                        for tk in tickets:
                            deals = mt5.history_deals_get(position=tk)
                            if not deals:
                                continue
                            for d in deals:
                                if d.entry == mt5.DEAL_ENTRY_OUT:
                                    total_real += float(d.profit) + float(getattr(d, 'commission', 0) or 0) + float(getattr(d, 'swap', 0) or 0)
                        if total_real != 0.0:
                            sig.pnl_usd = round(total_real, 2)
                            log(f"[Close] #{sig.id} pnl reale da deal MT5 = {sig.pnl_usd}$")
                except Exception as _e:
                    log(f"[Close] #{sig.id} errore calcolo pnl reale: {_e}")
                _append_trade_log(sig, "tg_close",
                    f"Close da Telegram applicato: posizioni chiuse (motivo: {reason_txt})",
                    {"reason": reason_txt})
                db.add(sig)
            elif pending_cancelled_any:
                # Solo pending mai riempiti: il trade non si è mai aperto.
                # Se non abbiamo un actual_entry_price il trade è "mancato" → cancelled.
                if not sig.actual_entry_price:
                    sig.status = "cancelled"
                    sig.notes = (sig.notes or "") + " [Close ricevuto: pending mai eseguiti]"
                    sig.closed_at = now
                    sig.updated_at = now
                    _append_trade_log(sig, "tg_cancel",
                        f"Close da Telegram applicato: pending mai eseguiti, segnale annullato (motivo: {reason_txt})",
                        {"reason": reason_txt})
                    db.add(sig)
                    # EMA: registra caso STOP mai filled
                    try:
                        import mt5_trader as _mt5t
                        _mt5t.analyze_ema_case(sig.id, "tg_close")
                    except Exception:
                        pass
                else:
                    sig.status = "closed"
                    sig.closed_at = now
                    sig.updated_at = now
                    _append_trade_log(sig, "tg_close",
                        f"Close da Telegram applicato (motivo: {reason_txt})",
                        {"reason": reason_txt})
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

    # Gestione "Highly Risky" / "#RiskyTrade" — marca il segnale.
    # Match anche hashtag attaccati (#RiskyTrade): no word-boundary in coda.
    # Se reply: marca il segnale referenziato.
    # Altrimenti: marca il segnale piu' recente entro 120s con status attivo
    # (caso #312: hashtag standalone arrivato 6s dopo il segnale).
    import re as _re
    risky_re = _re.compile(r'(?i)(?:^|[\s#])(?:risky|highly.?risky|high.?risk|aggressive)')
    if risky_re.search(text or ""):
        db = SessionLocal()
        try:
            sig = None
            if reply_to_msg_id:
                sig = db.query(Signal).filter(Signal.telegram_msg_id == reply_to_msg_id).first()
            if sig is None:
                from datetime import datetime as _dt, timedelta as _td
                cutoff = _dt.utcnow() - _td(seconds=120)
                sig = (db.query(Signal)
                         .filter(Signal.created_at >= cutoff)
                         .filter(Signal.status.in_(["open", "pending", "tp1", "tp2"]))
                         .order_by(Signal.created_at.desc())
                         .first())
            if sig and not sig.is_risky:
                sig.is_risky = True
                db.add(sig)
                db.commit()
                log(f"[Risky] Segnale #{sig.id} marcato RISKY (reply={reply_to_msg_id} text={text[:60]!r})")
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

    # FALLBACK REENTER: se LLM/regex hanno classificato come "other" o "update" ma
    # il msg contiene chiaramente un pattern di reenter ("re enter", "re-enter",
    # "reenter", "enter again"), forza reenter. Cattura casi come #345-style:
    # "Sl hit In Sudden Spike Risky Can Re enter Here With Same Sl" → other.
    if msg_type in ("other", "update", "ignore") and text:
        import re as _re_re
        if _re_re.search(r'\b(re\s*-?\s*enter|reenter|enter\s+again|open\s+again)\b', text, _re_re.IGNORECASE):
            from parser import ParsedReenter as _PR
            # Tenta a estrarre il simbolo dal testo (se presente)
            sym_m = _re_re.search(r'#([A-Z]{3,8})', text)
            forced_sym = sym_m.group(1).upper() if sym_m else None
            parsed = _PR(symbol=forced_sym, raw=text)
            msg_type = "reenter"
            log(f"[FallbackReenter] msg riclassificato 'other/update' → 'reenter' (sym={forced_sym})")

    db = SessionLocal()
    try:
        _save_raw(db, msg_id, sender, text, msg_type)

        if msg_type == "signal" and parsed:
            # ── PROP MODE GUARD: Daily DD kill-switch.
            # Gated da `prop_mode + daily_dd_limit_usd` settati sull'account
            # attivo. Per Avatrade (prop_mode=False) `should_block_new_trades`
            # ritorna SEMPRE None → questo if NON viene mai eseguito e il
            # comportamento Avatrade resta identico.
            try:
                from prop_mode import should_block_new_trades as _block_check, check_max_concurrent_trades as _conc_check
                _block_reason = _block_check()
                if _block_reason:
                    log(f"[PropGuard] signal msg={msg_id} BLOCCATO (daily DD): {_block_reason}")
                    return
                _conc_reason = _conc_check()
                if _conc_reason:
                    log(f"[PropGuard] signal msg={msg_id} BLOCCATO (max concurrent): {_conc_reason}")
                    return
            except Exception as _e:
                log(f"[PropGuard] errore check: {str(_e)[:80]}")

            # Duplicate signal detection con sostituzione del vecchio trade.
            # Caso #446/#447 (11/06): stesso signal BTCUSD ripostato 52min dopo
            # → doppio trade aperto.
            # Logica: signal recente (<2h) stesso simbolo+direzione+livelli
            # entro 0.3% (tolleranza stretta per evitare falsi positivi).
            #   - vecchio gia' in profitto forte (>50% verso TP1) → SKIP nuovo
            #     (tieni il vecchio che sta andando bene)
            #   - altrimenti tenta close del vecchio:
            #       close OK → procedi col nuovo signal normalmente
            #       close FAIL → ABORT nuovo (no doppio trade involontario)
            if parsed.symbol and parsed.direction and parsed.stoploss and parsed.tp1:
                from datetime import timedelta
                TOL = 0.003  # 0.3% tolleranza
                recent_cutoff = datetime.utcnow() - timedelta(hours=2)
                # Include anche i paper trade (is_filtered=True): non hanno ticket
                # MT5 per design ma il vecchio va comunque cancellato quando il
                # trader riposta lo stesso signal.
                candidates = db.query(Signal).filter(
                    Signal.symbol == parsed.symbol,
                    Signal.direction == parsed.direction,
                    Signal.created_at >= recent_cutoff,
                    Signal.status.in_(("pending", "open", "tp1", "tp2")),
                ).order_by(Signal.created_at.desc()).all()
                import json as _jl_dup, mt5_trader as _mt5t_dup
                _aborted = False
                for cand in candidates:
                    def _close(a, b):
                        if a is None or b is None: return False
                        if a == 0: return abs(b) < TOL
                        return abs(a - b) / abs(a) <= TOL
                    same_sl = _close(parsed.stoploss, cand.stoploss)
                    same_tp1 = _close(parsed.tp1, cand.tp1)
                    same_entry = _close(parsed.entry_price, cand.entry_price) or _close(parsed.entry_price, cand.entry_price_high)
                    if not (same_sl and same_tp1 and same_entry):
                        continue
                    # PAPER TRADE: nessun ticket MT5, marca direttamente cancelled
                    if getattr(cand, "is_filtered", False):
                        cand.status = "cancelled"
                        cand.closed_at = datetime.utcnow()
                        cand.updated_at = datetime.utcnow()
                        cand.notes = (cand.notes or "") + f" [Paper sostituito da signal duplicato msg={msg_id}]"
                        _append_trade_log(cand, "duplicate_replaced",
                            f"Trader ha ripostato signal identico: paper vecchio marcato cancelled.",
                            {"replaced_by_msg": msg_id})
                        db.add(cand); db.commit()
                        log(f"[Duplicate] msg={msg_id} sostituisce paper #{cand.id}: marcato cancelled. Procedo col nuovo.")
                        break
                    tk = []
                    if cand.mt5_tickets:
                        try: tk = _jl_dup.loads(cand.mt5_tickets)
                        except Exception: tk = []
                    elif cand.mt5_ticket:
                        tk = [cand.mt5_ticket]
                    mt5_inst_dup = _mt5t_dup._get_mt5() if _mt5t_dup.is_enabled() else None
                    if not (mt5_inst_dup and tk):
                        continue
                    # Verifica ticket ancora attivi
                    active_tickets = []
                    for t in tk:
                        if mt5_inst_dup.positions_get(ticket=t) or mt5_inst_dup.orders_get(ticket=t):
                            active_tickets.append(t)
                    if not active_tickets:
                        continue
                    # Check profitto > 50% verso TP1
                    is_buy_c = (cand.direction or "").lower() == "buy"
                    cur_price = None
                    try:
                        from mt5_trader import MT5_SYMBOL_MAP as _MAP_DUP
                        sym = _MAP_DUP.get(cand.symbol.upper(), cand.symbol)
                        tick = mt5_inst_dup.symbol_info_tick(sym)
                        if tick: cur_price = (tick.bid + tick.ask) / 2
                    except Exception: pass
                    in_strong_profit = False
                    if cur_price and cand.actual_entry_price and cand.tp1:
                        tp1_dist = abs(float(cand.tp1) - float(cand.actual_entry_price))
                        if tp1_dist > 0:
                            cur_dist = abs(cur_price - float(cand.actual_entry_price))
                            progress = cur_dist / tp1_dist
                            dir_ok = (is_buy_c and cur_price > cand.actual_entry_price) or (not is_buy_c and cur_price < cand.actual_entry_price)
                            in_strong_profit = dir_ok and progress > 0.5
                    if in_strong_profit:
                        log(f"[Duplicate] msg={msg_id} match #{cand.id} ma in profitto >50% verso TP1 → SKIP nuovo, tieni vecchio")
                        return
                    # Tenta close del vecchio
                    cancelled_pend = 0; closed_pos = 0; failed_tk = []
                    for t in active_tickets:
                        if mt5_inst_dup.orders_get(ticket=t):
                            r = mt5_inst_dup.order_send({"action": mt5_inst_dup.TRADE_ACTION_REMOVE, "order": t})
                            if r and r.retcode == mt5_inst_dup.TRADE_RETCODE_DONE: cancelled_pend += 1
                            else: failed_tk.append(t)
                        elif mt5_inst_dup.positions_get(ticket=t):
                            if _mt5t_dup.close_position(t, cand.symbol): closed_pos += 1
                            else: failed_tk.append(t)
                    if failed_tk:
                        log(f"[Duplicate] msg={msg_id} close vecchio #{cand.id} FAILED su {failed_tk} → ABORT nuovo (no doppio trade)")
                        _aborted = True
                        break
                    # Close OK: marca vecchio cancelled, procedi col nuovo
                    cand.status = "cancelled"
                    cand.closed_at = datetime.utcnow()
                    cand.updated_at = datetime.utcnow()
                    cand.notes = (cand.notes or "") + f" [Sostituito da signal duplicato msg={msg_id}: chiusi {closed_pos} pos / {cancelled_pend} pend]"
                    _append_trade_log(cand, "duplicate_replaced",
                        f"Trader ha ripostato signal identico (entro 0.3%): vecchio chiuso per fare spazio al nuovo. closed_pos={closed_pos} cancelled_pend={cancelled_pend}.",
                        {"replaced_by_msg": msg_id, "closed_positions": closed_pos, "cancelled_pendings": cancelled_pend})
                    db.add(cand); db.commit()
                    log(f"[Duplicate] msg={msg_id} sostituisce #{cand.id}: chiusi {closed_pos} pos / {cancelled_pend} pend. Procedo col nuovo.")
                    break
                if _aborted:
                    return
            # Anti-misclass LLM: signal reale richiede almeno SL + TP1.
            # Se entrambi mancano = msg interpretabile come istruzione sul signal
            # piu' recente (caso #436 09/06: "Everyone Enter Now Cmp 4341").
            # Routing:
            #  - trade originale gia' fillato → IGNORA (siamo dentro)
            #  - trade pending non filled + msg "cmp/enter now" → MARKET sul ref
            #  - trade pending non filled + msg con "at X" → MODIFY entry range
            #  - trade gia' chiuso/cancellato → IGNORA (no trade da modificare)
            if parsed.stoploss is None and parsed.tp1 is None:
                import re as _re_me, json as _jl_me, mt5_trader as _mt5t
                ref = None
                if parsed.symbol:
                    ref = db.query(Signal).filter(Signal.symbol == parsed.symbol).order_by(Signal.created_at.desc()).first()
                if not ref:
                    log(f"[MarketEntry] msg={msg_id} '{text[:60]}' nessun signal di riferimento → ignore")
                    return
                tickets_ref = []
                if ref.mt5_tickets:
                    try: tickets_ref = _jl_me.loads(ref.mt5_tickets)
                    except Exception: tickets_ref = []
                elif ref.mt5_ticket:
                    tickets_ref = [ref.mt5_ticket]
                mt5_inst = _mt5t._get_mt5() if _mt5t.is_enabled() else None
                has_open_pos = False
                has_pending_orders = False
                if mt5_inst and tickets_ref:
                    for t in tickets_ref:
                        if mt5_inst.positions_get(ticket=t): has_open_pos = True
                        elif mt5_inst.orders_get(ticket=t): has_pending_orders = True

                # CASO A: trade gia' fillato (siamo dentro)
                if has_open_pos:
                    log(f"[MarketEntry] #{ref.id} {ref.symbol} {ref.direction} gia' fillato → IGNORE msg '{text[:60]}'")
                    return

                # CASO B: trade pending non filled → market o modify entry
                if has_pending_orders and ref.status == "pending":
                    txt_low = (text or "").lower()
                    wants_market = bool(_re_me.search(r'\b(cmp|enter\s+now|now)\b', txt_low))
                    # Cerca "at X" / "@X" / "to X" per modify entry
                    modify_match = _re_me.search(r'(?:enter|move|modify|change|@|at|to)\s+(\d+(?:\.\d+)?)', txt_low)
                    if not wants_market and modify_match:
                        new_entry = float(modify_match.group(1))
                        log(f"[ModifyEntry] #{ref.id} {ref.symbol}: modifica entry range → {new_entry} (msg: '{text[:60]}')")
                        if mt5_inst:
                            for t in tickets_ref:
                                if mt5_inst.orders_get(ticket=t):
                                    mt5_inst.order_send({"action": mt5_inst.TRADE_ACTION_REMOVE, "order": t})
                        old_low, old_high = ref.entry_price, ref.entry_price_high
                        ref.entry_price = new_entry
                        ref.entry_price_high = new_entry
                        ref.mt5_ticket = None
                        ref.mt5_tickets = None
                        _append_trade_log(ref, "entry_modified",
                            f"TG '{text[:80]}': entry range {old_low}-{old_high} → {new_entry}, pending cancellati e ripiazzati.",
                            {"trigger_msg_id": msg_id, "old_low": old_low, "old_high": old_high, "new": new_entry})
                        db.add(ref); db.commit(); db.refresh(ref)
                        try:
                            tickets_new = _mt5t.place_orders(ref, catch_origin="realtime",
                                catch_reason=f"trader: modifica entry {new_entry}",
                                signal_ts=ref.created_at)
                            if tickets_new:
                                ref.mt5_tickets = _jl_me.dumps(tickets_new) if len(tickets_new) > 1 else None
                                ref.mt5_ticket = tickets_new[0]
                                db.add(ref); db.commit()
                                log(f"[ModifyEntry] #{ref.id} pending ripiazzati tickets={tickets_new}")
                            else:
                                log(f"[ModifyEntry] #{ref.id} place_orders vuoto")
                        except Exception as _e:
                            log(f"[ModifyEntry] #{ref.id} errore: {str(_e)[:120]}")
                        return
                    if wants_market:
                        log(f"[MarketEntry] #{ref.id} {ref.symbol}: MARKET entry su pending non filled (msg: '{text[:60]}')")
                        if mt5_inst:
                            for t in tickets_ref:
                                if mt5_inst.orders_get(ticket=t):
                                    mt5_inst.order_send({"action": mt5_inst.TRADE_ACTION_REMOVE, "order": t})
                        ref.mt5_ticket = None
                        ref.mt5_tickets = None
                        ref.entry_type = "market"  # forza MARKET nel place_orders
                        _append_trade_log(ref, "market_entry_forced",
                            f"TG '{text[:80]}': pending cancellati, ripiazzo MARKET (cmp/enter now).",
                            {"trigger_msg_id": msg_id})
                        db.add(ref); db.commit(); db.refresh(ref)
                        try:
                            tickets_new = _mt5t.place_orders(ref, catch_origin="realtime",
                                catch_reason="trader: market entry su signal pending",
                                signal_ts=ref.created_at)
                            if tickets_new:
                                ref.mt5_tickets = _jl_me.dumps(tickets_new) if len(tickets_new) > 1 else None
                                ref.mt5_ticket = tickets_new[0]
                                db.add(ref); db.commit()
                                log(f"[MarketEntry] #{ref.id} ripiazzato OK tickets={tickets_new}")
                            else:
                                log(f"[MarketEntry] #{ref.id} place_orders vuoto")
                        except Exception as _e:
                            log(f"[MarketEntry] #{ref.id} errore: {str(_e)[:120]}")
                        return
                    log(f"[MarketEntry] #{ref.id} pending ma msg ambiguo (no cmp/now, no 'at X'): IGNORE → '{text[:60]}'")
                    return

                # CASO C: trade gia' chiuso/cancellato → ignora
                log(f"[MarketEntry] #{ref.id} status={ref.status} (no pending, no open) → IGNORE msg '{text[:60]}'")
                return
            sig = _save_signal(db, parsed, msg_id)
            # ── FILTRI UTENTE (symbol exclusion + hour inclusion) ──
            # Se filtrato: il signal resta in DB con is_filtered=True per simulazione,
            # ma NON viene piazzato su MT5. Continua a ricevere sl_move/target_done/edit.
            if sig and not getattr(sig, "is_filtered", False):
                try:
                    from signal_filters import check_signal_filter
                    _filter_reason = check_signal_filter(sig.symbol, sig.created_at, db)
                    if _filter_reason:
                        sig.is_filtered = True
                        sig.filter_reason = _filter_reason
                        _append_trade_log(sig, "filtered", f"Signal filtrato (no MT5): {_filter_reason}")
                        db.add(sig); db.commit()
                        log(f"[Filter] #{sig.id} {sig.symbol} → is_filtered=True ({_filter_reason})")
                except Exception as _e:
                    log(f"[Filter] errore check signal #{getattr(sig,'id','?')}: {_e}")
            # Auto-trading: piazza ordine MT5 se abilitato (skip se filtrato)
            if sig and not getattr(sig, "is_filtered", False):
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
                                    # Distingui auto-cancellazione a monte (rr_suspect, typo guards,
                                    # ecc.) da rifiuto reale del broker. Se sig e' gia' stato marcato
                                    # cancelled da place_orders col suo tag specifico, NON sovrascrivere
                                    # con il messaggio "rifiutati dal broker" (mai inviati).
                                    db.refresh(sig)
                                    auto_cancelled = False
                                    try:
                                        import json as _jsonlib
                                        _tl = _jsonlib.loads(sig.trade_log) if sig.trade_log else []
                                        for ev in _tl:
                                            if ev.get("event") in ("rr_suspect", "mt5_skip", "mt5_no_lots"):
                                                auto_cancelled = True
                                                break
                                    except Exception:
                                        pass
                                    if auto_cancelled:
                                        log(f"[AutoTrade] #{sig.id} {sig.symbol} gia' auto-cancellato a monte (rr_suspect/skip) — nessun messaggio extra")
                                    else:
                                        sig.status = "cancelled"
                                        sig.notes = (sig.notes or "") + " [Tutti gli ordini MT5 rifiutati dal broker - vedi trade_log]"
                                        _append_trade_log(sig, "mt5_failed", "Nessun ticket MT5 ottenuto - tutti gli order_send rifiutati dal broker. Segnale annullato.")
                                        log(f"[AutoTrade] #{sig.id} {sig.symbol} ANNULLATO: tutti gli ordini rifiutati dal broker")
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

            # "Target Done" su un pending = il TG ha incassato ma il nostro
            # stop/limit entry non si e' mai attivato → trade perso, cancella.
            # Stesso razionale del SLMove drop: TG e' avanti al nostro fill.
            try:
                import re as _re, json as _json, mt5_trader
                # Detection target hit:
                # 1) Fast-path: LLM ha gia' classificato lo status (first/second/third/all_targets_done)
                # 2) Fallback regex robusto su testo normalizzato (rimuove markdown, emoji)
                semantic_hit_statuses = ("first_target_hit", "second_target_hit",
                                         "third_target_hit", "all_targets_done")
                is_target_hit = (parsed.status_text or "") in semantic_hit_statuses
                # Determina livello TP raggiunto (1/2/3, oppure 99=all)
                tp_level_hit = 0
                status_map = {"first_target_hit": 1, "second_target_hit": 2,
                              "third_target_hit": 3, "all_targets_done": 99}
                if is_target_hit:
                    tp_level_hit = status_map.get(parsed.status_text or "", 0)
                raw_combo = (parsed.status_text or "") + " " + (parsed.raw or "") + " " + (text or "")
                normalized = _re.sub(r'[^\w\s]+', ' ', raw_combo.lower())
                normalized = _re.sub(r'\s+', ' ', normalized)
                if not is_target_hit:
                    m = _re.search(r'\b(1st|first|2nd|second|3rd|third|all|last)\s+target\s+done\b', normalized)
                    if m:
                        is_target_hit = True
                        kw = m.group(1)
                        tp_level_hit = {"1st":1,"first":1,"2nd":2,"second":2,
                                        "3rd":3,"third":3,"all":99,"last":99}.get(kw, 1)
                # Fallback symbol extraction se LLM non l'ha messo: cerca #SYMBOL nel testo
                detected_symbol = parsed.symbol
                if is_target_hit and not detected_symbol and text:
                    sm = _re.search(r'#([A-Z]{3,10})', text.upper())
                    if sm:
                        detected_symbol = sm.group(1)
                # Detect istruzione esplicita di trail nel messaggio.
                # Doppio trigger:
                #  (a) LLM ha classificato il msg come status_text="trail_active"
                #      (cattura variazioni linguistiche del trader)
                #  (b) Fallback regex su keyword "trail"/"trailing" nel testo normalizzato
                # Senza questo, ignoriamo "target done" come pura notifica (regola
                # decisa dopo #405) e lasciamo SL invariato.
                trail_explicit_llm = (parsed.status_text == "trail_active")
                trail_explicit_regex = bool(_re.search(r'\b(trail|trailing)\b', normalized))
                trail_explicit = trail_explicit_llm or trail_explicit_regex
                if is_target_hit and detected_symbol and mt5_trader.is_enabled():
                    # Tutti i signal del simbolo non ancora chiusi (pending o open/tp1/tp2)
                    affected_sigs = db.query(Signal).filter(
                        Signal.status.in_(("pending", "open", "tp1", "tp2")),
                        Signal.mt5_tickets.isnot(None),
                        Signal.symbol == detected_symbol,
                    ).all()
                    mt5_inst = mt5_trader._get_mt5()
                    sigs_to_ema = []
                    for sig in affected_sigs:
                        tickets = _json.loads(sig.mt5_tickets) if sig.mt5_tickets else [sig.mt5_ticket]
                        cancelled_pending = 0
                        open_found = 0
                        sl_moved_to_be = 0
                        # Calcola SL trail target in base al TP raggiunto:
                        #   tp_level_hit=1 → SL = entry+1pip (BE)
                        #   tp_level_hit=2 → SL = TP1+1pip (locka profitto di TP1)
                        #   tp_level_hit=3 → SL = TP2+1pip (locka profitto di TP2)
                        # Stesso schema dell'auto-trail in sync_positions.
                        be_sl = None
                        trail_label = "BE+1pip"
                        if trail_explicit and mt5_inst:
                            try:
                                from mt5_trader import MT5_SYMBOL_MAP as _MAP
                                sym_info = mt5_inst.symbol_info(_MAP.get(sig.symbol.upper(), sig.symbol))
                                pip_size = sym_info.point * 10 if sym_info else 0
                                is_buy = (sig.direction or "").lower() == "buy"
                                anchor = None
                                if tp_level_hit >= 3 and sig.tp2:
                                    anchor = float(sig.tp2); trail_label = "TP2+1pip"
                                elif tp_level_hit >= 2 and sig.tp1:
                                    anchor = float(sig.tp1); trail_label = "TP1+1pip"
                                else:
                                    anchor = sig.actual_entry_price or sig.entry_price
                                    trail_label = "BE+1pip"
                                if anchor and pip_size > 0:
                                    be_sl = round(anchor + pip_size, 5) if is_buy else round(anchor - pip_size, 5)
                            except Exception:
                                be_sl = None
                        if mt5_inst:
                            for t in tickets:
                                orders = mt5_inst.orders_get(ticket=t)
                                if orders:
                                    # Pending order → cancella (entry mai filled).
                                    mt5_inst.order_send({"action": mt5_inst.TRADE_ACTION_REMOVE, "order": t})
                                    cancelled_pending += 1
                                    continue
                                positions = mt5_inst.positions_get(ticket=t)
                                if positions:
                                    open_found += 1
                                    # Posizione aperta:
                                    # - Se il msg e' SOLO "target done" (notifica): NESSUNA azione,
                                    #   lascia SL com'e' e lascia gestire i TP al broker (regola #405).
                                    # - Se il msg contiene ANCHE keyword di trail esplicito
                                    #   ("Safe Trail in Profits", "Trail in profit", ecc.),
                                    #   sposta SL a BE+1pip sui residui per proteggere il profitto
                                    #   (caso #424 GBPJPY: "First Target Done, Safe Trail in Profits").
                                    if trail_explicit and be_sl is not None:
                                        if mt5_trader.modify_sl(t, be_sl, sig.symbol):
                                            sl_moved_to_be += 1

                        # Marca cancelled SOLO se TUTTI i ticket erano pending non
                        # filled (nessuna posizione aperta). Se anche solo un ticket
                        # e' aperto, il trade e' vivo: non toccarlo.
                        if cancelled_pending and open_found == 0:
                            sig.status = "cancelled"
                            sig.updated_at = datetime.utcnow()
                            sig.closed_at = datetime.utcnow()
                            sig.notes = (sig.notes or "") + (
                                f" [Trade non partito: TG ha segnalato '{parsed.status_text or 'target_done'}' "
                                f"ma il LIMIT/STOP non si era mai attivato]"
                            )
                            _append_trade_log(sig, "pending_dropped",
                                f"Target raggiunto da TG su trade ancora pending: {cancelled_pending} pending cancellati.",
                                {"tickets": tickets, "cancelled": cancelled_pending, "trigger": "target_done"})
                            sigs_to_ema.append(sig)
                            db.add(sig)
                        elif sl_moved_to_be > 0 and be_sl is not None:
                            old_sl = sig.stoploss
                            sig.stoploss = be_sl
                            sig.updated_at = datetime.utcnow()
                            _append_trade_log(sig, "sl_move_trail_tg",
                                f"TG trail esplicito ('Safe Trail in Profits' o simile) su TP{tp_level_hit}: "
                                f"SL spostato a {be_sl} ({trail_label}) su {sl_moved_to_be} ticket "
                                f"(DB stoploss {old_sl} → {be_sl})",
                                {"new_sl": be_sl, "old_sl": old_sl, "tickets_modified": sl_moved_to_be,
                                 "tp_level_hit": tp_level_hit, "trail_label": trail_label, "trigger": "trail_explicit"})
                            db.add(sig)
                        log(f"[TargetDone] #{sig.id} {sig.symbol}: open={open_found} cancel_pend={cancelled_pending} sl→BE={sl_moved_to_be} trail_explicit={trail_explicit} (tp_lvl={tp_level_hit})")
                    if affected_sigs:
                        db.commit()
                        for sig in sigs_to_ema:
                            try:
                                mt5_trader.analyze_ema_case(sig.id, "target_done")
                            except Exception as _e:
                                log(f"[EMA] errore analisi sig #{sig.id}: {str(_e)[:80]}")
            except Exception as e:
                log(f"[TargetDone] Errore drop pending: {str(e)[:120]}")

            # Handler "trail standalone": messaggi tipo "Everyone Hold Book Or
            # Trail Accordingly" / "Trail in profits" che arrivano SENZA un
            # esplicito "Nth Target Done". Il trader sta dicendo di proteggere
            # il profitto. Trail target scalato sul livello TP gia' raggiunto
            # (status sig): tp1 -> TP1+1pip, tp2 -> TP2+1pip, altrimenti BE+1pip.
            # Caso #428 (08/06/2026): TP1 broker gia' colpito, msg standalone
            # "Hold Book Or Trail Accordingly" -> il bot lo ignorava perche'
            # cercava trail keyword solo dentro target_done. L'utente ha
            # dovuto fare lock_profit manuale.
            if not is_target_hit and trail_explicit and mt5_trader.is_enabled():
                try:
                    # Risoluzione simbolo per trail standalone:
                    # 1) parsed.symbol o detected_symbol via regex #SYMBOL
                    # 2) reply_to_msg_id → signal originale del trader
                    # 3) ultimo signal aperto del simbolo piu' attivo (fallback)
                    target_symbol = detected_symbol
                    if not target_symbol and reply_to_msg_id:
                        ref_sig = db.query(Signal).filter(Signal.telegram_msg_id == reply_to_msg_id).first()
                        if ref_sig:
                            target_symbol = ref_sig.symbol
                            log(f"[TrailStandalone] simbolo {target_symbol} risolto via reply a msg={reply_to_msg_id} (sig #{ref_sig.id})")
                    if not target_symbol:
                        # Fallback: signal aperto piu' recente
                        recent_open = db.query(Signal).filter(
                            Signal.status.in_(("open", "tp1", "tp2")),
                            Signal.mt5_tickets.isnot(None),
                            Signal.closed_at.is_(None),
                        ).order_by(Signal.created_at.desc()).first()
                        if recent_open:
                            target_symbol = recent_open.symbol
                            log(f"[TrailStandalone] simbolo non risolto, fallback a ultimo aperto: {target_symbol} (sig #{recent_open.id})")
                    if not target_symbol:
                        log(f"[TrailStandalone] nessun simbolo risolvibile, skip")
                        raise StopIteration
                    affected_sigs2 = db.query(Signal).filter(
                        Signal.status.in_(("open", "tp1", "tp2")),
                        Signal.mt5_tickets.isnot(None),
                        Signal.symbol == target_symbol,
                        Signal.closed_at.is_(None),
                    ).all()
                    mt5_inst2 = mt5_trader._get_mt5()
                    for sig in affected_sigs2:
                        # CASO A: signal ancora "open" (TP1 broker NON hit) →
                        # NON muovere SL ora, ARMA invece trail_stop_enabled.
                        # Cosi' quando il broker tocca davvero TP1, l'auto-trail
                        # in sync_positions sposta SL a BE+1pip al momento giusto.
                        # Caso #432 (09/06): "Near First Target, Safe Trail in Profits"
                        # arrivato col prezzo a 214.150, TP1 broker 214.200 non ancora hit.
                        if sig.status == "open":
                            if not sig.trail_stop_enabled:
                                sig.trail_stop_enabled = True
                                sig.updated_at = datetime.utcnow()
                                _append_trade_log(sig, "trail_armed_pre_tp",
                                    f"TG trail standalone con TP1 broker NON ancora hit ('{(text or '')[:80]}'): "
                                    f"armato trail_stop_enabled=True. SL invariato, scattera' alla chiusura TP1 reale via sync_positions.",
                                    {"status": sig.status, "trigger": "trail_standalone_pre_tp"})
                                db.add(sig)
                                log(f"[TrailStandalone] #{sig.id} {sig.symbol} status=open → armato trail_stop_enabled (TP1 broker non hit)")
                            else:
                                log(f"[TrailStandalone] #{sig.id} {sig.symbol} trail gia' armato, skip")
                            continue
                        # CASO B: signal gia' tp1/tp2 → sposta SL trail subito.
                        from mt5_trader import MT5_SYMBOL_MAP as _MAP2
                        sym_info2 = mt5_inst2.symbol_info(_MAP2.get(sig.symbol.upper(), sig.symbol)) if mt5_inst2 else None
                        pip2 = (sym_info2.point * 10) if sym_info2 else 0
                        if pip2 <= 0:
                            continue
                        is_buy2 = (sig.direction or "").lower() == "buy"
                        # Trail = lock del TP PRECEDENTE (passo indietro).
                        # status=tp2 (TP2 hit) → SL = TP1+1pip (locka TP1)
                        # status=tp1 (TP1 hit) → SL = entry+1pip (BE+1pip)
                        if sig.status == "tp2" and sig.tp1:
                            anchor2 = float(sig.tp1); label2 = "TP1+1pip"
                        else:
                            anchor2 = sig.actual_entry_price or sig.entry_price
                            label2 = "BE+1pip"
                        if not anchor2:
                            continue
                        target_sl2 = round(anchor2 + pip2, 5) if is_buy2 else round(anchor2 - pip2, 5)
                        # Per BUY non degradare: solo se nuovo SL > corrente
                        cur_sl2 = sig.stoploss
                        if cur_sl2 is not None:
                            if is_buy2 and target_sl2 <= cur_sl2: continue
                            if not is_buy2 and target_sl2 >= cur_sl2: continue
                        tickets2 = _json.loads(sig.mt5_tickets) if sig.mt5_tickets else [sig.mt5_ticket]
                        moved2 = 0
                        for t in tickets2:
                            if mt5_inst2 and mt5_inst2.positions_get(ticket=t):
                                if mt5_trader.modify_sl(t, target_sl2, sig.symbol):
                                    moved2 += 1
                        if moved2 > 0:
                            old2 = sig.stoploss
                            sig.stoploss = target_sl2
                            sig.updated_at = datetime.utcnow()
                            _append_trade_log(sig, "sl_move_trail_standalone",
                                f"TG trail standalone ('{(text or '')[:80]}'): SL spostato a {target_sl2} ({label2}) su {moved2} ticket "
                                f"(DB stoploss {old2} → {target_sl2})",
                                {"new_sl": target_sl2, "old_sl": old2, "tickets_modified": moved2,
                                 "trail_label": label2, "trigger": "trail_standalone", "status_at_trail": sig.status})
                            db.add(sig)
                            log(f"[TrailStandalone] #{sig.id} {sig.symbol} SL→{target_sl2} ({label2}) su {moved2} ticket")
                    if affected_sigs2:
                        db.commit()
                except Exception as _e:
                    log(f"[TrailStandalone] errore: {str(_e)[:120]}")

            await broadcast_ws({
                "event": "trade_update",
                "data": {
                    "symbol": parsed.symbol,
                    "price_from": parsed.price_from,
                    "price_to": parsed.price_to,
                    "status_text": parsed.status_text,
                },
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
                    # Identifica i pending in attesa che il messaggio coinvolge:
                    # uno SLMove (incluso BE) su un signal ancora pending = il
                    # nostro LIMIT/STOP non si e' mai filled mentre il TG ha
                    # gia' "incassato" virtualmente -> trade perso, cancellalo.
                    pending_sigs_to_drop = []
                    if reply_to_msg_id:
                        cand = db.query(Signal).filter(
                            Signal.telegram_msg_id == reply_to_msg_id,
                            Signal.status == "pending",
                            Signal.mt5_ticket.isnot(None),
                        ).first()
                        if cand:
                            pending_sigs_to_drop.append(cand)
                    if not pending_sigs_to_drop and parsed.symbol:
                        # Fallback per simbolo solo se ho il simbolo nel messaggio
                        pending_sigs_to_drop = db.query(Signal).filter(
                            Signal.status == "pending",
                            Signal.mt5_ticket.isnot(None),
                            Signal.symbol == parsed.symbol,
                        ).all()
                    for sig in pending_sigs_to_drop:
                        tickets = _json.loads(sig.mt5_tickets) if sig.mt5_tickets else [sig.mt5_ticket]
                        cancelled_count = 0
                        mt5_inst = mt5_trader._get_mt5()
                        if mt5_inst:
                            for t in tickets:
                                orders = mt5_inst.orders_get(ticket=t)
                                if orders:
                                    mt5_inst.order_send({"action": mt5_inst.TRADE_ACTION_REMOVE, "order": t})
                                    cancelled_count += 1
                        sig.status = "cancelled"
                        sig.notes = (sig.notes or "") + (
                            f" [Trade non partito: prezzo fuori dal range alla ricezione del segnale "
                            f"(LIMIT/STOP mai filled). TG ha segnalato BE/TP -> trade perso]"
                        )
                        sig.updated_at = datetime.utcnow()
                        sig.closed_at = datetime.utcnow()
                        _append_trade_log(sig, "pending_dropped",
                            f"SLMove ricevuto su trade ancora pending: TG ha continuato come se fosse "
                            f"entrato, ma il LIMIT/STOP non si e' mai filled. {cancelled_count} pending order "
                            f"cancellati, segnale annullato.",
                            {"tickets": tickets, "cancelled": cancelled_count})
                        db.add(sig)
                        log(f"[SLMove] #{sig.id} {sig.symbol} pending dropped: {cancelled_count} ordini cancellati")
                    if pending_sigs_to_drop:
                        db.commit()
                        # EMA: registra casi di STOP mai filled
                        for sig in pending_sigs_to_drop:
                            try:
                                mt5_trader.analyze_ema_case(sig.id, "sl_move_drop")
                            except Exception as _e:
                                log(f"[EMA] errore analisi sig #{sig.id}: {str(_e)[:80]}")

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
                            log(f"[SLMove] reply a #{target_sig.id} {target_sig.symbol} status={target_sig.status} (gia' chiuso/cancellato) -> skip")
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
                        # Post-TP1: ignora SL Move TG SOLO se trail auto e' attivo.
                        # Senza trail auto attivo, il msg TG e' l'unica protezione →
                        # va applicato. Caso #355 (con auto-trail ON): TG SL Move
                        # sovrapposto causava sl_immediate_close — quindi ignora.
                        # Caso #464 (auto-trail OFF): bot ignorava TG → SL originale
                        # colpito per -71$. Distinguere via trail_stop_enabled.
                        if sig.status in ("tp1", "tp2"):
                            # Determina se il trail automatico e' attivo per questo sig
                            _trail_active = False
                            if sig.trail_stop_enabled is True:
                                _trail_active = True
                            elif sig.trail_stop_enabled is None:
                                try:
                                    from risk import get_risk_settings as _grs
                                    _rs = _grs()
                                    _trail_active = bool(_rs.get("trail_stop_enabled", False))
                                except Exception:
                                    _trail_active = False
                            if _trail_active:
                                _append_trade_log(sig, "sl_move_ignored",
                                    f"SL Move da TG ignorato: TP1 gia' colpito (status={sig.status}), "
                                    f"trail auto attivo gestisce SL autonomamente.",
                                    {"new_sl_proposed": parsed.new_sl, "is_breakeven": parsed.is_breakeven,
                                     "current_status": sig.status, "trail_auto": True})
                                log(f"[SLMove] #{sig.id} {sig.symbol} IGNORATO: TP1 colpito + trail auto ON")
                                db.add(sig)
                                continue
                            # else: trail auto OFF → applica SL Move TG come unica protezione
                            log(f"[SLMove] #{sig.id} {sig.symbol} post-TP1 ma trail auto OFF → APPLICO SL Move TG")
                        new_sl = parsed.new_sl
                        if parsed.is_breakeven and sig.actual_entry_price:
                            new_sl = sig.actual_entry_price
                        if new_sl:
                            # ─── Validazione typo SL Move ancorata al prezzo ───
                            # Regole:
                            #  - BUY: SL deve stare SOTTO il prezzo corrente. Inoltre uno
                            #    SL Move sensato tightens (sale, verso BE); allontanarsi
                            #    sotto il vecchio SL e' tipicamente un typo.
                            #  - SELL: simmetrico.
                            #  - Se il valore proposto e' oltre il 2% dal prezzo o viola
                            #    la direzione (allontanamento), tenta single-digit fix.
                            try:
                                cur_price = mt5_trader.get_current_price(sig.symbol)
                                is_buy = (sig.direction or "").lower() == "buy"
                                old_sl = sig.stoploss
                                if cur_price:
                                    if is_buy:
                                        min_a = None
                                        max_a = cur_price * 0.9999           # SL BUY deve stare sotto il prezzo
                                    else:
                                        min_a = cur_price * 1.0001           # SL SELL deve stare sopra il prezzo
                                        max_a = None
                                    # digits dal simbolo
                                    try:
                                        sym_info_v = mt5_trader._get_mt5().symbol_info(
                                            mt5_trader.MT5_SYMBOL_MAP.get(sig.symbol.upper(), sig.symbol))
                                        digits_v = sym_info_v.digits if sym_info_v else 2
                                    except Exception:
                                        digits_v = 2
                                    fixed_sl, was_fixed, reason = mt5_trader.fix_price_typo(
                                        new_sl, cur_price, digits=digits_v,
                                        min_allowed=min_a, max_allowed=max_a, anchor_tol_pct=0.02)
                                    if was_fixed:
                                        _append_trade_log(sig, "sl_move_typo_fixed",
                                            f"SL Move TG ({new_sl}) incoerente col prezzo {cur_price:.{digits_v}f} "
                                            f"e/o col vecchio SL {old_sl}: auto-corretto a {fixed_sl}.",
                                            {"original": new_sl, "fixed": fixed_sl, "price": cur_price,
                                             "old_sl": old_sl, "reason": reason})
                                        log(f"[SLMove] #{sig.id} {sig.symbol} typo: {new_sl} -> {fixed_sl} ({reason})")
                                        new_sl = fixed_sl
                                    elif reason == "no_fix":
                                        _append_trade_log(sig, "sl_move_rejected",
                                            f"SL Move TG ({new_sl}) ignorato: incoerente col prezzo {cur_price:.{digits_v}f} "
                                            f"e/o col vecchio SL {old_sl}, nessuna correzione single-digit plausibile.",
                                            {"proposed": new_sl, "price": cur_price, "old_sl": old_sl,
                                             "min_allowed": min_a, "max_allowed": max_a})
                                        log(f"[SLMove] #{sig.id} {sig.symbol} RIFIUTATO {new_sl} (no fix plausibile)")
                                        db.add(sig)
                                        continue
                            except Exception as _e:
                                log(f"[SLMove] #{sig.id} validazione typo errore: {str(_e)[:80]}")
                            tickets = _json.loads(sig.mt5_tickets) if sig.mt5_tickets else [sig.mt5_ticket]
                            failed_invalid_sl = False
                            for ticket in tickets:
                                ok = mt5_trader.modify_sl(ticket, new_sl, sig.symbol)
                                if not ok:
                                    err = mt5_trader._last_modify_error
                                    # err = (retcode, comment) da result.retcode/comment.
                                    # 10016 = TRADE_RETCODE_INVALID_STOPS (SL/TP troppo
                                    # vicini al prezzo o lato sbagliato). Standard MT5,
                                    # indipendente da broker e da testo del comment.
                                    if err and isinstance(err, tuple) and len(err) >= 1:
                                        if err[0] == 10016:
                                            failed_invalid_sl = True
                            label = "BE" if parsed.is_breakeven else f"SL->{new_sl}"
                            if failed_invalid_sl:
                                # Modify rifiutato perche' SL troppo vicino al prezzo:
                                # significa che il prezzo ha gia' superato/raggiunto il
                                # livello voluto dal trader. Chiusura IMMEDIATA dei ticket
                                # a mercato (invece di attesa con coda pending che poteva
                                # fallire come in #330).
                                closed_count = 0
                                fail_tickets = []
                                for ticket in tickets:
                                    if mt5_trader.close_position(ticket, sig.symbol):
                                        closed_count += 1
                                    else:
                                        fail_tickets.append(ticket)
                                if fail_tickets:
                                    # Fallback: se anche close_position fallisce per qualcuno,
                                    # tienili in pending queue per riprovare
                                    mt5_trader.register_pending_sl(sig.id, new_sl, fail_tickets, sig.symbol, sig.direction)
                                _append_trade_log(sig, "sl_immediate_close",
                                    f"SL Move da TG ({label}) RIFIUTATO da MT5 (SL troppo vicino): "
                                    f"chiusura immediata a mercato di {closed_count}/{len(tickets)} ticket"
                                    + (f" ({len(fail_tickets)} fallback in pending queue)" if fail_tickets else ""),
                                    {"new_sl": new_sl, "closed": closed_count, "failed": fail_tickets})
                                # Aggiorna nota visibile nello storico
                                import re as _re
                                if sig.notes:
                                    sig.notes = _re.sub(r'\s*\[SL pending[^\]]*\]', '', sig.notes).strip() or None
                                sig.notes = (sig.notes or "") + f" [SL Move {new_sl} rifiutato → chiusura immediata: {closed_count}/{len(tickets)}]"
                                log(f"[SLMove] #{sig.id} {sig.symbol} SL->{new_sl} RIFIUTATO -> chiusura immediata {closed_count}/{len(tickets)}")
                            else:
                                old_db_sl = sig.stoploss
                                sig.stoploss = new_sl  # allinea DB col SL reale sul broker
                                _append_trade_log(sig, "sl_move",
                                    f"SL Move da TG ({label}) applicato su {len(tickets)} ticket (DB stoploss aggiornato: {old_db_sl} → {new_sl})",
                                    {"new_sl": new_sl, "is_breakeven": parsed.is_breakeven, "tickets": tickets, "old_db_sl": old_db_sl})
                                # Se c'era una pending precedente, viene sostituita: pulisci
                                mt5_trader.clear_pending_sl(sig.id)
                                log(f"[SLMove] #{sig.id} {sig.symbol} SL->{new_sl} su {len(tickets)} ticket (DB aggiornato)")
                            db.add(sig)
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

                # Guard anti-doppio-trade: se il signal di riferimento e' ancora
                # vivo sul broker (positions aperte o ordini pendenti), NON clonare.
                # Il msg di reenter in questo caso e' un'istruzione tardiva per chi
                # non era ancora entrato, non un vero rientro post-chiusura.
                # Caso #444 (11/06/2026): msg "Everyone Enter Now with 4084 SL"
                # arrivato 2 min dopo #443 ancora aperto -> il handler clonava e
                # apriva un secondo BUY identico, raddoppiando l'esposizione.
                if last_sig:
                    try:
                        import json as _json_re
                        _tickets_ref = []
                        if last_sig.mt5_tickets:
                            try: _tickets_ref = _json_re.loads(last_sig.mt5_tickets)
                            except Exception: _tickets_ref = []
                        elif last_sig.mt5_ticket:
                            _tickets_ref = [last_sig.mt5_ticket]
                        _mt5_inst = mt5_trader._get_mt5() if mt5_trader.is_enabled() else None
                        _has_active = False
                        if _mt5_inst and _tickets_ref:
                            for _t in _tickets_ref:
                                if _mt5_inst.positions_get(ticket=_t) or _mt5_inst.orders_get(ticket=_t):
                                    _has_active = True
                                    break
                        if _has_active:
                            log(f"[Reenter] msg={msg_id} #{last_sig.id} {last_sig.symbol} ha ticket ATTIVI sul broker → siamo gia' dentro, SKIP (no doppio trade)")
                            last_sig = None
                    except Exception as _e:
                        log(f"[Reenter] check broker errore: {str(_e)[:80]}")
                if last_sig:
                    # Override entry range se il msg di reenter contiene numeri.
                    # Pattern supportati nel raw del msg:
                    #   "Near 78-79"        → range 78-79 (formato 2-digit, va espanso col prefisso del clone)
                    #   "Near 4678-4679"    → range completo
                    #   "Near 4678-79"      → 4678 e 79 espanso a 4679
                    # I numeri vengono interpretati nel CONTESTO del livello del clone
                    # (es. clone era 4682-4684 → un "78-79" del reenter diventa 4678-4679).
                    import re as _re_reenter
                    override_low = override_high = None
                    raw_reenter = (parsed.raw or "")
                    # Range: "X-Y" oppure "X - Y" oppure "X to Y"
                    m_rng = _re_reenter.search(r'(?:near|@|at)?\s*([0-9]+(?:\.[0-9]+)?)\s*[-–toTO]+\s*([0-9]+(?:\.[0-9]+)?)', raw_reenter, _re_reenter.IGNORECASE)
                    if m_rng:
                        try:
                            n1, n2 = m_rng.group(1), m_rng.group(2)
                            v1 = float(n1)
                            v2 = float(n2)
                            # Espansione del secondo numero col prefisso del clone se troncato
                            # (es. clone 4682, msg "78-79" → 4678 e 4679).
                            ref = float(last_sig.entry_price or last_sig.entry_price_high or 0)
                            if ref > 0:
                                # Se i numeri sono molto piu' piccoli del ref e l'ordine di
                                # grandezza differisce, ricostruisci col prefisso del ref.
                                from parser import _expand_range
                                ref_str = str(int(ref))
                                if v1 < ref / 10:
                                    # Numero troncato: ricostruisci col prefisso
                                    # 4682 → prefisso "46", "78" → "4678"
                                    v1_str = str(int(v1))
                                    if len(v1_str) < len(ref_str):
                                        v1 = float(ref_str[:len(ref_str) - len(v1_str)] + v1_str)
                                # Espandi v2 vs v1 (es. "4678-79" → 4678, 4679)
                                v2_str = str(int(v2)) if v2 == int(v2) else str(v2)
                                v1_str_now = str(int(v1)) if v1 == int(v1) else str(v1)
                                if len(v2_str) < len(v1_str_now):
                                    v2 = float(v1_str_now[:len(v1_str_now) - len(v2_str)] + v2_str)
                            override_low, override_high = min(v1, v2), max(v1, v2)
                        except Exception:
                            override_low = override_high = None

                    # Clona come nuovo segnale (con eventuali override)
                    new_entry_low = override_low if override_low is not None else last_sig.entry_price
                    new_entry_high = override_high if override_high is not None else last_sig.entry_price_high
                    new_sig = Signal(
                        symbol=last_sig.symbol,
                        direction=last_sig.direction,
                        entry_price=new_entry_low,
                        entry_price_high=new_entry_high,
                        tp1=last_sig.tp1, tp2=last_sig.tp2, tp3=last_sig.tp3,
                        stoploss=last_sig.stoploss,
                        is_risky=last_sig.is_risky,
                        raw_message=f"[REENTER] {parsed.raw}",
                        telegram_msg_id=msg_id,
                    )
                    db.add(new_sig)
                    db.flush()  # get new_sig.id

                    override_note = ""
                    if override_low is not None:
                        override_note = f" entry override da msg: {override_low}-{override_high} (clone era {last_sig.entry_price}-{last_sig.entry_price_high})"
                    _append_trade_log(new_sig, "reenter", f"Rientro nel trade #{last_sig.id} {last_sig.symbol} {last_sig.direction}.{override_note}")
                    log(f"[Reenter] Nuovo segnale #{new_sig.id} clonato da #{last_sig.id} {last_sig.symbol}.{override_note}")

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

        elif msg_type == "enter_now" and parsed:
            # Istruzione "Entra ORA sul signal ancora attivo" (NON e' un reenter).
            # Routing: trova il signal piu' recente del simbolo e controlla broker:
            #  - gia' fillato → SKIP (siamo dentro)
            #  - pending non filled → cancella pending + MARKET (eventualmente
            #    aggiornando SL se il msg lo specifica)
            #  - chiuso/cancellato → SKIP (no signal vivo, NON e' un reenter)
            try:
                import mt5_trader as _mt5t
                import json as _jl_en
                # Trova signal ref
                ref = None
                if parsed.symbol:
                    ref = db.query(Signal).filter(Signal.symbol == parsed.symbol).order_by(Signal.created_at.desc()).first()
                if not ref:
                    log(f"[EnterNow] msg={msg_id} nessun signal di riferimento per {parsed.symbol} → ignore")
                else:
                    tickets_ref = []
                    if ref.mt5_tickets:
                        try: tickets_ref = _jl_en.loads(ref.mt5_tickets)
                        except Exception: tickets_ref = []
                    elif ref.mt5_ticket:
                        tickets_ref = [ref.mt5_ticket]
                    mt5_inst = _mt5t._get_mt5() if _mt5t.is_enabled() else None
                    has_open = False; has_pending = False
                    if mt5_inst and tickets_ref:
                        for t in tickets_ref:
                            if mt5_inst.positions_get(ticket=t): has_open = True
                            elif mt5_inst.orders_get(ticket=t): has_pending = True
                    if has_open:
                        log(f"[EnterNow] #{ref.id} {ref.symbol} {ref.direction} gia' fillato → SKIP (siamo dentro)")
                    elif has_pending and ref.status == "pending":
                        log(f"[EnterNow] #{ref.id} {ref.symbol}: pending non filled → cancello e ripiazzo MARKET")
                        for t in tickets_ref:
                            if mt5_inst.orders_get(ticket=t):
                                mt5_inst.order_send({"action": mt5_inst.TRADE_ACTION_REMOVE, "order": t})
                        ref.mt5_ticket = None
                        ref.mt5_tickets = None
                        ref.entry_type = "market"
                        # Se il msg specifica un nuovo SL ("with 4084 SL"), aggiornalo
                        old_sl = ref.stoploss
                        if parsed.sl is not None:
                            ref.stoploss = parsed.sl
                        _append_trade_log(ref, "enter_now_market",
                            f"TG enter_now '{(parsed.raw or '')[:80]}': pending cancellati, MARKET entry"
                            + (f", SL aggiornato {old_sl} → {parsed.sl}" if parsed.sl is not None and parsed.sl != old_sl else ""),
                            {"trigger_msg_id": msg_id, "old_sl": old_sl, "new_sl": parsed.sl})
                        db.add(ref); db.commit(); db.refresh(ref)
                        try:
                            tickets_new = _mt5t.place_orders(ref, catch_origin="realtime",
                                catch_reason="trader: enter now su signal pending non filled",
                                signal_ts=ref.created_at)
                            if tickets_new:
                                ref.mt5_tickets = _jl_en.dumps(tickets_new) if len(tickets_new) > 1 else None
                                ref.mt5_ticket = tickets_new[0]
                                db.add(ref); db.commit()
                                log(f"[EnterNow] #{ref.id} ripiazzato OK tickets={tickets_new}")
                        except Exception as _e:
                            log(f"[EnterNow] #{ref.id} errore: {str(_e)[:120]}")
                    else:
                        log(f"[EnterNow] #{ref.id} status={ref.status}, no positions/pending broker → SKIP (no trade vivo, NON e' un reenter)")
            except Exception as e:
                log(f"[EnterNow] Errore: {str(e)[:100]}")

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
                        _append_trade_log(sig, "received",
                            f"Replay: signal recuperato (msg_date={ts}). {parsed.symbol} {parsed.direction} entry={parsed.entry_price}-{parsed.entry_price_high} sl={parsed.stoploss} tp1={parsed.tp1}",
                            {"msg_id": msg_id, "via": "history_replay"})
                        db.add(sig)
                        db.commit()
                        db.refresh(sig)
                        # Se il signal e' "recente" (<30 min) e MT5 abilitato, tenta
                        # comunque il place_orders con catch_origin='replay'. Il
                        # pre-check tick di late-catch decidera' MARKET/LIMIT/CANCEL.
                        # Senza questo, signal arrivati durante un restart breve del
                        # backend venivano persi (caso #425: backend killato per deploy
                        # del fix #424 proprio nel minuto del signal arrivo).
                        try:
                            import mt5_trader as _mt5t
                            delay_sec = (datetime.utcnow() - ts).total_seconds()
                            if delay_sec < 1800 and _mt5t.is_enabled():
                                log(f"[Replay] #{sig.id} signal recente (delay {int(delay_sec)}s): tentativo place_orders con catch_origin=replay")
                                tickets = _mt5t.place_orders(sig, catch_origin="replay",
                                    catch_reason=f"recuperato da history replay, delay {int(delay_sec)}s",
                                    signal_ts=ts)
                                if tickets:
                                    import json as _jl
                                    db.refresh(sig)
                                    sig.mt5_tickets = _jl.dumps(tickets) if len(tickets) > 1 else None
                                    sig.mt5_ticket = tickets[0]
                                    db.add(sig)
                                    db.commit()
                                    log(f"[Replay] #{sig.id} piazzato OK tickets={tickets}")
                                else:
                                    log(f"[Replay] #{sig.id} place_orders ha ritornato vuoto (pre-check / broker reject)")
                        except Exception as _e:
                            log(f"[Replay] #{sig.id} errore place_orders: {str(_e)[:120]}")

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
        """Quando il trader cancella un msg TG entro 60 min:
        - se signal pending non filled → cancella pending broker + status=cancelled
        - se signal aperto sul broker (positions) → chiude a market + status=cancelled
        Caso #446 (11/06/2026): trader cancella signal con typo, listener aveva
        gia' aperto il trade. Senza questa logica il trade resta vivo fino al SL."""
        from datetime import datetime as _dt, timedelta
        import json as _jl_del
        deleted_ids = event.deleted_ids or []
        if not deleted_ids:
            return
        db = SessionLocal()
        try:
            import mt5_trader as _mt5t
            cutoff = _dt.utcnow() - timedelta(minutes=60)
            for tg_id in deleted_ids:
                sig = db.query(Signal).filter(
                    Signal.telegram_msg_id == tg_id,
                    Signal.created_at >= cutoff,
                    Signal.status.in_(("pending", "open", "tp1", "tp2")),
                ).first()
                if not sig:
                    continue
                tickets = []
                if sig.mt5_tickets:
                    try: tickets = _jl_del.loads(sig.mt5_tickets)
                    except Exception: tickets = []
                elif sig.mt5_ticket:
                    tickets = [sig.mt5_ticket]
                mt5_inst = _mt5t._get_mt5() if _mt5t.is_enabled() else None
                cancelled_pend = 0
                closed_pos = 0
                if mt5_inst and tickets:
                    for t in tickets:
                        if mt5_inst.orders_get(ticket=t):
                            mt5_inst.order_send({"action": mt5_inst.TRADE_ACTION_REMOVE, "order": t})
                            cancelled_pend += 1
                        elif mt5_inst.positions_get(ticket=t):
                            if _mt5t.close_position(t, sig.symbol):
                                closed_pos += 1
                sig.status = "cancelled"
                sig.closed_at = _dt.utcnow()
                sig.updated_at = _dt.utcnow()
                sig.notes = (sig.notes or "") + (
                    f" [Msg TG cancellato dal trader: chiusi {closed_pos} ticket aperti, "
                    f"cancellati {cancelled_pend} pending]"
                )
                _append_trade_log(sig, "tg_msg_deleted",
                    f"Trader ha cancellato il msg TG. Chiusura: positions={closed_pos}, pendings={cancelled_pend}.",
                    {"tickets": tickets, "closed_positions": closed_pos, "cancelled_pendings": cancelled_pend})
                db.add(sig)
                log(f"[Deleted] #{sig.id} {sig.symbol} cancellato: chiusi {closed_pos} pos / {cancelled_pend} pending broker")
            db.commit()
        except Exception as e:
            log(f"[Deleted] Errore: {str(e)[:120]}")
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
