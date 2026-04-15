"""
Parser LLM per messaggi Telegram del canale Inner Circle Trader.
Usa Claude Haiku per interpretare il linguaggio naturale in modo flessibile.
Fallback al parser regex se l'API non è disponibile.
"""
import json
import os
import re
from typing import Optional

_client = None


def _get_client():
    global _client
    if _client is None:
        import anthropic
        api_key = os.getenv("ANTHROPIC_API_KEY")
        if not api_key:
            return None
        _client = anthropic.Anthropic(api_key=api_key)
    return _client


SYSTEM_PROMPT = """Sei un assistente che analizza messaggi di un canale Telegram di trading forex/crypto.
Il tuo compito è estrarre l'intenzione e i dati strutturati da ogni messaggio.

Rispondi SEMPRE con un JSON valido e nient'altro. Schema:
{
  "type": "signal" | "close" | "sl_move" | "update" | "risky_flag" | "ignore",
  "symbol": "XAUUSD" | "BTCUSD" | "USDCAD" | null,
  "direction": "buy" | "sell" | null,
  "entry_low": numero | null,
  "entry_high": numero | null,
  "tp1": numero | null,
  "tp2": numero | null,
  "tp3": numero | null,
  "sl": numero | null,
  "is_risky": true | false,
  "new_sl": numero | null,
  "is_breakeven": true | false,
  "close_reason": "stringa motivo" | null,
  "price_from": numero | null,
  "price_to": numero | null
}

Regole di classificazione:
- "signal": nuovo segnale di trading con entry, TP, SL
- "close": chiudi il trade IMMEDIATAMENTE. Solo per ordini espliciti di chiusura:
  "close trade", "closing the trade", "everyone close", "closing here | BTC changing direction"
  NON classificare come close:
  - "take exit near XXXX" / "exit near XXXX" dopo un "First Target Done" = sl_move a breakeven
    (il prezzo indicato è vicino all'entry, quindi è un trailing SL a breakeven)
  - "book profit" = update informativo
- "sl_move": qualsiasi indicazione di spostare/aggiornare lo stoploss. Esempi:
  "move SL to 4550", "trail SL to 4550", "Everyone Hold With 4561 SL",
  "hold with SL at 4561", "keep SL at 4550", "SL now 4550", "new SL 4550",
  "move to cost", "move SL to breakeven", "trail to entry"
  → new_sl = il numero dello stoploss, is_breakeven = true se dice cost/breakeven/entry
- "update": aggiornamento prezzo (es. "XAUUSD | 4550 To 4560", "First Target Done")
- "reenter": rientrare nel trade appena chiuso. Esempi: "enter again", "everyone enter again now",
  "re-enter", "open again". Il trader dice di riaprire la stessa posizione.
- "risky_flag": messaggio che indica rischio elevato ("highly risky", "risky trade", "aggressive")
- "ignore": watchlist, livelli giornalieri, commenti generali senza azione

Per segnali con range entry come "Buy Near 4550-52" o "Buy Near-4567-69":
- entry_low = primo numero (4550 o 4567)
- entry_high = secondo numero espanso (4552 o 4569, stesso prefisso del primo)

Per simboli: #XAUUSD → "XAUUSD". Se non esplicito ma deducibile dal contesto → inferiscilo.

Esempi di sl_move:
- "Everyone Hold With 4561 SL" → type=sl_move, new_sl=4561
- "Move SL to breakeven" → type=sl_move, is_breakeven=true
- "Trail SL to 4992" → type=sl_move, new_sl=4992
- "Hold with 4546 stop" → type=sl_move, new_sl=4546
"""


def _build_context() -> str:
    """Costruisce il contesto dei trade aperti e messaggi recenti per il LLM."""
    try:
        from database import SessionLocal, Signal, RawMessage
        from datetime import datetime, timedelta
        db = SessionLocal()
        try:
            # Trade aperti/recenti
            active = db.query(Signal).filter(
                Signal.status.in_(["pending", "open", "tp1", "tp2"]),
                Signal.is_archived == False,
            ).order_by(Signal.created_at.desc()).limit(5).all()

            # Ultimi trade chiusi (per contesto reenter)
            recent_closed = db.query(Signal).filter(
                Signal.status.in_(["sl_hit", "tp1", "tp2", "tp3", "closed"]),
                Signal.created_at >= datetime.utcnow() - timedelta(hours=12),
            ).order_by(Signal.created_at.desc()).limit(3).all()

            # Ultimi messaggi (per contesto conversazione)
            recent_msgs = db.query(RawMessage).filter(
                RawMessage.created_at >= datetime.utcnow() - timedelta(minutes=30),
            ).order_by(RawMessage.created_at.desc()).limit(5).all()

            parts = []
            if active:
                parts.append("TRADE ATTIVI:")
                for s in active:
                    parts.append(f"  #{s.id} {s.symbol} {s.direction} entry={s.entry_price}-{s.entry_price_high} sl={s.stoploss} status={s.status}")
            if recent_closed:
                parts.append("TRADE RECENTI CHIUSI:")
                for s in recent_closed:
                    parts.append(f"  #{s.id} {s.symbol} {s.direction} status={s.status} pnl={s.pnl_usd}")
            if recent_msgs:
                parts.append("MESSAGGI RECENTI (dal piu recente):")
                for m in recent_msgs:
                    clean = ''.join(c if ord(c) < 128 else '' for c in (m.text or ''))[:100]
                    parts.append(f"  [{m.msg_type}] {clean}")
            return "\n".join(parts) if parts else ""
        finally:
            db.close()
    except Exception:
        return ""


def parse_with_llm(text: str) -> Optional[dict]:
    """
    Parsa un messaggio Telegram usando Claude Haiku.
    Ritorna un dict con i campi strutturati, o None se l'API non è disponibile.
    """
    client = _get_client()
    if client is None:
        return None

    context = _build_context()
    user_msg = f"Messaggio: {text}"
    if context:
        user_msg = f"CONTESTO ATTUALE:\n{context}\n\n{user_msg}"

    try:
        response = client.messages.create(
            model="claude-haiku-4-5-20251001",
            max_tokens=300,
            system=SYSTEM_PROMPT,
            messages=[{"role": "user", "content": user_msg}]
        )
        raw = response.content[0].text.strip()
        # Estrai JSON anche se c'è testo extra
        m = re.search(r'\{.*\}', raw, re.DOTALL)
        if m:
            return json.loads(m.group(0))
    except Exception as e:
        print(f"[LLMParser] Errore: {str(e)[:100]}", flush=True)

    return None


def llm_to_parsed(data: dict):
    """
    Converte il dict LLM negli oggetti ParsedXxx del parser originale.
    Ritorna (msg_type, parsed_object).
    """
    from parser import (
        ParsedSignal, ParsedUpdate, ParsedSLMove, ParsedClose,
        _expand_range, _parse_float
    )

    msg_type = data.get("type", "ignore")

    if msg_type == "signal":
        symbol = data.get("symbol")
        direction = data.get("direction")
        if not symbol or not direction:
            return "other", None
        return "signal", ParsedSignal(
            symbol=symbol,
            direction=direction,
            entry_price=data.get("entry_low"),
            entry_price_high=data.get("entry_high"),
            tp1=data.get("tp1"),
            tp2=data.get("tp2"),
            tp3=data.get("tp3"),
            stoploss=data.get("sl"),
            raw=data.get("_raw", ""),
            is_risky=data.get("is_risky", False),
        )

    elif msg_type == "close":
        return "close", ParsedClose(
            symbol=data.get("symbol"),
            close_price=data.get("price_from"),
            reason=data.get("close_reason"),
            raw=data.get("_raw", ""),
        )

    elif msg_type == "reenter":
        from parser import ParsedReenter
        return "reenter", ParsedReenter(
            symbol=data.get("symbol"),
            raw=data.get("_raw", ""),
        )

    elif msg_type == "sl_move":
        return "sl_move", ParsedSLMove(
            new_sl=data.get("new_sl"),
            is_breakeven=data.get("is_breakeven", False),
            symbol=data.get("symbol"),
            raw=data.get("_raw", ""),
        )

    elif msg_type == "update":
        from parser import ParsedUpdate
        return "update", ParsedUpdate(
            symbol=data.get("symbol", ""),
            price_from=data.get("price_from"),
            price_to=data.get("price_to"),
            status_text="update",
            raw=data.get("_raw", ""),
        )

    elif msg_type == "risky_flag":
        # Gestito separatamente in telegram_client tramite reply
        return "risky_flag", data

    return "other", None
