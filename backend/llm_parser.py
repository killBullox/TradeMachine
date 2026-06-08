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


SYSTEM_PROMPT = """Sei un parser preciso di messaggi Telegram di trading forex/crypto.
Rispondi SEMPRE con un JSON valido e nient'altro. Ignora markdown/emoji/asterischi nel testo
quando estrai i contenuti semantici (es. "**1st Target** Done" significa "first target done").

Schema:
{
  "type": "signal" | "close" | "sl_move" | "update" | "reenter" | "risky_flag" | "ignore",
  "symbol": "XAUUSD" | "BTCUSD" | "GBPJPY" | ... | null,
  "direction": "buy" | "sell" | null,
  "entry_type": "near" | "breakout" | null,
  "entry_low": numero | null,
  "entry_high": numero | null,
  "tp1": numero | null,
  "tp2": numero | null,
  "tp3": numero | null,
  "sl": numero | null,
  "is_risky": true | false,
  "new_sl": numero | null,
  "is_breakeven": true | false,
  "close_reason": "stringa" | null,
  "price_from": numero | null,
  "price_to": numero | null,
  "status_text": "first_target_hit" | "second_target_hit" | "third_target_hit" |
                 "all_targets_done" | "near_target" | "trail_active" | "in_profit" |
                 "price_update" | "general" | null
}

CLASSIFICAZIONE TYPE:

▸ "signal" — nuovo segnale di trading: deve avere symbol+direction+almeno un entry/TP/SL.
  Esempi: "XAUUSD Buy Near 4550-52, TP1 4560, SL 4540", "Sell Above 28000, TP 27800, SL 28100".
  entry_type:
    - "breakout" se il msg usa "above"/"below"/"break"/"break of": entrata STOP al breakout
    - "near" altrimenti (default): entrata LIMIT/MARKET al pullback

▸ "close" — chiudi tutto subito. ESEMPI:
  "Everyone close the trade", "Closing trade now", "Close the trade here",
  "Everyone Close at Cost CMP X" (CMP = current market price)
  NON è close:
  - "take exit near X" dopo "first target done" → sl_move a BE
  - "book profit X" → update informativo

▸ "sl_move" — sposta lo stoploss. ESEMPI:
  "Move SL to 4550" → new_sl=4550, is_breakeven=false
  "Everyone Hold With 4561 SL" → new_sl=4561, is_breakeven=false
  "Trail SL to 4992" → new_sl=4992
  "Move to cost / breakeven / entry" → is_breakeven=true, new_sl=null
  "Cost to cost" → is_breakeven=true
  "Take exit at cost" → is_breakeven=true

▸ "update" — aggiornamento prezzo/stato del trade in corso. status_text DEVE essere preciso:
  "1st/First Target Done" → status_text="first_target_hit"
  "2nd/Second Target Done" → status_text="second_target_hit"
  "3rd/Third/Last Target Done" → status_text="third_target_hit"
  "All Targets Done" → status_text="all_targets_done"
  "Near First Target" / "Approaching Target" → status_text="near_target"
  "Safe Trail in Profits" / "Running in profits" / "Hold Book Or Trail Accordingly" /
    "Hold And Trail" / "Book or Trail" / "Trail Accordingly" / "Trail your SL" /
    qualunque istruzione esplicita a fare trail dello stop loss → status_text="trail_active".
    IMPORTANTE: questa categoria deve catturare TUTTE le variazioni linguistiche con cui
    il trader chiede di muovere lo SL in profitto, non solo i pattern letterali sopra.
  "$XXX Profit Running" → status_text="in_profit"
  Solo "XAUUSD | 4550 To 4555" senza altro → status_text="price_update"
  Altrimenti → status_text="general"
  Estrai price_from e price_to dal pattern "X To Y" o "X to Y" se presente.

▸ "reenter" — rientra nel trade. Anche quando il msg menziona "SL hit" o "loss" PRIMA,
  l'intenzione principale e' "rientra". ESEMPI tutti type=reenter:
  - "enter again", "everyone enter again now"
  - "re-enter", "reenter", "Re enter" (con spazio), "RE-ENTER"
  - "open again", "open again now"
  - "Sl hit In Sudden Spike Risky Can Re enter Here With Same Sl" → reenter
    (il "Sl hit" e' contesto, l'azione e' "Re enter ... With Same Sl")
  - "SL hit but re-enter at same levels" → reenter
  - "Stop loss hit, you can re-enter here" → reenter

▸ "risky_flag" — segnala rischio elevato: "highly risky", "#risky", "aggressive", "#RiskyTrade"

▸ "ignore" — watchlist ("add X to watchlist"), livelli giornalieri ("Support: ..."),
  commenti senza azione ("good morning", news generiche).

ESTRAZIONE NUMERI:
- "Near 4550-52" / "Near-4550-52" → entry_low=4550, entry_high=4552 (espandi: 52 dopo 4550 = 4552)
- "Near 213.500-213.530" → entry_low=213.500, entry_high=213.530
- Simbolo: "#XAUUSD" → "XAUUSD". Se non esplicito ma deducibile → inferisci dal contesto.

ROBUSTEZZA:
- IGNORA caratteri markdown (`**`, `*`, `_`, `~`) ed emoji quando interpreti il significato.
  "**1st** Target Done" e "1st Target Done" significano la stessa cosa.
- IGNORA spazi multipli, newline, caratteri non-ASCII.
- Se il messaggio è ambiguo o non rientra in nessuna categoria → type="ignore", status_text=null.
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
        # entry_type ora viene direttamente dal LLM (più affidabile della post-regex).
        # Fallback regex se LLM non lo fornisce.
        raw_text = data.get("_raw", "") or ""
        entry_type = data.get("entry_type")
        if entry_type not in ("near", "breakout"):
            import re as _re_lp
            entry_type = "breakout" if _re_lp.search(r'\b(above|below)\b', raw_text, _re_lp.IGNORECASE) else "near"
        return "signal", ParsedSignal(
            symbol=symbol,
            direction=direction,
            entry_price=data.get("entry_low"),
            entry_price_high=data.get("entry_high"),
            tp1=data.get("tp1"),
            tp2=data.get("tp2"),
            tp3=data.get("tp3"),
            stoploss=data.get("sl"),
            raw=raw_text,
            is_risky=data.get("is_risky", False),
            entry_type=entry_type,
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
        # status_text ora viene direttamente dal LLM con valori semantici:
        # first_target_hit, second_target_hit, third_target_hit, all_targets_done,
        # near_target, trail_active, in_profit, price_update, general.
        status = data.get("status_text") or "general"
        return "update", ParsedUpdate(
            symbol=data.get("symbol", ""),
            price_from=data.get("price_from"),
            price_to=data.get("price_to"),
            status_text=status,
            raw=data.get("_raw", ""),
        )

    elif msg_type == "risky_flag":
        # Gestito separatamente in telegram_client tramite reply
        return "risky_flag", data

    return "other", None
