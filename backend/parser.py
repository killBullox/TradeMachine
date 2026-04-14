"""
Parser per i messaggi del gruppo Inner Circle Trader.
Riconosce e struttura i vari tipi di messaggio:
- Segnale: #XAUUSD | Buy Near 5006-08 | Target 1 : 5013 | ... | Stoploss : 4980
- Update prezzo: #XAUUSD | 5007.00 To 5013.00 + testo stato
- Livelli di mercato: Today's Important Levels for #XAUUSD
- Watchlist: Add #USDJPY to watchlist
"""
import re
from dataclasses import dataclass, field
from typing import Optional, List


@dataclass
class ParsedSignal:
    symbol: str
    direction: str          # buy / sell
    entry_price: Optional[float]
    entry_price_high: Optional[float]  # range superiore es. 5008
    tp1: Optional[float]
    tp2: Optional[float]
    tp3: Optional[float]
    stoploss: Optional[float]
    raw: str
    is_risky: bool = False


@dataclass
class ParsedUpdate:
    symbol: str
    price_from: Optional[float]
    price_to: Optional[float]
    status_text: str          # "First Target Done", "Everyone Trail in Profits", ...
    raw: str


@dataclass
class ParsedLevel:
    symbol: str
    support_levels: List[float] = field(default_factory=list)
    resistance_levels: List[float] = field(default_factory=list)
    raw: str = ""


@dataclass
class ParsedWatchlist:
    symbol: str
    raw: str


@dataclass
class ParsedSLMove:
    """Spostamento esplicito dello SL (trail/move/modify SL to X)."""
    new_sl: Optional[float]   # None = 'cost' (break-even = entry price)
    is_breakeven: bool        # True se "move SL to cost / break even"
    symbol: Optional[str]     # estratto dal testo se presente
    raw: str


@dataclass
class ParsedClose:
    """Istruzione di chiusura immediata del trade."""
    symbol: Optional[str]     # simbolo esplicito nel messaggio (o None = tutti)
    close_price: Optional[float]  # "CMP 1.38970" = prezzo di riferimento (info)
    reason: Optional[str]     # testo motivazione (es. "BTC is changing the direction")
    raw: str


@dataclass
class ParsedReenter:
    """Istruzione di riaprire l'ultimo trade chiuso sullo stesso simbolo."""
    symbol: Optional[str]
    raw: str


def _clean(text: str) -> str:
    """Rimuove emoji e caratteri non ASCII mantenendo testo leggibile."""
    return re.sub(r'[^\x00-\x7F]+', '', text).strip()


def _parse_float(s: str) -> Optional[float]:
    try:
        return float(s.strip().replace(',', '.'))
    except Exception:
        return None


def _expand_range(raw1: str, raw2: str) -> Optional[float]:
    """
    Espande la notazione abbreviata del range entry.
    Esempi:
      "4333", "35"    → 4335   (ultimi 2 digit sostituiti)
      "69.000", "100" → 69.100 (3 digit decimali sostituiti)
      "100.200", "300"→ 100.300
      "4698", "4700"  → 4700   (numero completo, stessa lunghezza)
    """
    dot_pos = raw1.find('.')
    digits_a = raw1.replace('.', '')   # es. "69000" o "4333"

    if len(raw2) < len(digits_a):
        # raw2 è suffisso parziale: sostituisce gli ultimi len(raw2) digit
        full_digits = digits_a[:-len(raw2)] + raw2
        if dot_pos >= 0:
            # reinserisce il punto decimale nella stessa posizione
            return _parse_float(full_digits[:dot_pos] + '.' + full_digits[dot_pos:])
        return _parse_float(full_digits)
    else:
        # raw2 è un prezzo completo
        return _parse_float(raw2)


# Simboli validi forex/crypto/commodity — tutto il resto viene scartato
VALID_SYMBOLS = {
    'XAUUSD', 'XAGUSD', 'EURUSD', 'GBPUSD', 'USDJPY', 'USDCHF', 'USDCAD',
    'AUDUSD', 'NZDUSD', 'GBPJPY', 'EURJPY', 'AUDJPY', 'GBPCHF', 'EURGBP',
    'BTCUSD', 'ETHUSD', 'BTCUSDT', 'ETHUSDT',
    'EURCHF', 'EURAUD', 'GBPAUD', 'NZDJPY', 'CADJPY', 'CHFJPY',
    'AUDCAD', 'AUDCHF', 'AUDNZD', 'CADCHF', 'EURNZD', 'GBPNZD',
    'USOIL', 'UKOIL', 'US30', 'US500', 'NAS100', 'DAX40',
}


def _extract_symbol(text: str) -> Optional[str]:
    m = re.search(r'#([A-Z]{3,8})', text.upper())
    if m:
        sym = m.group(1)
        if sym in VALID_SYMBOLS:
            return sym
    return None


# ─── Riconoscimento tipo messaggio ────────────────────────────────────────────

SIGNAL_PATTERNS = [
    r'buy\s+near',
    r'sell\s+near',
    r'buy\s+now',
    r'sell\s+now',
    r'buy\s+@',
    r'sell\s+@',
    r'target\s+1\s*:',
    r'stoploss\s*:',
]

UPDATE_PATTERN = re.compile(
    r'#?([A-Z]{3,8})\s*\|?\s*([\d]+\.?\d*)\.?\s+to\s+([\d]+\.?\d*)',
    re.IGNORECASE
)

LEVEL_PATTERN = re.compile(
    r"today'?s?\s+important\s+levels?\s+for\s+#?([A-Z]{3,8})",
    re.IGNORECASE
)

WATCHLIST_PATTERN = re.compile(
    r'add\s+#([A-Z]{3,8})\s+to\s+watchlist',
    re.IGNORECASE
)

# SL move patterns — linguaggio naturale flessibile
# "Move SL to 5019" / "Trail SL to 4992" / "Hold With 4561 SL" / "SL now 4550" / "keep SL at 4550"
SL_MOVE_PATTERN = re.compile(
    r'(?:'
    r'(?:move|trail|modify|time\s+to\s+move)\s+sl\s+to\s+([\d]+\.?[\d]*|cost|break.?even)'  # move SL to X
    r'|(?:hold|keep|maintain)\s+(?:with\s+)?([\d]+\.?[\d]*)\s+(?:sl|stop(?:loss)?)'         # hold with X SL
    r'|(?:sl|stop(?:loss)?)\s+(?:now|at|to|is)?\s*([\d]+\.?[\d]*)'                          # SL now/at X
    r'|(?:move|trail|hold)\s+(?:to\s+|with\s+)?(?:cost\s*(?:to\s*cost)?|break.?even|entry)' # hold with cost to cost / move to cost
    r')',
    re.IGNORECASE
)

# Pattern per messaggi di chiusura immediata del trade
CLOSE_PATTERNS = [
    r'\bclose\s+(?:the\s+)?trade\b',         # "close the trade", "close trade"
    r'\bclosing\s+(?:the\s+)?trade\b',        # "closing the trade"
    r'\bexit\s+(?:the\s+)?trade\b',           # "exit the trade"
    r'\bclose\s+(?:all\s+)?(?:position|trade)s?\b',
    r'\bbook\s+(?:full\s+|partial\s+)?profit\b',  # "book profit", "book full profit", "book partial profit"
    r'\btake\s+profit\s+now\b',               # "take profit now"
    r'\bexit\s+now\b',                        # "exit now"
    r'\bclose\s+here\b',                      # "close here"
    r'\bclose\s+(?:immediately|instant)\b',   # "close immediately"
    r'\beveryone\s+close\b',                  # "everyone close"
    r'\btake\s+exit\b',                       # "take exit near X"
]
CLOSE_PATTERN = re.compile('|'.join(CLOSE_PATTERNS), re.IGNORECASE)

# Pattern per "rientra nel trade" — riaprire l'ultimo segnale chiuso
REENTER_PATTERN = re.compile(
    r'\b(?:enter\s+again|re.?enter|reopen|open\s+again)\b',
    re.IGNORECASE
)


def classify_message(text: str) -> str:
    """Restituisce: 'signal' | 'update' | 'sl_move' | 'close' | 'level' | 'watchlist' | 'other'"""
    clean = text.lower()

    if LEVEL_PATTERN.search(text):
        return 'level'
    if WATCHLIST_PATTERN.search(text):
        return 'watchlist'

    signal_score = sum(1 for p in SIGNAL_PATTERNS if re.search(p, clean))
    if signal_score >= 2:
        return 'signal'

    if CLOSE_PATTERN.search(text):
        return 'close'

    if SL_MOVE_PATTERN.search(text):
        return 'sl_move'

    if REENTER_PATTERN.search(text):
        return 'reenter'

    if UPDATE_PATTERN.search(text):
        return 'update'

    return 'other'


# ─── Parser specifici ─────────────────────────────────────────────────────────

def parse_signal(text: str) -> Optional[ParsedSignal]:
    """
    Formato atteso (flessibile):
    #XAUUSD | Buy Near 5006-08 | Target 1 : 5013 | Target 2 : 5020 | Target 3 : 5030 | Stoploss : 4980
    """
    # Rimuove markdown Telegram (**bold**, _italic_) prima di parsare
    clean = re.sub(r'\*+|_+', '', text)

    symbol = _extract_symbol(clean)
    if not symbol:
        return None

    # Direzione
    direction = None
    if re.search(r'\bbuy\b', clean, re.IGNORECASE):
        direction = 'buy'
    elif re.search(r'\bsell\b', clean, re.IGNORECASE):
        direction = 'sell'
    if not direction:
        return None

    # Entry price — gestisce: Near, Below, Above, Limit, At, @, Now
    entry_low = entry_high = None
    range_m = re.search(
        r'(?:near|below|above|limit|at|@|now)\s*[-–]?\s*([\d]+\.?\d*)\s*[-–]\s*([\d]+\.?\d*)',
        clean, re.IGNORECASE
    )
    single_m = re.search(
        r'(?:near|below|above|limit|at|@|now)\s*[-–]?\s*([\d]+\.?\d*)',
        clean, re.IGNORECASE
    )
    if range_m:
        raw1 = range_m.group(1).strip()
        raw2 = range_m.group(2).strip()
        entry_low = _parse_float(raw1)
        entry_high = _expand_range(raw1, raw2)
    elif single_m:
        entry_low = _parse_float(single_m.group(1))

    # Targets
    tp_vals = {}
    for m in re.finditer(r'target\s*(\d)\s*[:\|]\s*([\d]+\.?\d*)', clean, re.IGNORECASE):
        tp_vals[int(m.group(1))] = _parse_float(m.group(2))

    # Stoploss — gestisce separatori : | e forma "Keep Stoploss 99.850"
    sl = None
    sl_m = re.search(r'stoploss?\s*[:\|\s]\s*([\d]+\.?\d*)', clean, re.IGNORECASE)
    if sl_m:
        sl = _parse_float(sl_m.group(1))

    # Validazione direzionale: per BUY i TP devono essere > entry, per SELL < entry
    # TP che vanno nella direzione sbagliata (es. typo nel messaggio) vengono scartati
    ref_price = entry_low or entry_high
    validated_tps = {}
    for n, val in tp_vals.items():
        if val is None:
            continue
        if ref_price:
            if direction == 'buy' and val <= ref_price:
                continue   # TP below entry for BUY → typo, scarta
            if direction == 'sell' and val >= ref_price:
                continue  # TP above entry for SELL → typo, scarta
        validated_tps[n] = val

    # Auto-correzione TP fuori sequenza (typo su una cifra)
    # Per BUY: TP1 < TP2 < TP3. Per SELL: TP1 > TP2 > TP3.
    # Se un TP è fuori ordine, prova a correggere la cifra sbagliata.
    tp_list = sorted(validated_tps.keys())
    if len(tp_list) >= 2 and ref_price:
        for n in tp_list:
            val = validated_tps[n]
            # Trova i TP adiacenti per capire dove dovrebbe stare
            neighbors = [validated_tps[k] for k in tp_list if k != n]
            if direction == 'buy':
                # TP devono essere in ordine crescente
                expected_between = all(validated_tps.get(k, 0) < val for k in tp_list if k < n) and \
                                   all(validated_tps.get(k, 0) > val for k in tp_list if k > n)
                if not expected_between:
                    # Prova a sostituire ogni cifra per trovare un valore che sia in ordine
                    val_str = str(int(val)) if val == int(val) else str(val)
                    lower = max([validated_tps[k] for k in tp_list if k < n], default=ref_price)
                    upper = min([validated_tps[k] for k in tp_list if k > n], default=val * 2)
                    for i in range(len(val_str)):
                        if not val_str[i].isdigit():
                            continue
                        for d in '0123456789':
                            if d == val_str[i]:
                                continue
                            candidate = _parse_float(val_str[:i] + d + val_str[i+1:])
                            if candidate and lower < candidate < upper:
                                validated_tps[n] = candidate
                                break
                        if validated_tps[n] != val:
                            break
            elif direction == 'sell':
                expected_between = all(validated_tps.get(k, float('inf')) > val for k in tp_list if k < n) and \
                                   all(validated_tps.get(k, 0) < val for k in tp_list if k > n)
                if not expected_between:
                    val_str = str(int(val)) if val == int(val) else str(val)
                    upper = min([validated_tps[k] for k in tp_list if k < n], default=ref_price)
                    lower = max([validated_tps[k] for k in tp_list if k > n], default=0)
                    for i in range(len(val_str)):
                        if not val_str[i].isdigit():
                            continue
                        for d in '0123456789':
                            if d == val_str[i]:
                                continue
                            candidate = _parse_float(val_str[:i] + d + val_str[i+1:])
                            if candidate and lower < candidate < upper:
                                validated_tps[n] = candidate
                                break
                        if validated_tps[n] != val:
                            break

    # Rilevamento segnale "risky" → dimezza il rischio
    risky_keywords = r'\b(risky|aggressive|high.?risk|rischioso|pericoloso)\b'
    is_risky = bool(re.search(risky_keywords, text, re.IGNORECASE))

    return ParsedSignal(
        symbol=symbol,
        direction=direction,
        entry_price=entry_low,
        entry_price_high=entry_high,
        tp1=validated_tps.get(1),
        tp2=validated_tps.get(2),
        tp3=validated_tps.get(3),
        stoploss=sl,
        raw=text,
        is_risky=is_risky,
    )


def parse_update(text: str) -> Optional[ParsedUpdate]:
    m = UPDATE_PATTERN.search(text)
    if not m:
        return None

    symbol = m.group(1).upper()
    price_from = _parse_float(m.group(2))
    price_to = _parse_float(m.group(3))

    # Testo di stato (righe successive)
    lines = [l.strip() for l in text.split('\n') if l.strip()]
    status_lines = [l for l in lines if not UPDATE_PATTERN.search(l) and not l.startswith('#')]
    status_text = ' | '.join(_clean(l) for l in status_lines if _clean(l))

    return ParsedUpdate(
        symbol=symbol,
        price_from=price_from,
        price_to=price_to,
        status_text=status_text or 'update',
        raw=text,
    )


def parse_level(text: str) -> Optional[ParsedLevel]:
    m = LEVEL_PATTERN.search(text)
    if not m:
        return None
    symbol = m.group(1).upper()

    supports = []
    resistances = []

    sup_m = re.search(r'support\s*:\s*([\d\s\-\.]+)', text, re.IGNORECASE)
    if sup_m:
        supports = [_parse_float(v) for v in re.findall(r'[\d]+\.?[\d]*', sup_m.group(1))]
        supports = [v for v in supports if v]

    res_m = re.search(r'resistance\s*:\s*([\d\s\-\.]+)', text, re.IGNORECASE)
    if res_m:
        resistances = [_parse_float(v) for v in re.findall(r'[\d]+\.?[\d]*', res_m.group(1))]
        resistances = [v for v in resistances if v]

    return ParsedLevel(symbol=symbol, support_levels=supports, resistance_levels=resistances, raw=text)


def parse_watchlist(text: str) -> Optional[ParsedWatchlist]:
    m = WATCHLIST_PATTERN.search(text)
    if not m:
        return None
    return ParsedWatchlist(symbol=m.group(1).upper(), raw=text)


def parse_sl_move(text: str) -> Optional[ParsedSLMove]:
    clean = re.sub(r'\*+|_+', '', text)
    m = SL_MOVE_PATTERN.search(clean)
    if not m:
        return None
    # Prendi il primo gruppo non-None tra i gruppi di cattura
    raw_val = next((g.lower().strip() for g in m.groups() if g), None)
    is_be = raw_val in ('cost', 'breakeven', 'break-even', 'break even') if raw_val else \
            bool(re.search(r'cost|break.?even|entry', m.group(0), re.IGNORECASE))
    new_sl = None if (is_be or raw_val is None) else _parse_float(raw_val)
    symbol = _extract_symbol(clean)
    return ParsedSLMove(new_sl=new_sl, is_breakeven=is_be, symbol=symbol, raw=text)


# Nomi brevi → simbolo completo (per messaggi senza #SIMBOLO)
SHORT_SYMBOL_MAP = {
    'BTC': 'BTCUSD', 'BITCOIN': 'BTCUSD',
    'GOLD': 'XAUUSD', 'XAU': 'XAUUSD',
    'SILVER': 'XAGUSD', 'XAG': 'XAGUSD',
    'EURO': 'EURUSD',
    'CABLE': 'GBPUSD',
}


def _extract_symbol_broad(text: str) -> Optional[str]:
    """Estrae simbolo: prima #XAUUSD, poi nomi brevi (BTC, GOLD, etc.)."""
    sym = _extract_symbol(text)
    if sym:
        return sym
    upper = text.upper()
    for short, full in SHORT_SYMBOL_MAP.items():
        if re.search(r'\b' + short + r'\b', upper):
            return full
    return None


def parse_close(text: str) -> Optional[ParsedClose]:
    """
    Parsa messaggi di chiusura immediata.
    Esempi:
      "Everyone Close the trade here CMP 1.38970-1.38980"
      "Closing the trade here | BTC is changing the direction"
      "Book full profit now"
    """
    clean = re.sub(r'\*+|_+', '', text)
    symbol = _extract_symbol_broad(clean)

    # Cerca prezzo: CMP X, near X, at X, around X
    close_price = None
    price_m = re.search(r'(?:CMP|near|at|around)\s+([\d]+\.?[\d]*)', clean, re.IGNORECASE)
    if price_m:
        close_price = _parse_float(price_m.group(1))

    # Estrai motivazione dopo "|" (non dopo "-" per evitare range di prezzi)
    reason = None
    reason_m = re.search(r'\|\s*(.{5,80})$', clean.strip(), re.IGNORECASE)
    if reason_m:
        candidate = reason_m.group(1).strip()
        # Scarta se è solo un numero o range (es. "1.38980")
        if not re.match(r'^[\d\.\-\s]+$', candidate):
            reason = candidate

    return ParsedClose(symbol=symbol, close_price=close_price, reason=reason, raw=text)


def parse_reenter(text: str) -> Optional[ParsedReenter]:
    """Parsa messaggi di rientro nel trade."""
    clean = re.sub(r'\*+|_+', '', text)
    symbol = _extract_symbol(clean) or _extract_symbol_broad(clean)
    return ParsedReenter(symbol=symbol, raw=text)


def parse_message(text: str):
    """Entry point principale. Ritorna (tipo, oggetto_parsato)."""
    msg_type = classify_message(text)
    if msg_type == 'signal':
        return msg_type, parse_signal(text)
    elif msg_type == 'update':
        return msg_type, parse_update(text)
    elif msg_type == 'sl_move':
        return msg_type, parse_sl_move(text)
    elif msg_type == 'close':
        return msg_type, parse_close(text)
    elif msg_type == 'reenter':
        return msg_type, parse_reenter(text)
    elif msg_type == 'level':
        return msg_type, parse_level(text)
    elif msg_type == 'watchlist':
        return msg_type, parse_watchlist(text)
    return 'other', None
