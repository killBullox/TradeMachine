"""
Risk Management — calcola position size e P&L per ogni segnale.

Logica:
  risk_usd = account_size * risk_pct / 100  (o valore fisso)
  sl_distance = |entry - stoploss| in punti del simbolo
  point_value = valore in $ di 1 punto per 1 lotto standard
  position_size (lotti) = risk_usd / (sl_distance * point_value)
  pnl_usd = (exit_price - entry_price) * position_size * point_value * direzione
"""

from typing import Optional
from database import SessionLocal, Signal, RiskSettings

# ─── Point value per simbolo ──────────────────────────────────────────────────
# (pip_size, point_value_per_lot_usd)
# pip_size: minima variazione di prezzo rilevante
# point_value: $ per 1 pip per 1 lotto standard (100k units per forex, 100 oz per gold)
# Per coppie JPY il point_value è approssimato a tasso ~155

SYMBOL_SPECS = {
    # Metalli
    "XAUUSD": {"pip": 0.01, "pv": 1.0,   "lot_units": 100},    # 100 oz, $1/pip/lot
    "XAGUSD": {"pip": 0.001,"pv": 5.0,   "lot_units": 5000},
    # Forex USD quote
    "EURUSD": {"pip": 0.0001,"pv": 10.0, "lot_units": 100000},
    "GBPUSD": {"pip": 0.0001,"pv": 10.0, "lot_units": 100000},
    "AUDUSD": {"pip": 0.0001,"pv": 10.0, "lot_units": 100000},
    "NZDUSD": {"pip": 0.0001,"pv": 10.0, "lot_units": 100000},
    # Forex USD base (inverted)
    "USDCHF": {"pip": 0.0001,"pv": 9.0,  "lot_units": 100000},  # ~1/1.11
    "USDCAD": {"pip": 0.0001,"pv": 7.3,  "lot_units": 100000},  # ~1/1.37
    "USDJPY": {"pip": 0.01,  "pv": 6.5,  "lot_units": 100000},  # pip/155*100k
    # Cross JPY
    "GBPJPY": {"pip": 0.01,  "pv": 6.5,  "lot_units": 100000},
    "EURJPY": {"pip": 0.01,  "pv": 6.5,  "lot_units": 100000},
    "AUDJPY": {"pip": 0.01,  "pv": 6.5,  "lot_units": 100000},
    # Cross non-USD
    "GBPCHF": {"pip": 0.0001,"pv": 11.0, "lot_units": 100000},
    "EURGBP": {"pip": 0.0001,"pv": 12.5, "lot_units": 100000},
    # Crypto (1 unità = 1 coin, point_value = $1 per $1 di prezzo per coin)
    "BTCUSD": {"pip": 1.0,   "pv": 1.0,  "lot_units": 1},
    "ETHUSD": {"pip": 0.1,   "pv": 1.0,  "lot_units": 1},
    # Indici (futures mini)
    "NASDAQ": {"pip": 0.25,  "pv": 5.0,  "lot_units": 1},       # NQ: $5/tick
    "US30":   {"pip": 1.0,   "pv": 5.0,  "lot_units": 1},       # YM: $5/tick
    "US500":  {"pip": 0.25,  "pv": 12.5, "lot_units": 1},       # ES: $12.5/tick
    # Petrolio
    "USOIL":  {"pip": 0.01,  "pv": 10.0, "lot_units": 1000},    # CL: $10/pip/lot
    "OIL":    {"pip": 0.01,  "pv": 10.0, "lot_units": 1000},
}

DEFAULT_SPEC = {"pip": 0.0001, "pv": 10.0, "lot_units": 100000}


def get_spec(symbol: str) -> dict:
    return SYMBOL_SPECS.get(symbol.upper(), DEFAULT_SPEC)


# ─── Settings ────────────────────────────────────────────────────────────────

def get_risk_settings() -> dict:
    db = SessionLocal()
    try:
        s = db.query(RiskSettings).first()
        if s is None:
            return {"account_size": 10000, "risk_per_trade_pct": 1.0,
                    "risk_per_trade_usd": None, "use_fixed_usd": False,
                    "entry_tolerance_pips": 3.0}
        return {
            "account_size":      s.account_size,
            "risk_per_trade_pct": s.risk_per_trade_pct,
            "risk_per_trade_usd": s.risk_per_trade_usd,
            "use_fixed_usd":     s.use_fixed_usd,
            "entry_tolerance_pips": getattr(s, "entry_tolerance_pips", None) or 3.0,
        }
    finally:
        db.close()


def save_risk_settings(account_size: float, risk_pct: float,
                       risk_usd: Optional[float] = None,
                       use_fixed: bool = False):
    db = SessionLocal()
    try:
        s = db.query(RiskSettings).first()
        if s is None:
            s = RiskSettings()
            db.add(s)
        s.account_size = account_size
        s.risk_per_trade_pct = risk_pct
        s.risk_per_trade_usd = risk_usd
        s.use_fixed_usd = use_fixed
        db.commit()
    finally:
        db.close()


# ─── Calcoli ─────────────────────────────────────────────────────────────────

def calc_risk_amount(settings: dict) -> float:
    """Ritorna il rischio in $ per trade."""
    if settings["use_fixed_usd"] and settings["risk_per_trade_usd"]:
        return settings["risk_per_trade_usd"]
    return settings["account_size"] * settings["risk_per_trade_pct"] / 100.0


def calc_position_size(symbol: str, entry: float, sl: float,
                       risk_usd: float) -> Optional[float]:
    """
    Calcola i lotti da tradare per non superare risk_usd.
    Ritorna i lotti con 2 decimali (es. 0.05 lotti).
    """
    if not entry or not sl or entry == sl:
        return None
    spec = get_spec(symbol)
    sl_distance_pips = abs(entry - sl) / spec["pip"]
    if sl_distance_pips == 0:
        return None
    pip_value_total = spec["pv"]  # $ per 1 pip per 1 lotto
    lots = risk_usd / (sl_distance_pips * pip_value_total)
    return round(lots, 2)


def calc_pnl(symbol: str, direction: str, entry: float, exit_price: float,
             position_size: float) -> float:
    """
    Calcola P&L in $ dato entry, exit e position size.
    direction: 'buy' o 'sell'
    """
    if not entry or not exit_price or not position_size:
        return 0.0
    spec = get_spec(symbol)
    price_diff = exit_price - entry
    if direction.lower() == "sell":
        price_diff = -price_diff
    pips = price_diff / spec["pip"]
    return round(pips * spec["pv"] * position_size, 2)


# ─── Ricalcola tutti i segnali con i settings attuali ────────────────────────

def _calc_pnl_from_trade_log(sig, lots: float, entry: float) -> tuple:
    """
    Analizza trade_log e calcola P&L per ogni evento.
    Ritorna (pnl_totale, trade_log_aggiornato_con_pnl).
    La posizione viene divisa equamente per il numero di TP definiti.
    """
    import json
    if not sig.trade_log:
        return None, None

    try:
        events = json.loads(sig.trade_log)
    except Exception:
        return None, None

    # Conta quanti TP sono definiti sul segnale
    num_tps = sum(1 for tp in [sig.tp1, sig.tp2, sig.tp3] if tp is not None)
    if num_tps == 0:
        num_tps = 1
    lots_per_tp = round(lots / num_tps, 2)

    # Quanti TP sono stati colpiti
    tp_events = [e for e in events if e["event"].startswith("tp")]
    tps_hit = len(tp_events)
    remaining_lots = round(lots - lots_per_tp * tps_hit, 2)

    total_pnl = 0.0
    updated_events = []
    for ev in events:
        ev_copy = dict(ev)
        price = ev.get("price")
        pnl = None

        if ev["event"] == "entry":
            ev_copy["pnl"] = None  # nessun P&L all'ingresso
        elif ev["event"].startswith("tp"):
            if price and entry and lots_per_tp > 0:
                pnl = calc_pnl(sig.symbol, sig.direction or "buy", entry, price, lots_per_tp)
                ev_copy["pnl"] = pnl
                total_pnl += pnl
        elif ev["event"] == "sl_hit":
            if price and entry and remaining_lots > 0:
                pnl = calc_pnl(sig.symbol, sig.direction or "buy", entry, price, remaining_lots)
                ev_copy["pnl"] = pnl
                total_pnl += pnl

        updated_events.append(ev_copy)

    return round(total_pnl, 2), json.dumps(updated_events)


def recalculate_all():
    """
    Per ogni segnale: ricalcola position_size, risk_usd, pnl_usd
    e aggiorna trade_log con P&L per evento.
    """
    settings = get_risk_settings()
    risk_amount = calc_risk_amount(settings)

    db = SessionLocal()
    try:
        signals = db.query(Signal).all()
        updated = 0
        for sig in signals:
            # Usa actual_entry_price se disponibile (prezzo reale di fill)
            entry = sig.actual_entry_price or sig.entry_price or sig.entry_price_high
            sl = sig.stoploss

            # Position size (dimezzata se segnale risky)
            effective_risk = risk_amount * 0.5 if getattr(sig, 'is_risky', False) else risk_amount
            lots = calc_position_size(sig.symbol, entry, sl, effective_risk)
            sig.risk_usd = effective_risk
            # Se il segnale è effettivamente piazzato su MT5 (ticket attivo, non
            # cancellato), il position_size autorevole arriva da place_orders /
            # sync_positions: NON sovrascrivere con il ricalcolo teorico, che
            # usa una formula diversa e spesso differisce dalla realtà MT5.
            is_mt5_active = bool(sig.mt5_ticket or sig.mt5_tickets) and sig.status != "cancelled"
            if not is_mt5_active:
                sig.position_size = lots

            if lots and entry:
                # Segnali MT5: P&L autorevole da sync_positions, non sovrascrivere
                if sig.mt5_ticket:
                    # Aggiorna solo position_size e risk_usd, lascia pnl_usd di MT5
                    db.add(sig)
                    updated += 1
                    continue

                # Backfill trade_log se mancante e il segnale è chiuso
                if not sig.trade_log and sig.status in ("sl_hit", "tp1", "tp2", "tp3", "closed"):
                    try:
                        from price_service import _backfill_signal
                        result = _backfill_signal(sig)
                        if result and result.get("trade_log"):
                            sig.trade_log = result["trade_log"]
                            sig.actual_entry_price = result.get("actual_entry_price") or sig.actual_entry_price
                            sig.entered_at = result.get("entered_at") or sig.entered_at
                            sig.exit_price = result.get("exit_price") or sig.exit_price
                            sig.closed_at = result.get("closed_at") or sig.closed_at
                    except Exception as e:
                        print(f"[Risk] Backfill #{sig.id} fallito: {e}", flush=True)

                # P&L da trade_log (con parziali per TP)
                total_pnl, updated_log = _calc_pnl_from_trade_log(sig, lots, entry)
                if total_pnl is not None:
                    sig.pnl_usd = total_pnl
                    sig.trade_log = updated_log
                elif sig.status in ("sl_hit", "tp1", "tp2", "tp3") and sig.exit_price:
                    # Fallback: P&L sull'intero lotto con exit_price
                    sig.pnl_usd = calc_pnl(sig.symbol, sig.direction or "buy",
                                            entry, sig.exit_price, lots)
                else:
                    sig.pnl_usd = None
            else:
                sig.pnl_usd = None

            db.add(sig)
            updated += 1

        db.commit()
        print(f"[Risk] Ricalcolati {updated} segnali (risk={risk_amount:.2f}$)", flush=True)
        return updated
    finally:
        db.close()
