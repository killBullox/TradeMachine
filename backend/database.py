from sqlalchemy import create_engine, Column, Integer, String, Float, DateTime, Boolean, Text, Enum
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool
from datetime import datetime
import enum
import os
from dotenv import load_dotenv

load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL", "sqlite:///./tradesdb.sqlite")
engine = create_engine(
    DATABASE_URL,
    connect_args={"check_same_thread": False},
    poolclass=StaticPool,
    isolation_level=None,  # autocommit: ogni statement è la propria transazione
)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()


class TradeStatus(str, enum.Enum):
    PENDING = "pending"
    OPEN = "open"
    TP1 = "tp1"
    TP2 = "tp2"
    TP3 = "tp3"
    CLOSED = "closed"
    SL_HIT = "sl_hit"
    CANCELLED = "cancelled"


class TradeDirection(str, enum.Enum):
    BUY = "buy"
    SELL = "sell"


class Signal(Base):
    __tablename__ = "signals"

    id = Column(Integer, primary_key=True, index=True)
    telegram_msg_id = Column(Integer, unique=True, nullable=True)
    symbol = Column(String(20), nullable=False)
    direction = Column(String(10), nullable=False)  # buy / sell
    entry_price = Column(Float, nullable=True)
    entry_price_high = Column(Float, nullable=True)  # per range entry
    tp1 = Column(Float, nullable=True)
    tp2 = Column(Float, nullable=True)
    tp3 = Column(Float, nullable=True)
    stoploss = Column(Float, nullable=True)
    status = Column(String(20), default="pending")
    actual_entry_price = Column(Float, nullable=True)  # prezzo reale di ingresso
    entered_at = Column(DateTime, nullable=True)       # orario reale di ingresso
    trade_log = Column(Text, nullable=True)            # JSON: lista eventi [{event,price,ts}]
    exit_price = Column(Float, nullable=True)
    closed_at = Column(DateTime, nullable=True)
    # Risk management
    risk_usd = Column(Float, nullable=True)       # rischio in $ per questo trade
    position_size = Column(Float, nullable=True)  # lotti calcolati
    pnl_usd = Column(Float, nullable=True)        # P&L realizzato in $
    raw_message = Column(Text, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    notes = Column(Text, nullable=True)
    is_archived = Column(Boolean, default=False)
    mt5_ticket = Column(Integer, nullable=True)    # ticket principale (primo ordine)
    mt5_tickets = Column(Text, nullable=True)      # JSON: [ticket1, ticket2, ticket3]
    is_risky = Column(Boolean, default=False)      # segnale "risky" → lotto dimezzato
    # Override per-trade del default globale risk_settings.trail_stop_enabled.
    # NULL = segui il default globale; True/False = override esplicito utente.
    trail_stop_enabled = Column(Boolean, nullable=True)
    # Broker su cui e' stato eseguito il trade (es. 'xm', 'avatrade').
    # Popolato all'apertura. NULL = legacy o broker non specificato.
    broker = Column(String(20), nullable=True)
    # Numero account MT5 al momento dell'apertura (es. 27640489 per XM demo).
    # Popolato all'apertura. Permette di distinguere trade fra conti diversi
    # dello stesso broker (es. demo vs reale).
    mt5_account = Column(Integer, nullable=True)
    # Tipo di entry richiesto dal segnale: 'near' (LIMIT/MARKET su pullback)
    # oppure 'breakout' (STOP sopra/sotto livello, da "Buy Above"/"Sell Below").
    entry_type = Column(String(20), nullable=True)
    # Signal escluso dai filtri utente (symbol/hour). Se True: il bot NON ha
    # piazzato ordini reali sul broker, ma gestisce comunque il signal (status
    # changes, sl_move, target_done, edit) per simulare l'esito ipotetico.
    # is_filtered=True trade vanno SEMPRE esclusi dalle statistiche reali e
    # mostrati solo in sezione what-if.
    is_filtered = Column(Boolean, default=False)
    filter_reason = Column(Text, nullable=True)  # es. "symbol XAUUSD excluded"


class TradeUpdate(Base):
    __tablename__ = "trade_updates"

    id = Column(Integer, primary_key=True, index=True)
    signal_id = Column(Integer, nullable=True)  # FK logica verso signals
    telegram_msg_id = Column(Integer, nullable=True)
    symbol = Column(String(20), nullable=True)
    price_from = Column(Float, nullable=True)
    price_to = Column(Float, nullable=True)
    update_text = Column(Text, nullable=True)
    raw_message = Column(Text, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow)


class MarketLevel(Base):
    __tablename__ = "market_levels"

    id = Column(Integer, primary_key=True, index=True)
    symbol = Column(String(20), nullable=False)
    support_levels = Column(Text, nullable=True)   # JSON string
    resistance_levels = Column(Text, nullable=True)  # JSON string
    date = Column(DateTime, default=datetime.utcnow)
    raw_message = Column(Text, nullable=True)


class RiskSettings(Base):
    """Parametri di rischio configurabili dall'utente."""
    __tablename__ = "risk_settings"

    id = Column(Integer, primary_key=True)
    account_size = Column(Float, default=10000.0)    # capitale in $
    risk_per_trade_pct = Column(Float, default=1.0)  # rischio % per trade
    risk_per_trade_usd = Column(Float, nullable=True) # rischio fisso $ (se usato)
    use_fixed_usd = Column(Boolean, default=False)    # usa $ fisso invece di %
    auto_trade = Column(Boolean, default=False)       # MT5 auto-trading attivo
    entry_tolerance_pips = Column(Float, default=3.0)  # tolleranza pip per entry MARKET vicino al range
    trail_stop_enabled = Column(Boolean, default=False)  # auto trail SL: TP1 hit -> BE+1pip, TP2 hit -> TP1+1pip
    max_margin_pct_per_trade = Column(Float, default=50.0)  # cap % free margin per trade (default 50%)
    # ─── Filtri signal (escludono determinati signal dal placing reale).
    # Il signal escluso viene comunque salvato con is_filtered=True e gestito
    # come se fosse aperto (sl_move/target_done/edit applicati) per fare
    # simulazione what-if. Nessuna interazione MT5 reale.
    excluded_symbols = Column(Text, nullable=True)  # JSON array es. ["EURJPY","USOIL"]
    allowed_hours = Column(Text, nullable=True)     # JSON array int 0-23 (Roma). None=tutte
    # ─── News filter (post-mortem #570: CPI gap -8166$) ───
    news_filter_enabled = Column(Boolean, default=True)   # blocco entry + flatten attorno a news
    friday_flatten_enabled = Column(Boolean, default=True) # chiusura totale venerdi' sera (weekend gap)
    # BE a TP1: appena colpito il primo TP, porta lo SL a BE e lascia li'.
    # Feature INDIPENDENTE dall'auto-trail (trail_stop_enabled): quello resta
    # com'e' (progressivo). Default ON (backtest XAUUSD +12.7%).
    be_at_tp1_enabled = Column(Boolean, default=True)
    updated_at = Column(DateTime, default=datetime.utcnow)


class NewsEvent(Base):
    """Eventi macro high-impact (CPI/NFP/FOMC...). Attorno a questi eventi:
    - [T-10min, T+5min]: blocco nuovi ingressi (market + pending)
    - [T-10min]: cancellazione pending non fillati
    - [T-5min] se flatten=True: chiusura totale posizioni aperte
    event_time e' UTC."""
    __tablename__ = "news_events"

    id = Column(Integer, primary_key=True, index=True)
    name = Column(String(120), nullable=False)          # es. "US CPI (June)"
    event_time = Column(DateTime, nullable=False)       # UTC
    currency = Column(String(10), default="USD")
    impact = Column(String(20), default="high")
    flatten = Column(Boolean, default=True)             # chiudi posizioni aperte a T-5
    flatten_done = Column(Boolean, default=False)       # idempotenza runner
    created_at = Column(DateTime, default=datetime.utcnow)


class JournalEntry(Base):
    __tablename__ = "journal"

    id = Column(Integer, primary_key=True, index=True)
    signal_id = Column(Integer, nullable=True)
    title = Column(String(200), nullable=True)
    content = Column(Text, nullable=False)
    trade_result = Column(Float, nullable=True)  # P&L in pips o $
    emotion = Column(String(50), nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow)


class SLMove(Base):
    """Spostamenti espliciti dello SL (trail/move SL to X)."""
    __tablename__ = "sl_moves"

    id = Column(Integer, primary_key=True, index=True)
    signal_id = Column(Integer, nullable=True)       # FK logica verso signals
    telegram_msg_id = Column(Integer, nullable=True)
    new_sl = Column(Float, nullable=True)            # None = break-even
    is_breakeven = Column(Boolean, default=False)
    raw_message = Column(Text, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow)


class RawMessage(Base):
    __tablename__ = "raw_messages"

    id = Column(Integer, primary_key=True, index=True)
    telegram_msg_id = Column(Integer, unique=True)
    sender = Column(String(200), nullable=True)
    text = Column(Text, nullable=True)
    msg_type = Column(String(50), nullable=True)  # signal / update / level / other
    created_at = Column(DateTime, default=datetime.utcnow)


class Mt5Account(Base):
    """Account MT5 configurati dall'utente."""
    __tablename__ = "mt5_accounts"

    id = Column(Integer, primary_key=True, index=True)
    login = Column(Integer, nullable=False, unique=True)
    server = Column(String(100), nullable=False)
    label = Column(String(200), nullable=False)
    is_demo = Column(Boolean, default=True)
    is_default = Column(Boolean, default=False)
    is_active = Column(Boolean, default=False)  # account attualmente in uso (uno solo True)
    mt5_path = Column(String(500), nullable=True)  # path al terminale MT5 dedicato (None = default da .env)
    broker = Column(String(50), nullable=True)  # tag broker (es. 'xm', 'avatrade'); popolato in signals.broker
    # ─── Prop Mode Settings (additivo, nullable). Account "normali" (Avatrade
    # demo) restano con questi a NULL/False e il comportamento del bot non
    # cambia di una virgola. Tutte le guardie prop-specific sono gated da
    # `if account.prop_mode:` e dai singoli limiti opzionali.
    prop_mode = Column(Boolean, default=False)
    daily_dd_limit_usd = Column(Float, nullable=True)  # blocca trade nuovi se P&L giorno < -X
    daily_dd_warning_usd = Column(Float, nullable=True)  # warning (UI/log) prima del block
    peak_equity_usd = Column(Float, nullable=True)  # max equity osservato (trailing DD)
    max_total_dd_usd = Column(Float, nullable=True)  # equita' inseguita: bust se equity < peak - X
    consistency_threshold_pct = Column(Float, default=30.0)  # max single-day vs total P&L
    max_concurrent_trades = Column(Integer, nullable=True)  # cap posizioni aperte simultaneamente
    created_at = Column(DateTime, default=datetime.utcnow)


class RestorePoint(Base):
    """Punto di ripristino creato durante l'archiviazione dei trade."""
    __tablename__ = "restore_points"

    id = Column(Integer, primary_key=True, index=True)
    name = Column(String(200), nullable=False)
    description = Column(Text, nullable=True)
    signals_count = Column(Integer, default=0)
    signals_data = Column(Text, nullable=False)  # JSON: snapshot completo dei segnali archiviati
    created_at = Column(DateTime, default=datetime.utcnow)


class EmaCase(Base):
    """Entry Market Assessment: traccia trade non entrati per mancanza pullback
    (STOP mai filled, droppato da TG). Simula esito se fossimo entrati a MARKET."""
    __tablename__ = "ema_cases"
    id = Column(Integer, primary_key=True, index=True)
    signal_id = Column(Integer, nullable=False, index=True)
    symbol = Column(String(20))
    direction = Column(String(10))
    signal_time = Column(DateTime)
    cancel_time = Column(DateTime)
    cancel_reason = Column(String(40))  # 'target_done', 'sl_move_drop', 'tg_close', 'expired'
    entry_signal = Column(Float)         # entry dichiarato dal segnale
    entry_market = Column(Float)         # prezzo che avremmo usato a MARKET (ask BUY / bid SELL)
    stoploss = Column(Float)
    tp1 = Column(Float)
    tp2 = Column(Float)
    tp3 = Column(Float)
    sim_outcome = Column(String(20))     # 'tp1','tp2','tp3','sl_hit','no_hit'
    sim_pnl_usd = Column(Float)
    sim_close_time = Column(DateTime)
    sim_max_favorable_pct = Column(Float)  # max distanza verso TP3 in % del prezzo
    sim_max_adverse_pct = Column(Float)    # max distanza verso SL in %
    notes = Column(Text)
    created_at = Column(DateTime, default=datetime.utcnow)


def init_db():
    Base.metadata.create_all(bind=engine)
    import sqlalchemy as sa
    with engine.connect() as conn:
        # Migrazione incrementale colonne signals
        existing = [row[1] for row in conn.execute(sa.text("PRAGMA table_info(signals)")).fetchall()]
        new_cols = [
            ("exit_price",          "FLOAT"),
            ("closed_at",           "DATETIME"),
            ("risk_usd",            "FLOAT"),
            ("position_size",       "FLOAT"),
            ("pnl_usd",             "FLOAT"),
            ("actual_entry_price",  "FLOAT"),
            ("entered_at",          "DATETIME"),
            ("trade_log",           "TEXT"),
            ("mt5_ticket",          "INTEGER"),
        ]
        for col, typedef in new_cols:
            if col not in existing:
                conn.execute(sa.text(f"ALTER TABLE signals ADD COLUMN {col} {typedef}"))
        if "mt5_tickets" not in existing:
            conn.execute(sa.text("ALTER TABLE signals ADD COLUMN mt5_tickets TEXT"))
        if "is_risky" not in existing:
            conn.execute(sa.text("ALTER TABLE signals ADD COLUMN is_risky BOOLEAN DEFAULT 0"))
        if "trail_stop_enabled" not in existing:
            conn.execute(sa.text("ALTER TABLE signals ADD COLUMN trail_stop_enabled BOOLEAN"))
        if "broker" not in existing:
            conn.execute(sa.text("ALTER TABLE signals ADD COLUMN broker VARCHAR(20)"))
        if "mt5_account" not in existing:
            conn.execute(sa.text("ALTER TABLE signals ADD COLUMN mt5_account INTEGER"))
        if "entry_type" not in existing:
            conn.execute(sa.text("ALTER TABLE signals ADD COLUMN entry_type VARCHAR(20)"))
        if "is_filtered" not in existing:
            conn.execute(sa.text("ALTER TABLE signals ADD COLUMN is_filtered BOOLEAN DEFAULT 0"))
        if "filter_reason" not in existing:
            conn.execute(sa.text("ALTER TABLE signals ADD COLUMN filter_reason TEXT"))
        conn.commit()
        # Migrazione risk_settings
        rs_existing = [row[1] for row in conn.execute(sa.text("PRAGMA table_info(risk_settings)")).fetchall()]
        if "auto_trade" not in rs_existing:
            conn.execute(sa.text("ALTER TABLE risk_settings ADD COLUMN auto_trade BOOLEAN DEFAULT 0"))
            conn.commit()
        if "entry_tolerance_pips" not in rs_existing:
            conn.execute(sa.text("ALTER TABLE risk_settings ADD COLUMN entry_tolerance_pips FLOAT DEFAULT 3.0"))
            conn.commit()
        if "trail_stop_enabled" not in rs_existing:
            conn.execute(sa.text("ALTER TABLE risk_settings ADD COLUMN trail_stop_enabled BOOLEAN DEFAULT 0"))
            conn.commit()
        if "max_margin_pct_per_trade" not in rs_existing:
            conn.execute(sa.text("ALTER TABLE risk_settings ADD COLUMN max_margin_pct_per_trade FLOAT DEFAULT 50.0"))
            conn.commit()
        if "excluded_symbols" not in rs_existing:
            conn.execute(sa.text("ALTER TABLE risk_settings ADD COLUMN excluded_symbols TEXT"))
            conn.commit()
        if "allowed_hours" not in rs_existing:
            conn.execute(sa.text("ALTER TABLE risk_settings ADD COLUMN allowed_hours TEXT"))
            conn.commit()
        if "news_filter_enabled" not in rs_existing:
            conn.execute(sa.text("ALTER TABLE risk_settings ADD COLUMN news_filter_enabled BOOLEAN DEFAULT 1"))
            conn.commit()
        if "friday_flatten_enabled" not in rs_existing:
            conn.execute(sa.text("ALTER TABLE risk_settings ADD COLUMN friday_flatten_enabled BOOLEAN DEFAULT 1"))
            conn.commit()
        if "be_at_tp1_enabled" not in rs_existing:
            conn.execute(sa.text("ALTER TABLE risk_settings ADD COLUMN be_at_tp1_enabled BOOLEAN DEFAULT 1"))
            conn.commit()
        # Migrazione mt5_accounts
        try:
            mt5_existing = [row[1] for row in conn.execute(sa.text("PRAGMA table_info(mt5_accounts)")).fetchall()]
            if mt5_existing:
                if "is_active" not in mt5_existing:
                    conn.execute(sa.text("ALTER TABLE mt5_accounts ADD COLUMN is_active BOOLEAN DEFAULT 0"))
                if "mt5_path" not in mt5_existing:
                    conn.execute(sa.text("ALTER TABLE mt5_accounts ADD COLUMN mt5_path VARCHAR(500)"))
                if "broker" not in mt5_existing:
                    conn.execute(sa.text("ALTER TABLE mt5_accounts ADD COLUMN broker VARCHAR(50)"))
                # ── Prop Mode columns (additive, nullable). Account esistenti
                # (Avatrade) restano con default False/NULL → comportamento invariato.
                if "prop_mode" not in mt5_existing:
                    conn.execute(sa.text("ALTER TABLE mt5_accounts ADD COLUMN prop_mode BOOLEAN DEFAULT 0"))
                if "daily_dd_limit_usd" not in mt5_existing:
                    conn.execute(sa.text("ALTER TABLE mt5_accounts ADD COLUMN daily_dd_limit_usd FLOAT"))
                if "daily_dd_warning_usd" not in mt5_existing:
                    conn.execute(sa.text("ALTER TABLE mt5_accounts ADD COLUMN daily_dd_warning_usd FLOAT"))
                if "peak_equity_usd" not in mt5_existing:
                    conn.execute(sa.text("ALTER TABLE mt5_accounts ADD COLUMN peak_equity_usd FLOAT"))
                if "max_total_dd_usd" not in mt5_existing:
                    conn.execute(sa.text("ALTER TABLE mt5_accounts ADD COLUMN max_total_dd_usd FLOAT"))
                if "consistency_threshold_pct" not in mt5_existing:
                    conn.execute(sa.text("ALTER TABLE mt5_accounts ADD COLUMN consistency_threshold_pct FLOAT DEFAULT 30.0"))
                if "max_concurrent_trades" not in mt5_existing:
                    conn.execute(sa.text("ALTER TABLE mt5_accounts ADD COLUMN max_concurrent_trades INTEGER"))
                conn.commit()
        except Exception:
            pass
        # Inserisce settings di default se non esistono
        count = conn.execute(sa.text("SELECT COUNT(*) FROM risk_settings")).scalar()
        if count == 0:
            conn.execute(sa.text(
                "INSERT INTO risk_settings (account_size, risk_per_trade_pct, use_fixed_usd) VALUES (10000, 1.0, 0)"
            ))
            conn.commit()


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
