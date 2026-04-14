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
    updated_at = Column(DateTime, default=datetime.utcnow)


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
        conn.commit()
        # Migrazione risk_settings
        rs_existing = [row[1] for row in conn.execute(sa.text("PRAGMA table_info(risk_settings)")).fetchall()]
        if "auto_trade" not in rs_existing:
            conn.execute(sa.text("ALTER TABLE risk_settings ADD COLUMN auto_trade BOOLEAN DEFAULT 0"))
            conn.commit()
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
