"""Pytest fixtures e mock per i test del backend TradeMachine.

Mock di MetaTrader5: i test NON parlano con un broker reale. Forniamo un
modulo fittizio importabile come `MetaTrader5` con costanti e oggetti tipo
ticket/posizione/ordine emulati.
"""
import os
import sys
import types
from dataclasses import dataclass
from typing import Optional
import pytest


# ─── Mock MetaTrader5 ─────────────────────────────────────────────────────────

class _MT5Constants:
    TRADE_ACTION_DEAL = 1
    TRADE_ACTION_PENDING = 2
    TRADE_ACTION_SLTP = 3
    TRADE_ACTION_MODIFY = 4
    TRADE_ACTION_REMOVE = 5
    TRADE_ACTION_CLOSE_BY = 6
    ORDER_TYPE_BUY = 0
    ORDER_TYPE_SELL = 1
    ORDER_TYPE_BUY_LIMIT = 2
    ORDER_TYPE_SELL_LIMIT = 3
    ORDER_TYPE_BUY_STOP = 4
    ORDER_TYPE_SELL_STOP = 5
    POSITION_TYPE_BUY = 0
    POSITION_TYPE_SELL = 1
    ORDER_FILLING_FOK = 0
    ORDER_FILLING_IOC = 1
    ORDER_FILLING_RETURN = 2
    DEAL_ENTRY_IN = 0
    DEAL_ENTRY_OUT = 1
    DEAL_ENTRY_INOUT = 2
    TRADE_RETCODE_DONE = 10009
    TRADE_RETCODE_INVALID_STOPS = 10016
    COPY_TICKS_ALL = 0


@dataclass
class _SymbolInfo:
    digits: int = 2
    point: float = 0.01
    trade_stops_level: int = 0
    trade_freeze_level: int = 0
    trade_contract_size: float = 100.0
    filling_mode: int = 1  # FOK


@dataclass
class _Tick:
    bid: float = 0.0
    ask: float = 0.0
    time: int = 0
    last: float = 0.0


@dataclass
class _Position:
    ticket: int = 0
    symbol: str = ""
    type: int = 0  # POSITION_TYPE_BUY
    volume: float = 0.0
    price_open: float = 0.0
    sl: float = 0.0
    tp: float = 0.0
    profit: float = 0.0


@dataclass
class _Order:
    ticket: int = 0
    symbol: str = ""
    type: int = 0
    volume_initial: float = 0.0
    price_open: float = 0.0
    sl: float = 0.0
    tp: float = 0.0
    state: int = 1  # placed


@dataclass
class _OrderResult:
    retcode: int = 10009  # TRADE_RETCODE_DONE
    order: int = 0
    deal: int = 0
    comment: str = "ok"


@dataclass
class _AccountInfo:
    login: int = 0
    server: str = ""
    balance: float = 10000.0
    equity: float = 10000.0
    margin: float = 0.0
    margin_free: float = 10000.0
    profit: float = 0.0
    leverage: int = 30
    currency: str = "USD"


class FakeMT5(_MT5Constants):
    """Mock MetaTrader5 con stato interno controllabile dai test."""
    def __init__(self):
        self.positions: dict[int, _Position] = {}
        self.orders: dict[int, _Order] = {}
        self.symbols_info: dict[str, _SymbolInfo] = {}
        self.ticks: dict[str, _Tick] = {}
        self._account = _AccountInfo()
        self._initialized = False
        self._last_error_tuple = (1, "Success")
        self._next_ticket = 1000000
        self.history_deals_by_position: dict[int, list] = {}
        # script di response: lista di retcode per le prossime order_send (per simulare reject)
        self.order_send_results: list = []

    def initialize(self, **kwargs):
        self._initialized = True
        return True

    def shutdown(self):
        self._initialized = False

    def last_error(self):
        return self._last_error_tuple

    def account_info(self):
        return self._account

    def symbol_info(self, symbol):
        return self.symbols_info.get(symbol)

    def symbol_info_tick(self, symbol):
        return self.ticks.get(symbol)

    def symbol_select(self, symbol, enable):
        return True

    def positions_get(self, ticket=None, symbol=None):
        if ticket is not None:
            p = self.positions.get(ticket)
            return (p,) if p else None
        if symbol is not None:
            res = [p for p in self.positions.values() if p.symbol == symbol]
            return tuple(res) if res else None
        return tuple(self.positions.values()) if self.positions else ()

    def orders_get(self, ticket=None, symbol=None):
        if ticket is not None:
            o = self.orders.get(ticket)
            return (o,) if o else None
        if symbol is not None:
            res = [o for o in self.orders.values() if o.symbol == symbol]
            return tuple(res) if res else None
        return tuple(self.orders.values()) if self.orders else ()

    def terminal_info(self):
        return types.SimpleNamespace(trade_allowed=True)

    def order_send(self, request):
        # Pop scripted result se presente
        if self.order_send_results:
            r = self.order_send_results.pop(0)
            return r
        # Behavior default: simula azione
        action = request.get("action")
        if action == self.TRADE_ACTION_DEAL:
            ticket = self._next_ticket
            self._next_ticket += 1
            symbol = request.get("symbol", "")
            order_type = request.get("type", self.ORDER_TYPE_BUY)
            pos_type = self.POSITION_TYPE_BUY if order_type == self.ORDER_TYPE_BUY else self.POSITION_TYPE_SELL
            self.positions[ticket] = _Position(
                ticket=ticket, symbol=symbol, type=pos_type,
                volume=request.get("volume", 0.0),
                price_open=request.get("price", 0.0),
                sl=request.get("sl", 0.0),
                tp=request.get("tp", 0.0),
            )
            return _OrderResult(retcode=self.TRADE_RETCODE_DONE, order=ticket, deal=ticket)
        elif action == self.TRADE_ACTION_PENDING:
            ticket = self._next_ticket
            self._next_ticket += 1
            self.orders[ticket] = _Order(
                ticket=ticket, symbol=request.get("symbol", ""),
                type=request.get("type", 0),
                volume_initial=request.get("volume", 0.0),
                price_open=request.get("price", 0.0),
                sl=request.get("sl", 0.0),
                tp=request.get("tp", 0.0),
            )
            return _OrderResult(retcode=self.TRADE_RETCODE_DONE, order=ticket)
        elif action == self.TRADE_ACTION_REMOVE:
            tk = request.get("order")
            self.orders.pop(tk, None)
            return _OrderResult(retcode=self.TRADE_RETCODE_DONE, order=tk)
        elif action == self.TRADE_ACTION_SLTP:
            tk = request.get("position")
            if tk in self.positions:
                p = self.positions[tk]
                if "sl" in request: p.sl = request["sl"]
                if "tp" in request: p.tp = request["tp"]
            return _OrderResult(retcode=self.TRADE_RETCODE_DONE, order=tk)
        return _OrderResult(retcode=self.TRADE_RETCODE_DONE)

    def history_deals_get(self, *args, **kwargs):
        position = kwargs.get("position")
        if position is not None:
            return tuple(self.history_deals_by_position.get(position, []))
        return ()

    def history_orders_get(self, *args, **kwargs):
        return ()

    def copy_ticks_range(self, *args, **kwargs):
        return []


# ─── Fixtures ─────────────────────────────────────────────────────────────────

@pytest.fixture(autouse=True)
def _add_backend_to_path(monkeypatch):
    """Aggiunge il path di backend/ al sys.path per tutti i test."""
    backend_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    if backend_dir not in sys.path:
        sys.path.insert(0, backend_dir)
    # Disabilita LLM calls reali sui test
    monkeypatch.setenv("ANTHROPIC_API_KEY", "")
    yield


@pytest.fixture(autouse=True)
def _block_real_mt5(monkeypatch):
    """GUARDIA GLOBALE (autouse): NESSUN test deve mai importare il modulo
    MetaTrader5 reale — mt5.initialize() AVVIA il terminale MT5 sulla macchina
    dove gira pytest (incidente 12/07: pytest sul laptop ha lanciato MT5 locale,
    violando la regola 'MT5 solo su VPS'). Inietta sempre il FakeMT5; i test
    che vogliono configurarlo usano la fixture fake_mt5 che ritorna l'istanza."""
    fake = FakeMT5()
    monkeypatch.setitem(sys.modules, "MetaTrader5", fake)
    yield fake


@pytest.fixture
def fake_mt5(_block_real_mt5):
    """Ritorna il FakeMT5 gia' iniettato dalla guardia globale autouse."""
    return _block_real_mt5


@pytest.fixture
def in_memory_db(monkeypatch, tmp_path):
    """Crea un DB SQLite in memoria per i test e patcha SessionLocal."""
    db_file = tmp_path / "test_trades.db"
    monkeypatch.setenv("DATABASE_URL", f"sqlite:///{db_file}")
    # Reset moduli database se importati
    for mod in list(sys.modules.keys()):
        if mod.startswith("database"):
            del sys.modules[mod]
    from sqlalchemy import create_engine
    from sqlalchemy.orm import sessionmaker
    import database as db_mod
    engine = create_engine(f"sqlite:///{db_file}", connect_args={"check_same_thread": False})
    SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
    db_mod.Base.metadata.create_all(bind=engine)
    monkeypatch.setattr(db_mod, "SessionLocal", SessionLocal)
    monkeypatch.setattr(db_mod, "engine", engine, raising=False)
    yield SessionLocal
    engine.dispose()
