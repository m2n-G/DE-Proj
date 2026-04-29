"""Lightweight records used between pipeline steps."""

from dataclasses import dataclass


@dataclass(frozen=True)
class TradeRecord:
    stock_code: str
    trade_date: str
    trade_time: str
    trade_price: int
    volume: int
    open_price: int | None
    prev_close: int | None


@dataclass(frozen=True)
class SignalRecord:
    date: str
    stock_code: str
    open: int
    ma_short: float
    ma_long: float
    signal: str
    ma_combo: str
    detected_at: str

