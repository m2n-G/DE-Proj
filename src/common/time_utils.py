"""Time helpers for Korea market data."""

from datetime import datetime

from config.market_config import MARKET_TIMEZONE


def now_kst() -> datetime:
    return datetime.now(tz=MARKET_TIMEZONE)


def yyyymmdd(dt: datetime) -> str:
    return dt.strftime("%Y%m%d")


def hour_hh(dt: datetime) -> str:
    return dt.strftime("%H")

