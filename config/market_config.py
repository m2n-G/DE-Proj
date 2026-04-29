"""Korean stock market time configuration."""

from datetime import time
from zoneinfo import ZoneInfo

MARKET_TIMEZONE = ZoneInfo("Asia/Seoul")
MARKET_OPEN = time(9, 0)
MARKET_CLOSE = time(15, 30)


def is_market_time(current_time: time) -> bool:
    return MARKET_OPEN <= current_time <= MARKET_CLOSE
