"""Market calendar and trading-hour helpers."""

from datetime import date, time, timedelta, timezone
import logging


logger = logging.getLogger(__name__)

MARKET_TIMEZONE = timezone(timedelta(hours=9))
MARKET_OPEN = time(9, 0)
MARKET_CLOSE = time(15, 30)
MARKET_DAYS = {0, 1, 2, 3, 4}

_holiday_checker = None
_holiday_checker_load_failed = False


def _get_holiday_checker():
    global _holiday_checker, _holiday_checker_load_failed

    if _holiday_checker is not None:
        return _holiday_checker

    if _holiday_checker_load_failed:
        return None

    try:
        from holidayskr import is_holiday as holidayskr_is_holiday
    except Exception as exc:  # pragma: no cover - dependency/network fallback
        _holiday_checker_load_failed = True
        logger.warning(
            "holidayskr is unavailable; using weekday-only market day check: %s",
            exc,
        )
        return None

    _holiday_checker = holidayskr_is_holiday
    return _holiday_checker


def is_market_time(current_time: time) -> bool:
    return MARKET_OPEN <= current_time <= MARKET_CLOSE


def is_market_day(current_date: date) -> bool:
    holiday_checker = _get_holiday_checker()
    if holiday_checker is None:
        return current_date.weekday() in MARKET_DAYS

    if holiday_checker(current_date):
        logger.info("%s is a holiday.", current_date)
        return False

    return current_date.weekday() in MARKET_DAYS
