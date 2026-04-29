"""S3 key builders for the data lake layout."""

RAW_PREFIX = "raw"
PROCESSED_PREFIX = "processed"
SIGNAL_PREFIX = "signal"


def bronze_key(stock_code: str, date_yyyymmdd: str, hour_hh: str) -> str:
    return f"{RAW_PREFIX}/{stock_code}/date={date_yyyymmdd}/hour={hour_hh}/{stock_code}.log"


def silver_trade_key(stock_code: str, date_yyyymmdd: str, hour_hh: str) -> str:
    return (
        f"{PROCESSED_PREFIX}/{stock_code}/date={date_yyyymmdd}/"
        f"hour={hour_hh}/{stock_code}.parquet"
    )


def silver_daily_key(stock_code: str, date_yyyymmdd: str) -> str:
    return (
        f"{PROCESSED_PREFIX}/daily/{stock_code}/date={date_yyyymmdd}/"
        f"{stock_code}_daily.parquet"
    )


def gold_signal_key(stock_code: str, date_yyyymmdd: str) -> str:
    return (
        f"{SIGNAL_PREFIX}/{stock_code}/date={date_yyyymmdd}/"
        f"{stock_code}_signals.parquet"
    )
