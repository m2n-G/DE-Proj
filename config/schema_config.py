"""Column definitions used across Bronze, Silver, and Gold datasets."""

BRONZE_FIELDS = [
    "raw_message",
    "received_at",
]

SILVER_TRADE_COLUMNS = [
    "stock_code",
    "trade_date",
    "trade_time",
    "trade_price",
    "volume",
    "open_price",
    "prev_close",
]

SILVER_DAILY_COLUMNS = [
    "stock_code",
    "date",
    "open",
    "high",
    "low",
    "close",
    "volume",
]

GOLD_SIGNAL_COLUMNS = [
    "date",
    "stock_code",
    "open",
    "ma_short",
    "ma_long",
    "signal",
    "ma_combo",
    "detected_at",
]
