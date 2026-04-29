"""Path helpers wrapping config-level S3 key builders."""

from config.path_config import (
    bronze_key,
    gold_signal_key,
    silver_daily_key,
    silver_trade_key,
)

__all__ = [
    "bronze_key",
    "silver_trade_key",
    "silver_daily_key",
    "gold_signal_key",
]

