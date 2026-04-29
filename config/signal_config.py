"""Signal configuration for the MVP pipeline."""

WATCHLIST = {
    "005930": "Samsung Electronics",
    "000660": "SK hynix",
}

DEFAULT_MA_SHORT = 5
DEFAULT_MA_LONG = 20

MA_CONFIG = {
    "005930": {"short": 5, "long": 20},
    "000660": {"short": 20, "long": 60},
}


def get_ma_config(stock_code: str) -> dict[str, int]:
    """Return the configured MA pair for a stock code."""
    return MA_CONFIG.get(
        stock_code,
        {"short": DEFAULT_MA_SHORT, "long": DEFAULT_MA_LONG},
    )
