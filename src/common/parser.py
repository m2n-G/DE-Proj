"""Parser for Korea Investment raw WebSocket trade messages."""

from src.common.models import TradeRecord


def parse_trade_message(raw_message: str, trade_date: str) -> TradeRecord:
    """Parse one raw trade message into a typed trade record.

    Expected example:
    0|H0STCNT0|004|005930^102305^78500^5^3200^248720^78200^78600^75300^...
    """
    try:
        payload = raw_message.strip().split("|", maxsplit=3)[3]
        fields = payload.split("^")
        stock_code = fields[0]
        trade_time = fields[1]
        trade_price = int(fields[2])
        volume = int(fields[4])
        open_price = int(fields[7])
        prev_close = int(fields[8])
    except (IndexError, ValueError) as exc:
        raise ValueError(f"Invalid raw trade message: {raw_message}") from exc

    return TradeRecord(
        stock_code=stock_code,
        trade_date=trade_date,
        trade_time=trade_time,
        trade_price=trade_price,
        volume=volume,
        open_price=open_price,
        prev_close=prev_close,
    )

