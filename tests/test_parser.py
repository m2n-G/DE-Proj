from src.common.parser import parse_trade_message


def test_parse_trade_message():
    raw = "0|H0STCNT0|004|005930^102305^78500^5^3200^248720^78200^78600^75300"

    record = parse_trade_message(raw, trade_date="20240115")

    assert record.stock_code == "005930"
    assert record.trade_time == "102305"
    assert record.trade_price == 78500
    assert record.volume == 3200
    assert record.open_price == 78600
    assert record.prev_close == 75300

