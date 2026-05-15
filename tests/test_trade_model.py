import pytest
import time
from models.market_data import TradeEvent


@pytest.fixture
def valid_message():
    return {
        "e": "trade",
        "E": int(time.time() * 1000),
        "s": "BTCUSDT",
        "t": 99999,
        "p": "80500.00",
        "q": "0.00150",
        "T": int(time.time() * 1000),
        "m": False,
        "M": True,
    }


def test_parse_valid_message(valid_message):
    event = TradeEvent.from_binance_message(valid_message)
    assert event.symbol == "BTCUSDT"
    assert event.price == pytest.approx(80500.00)
    assert event.quantity == pytest.approx(0.00150)
    assert event.is_buyer_maker is False


def test_price_is_float(valid_message):
    event = TradeEvent.from_binance_message(valid_message)
    assert isinstance(event.price, float)


def test_quantity_is_float(valid_message):
    event = TradeEvent.from_binance_message(valid_message)
    assert isinstance(event.quantity, float)


def test_immutability(valid_message):
    event = TradeEvent.from_binance_message(valid_message)
    with pytest.raises(Exception):
        event.price = 999.0


def test_missing_buyer_order_id_defaults_none(valid_message):
    # Binance removed 'b' field — must handle gracefully
    assert "b" not in valid_message
    event = TradeEvent.from_binance_message(valid_message)
    assert event.buyer_order_id is None


def test_buyer_order_id_parsed_when_present(valid_message):
    valid_message["b"] = 12345
    valid_message["a"] = 67890
    event = TradeEvent.from_binance_message(valid_message)
    assert event.buyer_order_id == 12345
    assert event.seller_order_id == 67890


def test_missing_required_field_raises(valid_message):
    del valid_message["p"]
    with pytest.raises(KeyError):
        TradeEvent.from_binance_message(valid_message)


def test_trade_id_parsed(valid_message):
    event = TradeEvent.from_binance_message(valid_message)
    assert event.trade_id == 99999
