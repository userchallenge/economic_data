import pytest
from economic_data.load.save_data import (
    save_indicator,
    save_indicator_data,
    save_stock_index,
    save_stock_data,
    save_threshold,
)
from economic_data.db.schema import EconomicIndicator, EconomicIndicatorData
from economic_data.db.session import Session


@pytest.fixture(scope="function")
def db_session():
    session = Session()
    yield session
    session.rollback()  # rollback any changes after each test
    session.close()


def test_save_economic_indicator(db_session):
    data = {
        "name": "Test Interest Rate",
        "unit": "%",
        "description": "Test rate",
    }
    indicator_id = save_indicator(data)
    result = db_session.query(EconomicIndicator).get(indicator_id)
    assert result is not None
    assert result.name == "Test Interest Rate"


def test_save_indicator_data(db_session):
    indicator_id = save_indicator(
        {
            "name": "GDP",
            "unit": "Billion",
            "description": "Test GDP",
        }
    )

    save_indicator_data(
        indicator_id,
        [
            {"date": "2023-01-01", "value": 1000.0, "frequency": "monthly"},
            {"date": "2023-02-01", "value": 1050.0, "frequency": "monthly"},
        ],
    )
    results = (
        db_session.query(EconomicIndicatorData)
        .filter_by(indicator_id=indicator_id)
        .all()
    )
    assert len(results) == 2


def test_save_stock_index(db_session):
    data = {
        "name": "S&P 500",
        "description": "US stock market index",
    }
    index_id = save_stock_index(data)
    result = db_session.query(EconomicIndicator).get(index_id)
    assert (
        result is None
    )  # Should be None because index_id is from StockIndex, not EconomicIndicator
    # So instead test StockIndex table directly:
    from economic_data.db.schema import StockIndex

    index = db_session.query(StockIndex).get(index_id)
    assert index is not None
    assert index.name == "S&P 500"


def test_save_stock_data(db_session):
    from economic_data.db.schema import StockIndex

    index_id = save_stock_index({"name": "NASDAQ", "description": "Tech stocks"})
    save_stock_data(
        index_id,
        [
            {"date": "2023-01-01", "value": 13000.0, "frequency": "daily"},
            {"date": "2023-01-02", "value": 13100.0, "frequency": "daily"},
        ],
    )
    from economic_data.db.schema import StockIndexData

    results = db_session.query(StockIndexData).filter_by(index_id=index_id).all()
    assert len(results) == 2


def test_save_threshold(db_session):
    indicator_id = save_indicator(
        {
            "name": "Inflation Rate",
            "unit": "%",
            "description": "Inflation test",
        }
    )
    threshold_data = {
        "indicator_id": indicator_id,
        "good_min": 1.0,
        "good_max": 2.0,
        "normal_min": 0.5,
        "normal_max": 3.0,
        "bad_min": None,
        "bad_max": None,
        "description": "Inflation thresholds",
    }
    threshold_id = save_threshold(threshold_data)
    assert threshold_id is not None
