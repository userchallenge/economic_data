# economic_data/load/save_data.py

from economic_data.db.schema import (
    EconomicIndicator,
    EconomicIndicatorData,
    StockIndex,
    StockIndexData,
    Threshold,
)
from economic_data.db.session import Session


def save_economic_indicator(indicator_data: dict):
    session = Session()
    try:
        indicator = EconomicIndicator(**indicator_data)
        session.add(indicator)
        session.commit()
        return indicator.id
    except Exception as e:
        session.rollback()
        raise e
    finally:
        session.close()


def save_indicator_data(indicator_id: int, data: list):
    session = Session()
    try:
        for entry in data:
            record = EconomicIndicatorData(indicator_id=indicator_id, **entry)
            session.add(record)
        session.commit()
    except Exception as e:
        session.rollback()
        raise e
    finally:
        session.close()


def save_stock_index(index_data: dict):
    session = Session()
    try:
        index = StockIndex(**index_data)
        session.add(index)
        session.commit()
        return index.id
    except Exception as e:
        session.rollback()
        raise e
    finally:
        session.close()


def save_stock_data(index_id: int, data: list):
    session = Session()
    try:
        for entry in data:
            record = StockIndexData(index_id=index_id, **entry)
            session.add(record)
        session.commit()
    except Exception as e:
        session.rollback()
        raise e
    finally:
        session.close()


def save_threshold(threshold_data: dict):
    session = Session()
    try:
        threshold = Threshold(**threshold_data)
        session.add(threshold)
        session.commit()
        return threshold.id
    except Exception as e:
        session.rollback()
        raise e
    finally:
        session.close()
