# economic_data/load/save_data.py

from economic_data.db.schema import (
    EconomicIndicator,
    EconomicIndicatorData,
    StockIndex,
    StockIndexData,
    Threshold,
)
from economic_data.db.session import Session


def get_all_indicators():
    session = Session()
    try:
        return session.query(EconomicIndicator).all()
    finally:
        session.close()


def get_indicator_data(indicator_id: int):
    session = Session()
    try:
        return (
            session.query(EconomicIndicatorData)
            .filter_by(indicator_id=indicator_id)
            .all()
        )
    finally:
        session.close()


def get_all_stock_indices():
    session = Session()
    try:
        return session.query(StockIndex).all()
    finally:
        session.close()


def get_stock_data(index_id: int):
    session = Session()
    try:
        return session.query(StockIndexData).filter_by(index_id=index_id).all()
    finally:
        session.close()


def get_thresholds_for_indicator(indicator_id: int):
    session = Session()
    try:
        return session.query(Threshold).filter_by(indicator_id=indicator_id).all()
    finally:
        session.close()


def get_thresholds_for_stock_index(index_id: int):
    session = Session()
    try:
        return session.query(Threshold).filter_by(stock_index_id=index_id).all()
    finally:
        session.close()
