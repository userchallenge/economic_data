# economic_data/load/save_data.py
import logging
from sqlalchemy.exc import IntegrityError

logger = logging.getLogger(__name__)

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
    except IntegrityError:
        # Handle unique constraint violation

        session.rollback()
        # print(
        #     f"Stock index with ticker_id '{index_data['ticker_id']}' already exists. Skipping."
        # )
        logger.info(
            f"Stock index with ticker_id '{index_data['ticker_id']}' already exists. Skipping."
        )

        # Optionally: return the existing ID
        existing = (
            session.query(StockIndex)
            .filter_by(ticker_id=index_data["ticker_id"])
            .first()
        )
        return existing.id if existing else None

    except Exception as e:
        session.rollback()
        raise e
    finally:
        session.close()


def save_stock_data(index_id: int, data: list):
    session = Session()
    try:
        # Fetch all existing (index_id, date) combinations
        existing = set(
            session.query(StockIndexData.date)
            .filter(StockIndexData.index_id == index_id)
            .all()
        )
        # Flatten from list of tuples to set of dates
        existing_dates = {d[0] for d in existing}

        new_records = []
        for entry in data:
            entry_date = entry["date"]
            if entry_date not in existing_dates:
                new_records.append(StockIndexData(index_id=index_id, **entry))

        session.add_all(new_records)
        session.commit()
        # print(f"Inserted {len(new_records)} new records.")
        logger.info(f"Inserted {len(new_records)} new records for index ID {index_id}.")
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
