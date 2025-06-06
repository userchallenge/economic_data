# economic_data/db_test_tmp.py

import os
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from economic_data.db.schema import EconomicIndicator
from economic_data.db.create_db import create_database

current_dir = os.path.dirname(os.path.abspath(__file__))
db_file = os.path.join(current_dir, "economic_data", "db", "economic_data.sqlite")
print(f"DB file path is: {db_file}")

db_url = f"sqlite:///{db_file}"


def insert_sample_data():
    engine = create_engine(db_url)
    Session = sessionmaker(bind=engine)
    session = Session()

    indicators = [
        EconomicIndicator(
            name="GDP",
            description="Gross Domestic Product",
            unit="USD",
            frequency="quarterly",
            source="World Bank",
        ),
        EconomicIndicator(
            name="Inflation",
            description="Consumer Price Index",
            unit="%",
            frequency="monthly",
            source="IMF",
        ),
        EconomicIndicator(
            name="Unemployment",
            description="Unemployment Rate",
            unit="%",
            frequency="monthly",
            source="BLS",
        ),
    ]

    session.add_all(indicators)
    session.commit()
    session.close()


def read_data_with_pandas():
    engine = create_engine(db_url)
    df = pd.read_sql("SELECT * FROM economic_indicators", engine)
    print(df)


if __name__ == "__main__":
    create_database()
    print(f"Using database file at: {db_file}")
    insert_sample_data()
    read_data_with_pandas()
