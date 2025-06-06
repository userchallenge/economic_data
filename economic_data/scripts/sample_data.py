# economic_data/scripts/sample_data.py

import sqlite3
import pandas as pd


def insert_sample_data(db_path="economic_data.db"):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    sample_data = [
        ("GDP", "Gross Domestic Product", "USD", "annual", "World Bank"),
        ("Inflation", "Consumer Price Index", "%", "monthly", "IMF"),
        ("Unemployment", "Unemployment Rate", "%", "monthly", "BLS"),
    ]
    cursor.executemany(
        """
        INSERT INTO economic_indicators (name, description, unit, frequency, source)
        VALUES (?, ?, ?, ?, ?);
    """,
        sample_data,
    )
    conn.commit()
    conn.close()
    print("Sample data inserted.")


def query_data(db_path="economic_data.db"):
    conn = sqlite3.connect(db_path)
    df = pd.read_sql_query("SELECT * FROM economic_indicators;", conn)
    conn.close()
    print(df)


if __name__ == "__main__":
    insert_sample_data()
    query_data()
