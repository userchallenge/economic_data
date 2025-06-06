# economic_data/db/create_db.py

import os
from sqlalchemy import create_engine
from economic_data.db.schema import Base


def create_database():
    current_dir = os.path.dirname(os.path.abspath(__file__))
    os.makedirs(current_dir, exist_ok=True)  # Ensure db folder exists

    db_file = os.path.join(current_dir, "economic_data.sqlite")
    db_url = f"sqlite:///{db_file}"

    engine = create_engine(db_url)
    Base.metadata.create_all(engine)
    print(f"Database created at: {db_file}")


if __name__ == "__main__":
    create_database()
