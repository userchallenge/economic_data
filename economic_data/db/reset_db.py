# economic_data/db/reset_db.py

import os
from economic_data.db.create_db import create_database


def reset_database():
    current_dir = os.path.dirname(os.path.abspath(__file__))
    db_file = os.path.join(current_dir, "economic_data.sqlite")

    if os.path.exists(db_file):
        os.remove(db_file)
        print(f"Deleted existing database: {db_file}")

    create_database()


if __name__ == "__main__":
    reset_database()
