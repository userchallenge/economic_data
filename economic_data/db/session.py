from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

DB_PATH = "economic_data/db/economic_data.sqlite"
engine = create_engine(f"sqlite:///{DB_PATH}", echo=False)

Session = sessionmaker(bind=engine)
