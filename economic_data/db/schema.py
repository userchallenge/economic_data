from sqlalchemy import (
    Column,
    Integer,
    String,
    Float,
    Date,
    ForeignKey,
    Enum,
    UniqueConstraint,
)
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship
import enum

Base = declarative_base()


class Frequency(enum.Enum):
    daily = "daily"
    monthly = "monthly"
    quarterly = "quarterly"
    yearly = "yearly"


class EconomicIndicator(Base):
    __tablename__ = "economic_indicators"

    id = Column(Integer, primary_key=True)
    indicator_id = Column(String, unique=True, nullable=False)  # Official name/id
    name = Column(String)  # Descriptive name of the indicator that I use
    description = Column(String)  # Description of the indicator
    unit = Column(String)
    frequency = Column(Enum(Frequency))
    source = Column(String)

    data_points = relationship("EconomicIndicatorData", back_populates="indicator")
    thresholds = relationship("Threshold", back_populates="indicator")


class EconomicIndicatorData(Base):
    __tablename__ = "economic_indicator_data"
    __table_args__ = (
        # Ensure (index_id, date) is unique
        UniqueConstraint("indicator_id", "date", name="uix_indicator_id_date"),
    )

    id = Column(Integer, primary_key=True)
    indicator_id = Column(Integer, ForeignKey("economic_indicators.id"), nullable=False)
    date = Column(Date, nullable=False)
    value = Column(Float, nullable=False)

    indicator = relationship("EconomicIndicator", back_populates="data_points")


# class StockIndex(Base):
#     __tablename__ = "stock_indices"

#     id = Column(Integer, primary_key=True)
#     name = Column(String, unique=True, nullable=False)
#     description = Column(String)
#     symbol = Column(String, unique=True)
#     source = Column(String)

#     data_points = relationship("StockIndexData", back_populates="index")
#     thresholds = relationship("Threshold", back_populates="stock_index")


class StockIndex(Base):
    """Represents a stock index with its associated data and thresholds.

    Attributes:

        id (int): Unique identifier for the stock index.
        ticker_id (str): Unique ticker/symbol for the stock index, e.g., 'INDEXNASDAQ:OMXSPI'.
        name (str): Descriptive name of the stock index, e.g., 'OMX Stockholm All-Share Index'.
        description (str): Additional information about the stock index.
        source (str): Source of the stock index data.
        data_points (list): List of StockIndexData objects associated with this index.
        thresholds (list): List of Threshold objects associated with this stock index.
    """

    __tablename__ = "stock_indices"

    id = Column(Integer, primary_key=True)
    ticker_id = Column(
        String, unique=True, nullable=False
    )  # unique ticker/symbol like 'INDEXNASDAQ:OMXSPI'
    name = Column(
        String, nullable=False
    )  # descriptive name like 'OMX Stockholm All-Share Index'
    description = Column(String)
    source = Column(String)

    data_points = relationship("StockIndexData", back_populates="index")
    thresholds = relationship("Threshold", back_populates="stock_index")


class StockIndexData(Base):
    """Represents a data point for a stock index.
    Attributes:

        id (int): Unique identifier for the stock index data point.
        index_id (int): Foreign key referencing the associated stock index.
        date (date): Date of the data point.
        close_value (float): Closing value of the stock index on the given date.
        open_value (float): Opening value of the stock index on the given date.
        high_value (float): Highest value of the stock index on the given date.
        low_value (float): Lowest value of the stock index on the given date.
        index (StockIndex): Relationship to the StockIndex object this data point belongs to.
    """

    __tablename__ = "stock_index_data"
    __table_args__ = (
        # Ensure (index_id, date) is unique
        UniqueConstraint("index_id", "date", name="uix_index_id_date"),
    )

    id = Column(Integer, primary_key=True)
    index_id = Column(Integer, ForeignKey("stock_indices.id"), nullable=False)
    date = Column(Date, nullable=False)
    close_value = Column(Float, nullable=False)
    open_value = Column(Float, nullable=True)
    high_value = Column(Float, nullable=True)
    low_value = Column(Float, nullable=True)
    volume = Column(Float, nullable=True)
    index = relationship("StockIndex", back_populates="data_points")


class ThresholdCategory(enum.Enum):
    bad = "bad"
    normal = "normal"
    good = "good"


class Threshold(Base):
    __tablename__ = "thresholds"

    id = Column(Integer, primary_key=True)
    indicator_id = Column(Integer, ForeignKey("economic_indicators.id"), nullable=True)
    stock_index_id = Column(Integer, ForeignKey("stock_indices.id"), nullable=True)
    category = Column(Enum(ThresholdCategory), nullable=False)
    min_value = Column(Float, nullable=True)
    max_value = Column(Float, nullable=True)

    indicator = relationship("EconomicIndicator", back_populates="thresholds")
    stock_index = relationship("StockIndex", back_populates="thresholds")
