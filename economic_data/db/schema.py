# # economic_data/db/schema.py

# from sqlalchemy import Column, Integer, String
# from sqlalchemy.ext.declarative import declarative_base

# Base = declarative_base()


# class EconomicIndicator(Base):
#     __tablename__ = "economic_indicators"

#     id = Column(Integer, primary_key=True, autoincrement=True)
#     name = Column(String, nullable=False)
#     description = Column(String)
#     unit = Column(String)
#     frequency = Column(String)
#     source = Column(String)


# economic_data/db/schema.py

from sqlalchemy import Column, Integer, String, Float, Date, ForeignKey, Enum
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
    name = Column(String, unique=True, nullable=False)
    description = Column(String)
    unit = Column(String)
    frequency = Column(Enum(Frequency))
    source = Column(String)

    data_points = relationship("EconomicIndicatorData", back_populates="indicator")
    thresholds = relationship("Threshold", back_populates="indicator")


class EconomicIndicatorData(Base):
    __tablename__ = "economic_indicator_data"

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
    __tablename__ = "stock_index_data"

    id = Column(Integer, primary_key=True)
    index_id = Column(Integer, ForeignKey("stock_indices.id"), nullable=False)
    date = Column(Date, nullable=False)
    close_value = Column(Float, nullable=False)

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
