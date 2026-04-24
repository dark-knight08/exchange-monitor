"""
Database models for HKEX Market Monitor.
SQLAlchemy ORM models for storing market data, snapshots, and alerts.
"""

from sqlalchemy import (
    Column, Integer, String, Float, DateTime, Date, 
    Boolean, ForeignKey, Index, create_engine, Text, Enum, JSON
)
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship, sessionmaker
from datetime import datetime
from enum import Enum as PyEnum
import os

Base = declarative_base()


class AssetClass(PyEnum):
    """Asset class enumeration."""
    EQUITY = "equity"
    ETF = "etf"
    FUTURE = "future"
    OPTION = "option"
    CBBC = "cbbc"
    WARRANT = "warrant"
    DERIVATIVE = "derivative"


class Asset(Base):
    """Base asset information."""
    __tablename__ = 'assets'
    
    id = Column(Integer, primary_key=True)
    symbol = Column(String(20), unique=True, nullable=False, index=True)
    name = Column(String(200), nullable=False)
    asset_class = Column(String(50), nullable=False, index=True)
    subtype = Column(String(50))  # e.g., HSI Futures, CBBC, etc.
    
    # Classification
    sector = Column(String(100))
    industry = Column(String(100))
    
    # Static attributes
    currency = Column(String(3), default='HKD')
    listing_date = Column(Date)
    
    # For derivatives
    underlying_symbol = Column(String(20))
    expiry_date = Column(Date)
    strike_price = Column(Float)
    contract_size = Column(Float)
    
    # Metadata
    is_active = Column(Boolean, default=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    # Relationships
    snapshots = relationship("MarketSnapshot", back_populates="asset", lazy="dynamic")
    daily_bars = relationship("DailyBar", back_populates="asset", lazy="dynamic")
    
    __table_args__ = (
        Index('idx_asset_class_symbol', 'asset_class', 'symbol'),
    )


class MarketSnapshot(Base):
    """Real-time or near real-time market snapshot."""
    __tablename__ = 'market_snapshots'
    
    id = Column(Integer, primary_key=True)
    asset_id = Column(Integer, ForeignKey('assets.id'), nullable=False, index=True)
    snapshot_time = Column(DateTime, nullable=False, index=True)
    snapshot_type = Column(String(20), default='realtime')  # realtime, intraday, daily, pre_market, etc.
    
    # Price data
    open = Column(Float)
    high = Column(Float)
    low = Column(Float)
    close = Column(Float)
    previous_close = Column(Float)
    
    # Change metrics
    change = Column(Float)
    change_percent = Column(Float)
    
    # Volume data
    volume = Column(Float)
    turnover = Column(Float)
    
    # Extended metrics (stored as JSON for flexibility)
    extended_metrics = Column(JSON)
    
    # Metadata
    created_at = Column(DateTime, default=datetime.utcnow)
    
    # Relationships
    asset = relationship("Asset", back_populates="snapshots")
    
    __table_args__ = (
        Index('idx_snapshot_time_asset', 'snapshot_time', 'asset_id'),
        Index('idx_snapshot_asset_time', 'asset_id', 'snapshot_time'),
    )


class DailyBar(Base):
    """End-of-day market data (OHLCV)."""
    __tablename__ = 'daily_bars'
    
    id = Column(Integer, primary_key=True)
    asset_id = Column(Integer, ForeignKey('assets.id'), nullable=False, index=True)
    date = Column(Date, nullable=False, index=True)
    
    # OHLCV
    open = Column(Float, nullable=False)
    high = Column(Float, nullable=False)
    low = Column(Float, nullable=False)
    close = Column(Float, nullable=False)
    volume = Column(Float, nullable=False)
    turnover = Column(Float)
    
    # Change from previous day
    change = Column(Float)
    change_percent = Column(Float)
    
    # Extended metrics
    market_cap = Column(Float)
    pe_ratio = Column(Float)
    dividend_yield = Column(Float)
    
    # For derivatives
    open_interest = Column(Float)
    implied_volatility = Column(Float)
    
    # Greeks for options
    delta = Column(Float)
    gamma = Column(Float)
    theta = Column(Float)
    vega = Column(Float)
    
    # For CBBCs/Warrants
    gearing = Column(Float)
    premium = Column(Float)
    call_level = Column(Float)
    entitlement_ratio = Column(Float)
    
    # Metadata
    created_at = Column(DateTime, default=datetime.utcnow)
    
    # Relationships
    asset = relationship("Asset", back_populates="daily_bars")
    
    __table_args__ = (
        Index('idx_daily_asset_date', 'asset_id', 'date'),
        Index('idx_daily_date_asset', 'date', 'asset_id'),
        Index('idx_daily_date_change', 'date', 'change_percent'),
    )


class DailyChange(Base):
    """Pre-calculated daily changes for efficient querying."""
    __tablename__ = 'daily_changes'
    
    id = Column(Integer, primary_key=True)
    asset_id = Column(Integer, ForeignKey('assets.id'), nullable=False, index=True)
    date = Column(Date, nullable=False, index=True)
    asset_class = Column(String(50), nullable=False, index=True)
    
    # Daily change metrics
    price_change = Column(Float)
    price_change_percent = Column(Float)
    volume_change = Column(Float)
    volume_change_percent = Column(Float)
    turnover_change = Column(Float)
    turnover_change_percent = Column(Float)
    
    # Ranking within asset class
    rank_by_volume = Column(Integer)
    rank_by_turnover = Column(Integer)
    rank_by_change = Column(Integer)
    
    # Market context
    market_status = Column(String(20))  # bull, bear, flat
    relative_strength = Column(Float)  # vs market average
    
    # Metadata
    calculated_at = Column(DateTime, default=datetime.utcnow)
    
    __table_args__ = (
        Index('idx_change_date_class', 'date', 'asset_class'),
        Index('idx_change_asset_date', 'asset_id', 'date'),
    )


class Alert(Base):
    """System alerts for unusual market activity."""
    __tablename__ = 'alerts'
    
    id = Column(Integer, primary_key=True)
    asset_id = Column(Integer, ForeignKey('assets.id'), nullable=False, index=True)
    alert_type = Column(String(50), nullable=False, index=True)
    severity = Column(String(20), default='info')  # info, warning, critical
    
    # Alert details
    title = Column(String(200), nullable=False)
    description = Column(Text)
    triggered_value = Column(Float)
    threshold_value = Column(Float)
    
    # Status
    is_active = Column(Boolean, default=True)
    triggered_at = Column(DateTime, default=datetime.utcnow)
    acknowledged_at = Column(DateTime)
    acknowledged_by = Column(String(100))
    resolved_at = Column(DateTime)
    
    # Snapshot reference
    snapshot_id = Column(Integer, ForeignKey('market_snapshots.id'))
    
    # Metadata
    notification_sent = Column(Boolean, default=False)
    notification_channels = Column(JSON)
    
    __table_args__ = (
        Index('idx_alert_active_type', 'is_active', 'alert_type'),
        Index('idx_alert_triggered', 'triggered_at'),
    )


class AssetClassSummary(Base):
    """Daily summary statistics by asset class."""
    __tablename__ = 'asset_class_summaries'
    
    id = Column(Integer, primary_key=True)
    date = Column(Date, nullable=False, index=True)
    asset_class = Column(String(50), nullable=False, index=True)
    
    # Market statistics
    total_count = Column(Integer)
    active_count = Column(Integer)
    
    # Volume stats
    total_volume = Column(Float)
    total_turnover = Column(Float)
    avg_volume = Column(Float)
    avg_turnover = Column(Float)
    
    # Performance stats
    advancers = Column(Integer)
    decliners = Column(Integer)
    unchanged = Column(Integer)
    
    avg_change_percent = Column(Float)
    median_change_percent = Column(Float)
    max_change_percent = Column(Float)
    min_change_percent = Column(Float)
    
    # Top performers
    top_gainer_symbol = Column(String(20))
    top_gainer_change = Column(Float)
    top_loser_symbol = Column(String(20))
    top_loser_change = Column(Float)
    most_active_symbol = Column(String(20))
    most_active_volume = Column(Float)
    
    # Metadata
    calculated_at = Column(DateTime, default=datetime.utcnow)
    
    __table_args__ = (
        Index('idx_summary_date_class', 'date', 'asset_class'),
    )


class CollectionLog(Base):
    """Log of data collection runs."""
    __tablename__ = 'collection_logs'
    
    id = Column(Integer, primary_key=True)
    collection_type = Column(String(50), nullable=False)  # snapshot, daily, intraday
    asset_class = Column(String(50))
    started_at = Column(DateTime, default=datetime.utcnow)
    completed_at = Column(DateTime)
    status = Column(String(20), default='running')  # running, success, failed, partial
    
    # Statistics
    assets_processed = Column(Integer, default=0)
    assets_failed = Column(Integer, default=0)
    records_created = Column(Integer, default=0)
    
    # Error details
    error_message = Column(Text)
    stack_trace = Column(Text)
    
    # Metadata
    execution_time_ms = Column(Integer)
    triggered_by = Column(String(50), default='scheduler')  # scheduler, manual, api


# Database initialization
def init_database(db_path='data/hkex_monitor.db'):
    """Initialize database with all tables."""
    # Ensure directory exists
    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    
    engine = create_engine(f'sqlite:///{db_path}')
    Base.metadata.create_all(engine)
    return engine


def get_session_maker(engine):
    """Get session maker bound to engine."""
    return sessionmaker(bind=engine)


if __name__ == '__main__':
    # Initialize database
    engine = init_database()
    print("Database initialized successfully!")
    print(f"Tables created: {list(Base.metadata.tables.keys())}")
