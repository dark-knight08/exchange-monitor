"""
Data store for persisting and retrieving market data.
"""

from typing import List, Dict, Any, Optional, Tuple
from datetime import datetime, date, timedelta
from sqlalchemy.orm import Session
from sqlalchemy import func, and_, desc, asc
import pandas as pd
import logging

from .models import (
    Asset, MarketSnapshot, DailyBar, DailyChange, 
    Alert, AssetClassSummary, CollectionLog
)
from . import init_database, get_session_maker

logger = logging.getLogger(__name__)


class DataStore:
    """Main data store for market data."""
    
    def __init__(self, db_path: str = 'data/hkex_monitor.db'):
        self.engine = init_database(db_path)
        self.Session = get_session_maker(self.engine)
        logger.info(f"DataStore initialized with database: {db_path}")
    
    # Asset Management
    def get_or_create_asset(self, symbol: str, name: str, asset_class: str, 
                           **kwargs) -> Asset:
        """Get existing asset or create new one."""
        session = self.Session()
        try:
            asset = session.query(Asset).filter(Asset.symbol == symbol).first()
            
            if not asset:
                asset = Asset(
                    symbol=symbol,
                    name=name,
                    asset_class=asset_class,
                    **kwargs
                )
                session.add(asset)
                session.commit()
                logger.info(f"Created new asset: {symbol} ({name})")
            
            # Detach from session so it can be used after session closes
            session.expunge(asset)
            return asset
        finally:
            session.close()
    
    def get_assets_by_class(self, asset_class: str, active_only: bool = True) -> List[Asset]:
        """Get all assets of a specific class."""
        session = self.Session()
        try:
            query = session.query(Asset).filter(Asset.asset_class == asset_class)
            if active_only:
                query = query.filter(Asset.is_active == True)
            assets = query.all()
            # Detach all assets from session
            for asset in assets:
                session.expunge(asset)
            return assets
        finally:
            session.close()
    
    # Snapshot Management
    def save_snapshot(self, symbol: str, data: Dict[str, Any], 
                     snapshot_type: str = 'realtime') -> MarketSnapshot:
        """Save a market snapshot."""
        session = self.Session()
        try:
            asset = session.query(Asset).filter(Asset.symbol == symbol).first()
            if not asset:
                raise ValueError(f"Asset not found: {symbol}")
            
            snapshot = MarketSnapshot(
                asset_id=asset.id,
                snapshot_time=data.get('timestamp', datetime.utcnow()),
                snapshot_type=snapshot_type,
                open=data.get('open'),
                high=data.get('high'),
                low=data.get('low'),
                close=data.get('price') or data.get('close'),
                previous_close=data.get('previous_close'),
                change=data.get('change'),
                change_percent=data.get('change_percent'),
                volume=data.get('volume'),
                turnover=data.get('turnover'),
                extended_metrics=data.get('extended', {})
            )
            
            session.add(snapshot)
            session.commit()
            
            logger.debug(f"Saved snapshot for {symbol}")
            return snapshot
        finally:
            session.close()
    
    def save_snapshots_batch(self, snapshots: List[Dict[str, Any]], 
                            snapshot_type: str = 'realtime') -> int:
        """Save multiple snapshots efficiently."""
        session = self.Session()
        count = 0
        try:
            for data in snapshots:
                symbol = data.get('symbol')
                asset = session.query(Asset).filter(Asset.symbol == symbol).first()
                
                if not asset:
                    # Create asset if not exists
                    asset = Asset(
                        symbol=symbol,
                        name=data.get('name', symbol),
                        asset_class=data.get('asset_class', 'unknown')
                    )
                    session.add(asset)
                    session.flush()  # Get asset.id
                
                snapshot = MarketSnapshot(
                    asset_id=asset.id,
                    snapshot_time=data.get('timestamp', datetime.utcnow()),
                    snapshot_type=snapshot_type,
                    open=data.get('open'),
                    high=data.get('high'),
                    low=data.get('low'),
                    close=data.get('price') or data.get('close'),
                    previous_close=data.get('previous_close'),
                    change=data.get('change'),
                    change_percent=data.get('change_percent'),
                    volume=data.get('volume'),
                    turnover=data.get('turnover'),
                    extended_metrics=data.get('extended', {})
                )
                
                session.add(snapshot)
                count += 1
            
            session.commit()
            logger.info(f"Saved {count} snapshots")
        except Exception as e:
            logger.error(f"Failed to save snapshots batch: {e}")
            session.rollback()
            raise
        finally:
            session.close()
        
        return count
    
    def get_latest_snapshots(self, asset_class: Optional[str] = None,
                            limit: int = 100) -> List[Dict]:
        """Get latest snapshots, optionally filtered by asset class."""
        session = self.Session()
        try:
            query = session.query(MarketSnapshot, Asset).join(Asset)
            
            if asset_class:
                query = query.filter(Asset.asset_class == asset_class)
            
            # Get latest per asset
            subquery = session.query(
                MarketSnapshot.asset_id,
                func.max(MarketSnapshot.snapshot_time).label('max_time')
            ).group_by(MarketSnapshot.asset_id).subquery()
            
            query = query.join(
                subquery,
                and_(
                    MarketSnapshot.asset_id == subquery.c.asset_id,
                    MarketSnapshot.snapshot_time == subquery.c.max_time
                )
            )
            
            results = query.limit(limit).all()
            
            return [{
                'symbol': r.Asset.symbol,
                'name': r.Asset.name,
                'asset_class': r.Asset.asset_class,
                'price': r.MarketSnapshot.close,
                'change': r.MarketSnapshot.change,
                'change_percent': r.MarketSnapshot.change_percent,
                'volume': r.MarketSnapshot.volume,
                'turnover': r.MarketSnapshot.turnover,
                'timestamp': r.MarketSnapshot.snapshot_time,
                'extended': r.MarketSnapshot.extended_metrics or {}
            } for r in results]
        finally:
            session.close()
    
    # Daily Bar Management
    def save_daily_bar(self, symbol: str, date_val: date, data: Dict[str, Any]) -> DailyBar:
        """Save end-of-day data."""
        session = self.Session()
        try:
            asset = session.query(Asset).filter(Asset.symbol == symbol).first()
            if not asset:
                raise ValueError(f"Asset not found: {symbol}")
            
            # Check if already exists
            existing = session.query(DailyBar).filter(
                and_(DailyBar.asset_id == asset.id, DailyBar.date == date_val)
            ).first()
            
            if existing:
                # Update existing
                for key, value in data.items():
                    if hasattr(existing, key) and key not in ['id', 'asset_id', 'date', 'created_at']:
                        setattr(existing, key, value)
                existing.created_at = datetime.utcnow()
            else:
                # Create new
                bar = DailyBar(
                    asset_id=asset.id,
                    date=date_val,
                    open=data.get('open'),
                    high=data.get('high'),
                    low=data.get('low'),
                    close=data.get('close') or data.get('price'),
                    volume=data.get('volume'),
                    turnover=data.get('turnover'),
                    change=data.get('change'),
                    change_percent=data.get('change_percent'),
                    market_cap=data.get('market_cap'),
                    pe_ratio=data.get('pe_ratio'),
                    dividend_yield=data.get('dividend_yield'),
                    open_interest=data.get('open_interest'),
                    implied_volatility=data.get('implied_volatility'),
                    delta=data.get('delta'),
                    gamma=data.get('gamma'),
                    theta=data.get('theta'),
                    vega=data.get('vega'),
                    gearing=data.get('gearing'),
                    premium=data.get('premium'),
                    call_level=data.get('call_level'),
                    entitlement_ratio=data.get('entitlement_ratio')
                )
                session.add(bar)
            
            session.commit()
            logger.debug(f"Saved daily bar for {symbol} on {date_val}")
            
            return existing if existing else bar
        finally:
            session.close()
    
    def get_daily_bars(self, symbol: str, start_date: date, 
                      end_date: Optional[date] = None) -> pd.DataFrame:
        """Get daily bars for a symbol over a date range."""
        session = self.Session()
        try:
            asset = session.query(Asset).filter(Asset.symbol == symbol).first()
            if not asset:
                return pd.DataFrame()
            
            end = end_date or date.today()
            
            bars = session.query(DailyBar).filter(
                and_(
                    DailyBar.asset_id == asset.id,
                    DailyBar.date >= start_date,
                    DailyBar.date <= end
                )
            ).order_by(DailyBar.date).all()
            
            if not bars:
                return pd.DataFrame()
            
            data = [{
                'date': b.date,
                'open': b.open,
                'high': b.high,
                'low': b.low,
                'close': b.close,
                'volume': b.volume,
                'turnover': b.turnover,
                'change': b.change,
                'change_percent': b.change_percent,
            } for b in bars]
            
            return pd.DataFrame(data)
        finally:
            session.close()
    
    # Daily Changes
    def calculate_daily_changes(self, target_date: Optional[date] = None) -> List[DailyChange]:
        """Calculate daily changes for all assets."""
        session = self.Session()
        try:
            target = target_date or date.today()
            yesterday = target - timedelta(days=1)
            
            # Get today's and yesterday's bars
            today_bars = session.query(DailyBar, Asset).join(Asset).filter(
                DailyBar.date == target
            ).all()
            
            yesterday_bars = session.query(DailyBar).filter(
                DailyBar.date == yesterday
            ).all()
            
            yesterday_map = {b.asset_id: b for b in yesterday_bars}
            
            changes = []
            for bar, asset in today_bars:
                yest_bar = yesterday_map.get(bar.asset_id)
                
                if yest_bar:
                    change = DailyChange(
                        asset_id=asset.id,
                        date=target,
                        asset_class=asset.asset_class,
                        price_change=bar.change,
                        price_change_percent=bar.change_percent,
                        volume_change=bar.volume - yest_bar.volume if yest_bar.volume else None,
                        volume_change_percent=((bar.volume - yest_bar.volume) / yest_bar.volume * 100) 
                                              if yest_bar.volume else None,
                        turnover_change=bar.turnover - yest_bar.turnover if yest_bar.turnover else None,
                    )
                    changes.append(change)
            
            # Bulk insert
            session.bulk_save_objects(changes)
            session.commit()
            
            logger.info(f"Calculated {len(changes)} daily changes for {target}")
            return changes
        finally:
            session.close()
    
    def get_daily_changes(self, target_date: Optional[date] = None,
                         asset_class: Optional[str] = None,
                         min_change: Optional[float] = None,
                         limit: int = 100) -> List[Dict]:
        """Get daily changes with filtering."""
        session = self.Session()
        try:
            target = target_date or date.today()
            
            query = session.query(DailyChange, Asset).join(Asset).filter(
                DailyChange.date == target
            )
            
            if asset_class:
                query = query.filter(DailyChange.asset_class == asset_class)
            
            if min_change is not None:
                query = query.filter(
                    func.abs(DailyChange.price_change_percent) >= min_change
                )
            
            query = query.order_by(desc(func.abs(DailyChange.price_change_percent)))
            
            results = query.limit(limit).all()
            
            return [{
                'symbol': r.Asset.symbol,
                'name': r.Asset.name,
                'asset_class': r.Asset.asset_class,
                'price_change': r.DailyChange.price_change,
                'price_change_percent': r.DailyChange.price_change_percent,
                'volume_change_percent': r.DailyChange.volume_change_percent,
                'turnover_change_percent': r.DailyChange.turnover_change_percent,
                'rank_by_change': r.DailyChange.rank_by_change,
            } for r in results]
        finally:
            session.close()
    
    # Alerts
    def create_alert(self, symbol: str, alert_type: str, severity: str,
                    title: str, description: str, triggered_value: float,
                    threshold_value: float) -> Alert:
        """Create a new alert."""
        session = self.Session()
        try:
            asset = session.query(Asset).filter(Asset.symbol == symbol).first()
            if not asset:
                raise ValueError(f"Asset not found: {symbol}")
            
            alert = Alert(
                asset_id=asset.id,
                alert_type=alert_type,
                severity=severity,
                title=title,
                description=description,
                triggered_value=triggered_value,
                threshold_value=threshold_value,
                is_active=True
            )
            
            session.add(alert)
            session.commit()
            
            logger.info(f"Created alert: {title} for {symbol}")
            return alert
        finally:
            session.close()
    
    def get_active_alerts(self, asset_class: Optional[str] = None,
                         severity: Optional[str] = None) -> List[Dict]:
        """Get active alerts."""
        session = self.Session()
        try:
            query = session.query(Alert, Asset).join(Asset).filter(Alert.is_active == True)
            
            if asset_class:
                query = query.filter(Asset.asset_class == asset_class)
            
            if severity:
                query = query.filter(Alert.severity == severity)
            
            query = query.order_by(desc(Alert.triggered_at))
            
            alerts = query.all()
            
            return [{
                'id': a.Alert.id,
                'symbol': a.Asset.symbol,
                'name': a.Asset.name,
                'asset_class': a.Asset.asset_class,
                'alert_type': a.Alert.alert_type,
                'severity': a.Alert.severity,
                'title': a.Alert.title,
                'description': a.Alert.description,
                'triggered_value': a.Alert.triggered_value,
                'threshold_value': a.Alert.threshold_value,
                'triggered_at': a.Alert.triggered_at,
            } for a in alerts]
        finally:
            session.close()
    
    # Asset Class Summary
    def calculate_asset_class_summary(self, target_date: Optional[date] = None) -> List[AssetClassSummary]:
        """Calculate summary statistics by asset class."""
        session = self.Session()
        try:
            target = target_date or date.today()
            
            # Get all daily bars for the date
            bars = session.query(DailyBar, Asset).join(Asset).filter(
                DailyBar.date == target
            ).all()
            
            # Group by asset class
            by_class = {}
            for bar, asset in bars:
                if asset.asset_class not in by_class:
                    by_class[asset.asset_class] = []
                by_class[asset.asset_class].append((bar, asset))
            
            summaries = []
            for asset_class, items in by_class.items():
                changes = [b.change_percent for b, _ in items if b.change_percent is not None]
                volumes = [b.volume for b, _ in items if b.volume is not None]
                turnovers = [b.turnover for b, _ in items if b.turnover is not None]
                
                # Top performers
                sorted_by_change = sorted(items, key=lambda x: x[0].change_percent or 0, reverse=True)
                sorted_by_volume = sorted(items, key=lambda x: x[0].volume or 0, reverse=True)
                
                advancers = sum(1 for c in changes if c > 0)
                decliners = sum(1 for c in changes if c < 0)
                unchanged = len(changes) - advancers - decliners
                
                summary = AssetClassSummary(
                    date=target,
                    asset_class=asset_class,
                    total_count=len(items),
                    active_count=len(items),
                    total_volume=sum(volumes) if volumes else 0,
                    total_turnover=sum(turnovers) if turnovers else 0,
                    avg_volume=sum(volumes) / len(volumes) if volumes else 0,
                    avg_turnover=sum(turnovers) / len(turnovers) if turnovers else 0,
                    advancers=advancers,
                    decliners=decliners,
                    unchanged=unchanged,
                    avg_change_percent=sum(changes) / len(changes) if changes else 0,
                    median_change_percent=sorted(changes)[len(changes)//2] if changes else 0,
                    max_change_percent=max(changes) if changes else 0,
                    min_change_percent=min(changes) if changes else 0,
                    top_gainer_symbol=sorted_by_change[0][1].symbol if sorted_by_change else None,
                    top_gainer_change=sorted_by_change[0][0].change_percent if sorted_by_change else None,
                    top_loser_symbol=sorted_by_change[-1][1].symbol if sorted_by_change else None,
                    top_loser_change=sorted_by_change[-1][0].change_percent if sorted_by_change else None,
                    most_active_symbol=sorted_by_volume[0][1].symbol if sorted_by_volume else None,
                    most_active_volume=sorted_by_volume[0][0].volume if sorted_by_volume else None,
                )
                
                summaries.append(summary)
            
            session.bulk_save_objects(summaries)
            session.commit()
            
            logger.info(f"Calculated {len(summaries)} asset class summaries for {target}")
            return summaries
        finally:
            session.close()
    
    def get_asset_class_summary(self, target_date: Optional[date] = None) -> List[Dict]:
        """Get asset class summaries."""
        session = self.Session()
        try:
            target = target_date or date.today()
            
            summaries = session.query(AssetClassSummary).filter(
                AssetClassSummary.date == target
            ).all()
            
            return [{
                'asset_class': s.asset_class,
                'total_count': s.total_count,
                'advancers': s.advancers,
                'decliners': s.decliners,
                'unchanged': s.unchanged,
                'total_volume': s.total_volume,
                'total_turnover': s.total_turnover,
                'avg_change_percent': s.avg_change_percent,
                'top_gainer': s.top_gainer_symbol,
                'top_gainer_change': s.top_gainer_change,
                'top_loser': s.top_loser_symbol,
                'top_loser_change': s.top_loser_change,
                'most_active': s.most_active_symbol,
            } for s in summaries]
        finally:
            session.close()


if __name__ == '__main__':
    # Test the data store
    store = DataStore()
    
    # Create test assets
    assets = [
        ('0700.HK', 'Tencent Holdings', 'equity'),
        ('2800.HK', 'Tracker Fund', 'etf'),
    ]
    
    for symbol, name, asset_class in assets:
        asset = store.get_or_create_asset(symbol, name, asset_class)
        print(f"Asset: {asset.symbol} - {asset.name}")
