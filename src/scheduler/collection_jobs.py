"""
Collection jobs for different asset classes and time intervals.
"""

import asyncio
import logging
from datetime import datetime, date
from typing import List, Dict, Any

from ..collectors import (
    MultiAssetCollector, EquityCollector, ETFCollector,
    DerivativeCollector, CBBCWarrantCollector, FuturesCollector, OptionsCollector,
    MarketData
)
from ..database.data_store import DataStore
from ..database.models import CollectionLog
from ..alerts.alert_engine import AlertEngine

logger = logging.getLogger(__name__)


class CollectionJobs:
    """Manages data collection jobs."""
    
    def __init__(self, data_store: DataStore, alert_engine: AlertEngine = None):
        self.data_store = data_store
        self.alert_engine = alert_engine or AlertEngine(data_store)
        self.collector_manager = MultiAssetCollector()
        
        # Register collectors
        self.collector_manager.register(EquityCollector())
        self.collector_manager.register(ETFCollector())
        self.collector_manager.register(FuturesCollector())
        self.collector_manager.register(OptionsCollector())
        self.collector_manager.register(DerivativeCollector())  # Keep for backwards compatibility
        self.collector_manager.register(CBBCWarrantCollector())
        
        logger.info("CollectionJobs initialized")
    
    async def collect_realtime_snapshot(self, asset_classes: List[str] = None):
        """Collect real-time market snapshot."""
        start_time = datetime.utcnow()
        
        # Map asset classes to collector names
        if asset_classes:
            # Handle 'all' keyword
            if 'all' in asset_classes:
                classes = ["equity", "etf", "future", "option", "cbbc_warrant"]
            else:
                # Map asset classes to their collectors
                class_map = {
                    'cbbc': 'cbbc_warrant',
                    'warrant': 'cbbc_warrant',
                    'derivative': 'future',  # Default derivative to futures for now
                    'futures': 'future',
                    'options': 'option'
                }
                classes = list(set(class_map.get(c, c) for c in asset_classes))
        else:
            classes = ["equity", "etf", "future", "option", "cbbc_warrant"]
        
        log = CollectionLog(
            collection_type="realtime_snapshot",
            started_at=start_time,
            status="running"
        )
        
        try:
            # Fetch data
            results = await self.collector_manager.fetch_all(classes)
            
            total_records = 0
            for asset_class, data in results.items():
                if data:
                    # Convert to dict format
                    snapshots = [self._market_data_to_dict(d) for d in data]
                    
                    # Save to database
                    count = self.data_store.save_snapshots_batch(
                        snapshots, 
                        snapshot_type="realtime"
                    )
                    total_records += count or 0
                    
                    log.assets_processed += len(data)
                    
                    logger.info(f"Collected {len(data)} {asset_class} snapshots")
            
            # Check for alerts
            if self.alert_engine:
                await self.alert_engine.check_all_alerts()
            
            log.status = "success"
            log.records_created = total_records
            
        except Exception as e:
            logger.error(f"Collection failed: {e}")
            log.status = "failed"
            log.error_message = str(e)
        
        finally:
            log.completed_at = datetime.utcnow()
            log.execution_time_ms = int(
                (log.completed_at - start_time).total_seconds() * 1000
            )
            
            # Save log
            session = self.data_store.Session()
            try:
                session.add(log)
                session.commit()
            finally:
                session.close()
    
    async def collect_daily_close(self):
        """Collect end-of-day data for all assets."""
        start_time = datetime.utcnow()
        today = date.today()
        
        log = CollectionLog(
            collection_type="daily_close",
            started_at=start_time,
            status="running"
        )
        
        try:
            # Collect all asset classes
            classes = ["equity", "etf", "derivative", "cbbc", "warrant"]
            results = await self.collector_manager.fetch_all(classes)
            
            total_records = 0
            for asset_class, data in results.items():
                for market_data in data:
                    # Save as daily bar
                    symbol = market_data.symbol
                    data_dict = self._market_data_to_dict(market_data)
                    
                    self.data_store.save_daily_bar(symbol, today, data_dict)
                    total_records += 1
                
                log.assets_processed += len(data)
                logger.info(f"Saved {len(data)} {asset_class} daily bars")
            
            # Calculate daily changes
            self.data_store.calculate_daily_changes(today)
            
            # Calculate asset class summaries
            self.data_store.calculate_asset_class_summary(today)
            
            # Run end-of-day alerts
            if self.alert_engine:
                await self.alert_engine.check_daily_summary_alerts()
            
            log.status = "success"
            log.records_created = total_records
            
        except Exception as e:
            logger.error(f"Daily collection failed: {e}")
            log.status = "failed"
            log.error_message = str(e)
        
        finally:
            log.completed_at = datetime.utcnow()
            log.execution_time_ms = int(
                (log.completed_at - start_time).total_seconds() * 1000
            )
            
            session = self.data_store.Session()
            try:
                session.add(log)
                session.commit()
            finally:
                session.close()
    
    async def collect_single_asset_class(self, asset_class: str, limit: int = 50):
        """Collect data for a single asset class."""
        start_time = datetime.utcnow()
        
        log = CollectionLog(
            collection_type=f"{asset_class}_snapshot",
            asset_class=asset_class,
            started_at=start_time,
            status="running"
        )
        
        try:
            # Get collector for this asset class
            if asset_class not in self.collector_manager.collectors:
                raise ValueError(f"No collector for asset class: {asset_class}")
            
            collector = self.collector_manager.collectors[asset_class]
            
            async with collector:
                data = await collector.fetch_top_liquid(limit)
            
            # Save snapshots
            snapshots = [self._market_data_to_dict(d) for d in data]
            count = self.data_store.save_snapshots_batch(snapshots, "intraday")
            
            log.assets_processed = len(data)
            log.records_created = count
            log.status = "success"
            
            logger.info(f"Collected {len(data)} {asset_class} records")
            
        except Exception as e:
            logger.error(f"Collection failed for {asset_class}: {e}")
            log.status = "failed"
            log.error_message = str(e)
        
        finally:
            log.completed_at = datetime.utcnow()
            log.execution_time_ms = int(
                (log.completed_at - start_time).total_seconds() * 1000
            )
            
            session = self.data_store.Session()
            try:
                session.add(log)
                session.commit()
            finally:
                session.close()
    
    def _market_data_to_dict(self, data: MarketData) -> Dict[str, Any]:
        """Convert MarketData to dictionary for storage."""
        return {
            "symbol": data.symbol,
            "name": data.name,
            "asset_class": data.asset_class,
            "price": data.price,
            "open": data.open,
            "high": data.high,
            "low": data.low,
            "previous_close": data.previous_close,
            "change": data.change,
            "change_percent": data.change_percent,
            "volume": data.volume,
            "turnover": data.turnover,
            "timestamp": data.timestamp,
            "extended": data.extended,
        }


if __name__ == "__main__":
    # Test collection
    from ..database.models import init_database
    
    init_database()
    
    store = DataStore()
    jobs = CollectionJobs(store)
    
    # Run collection
    asyncio.run(jobs.collect_realtime_snapshot())
