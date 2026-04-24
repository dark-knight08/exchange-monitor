"""
Base collector class for HKEX market data.
"""

from abc import ABC, abstractmethod
from typing import List, Dict, Any, Optional
from datetime import datetime
import logging
import requests
import aiohttp
import asyncio
from dataclasses import dataclass

logger = logging.getLogger(__name__)


@dataclass
class MarketData:
    """Standardized market data structure."""
    symbol: str
    name: str
    asset_class: str
    
    # Price data
    price: float
    open: Optional[float] = None
    high: Optional[float] = None
    low: Optional[float] = None
    previous_close: Optional[float] = None
    
    # Change data
    change: Optional[float] = None
    change_percent: Optional[float] = None
    
    # Volume data
    volume: Optional[float] = None
    turnover: Optional[float] = None
    
    # Extended data (asset class specific)
    extended: Dict[str, Any] = None
    
    # Metadata
    timestamp: datetime = None
    data_source: str = ""
    
    def __post_init__(self):
        if self.timestamp is None:
            self.timestamp = datetime.utcnow()
        if self.extended is None:
            self.extended = {}


class BaseCollector(ABC):
    """Abstract base class for market data collectors."""
    
    def __init__(self, config: Dict[str, Any] = None):
        self.config = config or {}
        self.logger = logging.getLogger(self.__class__.__name__)
        self.session: Optional[aiohttp.ClientSession] = None
        
    async def __aenter__(self):
        """Async context manager entry."""
        timeout = aiohttp.ClientTimeout(total=30)
        self.session = aiohttp.ClientSession(timeout=timeout)
        return self
        
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit."""
        if self.session:
            await self.session.close()
            self.session = None
    
    @abstractmethod
    def get_asset_class(self) -> str:
        """Return the asset class this collector handles."""
        pass
    
    @abstractmethod
    async def fetch_top_liquid(self, limit: int = 50) -> List[MarketData]:
        """Fetch top liquid products for this asset class."""
        pass
    
    @abstractmethod
    async def fetch_snapshot(self, symbols: List[str]) -> List[MarketData]:
        """Fetch current snapshot for given symbols."""
        pass
    
    async def fetch_all(self) -> List[MarketData]:
        """Fetch all available data for this asset class."""
        return await self.fetch_top_liquid()
    
    def _make_request(self, url: str, headers: Dict = None, params: Dict = None) -> Dict:
        """Make synchronous HTTP request."""
        try:
            response = requests.get(url, headers=headers, params=params, timeout=30)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            self.logger.error(f"Request failed: {url} - {e}")
            raise
    
    async def _make_async_request(self, url: str, headers: Dict = None, params: Dict = None) -> Dict:
        """Make asynchronous HTTP request."""
        if not self.session:
            raise RuntimeError("Session not initialized. Use async context manager.")
        
        try:
            async with self.session.get(url, headers=headers, params=params) as response:
                response.raise_for_status()
                return await response.json()
        except Exception as e:
            self.logger.error(f"Async request failed: {url} - {e}")
            raise


class MultiAssetCollector:
    """Manager for multiple asset class collectors."""
    
    def __init__(self):
        self.collectors: Dict[str, BaseCollector] = {}
        self.logger = logging.getLogger(self.__class__.__name__)
    
    def register(self, collector: BaseCollector):
        """Register a collector for an asset class."""
        asset_class = collector.get_asset_class()
        self.collectors[asset_class] = collector
        self.logger.info(f"Registered collector for {asset_class}")
    
    async def fetch_all(self, asset_classes: List[str] = None) -> Dict[str, List[MarketData]]:
        """Fetch data for all registered or specified asset classes."""
        classes = asset_classes or list(self.collectors.keys())
        results = {}
        
        tasks = []
        for cls in classes:
            if cls in self.collectors:
                collector = self.collectors[cls]
                tasks.append(self._fetch_with_context(collector, cls))
            else:
                self.logger.warning(f"No collector registered for {cls}")
        
        results_list = await asyncio.gather(*tasks, return_exceptions=True)
        
        for cls, result in zip(classes, results_list):
            if isinstance(result, Exception):
                self.logger.error(f"Failed to fetch {cls}: {result}")
                results[cls] = []
            else:
                results[cls] = result
        
        return results
    
    async def _fetch_with_context(self, collector: BaseCollector, asset_class: str):
        """Fetch data with collector context manager."""
        async with collector:
            return await collector.fetch_all()
    
    async def fetch_snapshots(self, symbols_by_class: Dict[str, List[str]]) -> Dict[str, List[MarketData]]:
        """Fetch snapshots for specific symbols by asset class."""
        results = {}
        
        tasks = []
        for cls, symbols in symbols_by_class.items():
            if cls in self.collectors:
                collector = self.collectors[cls]
                tasks.append(self._fetch_snapshot_with_context(collector, symbols, cls))
        
        results_list = await asyncio.gather(*tasks, return_exceptions=True)
        
        for (cls, _), result in zip(symbols_by_class.items(), results_list):
            if isinstance(result, Exception):
                self.logger.error(f"Failed to fetch snapshot for {cls}: {result}")
                results[cls] = []
            else:
                results[cls] = result
        
        return results
    
    async def _fetch_snapshot_with_context(self, collector: BaseCollector, symbols: List[str], asset_class: str):
        """Fetch snapshot with collector context manager."""
        async with collector:
            return await collector.fetch_snapshot(symbols)
