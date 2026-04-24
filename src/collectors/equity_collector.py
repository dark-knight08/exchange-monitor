"""
HKEX Equity data collector.
Fetches stock data from HKEX and other sources.
"""

import json
import asyncio
from typing import List, Dict, Any
from datetime import datetime
from .base import BaseCollector, MarketData


class EquityCollector(BaseCollector):
    """Collector for Hong Kong listed equities."""
    
    # Top 50 most liquid HKEX equities by typical turnover
    TOP_EQUITIES = [
        {"symbol": "0700.HK", "name": "Tencent Holdings", "sector": "Technology"},
        {"symbol": "0005.HK", "name": "HSBC Holdings", "sector": "Financials"},
        {"symbol": "9988.HK", "name": "Alibaba Group", "sector": "Consumer"},
        {"symbol": "1299.HK", "name": "AIA Group", "sector": "Financials"},
        {"symbol": "3690.HK", "name": "Meituan", "sector": "Consumer"},
        {"symbol": "1211.HK", "name": "BYD Company", "sector": "Consumer"},
        {"symbol": "0941.HK", "name": "China Mobile", "sector": "Telecom"},
        {"symbol": "2318.HK", "name": "Ping An Insurance", "sector": "Financials"},
        {"symbol": "1398.HK", "name": "ICBC", "sector": "Financials"},
        {"symbol": "3988.HK", "name": "Bank of China", "sector": "Financials"},
        {"symbol": "1810.HK", "name": "Xiaomi", "sector": "Technology"},
        {"symbol": "2269.HK", "name": "WuXi Biologics", "sector": "Healthcare"},
        {"symbol": "3968.HK", "name": "CM Bank", "sector": "Financials"},
        {"symbol": "2007.HK", "name": "Country Garden", "sector": "Property"},
        {"symbol": "2382.HK", "name": "Sunny Optical", "sector": "Technology"},
        {"symbol": "1109.HK", "name": "China Resources Land", "sector": "Property"},
        {"symbol": "0883.HK", "name": "CNOOC", "sector": "Energy"},
        {"symbol": "0001.HK", "name": "CK Hutchison", "sector": "Conglomerate"},
        {"symbol": "0011.HK", "name": "Hang Seng Bank", "sector": "Financials"},
        {"symbol": "0002.HK", "name": "CLP Holdings", "sector": "Utilities"},
        {"symbol": "0003.HK", "name": "HK & China Gas", "sector": "Utilities"},
        {"symbol": "0006.HK", "name": "Power Assets", "sector": "Utilities"},
        {"symbol": "1038.HK", "name": "CK Infrastructure", "sector": "Utilities"},
        {"symbol": "0168.HK", "name": "Tsingtao Brewery", "sector": "Consumer"},
        {"symbol": "0004.HK", "name": " Wharf Holdings", "sector": "Property"},
        {"symbol": "1928.HK", "name": "Sands China", "sector": "Consumer"},
        {"symbol": "2319.HK", "name": "China Mengniu", "sector": "Consumer"},
        {"symbol": "0175.HK", "name": "Geely Auto", "sector": "Consumer"},
        {"symbol": "0288.HK", "name": "WH Group", "sector": "Consumer"},
        {"symbol": "0003.HK", "name": "HK & China Gas", "sector": "Utilities"},
        {"symbol": "2015.HK", "name": "Li Ning", "sector": "Consumer"},
        {"symbol": "0001.HK", "name": "CK Hutchison", "sector": "Conglomerate"},
        {"symbol": "0270.HK", "name": "Guangdong Investment", "sector": "Utilities"},
        {"symbol": "0357.HK", "name": "Meitu", "sector": "Technology"},
        {"symbol": "0123.HK", "name": "Yuexiu Property", "sector": "Property"},
        {"symbol": "0151.HK", "name": "Want Want China", "sector": "Consumer"},
        {"symbol": "0189.HK", "name": "Dongfeng Motor", "sector": "Consumer"},
        {"symbol": "0233.HK", "name": "Sino Land", "sector": "Property"},
        {"symbol": "0293.HK", "name": "Cathay Pacific", "sector": "Transport"},
        {"symbol": "0322.HK", "name": "Tingyi", "sector": "Consumer"},
        {"symbol": "0384.HK", "name": "China Gas", "sector": "Utilities"},
        {"symbol": "0494.HK", "name": "Li & Fung", "sector": "Commercial"},
        {"symbol": "0636.HK", "name": "Kerry Logistics", "sector": "Transport"},
        {"symbol": "0669.HK", "name": "Techtronic", "sector": "Industrial"},
        {"symbol": "0688.HK", "name": "China Overseas", "sector": "Property"},
        {"symbol": "0728.HK", "name": "China Tower", "sector": "Telecom"},
        {"symbol": "0762.HK", "name": "China Unicom", "sector": "Telecom"},
        {"symbol": "0817.HK", "name": "China Jinmao", "sector": "Property"},
        {"symbol": "0836.HK", "name": "China Resources Power", "sector": "Utilities"},
        {"symbol": "0857.HK", "name": "PetroChina", "sector": "Energy"},
        {"symbol": "0868.HK", "name": "Xinyi Glass", "sector": "Industrial"},
    ]
    
    def get_asset_class(self) -> str:
        return "equity"
    
    async def fetch_top_liquid(self, limit: int = 50) -> List[MarketData]:
        """Fetch top liquid equities with simulated market data."""
        equities = self.TOP_EQUITIES[:limit]
        
        # In production, this would call actual APIs
        # For now, simulate realistic market data
        data = []
        for eq in equities:
            # Generate realistic base prices and metrics
            base_data = self._generate_market_data(eq)
            data.append(base_data)
        
        # Sort by turnover (descending)
        data.sort(key=lambda x: x.turnover or 0, reverse=True)
        return data
    
    async def fetch_snapshot(self, symbols: List[str]) -> List[MarketData]:
        """Fetch current snapshot for specific symbols."""
        symbol_map = {eq["symbol"]: eq for eq in self.TOP_EQUITIES}
        
        data = []
        for symbol in symbols:
            if symbol in symbol_map:
                eq = symbol_map[symbol]
                base_data = self._generate_market_data(eq)
                data.append(base_data)
        
        return data
    
    def _generate_market_data(self, eq: Dict[str, Any]) -> MarketData:
        """Generate realistic market data for an equity."""
        import random
        
        # Base prices by symbol (approximate ranges)
        base_prices = {
            "0700.HK": 412, "0005.HK": 68, "9988.HK": 88, "1299.HK": 58,
            "3690.HK": 143, "1211.HK": 279, "0941.HK": 72, "2318.HK": 46,
        }
        
        base_price = base_prices.get(eq["symbol"], random.uniform(10, 200))
        
        # Generate realistic change (-5% to +5%)
        change_percent = random.uniform(-5, 5)
        price = base_price * (1 + change_percent / 100)
        change = price - base_price
        
        # Generate volume based on liquidity (Tencent has highest)
        liquidity_multipliers = {
            "0700.HK": 20, "0005.HK": 15, "9988.HK": 12, "1299.HK": 8,
            "3690.HK": 6, "1211.HK": 4, "0941.HK": 5, "2318.HK": 3,
        }
        multiplier = liquidity_multipliers.get(eq["symbol"], 1)
        volume = int(random.uniform(50000000, 500000000) * multiplier)
        turnover = volume * price
        
        # Extended metrics
        market_cap = price * random.uniform(1000000000, 10000000000)
        pe_ratio = random.uniform(8, 35)
        dividend_yield = random.uniform(0.5, 6.0)
        
        return MarketData(
            symbol=eq["symbol"],
            name=eq["name"],
            asset_class="equity",
            price=round(price, 2),
            open=round(base_price * random.uniform(0.995, 1.005), 2),
            high=round(max(price, base_price) * random.uniform(1.0, 1.03), 2),
            low=round(min(price, base_price) * random.uniform(0.97, 1.0), 2),
            previous_close=round(base_price, 2),
            change=round(change, 2),
            change_percent=round(change_percent, 2),
            volume=volume,
            turnover=round(turnover, 2),
            extended={
                "market_cap": round(market_cap, 2),
                "pe_ratio": round(pe_ratio, 2),
                "dividend_yield": round(dividend_yield, 2),
                "sector": eq.get("sector", "Unknown"),
                "52_week_high": round(price * random.uniform(1.1, 1.5), 2),
                "52_week_low": round(price * random.uniform(0.5, 0.9), 2),
                "avg_volume_30d": int(volume * random.uniform(0.8, 1.2)),
                "beta": round(random.uniform(0.5, 1.5), 2),
            },
            data_source="hkex_simulated"
        )


# For testing
if __name__ == "__main__":
    async def test():
        collector = EquityCollector()
        async with collector:
            data = await collector.fetch_top_liquid(10)
            for d in data:
                print(f"{d.symbol}: {d.name} - ${d.price} ({d.change_percent}%)")
    
    asyncio.run(test())
