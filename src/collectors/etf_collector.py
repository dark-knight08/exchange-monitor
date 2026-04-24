"""
HKEX ETF data collector.
Fetches ETF data including NAV, AUM, and tracking metrics.
"""

from typing import List, Dict, Any
from datetime import datetime
from .base import BaseCollector, MarketData


class ETFCollector(BaseCollector):
    """Collector for Hong Kong listed ETFs."""
    
    # Top HKEX ETFs by typical turnover and AUM
    TOP_ETFS = [
        {"symbol": "2800.HK", "name": "Tracker Fund of Hong Kong", "underlying": "HSI", "aum_billions": 25},
        {"symbol": "2828.HK", "name": "HSCEI ETF", "underlying": "HSCEI", "aum_billions": 8},
        {"symbol": "3188.HK", "name": "CSOP CSI 300 ETF", "underlying": "CSI300", "aum_billions": 15},
        {"symbol": "3033.HK", "name": "CSOP HSCEI ETF", "underlying": "HSCEI", "aum_billions": 4},
        {"symbol": "2833.HK", "name": "Hang Seng H-Share ETF", "underlying": "HSCEI", "aum_billions": 3},
        {"symbol": "3008.HK", "name": "Global X China Biotech ETF", "underlying": "Sector", "aum_billions": 1},
        {"symbol": "2840.HK", "name": "SPDR Gold Shares", "underlying": "Gold", "aum_billions": 12},
        {"symbol": "3118.HK", "name": "Global X Hang Seng High Div Yield ETF", "underlying": "Strategy", "aum_billions": 0.5},
        {"symbol": "3081.HK", "name": "Value China ETF", "underlying": "CSI300", "aum_billions": 0.3},
        {"symbol": "2836.HK", "name": "Hang Seng Index ETF", "underlying": "HSI", "aum_billions": 1.5},
        {"symbol": "3167.HK", "name": "ChinaAMC MSCI China A 50 ETF", "underlying": "A50", "aum_billions": 0.8},
        {"symbol": "3041.HK", "name": "Global X Hang Seng Tech ETF", "underlying": "HSTECH", "aum_billions": 0.6},
        {"symbol": "3097.HK", "name": "Fubon FTSE Taiwan ETF", "underlying": "TWSE", "aum_billions": 0.4},
        {"symbol": "3010.HK", "name": "Premia China Treasury 7-10y ETF", "underlying": "Bonds", "aum_billions": 0.3},
        {"symbol": "3122.HK", "name": "Global X Asia Semiconductor ETF", "underlying": "Sector", "aum_billions": 0.2},
        {"symbol": "3091.HK", "name": "Global X China EV ETF", "underlying": "Sector", "aum_billions": 0.15},
        {"symbol": "3109.HK", "name": "Global X China Clean Energy ETF", "underlying": "Sector", "aum_billions": 0.1},
        {"symbol": "3192.HK", "name": "Global X China Innovation ETF", "underlying": "Theme", "aum_billions": 0.1},
        {"symbol": "3007.K", "name": "Global X MSCI China ETF", "underlying": "MSCI China", "aum_billions": 0.2},
        {"symbol": "3110.HK", "name": "Global X China Cloud Computing ETF", "underlying": "Sector", "aum_billions": 0.1},
        {"symbol": "3073.HK", "name": "Global X China Semicon ETF", "underlying": "Sector", "aum_billions": 0.15},
        {"symbol": "3099.HK", "name": "Global X China Consumer ETF", "underlying": "Sector", "aum_billions": 0.12},
        {"symbol": "3131.HK", "name": "Global X China Healthcare ETF", "underlying": "Sector", "aum_billions": 0.1},
        {"symbol": "2847.HK", "name": "Hang Seng Index ETF", "underlying": "HSI", "aum_billions": 0.3},
        {"symbol": "3058.HK", "name": "Global X China FinTech ETF", "underlying": "Sector", "aum_billions": 0.08},
    ]
    
    def get_asset_class(self) -> str:
        return "etf"
    
    async def fetch_top_liquid(self, limit: int = 30) -> List[MarketData]:
        """Fetch top liquid ETFs."""
        etfs = self.TOP_ETFS[:limit]
        data = [self._generate_market_data(etf) for etf in etfs]
        data.sort(key=lambda x: x.turnover or 0, reverse=True)
        return data
    
    async def fetch_snapshot(self, symbols: List[str]) -> List[MarketData]:
        """Fetch current snapshot for specific ETF symbols."""
        symbol_map = {etf["symbol"]: etf for etf in self.TOP_ETFS}
        return [self._generate_market_data(symbol_map[s]) for s in symbols if s in symbol_map]
    
    def _generate_market_data(self, etf: Dict[str, Any]) -> MarketData:
        """Generate realistic ETF market data."""
        import random
        
        # Base prices
        base_prices = {"2800.HK": 21.4, "2828.HK": 68.4, "3188.HK": 12.8, "3033.HK": 25.2, "2840.HK": 1650}
        base_price = base_prices.get(etf["symbol"], random.uniform(5, 100))
        
        # Generate change
        change_percent = random.uniform(-3, 3)
        price = base_price * (1 + change_percent / 100)
        
        # Volume based on AUM and liquidity
        aum = etf.get("aum_billions", 1)
        volume = int(random.uniform(1000000, 100000000) * aum)
        turnover = volume * price
        
        # NAV calculation (tracking error)
        nav = price * random.uniform(0.998, 1.002)
        tracking_error = abs(price - nav) / nav * 100
        
        return MarketData(
            symbol=etf["symbol"],
            name=etf["name"],
            asset_class="etf",
            price=round(price, 3),
            open=round(base_price * random.uniform(0.997, 1.003), 3),
            high=round(max(price, base_price) * random.uniform(1.0, 1.02), 3),
            low=round(min(price, base_price) * random.uniform(0.98, 1.0), 3),
            previous_close=round(base_price, 3),
            change=round(price - base_price, 3),
            change_percent=round(change_percent, 2),
            volume=volume,
            turnover=round(turnover, 2),
            extended={
                "nav": round(nav, 4),
                "aum_hkd_billions": round(aum * 7.8, 2),  # Approx USD to HKD
                "tracking_error_percent": round(tracking_error, 3),
                "expense_ratio": round(random.uniform(0.05, 0.99), 2),
                "underlying": etf.get("underlying", "Index"),
                "discount_premium": round((price - nav) / nav * 100, 3),
                "creation_units": int(volume / random.uniform(50000, 200000)),
                "dividend_yield": round(random.uniform(0, 4), 2),
            },
            data_source="hkex_etf_simulated"
        )


if __name__ == "__main__":
    import asyncio
    
    async def test():
        collector = ETFCollector()
        async with collector:
            data = await collector.fetch_top_liquid(10)
            for d in data:
                print(f"{d.symbol}: {d.name} - ${d.price} | NAV: ${d.extended.get('nav')} | AUM: ${d.extended.get('aum_hkd_billions')}B")
    
    asyncio.run(test())
