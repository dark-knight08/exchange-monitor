"""
HKEX Futures data collector.
Separates futures from options for clearer asset class organization.
"""

from typing import List, Dict, Any
from datetime import datetime, timedelta
from .base import BaseCollector, MarketData


class FuturesCollector(BaseCollector):
    """Collector for HKEX futures contracts."""
    
    # Major HKEX futures contracts
    FUTURES_CONTRACTS = [
        {"code": "HSI", "name": "Hang Seng Index Futures", "contract_size": 50, "tick_size": 1},
        {"code": "HHI", "name": "HSCEI Futures", "contract_size": 50, "tick_size": 1},
        {"code": "MHI", "name": "Mini-HSI Futures", "contract_size": 10, "tick_size": 1},
        {"code": "MCH", "name": "Mini-HSCEI Futures", "contract_size": 10, "tick_size": 1},
        {"code": "HTI", "name": "Hang Seng Tech Index Futures", "contract_size": 50, "tick_size": 1},
        {"code": "VHS", "name": "HSI Volatility Index Futures", "contract_size": 1000, "tick_size": 0.01},
        {"code": "CUS", "name": "USD/CNH Futures", "contract_size": 100000, "tick_size": 0.0001},
        {"code": "EUA", "name": "EUR/USD Futures", "contract_size": 125000, "tick_size": 0.0001},
        {"code": "JPY", "name": "JPY/USD Futures", "contract_size": 12500000, "tick_size": 0.000001},
        {"code": "GBP", "name": "GBP/USD Futures", "contract_size": 62500, "tick_size": 0.0001},
        {"code": "AUD", "name": "AUD/USD Futures", "contract_size": 100000, "tick_size": 0.0001},
        {"code": "CAD", "name": "USD/CAD Futures", "contract_size": 100000, "tick_size": 0.0001},
    ]
    
    def get_asset_class(self) -> str:
        return "future"
    
    async def fetch_top_liquid(self, limit: int = 20) -> List[MarketData]:
        """Fetch top liquid futures."""
        data = []
        
        for contract in self.FUTURES_CONTRACTS[:limit]:
            data.append(self._generate_futures_data(contract))
        
        # Sort by open interest (most liquid indicator)
        data.sort(key=lambda x: x.extended.get("open_interest", 0), reverse=True)
        return data[:limit]
    
    async def fetch_snapshot(self, symbols: List[str]) -> List[MarketData]:
        """Fetch snapshot for specific futures symbols."""
        symbol_map = {f"{c['code']}-{self._get_expiry()}": c for c in self.FUTURES_CONTRACTS}
        
        data = []
        for symbol in symbols:
            if symbol in symbol_map:
                data.append(self._generate_futures_data(symbol_map[symbol]))
        
        return data
    
    def _get_expiry(self) -> str:
        """Generate typical expiry month (near month)."""
        today = datetime.now()
        expiry = today + timedelta(days=30)
        return expiry.strftime("%Y%m")
    
    def _generate_futures_data(self, contract: Dict[str, Any]) -> MarketData:
        """Generate realistic futures market data."""
        import random
        
        # Base index values
        base_values = {
            "HSI": 16200, "HHI": 5600, "MHI": 16200, "MCH": 5600, 
            "HTI": 3800, "VHS": 25, "CUS": 7.2, "EUA": 1.08,
            "JPY": 0.0067, "GBP": 1.26, "AUD": 0.66, "CAD": 1.36
        }
        base = base_values.get(contract["code"], 100)
        
        # Generate price movement
        change_percent = random.uniform(-2, 2)
        price = base * (1 + change_percent / 100)
        
        # Generate expiry
        today = datetime.now()
        expiry_months = [0, 1, 2, 3]
        expiry = today + timedelta(days=30 * random.choice(expiry_months))
        expiry_str = expiry.strftime("%Y%m")
        
        # Volume and open interest
        contract_size = contract["contract_size"]
        notional_value = price * contract_size
        
        # HSI has highest liquidity
        liquidity_mult = {"HSI": 10, "HHI": 5, "MHI": 3, "MCH": 2, "HTI": 2}.get(contract["code"], 1)
        volume = int(random.uniform(5000, 50000) * liquidity_mult)
        open_interest = int(volume * random.uniform(2, 10))
        turnover = volume * notional_value
        
        symbol = f"{contract['code']}-{expiry_str}"
        
        return MarketData(
            symbol=symbol,
            name=f"{contract['name']} ({expiry_str})",
            asset_class="future",
            price=round(price, 4),
            open=round(base * random.uniform(0.995, 1.005), 4),
            high=round(max(price, base) * random.uniform(1.0, 1.015), 4),
            low=round(min(price, base) * random.uniform(0.985, 1.0), 4),
            previous_close=round(base, 4),
            change=round(price - base, 4),
            change_percent=round(change_percent, 2),
            volume=volume,
            turnover=round(turnover, 2),
            extended={
                "contract_code": contract["code"],
                "expiry_month": expiry_str,
                "contract_size": contract_size,
                "notional_value": round(notional_value, 2),
                "tick_size": contract["tick_size"],
                "tick_value": round(contract["tick_size"] * contract_size, 2),
                "open_interest": open_interest,
                "implied_volatility": round(random.uniform(15, 35), 2),
                "settlement_price": round(price * random.uniform(0.999, 1.001), 4),
                "oi_change": int(open_interest * random.uniform(-0.1, 0.1)),
                "basis": round(random.uniform(-50, 50), 2),
                "product_type": "Future"
            },
            data_source="hkex_futures_simulated"
        )


if __name__ == "__main__":
    import asyncio
    
    async def test():
        collector = FuturesCollector()
        async with collector:
            data = await collector.fetch_top_liquid(5)
            for d in data:
                oi = d.extended.get("open_interest", 0)
                print(f"{d.symbol}: {d.name} - ${d.price} | OI: {oi:,}")
    
    asyncio.run(test())
