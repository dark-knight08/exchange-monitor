"""
HKEX Options data collector.
Separates options from futures for clearer asset class organization.
"""

from typing import List, Dict, Any
from datetime import datetime, timedelta
from .base import BaseCollector, MarketData


class OptionsCollector(BaseCollector):
    """Collector for HKEX stock and index options."""
    
    # Index options
    INDEX_OPTIONS = [
        {"underlying": "HSI", "name": "HSI Index Options", "multiplier": 50},
        {"underlying": "HHI", "name": "HSCEI Index Options", "multiplier": 50},
        {"underlying": "MHI", "name": "Mini-HSI Options", "multiplier": 10},
        {"underlying": "HTI", "name": "Hang Seng Tech Options", "multiplier": 50},
    ]
    
    # Stock options on liquid underlyings
    STOCK_OPTION_UNDERLYINGS = [
        {"code": "0700", "name": "Tencent", "price": 412},
        {"code": "0005", "name": "HSBC", "price": 68},
        {"code": "9988", "name": "Alibaba", "price": 88},
        {"code": "1299", "name": "AIA", "price": 58},
        {"code": "3690", "name": "Meituan", "price": 143},
        {"code": "1211", "name": "BYD", "price": 279},
        {"code": "0941", "name": "China Mobile", "price": 72},
        {"code": "2318", "name": "Ping An", "price": 46},
        {"code": "1398", "name": "ICBC", "price": 4.2},
        {"code": "3988", "name": "Bank of China", "price": 3.5},
    ]
    
    def get_asset_class(self) -> str:
        return "option"
    
    async def fetch_top_liquid(self, limit: int = 20) -> List[MarketData]:
        """Fetch top liquid options (index + stock)."""
        data = []
        
        # Generate index options
        for index_opt in self.INDEX_OPTIONS:
            data.extend(self._generate_index_options(index_opt))
        
        # Generate stock options for top underlyings
        for stock in self.STOCK_OPTION_UNDERLYINGS[:5]:
            data.extend(self._generate_stock_options(stock))
        
        # Sort by open interest
        data.sort(key=lambda x: x.extended.get("open_interest", 0), reverse=True)
        return data[:limit]
    
    async def fetch_snapshot(self, symbols: List[str]) -> List[MarketData]:
        """Fetch snapshot for specific option symbols."""
        data = []
        for symbol in symbols:
            # Parse symbol to extract underlying and type
            for stock in self.STOCK_OPTION_UNDERLYINGS:
                if stock["code"] in symbol:
                    data.extend(self._generate_stock_options(stock))
                    break
        return data
    
    def _generate_index_options(self, index_opt: Dict[str, Any]) -> List[MarketData]:
        """Generate options for an index."""
        import random
        
        underlying_prices = {"HSI": 16200, "HHI": 5600, "MHI": 16200, "HTI": 3800}
        underlying_price = underlying_prices.get(index_opt["underlying"], 1000)
        multiplier = index_opt["multiplier"]
        
        options = []
        
        # Generate ATM and OTM calls/puts
        for opt_type in ["CALL", "PUT"]:
            for moneyness in [0.98, 1.0, 1.02]:  # ITM, ATM, OTM
                strike = round(underlying_price * moneyness, 2)
                
                # Option pricing
                if opt_type == "CALL":
                    intrinsic = max(0, underlying_price - strike)
                else:
                    intrinsic = max(0, strike - underlying_price)
                
                time_to_expiry = random.randint(7, 90) / 365
                iv = random.uniform(20, 45)
                time_value = underlying_price * (iv / 100) * (time_to_expiry ** 0.5)
                
                option_price = (intrinsic + time_value) / multiplier
                option_price = max(0.001, option_price)
                
                # Greeks
                if opt_type == "CALL":
                    delta = 0.5 + (1 - moneyness) * 5 + random.uniform(-0.1, 0.1)
                    delta = min(0.99, max(0.01, delta))
                else:
                    delta = -0.5 - (1 - moneyness) * 5 + random.uniform(-0.1, 0.1)
                    delta = max(-0.99, min(-0.01, delta))
                
                gamma = random.uniform(0.001, 0.01)
                theta = -random.uniform(1, 10)
                vega = random.uniform(0.5, 5)
                
                volume = int(random.uniform(100, 5000))
                open_interest = int(volume * random.uniform(2, 20))
                
                expiry = (datetime.now() + timedelta(days=int(time_to_expiry * 365))).strftime("%Y%m%d")
                symbol = f"{index_opt['underlying']}{expiry}{opt_type[0]}{int(strike)}"
                
                options.append(MarketData(
                    symbol=symbol,
                    name=f"{index_opt['name']} {expiry} {opt_type} @{strike}",
                    asset_class="option",
                    price=round(option_price, 4),
                    change=round(random.uniform(-0.5, 0.5), 4),
                    change_percent=round(random.uniform(-20, 20), 2),
                    volume=volume,
                    turnover=round(volume * option_price * multiplier, 2),
                    extended={
                        "option_type": opt_type,
                        "strike": strike,
                        "expiry": expiry,
                        "underlying": index_opt["underlying"],
                        "underlying_price": underlying_price,
                        "multiplier": multiplier,
                        "implied_volatility": round(iv, 2),
                        "delta": round(delta, 4),
                        "gamma": round(gamma, 5),
                        "theta": round(theta, 4),
                        "vega": round(vega, 4),
                        "open_interest": open_interest,
                        "intrinsic_value": round(intrinsic / multiplier, 4),
                        "time_value": round(time_value / multiplier, 4),
                        "moneyness": round(underlying_price / strike, 4),
                        "product_type": "Index Option"
                    },
                    data_source="hkex_options_simulated"
                ))
        
        return options
    
    def _generate_stock_options(self, stock: Dict[str, Any]) -> List[MarketData]:
        """Generate options for a single stock."""
        import random
        
        underlying_price = stock["price"]
        multiplier = 100  # Standard stock option multiplier
        
        options = []
        
        # Generate ATM call and put
        for opt_type in ["CALL", "PUT"]:
            strike = round(underlying_price * random.uniform(0.95, 1.05), 2)
            
            if opt_type == "CALL":
                intrinsic = max(0, underlying_price - strike)
            else:
                intrinsic = max(0, strike - underlying_price)
            
            time_to_expiry = random.randint(30, 180) / 365
            iv = random.uniform(25, 60)
            time_value = underlying_price * (iv / 100) * (time_to_expiry ** 0.5)
            
            option_price = (intrinsic + time_value) / multiplier
            option_price = max(0.01, option_price)
            
            if opt_type == "CALL":
                delta = random.uniform(0.3, 0.8)
            else:
                delta = random.uniform(-0.8, -0.3)
            
            gamma = random.uniform(0.01, 0.05)
            theta = -random.uniform(0.01, 0.1)
            vega = random.uniform(0.05, 0.2)
            
            volume = int(random.uniform(100, 5000))
            open_interest = int(volume * random.uniform(2, 20))
            
            expiry = (datetime.now() + timedelta(days=int(time_to_expiry * 365))).strftime("%Y%m%d")
            symbol = f"{stock['code']}{expiry}{opt_type[0]}{int(strike * 100):08d}"
            
            options.append(MarketData(
                symbol=symbol,
                name=f"{stock['name']} {expiry} {opt_type} @{strike}",
                asset_class="option",
                price=round(option_price, 4),
                change=round(random.uniform(-0.1, 0.1), 4),
                change_percent=round(random.uniform(-15, 15), 2),
                volume=volume,
                turnover=round(volume * option_price * multiplier, 2),
                extended={
                    "option_type": opt_type,
                    "strike": strike,
                    "expiry": expiry,
                    "underlying": stock["code"],
                    "underlying_price": underlying_price,
                    "multiplier": multiplier,
                    "implied_volatility": round(iv, 2),
                    "delta": round(delta, 4),
                    "gamma": round(gamma, 4),
                    "theta": round(theta, 4),
                    "vega": round(vega, 4),
                    "open_interest": open_interest,
                    "intrinsic_value": round(intrinsic / multiplier, 4),
                    "time_value": round(time_value / multiplier, 4),
                    "moneyness": round(underlying_price / strike, 4),
                    "product_type": "Stock Option"
                },
                data_source="hkex_options_simulated"
            ))
        
        return options


if __name__ == "__main__":
    import asyncio
    
    async def test():
        collector = OptionsCollector()
        async with collector:
            data = await collector.fetch_top_liquid(10)
            for d in data:
                oi = d.extended.get("open_interest", 0)
                opt_type = d.extended.get("option_type", "")
                print(f"{d.symbol}: {opt_type} - ${d.price} | OI: {oi:,}")
    
    asyncio.run(test())
