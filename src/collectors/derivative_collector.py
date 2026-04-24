"""
HKEX Derivatives (Futures & Options) data collector.
Futures: HSI, HSCEI, Mini-HSI, etc.
Options: Index and Stock Options
"""

from typing import List, Dict, Any
from datetime import datetime, timedelta
from .base import BaseCollector, MarketData


class DerivativeCollector(BaseCollector):
    """Collector for HKEX futures and options."""
    
    # Major HKEX futures contracts
    FUTURES_CONTRACTS = [
        {"code": "HSI", "name": "Hang Seng Index Futures", "contract_size": 50, "tick_size": 1, "currency": "HKD"},
        {"code": "HHI", "name": "HSCEI Futures", "contract_size": 50, "tick_size": 1, "currency": "HKD"},
        {"code": "MHI", "name": "Mini-HSI Futures", "contract_size": 10, "tick_size": 1, "currency": "HKD"},
        {"code": "MCH", "name": "Mini-HSCEI Futures", "contract_size": 10, "tick_size": 1, "currency": "HKD"},
        {"code": "HTI", "name": "Hang Seng Tech Index Futures", "contract_size": 50, "tick_size": 1, "currency": "HKD"},
        {"code": "VHS", "name": "HSI Volatility Index Futures", "contract_size": 1000, "tick_size": 0.01, "currency": "HKD"},
        {"code": "CUS", "name": "USD/CNH Futures", "contract_size": 100000, "tick_size": 0.0001, "currency": "CNH"},
        {"code": "EUA", "name": "EUR/USD Futures", "contract_size": 125000, "tick_size": 0.0001, "currency": "USD"},
        {"code": "JPY", "name": "JPY/USD Futures", "contract_size": 12500000, "tick_size": 0.000001, "currency": "USD"},
    ]
    
    # Stock options on liquid underlyings
    STOCK_OPTION_UNDERLYINGS = [
        "0700", "0005", "9988", "1299", "3690", "1211", "0941", "2318",
        "1398", "3988", "1810", "2269", "3968", "2007", "2382", "1109"
    ]
    
    def get_asset_class(self) -> str:
        return "derivative"
    
    async def fetch_top_liquid(self, limit: int = 20) -> List[MarketData]:
        """Fetch top liquid derivatives (futures + most active options)."""
        data = []
        
        # Generate futures data
        for contract in self.FUTURES_CONTRACTS:
            data.append(self._generate_futures_data(contract))
        
        # Generate stock options data for top underlyings
        for underlying in self.STOCK_OPTION_UNDERLYINGS[:5]:  # Top 5
            data.extend(self._generate_options_data(underlying))
        
        # Sort by open interest (most liquid indicator for derivatives)
        data.sort(key=lambda x: x.extended.get("open_interest", 0), reverse=True)
        return data[:limit]
    
    async def fetch_snapshot(self, symbols: List[str]) -> List[MarketData]:
        """Fetch snapshot for specific derivative symbols."""
        # For futures, symbols like HSI-202401
        data = []
        for symbol in symbols:
            code = symbol.split("-")[0] if "-" in symbol else symbol
            contract = next((c for c in self.FUTURES_CONTRACTS if c["code"] == code), None)
            if contract:
                data.append(self._generate_futures_data(contract))
        return data
    
    def _generate_futures_data(self, contract: Dict[str, Any]) -> MarketData:
        """Generate realistic futures market data."""
        import random
        
        # Base index values
        base_values = {"HSI": 16200, "HHI": 5600, "MHI": 16200, "MCH": 5600, "HTI": 3800}
        base = base_values.get(contract["code"], 10000)
        
        # Generate price movement
        change_percent = random.uniform(-2, 2)
        price = base * (1 + change_percent / 100)
        
        # Generate expiry (near month, next month, quarterlies)
        today = datetime.now()
        expiry_months = [0, 1, 2, 3]  # Current + 3 future months
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
            asset_class="derivative",
            price=round(price, 2),
            open=round(base * random.uniform(0.995, 1.005), 2),
            high=round(max(price, base) * random.uniform(1.0, 1.015), 2),
            low=round(min(price, base) * random.uniform(0.985, 1.0), 2),
            previous_close=round(base, 2),
            change=round(price - base, 2),
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
                "settlement_price": round(price * random.uniform(0.999, 1.001), 2),
                "oi_change": int(open_interest * random.uniform(-0.1, 0.1)),
                "basis": round(random.uniform(-50, 50), 2),  # Futures - Spot
            },
            data_source="hkex_derivatives_simulated"
        )
    
    def _generate_options_data(self, underlying: str) -> List[MarketData]:
        """Generate options data for an underlying."""
        import random
        
        # Base price for underlying (approximate)
        base_prices = {
            "0700": 412, "0005": 68, "9988": 88, "1299": 58, "3690": 143,
            "1211": 279, "0941": 72, "2318": 46, "1398": 4.2, "3988": 3.5,
        }
        underlying_price = base_prices.get(underlying, 50)
        
        options = []
        
        # Generate ATM call and put
        for opt_type in ["CALL", "PUT"]:
            strike = round(underlying_price * random.uniform(0.95, 1.05), 2)
            
            # Option price calculation (simplified)
            iv = random.uniform(20, 50)  # Implied volatility
            time_to_expiry = 30 / 365  # ~1 month
            
            # Simplified Black-Scholes-like pricing
            if opt_type == "CALL":
                intrinsic = max(0, underlying_price - strike)
            else:
                intrinsic = max(0, strike - underlying_price)
            
            time_value = underlying_price * (iv / 100) * (time_to_expiry ** 0.5)
            option_price = intrinsic + time_value + random.uniform(-0.5, 0.5)
            option_price = max(0.01, option_price)
            
            # Greeks (simplified)
            if opt_type == "CALL":
                delta = 0.5 + random.uniform(-0.3, 0.3)
                delta = min(0.99, max(0.01, delta))
            else:
                delta = -0.5 + random.uniform(-0.3, 0.3)
                delta = max(-0.99, min(-0.01, delta))
            
            gamma = random.uniform(0.01, 0.05)
            theta = -random.uniform(0.01, 0.1)  # Negative for long options
            vega = random.uniform(0.05, 0.2)
            
            volume = int(random.uniform(100, 5000))
            open_interest = int(volume * random.uniform(2, 20))
            
            expiry = (datetime.now() + timedelta(days=30)).strftime("%Y%m%d")
            symbol = f"{underlying}{expiry}{opt_type[0]}{int(strike * 100):08d}"
            
            options.append(MarketData(
                symbol=symbol,
                name=f"{underlying} {expiry} {opt_type} @{strike}",
                asset_class="derivative",
                price=round(option_price, 3),
                change=round(random.uniform(-1, 1), 3),
                change_percent=round(random.uniform(-20, 20), 2),
                volume=volume,
                turnover=round(volume * option_price * 100, 2),  # Contract multiplier
                extended={
                    "option_type": opt_type,
                    "strike": strike,
                    "expiry": expiry,
                    "underlying": underlying,
                    "underlying_symbol": f"{underlying}.HK",
                    "underlying_price": underlying_price,
                    "implied_volatility": round(iv, 2),
                    "delta": round(delta, 4),
                    "gamma": round(gamma, 4),
                    "theta": round(theta, 4),
                    "vega": round(vega, 4),
                    "open_interest": open_interest,
                    "intrinsic_value": round(intrinsic, 3),
                    "time_value": round(time_value, 3),
                    "moneyness": round(underlying_price / strike, 4),
                },
                data_source="hkex_options_simulated"
            ))
        
        return options


if __name__ == "__main__":
    import asyncio
    
    async def test():
        collector = DerivativeCollector()
        async with collector:
            data = await collector.fetch_top_liquid(10)
            for d in data:
                oi = d.extended.get("open_interest", 0)
                print(f"{d.symbol}: {d.name} - ${d.price} | OI: {oi:,}")
    
    asyncio.run(test())
