"""
HKEX CBBCs (Callable Bull/Bear Contracts) and Warrants collector.
"""

from typing import List, Dict, Any
from datetime import datetime, timedelta
from .base import BaseCollector, MarketData


class CBBCWarrantCollector(BaseCollector):
    """Collector for CBBCs and Warrants."""
    
    # Major issuers and popular products
    POPULAR_CBBCS = [
        # HSITargeting HSI
        {"symbol": "68001.HK", "name": "UBS HSI CBBC Bull", "underlying": "HSI", "issuer": "UBS", "type": "bull"},
        {"symbol": "68002.HK", "name": "Morgan Stanley HSI CBBC Bull", "underlying": "HSI", "issuer": "MS", "type": "bull"},
        {"symbol": "68003.HK", "name": "Credit Suisse HSI CBBC Bear", "underlying": "HSI", "issuer": "CS", "type": "bear"},
        {"symbol": "68004.HK", "name": "Goldman Sachs HSI CBBC Bear", "underlying": "HSI", "issuer": "GS", "type": "bear"},
        
        # HSCEI
        {"symbol": "68005.HK", "name": "UBS HSCEI CBBC Bull", "underlying": "HSCEI", "issuer": "UBS", "type": "bull"},
        {"symbol": "68006.HK", "name": "JPM HSCEI CBBC Bear", "underlying": "HSCEI", "issuer": "JPM", "type": "bear"},
        
        # Single Stock - Tencent
        {"symbol": "68007.HK", "name": "UBS Tencent CBBC Bull", "underlying": "0700", "issuer": "UBS", "type": "bull"},
        {"symbol": "68008.HK", "name": "BOCI Tencent CBBC Bear", "underlying": "0700", "issuer": "BOCI", "type": "bear"},
        
        # Alibaba
        {"symbol": "68009.HK", "name": "HSBC Alibaba CBBC Bull", "underlying": "9988", "issuer": "HSBC", "type": "bull"},
        {"symbol": "68010.HK", "name": "Citi Alibaba CBBC Bear", "underlying": "9988", "issuer": "Citi", "type": "bear"},
    ]
    
    POPULAR_WARRANTS = [
        # HSI Warrants
        {"symbol": "12001.HK", "name": "UBS HSI Call Warrant", "underlying": "HSI", "issuer": "UBS", "type": "call"},
        {"symbol": "12002.HK", "name": "Morgan Stanley HSI Put Warrant", "underlying": "HSI", "issuer": "MS", "type": "put"},
        
        # Stock Warrants
        {"symbol": "12003.HK", "name": "UBS Tencent Call", "underlying": "0700", "issuer": "UBS", "type": "call"},
        {"symbol": "12004.HK", "name": "BOCI Tencent Put", "underlying": "0700", "issuer": "BOCI", "type": "put"},
        {"symbol": "12005.HK", "name": "HSBC HSBC Call", "underlying": "0005", "issuer": "HSBC", "type": "call"},
        {"symbol": "12006.HK", "name": "Credit Suisse HSBC Put", "underlying": "0005", "issuer": "CS", "type": "put"},
        {"symbol": "12007.HK", "name": "Goldman Sachs Alibaba Call", "underlying": "9988", "issuer": "GS", "type": "call"},
        {"symbol": "12008.HK", "name": "JPM Alibaba Put", "underlying": "9988", "issuer": "JPM", "type": "put"},
        {"symbol": "12009.HK", "name": "Citi AIA Call", "underlying": "1299", "issuer": "Citi", "type": "call"},
        {"symbol": "12010.HK", "name": "Morgan Stanley Meituan Call", "underlying": "3690", "issuer": "MS", "type": "call"},
    ]
    
    def get_asset_class(self) -> str:
        return "cbbc_warrant"
    
    async def fetch_top_liquid(self, limit: int = 30) -> List[MarketData]:
        """Fetch top liquid CBBCs and Warrants."""
        data = []
        
        # CBBCs
        for cbbc in self.POPULAR_CBBCS:
            data.append(self._generate_cbbc_data(cbbc))
        
        # Warrants
        for warrant in self.POPULAR_WARRANTS:
            data.append(self._generate_warrant_data(warrant))
        
        # Sort by turnover
        data.sort(key=lambda x: x.turnover or 0, reverse=True)
        return data[:limit]
    
    async def fetch_snapshot(self, symbols: List[str]) -> List[MarketData]:
        """Fetch snapshot for specific CBBC/Warrant symbols."""
        all_products = {p["symbol"]: p for p in self.POPULAR_CBBCS + self.POPULAR_WARRANTS}
        
        data = []
        for symbol in symbols:
            if symbol in all_products:
                product = all_products[symbol]
                if "CBBC" in product["name"]:
                    data.append(self._generate_cbbc_data(product))
                else:
                    data.append(self._generate_warrant_data(product))
        
        return data
    
    def _generate_cbbc_data(self, cbbc: Dict[str, Any]) -> MarketData:
        """Generate realistic CBBC market data."""
        import random
        
        # Underlying prices
        underlying_prices = {
            "HSI": 16200, "HSCEI": 5600, "0700": 412, "9988": 88,
            "1299": 58, "3690": 143, "0005": 68
        }
        underlying_price = underlying_prices.get(cbbc["underlying"], 100)
        
        # CBBC pricing
        is_bull = cbbc["type"] == "bull"
        
        # Strike and call levels
        if is_bull:
            strike_pct = random.uniform(0.05, 0.15)  # 5-15% OTM
            strike = underlying_price * (1 - strike_pct)
            call_level = strike * 1.02  # Call level above strike
        else:
            strike_pct = random.uniform(0.05, 0.15)
            strike = underlying_price * (1 + strike_pct)
            call_level = strike * 0.98  # Call level below strike
        
        # CBBC price (typically cheap, around $0.01-$0.50)
        entitlement_ratio = random.choice([100, 500, 1000, 5000, 10000])
        intrinsic = abs(underlying_price - strike) / entitlement_ratio
        price = intrinsic + random.uniform(0.001, 0.01)
        price = max(0.01, min(1.0, price))
        
        # Generate change
        if is_bull:
            change_percent = random.uniform(-20, 20)
        else:
            change_percent = random.uniform(-20, 20) * -1  # Inverse
        
        # Volume (CBBCs are highly traded)
        volume = int(random.uniform(50000000, 500000000))
        turnover = volume * price
        
        # Distance to call level
        distance_to_call = abs(underlying_price - call_level) / underlying_price * 100
        
        # Gearing (leverage)
        gearing = underlying_price / (price * entitlement_ratio)
        
        # Premium
        if is_bull:
            premium = (strike + price * entitlement_ratio - underlying_price) / underlying_price * 100
        else:
            premium = (underlying_price + price * entitlement_ratio - strike) / underlying_price * 100
        
        return MarketData(
            symbol=cbbc["symbol"],
            name=cbbc["name"],
            asset_class="cbbc",
            price=round(price, 4),
            change=round(price * change_percent / 100, 4),
            change_percent=round(change_percent, 2),
            volume=volume,
            turnover=round(turnover, 2),
            extended={
                "product_type": "CBBC",
                "cbbc_type": "Bull" if is_bull else "Bear",
                "issuer": cbbc["issuer"],
                "underlying": cbbc["underlying"],
                "underlying_symbol": f"{cbbc['underlying']}.HK" if not cbbc['underlying'].isdigit() else f"{cbbc['underlying']}.HK",
                "underlying_price": underlying_price,
                "strike": round(strike, 2),
                "call_level": round(call_level, 2),
                "distance_to_call_percent": round(distance_to_call, 2),
                "entitlement_ratio": entitlement_ratio,
                "gearing": round(gearing, 2),
                "premium_percent": round(premium, 2),
                "implied_volatility": round(random.uniform(20, 40), 2),
                "delta": round(random.uniform(0.7, 1.0), 4),
                "expiry": (datetime.now() + timedelta(days=random.randint(90, 730))).strftime("%Y-%m-%d"),
                "outstanding": int(random.uniform(100000000, 1000000000)),
                "money_held": round(turnover * random.uniform(0.1, 0.3), 2),
            },
            data_source="hkex_cbbc_simulated"
        )
    
    def _generate_warrant_data(self, warrant: Dict[str, Any]) -> MarketData:
        """Generate realistic Warrant market data."""
        import random
        
        # Underlying prices
        underlying_prices = {
            "HSI": 16200, "HSCEI": 5600, "0700": 412, "9988": 88,
            "1299": 58, "3690": 143, "0005": 68
        }
        underlying_price = underlying_prices.get(warrant["underlying"], 100)
        
        is_call = warrant["type"] == "call"
        
        # Strike price
        if is_call:
            strike = underlying_price * random.uniform(0.9, 1.2)
        else:
            strike = underlying_price * random.uniform(0.8, 1.1)
        
        # Warrant pricing
        entitlement_ratio = random.choice([1, 10, 100])
        
        # Simplified pricing
        if is_call:
            intrinsic = max(0, underlying_price - strike) / entitlement_ratio
        else:
            intrinsic = max(0, strike - underlying_price) / entitlement_ratio
        
        # Time value component
        time_to_expiry = random.randint(90, 730) / 365
        iv = random.uniform(25, 60)
        time_value = underlying_price * (iv / 100) * (time_to_expiry ** 0.5) / entitlement_ratio
        
        price = intrinsic + time_value + random.uniform(-0.001, 0.001)
        price = max(0.01, price)
        
        # Greeks
        if is_call:
            delta = random.uniform(0.3, 0.8)
        else:
            delta = random.uniform(-0.8, -0.3)
        
        gearing = underlying_price / (price * entitlement_ratio)
        
        # Premium
        if is_call:
            effective_strike = strike + price * entitlement_ratio
            premium = (effective_strike / underlying_price - 1) * 100
        else:
            effective_strike = strike - price * entitlement_ratio
            premium = (1 - effective_strike / underlying_price) * 100
        
        # Volume
        volume = int(random.uniform(10000000, 200000000))
        turnover = volume * price
        
        return MarketData(
            symbol=warrant["symbol"],
            name=warrant["name"],
            asset_class="warrant",
            price=round(price, 4),
            change=round(random.uniform(-0.01, 0.01), 4),
            change_percent=round(random.uniform(-15, 15), 2),
            volume=volume,
            turnover=round(turnover, 2),
            extended={
                "product_type": "Warrant",
                "warrant_type": "Call" if is_call else "Put",
                "issuer": warrant["issuer"],
                "underlying": warrant["underlying"],
                "underlying_symbol": f"{warrant['underlying']}.HK",
                "underlying_price": underlying_price,
                "strike": round(strike, 2),
                "entitlement_ratio": entitlement_ratio,
                "gearing": round(gearing, 2),
                "premium_percent": round(premium, 2),
                "implied_volatility": round(iv, 2),
                "delta": round(delta, 4),
                "expiry": (datetime.now() + timedelta(days=random.randint(90, 730))).strftime("%Y-%m-%d"),
                "outstanding": int(random.uniform(50000000, 500000000)),
                "intrinsic_value": round(intrinsic, 4),
                "time_value": round(time_value, 4),
                "moneyness": round(underlying_price / strike, 4),
            },
            data_source="hkex_warrant_simulated"
        )


if __name__ == "__main__":
    import asyncio
    
    async def test():
        collector = CBBCWarrantCollector()
        async with collector:
            data = await collector.fetch_top_liquid(10)
            for d in data:
                gearing = d.extended.get("gearing", 0)
                print(f"{d.symbol}: {d.name} - ${d.price} | Gearing: {gearing:.1f}x")
    
    asyncio.run(test())
