"""
FastAPI server for HKEX Market Monitor.
Provides RESTful API and WebSocket for real-time updates.
"""

from fastapi import FastAPI, HTTPException, WebSocket, WebSocketDisconnect, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, FileResponse
from fastapi.staticfiles import StaticFiles
from datetime import date, datetime, timedelta
from typing import List, Optional, Dict, Any
import logging
import asyncio
import json
from pathlib import Path

from ..database.data_store import DataStore
from ..collectors import (
    MultiAssetCollector, EquityCollector, ETFCollector,
    DerivativeCollector, CBBCWarrantCollector
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Get paths
BASE_DIR = Path(__file__).parent.parent.parent
WEB_DIR = BASE_DIR / "web"

# Create FastAPI app
app = FastAPI(
    title="HKEX Market Monitor API",
    description="Real-time and historical market data for HKEX multi-asset classes",
    version="1.0.0"
)

# Mount static files (dashboard) if web directory exists
if WEB_DIR.exists():
    app.mount("/static", StaticFiles(directory=str(WEB_DIR)), name="static")

# CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Configure properly for production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Global instances
data_store = DataStore()
collector_manager = MultiAssetCollector()

# Initialize collectors
collector_manager.register(EquityCollector())
collector_manager.register(ETFCollector())
collector_manager.register(DerivativeCollector())
collector_manager.register(CBBCWarrantCollector())

# WebSocket connection manager
class ConnectionManager:
    def __init__(self):
        self.active_connections: List[WebSocket] = []
    
    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.append(websocket)
        logger.info(f"WebSocket connected. Total: {len(self.active_connections)}")
    
    def disconnect(self, websocket: WebSocket):
        if websocket in self.active_connections:
            self.active_connections.remove(websocket)
        logger.info(f"WebSocket disconnected. Total: {len(self.active_connections)}")
    
    async def broadcast(self, message: Dict[str, Any]):
        disconnected = []
        for connection in self.active_connections:
            try:
                await connection.send_json(message)
            except:
                disconnected.append(connection)
        
        # Clean up disconnected
        for conn in disconnected:
            self.disconnect(conn)

manager = ConnectionManager()


# API Endpoints

@app.get("/health")
@app.get("/api/v1/health")
async def health_check():
    """Health check endpoint for Docker/load balancer."""
    return {"status": "healthy", "service": "hkex-monitor", "timestamp": datetime.utcnow().isoformat()}


@app.get("/")
async def root():
    """Serve the dashboard if available, otherwise return API info."""
    index_file = WEB_DIR / "index.html"
    if index_file.exists():
        return FileResponse(str(index_file))

    return {
        "name": "HKEX Market Monitor API",
        "version": "1.0.0",
        "endpoints": {
            "assets": "/api/v1/assets/{asset_class}",
            "snapshot": "/api/v1/snapshot/current",
            "daily": "/api/v1/daily/{date}",
            "changes": "/api/v1/changes/daily",
            "alerts": "/api/v1/alerts",
            "summary": "/api/v1/summary",
            "websocket": "/ws/market"
        }
    }


@app.get("/api/v1/assets/{asset_class}")
async def get_assets(
    asset_class: str,
    limit: int = Query(50, ge=1, le=200),
    sort_by: str = Query("turnover", enum=["turnover", "volume", "change", "price"]),
    min_volume: Optional[float] = None,
    sort_order: str = Query("desc", enum=["asc", "desc"])
):
    """
    Get assets by class with filtering and sorting.
    
    Asset classes: equity, etf, future, option, cbbc, warrant
    """
    valid_classes = ["equity", "etf", "future", "option", "cbbc", "warrant", "all"]
    
    if asset_class not in valid_classes:
        raise HTTPException(status_code=400, detail=f"Invalid asset class. Must be one of: {valid_classes}")
    
    # Get latest snapshots
    if asset_class == "all":
        results = []
        for cls in ["equity", "etf", "future", "option", "cbbc", "warrant"]:
            snapshots = data_store.get_latest_snapshots(asset_class=cls, limit=limit)
            results.extend(snapshots)
    else:
        results = data_store.get_latest_snapshots(asset_class=asset_class, limit=limit)
    
    # Filter by volume
    if min_volume:
        results = [r for r in results if (r.get('volume') or 0) >= min_volume]
    
    # Sort
    sort_key = {
        "turnover": "turnover",
        "volume": "volume", 
        "change": "change_percent",
        "price": "price"
    }.get(sort_by, "turnover")
    
    reverse = sort_order == "desc"
    results.sort(key=lambda x: x.get(sort_key) or 0, reverse=reverse)
    
    return {
        "asset_class": asset_class,
        "count": len(results),
        "data": results[:limit]
    }


@app.get("/api/v1/snapshot/current")
async def get_current_snapshot(
    asset_class: Optional[str] = None,
    symbols: Optional[str] = None
):
    """Get current market snapshot."""
    
    # If specific symbols requested
    if symbols:
        symbol_list = [s.strip() for s in symbols.split(",")]
        # Fetch from collectors
        # For now, return cached data
        results = data_store.get_latest_snapshots(asset_class=asset_class, limit=200)
        filtered = [r for r in results if r.get('symbol') in symbol_list]
        return {"count": len(filtered), "data": filtered}
    
    # Get all latest snapshots
    results = data_store.get_latest_snapshots(asset_class=asset_class, limit=100)
    
    return {
        "timestamp": datetime.utcnow().isoformat(),
        "count": len(results),
        "data": results
    }


@app.get("/api/v1/daily/{date_str}")
async def get_daily_data(
    date_str: str,
    asset_class: Optional[str] = None,
    limit: int = Query(100, ge=1, le=500)
):
    """
    Get end-of-day data for a specific date.
    Date format: YYYY-MM-DD
    """
    try:
        target_date = datetime.strptime(date_str, "%Y-%m-%d").date()
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid date format. Use YYYY-MM-DD")
    
    # Query daily bars
    from ..database.models import DailyBar, Asset
    from sqlalchemy.orm import Session
    from sqlalchemy import desc
    
    session = data_store.Session()
    try:
        query = session.query(DailyBar, Asset).join(Asset).filter(DailyBar.date == target_date)
        
        if asset_class:
            query = query.filter(Asset.asset_class == asset_class)
        
        query = query.order_by(desc(DailyBar.turnover))
        results = query.limit(limit).all()
        
        data = [{
            "symbol": r.Asset.symbol,
            "name": r.Asset.name,
            "asset_class": r.Asset.asset_class,
            "open": r.DailyBar.open,
            "high": r.DailyBar.high,
            "low": r.DailyBar.low,
            "close": r.DailyBar.close,
            "volume": r.DailyBar.volume,
            "turnover": r.DailyBar.turnover,
            "change": r.DailyBar.change,
            "change_percent": r.DailyBar.change_percent,
        } for r in results]
        
        return {
            "date": date_str,
            "count": len(data),
            "data": data
        }
    finally:
        session.close()


@app.get("/api/v1/changes/daily")
async def get_daily_changes(
    date_str: Optional[str] = None,
    asset_class: Optional[str] = None,
    min_change: Optional[float] = None,
    top_gainers: bool = False,
    top_losers: bool = False,
    limit: int = Query(20, ge=1, le=100)
):
    """Get daily price changes."""
    
    if date_str:
        try:
            target_date = datetime.strptime(date_str, "%Y-%m-%d").date()
        except ValueError:
            raise HTTPException(status_code=400, detail="Invalid date format. Use YYYY-MM-DD")
    else:
        target_date = date.today()
    
    # Build filter
    if top_gainers:
        min_change = min_change or 0
    elif top_losers:
        # Will filter negative changes
        pass
    
    changes = data_store.get_daily_changes(
        target_date=target_date,
        asset_class=asset_class,
        min_change=min_change,
        limit=limit
    )
    
    if top_losers:
        changes = [c for c in changes if (c.get('price_change_percent') or 0) < 0]
        changes.sort(key=lambda x: x.get('price_change_percent', 0))
    elif top_gainers:
        changes = [c for c in changes if (c.get('price_change_percent') or 0) > 0]
        changes.sort(key=lambda x: x.get('price_change_percent', 0), reverse=True)
    
    return {
        "date": target_date.isoformat(),
        "count": len(changes),
        "data": changes[:limit]
    }


@app.get("/api/v1/history/{symbol}")
async def get_symbol_history(
    symbol: str,
    days: int = Query(30, ge=1, le=365),
    interval: str = Query("daily", enum=["daily", "weekly"])
):
    """Get historical price data for a symbol."""
    
    end_date = date.today()
    start_date = end_date - timedelta(days=days)
    
    df = data_store.get_daily_bars(symbol, start_date, end_date)
    
    if df.empty:
        raise HTTPException(status_code=404, detail=f"No data found for symbol: {symbol}")
    
    return {
        "symbol": symbol,
        "days": days,
        "count": len(df),
        "data": df.to_dict('records')
    }


@app.get("/api/v1/alerts")
async def get_alerts(
    active_only: bool = True,
    asset_class: Optional[str] = None,
    severity: Optional[str] = None,
    limit: int = Query(50, ge=1, le=200)
):
    """Get market alerts."""
    
    if active_only:
        alerts = data_store.get_active_alerts(asset_class=asset_class, severity=severity)
    else:
        # Get all alerts
        from ..database.models import Alert, Asset
        from sqlalchemy import desc
        
        session = data_store.Session()
        try:
            query = session.query(Alert, Asset).join(Asset)
            
            if asset_class:
                query = query.filter(Asset.asset_class == asset_class)
            if severity:
                query = query.filter(Alert.severity == severity)
            
            query = query.order_by(desc(Alert.triggered_at))
            results = query.limit(limit).all()
            
            alerts = [{
                'id': a.Alert.id,
                'symbol': a.Asset.symbol,
                'name': a.Asset.name,
                'asset_class': a.Asset.asset_class,
                'alert_type': a.Alert.alert_type,
                'severity': a.Alert.severity,
                'title': a.Alert.title,
                'description': a.Alert.description,
                'triggered_value': a.Alert.triggered_value,
                'triggered_at': a.Alert.triggered_at,
                'is_active': a.Alert.is_active,
            } for a in results]
        finally:
            session.close()
    
    return {
        "count": len(alerts),
        "data": alerts
    }


@app.get("/api/v1/summary")
async def get_market_summary(
    date_str: Optional[str] = None
):
    """Get market summary by asset class."""
    
    if date_str:
        try:
            target_date = datetime.strptime(date_str, "%Y-%m-%d").date()
        except ValueError:
            raise HTTPException(status_code=400, detail="Invalid date format")
    else:
        target_date = date.today()
    
    summaries = data_store.get_asset_class_summary(target_date)
    
    # Also get latest snapshots for current prices
    latest = data_store.get_latest_snapshots(limit=100)
    
    return {
        "date": target_date.isoformat(),
        "timestamp": datetime.utcnow().isoformat(),
        "summaries": summaries,
        "total_securities": len(latest)
    }


@app.get("/api/v1/search")
async def search_symbols(
    query: str = Query(..., min_length=1),
    limit: int = Query(10, ge=1, le=50)
):
    """Search for symbols by name or code."""
    
    from ..database.models import Asset
    from sqlalchemy import or_
    
    session = data_store.Session()
    try:
        results = session.query(Asset).filter(
            or_(
                Asset.symbol.ilike(f"%{query}%"),
                Asset.name.ilike(f"%{query}%")
            )
        ).limit(limit).all()
        
        return {
            "query": query,
            "count": len(results),
            "results": [{
                "symbol": r.symbol,
                "name": r.name,
                "asset_class": r.asset_class,
            } for r in results]
        }
    finally:
        session.close()


# WebSocket endpoint
@app.websocket("/ws/market")
async def websocket_endpoint(websocket: WebSocket):
    """WebSocket for real-time market updates."""
    await manager.connect(websocket)
    
    try:
        while True:
            # Receive message from client (if any)
            try:
                message = await asyncio.wait_for(
                    websocket.receive_text(),
                    timeout=30.0
                )
                
                data = json.loads(message)
                action = data.get('action')
                
                if action == 'subscribe':
                    # Client subscribing to specific asset class
                    asset_class = data.get('asset_class')
                    await websocket.send_json({
                        "type": "subscribed",
                        "asset_class": asset_class
                    })
                
                elif action == 'get_snapshot':
                    # Client requesting snapshot
                    asset_class = data.get('asset_class')
                    snapshots = data_store.get_latest_snapshots(asset_class, limit=50)
                    await websocket.send_json({
                        "type": "snapshot",
                        "data": snapshots
                    })
                    
            except asyncio.TimeoutError:
                # Send heartbeat
                await websocket.send_json({"type": "heartbeat", "timestamp": datetime.utcnow().isoformat()})
                
    except WebSocketDisconnect:
        manager.disconnect(websocket)
    except Exception as e:
        logger.error(f"WebSocket error: {e}")
        manager.disconnect(websocket)


# Background task to broadcast updates
async def broadcast_market_updates():
    """Broadcast market updates to all connected WebSocket clients."""
    while True:
        try:
            # Get latest data
            snapshots = data_store.get_latest_snapshots(limit=20)
            
            if snapshots and manager.active_connections:
                await manager.broadcast({
                    "type": "market_update",
                    "timestamp": datetime.utcnow().isoformat(),
                    "data": snapshots[:10]  # Top 10 movers
                })
            
            await asyncio.sleep(30)  # Update every 30 seconds
            
        except Exception as e:
            logger.error(f"Broadcast error: {e}")
            await asyncio.sleep(30)


@app.on_event("startup")
async def startup_event():
    """Start background tasks on server startup."""
    asyncio.create_task(broadcast_market_updates())
    logger.info("Market update broadcaster started")


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
