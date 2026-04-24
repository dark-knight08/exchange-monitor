"""
Alert engine for detecting unusual market activity.
"""

import asyncio
import logging
import sys
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional

# Handle both relative and absolute imports
try:
    from ..database.data_store import DataStore
    from ..database.models import MarketSnapshot, Asset
except ImportError:
    from database.data_store import DataStore
    from database.models import MarketSnapshot, Asset

logger = logging.getLogger(__name__)


class AlertEngine:
    """Detects and generates market alerts."""
    
    def __init__(self, data_store: DataStore):
        self.data_store = data_store
        self.alert_handlers = []
        
        # Alert thresholds
        self.price_change_threshold = 5.0  # 5% move
        self.volume_spike_multiplier = 3.0  # 3x average volume
        
        logger.info("AlertEngine initialized")
    
    def register_handler(self, handler):
        """Register an alert notification handler."""
        self.alert_handlers.append(handler)
    
    async def check_all_alerts(self):
        """Check for all types of alerts."""
        await asyncio.gather(
            self.check_price_movement_alerts(),
            self.check_volume_spike_alerts(),
            self.check_breakout_alerts()
        )
    
    async def check_price_movement_alerts(self):
        """Check for significant price movements."""
        from sqlalchemy import func
        from sqlalchemy.orm import Session
        
        session = self.data_store.Session()
        try:
            # Get latest snapshots with significant changes
            latest = session.query(MarketSnapshot, Asset).join(Asset).filter(
                func.abs(MarketSnapshot.change_percent) >= self.price_change_threshold
            ).all()
            
            for snapshot, asset in latest:
                direction = "up" if snapshot.change_percent > 0 else "down"
                severity = "critical" if abs(snapshot.change_percent) >= 10 else "warning"
                
                alert = self.data_store.create_alert(
                    symbol=asset.symbol,
                    alert_type="price_movement",
                    severity=severity,
                    title=f"{asset.symbol} moved {direction} {abs(snapshot.change_percent):.2f}%",
                    description=f"{asset.name} ({asset.symbol}) moved {abs(snapshot.change_percent):.2f}% "
                               f"to ${snapshot.close:.2f}",
                    triggered_value=snapshot.change_percent,
                    threshold_value=self.price_change_threshold
                )
                
                await self._notify_handlers({
                    "alert_id": alert.id,
                    "symbol": asset.symbol,
                    "type": "price_movement",
                    "severity": severity,
                    "message": alert.title,
                    "timestamp": datetime.utcnow().isoformat()
                })
            
            if latest:
                logger.info(f"Created {len(latest)} price movement alerts")
                
        finally:
            session.close()
    
    async def check_volume_spike_alerts(self):
        """Check for unusual volume spikes."""
        # Get recent snapshots and compare to 30-day average
        logger.info("Checking volume spike alerts")
        
        # Implementation would query historical volume data
        # and compare to current volume
        pass
    
    async def check_breakout_alerts(self):
        """Check for price breakouts (new highs/lows)."""
        logger.info("Checking breakout alerts")
        
        # Implementation would check if price breaks
        # 52-week high/low or key technical levels
        pass
    
    async def check_daily_summary_alerts(self):
        """Check for end-of-day summary alerts."""
        logger.info("Checking daily summary alerts")
        
        # Check market-wide anomalies
        # - Unusual market breadth
        # - Extreme volatility
        # - Sector rotation signals
        pass
    
    async def _notify_handlers(self, alert_data: Dict[str, Any]):
        """Notify all registered handlers."""
        for handler in self.alert_handlers:
            try:
                await handler(alert_data)
            except Exception as e:
                logger.error(f"Handler notification failed: {e}")
    
    def get_active_alerts_summary(self) -> Dict[str, Any]:
        """Get summary of active alerts."""
        alerts = self.data_store.get_active_alerts()
        
        by_severity = {"critical": 0, "warning": 0, "info": 0}
        by_type = {}
        
        for alert in alerts:
            severity = alert.get("severity", "info")
            alert_type = alert.get("alert_type", "unknown")
            
            by_severity[severity] = by_severity.get(severity, 0) + 1
            by_type[alert_type] = by_type.get(alert_type, 0) + 1
        
        return {
            "total": len(alerts),
            "by_severity": by_severity,
            "by_type": by_type,
            "latest": alerts[:5] if alerts else []
        }


# Example notification handlers
class NotificationHandler:
    """Base class for notification handlers."""
    
    async def __call__(self, alert_data: Dict[str, Any]):
        raise NotImplementedError


class ConsoleNotificationHandler(NotificationHandler):
    """Prints alerts to console."""
    
    async def __call__(self, alert_data: Dict[str, Any]):
        print(f"[ALERT] {alert_data['severity'].upper()}: {alert_data['message']}")


class WebhookNotificationHandler(NotificationHandler):
    """Sends alerts to webhook."""
    
    def __init__(self, webhook_url: str):
        self.webhook_url = webhook_url
    
    async def __call__(self, alert_data: Dict[str, Any]):
        import aiohttp
        
        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(
                    self.webhook_url,
                    json=alert_data
                ) as response:
                    if response.status != 200:
                        logger.error(f"Webhook notification failed: {response.status}")
        except Exception as e:
            logger.error(f"Webhook notification error: {e}")


if __name__ == "__main__":
    # Test alert engine
    from ..database.models import init_database
    
    init_database()
    
    store = DataStore()
    engine = AlertEngine(store)
    
    # Register console handler
    engine.register_handler(ConsoleNotificationHandler())
    
    # Run checks
    asyncio.run(engine.check_all_alerts())
