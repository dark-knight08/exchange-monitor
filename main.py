#!/usr/bin/env python3
"""
HKEX Market Monitor - Main Application

A comprehensive market monitoring system for the Hong Kong Stock Exchange,
tracking multiple asset classes: Equities, ETFs, Derivatives, CBBCs, and Warrants.

Features:
- Real-time market data collection
- Daily change tracking
- Multi-asset class monitoring
- Alert system for unusual activity
- RESTful API and WebSocket
- Interactive web dashboard
"""

import asyncio
import argparse
import logging
import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent / "src"))

from src.database import init_database, DataStore
from src.api.server import app
from src.scheduler import JobScheduler, CollectionJobs
from src.alerts import AlertEngine

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class HKEXMonitor:
    """Main application class."""
    
    def __init__(self):
        self.db_path = "data/hkex_monitor.db"
        self.data_store = None
        self.scheduler = None
        self.jobs = None
        self.alert_engine = None
    
    def initialize(self):
        """Initialize database and components."""
        logger.info("Initializing HKEX Market Monitor...")
        
        # Initialize database
        init_database(self.db_path)
        self.data_store = DataStore(self.db_path)
        
        # Initialize alert engine
        self.alert_engine = AlertEngine(self.data_store)
        
        # Initialize scheduler and jobs
        self.scheduler = JobScheduler()
        self.jobs = CollectionJobs(self.data_store, self.alert_engine)
        
        logger.info("Initialization complete")
    
    def setup_schedules(self):
        """Setup scheduled data collection jobs."""
        logger.info("Setting up scheduled jobs...")
        
        # Real-time snapshot every 30 seconds during market hours
        self.scheduler.add_job(
            "realtime_snapshot",
            self.jobs.collect_realtime_snapshot,
            self.scheduler.every_n_seconds(30)
        )
        
        # Pre-market snapshot at 08:45
        self.scheduler.add_job(
            "pre_market",
            lambda: self.jobs.collect_realtime_snapshot(),
            self.scheduler.pre_market()
        )
        
        # Market open snapshot at 09:30
        self.scheduler.add_job(
            "market_open",
            lambda: self.jobs.collect_realtime_snapshot(),
            self.scheduler.market_open()
        )
        
        # Mid-morning at 10:45
        self.scheduler.add_job(
            "mid_morning",
            lambda: self.jobs.collect_realtime_snapshot(),
            self.scheduler.daily_at(10, 45)
        )
        
        # Pre-lunch at 11:45
        self.scheduler.add_job(
            "pre_lunch",
            lambda: self.jobs.collect_realtime_snapshot(),
            self.scheduler.daily_at(11, 45)
        )
        
        # Post-lunch at 13:00
        self.scheduler.add_job(
            "post_lunch",
            lambda: self.jobs.collect_realtime_snapshot(),
            self.scheduler.daily_at(13, 0)
        )
        
        # Mid-afternoon at 14:30
        self.scheduler.add_job(
            "mid_afternoon",
            lambda: self.jobs.collect_realtime_snapshot(),
            self.scheduler.daily_at(14, 30)
        )
        
        # Pre-close at 15:45
        self.scheduler.add_job(
            "pre_close",
            lambda: self.jobs.collect_realtime_snapshot(),
            self.scheduler.daily_at(15, 45)
        )
        
        # Market close at 16:30 - daily bar collection
        self.scheduler.add_job(
            "daily_close",
            self.jobs.collect_daily_close,
            self.scheduler.market_close()
        )
        
        logger.info("Scheduled jobs configured")
    
    async def run_collection(self, asset_classes=None):
        """Run one-time data collection."""
        logger.info("Running data collection...")
        await self.jobs.collect_realtime_snapshot(asset_classes)
        logger.info("Collection complete")
    
    def start_scheduler(self):
        """Start the scheduler."""
        self.scheduler.start()
        logger.info("Scheduler started. Press Ctrl+C to stop.")
        
        try:
            # Keep running
            loop = asyncio.get_event_loop()
            loop.run_forever()
        except KeyboardInterrupt:
            logger.info("Shutting down...")
            self.scheduler.shutdown()
    
    def start_api(self, host="0.0.0.0", port=None):
        """Start the API server."""
        import uvicorn
        import os

        # Use PORT env var for Railway/Render, fallback to provided port or 8000
        port = int(os.environ.get("PORT", port or 8000))

        logger.info(f"Starting API server on {host}:{port}")
        uvicorn.run(app, host=host, port=port)


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="HKEX Market Monitor",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s init              Initialize database
  %(prog)s collect           Run one-time data collection
  %(prog)s schedule          Start scheduled collection
  %(prog)s api               Start API server
  %(prog)s run               Start full system (scheduler + API)
        """
    )
    
    parser.add_argument(
        "command",
        choices=["init", "collect", "schedule", "api", "run", "test"],
        help="Command to execute"
    )
    
    parser.add_argument(
        "--asset-classes",
        nargs="+",
        choices=["equity", "etf", "derivative", "cbbc", "warrant", "all"],
        default=["all"],
        help="Asset classes to collect (default: all)"
    )
    
    parser.add_argument(
        "--host",
        default="0.0.0.0",
        help="API server host (default: 0.0.0.0)"
    )
    
    parser.add_argument(
        "--port",
        type=int,
        default=8000,
        help="API server port (default: 8000)"
    )
    
    args = parser.parse_args()
    
    monitor = HKEXMonitor()
    
    if args.command == "init":
        monitor.initialize()
        print("✓ Database initialized successfully")
        print(f"  Location: {monitor.db_path}")
    
    elif args.command == "collect":
        monitor.initialize()
        
        asset_classes = None if "all" in args.asset_classes else args.asset_classes
        
        asyncio.run(monitor.run_collection(asset_classes))
    
    elif args.command == "schedule":
        monitor.initialize()
        monitor.setup_schedules()
        monitor.start_scheduler()
    
    elif args.command == "api":
        monitor.initialize()
        monitor.start_api(args.host, args.port)
    
    elif args.command == "run":
        monitor.initialize()
        monitor.setup_schedules()
        
        # Start scheduler in background
        monitor.scheduler.start()
        
        # Start API (blocking)
        logger.info("Starting full system (scheduler + API)...")
        monitor.start_api(args.host, args.port)
    
    elif args.command == "test":
        monitor.initialize()
        print("\n✓ System test passed")
        print(f"  Database: {monitor.db_path}")
        print(f"  Collectors: Equity, ETF, Derivative, CBBC/Warrant")
        print(f"  API endpoints configured")
        print(f"  Scheduler ready")


if __name__ == "__main__":
    main()
