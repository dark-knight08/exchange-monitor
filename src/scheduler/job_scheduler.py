"""
Job scheduler for automated data collection and processing.
Uses APScheduler for task scheduling.
"""

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger
from datetime import datetime, time
import logging
import asyncio
from typing import Callable, Dict, Any

logger = logging.getLogger(__name__)


class JobScheduler:
    """Manages scheduled jobs for data collection."""
    
    def __init__(self, timezone: str = "Asia/Hong_Kong"):
        self.scheduler = AsyncIOScheduler(timezone=timezone)
        self.jobs: Dict[str, Any] = {}
        logger.info(f"JobScheduler initialized with timezone: {timezone}")
    
    def start(self):
        """Start the scheduler."""
        self.scheduler.start()
        logger.info("JobScheduler started")
    
    def shutdown(self):
        """Shutdown the scheduler."""
        self.scheduler.shutdown()
        logger.info("JobScheduler shutdown")
    
    def add_job(self, job_id: str, func: Callable, trigger, **kwargs):
        """Add a scheduled job."""
        job = self.scheduler.add_job(
            func,
            trigger=trigger,
            id=job_id,
            replace_existing=True,
            **kwargs
        )
        self.jobs[job_id] = job
        logger.info(f"Added job: {job_id}")
        return job
    
    def remove_job(self, job_id: str):
        """Remove a scheduled job."""
        try:
            self.scheduler.remove_job(job_id)
            del self.jobs[job_id]
            logger.info(f"Removed job: {job_id}")
        except Exception as e:
            logger.error(f"Failed to remove job {job_id}: {e}")
    
    def get_jobs(self):
        """Get all scheduled jobs."""
        return self.scheduler.get_jobs()
    
    # Predefined triggers
    @staticmethod
    def every_n_seconds(n: int):
        """Trigger every n seconds."""
        return IntervalTrigger(seconds=n)
    
    @staticmethod
    def every_n_minutes(n: int):
        """Trigger every n minutes."""
        return IntervalTrigger(minutes=n)
    
    @staticmethod
    def daily_at(hour: int, minute: int = 0):
        """Trigger daily at specific time."""
        return CronTrigger(hour=hour, minute=minute)
    
    @staticmethod
    def market_open():
        """Trigger at HKEX market open (09:30)."""
        return CronTrigger(hour=9, minute=30, day_of_week='mon-fri')
    
    @staticmethod
    def market_close():
        """Trigger at HKEX market close (16:30)."""
        return CronTrigger(hour=16, minute=30, day_of_week='mon-fri')
    
    @staticmethod
    def pre_market():
        """Trigger before market open (08:45)."""
        return CronTrigger(hour=8, minute=45, day_of_week='mon-fri')


if __name__ == "__main__":
    # Test the scheduler
    scheduler = JobScheduler()
    
    async def test_job():
        logger.info(f"Test job executed at {datetime.now()}")
    
    # Add test job
    scheduler.add_job("test", test_job, scheduler.every_n_seconds(5))
    
    scheduler.start()
    
    # Run for 20 seconds
    loop = asyncio.get_event_loop()
    loop.run_until_complete(asyncio.sleep(20))
    
    scheduler.shutdown()
