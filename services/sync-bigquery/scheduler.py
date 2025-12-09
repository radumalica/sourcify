#!/usr/bin/env python3
"""
BigQuery Sync Scheduler

Runs the BigQuery sync on a schedule (default: daily at 2 AM).
"""

import os
import sys
import time
import logging
import subprocess
from datetime import datetime
from dotenv import load_dotenv
import schedule

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('/app/logs/scheduler.log')
    ]
)
logger = logging.getLogger(__name__)


def run_sync():
    """Execute the BigQuery sync"""
    logger.info("=" * 80)
    logger.info("Starting scheduled BigQuery sync")
    logger.info("=" * 80)
    
    try:
        # Run the sync script
        result = subprocess.run(
            ['python', '/app/sync_bigquery.py'],
            capture_output=True,
            text=True
        )
        
        if result.returncode == 0:
            logger.info("Sync completed successfully")
            logger.info(f"Output: {result.stdout}")
        else:
            logger.error(f"Sync failed with return code {result.returncode}")
            logger.error(f"Error output: {result.stderr}")
            
    except Exception as e:
        logger.error(f"Error running sync: {e}", exc_info=True)


def main():
    """Main scheduler function"""
    load_dotenv()
    
    # Get schedule from environment (cron format: "0 2 * * *" = daily at 2 AM)
    sync_schedule = os.getenv('SYNC_SCHEDULE', '0 2 * * *')
    
    logger.info("=" * 80)
    logger.info("BigQuery Sync Scheduler Started")
    logger.info("=" * 80)
    logger.info(f"Schedule: {sync_schedule}")
    
    # Parse cron schedule (simplified - supports daily schedules)
    # Format: "minute hour * * *"
    parts = sync_schedule.split()
    if len(parts) >= 2:
        minute = parts[0]
        hour = parts[1]
        time_str = f"{hour.zfill(2)}:{minute.zfill(2)}"
        
        logger.info(f"Scheduling daily sync at {time_str}")
        schedule.every().day.at(time_str).do(run_sync)
    else:
        logger.error(f"Invalid schedule format: {sync_schedule}")
        logger.info("Using default: daily at 02:00")
        schedule.every().day.at("02:00").do(run_sync)
    
    # Run immediately on startup if requested
    if os.getenv('RUN_ON_STARTUP', 'false').lower() == 'true':
        logger.info("Running initial sync on startup...")
        run_sync()
    
    # Main loop
    logger.info("Scheduler is running. Waiting for scheduled tasks...")
    while True:
        schedule.run_pending()
        time.sleep(60)  # Check every minute


if __name__ == '__main__':
    main()
