#!/usr/bin/env python3
"""
Sourcify Parquet Sync Scheduler

Runs sync.py on a schedule using cron expressions.
"""

import os
import sys
import time
import logging
import subprocess
from datetime import datetime
from croniter import croniter
from dotenv import load_dotenv


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
    """Execute the sync script"""
    logger.info(f"Starting scheduled sync at {datetime.now()}")

    try:
        result = subprocess.run(
            ['python', '/app/sync.py'],
            capture_output=True,
            text=True,
            timeout=3600 * 6  # 6 hour timeout
        )

        if result.returncode == 0:
            logger.info(f"Sync completed successfully")
            logger.debug(f"Output: {result.stdout}")
        else:
            logger.error(f"Sync failed with exit code {result.returncode}")
            logger.error(f"Error output: {result.stderr}")

        return result.returncode

    except subprocess.TimeoutExpired:
        logger.error("Sync timed out after 6 hours")
        return 1

    except Exception as e:
        logger.error(f"Error running sync: {e}", exc_info=True)
        return 1


def main():
    """Main scheduler function"""
    # Load environment variables
    load_dotenv()

    # Get cron schedule from environment
    sync_schedule = os.getenv('SYNC_SCHEDULE', '0 2 * * *')  # Default: Daily at 2 AM

    logger.info("=" * 80)
    logger.info("Sourcify Parquet Sync Scheduler")
    logger.info("=" * 80)
    logger.info(f"Schedule: {sync_schedule}")

    try:
        cron = croniter(sync_schedule, datetime.now())
        logger.info("Scheduler started successfully")

        while True:
            # Calculate next run time
            next_run = cron.get_next(datetime)
            now = datetime.now()
            sleep_seconds = (next_run - now).total_seconds()

            if sleep_seconds > 0:
                logger.info(
                    f"Next sync scheduled for: {next_run.strftime('%Y-%m-%d %H:%M:%S')} "
                    f"(in {sleep_seconds:.0f} seconds / {sleep_seconds/3600:.1f} hours)"
                )
                time.sleep(sleep_seconds)

            # Run the sync
            logger.info("-" * 80)
            exit_code = run_sync()
            logger.info(f"Sync completed with exit code: {exit_code}")
            logger.info("-" * 80)

    except KeyboardInterrupt:
        logger.info("Scheduler stopped by user")
        sys.exit(0)

    except Exception as e:
        logger.error(f"Fatal error in scheduler: {e}", exc_info=True)
        sys.exit(1)


if __name__ == '__main__':
    main()
