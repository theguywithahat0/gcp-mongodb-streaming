#!/usr/bin/env python3
"""
Simplified Mock Test for Stock Position Monitoring

This test focuses only on monitoring Pub/Sub topics for tenant-specific messages,
bypassing MongoDB connections entirely and using live Google Cloud Pub/Sub.
"""

import os
import sys
import json
import asyncio
import logging
import argparse
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, List
from dotenv import load_dotenv

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Add parent directory to path for relative imports
current_dir = Path(__file__).resolve().parent
parent_dir = current_dir.parent
sys.path.append(str(parent_dir))

# Import local modules
from utils.pubsub_monitor import PubSubMonitor

# Load environment variables - first try .env.test, then fall back to project .env
env_test_path = parent_dir / 'tests' / '.env.test'
env_main_path = parent_dir / '.env'

if env_test_path.exists():
    logger.info(f"Loading test environment from {env_test_path}")
    load_dotenv(str(env_test_path))
elif env_main_path.exists():
    logger.info(f"Test environment not found, using project environment from {env_main_path}")
    load_dotenv(str(env_main_path))
else:
    raise FileNotFoundError(f"No environment file found at {env_test_path} or {env_main_path}")

# Common configuration
PROJECT_ID = os.getenv('GCP_PROJECT_ID')

# Get list of trading sites
SITES = os.getenv('TEST_TRADING_SITES', '').split(',')

# Test duration in seconds
TEST_DURATION = int(os.getenv('TEST_DURATION', '30'))


async def run_mock_test(duration=30):
    """
    Run a simplified test that just monitors Pub/Sub for messages.
    
    Args:
        duration: How long to monitor for messages in seconds
    """
    logger.info("Starting mock PubSub test")
    
    # Collect Pub/Sub topics and subscriptions for each tenant
    site_topics = {}
    site_subscriptions = {}
    
    for site in SITES:
        topic = os.getenv(f'PUBSUB_TOPIC_{site.upper()}')
        subscription = os.getenv(f'PUBSUB_SUBSCRIPTION_{site.upper()}')
        
        if not topic:
            logger.warning(f"Missing Pub/Sub topic for site {site}")
            continue
            
        if not subscription:
            subscription = f"{topic}-sub"
            
        site_topics[site] = topic
        site_subscriptions[site] = subscription
        
        logger.info(f"Configured tenant {site}: Topic={topic}, Subscription={subscription}")
    
    if not site_topics:
        logger.error("No valid tenants configured")
        return False
        
    # Set up PubSub monitor
    monitor = PubSubMonitor(
        project_id=PROJECT_ID,
        site_topics=site_topics,
        site_subscriptions=site_subscriptions
    )
    
    try:
        # Set up PubSub resources
        if not await monitor.setup():
            logger.error("Failed to set up PubSub monitor")
            return False
            
        # Start monitoring
        logger.info(f"Monitoring PubSub for {duration} seconds...")
        await monitor.start_monitoring(max_wait_time=duration)
        
        # Get and analyze results
        results = monitor.get_results()
        
        # Save results to file
        reports_dir = parent_dir / 'reports'
        reports_dir.mkdir(exist_ok=True)
        results_file = reports_dir / f'mock_test_results_{datetime.now().strftime("%Y-%m-%d_%H-%M-%S")}.json'
        
        with open(results_file, 'w') as f:
            json.dump(results, f, indent=2)
            
        logger.info(f"Test results saved to {results_file}")
        
        # Report results
        logger.info(f"Test completed. Received {results.get('total_messages', 0)} messages")
        
        # Log tenant-based summary
        for site, summary in results.get('tenant_summary', {}).items():
            logger.info(f"Tenant {site}: {summary['total_messages']} messages")
            
        return True
        
    except Exception as e:
        logger.error(f"Test failed with error: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return False
    finally:
        # Close the monitor
        await monitor.close()
        logger.info("Test cleanup completed")


async def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(description="Run a mock test for Pub/Sub monitoring")
    parser.add_argument('--duration', type=int, default=30, help='How long to monitor for messages (seconds)')
    args = parser.parse_args()
    
    try:
        # Run the mock test
        success = await run_mock_test(
            duration=args.duration
        )
        return 0 if success else 1
    except KeyboardInterrupt:
        logger.info("Test interrupted by user")
        return 130
    except Exception as e:
        logger.exception(f"Test failed with error: {e}")
        return 1


if __name__ == "__main__":
    asyncio.run(main()) 