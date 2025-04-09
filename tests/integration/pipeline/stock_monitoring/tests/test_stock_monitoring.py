#!/usr/bin/env python3
"""
Integration Test for Stock Position Monitoring Pipeline

This test verifies the end-to-end functionality of the stock position monitoring
pipeline by:
1. Generating and loading test stock trade data into MongoDB
2. Running the pipeline to process the data
3. Monitoring Pub/Sub for the expected outputs

Configuration is grouped by tenant (trading site) for clearer organization.
"""

import os
import sys
import json
import time
import random
import asyncio
import logging
import argparse
import subprocess
from pathlib import Path
from typing import Dict, List, Any, Optional, Tuple
from dotenv import load_dotenv

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Add project root to the Python path
current_dir = Path(__file__).resolve().parent
project_root = current_dir.parent.parent.parent.parent
sys.path.append(str(project_root))

# Import local modules
from tests.integration.pipeline.stock_monitoring.data_generator import (
    StockTradeGenerator, load_data_to_mongodb
)
from tests.integration.pipeline.stock_monitoring.pubsub_monitor import PubSubMonitor

# Load environment variables from .env.test file
env_path = project_root / 'tests' / '.env.test'
if not env_path.exists():
    raise FileNotFoundError(f"Test environment file not found at {env_path}")
load_dotenv(str(env_path))

# Common configuration
PROJECT_ID = os.getenv('GCP_PROJECT_ID')

# Get list of trading sites
SITES = os.getenv('TEST_TRADING_SITES', '').split(',')

# Test configuration
TEST_DURATION = int(os.getenv('TEST_DURATION', '600'))  # 10 minutes max
POSITION_THRESHOLDS = json.loads(os.getenv('TEST_POSITION_THRESHOLDS', '{"thresholds":[]}'))
WINDOW_SIZE = int(os.getenv('TEST_WINDOW_SIZE', '10'))
NUM_TRADES = int(os.getenv('TEST_NUM_DOCS', '50'))

# Get stock symbols
STOCKS = os.getenv('TEST_STOCKS', 'AAPL,MSFT,GOOGL,AMZN,TSLA').split(',')

# Pipeline script path
PIPELINE_SCRIPT = project_root / 'src' / 'pipeline' / 'stock_monitoring.py'

class TenantConfig:
    """Configuration for a single tenant (trading site)."""
    
    def __init__(self, site_name: str):
        """Initialize with site name and load configuration from environment."""
        self.site_name = site_name
        
        # MongoDB configuration
        self.mongodb_uri = os.getenv(f'TEST_MONGODB_{site_name.upper()}_URI')
        self.mongodb_db = os.getenv(f'TEST_MONGODB_{site_name.upper()}_DB')
        self.collection_name = None  # Will be generated during test setup
        
        # Pub/Sub configuration
        self.pubsub_topic = os.getenv(f'PUBSUB_TOPIC_{site_name.upper()}')
        self.pubsub_subscription = os.getenv(f'PUBSUB_SUBSCRIPTION_{site_name.upper()}')
        
        # For MongoDB connection validation
        if not self.mongodb_uri:
            raise ValueError(f"Missing MongoDB URI for site {site_name}")
        
        # For PubSub validation
        if not self.pubsub_topic:
            raise ValueError(f"Missing Pub/Sub topic for site {site_name}")
        
        if not self.pubsub_subscription:
            # Generate a subscription ID if not explicitly provided
            self.pubsub_subscription = f"{self.pubsub_topic}-sub"
    
    def __str__(self):
        """String representation for logging."""
        return (f"Tenant({self.site_name}): "
                f"MongoDB={self.mongodb_uri.split('@')[1] if '@' in self.mongodb_uri else '***'}, "
                f"Collection={self.collection_name}, "
                f"Topic={self.pubsub_topic}")

class StockMonitoringTest:
    """Integration test for the stock position monitoring pipeline."""
    
    def __init__(self):
        """Initialize the test with configuration from environment."""
        if not PROJECT_ID:
            raise ValueError("GCP_PROJECT_ID environment variable is required")
        
        if not SITES:
            raise ValueError("TEST_TRADING_SITES environment variable is required")
        
        self.project_id = PROJECT_ID
        
        # Configure each tenant
        self.tenants = {}
        for site in SITES:
            self.tenants[site] = TenantConfig(site)
            logger.info(f"Configured tenant: {self.tenants[site]}")
        
        self.pipeline_process = None
        self.monitor = None
        
        # Create output directory for reports
        self.reports_dir = current_dir.parent / 'reports'
        self.reports_dir.mkdir(exist_ok=True)
    
    async def setup(self):
        """Set up the test environment."""
        logger.info("Setting up test environment")
        
        # Generate and load test data for each tenant
        for site, tenant in self.tenants.items():
            # Create collection name with timestamp to avoid conflicts
            timestamp = int(time.time())
            tenant.collection_name = f"stock_trades_{site}_{timestamp}"
            
            # Generate test data
            generator = StockTradeGenerator(
                site=site,
                stocks=STOCKS,
                num_trades=NUM_TRADES,
                max_quantity=int(os.getenv('TEST_STOCK_MAX_QTY', '250'))
            )
            trades = generator.generate_trades()
            
            # Load data to MongoDB
            await load_data_to_mongodb(
                mongodb_uri=tenant.mongodb_uri,
                collection_name=tenant.collection_name,
                documents=trades
            )
            
            logger.info(f"Loaded {len(trades)} trades for {site} into {tenant.collection_name}")
        
        # Collect site-specific Pub/Sub topics and subscriptions
        site_topics = {
            site: tenant.pubsub_topic 
            for site, tenant in self.tenants.items()
        }
        
        site_subscriptions = {
            site: tenant.pubsub_subscription
            for site, tenant in self.tenants.items()
        }
        
        # Set up PubSub monitor
        self.monitor = PubSubMonitor(
            project_id=self.project_id,
            site_topics=site_topics,
            site_subscriptions=site_subscriptions
        )
        
        if not await self.monitor.setup():
            raise RuntimeError("Failed to set up PubSub monitor")
    
    def start_pipeline(self):
        """Start the monitoring pipeline."""
        logger.info("Starting pipeline")
        
        # Prepare command line arguments
        cmd_args = [
            sys.executable, str(PIPELINE_SCRIPT),
            "--project", self.project_id,
            "--window-size", str(WINDOW_SIZE),
            "--thresholds", json.dumps(POSITION_THRESHOLDS),
        ]
        
        # Add tenant-specific MongoDB and Pub/Sub arguments
        for site, tenant in self.tenants.items():
            cmd_args.extend([
                "--mongodb-uri", tenant.mongodb_uri,
                "--collection", tenant.collection_name,
                "--site", site,
                "--site-topic", f"{site}:{tenant.pubsub_topic}"
            ])
        
        # Log the command (but mask sensitive info)
        log_cmd = []
        skip_next = False
        for i, arg in enumerate(cmd_args):
            if skip_next:
                log_cmd.append("***")  # Mask sensitive info
                skip_next = False
            elif arg == "--mongodb-uri":
                log_cmd.append(arg)
                skip_next = True
            else:
                log_cmd.append(arg)
        
        logger.info(f"Pipeline command: {' '.join(str(arg) for arg in log_cmd)}")
        
        # Start the pipeline in a subprocess
        self.pipeline_process = subprocess.Popen(
            cmd_args,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            bufsize=1
        )
        
        logger.info(f"Pipeline started with PID {self.pipeline_process.pid}")
        
        # Start a background task to monitor pipeline output
        asyncio.create_task(self._monitor_pipeline_output())
    
    async def _monitor_pipeline_output(self):
        """Monitor and log pipeline output."""
        try:
            while self.pipeline_process and self.pipeline_process.poll() is None:
                stdout_line = self.pipeline_process.stdout.readline()
                if stdout_line:
                    logger.debug(f"Pipeline stdout: {stdout_line.strip()}")
                
                stderr_line = self.pipeline_process.stderr.readline()
                if stderr_line:
                    logger.warning(f"Pipeline stderr: {stderr_line.strip()}")
                
                await asyncio.sleep(0.1)
        except Exception as e:
            logger.error(f"Error monitoring pipeline output: {e}")
    
    async def run_test(self):
        """Run the integration test."""
        try:
            # Set up test environment
            await self.setup()
            
            # Start the pipeline
            self.start_pipeline()
            
            # Allow the pipeline to start processing
            logger.info("Waiting for pipeline to start processing...")
            await asyncio.sleep(10)
            
            # Start monitoring PubSub for output
            logger.info(f"Monitoring PubSub for {TEST_DURATION} seconds")
            await self.monitor.start_monitoring(max_wait_time=TEST_DURATION)
            
            # Get and analyze results
            results = self.monitor.get_results()
            
            # Save results to file
            results_file = self.reports_dir / 'test_results.json'
            with open(results_file, 'w') as f:
                json.dump(results, f, indent=2)
            
            logger.info(f"Test results saved to {results_file}")
            
            # Check if we received any messages
            if results['total_messages'] == 0:
                logger.error("Test failed: No messages received from pipeline")
                return False
            
            # Check if we received alerts
            if results['alert_messages'] == 0:
                logger.warning("No alert messages received - threshold may be too high")
            
            # Check if all sites have messages
            sites_with_messages = set(results.get('site_counts', {}).keys())
            missing_sites = set(self.tenants.keys()) - sites_with_messages
            
            if missing_sites:
                logger.warning(f"No messages received for sites: {', '.join(missing_sites)}")
            
            logger.info(f"Test completed successfully. Received {results['total_messages']} "
                      f"messages, {results.get('position_messages', 0)} position updates, "
                      f"{results.get('alert_messages', 0)} alerts")
            
            # Log site-specific counts
            for site, count in results.get('site_counts', {}).items():
                logger.info(f"Site {site}: {count} messages")
            
            # Log stock-specific counts
            for stock, count in results.get('stock_counts', {}).items():
                logger.info(f"Stock {stock}: {count} messages")
            
            return True
            
        except Exception as e:
            logger.error(f"Test failed with error: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return False
        finally:
            await self.cleanup()
    
    async def cleanup(self):
        """Clean up resources after the test."""
        logger.info("Cleaning up test resources")
        
        # Stop the pipeline
        if self.pipeline_process:
            logger.info(f"Stopping pipeline (PID {self.pipeline_process.pid})")
            try:
                self.pipeline_process.terminate()
                await asyncio.sleep(2)
                if self.pipeline_process.poll() is None:
                    self.pipeline_process.kill()
            except Exception as e:
                logger.error(f"Error stopping pipeline: {e}")
        
        # Close the PubSub monitor
        if self.monitor:
            await self.monitor.close()
        
        logger.info("Test cleanup completed")

async def main():
    """Main entry point for the integration test."""
    # Define global variables at the beginning of the function
    global TEST_DURATION
    
    parser = argparse.ArgumentParser(description="Run stock monitoring integration test")
    parser.add_argument("--duration", type=int, default=TEST_DURATION,
                      help="Maximum test duration in seconds")
    parser.add_argument("--mock", action="store_true",
                      help="Use mock mode to bypass MongoDB connections")
    args = parser.parse_args()
    
    # Set test duration from command line if provided
    TEST_DURATION = args.duration
    
    # Create the test
    test = StockMonitoringTest()
    
    # If mock mode is enabled, skip MongoDB connections
    if args.mock:
        logger.info("Running in mock mode - bypassing MongoDB connections")
        
        # Set up just the PubSub monitor
        site_topics = {
            site: tenant.pubsub_topic 
            for site, tenant in test.tenants.items()
        }
        
        site_subscriptions = {
            site: tenant.pubsub_subscription
            for site, tenant in test.tenants.items()
        }
        
        # Set up PubSub monitor
        test.monitor = PubSubMonitor(
            project_id=test.project_id,
            site_topics=site_topics,
            site_subscriptions=site_subscriptions
        )
        
        if not await test.monitor.setup():
            logger.error("Failed to set up PubSub monitor")
            sys.exit(1)
            
        # Directly start monitoring PubSub
        await test.monitor.start_monitoring(max_wait_time=TEST_DURATION)
        
        # Get and analyze results
        results = test.monitor.get_results()
        logger.info(f"Mock test completed. Monitored PubSub for {TEST_DURATION} seconds.")
        
        # Exit with success
        sys.exit(0)
    else:
        # Run the full test
        success = await test.run_test()
        
        # Exit with appropriate status code
        sys.exit(0 if success else 1)

if __name__ == "__main__":
    asyncio.run(main()) 