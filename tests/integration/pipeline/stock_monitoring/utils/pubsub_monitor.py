#!/usr/bin/env python3
"""
PubSub Monitor for Pipeline Integration Test

This script monitors Pub/Sub subscriptions for messages produced by the pipeline,
capturing the data for analysis.

It supports tenant-specific topics with results grouped by tenant for better
organization and analysis.
"""

import os
import sys
import asyncio
import json
import logging
import datetime
import time
from pathlib import Path
from typing import List, Dict, Any, Optional, Set
from google.cloud import pubsub_v1
from google.api_core.exceptions import NotFound
from concurrent.futures import TimeoutError
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
from utils.pubsub_client import PubSubClient

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

# GCP and PubSub configuration
PROJECT_ID = os.getenv('GCP_PROJECT_ID')

# Get the list of sites
SITES = os.getenv('TEST_TRADING_SITES', '').split(',')

# Site-specific topics and subscriptions
SITE_TOPICS = {}
SITE_SUBSCRIPTIONS = {}
for site in SITES:
    topic_env = f'PUBSUB_TOPIC_{site.upper()}'
    sub_env = f'PUBSUB_SUBSCRIPTION_{site.upper()}'
    if os.getenv(topic_env):
        SITE_TOPICS[site] = os.getenv(topic_env)
        SITE_SUBSCRIPTIONS[site] = os.getenv(sub_env) or f"{os.getenv(topic_env)}-sub"

# Test configuration
MAX_WAIT_TIME = int(os.getenv('TEST_MAX_WAIT_TIME', '30'))
MIN_EXPECTED_DOCS = int(os.getenv('TEST_MIN_DOCS', '50'))

class PubSubMonitor:
    """Monitors Pub/Sub subscriptions for messages."""
    
    def __init__(
        self, 
        project_id: str, 
        site_topics: Dict[str, str],
        site_subscriptions: Optional[Dict[str, str]] = None
    ):
        """Initialize the monitor with GCP project and subscription details."""
        self.project_id = project_id
        self.site_topics = site_topics
        self.site_subscriptions = site_subscriptions or {
            site: f"{topic}-sub" for site, topic in site_topics.items()
        }
        
        # Create PubSub client
        self.pubsub_client = PubSubClient(project_id)
        
        # Shared message store
        self.messages = []
        self.message_ids = set()  # Track message IDs to avoid duplicates
        self.running = False
        self.start_time = None
        self.end_time = None
        
        # Active subscriptions
        self.active_subscriptions = []
    
    async def setup(self):
        """Set up all topics and subscriptions."""
        try:
            # Setup site-specific topics and subscriptions
            for site, topic_id in self.site_topics.items():
                sub_id = self.site_subscriptions[site]
                self.pubsub_client.create_topic(topic_id)
                self.pubsub_client.create_subscription(topic_id, sub_id)
                logger.info(f"Set up topic {topic_id} and subscription {sub_id} for {site}")
            
            return True
        except Exception as e:
            logger.error(f"Error setting up PubSub resources: {e}")
            return False
    
    def _message_callback(self, message):
        """Process a received message."""
        try:
            message_data = message.data.decode("utf-8")
            message_attributes = {k: v for k, v in message.attributes.items()}
            
            # Parse JSON data
            try:
                data = json.loads(message_data)
            except json.JSONDecodeError:
                logger.warning(f"Received non-JSON message: {message_data}")
                data = {"raw": message_data}
            
            # Check for duplicate message (based on content hash)
            msg_hash = hash(message_data)
            if msg_hash in self.message_ids:
                logger.debug("Received duplicate message, acknowledging and skipping")
                message.ack()
                return
            
            self.message_ids.add(msg_hash)
            
            # Get message source details
            site = data.get('site', message_attributes.get('site', 'unknown'))
            stock = data.get('stock', data.get('stock_symbol', 'unknown'))
            
            # Record the message
            received_time = datetime.datetime.now(datetime.UTC).isoformat()
            message_record = {
                "data": data,
                "attributes": message_attributes,
                "received_at": received_time,
                "site": site  # Add site directly for easier analysis
            }
            
            # Add subscription if available
            try:
                message_record["subscription"] = message.subscription
            except AttributeError:
                # Handle case where message doesn't have subscription attribute
                for site_name, sub_id in self.site_subscriptions.items():
                    if site == site_name:
                        message_record["subscription"] = sub_id
                        break
                else:
                    message_record["subscription"] = "unknown"
            
            self.messages.append(message_record)
            
            # Log receipt
            is_alert = "ALERT" if data.get('alert') is True else ""
            logger.info(f"Received message {len(self.messages)}: "
                      f"{site}/{stock} {is_alert}")
            
            # Acknowledge the message
            message.ack()
            
        except Exception as e:
            logger.error(f"Error processing message: {e}")
            # Negative acknowledge in case of error
            message.nack()
    
    async def start_monitoring(self, max_wait_time: int = 30):
        """Start monitoring for messages with a maximum wait time."""
        if self.running:
            logger.warning("Monitor is already running")
            return
        
        self.running = True
        self.start_time = datetime.datetime.now(datetime.UTC)
        self.messages = []
        self.message_ids = set()
        
        logger.info(f"Starting to monitor PubSub subscriptions")
        
        # Site subscriptions
        for site, sub_id in self.site_subscriptions.items():
            self.active_subscriptions.append(
                self.pubsub_client.subscribe(
                    sub_id,
                    self._message_callback
                )
            )
            logger.info(f"Monitoring subscription {sub_id} for {site}")
        
        # Wait for specified time
        await asyncio.sleep(max_wait_time)
        
        # Stop monitoring
        self.stop_monitoring()
    
    def stop_monitoring(self):
        """Stop the monitoring process."""
        if not self.running:
            return
        
        self.running = False
        self.end_time = datetime.datetime.now(datetime.UTC)
        
        # Stop all subscriptions
        self.pubsub_client.stop_all_subscriptions()
        self.active_subscriptions = []
        
        duration = (self.end_time - self.start_time).total_seconds()
        logger.info(f"Monitoring completed. Received {len(self.messages)} "
                  f"messages in {duration:.1f} seconds")
    
    def get_results(self) -> Dict[str, Any]:
        """Get the monitoring results with tenant-based grouping."""
        if not self.start_time or not self.end_time:
            return {
                "status": "not_run",
                "messages": []
            }
        
        duration = (self.end_time - self.start_time).total_seconds()
        
        # Group messages by tenant (site)
        tenant_messages = {}
        for msg in self.messages:
            data = msg['data']
            site = data.get('site', msg['attributes'].get('site', 'unknown'))
            
            if site not in tenant_messages:
                tenant_messages[site] = []
            
            tenant_messages[site].append(msg)
        
        # Calculate statistics on messages
        positions = [m for m in self.messages if 'position' in m['data']]
        alerts = [m for m in self.messages if m['data'].get('alert') is True]
        
        # Count by site
        site_counts = {
            site: len(messages) for site, messages in tenant_messages.items()
        }
        
        # Count by stock
        stock_counts = {}
        for msg in self.messages:
            data = msg['data']
            stock = data.get('stock', data.get('stock_symbol', 'unknown'))
            
            if stock not in stock_counts:
                stock_counts[stock] = 0
            
            stock_counts[stock] += 1
        
        # Count alerts by site and stock
        alert_counts = {
            "by_site": {},
            "by_stock": {}
        }
        
        for msg in alerts:
            data = msg['data']
            site = data.get('site', msg['attributes'].get('site', 'unknown'))
            stock = data.get('stock', data.get('stock_symbol', 'unknown'))
            
            # Count by site
            if site not in alert_counts["by_site"]:
                alert_counts["by_site"][site] = 0
            alert_counts["by_site"][site] += 1
            
            # Count by stock
            if stock not in alert_counts["by_stock"]:
                alert_counts["by_stock"][stock] = 0
            alert_counts["by_stock"][stock] += 1
        
        # Create tenant-based summary
        tenant_summary = {}
        for site, messages in tenant_messages.items():
            site_positions = [m for m in messages if 'position' in m['data']]
            site_alerts = [m for m in messages if m['data'].get('alert') is True]
            
            # Count stocks for this site
            site_stocks = set()
            for msg in messages:
                data = msg['data']
                stock = data.get('stock', data.get('stock_symbol', 'unknown'))
                if stock != 'unknown':
                    site_stocks.add(stock)
            
            tenant_summary[site] = {
                "total_messages": len(messages),
                "position_messages": len(site_positions),
                "alert_messages": len(site_alerts),
                "unique_stocks": len(site_stocks),
                "stocks": list(site_stocks)
            }
        
        return {
            "status": "completed",
            "start_time": self.start_time.isoformat(),
            "end_time": self.end_time.isoformat(),
            "duration": duration,
            "total_messages": len(self.messages),
            "position_messages": len(positions),
            "alert_messages": len(alerts),
            "site_counts": site_counts,
            "stock_counts": stock_counts,
            "alert_counts": alert_counts,
            "tenant_summary": tenant_summary,
            "messages": self.messages
        }
    
    async def close(self):
        """Close the PubSub connections."""
        if self.running:
            self.stop_monitoring()
        
        await self.pubsub_client.close()
        logger.info("Closed PubSub connections")

async def main():
    """Main entry point."""
    if not PROJECT_ID:
        raise ValueError("GCP_PROJECT_ID environment variable is required")
    
    # Create directory for reports
    reports_dir = Path(__file__).resolve().parent.parent / 'reports'
    reports_dir.mkdir(exist_ok=True)
    
    # Initialize monitor with site-specific topics
    monitor = PubSubMonitor(
        project_id=PROJECT_ID,
        site_topics=SITE_TOPICS,
        site_subscriptions=SITE_SUBSCRIPTIONS
    )
    
    try:
        # Set up PubSub resources
        if not await monitor.setup():
            sys.exit(1)
        
        # Start monitoring for messages
        await monitor.start_monitoring(max_wait_time=MAX_WAIT_TIME)
        
        # Get and save results
        results = monitor.get_results()
        
        # Write results to file
        with open(reports_dir / 'pubsub_monitor_results.json', 'w') as f:
            json.dump(results, f, indent=2)
            logger.info(f"Wrote monitor results to {reports_dir / 'pubsub_monitor_results.json'}")
        
        # Check if we received enough messages
        if results['total_messages'] < MIN_EXPECTED_DOCS:
            logger.error(f"Received too few messages: {results['total_messages']} "
                       f"(expected at least {MIN_EXPECTED_DOCS})")
            sys.exit(1)
        
        # Log tenant-based summary
        logger.info(f"Successfully received {results['total_messages']} messages "
                  f"({results['alert_messages']} alerts)")
        
        for site, summary in results.get('tenant_summary', {}).items():
            logger.info(f"Tenant {site}: {summary['total_messages']} messages, "
                      f"{summary['alert_messages']} alerts, "
                      f"{summary['unique_stocks']} stocks")
        
    except KeyboardInterrupt:
        logger.info("Test interrupted by user")
    except Exception as e:
        logger.error(f"Test failed: {str(e)}")
        raise
    finally:
        await monitor.close()

if __name__ == '__main__':
    asyncio.run(main()) 