#!/usr/bin/env python3
"""
Publish Test Messages to PubSub

This script generates and publishes test stock trade messages to PubSub topics.
"""

import os
import sys
import json
import asyncio
import random
import logging
import argparse
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Any, Optional
from dotenv import load_dotenv

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(message)s')
logger = logging.getLogger(__name__)

# Add project root to the Python path
current_dir = Path(__file__).resolve().parent
project_root = current_dir.parent.parent.parent.parent
sys.path.append(str(project_root))

# Import local modules
from tests.integration.pipeline.stock_monitoring.pubsub_client import PubSubClient
from tests.integration.pipeline.stock_monitoring.data_generator import StockTradeGenerator

# Load environment variables - first try .env.test, then fall back to project .env
env_test_path = project_root / 'tests' / '.env.test'
env_main_path = project_root / '.env'

if env_test_path.exists():
    logger.info(f"Loading test environment from {env_test_path}")
    load_dotenv(str(env_test_path))
elif env_main_path.exists():
    logger.info(f"Test environment not found, using project environment from {env_main_path}")
    load_dotenv(str(env_main_path))
else:
    raise FileNotFoundError(f"No environment file found at {env_test_path} or {env_main_path}")

# Get GCP project
PROJECT_ID = os.getenv('GCP_PROJECT_ID')
if not PROJECT_ID:
    raise ValueError("GCP_PROJECT_ID not defined in .env.test")

# Get list of trading sites
SITES = os.getenv('TEST_TRADING_SITES', '').split(',')

# Get stock symbols
STOCKS = os.getenv('TEST_STOCKS', 'AAPL,MSFT,GOOGL,AMZN,TSLA').split(',')

async def publish_messages(
    project_id: str,
    site: str,
    topic_id: str,
    num_messages: int,
    delay: float = 0.1
):
    """Publish test messages to a PubSub topic."""
    # Create PubSub client
    client = PubSubClient(project_id=project_id)
    
    # Create the topic
    client.create_topic(topic_id)
    logger.info(f"Publishing {num_messages} messages to topic '{topic_id}' for site '{site}'")
    
    # Generate trades
    generator = StockTradeGenerator(
        site=site,
        stocks=STOCKS,
        num_trades=num_messages,
        max_quantity=250
    )
    
    trades = generator.generate_trades()
    logger.info(f"Generated {len(trades)} trade messages")
    
    # Publish trades
    count = 0
    for trade in trades:
        # Convert datetime to string for JSON serialization
        if isinstance(trade.get('timestamp'), datetime):
            trade['timestamp'] = trade['timestamp'].isoformat()
        
        # Add message attributes
        attributes = {
            'site': site,
            'message_type': 'stock_trade',
            'stock_symbol': trade['stock_symbol'],
            'trader_id': trade['trader_id']
        }
        
        # Publish the message
        try:
            message_id = await client.publish_message(
                topic_id=topic_id,
                data=trade,
                attributes=attributes
            )
            count += 1
            
            if count % 10 == 0 or count == num_messages:
                logger.info(f"Published {count}/{num_messages} messages")
            
            # Add a delay to avoid overwhelming the emulator
            if delay > 0:
                await asyncio.sleep(delay)
                
        except Exception as e:
            logger.error(f"Error publishing message: {e}")
    
    # Close the client
    await client.close()
    
    logger.info(f"Finished publishing {count} messages to topic '{topic_id}'")
    return count

async def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(description="Publish test messages to PubSub")
    parser.add_argument("--project", default=PROJECT_ID, help="GCP project ID")
    parser.add_argument("--site", choices=SITES, help="Trading site to publish for (if not specified, all sites)")
    parser.add_argument("--messages", type=int, default=10, help="Number of messages to publish per topic")
    parser.add_argument("--delay", type=float, default=0.1, help="Delay between messages in seconds")
    args = parser.parse_args()
    
    # Determine which sites to publish for
    target_sites = [args.site] if args.site else SITES
    
    # Check that topics are configured
    sites_to_publish = []
    for site in target_sites:
        topic = os.getenv(f'PUBSUB_TOPIC_{site.upper()}')
        if not topic:
            logger.warning(f"No topic defined for site {site}, skipping")
            continue
        sites_to_publish.append((site, topic))
    
    if not sites_to_publish:
        logger.error("No valid sites to publish for")
        return 1
    
    # Publish messages for each site
    total_published = 0
    for site, topic in sites_to_publish:
        logger.info(f"\n--- Publishing for {site} ---")
        count = await publish_messages(
            project_id=args.project,
            site=site,
            topic_id=topic,
            num_messages=args.messages,
            delay=args.delay
        )
        total_published += count
    
    logger.info(f"\n=== Summary ===")
    logger.info(f"Total published messages: {total_published}")
    
    return 0

if __name__ == "__main__":
    sys.exit(asyncio.run(main())) 