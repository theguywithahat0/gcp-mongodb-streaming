#!/usr/bin/env python3
"""
Script to pull messages from Google Cloud Pub/Sub subscriptions for the stock monitoring pipeline.
"""
import argparse
import json
import logging
import os
import sys
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple

from dotenv import load_dotenv
from google.cloud import pubsub_v1

# Add parent directory to path for relative imports
current_dir = Path(__file__).resolve().parent
parent_dir = current_dir.parent
sys.path.append(str(parent_dir))

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)

# Load environment variables
project_root = parent_dir.parent.parent.parent.parent
env_path = project_root / 'tests' / 'config' / '.env.integration'
logger.info(f"Looking for env file at: {env_path}")
if env_path.exists():
    logger.info(f"Loading environment from {env_path}")
    load_dotenv(str(env_path))
else:
    logger.warning(f"Environment file not found at {env_path}")

# Debug: Print relevant environment variables
logger.info("Environment variables:")
for site in ['fidelity', 'etrade', 'robinhood']:
    subscription_var = f"PUBSUB_SUBSCRIPTION_{site.upper()}"
    logger.info(f"  {subscription_var} = {os.environ.get(subscription_var)}")
logger.info(f"  TEST_TRADING_SITES = {os.environ.get('TEST_TRADING_SITES')}")

def pull_messages(project_id: str, subscription_id: str, max_messages: int = 100, 
                 acknowledge: bool = False) -> List[Dict]:
    """
    Pull messages from a Google Cloud Pub/Sub subscription.
    
    Args:
        project_id: The GCP project ID
        subscription_id: The subscription ID to pull messages from
        max_messages: Maximum number of messages to pull (default: 100)
        acknowledge: Whether to acknowledge the messages (default: False)
        
    Returns:
        List of message dictionaries
    """
    try:
        # Create subscriber client
        subscriber = pubsub_v1.SubscriberClient()
        subscription_path = subscriber.subscription_path(project_id, subscription_id)
        
        # Pull messages
        logger.info(f"Pulling up to {max_messages} messages from {subscription_id}...")
        response = subscriber.pull(
            request={"subscription": subscription_path, "max_messages": max_messages}
        )
        
        messages = []
        for received_message in response.received_messages:
            try:
                # Parse message data
                message_data = json.loads(received_message.message.data.decode("utf-8"))
                
                # Add publish time
                publish_time = received_message.message.publish_time.isoformat()
                message_data["publish_time"] = publish_time
                
                # Add ack_id for potential acknowledgment
                message_data["ack_id"] = received_message.ack_id
                
                messages.append(message_data)
                
                # Format and display the message
                # Check for common stock symbol names
                symbol = message_data.get("symbol") or message_data.get("stock_symbol", "N/A")
                trade_type = message_data.get("trade_type", "N/A")
                quantity = message_data.get("quantity", "N/A")
                price = message_data.get("price", "N/A")
                
                logger.info(f"Message: {symbol} | {trade_type} | {quantity} | ${price} | {publish_time}")
                
            except json.JSONDecodeError:
                logger.warning(f"Failed to decode message: {received_message.message.data}")
                
        # Acknowledge messages if requested
        if acknowledge and messages:
            ack_ids = [msg["ack_id"] for msg in messages]
            subscriber.acknowledge(request={"subscription": subscription_path, "ack_ids": ack_ids})
            logger.info(f"Acknowledged {len(ack_ids)} messages")
            
        # Close the subscriber
        subscriber.close()
        
        return messages
        
    except Exception as e:
        logger.error(f"Error pulling messages: {str(e)}")
        return []

def main():
    """Main function to pull and display messages from Pub/Sub subscriptions."""
    # Define command line arguments
    parser = argparse.ArgumentParser(description="Pull messages from Google Cloud Pub/Sub subscriptions")
    parser.add_argument("--project-id", type=str, default="jh-testing-project",
                        help="Google Cloud project ID (default: jh-testing-project)")
    parser.add_argument("--sites", type=str, default="all",
                        help="Comma-separated list of sites to pull messages from, or 'all' (default: all)")
    parser.add_argument("--max-messages", type=int, default=10,
                        help="Maximum number of messages to pull per subscription (default: 10)")
    parser.add_argument("--acknowledge", action="store_true",
                        help="Acknowledge the messages after pulling (default: False)")
    
    args = parser.parse_args()
    
    # Get sites from environment
    available_sites = os.environ.get('TEST_TRADING_SITES', 'fidelity,etrade,robinhood').split(',')
    
    # Define subscription IDs for different sites based on environment variables
    subscription_map = {}
    for site in available_sites:
        subscription_env_var = f"PUBSUB_SUBSCRIPTION_{site.upper()}"
        subscription_id = os.environ.get(subscription_env_var)
        if subscription_id:
            subscription_map[site] = subscription_id
        else:
            logger.warning(f"No subscription ID found for {site} (environment variable {subscription_env_var} not set)")
    
    if not subscription_map:
        logger.error("No valid subscriptions found in environment variables")
        return
    
    # Determine which sites to pull messages from
    sites_to_pull = list(subscription_map.keys()) if args.sites.lower() == "all" else args.sites.split(",")
    
    # Pull messages from each subscription
    total_messages = 0
    for site in sites_to_pull:
        if site not in subscription_map:
            logger.warning(f"Unknown site: {site}. Skipping.")
            continue
            
        subscription_id = subscription_map[site]
        logger.info(f"Pulling messages for site: {site} (Subscription: {subscription_id})")
        
        messages = pull_messages(
            project_id=args.project_id,
            subscription_id=subscription_id,
            max_messages=args.max_messages,
            acknowledge=args.acknowledge
        )
        
        logger.info(f"Retrieved {len(messages)} messages from {site}")
        total_messages += len(messages)
    
    logger.info(f"Total messages pulled: {total_messages}")

if __name__ == "__main__":
    main() 