#!/usr/bin/env python3
"""
Minimal PubSub Test Script

This script verifies that the required Pub/Sub topics and subscriptions exist
using the gcloud CLI directly instead of the Python client.
"""

import os
import sys
import json
import subprocess
import logging
from pathlib import Path
from dotenv import load_dotenv

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(message)s')
logger = logging.getLogger(__name__)

# Add project root to the Python path
current_dir = Path(__file__).resolve().parent
project_root = current_dir.parent.parent.parent.parent
sys.path.append(str(project_root))

# Load environment variables - first try .env.test, then fall back to project .env
env_test_path = project_root / 'tests' / '.env.test'
env_main_path = project_root / '.env'

if env_test_path.exists():
    logger.info(f"Loading test environment from {env_test_path}")
    load_dotenv(str(env_test_path))
elif env_main_path.exists():
    logger.info(f"Loading environment from {env_main_path}")
    load_dotenv(str(env_main_path))
else:
    raise FileNotFoundError(f"No environment file found at {env_test_path} or {env_main_path}")

# Get GCP project
PROJECT_ID = os.getenv('GCP_PROJECT_ID')
if not PROJECT_ID:
    raise ValueError("GCP_PROJECT_ID not defined in environment file")

# Get list of trading sites
SITES = os.getenv('TEST_TRADING_SITES', '').split(',')

def run_command(cmd):
    """Run a shell command and return the output."""
    try:
        result = subprocess.run(
            cmd, 
            shell=True, 
            check=True, 
            stdout=subprocess.PIPE, 
            stderr=subprocess.PIPE, 
            text=True
        )
        return True, result.stdout
    except subprocess.CalledProcessError as e:
        return False, e.stderr

def verify_gcloud_auth():
    """Verify gcloud authentication."""
    logger.info("Verifying gcloud authentication...")
    success, output = run_command("gcloud auth list --filter=status:ACTIVE --format='value(account)'")
    
    if not success:
        logger.error("gcloud authentication error: %s", output)
        return False
    
    if not output.strip():
        logger.error("No active gcloud account found")
        return False
    
    logger.info("Authenticated as: %s", output.strip())
    return True

def verify_project():
    """Verify gcloud project configuration."""
    logger.info("Verifying gcloud project configuration...")
    success, output = run_command(f"gcloud config get-value project")
    
    if not success:
        logger.error("Error getting project: %s", output)
        return False
    
    current_project = output.strip()
    if current_project != PROJECT_ID:
        logger.warning(f"Current project ({current_project}) doesn't match .env.test ({PROJECT_ID})")
        
        # Set the project
        logger.info(f"Setting project to {PROJECT_ID}...")
        success, output = run_command(f"gcloud config set project {PROJECT_ID}")
        if not success:
            logger.error(f"Failed to set project: {output}")
            return False
    
    logger.info(f"Using project: {PROJECT_ID}")
    return True

def list_pubsub_topics():
    """List existing PubSub topics."""
    logger.info("Listing existing PubSub topics...")
    success, output = run_command(f"gcloud pubsub topics list --project={PROJECT_ID} --format=json")
    
    if not success:
        logger.error(f"Failed to list topics: {output}")
        return []
    
    try:
        topics = json.loads(output)
        return [topic['name'].split('/')[-1] for topic in topics]
    except (json.JSONDecodeError, KeyError) as e:
        logger.error(f"Error parsing topics: {e}")
        return []

def list_pubsub_subscriptions():
    """List existing PubSub subscriptions."""
    logger.info("Listing existing PubSub subscriptions...")
    success, output = run_command(f"gcloud pubsub subscriptions list --project={PROJECT_ID} --format=json")
    
    if not success:
        logger.error(f"Failed to list subscriptions: {output}")
        return []
    
    try:
        subscriptions = json.loads(output)
        return [sub['name'].split('/')[-1] for sub in subscriptions]
    except (json.JSONDecodeError, KeyError) as e:
        logger.error(f"Error parsing subscriptions: {e}")
        return []

def create_pubsub_topic(topic):
    """Create a PubSub topic if it doesn't exist."""
    logger.info(f"Creating topic '{topic}'...")
    success, output = run_command(f"gcloud pubsub topics create {topic} --project={PROJECT_ID}")
    if not success:
        if "Resource already exists" in output:
            logger.info(f"Topic '{topic}' already exists")
            return True
        else:
            logger.error(f"Failed to create topic '{topic}': {output}")
            return False
    
    logger.info(f"Topic '{topic}' created successfully")
    return True

def create_pubsub_subscription(subscription, topic):
    """Create a PubSub subscription if it doesn't exist."""
    logger.info(f"Creating subscription '{subscription}' for topic '{topic}'...")
    success, output = run_command(
        f"gcloud pubsub subscriptions create {subscription} --topic={topic} --project={PROJECT_ID}"
    )
    if not success:
        if "Resource already exists" in output:
            logger.info(f"Subscription '{subscription}' already exists")
            return True
        else:
            logger.error(f"Failed to create subscription '{subscription}': {output}")
            return False
    
    logger.info(f"Subscription '{subscription}' created successfully")
    return True

def setup_tenant_pubsub(site):
    """Set up PubSub resources for a tenant."""
    topic = os.getenv(f'PUBSUB_TOPIC_{site.upper()}')
    subscription = os.getenv(f'PUBSUB_SUBSCRIPTION_{site.upper()}')
    
    if not topic:
        logger.warning(f"No topic defined for tenant {site}")
        return False
    
    if not subscription:
        subscription = f"{topic}-sub"
    
    logger.info(f"Setting up PubSub for tenant {site}:")
    logger.info(f"  Topic: {topic}")
    logger.info(f"  Subscription: {subscription}")
    
    # Create topic
    if not create_pubsub_topic(topic):
        return False
    
    # Create subscription
    if not create_pubsub_subscription(subscription, topic):
        return False
    
    return True

def main():
    """Main entry point."""
    logger.info("=== PubSub Test Script ===")
    
    # Check if we're explicitly using cloud Pub/Sub
    if "USE_CLOUD_PUBSUB" in os.environ and os.environ["USE_CLOUD_PUBSUB"].lower() == "true":
        if "PUBSUB_EMULATOR_HOST" in os.environ:
            # Remove emulator setting for cloud mode
            logger.info("Using cloud Pub/Sub (removing emulator setting)")
            del os.environ["PUBSUB_EMULATOR_HOST"]
    # If not explicitly using cloud and emulator host not set, use emulator as fallback
    elif os.environ.get("PUBSUB_EMULATOR_HOST") is None:
        emulator_host = "localhost:8918"
        os.environ["PUBSUB_EMULATOR_HOST"] = emulator_host
        logger.info(f"Using Pub/Sub emulator at {emulator_host}")
    
    # Verify gcloud authentication
    if not verify_gcloud_auth():
        logger.error("Authentication check failed")
        return 1
    
    # Verify project configuration
    if not verify_project():
        logger.error("Project configuration check failed")
        return 1
    
    # List existing topics and subscriptions
    existing_topics = list_pubsub_topics()
    logger.info(f"Found {len(existing_topics)} existing topics")
    
    existing_subscriptions = list_pubsub_subscriptions()
    logger.info(f"Found {len(existing_subscriptions)} existing subscriptions")
    
    # Check and set up tenant-specific topics and subscriptions
    succeeded = 0
    failures = 0
    
    for site in SITES:
        logger.info(f"\n--- Setting up PubSub for tenant: {site} ---")
        if setup_tenant_pubsub(site):
            succeeded += 1
        else:
            failures += 1
    
    logger.info(f"\n=== Summary ===")
    logger.info(f"Successfully set up {succeeded} of {len(SITES)} tenants")
    
    if failures > 0:
        logger.error(f"Failed to set up {failures} tenants")
        return 1
    
    logger.info("All PubSub resources are ready")
    return 0

if __name__ == "__main__":
    sys.exit(main()) 