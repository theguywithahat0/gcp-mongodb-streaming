#!/usr/bin/env python3
"""
Test script for monitoring MongoDB change streams using our connection manager.
Watches the test collection for changes and logs them.
"""

import os
import sys
import asyncio
import logging
from motor.motor_asyncio import AsyncIOMotorClient
from dotenv import load_dotenv
from pymongo import ReadPreference
from pathlib import Path

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Add the project root to the Python path
current_dir = Path(__file__).resolve().parent
project_root = current_dir.parent.parent.parent.parent
sys.path.append(str(project_root))

# Add the integration tests directory to the Python path for fixtures
integration_dir = current_dir.parent.parent
sys.path.append(str(integration_dir))

# Load environment variables from .env.integration
env_path = project_root / 'tests' / 'config' / '.env.integration'
if not env_path.exists():
    raise FileNotFoundError(
        f"Integration test environment file not found at {env_path}. "
        "Please ensure .env.integration exists in the tests/config directory."
    )
load_dotenv(str(env_path))
logger.info(f"Loaded integration test environment from {env_path}")

async def monitor_changes(uri, database, collection, duration):
    """Simple change stream monitor implementation."""
    client = AsyncIOMotorClient(uri)
    db = client[database]
    coll = db[collection]
    
    try:
        logger.info(f"Starting change stream monitor on {database}.{collection}")
        
        # Start from now
        start_time = asyncio.get_event_loop().time()
        processed = 0
        
        async with coll.watch(
            pipeline=[],
            full_document='updateLookup',
            max_await_time_ms=1000
        ) as stream:
            while (asyncio.get_event_loop().time() - start_time) < duration:
                try:
                    async for change in stream:
                        processed += 1
                        logger.info(f"Change detected: {change['operationType']} - Document: {change.get('fullDocument')}")
                        
                        if (asyncio.get_event_loop().time() - start_time) >= duration:
                            break
                            
                except Exception as e:
                    logger.error(f"Error processing change: {e}")
                    # Brief pause before continuing
                    await asyncio.sleep(1)
                    
        logger.info(f"Monitor completed. Processed {processed} changes.")
                    
    except Exception as e:
        logger.error(f"Error in change stream: {e}")
        raise
    finally:
        client.close()

async def run_monitor():
    # Test configuration
    TEST_DURATION = int(os.getenv('TEST_DURATION', '60'))
    MONGODB_URI = os.getenv('MONGODB_TEST_URI')
    DATABASE = os.getenv('MONGODB_TEST_DB', 'source_db_test')
    COLLECTION = os.getenv('MONGODB_TEST_COLLECTION', 'source_collection_test')

    if not MONGODB_URI:
        raise ValueError("MONGODB_TEST_URI environment variable is required")

    try:
        await monitor_changes(MONGODB_URI, DATABASE, COLLECTION, TEST_DURATION)
    except Exception as e:
        logger.error(f"Monitor failed: {e}")
        raise

if __name__ == '__main__':
    try:
        asyncio.run(run_monitor())
    except KeyboardInterrupt:
        logger.info("Monitor stopped by user")
    except Exception as e:
        logger.error(f"Monitor failed: {e}")
        raise 