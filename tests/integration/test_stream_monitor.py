#!/usr/bin/env python3
"""
Test script for monitoring MongoDB change streams using our connection manager.
Watches the test collection for changes and logs them.
"""

import os
import asyncio
import logging
from motor.motor_asyncio import AsyncIOMotorClient
from dotenv import load_dotenv
from pymongo import ReadPreference

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Load environment variables from tests/.env.test
env_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), '.env.test')
load_dotenv(env_path)

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