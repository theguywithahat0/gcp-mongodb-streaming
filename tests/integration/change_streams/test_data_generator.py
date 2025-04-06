#!/usr/bin/env python3
"""
Test script for generating test data and writing to MongoDB.
Generates random orders and writes them to the test collection.
"""

import os
import asyncio
import random
import datetime
import logging
from motor.motor_asyncio import AsyncIOMotorClient
from dotenv import load_dotenv
from pymongo import IndexModel, ASCENDING

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Load environment variables from tests/.env.test
env_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), '.env.test')
load_dotenv(env_path)

# MongoDB configuration
MONGODB_URI = os.getenv('MONGODB_TEST_URI')
DATABASE = os.getenv('MONGODB_TEST_DB', 'source_db_test')
COLLECTION = os.getenv('MONGODB_TEST_COLLECTION', 'source_collection_test')

# Test configuration
WRITE_INTERVAL = int(os.getenv('TEST_WRITE_INTERVAL', '2'))
TEST_DURATION = int(os.getenv('TEST_DURATION', '60'))

# Sample data generation
PRODUCTS = ['laptop', 'phone', 'tablet', 'watch', 'headphones']
STATUSES = ['pending', 'processing', 'shipped', 'delivered']

async def setup_collection(collection):
    """Set up collection with necessary indexes."""
    logger.info("Setting up collection indexes...")
    
    # Create indexes
    indexes = [
        IndexModel([("order_id", ASCENDING)], unique=True),
        IndexModel([("created_at", ASCENDING)]),
        IndexModel([("status", ASCENDING)])
    ]
    
    try:
        await collection.create_indexes(indexes)
        logger.info("Collection indexes created successfully")
    except Exception as e:
        logger.warning(f"Error creating indexes: {e}")

async def generate_order():
    """Generate a random order document."""
    return {
        'order_id': f'ORD-{random.randint(10000, 99999)}',
        'product': random.choice(PRODUCTS),
        'quantity': random.randint(1, 5),
        'status': random.choice(STATUSES),
        'price': round(random.uniform(100, 1000), 2),
        'created_at': datetime.datetime.utcnow(),
        'customer_id': f'CUST-{random.randint(1000, 9999)}'
    }

async def write_test_data():
    """Write test data to MongoDB at regular intervals."""
    client = AsyncIOMotorClient(MONGODB_URI)
    db = client[DATABASE]
    collection = db[COLLECTION]
    
    try:
        # Ensure collection exists and has proper indexes
        await setup_collection(collection)
        
        logger.info(f"Connected to MongoDB. Writing to {DATABASE}.{COLLECTION}")
        logger.info(f"Will generate data for {TEST_DURATION} seconds, one document every {WRITE_INTERVAL} seconds")
        
        start_time = datetime.datetime.utcnow()
        operations_count = 0
        
        while (datetime.datetime.utcnow() - start_time).total_seconds() < TEST_DURATION:
            order = await generate_order()
            try:
                result = await collection.insert_one(order)
                operations_count += 1
                logger.info(f"Inserted document {order['order_id']} - {operations_count} operations so far")
            except Exception as e:
                if "duplicate key error" in str(e).lower():
                    logger.warning(f"Duplicate order_id {order['order_id']}, retrying...")
                    continue
                raise
                
            await asyncio.sleep(WRITE_INTERVAL)
            
    except Exception as e:
        logger.error(f"Error writing test data: {str(e)}")
        raise
    finally:
        client.close()
        logger.info(f"Test completed. Generated {operations_count} documents")

if __name__ == '__main__':
    if not MONGODB_URI:
        raise ValueError("MONGODB_TEST_URI environment variable is required")
        
    try:
        asyncio.run(write_test_data())
    except KeyboardInterrupt:
        logger.info("Test interrupted by user")
    except Exception as e:
        logger.error(f"Test failed: {str(e)}")
        raise  # Re-raise to see the full traceback 