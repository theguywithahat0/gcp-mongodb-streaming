#!/usr/bin/env python3
"""
Test script for generating test data and writing to MongoDB.
Generates random orders and writes them to the test collection.
"""

import os
import sys
import asyncio
import random
import datetime
import logging
import json
from motor.motor_asyncio import AsyncIOMotorClient
from dotenv import load_dotenv
from pymongo import IndexModel, ASCENDING
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
        logger.info("Will generate exactly 12 documents")
        
        operations_count = 0
        generation_details = []
        target_docs = 12  # Fixed number of documents
        
        while operations_count < target_docs:
            order = await generate_order()
            try:
                result = await collection.insert_one(order)
                operations_count += 1
                logger.info(f"Inserted document {order['order_id']} - {operations_count}/{target_docs} documents")
                generation_time = datetime.datetime.utcnow().isoformat()
                generation_details.append({
                    "doc_id": str(result.inserted_id),
                    "order_id": order['order_id'],
                    "generated_at": generation_time,
                    "timezone": "UTC"
                })
            except Exception as e:
                if "duplicate key error" in str(e).lower():
                    logger.warning(f"Duplicate order_id {order['order_id']}, retrying...")
                    continue
                raise
                
            # Add a small lag to help monitor catch up
            await asyncio.sleep(0.1)  # 100ms lag
            if operations_count < target_docs:  # Don't sleep after the last document
                await asyncio.sleep(WRITE_INTERVAL)
            
    except Exception as e:
        logger.error(f"Error writing test data: {str(e)}")
        raise
    finally:
        client.close()
        logger.info(f"Test completed. Generated {operations_count} documents")
        # Write generation details to file
        reports_dir = Path(__file__).parent.parent / 'reports'
        reports_dir.mkdir(exist_ok=True)
        with open(reports_dir / 'generation_details.json', 'w') as f:
            json.dump(generation_details, f, indent=2)

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