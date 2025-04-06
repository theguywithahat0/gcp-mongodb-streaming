#!/usr/bin/env python3
"""
Test data generator for validator integration testing.
Generates both valid and invalid test documents to verify validation rules.
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
TEST_DURATION = int(os.getenv('TEST_DURATION', '30'))  # Shorter duration for validator testing

# Sample data generation
PRODUCTS = ['laptop', 'phone', 'tablet', 'watch', 'headphones']
STATUSES = ['pending', 'processing', 'shipped', 'delivered']

# Invalid data cases
INVALID_PRODUCTS = ['desktop', 'printer', 'scanner']  # Products not in allowed list
INVALID_STATUSES = ['cancelled', 'refunded', 'unknown']  # Statuses not in allowed list

async def setup_collection(collection):
    """Set up collection with necessary indexes."""
    logger.info("Setting up collection indexes...")
    
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

async def generate_valid_order():
    """Generate a valid order document."""
    return {
        'order_id': f'ORD-{random.randint(10000, 99999)}',
        'product': random.choice(PRODUCTS),
        'quantity': random.randint(1, 10),
        'status': random.choice(STATUSES),
        'price': round(random.uniform(100, 1000), 2),
        'created_at': datetime.datetime.utcnow(),
        'customer_id': f'CUST-{random.randint(1000, 9999)}'
    }

async def generate_invalid_order():
    """Generate an invalid order document with various validation issues."""
    invalid_cases = [
        # Invalid product
        lambda: {
            'order_id': f'ORD-{random.randint(10000, 99999)}',
            'product': random.choice(INVALID_PRODUCTS),
            'quantity': random.randint(1, 10),
            'status': random.choice(STATUSES),
            'price': round(random.uniform(100, 1000), 2),
            'created_at': datetime.datetime.utcnow(),
            'customer_id': f'CUST-{random.randint(1000, 9999)}'
        },
        # Invalid quantity
        lambda: {
            'order_id': f'ORD-{random.randint(10000, 99999)}',
            'product': random.choice(PRODUCTS),
            'quantity': random.randint(11, 20),  # Exceeds max quantity
            'status': random.choice(STATUSES),
            'price': round(random.uniform(100, 1000), 2),
            'created_at': datetime.datetime.utcnow(),
            'customer_id': f'CUST-{random.randint(1000, 9999)}'
        },
        # Invalid status
        lambda: {
            'order_id': f'ORD-{random.randint(10000, 99999)}',
            'product': random.choice(PRODUCTS),
            'quantity': random.randint(1, 10),
            'status': random.choice(INVALID_STATUSES),
            'price': round(random.uniform(100, 1000), 2),
            'created_at': datetime.datetime.utcnow(),
            'customer_id': f'CUST-{random.randint(1000, 9999)}'
        },
        # Invalid price
        lambda: {
            'order_id': f'ORD-{random.randint(10000, 99999)}',
            'product': random.choice(PRODUCTS),
            'quantity': random.randint(1, 10),
            'status': random.choice(STATUSES),
            'price': -round(random.uniform(100, 1000), 2),  # Negative price
            'created_at': datetime.datetime.utcnow(),
            'customer_id': f'CUST-{random.randint(1000, 9999)}'
        },
        # Invalid order_id format
        lambda: {
            'order_id': f'ORDER-{random.randint(10000, 99999)}',  # Wrong prefix
            'product': random.choice(PRODUCTS),
            'quantity': random.randint(1, 10),
            'status': random.choice(STATUSES),
            'price': round(random.uniform(100, 1000), 2),
            'created_at': datetime.datetime.utcnow(),
            'customer_id': f'CUST-{random.randint(1000, 9999)}'
        },
        # Invalid customer_id format
        lambda: {
            'order_id': f'ORD-{random.randint(10000, 99999)}',
            'product': random.choice(PRODUCTS),
            'quantity': random.randint(1, 10),
            'status': random.choice(STATUSES),
            'price': round(random.uniform(100, 1000), 2),
            'created_at': datetime.datetime.utcnow(),
            'customer_id': f'CUSTOMER-{random.randint(1000, 9999)}'  # Wrong prefix
        }
    ]
    return random.choice(invalid_cases)()

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
        valid_count = 0
        invalid_count = 0
        
        while (datetime.datetime.utcnow() - start_time).total_seconds() < TEST_DURATION:
            # Alternate between valid and invalid documents
            order = await generate_valid_order() if operations_count % 2 == 0 else await generate_invalid_order()
            
            try:
                result = await collection.insert_one(order)
                operations_count += 1
                if operations_count % 2 == 0:
                    valid_count += 1
                    logger.info(f"Inserted valid document {order['order_id']} - Valid: {valid_count}, Invalid: {invalid_count}")
                else:
                    invalid_count += 1
                    logger.info(f"Inserted invalid document {order['order_id']} - Valid: {valid_count}, Invalid: {invalid_count}")
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
        logger.info(f"Test completed. Generated {operations_count} documents (Valid: {valid_count}, Invalid: {invalid_count})")

if __name__ == '__main__':
    if not MONGODB_URI:
        raise ValueError("MONGODB_TEST_URI environment variable is required")
        
    try:
        asyncio.run(write_test_data())
    except KeyboardInterrupt:
        logger.info("Test interrupted by user")
    except Exception as e:
        logger.error(f"Test failed: {str(e)}")
        raise 