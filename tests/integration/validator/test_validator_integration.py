#!/usr/bin/env python3
"""
Integration test for MongoDB change stream validation.
Tests the DocumentValidator with real MongoDB change streams.
"""

import os
import asyncio
import logging
from datetime import datetime, UTC
from motor.motor_asyncio import AsyncIOMotorClient
from dotenv import load_dotenv

from src.pipeline.mongodb.validator import DocumentValidator

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Load environment variables from tests/.env.test
env_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), '.env.test')
load_dotenv(env_path)

# Test schema for validation
TEST_SCHEMA = {
    "source_db_test.source_collection_test": {
        "order_id": {
            "type": "string",
            "required": True,
            "nullable": False,
            "validation": lambda x: x.startswith("ORD-")
        },
        "product": {
            "type": "string",
            "required": True,
            "nullable": False,
            "validation": lambda x: x in ["laptop", "phone", "tablet", "watch", "headphones"]
        },
        "quantity": {
            "type": "number",
            "required": True,
            "nullable": False,
            "validation": lambda x: 0 < x <= 10
        },
        "status": {
            "type": "string",
            "required": True,
            "nullable": False,
            "validation": lambda x: x in ["pending", "processing", "shipped", "delivered"]
        },
        "price": {
            "type": "number",
            "required": True,
            "nullable": False,
            "validation": lambda x: x > 0
        },
        "created_at": {
            "type": "date",
            "required": True,
            "nullable": False
        },
        "customer_id": {
            "type": "string",
            "required": True,
            "nullable": False,
            "validation": lambda x: x.startswith("CUST-")
        }
    }
}

async def validate_changes(uri, database, collection, duration):
    """Monitor and validate changes using DocumentValidator."""
    client = AsyncIOMotorClient(uri)
    db = client[database]
    coll = db[collection]
    validator = DocumentValidator(TEST_SCHEMA)
    stream_id = f"{database}.{collection}"
    
    try:
        logger.info(f"Starting change stream validation on {database}.{collection}")
        
        # Track statistics
        start_time = asyncio.get_event_loop().time()
        processed = 0
        valid_docs = 0
        invalid_docs = 0
        
        async with coll.watch(
            pipeline=[],
            full_document='updateLookup',
            max_await_time_ms=1000
        ) as stream:
            while (asyncio.get_event_loop().time() - start_time) < duration:
                try:
                    async for change in stream:
                        processed += 1
                        try:
                            # Validate and transform document
                            transformed = validator.transform_document(stream_id, change["fullDocument"])
                            # Prepare for Pub/Sub
                            pubsub_msg = validator.prepare_for_pubsub(transformed, change["operationType"])
                            valid_docs += 1
                            logger.info(
                                f"Valid document: {change['operationType']} - "
                                f"ID: {transformed.get('order_id')} - "
                                f"Status: {transformed.get('status')}"
                            )
                            
                        except ValueError as e:
                            invalid_docs += 1
                            logger.warning(f"Invalid document: {str(e)}")
                            
                        if (asyncio.get_event_loop().time() - start_time) >= duration:
                            break
                            
                except Exception as e:
                    logger.error(f"Error processing change: {e}")
                    await asyncio.sleep(1)
                    
        logger.info(
            f"Validation completed. "
            f"Processed: {processed}, "
            f"Valid: {valid_docs}, "
            f"Invalid: {invalid_docs}"
        )
                    
    except Exception as e:
        logger.error(f"Error in change stream: {e}")
        raise
    finally:
        client.close()

async def run_validator():
    """Run the validator integration test."""
    # Test configuration
    TEST_DURATION = int(os.getenv('TEST_DURATION', '60'))
    MONGODB_URI = os.getenv('MONGODB_TEST_URI')
    DATABASE = os.getenv('MONGODB_TEST_DB', 'source_db_test')
    COLLECTION = os.getenv('MONGODB_TEST_COLLECTION', 'source_collection_test')

    if not MONGODB_URI:
        raise ValueError("MONGODB_TEST_URI environment variable is required")

    try:
        await validate_changes(MONGODB_URI, DATABASE, COLLECTION, TEST_DURATION)
    except Exception as e:
        logger.error(f"Validator failed: {e}")
        raise

if __name__ == '__main__':
    try:
        asyncio.run(run_validator())
    except KeyboardInterrupt:
        logger.info("Validator stopped by user")
    except Exception as e:
        logger.error(f"Validator failed: {e}")
        raise 