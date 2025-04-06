#!/usr/bin/env python3
"""
Test script for validating MongoDB documents using our DocumentValidator.
Watches the test collection for changes and validates them against a schema.
"""

import os
import sys
import asyncio
import logging
import json
from datetime import datetime, UTC
from pathlib import Path
from motor.motor_asyncio import AsyncIOMotorClient
from dotenv import load_dotenv

def setup_logging():
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s [%(levelname)s] %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    # Set MongoDB driver logging to WARNING level to reduce noise
    logging.getLogger('mongodb').setLevel(logging.WARNING)
    logging.getLogger('pymongo').setLevel(logging.WARNING)
    logging.getLogger('motor').setLevel(logging.WARNING)

logger = logging.getLogger(__name__)

# Add the project root to the Python path
current_dir = Path(__file__).resolve().parent
project_root = current_dir.parent.parent.parent.parent
sys.path.append(str(project_root))

# Import our validator
from src.pipeline.mongodb.validator import DocumentValidator

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

# Test schema for validation
TEST_SCHEMA = {
    "test.stream_test": {
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

async def validate_documents(uri, database, collection, duration):
    """Monitor and validate documents using DocumentValidator."""
    setup_logging()
    client = AsyncIOMotorClient(uri)
    db = client[database]
    coll = db[collection]
    validator = DocumentValidator(TEST_SCHEMA)
    stream_id = f"{database}.{collection}"
    
    processing_details = []
    reports_dir = Path(__file__).parent.parent / 'reports'
    reports_dir.mkdir(exist_ok=True)
    details_file = reports_dir / 'processing_details.json'
    
    # Clear processing details at start of run
    with open(details_file, 'w') as f:
        json.dump([], f, indent=2)
    
    try:
        logger.info(f"Starting document validation on {database}.{collection}")
        
        # Start from now using wall clock time to match test runner
        start_time = datetime.now(UTC)
        processed = 0
        valid_docs = 0
        invalid_docs = 0
        last_stats_log = 0
        
        # Watch for new documents
        async with coll.watch(
            pipeline=[],
            full_document='updateLookup',
            max_await_time_ms=1000
        ) as stream:
            while (datetime.now(UTC) - start_time).total_seconds() < duration:
                try:
                    async for change in stream:
                        if change['operationType'] == 'insert':
                            processed += 1
                            doc = change['fullDocument']
                            
                            try:
                                # Validate and transform document
                                transformed_doc = validator.transform_document(stream_id, doc)
                                valid_docs += 1
                                
                                # Log successful validation
                                detail = {
                                    "doc_id": str(doc.get("_id")),
                                    "order_id": doc.get("order_id"),
                                    "processed_at": datetime.utcnow().isoformat(),
                                    "timezone": "UTC",
                                    "validation": "success",
                                    "metadata": transformed_doc.get("_metadata", {})
                                }
                                logger.info(f"Validated document {doc.get('order_id')} successfully")
                                
                            except ValueError as e:
                                invalid_docs += 1
                                # Log validation failure
                                detail = {
                                    "doc_id": str(doc.get("_id")),
                                    "order_id": doc.get("order_id"),
                                    "processed_at": datetime.utcnow().isoformat(),
                                    "timezone": "UTC",
                                    "validation": "failed",
                                    "error": str(e)
                                }
                                logger.warning(f"Validation failed for document {doc.get('order_id')}: {e}")
                            
                            processing_details.append(detail)
                            # Write details after each document
                            with open(details_file, 'w') as f:
                                json.dump(processing_details, f, indent=2)
                            
                            # Log stats every 5 seconds
                            current_time = datetime.now(UTC)
                            if last_stats_log == 0 or (current_time - datetime.fromtimestamp(last_stats_log, UTC)).total_seconds() >= 5:
                                logger.info(
                                    f"Validation stats - "
                                    f"Processed: {processed}, "
                                    f"Valid: {valid_docs}, "
                                    f"Invalid: {invalid_docs}"
                                )
                                last_stats_log = current_time.timestamp()
                            
                except Exception as e:
                    logger.error(f"Error processing change: {e}")
                    continue
                
    except Exception as e:
        logger.error(f"Error in validation: {e}")
        raise
    finally:
        # Log final counts
        logger.info(f"Monitor completed. Processed {processed} documents ({valid_docs} valid, {invalid_docs} invalid)")
        client.close()

async def run_validator():
    # Test configuration
    TEST_DURATION = int(os.getenv('TEST_DURATION', '60'))
    MONGODB_URI = os.getenv('MONGODB_TEST_URI')
    DATABASE = os.getenv('MONGODB_TEST_DB', 'test')
    COLLECTION = os.getenv('MONGODB_TEST_COLLECTION', 'stream_test')
    
    if not MONGODB_URI:
        raise ValueError("MONGODB_TEST_URI environment variable is required")
    
    await validate_documents(MONGODB_URI, DATABASE, COLLECTION, TEST_DURATION)

if __name__ == '__main__':
    try:
        asyncio.run(run_validator())
    except KeyboardInterrupt:
        logger.info("Validator interrupted by user")
        sys.exit(130)
    except Exception as e:
        logger.error(f"Validator failed: {e}")
        sys.exit(1) 