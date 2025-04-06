#!/usr/bin/env python3
"""
Integration test for MongoDB change stream validation.
Tests the DocumentValidator with real MongoDB change streams.
"""

import json
import os
import sys
import time
from datetime import datetime
from pathlib import Path
import logging
import asyncio
from dotenv import load_dotenv
from motor.motor_asyncio import AsyncIOMotorClient

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Path setup
project_root = Path(__file__).resolve().parents[5]  # One more parent up to reach integration_tests
env_file = project_root / 'integration_tests' / '.env.integration'
logger.info(f"Loading environment from: {env_file}")

# Add project root to Python path for imports
sys.path.insert(0, str(project_root))
from src.pipeline.mongodb.validator import DocumentValidator

if not env_file.exists():
    raise FileNotFoundError(f"Environment file not found at {env_file}")

# Load environment variables
load_dotenv(env_file)

class ValidatorIntegrationTest:
    def __init__(self):
        self.mongo_uri = os.getenv('MONGODB_TEST_URI')
        self.database_name = os.getenv('MONGODB_TEST_DB')
        self.collection_name = os.getenv('MONGODB_TEST_COLLECTION')
        if not all([self.mongo_uri, self.database_name, self.collection_name]):
            raise ValueError("Required environment variables not set")
        self.client = None
        self.db = None
        self.collection = None
        self.test_results = {
            'timestamp': datetime.now().isoformat(),
            'status': 'PENDING',
            'duration': 0,
            'documents_validated': 0,
            'errors': []
        }

    async def setup(self):
        """Initialize MongoDB connection and set up test environment."""
        try:
            self.client = AsyncIOMotorClient(self.mongo_uri)
            self.db = self.client[self.database_name]
            self.collection = self.db[self.collection_name]
            await self.client.admin.command('ping')
            logger.info("Successfully connected to MongoDB")
        except Exception as e:
            self.test_results['errors'].append(f"Setup error: {str(e)}")
            raise

    async def cleanup(self):
        """Clean up test resources."""
        if self.client:
            self.client.close()
            logger.info("Closed MongoDB connection")

    def save_report(self):
        """Save test results to JSON reports."""
        reports_dir = Path(__file__).resolve().parent.parent / 'reports'
        reports_dir.mkdir(exist_ok=True)

        # Save timestamped report
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        report_path = reports_dir / f'report_{timestamp}.json'
        with open(report_path, 'w') as f:
            json.dump(self.test_results, f, indent=2)

        # Save latest report
        latest_path = reports_dir / 'latest.json'
        with open(latest_path, 'w') as f:
            json.dump(self.test_results, f, indent=2)

        logger.info(f"Test reports written to {reports_dir}")

    async def run_test(self):
        """Run the validator integration test."""
        start_time = time.time()
        test_duration = int(os.getenv('TEST_DURATION', '30'))  # Default 30 seconds
        stream_task = None
        
        try:
            await self.setup()
            
            # Test schema for validation
            stream_id = f"{str(self.database_name)}.{str(self.collection_name)}"
            logger.info(f"Using stream ID: {stream_id}")
            
            TEST_SCHEMA = {
                stream_id: {
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

            validator = DocumentValidator(TEST_SCHEMA)
            logger.info(f"Starting change stream validation on {stream_id}")
            
            # Track statistics
            processed = 0
            valid_docs = 0
            invalid_docs = 0
            
            async def process_stream():
                nonlocal processed, valid_docs, invalid_docs
                async with self.collection.watch(
                    pipeline=[],
                    full_document='updateLookup',
                    max_await_time_ms=1000
                ) as stream:
                    while (time.time() - start_time) < test_duration:
                        try:
                            async for change in stream:
                                if (time.time() - start_time) >= test_duration:
                                    break
                                    
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
                                
                        except Exception as e:
                            logger.error(f"Error processing change: {e}")
                            await asyncio.sleep(1)
                            
                            if (time.time() - start_time) >= test_duration:
                                break
            
            # Start the stream processing task
            stream_task = asyncio.create_task(process_stream())
            
            # Wait for the test duration
            try:
                await asyncio.wait_for(stream_task, timeout=test_duration)
            except asyncio.TimeoutError:
                # This is expected when the test duration is reached
                pass
                    
            logger.info(
                f"Validation completed. "
                f"Processed: {processed}, "
                f"Valid: {valid_docs}, "
                f"Invalid: {invalid_docs}"
            )
                    
            self.test_results['documents_validated'] = processed
            self.test_results['status'] = 'PASS'
            logger.info("Validator integration test completed successfully")
            
        except Exception as e:
            self.test_results['status'] = 'FAIL'
            self.test_results['errors'].append(str(e))
            logger.error(f"Test failed: {e}")
            raise
        finally:
            # Cancel the stream task if it's still running
            if stream_task and not stream_task.done():
                stream_task.cancel()
                try:
                    await stream_task
                except asyncio.CancelledError:
                    pass
                
            self.test_results['duration'] = round(time.time() - start_time, 2)
            self.save_report()
            await self.cleanup()

async def main():
    test = ValidatorIntegrationTest()
    try:
        await test.run_test()
        if test.test_results['status'] == 'PASS':
            sys.exit(0)
        else:
            sys.exit(1)
    except Exception as e:
        logger.error(f"Test failed with error: {e}")
        sys.exit(1)

if __name__ == '__main__':
    asyncio.run(main()) 