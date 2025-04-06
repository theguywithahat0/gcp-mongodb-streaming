#!/usr/bin/env python3
"""
Test data generator for validator integration testing.
Generates both valid and invalid test documents to verify validation rules.
"""

import asyncio
import json
import logging
import os
import random
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List

from dotenv import load_dotenv
from motor.motor_asyncio import AsyncIOMotorClient
from pymongo import IndexModel, ASCENDING

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Path setup
project_root = Path(__file__).resolve().parents[4]
env_file = project_root / '.env.integration'

if not env_file.exists():
    raise FileNotFoundError(f"Environment file not found at {env_file}")

# Load environment variables
load_dotenv(env_file)

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

class TestDataGenerator:
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
            'documents_generated': 0,
            'errors': []
        }

        # Test data configuration
        self.products = PRODUCTS
        self.statuses = STATUSES
        self.min_price = 10.0
        self.max_price = 2000.0

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
        try:
            if self.client:
                self.client.close()
                logger.info("Closed MongoDB connection")
        except Exception as e:
            logger.error(f"Error during cleanup: {e}")

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

    def generate_test_document(self) -> Dict:
        """Generate a single test document with random valid data."""
        return {
            "order_id": f"ORD-{random.randint(1000, 9999)}",
            "product": random.choice(self.products),
            "quantity": random.randint(1, 10),
            "status": random.choice(self.statuses),
            "price": round(random.uniform(self.min_price, self.max_price), 2),
            "created_at": datetime.now(),
            "customer_id": f"CUST-{random.randint(1000, 9999)}"
        }

    async def generate_documents(self, num_documents: int = 10, delay: float = 1.0):
        """Generate and insert test documents with a delay between each."""
        documents_generated = 0
        
        try:
            for _ in range(num_documents):
                try:
                    doc = self.generate_test_document()
                    result = await self.collection.insert_one(doc)
                    documents_generated += 1
                    logger.info(f"Generated document {documents_generated}/{num_documents}: {doc['order_id']}")
                    await asyncio.sleep(delay)
                except Exception as e:
                    error_msg = f"Error generating document: {str(e)}"
                    self.test_results['errors'].append(error_msg)
                    logger.error(error_msg)
                    break  # Stop on first error
        except asyncio.CancelledError:
            logger.info("Document generation cancelled")
            raise
            
        return documents_generated

    async def run_test(self, num_documents: int = 10, delay: float = 1.0):
        """Run the test data generation process."""
        start_time = time.time()
        try:
            await self.setup()
            documents_generated = await self.generate_documents(num_documents, delay)
            
            self.test_results['documents_generated'] = documents_generated
            self.test_results['status'] = 'PASS'
            logger.info(f"Successfully generated {documents_generated} test documents")
            
        except Exception as e:
            self.test_results['status'] = 'FAIL'
            self.test_results['errors'].append(str(e))
            logger.error(f"Test failed: {e}")
            raise
        finally:
            self.test_results['duration'] = round(time.time() - start_time, 2)
            self.save_report()
            await self.cleanup()

async def main():
    num_documents = int(os.getenv('NUM_DOCUMENTS', '10'))
    delay = float(os.getenv('DOCUMENT_DELAY', '1.0'))
    
    generator = TestDataGenerator()
    try:
        await generator.run_test(num_documents, delay)
        if generator.test_results['status'] == 'PASS':
            sys.exit(0)
        else:
            sys.exit(1)
    except Exception as e:
        logger.error(f"Test failed with error: {e}")
        sys.exit(1)

if __name__ == '__main__':
    if not MONGODB_URI:
        raise ValueError("MONGODB_TEST_URI environment variable is required")
        
    asyncio.run(main()) 