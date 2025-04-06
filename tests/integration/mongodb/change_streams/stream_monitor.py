#!/usr/bin/env python3
"""
Test script for monitoring MongoDB change streams using our connection manager.
Watches the test collection for changes and logs them.
"""

import os
import sys
import asyncio
import logging
from pathlib import Path
from dotenv import load_dotenv
import json
from datetime import datetime, UTC

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

# Configure logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)  # Ensure logger itself is at DEBUG level

# Add the project root to the Python path
current_dir = Path(__file__).resolve().parent
project_root = current_dir.parent.parent.parent.parent
sys.path.append(str(project_root))

# Import our connection manager
from src.pipeline.mongodb.connection_manager import AsyncMongoDBConnectionManager

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
    """Monitor changes using our connection manager."""
    setup_logging()
    # Create configuration for the connection manager
    config = {
        'mongodb': {
            'connections': {
                'test': {
                    'uri': uri,
                    'sources': {
                        'test_stream': {
                            'database': database,
                            'collection': collection,
                            'pipeline': [],  # Empty pipeline to catch all changes
                            'batch_size': 1,
                            'max_await_time_ms': 1000,
                            'full_document': 'updateLookup'  # Get full document for all operations
                        }
                    }
                }
            }
        },
        'error_handling': {
            'max_retries': 3,
            'max_backoff': 5
        },
        'monitoring': {
            'connection_timeout': 5000,
            'max_inactivity_seconds': 60  # Increased to ensure we catch all documents
        }
    }
    
    # Initialize connection manager
    manager = AsyncMongoDBConnectionManager(config)
    processing_details = []
    reports_dir = Path(__file__).parent.parent / 'reports'
    reports_dir.mkdir(exist_ok=True)
    details_file = reports_dir / 'processing_details.json'
    
    # Clear processing details at start of run
    with open(details_file, 'w') as f:
        json.dump([], f, indent=2)
    
    try:
        logger.info(f"Starting change stream monitor on {database}.{collection}")
        
        # Initialize connections
        await manager.initialize_connections()
        logger.info("Successfully initialized connection manager")
        
        # Start from now using wall clock time to match test runner
        start_time = datetime.now(UTC)
        processed = 0
        last_stats_log = 0
        stream_id = 'test.test_stream'
        last_new_doc_time = None
        
        # Monitor until duration is reached and no new documents for grace period
        while True:
            current_time = datetime.now(UTC)
            elapsed = (current_time - start_time).total_seconds()
            
            if stream_id in manager.status:
                status = manager.status[stream_id]
                stream_info = manager.streams.get(stream_id, {})
                
                if status.processed_changes > processed:
                    # Log when new documents are processed
                    new_docs = status.processed_changes - processed
                    logger.info(f"Processed {new_docs} new documents (total: {status.processed_changes})")
                    processed = status.processed_changes
                    last_new_doc_time = current_time
                    
                    # Get the latest processed document details
                    if 'last_document' in stream_info:
                        doc = stream_info['last_document']
                        if isinstance(doc, dict):
                            detail = {
                                "doc_id": str(doc.get("_id")),
                                "order_id": doc.get("order_id"),
                                "processed_at": datetime.utcnow().isoformat(),
                                "timezone": "UTC"
                            }
                            processing_details.append(detail)
                            # Write details after each document
                            with open(details_file, 'w') as f:
                                json.dump(processing_details, f, indent=2)
                
                # Log errors if any
                if status.last_error:
                    logger.warning(f"Stream error: {status.last_error} at {status.last_error_time}")
                
                # Log detailed stats every 5 seconds
                if last_stats_log == 0 or (current_time - datetime.fromtimestamp(last_stats_log, UTC)).total_seconds() >= 5:
                    logger.info(
                        f"Stream stats - Active: {status.active}, "
                        f"Processed: {status.processed_changes}, "
                        f"Errors: {status.total_errors}, "
                        f"Last healthy: {status.last_healthy}"
                    )
                    last_stats_log = current_time.timestamp()
            
            # Check if we should exit
            if elapsed >= duration:
                # If we've exceeded duration, wait for a grace period after the last document
                if last_new_doc_time is None or (current_time - last_new_doc_time).total_seconds() >= 15:
                    logger.info(f"Monitoring complete. No new documents for {15 if last_new_doc_time else 0} seconds after duration.")
                    break
            
            await asyncio.sleep(1)  # Check stats every second
            
    except Exception as e:
        logger.error(f"Error in change stream: {e}")
        raise
    finally:
        # Log final count and ensure it matches the manager's count
        if stream_id in manager.status:
            processed = manager.status[stream_id].processed_changes
        logger.info(f"Monitor completed. Processed {processed} documents")
        await manager.cleanup()

async def run_monitor():
    # Test configuration
    TEST_DURATION = int(os.getenv('TEST_DURATION', '60'))
    MONGODB_URI = os.getenv('MONGODB_TEST_URI')
    DATABASE = os.getenv('MONGODB_TEST_DB', 'test')  # Match default in .env.integration
    COLLECTION = os.getenv('MONGODB_TEST_COLLECTION', 'stream_test')  # Match default in .env.integration

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