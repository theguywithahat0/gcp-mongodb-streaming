#!/usr/bin/env python3
"""
Stock Trade Generator for Pipeline Integration Test

This script generates synthetic stock trading data and writes it to MongoDB
for testing the StockPositionMonitor transform in the pipeline.

Each trading site has its own MongoDB cluster.
"""

import os
import sys
import asyncio
import random
import logging
import json
import datetime
from pathlib import Path
from typing import List, Dict, Any, Tuple
from motor.motor_asyncio import AsyncIOMotorClient
from pymongo import IndexModel, ASCENDING
from dotenv import load_dotenv

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Add project root to the Python path
current_dir = Path(__file__).resolve().parent
project_root = current_dir.parent.parent.parent.parent
sys.path.append(str(project_root))

# Load environment variables
env_path = project_root / 'tests' / '.env.test'
if not env_path.exists():
    raise FileNotFoundError(f"Test environment file not found at {env_path}")
load_dotenv(str(env_path))

# MongoDB configuration
COLLECTION = os.getenv('TEST_MONGODB_COLLECTION', 'stock_trades')

# Test configuration
WRITE_INTERVAL = float(os.getenv('TEST_WRITE_INTERVAL', '1'))
TEST_DURATION = int(os.getenv('TEST_DURATION', '60'))
TEST_NUM_DOCS = int(os.getenv('TEST_NUM_DOCS', '50'))
TEST_BATCH_SIZE = int(os.getenv('TEST_BATCH_SIZE', '5'))

# Stock data configuration
STOCK_SYMBOLS = os.getenv('TEST_STOCKS', 'AAPL,MSFT,GOOGL').split(',')
TRADING_SITES = os.getenv('TEST_TRADING_SITES', 'fidelity,etrade,robinhood').split(',')
MIN_QUANTITY = int(os.getenv('TEST_STOCK_MIN_QTY', '10'))
MAX_QUANTITY = int(os.getenv('TEST_STOCK_MAX_QTY', '250'))

# Thresholds for generating data that will trigger alerts
THRESHOLDS_JSON = os.getenv('TEST_POSITION_THRESHOLDS', '{"thresholds":[]}')
try:
    THRESHOLDS = json.loads(THRESHOLDS_JSON)['thresholds']
except (json.JSONDecodeError, KeyError):
    logger.warning(f"Invalid threshold JSON: {THRESHOLDS_JSON}")
    THRESHOLDS = []

class SiteConnection:
    """Represents a connection to a site-specific MongoDB cluster."""
    
    def __init__(self, site: str, uri: str, collection: str):
        """Initialize a site connection."""
        self.site = site
        self.uri = uri
        self.collection_name = collection
        self.client = None
        self.db = None
        self.coll = None
    
    async def connect(self) -> bool:
        """Connect to the site's MongoDB cluster."""
        try:
            logger.info(f"Connecting to site {self.site} - {self.uri}")
            self.client = AsyncIOMotorClient(self.uri)
            
            # The database name is extracted from the URI
            # When using MongoDB Atlas or similar, the URI includes the database
            db_name = self.uri.split('/')[-1] or 'test'
            self.db = self.client.get_database()
            self.coll = self.db[self.collection_name]
            
            # Set up collection
            await self.setup_collection()
            logger.info(f"Connected to {self.site} cluster successfully")
            return True
        except Exception as e:
            logger.error(f"Failed to connect to {self.site} cluster: {e}")
            return False
    
    async def setup_collection(self):
        """Set up the collection with necessary indexes."""
        indexes = [
            IndexModel([("trade_id", ASCENDING)], unique=True),
            IndexModel([("timestamp", ASCENDING)]),
            IndexModel([("symbol", ASCENDING)])
        ]
        
        try:
            # Drop collection if it exists to start fresh
            await self.db.drop_collection(self.collection_name)
            self.coll = self.db[self.collection_name]
            
            # Create indexes
            await self.coll.create_indexes(indexes)
            logger.info(f"Collection {self.site}.{self.collection_name} set up with indexes")
        except Exception as e:
            logger.warning(f"Error setting up collection for {self.site}: {e}")
    
    async def insert_document(self, document: Dict[str, Any]) -> bool:
        """Insert a document into the site's collection."""
        try:
            await self.coll.insert_one(document)
            return True
        except Exception as e:
            if "duplicate key" not in str(e):
                logger.error(f"Error inserting document into {self.site}: {e}")
            return False
    
    def close(self):
        """Close the MongoDB connection."""
        if self.client:
            self.client.close()
            logger.info(f"Closed connection to {self.site}")

class StockTradeGenerator:
    """Generates synthetic stock trade data for testing multiple site clusters."""
    
    def __init__(self):
        """Initialize the generator."""
        self.site_connections = {}  # Dict of site_name -> SiteConnection
    
    async def connect_all_sites(self) -> bool:
        """Connect to all site clusters."""
        all_connected = True
        
        for site in TRADING_SITES:
            # Get site-specific connection URI
            site_uri_env = f"TEST_MONGODB_{site.upper()}_URI"
            site_uri = os.getenv(site_uri_env)
            
            if not site_uri:
                logger.error(f"Missing connection URI for site {site} (environment variable {site_uri_env})")
                all_connected = False
                continue
            
            conn = SiteConnection(
                site=site,
                uri=site_uri,
                collection=COLLECTION
            )
            
            if await conn.connect():
                self.site_connections[site] = conn
            else:
                all_connected = False
        
        if not self.site_connections:
            logger.error("Failed to connect to any site clusters")
            return False
        
        logger.info(f"Connected to {len(self.site_connections)} site clusters")
        return all_connected
    
    def generate_trade(self, site: str) -> Dict[str, Any]:
        """Generate a single random stock trade for a specific site."""
        # Select stock symbol
        symbol = random.choice(STOCK_SYMBOLS)
        
        # Check if this is a threshold pair to generate more of these
        is_threshold_pair = any(
            t['site'] == site and t['stock'] == symbol for t in THRESHOLDS
        )
        
        # Generate higher quantities for threshold pairs to trigger alerts
        if is_threshold_pair and random.random() < 0.7:  # 70% chance
            quantity = random.randint(
                MAX_QUANTITY // 4,
                MAX_QUANTITY
            )
        else:
            quantity = random.randint(MIN_QUANTITY, MAX_QUANTITY // 3)
        
        # Generate buy/sell (more buys for threshold pairs)
        if is_threshold_pair and random.random() < 0.8:  # 80% chance of buys
            operation = 'buy'
        else:
            operation = random.choice(['buy', 'sell'])
        
        # Generate trade document
        return {
            'trade_id': f'TRADE-{site}-{random.randint(10000, 99999)}',
            'site': site,  # Include site in the document for reference
            'symbol': symbol,
            'quantity': quantity,
            'operation': operation,
            'price': round(random.uniform(50, 1000), 2),
            'timestamp': datetime.datetime.now(datetime.UTC).isoformat(),
            'user_id': f'USER-{random.randint(1000, 9999)}'
        }
    
    async def generate_batch(self, batch_size: int) -> List[Tuple[str, Dict[str, Any]]]:
        """Generate a batch of trades distributed across sites."""
        trades = []
        # Divide batch evenly among sites
        trades_per_site = max(1, batch_size // len(self.site_connections))
        remaining = batch_size
        
        for site in self.site_connections:
            site_trades = min(trades_per_site, remaining)
            for _ in range(site_trades):
                trades.append((site, self.generate_trade(site)))
                remaining -= 1
        
        # Distribute any remaining trades
        while remaining > 0:
            site = random.choice(list(self.site_connections.keys()))
            trades.append((site, self.generate_trade(site)))
            remaining -= 1
        
        return trades
    
    async def write_batch(self, batch: List[Tuple[str, Dict[str, Any]]]) -> int:
        """Write a batch of trades to their respective site clusters."""
        inserted = 0
        
        for site, trade in batch:
            if site not in self.site_connections:
                logger.warning(f"No connection for site {site}, skipping trade")
                continue
            
            if await self.site_connections[site].insert_document(trade):
                inserted += 1
        
        return inserted
    
    async def generate_and_write_trades(
        self, 
        total_trades: int, 
        batch_size: int, 
        interval: float
    ) -> List[Dict[str, Any]]:
        """Generate and write a specific number of trades."""
        trades_written = 0
        generation_details = []
        
        logger.info(f"Starting to generate {total_trades} trades across {len(self.site_connections)} sites in batches of {batch_size}")
        
        while trades_written < total_trades:
            # Calculate how many trades to generate in this batch
            remaining = total_trades - trades_written
            current_batch_size = min(batch_size, remaining)
            
            # Generate and write batch
            batch = await self.generate_batch(current_batch_size)
            inserted = await self.write_batch(batch)
            trades_written += inserted
            
            # Record details about generated trades
            generation_time = datetime.datetime.now(datetime.UTC).isoformat()
            for site, trade in batch:
                generation_details.append({
                    "trade_id": trade['trade_id'],
                    "site": site,
                    "cluster_uri": os.getenv(f"TEST_MONGODB_{site.upper()}_URI", "").split('@')[-1].split('/')[0],
                    "symbol": trade['symbol'],
                    "operation": trade['operation'],
                    "quantity": trade['quantity'],
                    "timestamp": trade['timestamp'],
                    "generated_at": generation_time
                })
            
            # Log progress
            progress = (trades_written / total_trades) * 100
            logger.info(f"Written {trades_written}/{total_trades} trades ({progress:.1f}%)")
            
            # Sleep before next batch if not done
            if trades_written < total_trades:
                await asyncio.sleep(interval)
        
        logger.info(f"Completed generating {trades_written} trades across {len(self.site_connections)} sites")
        return generation_details
    
    def close_all(self):
        """Close all MongoDB connections."""
        for site, conn in self.site_connections.items():
            conn.close()
        logger.info("Closed all MongoDB connections")

async def main():
    """Main entry point."""
    # Create directory for reports
    reports_dir = Path(__file__).resolve().parent.parent / 'reports'
    reports_dir.mkdir(exist_ok=True)
    
    # Initialize generator
    generator = StockTradeGenerator()
    
    try:
        # Connect to all site clusters
        if not await generator.connect_all_sites():
            logger.error("Failed to connect to all required site clusters")
            sys.exit(1)
        
        # Generate and write trades
        generation_details = await generator.generate_and_write_trades(
            total_trades=TEST_NUM_DOCS,
            batch_size=TEST_BATCH_SIZE,
            interval=WRITE_INTERVAL
        )
        
        # Write generation details to file
        with open(reports_dir / 'trade_generation_details.json', 'w') as f:
            json.dump(generation_details, f, indent=2)
            logger.info(f"Wrote generation details to {reports_dir / 'trade_generation_details.json'}")
        
    except KeyboardInterrupt:
        logger.info("Test interrupted by user")
    except Exception as e:
        logger.error(f"Test failed: {str(e)}")
        raise
    finally:
        generator.close_all()

if __name__ == '__main__':
    asyncio.run(main()) 