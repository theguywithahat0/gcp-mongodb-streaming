#!/usr/bin/env python3
"""
Data Generator for Stock Monitoring Integration Tests

This module provides functionality to generate realistic stock trading data
and load it into MongoDB for testing the stock position monitoring pipeline.
"""

import random
import datetime
import asyncio
import logging
from typing import Dict, List, Any, Optional
from motor.motor_asyncio import AsyncIOMotorClient

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class StockTradeGenerator:
    """Generator for realistic stock trade data."""
    
    def __init__(
        self, 
        site: str,
        stocks: List[str],
        num_trades: int = 100,
        start_time: Optional[datetime.datetime] = None,
        end_time: Optional[datetime.datetime] = None,
        max_price: float = 1000.0,
        max_quantity: int = 100
    ):
        """
        Initialize the stock trade generator.
        
        Args:
            site: Trading site identifier
            stocks: List of stock symbols to generate trades for
            num_trades: Number of trades to generate
            start_time: Start time for trade timestamps (defaults to 1 hour ago)
            end_time: End time for trade timestamps (defaults to now)
            max_price: Maximum stock price
            max_quantity: Maximum quantity per trade
        """
        self.site = site
        self.stocks = stocks
        self.num_trades = num_trades
        
        # Set time range for trades
        now = datetime.datetime.utcnow()
        self.start_time = start_time or (now - datetime.timedelta(hours=1))
        self.end_time = end_time or now
        
        self.max_price = max_price
        self.max_quantity = max_quantity
        
        # Initialize traders
        num_traders = max(1, min(20, num_trades // 5))  # At least 1 trader
        self.traders = [f"TRADER{i:03d}" for i in range(1, num_traders + 1)]
        
        # Set random seed for reproducibility
        random.seed(42)
    
    def generate_trades(self) -> List[Dict[str, Any]]:
        """
        Generate a list of random stock trades.
        
        Returns:
            List of trade documents ready to be inserted into MongoDB
        """
        trades = []
        
        try:
            # Time range in seconds
            time_range = int((self.end_time - self.start_time).total_seconds())
            
            logger.info(f"Generating {self.num_trades} trades for site {self.site}")
            logger.info(f"Using {len(self.stocks)} stocks and {len(self.traders)} traders")
            
            for i in range(self.num_trades):
                # Select random stock
                stock = random.choice(self.stocks)
                
                # Generate random price with some volatility
                base_price = random.uniform(10.0, self.max_price)
                price = round(base_price * random.uniform(0.98, 1.02), 2)
                
                # Generate random quantity
                quantity = random.randint(1, self.max_quantity)
                
                # Generate random trader
                trader = random.choice(self.traders)
                
                # Generate random timestamp within the time range
                seconds_offset = random.randint(0, time_range)
                timestamp = self.start_time + datetime.timedelta(seconds=seconds_offset)
                
                # Determine trade type (buy or sell)
                trade_type = random.choice(["buy", "sell"])
                
                # Create trade document
                trade = {
                    "site": self.site,
                    "trader_id": trader,
                    "stock_symbol": stock,
                    "trade_type": trade_type,
                    "quantity": quantity,
                    "price": price,
                    "timestamp": timestamp,
                    "total_value": round(price * quantity, 2),
                    "status": "completed"
                }
                
                trades.append(trade)
            
            # Sort trades by timestamp
            trades.sort(key=lambda x: x["timestamp"])
            
            logger.info(f"Generated {len(trades)} trades for site {self.site}")
            
        except Exception as e:
            logger.error(f"Error generating trades: {e}")
            logger.error(f"Site: {self.site}, Stocks: {self.stocks}, Traders: {self.traders}")
            raise
            
        return trades

async def load_data_to_mongodb(
    mongodb_uri: str, 
    collection_name: str, 
    documents: List[Dict[str, Any]]
) -> bool:
    """
    Load generated documents into MongoDB.
    
    Args:
        mongodb_uri: MongoDB connection URI
        collection_name: Name of the collection to insert documents into
        documents: List of documents to insert
        
    Returns:
        True if successful, False otherwise
    """
    client = None
    try:
        # Connect to MongoDB with a timeout
        logger.info(f"Connecting to MongoDB at {mongodb_uri.split('@')[-1] if '@' in mongodb_uri else '***'}")
        client = AsyncIOMotorClient(
            mongodb_uri,
            serverSelectionTimeoutMS=5000,  # 5 second timeout for server selection
            connectTimeoutMS=5000,          # 5 second timeout for connection
            socketTimeoutMS=10000           # 10 second timeout for socket operations
        )
        
        # Check connection before proceeding
        await client.admin.command('ping')
        logger.info("Successfully connected to MongoDB")
        
        # Extract database name from URI
        db_name = mongodb_uri.split("/")[-1].split("?")[0]
        if not db_name:
            db_name = "test"
        
        db = client[db_name]
        collection = db[collection_name]
        
        if not documents:
            logger.warning("No documents to insert")
            return True
            
        logger.info(f"Inserting {len(documents)} documents into {collection_name}")
        
        # Insert documents in batches for better performance
        batch_size = min(100, len(documents))  # Smaller batch size for faster operation
        for i in range(0, len(documents), batch_size):
            batch = documents[i:i+batch_size]
            try:
                await collection.insert_many(batch, ordered=False)
                logger.info(f"Inserted batch {i//batch_size + 1}/{(len(documents)-1)//batch_size + 1}")
            except Exception as e:
                logger.error(f"Error inserting batch: {e}")
                return False
        
        # Create indexes if needed
        try:
            logger.info("Creating indexes...")
            await collection.create_index("timestamp")
            await collection.create_index([("stock_symbol", 1), ("timestamp", 1)])
            logger.info("Indexes created successfully")
        except Exception as e:
            logger.warning(f"Error creating indexes: {e}")
            # Continue even if index creation fails
        
        logger.info(f"Successfully loaded {len(documents)} documents into {collection_name}")
        return True
        
    except Exception as e:
        logger.error(f"Error loading data to MongoDB: {e}")
        return False
    finally:
        if client:
            logger.info("Closing MongoDB connection")
            client.close()

async def main():
    """Example usage of the data generator."""
    # Generate sample trades
    generator = StockTradeGenerator(
        site="nyse",
        stocks=["AAPL", "MSFT", "GOOG", "AMZN", "FB"],
        num_trades=100
    )
    trades = generator.generate_trades()
    
    # Print sample trade
    if trades:
        print("Sample trade:")
        print(trades[0])
    
    # Note: To load data to MongoDB, uncomment the following code and provide a valid URI
    # mongodb_uri = "mongodb://localhost:27017/test"
    # await load_data_to_mongodb(
    #     mongodb_uri=mongodb_uri,
    #     collection_name="sample_trades",
    #     documents=trades
    # )

if __name__ == "__main__":
    asyncio.run(main()) 