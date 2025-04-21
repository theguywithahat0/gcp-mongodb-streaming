"""Main connector class that manages multiple change stream listeners."""

import asyncio
import signal
from typing import Dict, List, Optional

import structlog
from google.cloud import firestore, pubsub_v1
from pymongo import MongoClient

from ..config.config_manager import ConfigurationManager
from .change_stream_listener import ChangeStreamListener

logger = structlog.get_logger(__name__)

class MongoDBConnector:
    """Main connector class that manages multiple change stream listeners."""

    def __init__(self, config_path: Optional[str] = None):
        """Initialize the connector.
        
        Args:
            config_path: Optional path to the configuration file.
        """
        # Load configuration
        self.config_manager = ConfigurationManager(config_path)
        self.config = self.config_manager.get_config()

        # Initialize clients
        self.mongo_client = MongoClient(
            self.config.mongodb.uri,
            **self.config.mongodb.options
        )
        self.publisher = pubsub_v1.PublisherClient()
        self.firestore_client = firestore.Client()

        # Initialize listeners
        self.listeners: Dict[str, ChangeStreamListener] = {}
        self.running = False

    async def start(self) -> None:
        """Start all configured change stream listeners."""
        if self.running:
            return

        self.running = True
        logger.info("Starting MongoDB connector")

        # Create listeners for each configured collection
        tasks = []
        for collection_config in self.config.mongodb.collections:
            listener = ChangeStreamListener(
                config=self.config,
                collection_config=collection_config,
                mongo_client=self.mongo_client,
                publisher=self.publisher,
                firestore_client=self.firestore_client
            )
            self.listeners[collection_config.name] = listener
            tasks.append(listener.start())

        # Start all listeners
        await asyncio.gather(*tasks)

    async def stop(self) -> None:
        """Stop all change stream listeners."""
        if not self.running:
            return

        self.running = False
        logger.info("Stopping MongoDB connector")

        # Stop all listeners
        tasks = [
            listener.stop()
            for listener in self.listeners.values()
        ]
        await asyncio.gather(*tasks)

        # Close clients
        self.mongo_client.close()
        self.publisher.close()

    def _setup_signal_handlers(self) -> None:
        """Set up signal handlers for graceful shutdown."""
        loop = asyncio.get_event_loop()

        for sig in (signal.SIGTERM, signal.SIGINT):
            loop.add_signal_handler(
                sig,
                lambda s=sig: asyncio.create_task(self._handle_signal(s))
            )

    async def _handle_signal(self, sig: signal.Signals) -> None:
        """Handle termination signals.
        
        Args:
            sig: The signal received.
        """
        logger.info(f"Received signal {sig.name}")
        await self.stop()
        asyncio.get_event_loop().stop()

    async def __aenter__(self) -> 'MongoDBConnector':
        """Context manager entry."""
        self._setup_signal_handlers()
        await self.start()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        """Context manager exit."""
        await self.stop()

    @classmethod
    async def run(cls, config_path: Optional[str] = None) -> None:
        """Run the connector.
        
        This is the main entry point for running the connector.
        
        Args:
            config_path: Optional path to the configuration file.
        """
        async with cls(config_path) as connector:
            # Keep running until stopped
            while connector.running:
                await asyncio.sleep(1)

def main() -> None:
    """Main entry point for the connector."""
    asyncio.run(MongoDBConnector.run()) 