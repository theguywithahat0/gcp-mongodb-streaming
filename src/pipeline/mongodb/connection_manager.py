from dataclasses import dataclass
from typing import Dict, Optional, Any, List
from motor.motor_asyncio import AsyncIOMotorClient
import asyncio
import logging
from datetime import datetime
from pymongo.errors import PyMongoError

@dataclass
class StreamStatus:
    """
    Tracks the status of a change stream for a specific source
    Example: status of a specific collection's change stream from a given connection
    """
    active: bool
    connection_id: str
    source_name: str
    last_error: Optional[str] = None
    last_error_time: Optional[datetime] = None
    retry_count: int = 0
    last_healthy: Optional[datetime] = None
    total_errors: int = 0
    processed_changes: int = 0

class AsyncMongoDBConnectionManager:
    """
    Async manager for multiple MongoDB connections and their change streams
    Handles parallel processing of changes from different sources
    """

    def __init__(self, config: dict):
        self.config = config
        self.clients: Dict[str, AsyncIOMotorClient] = {}
        self.streams: Dict[str, Dict[str, Any]] = {}
        self.status: Dict[str, StreamStatus] = {}
        self.logger = logging.getLogger(__name__)
        self.processing_tasks: Dict[str, asyncio.Task] = {}
        
    async def initialize_connections(self):
        """Initialize all configured MongoDB connections"""
        # Create tasks for each client connection
        init_tasks = []
        for client_id, client_config in self.config['mongodb']['connections'].items():
            task = asyncio.create_task(
                self._initialize_client_connection(client_id, client_config)
            )
            init_tasks.append(task)
        
        # Wait for all connections to initialize
        await asyncio.gather(*init_tasks)

    async def _initialize_client_connection(self, client_id: str, client_config: dict):
        """Initialize a single client's connection and its change streams"""
        try:
            # Create async MongoDB client
            client = AsyncIOMotorClient(
                client_config['uri'],
                serverSelectionTimeoutMS=self.config['monitoring']['connection_timeout']
            )
            
            # Test connection
            await client.admin.command('ping')
            
            self.clients[client_id] = client
            
            # Initialize streams for different sources
            stream_tasks = []
            for source_name, source_config in client_config['sources'].items():
                task = asyncio.create_task(
                    self._initialize_stream(client_id, source_name, source_config)
                )
                stream_tasks.append(task)
            
            await asyncio.gather(*stream_tasks)
            
            self.logger.info(f"Successfully initialized client connection: {client_id}")
            
        except Exception as e:
            self.logger.error(f"Failed to initialize client {client_id}: {str(e)}")
            await self._handle_connection_error(client_id, e)

    async def _initialize_stream(
        self, 
        connection_id: str, 
        stream_name: str, 
        stream_config: dict
    ):
        """Initialize a change stream for a specific source"""
        try:
            client = self.clients[connection_id]
            db = client[stream_config['database']]
            collection = db[stream_config['collection']]
            
            # Create change stream with configured pipeline
            pipeline = stream_config.get('pipeline', [])
            stream = collection.watch(
                pipeline=pipeline,
                batch_size=stream_config.get('batch_size', 100)
            )
            
            # Store stream configuration and status
            status = StreamStatus(
                active=True,
                connection_id=connection_id,
                source_name=stream_name,
                last_healthy=datetime.now()
            )
            
            stream_id = f"{connection_id}.{stream_name}"
            self.streams[stream_id] = {
                'stream': stream,
                'config': stream_config,
                'last_processed': datetime.now()
            }
            self.status[stream_id] = status
            
            # Start processing task for this stream
            self.processing_tasks[stream_id] = asyncio.create_task(
                self._process_stream(stream_id)
            )
            
        except Exception as e:
            self.logger.error(
                f"Failed to initialize stream {stream_name} for connection {connection_id}: {str(e)}"
            )
            await self._handle_connection_error(connection_id, e)

    async def _process_stream(self, stream_id: str):
        """Process changes from a specific stream"""
        stream_info = self.streams[stream_id]
        status = self.status[stream_id]
        
        while True:
            try:
                async with stream_info['stream'] as stream:
                    async for change in stream:
                        await self._process_change(stream_id, change)
                        
            except PyMongoError as e:
                self.logger.error(f"Error in stream {stream_id}: {str(e)}")
                await self._handle_connection_error(status.connection_id, e)
                
                if status.retry_count > self.config['error_handling']['max_retries']:
                    self.logger.error(f"Stopping stream {stream_id} after max retries")
                    break
                    
                await asyncio.sleep(
                    self.config['error_handling']['backoff_factor'] ** status.retry_count
                )

    async def _process_change(self, stream_id: str, change: dict):
        """Process a single change event from the change stream"""
        try:
            status = self.status[stream_id]
            
            # Update processing statistics
            status.processed_changes += 1
            
            # Update last processed time
            self.streams[stream_id]['last_processed'] = datetime.now()
            
        except Exception as e:
            self.logger.error(f"Error processing change in stream {stream_id}: {str(e)}")

    async def _handle_connection_error(self, connection_id: str, error: Exception):
        """Handle connection errors with retry logic"""
        for stream_id, status in self.status.items():
            if status.connection_id == connection_id:
                status.active = False
                status.last_error = str(error)
                status.last_error_time = datetime.now()
                status.retry_count += 1
                status.total_errors += 1

    async def cleanup(self):
        """Clean up all connections and streams"""
        # Cancel all processing tasks
        for task in self.processing_tasks.values():
            task.cancel()
        
        # Wait for tasks to complete
        await asyncio.gather(
            *self.processing_tasks.values(), 
            return_exceptions=True
        )
        
        # Close all clients
        for client in self.clients.values():
            client.close()

    async def get_stream_statistics(self) -> Dict[str, Dict[str, Any]]:
        """Get statistics for all streams"""
        stats = {}
        for stream_id, status in self.status.items():
            stats[stream_id] = {
                'active': status.active,
                'connection_id': status.connection_id,
                'source_name': status.source_name,
                'processed_changes': status.processed_changes,
                'last_processed': self.streams[stream_id]['last_processed'],
                'errors': {
                    'total': status.total_errors,
                    'last_error': status.last_error,
                    'last_error_time': status.last_error_time
                }
            }
        return stats 