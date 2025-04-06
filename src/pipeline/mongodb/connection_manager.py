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
                self._initialize_client(client_id, client_config)
            )
            init_tasks.append(task)
        
        # Wait for all connections to initialize
        await asyncio.gather(*init_tasks)

    async def _initialize_client(self, client_id: str, client_config: dict):
        """Initialize a client connection with retry logic"""
        max_retries = self.config['error_handling']['max_retries']
        retry_count = 0
        last_error = None
        client = None
        
        while retry_count < max_retries:
            try:
                # Create a new client for each attempt
                client = AsyncIOMotorClient(
                    client_config['uri'],
                    serverSelectionTimeoutMS=self.config['monitoring']['connection_timeout']
                )
                
                # Test connection
                await client.admin.command('ping')
                
                # Store the successful client
                self.clients[client_id] = client
                
                # Initialize streams
                stream_tasks = []
                for source_name, source_config in client_config['sources'].items():
                    task = asyncio.create_task(
                        self._initialize_stream(client_id, source_name, source_config)
                    )
                    stream_tasks.append(task)
                
                await asyncio.gather(*stream_tasks)
                
                self.logger.info(f"Successfully initialized client connection: {client_id}")
                return  # Success
                
            except Exception as e:
                last_error = e
                
                # Clean up the client if it was created
                if client:
                    try:
                        await client.close()
                    except Exception as close_error:
                        self.logger.warning(f"Error closing client {client_id}: {str(close_error)}")
                    client = None
                
                retry_count += 1
                if retry_count >= max_retries:
                    break
                
                # Calculate backoff time
                backoff = min(
                    2 ** (retry_count - 1),
                    self.config['error_handling'].get('max_backoff', 30)
                )
                self.logger.info(f"Retrying client {client_id} initialization in {backoff}s (attempt {retry_count}/{max_retries})")
                await asyncio.sleep(backoff)
        
        # If we get here, we've exhausted retries
        self.logger.error(f"Failed to initialize client {client_id} after {max_retries} retries: {str(last_error)}")
        # Ensure status is updated for all streams
        for source_name in client_config['sources'].keys():
            stream_id = f"{client_id}.{source_name}"
            status = StreamStatus(
                active=False,
                connection_id=client_id,
                source_name=source_name,
                last_error=str(last_error),
                last_error_time=datetime.now(),
                retry_count=retry_count,
                total_errors=retry_count
            )
            self.status[stream_id] = status

    async def _initialize_stream(
        self, 
        connection_id: str, 
        stream_name: str, 
        stream_config: dict
    ):
        """Initialize a change stream for a specific source"""
        stream_id = f"{connection_id}.{stream_name}"
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
            
            self.streams[stream_id] = {
                'stream': stream,
                'config': stream_config,
                'last_processed': datetime.now()
            }
            self.status[stream_id] = status
            
            # Only create processing task if stream initialization succeeded
            self.processing_tasks[stream_id] = asyncio.create_task(
                self._process_stream(stream_id)
            )
            
        except Exception as e:
            self.logger.error(
                f"Failed to initialize stream {stream_name} for connection {connection_id}: {str(e)}"
            )
            # Create or update status for failed stream
            status = StreamStatus(
                active=False,
                connection_id=connection_id,
                source_name=stream_name,
                last_error=str(e),
                last_error_time=datetime.now(),
                total_errors=1
            )
            self.status[stream_id] = status
            
            # Don't create processing task for failed stream
            if stream_id in self.processing_tasks:
                self.processing_tasks[stream_id].cancel()
                del self.processing_tasks[stream_id]
                
            await self._handle_connection_error(connection_id, e)

    async def _process_stream(self, stream_id: str):
        """Process changes from a specific stream"""
        stream_info = self.streams[stream_id]
        status = self.status[stream_id]
        client = self.clients[status.connection_id]
        max_retries = self.config['error_handling']['max_retries']
        
        self.logger.info(f"Starting stream processing for {stream_id}")
        
        while status.active and status.retry_count <= max_retries:
            try:
                # Use existing stream or create a new one
                stream = stream_info.get('stream')
                if stream is None:
                    self.logger.info(f"Creating new stream for {stream_id}")
                    db = client[stream_info['config']['database']]
                    collection = db[stream_info['config']['collection']]
                    try:
                        stream = collection.watch(
                            pipeline=stream_info['config'].get('pipeline', []),
                            batch_size=stream_info['config'].get('batch_size', 100)
                        )
                        stream_info['stream'] = stream
                    except PyMongoError as e:
                        self.logger.error(f"Failed to create stream for {stream_id}: {str(e)}")
                        await self._handle_stream_error(stream_id, e)
                        if status.retry_count >= max_retries:
                            status.active = False
                            return
                        continue
                else:
                    self.logger.info(f"Using existing stream for {stream_id}")
                
                async with stream as active_stream:
                    self.logger.info(f"Entered stream context for {stream_id}")
                    while status.active:
                        try:
                            # Use asyncio.wait_for to make the next() call cancellable
                            self.logger.debug(f"Waiting for next change in {stream_id}")
                            change = await asyncio.wait_for(
                                active_stream.__anext__(),
                                timeout=1.0  # 1 second timeout to check status
                            )
                            
                            if change.get('operationType') != 'noop':
                                self.logger.info(f"Processing change in {stream_id}: {change.get('operationType')}")
                                await self._process_change(stream_id, change)
                                status.last_healthy = datetime.now()
                                status.retry_count = 0  # Reset retry count on successful processing
                            else:
                                self.logger.debug(f"Skipping noop change in {stream_id}")
                            
                        except asyncio.TimeoutError:
                            if not status.active:
                                self.logger.info(f"Stream {stream_id} received stop signal")
                                return
                            continue
                            
                        except StopAsyncIteration:
                            self.logger.info(f"Stream {stream_id} iteration complete")
                            if not status.active:
                                return
                            # Add exponential backoff for reconnects
                            backoff = min(
                                2 ** status.retry_count,
                                self.config['error_handling'].get('max_backoff', 30)
                            )
                            self.logger.info(f"Stream {stream_id} waiting {backoff}s before reconnect")
                            await asyncio.sleep(backoff)
                            break
                            
            except asyncio.CancelledError:
                self.logger.info(f"Stream {stream_id} task cancelled")
                status.active = False
                return
                            
            except PyMongoError as e:
                self.logger.error(f"Error in stream {stream_id}: {str(e)}")
                await self._handle_stream_error(stream_id, e)
                
                if status.retry_count >= max_retries:
                    self.logger.error(f"Stopping stream {stream_id} after max retries")
                    status.active = False
                    return
                
                if not status.active:
                    return
                    
                # Wait before retry with backoff
                retry_delay = min(
                    2 ** status.retry_count,
                    self.config['error_handling'].get('max_backoff', 300)
                )
                self.logger.info(f"Stream {stream_id} waiting {retry_delay}s before retry")
                await asyncio.sleep(retry_delay)

    async def _process_change(self, stream_id: str, change: dict):
        """Process a single change event from the change stream"""
        try:
            # Skip noop events
            if change.get('operationType') == 'noop':
                return
            
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
                status.last_error = str(error)
                status.last_error_time = datetime.now()
                status.retry_count += 1
                status.total_errors += 1

    async def _handle_stream_error(self, stream_id: str, error: Exception):
        """Handle stream-specific errors"""
        if stream_id in self.status:
            status = self.status[stream_id]
            status.last_error = str(error)
            status.last_error_time = datetime.now()
            status.retry_count += 1
            status.total_errors += 1

    async def cleanup(self):
        """Clean up all connections and streams"""
        # Stop all streams
        for stream_id in self.status:
            self.status[stream_id].active = False
        
        # Cancel all processing tasks
        if self.processing_tasks:
            tasks_to_cancel = []
            for stream_id, task in self.processing_tasks.items():
                if not task.done():
                    task.cancel()
                    tasks_to_cancel.append(task)
            
            if tasks_to_cancel:
                try:
                    await asyncio.wait_for(
                        asyncio.gather(*tasks_to_cancel, return_exceptions=True),
                        timeout=1.0
                    )
                except asyncio.TimeoutError:
                    self.logger.warning("Timeout waiting for tasks to cancel")
        
        # Close all clients
        close_tasks = []
        for client in self.clients.values():
            close_tasks.append(client.close())
        
        if close_tasks:
            await asyncio.gather(*close_tasks, return_exceptions=True)
        
        # Clear internal state
        self.processing_tasks.clear()
        self.streams.clear()
        self.clients.clear()

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