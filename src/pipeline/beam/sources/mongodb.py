"""
MongoDB source for Apache Beam.

This module provides:
1. Custom MongoDB source for Apache Beam that reads from change streams
2. Integration with the AsyncMongoDBConnectionManager
3. Conversion of MongoDB change stream events to PCollection elements
"""

import apache_beam as beam
from apache_beam.io import iobase
from apache_beam.io.range_trackers import OffsetRangeTracker, UnsplittableRangeTracker
import asyncio
import logging
import json
import threading
import time
from typing import Dict, Any, Optional, List, Iterable
from queue import Queue, Empty

from ...mongodb.connection_manager import AsyncMongoDBConnectionManager
from ...mongodb.validator import DocumentValidator

logger = logging.getLogger(__name__)

class MongoDBSource(iobase.BoundedSource):
    """
    A BoundedSource for reading from MongoDB change streams using the connection manager.
    
    Note: While change streams are unbounded by nature, this implementation is bounded
    for simplicity in the first version. A future version will implement UnboundedSource.
    """
    
    def __init__(self, config: Dict[str, Any], read_timeout: int = 300):
        """
        Initialize the MongoDB source.
        
        Args:
            config: Configuration dictionary containing MongoDB connection info
            read_timeout: Maximum time in seconds to read from change streams
        """
        self.config = config
        self.read_timeout = read_timeout
        
    def estimate_size(self):
        """
        Estimate the size of the source.
        Since this is a streaming source, we return 0.
        """
        return 0
        
    def split(self, desired_bundle_size, start_position=None, stop_position=None):
        """
        Split the source into bundles.
        This source is not splittable, so we return only self.
        """
        yield iobase.SourceBundle(0, self, None, None)
        
    def get_range_tracker(self, start_position, stop_position):
        """
        Get a range tracker for reading from MongoDB.
        Since this is not splittable, we use UnsplittableRangeTracker.
        """
        return UnsplittableRangeTracker()
        
    def read(self, range_tracker):
        """
        Read from MongoDB change streams.
        
        Args:
            range_tracker: Range tracker (unused for this source)
            
        Returns:
            Generator yielding documents from change streams
        """
        # Use a queue to transfer documents from async world to sync world
        document_queue = Queue()
        stop_event = threading.Event()
        
        # Create and start the async reader thread
        reader_thread = threading.Thread(
            target=self._run_async_reader,
            args=(document_queue, stop_event)
        )
        reader_thread.daemon = True
        reader_thread.start()
        
        # Read from queue until timeout or thread ends
        start_time = time.time()
        try:
            while not stop_event.is_set():
                # Check if we've exceeded the timeout
                if time.time() - start_time > self.read_timeout:
                    logger.info(f"Read timeout reached after {self.read_timeout} seconds")
                    break
                    
                try:
                    # Try to get a document with timeout
                    document = document_queue.get(timeout=1.0)
                    yield document
                    document_queue.task_done()
                except Empty:
                    # Queue is empty, check if reader is still running
                    if not reader_thread.is_alive():
                        logger.info("Reader thread has ended")
                        break
        finally:
            # Signal to the reader to stop
            stop_event.set()
            
            # Wait for reader thread to end (with timeout)
            reader_thread.join(timeout=5.0)
            if reader_thread.is_alive():
                logger.warning("Reader thread did not stop cleanly")
        
    def _run_async_reader(self, document_queue: Queue, stop_event: threading.Event):
        """
        Run the async reader in a separate thread.
        
        Args:
            document_queue: Queue to store documents
            stop_event: Event to signal when to stop
        """
        # Create a new event loop for this thread
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        
        try:
            # Run the async reader
            loop.run_until_complete(
                self._async_read_from_mongodb(document_queue, stop_event)
            )
        except Exception as e:
            logger.error(f"Error in async reader: {str(e)}")
        finally:
            loop.close()
    
    async def _async_read_from_mongodb(self, document_queue: Queue, stop_event: threading.Event):
        """
        Async method to read from MongoDB change streams.
        
        Args:
            document_queue: Queue to store documents
            stop_event: Event to signal when to stop
        """
        # Initialize connection manager
        connection_manager = AsyncMongoDBConnectionManager(self.config)
        
        try:
            # Initialize connections and streams
            await connection_manager.initialize_connections()
            
            # Register a callback to process documents
            async def document_callback(stream_id: str, document: Dict[str, Any]):
                if stop_event.is_set():
                    return False  # Signal to stop processing
                
                # Add document to queue
                document_queue.put(document)
                return True
            
            # Register callback
            connection_manager.register_document_callback(document_callback)
            
            # Start processing
            await connection_manager.start_processing()
            
            # Keep running until stop signal
            while not stop_event.is_set():
                await asyncio.sleep(1.0)
                
        except Exception as e:
            logger.error(f"Error reading from MongoDB: {str(e)}")
        finally:
            # Clean up
            await connection_manager.stop_processing()
            await connection_manager.close()

class ReadFromMongoDB(beam.PTransform):
    """
    A PTransform for reading from MongoDB change streams.
    Uses the MongoDBSource and adds validation/transformation.
    """
    
    def __init__(self, config: Dict[str, Any], schemas: Optional[Dict[str, Dict[str, Any]]] = None):
        """
        Initialize the ReadFromMongoDB transform.
        
        Args:
            config: MongoDB connection configuration
            schemas: Optional validation schemas for documents
        """
        super().__init__()
        self.config = config
        self.schemas = schemas
        
    def expand(self, pcoll):
        """
        Expand the PTransform to include reading and validation.
        
        Args:
            pcoll: Input PCollection (not used)
            
        Returns:
            PCollection of validated documents
        """
        return (
            pcoll.pipeline
            | 'Read' >> beam.io.Read(MongoDBSource(self.config))
            | 'Validate' >> beam.ParDo(ValidateDocumentFn(self.schemas))
        )

class ValidateDocumentFn(beam.DoFn):
    """
    DoFn for validating and transforming MongoDB documents.
    """
    
    def __init__(self, schemas: Optional[Dict[str, Dict[str, Any]]] = None):
        """
        Initialize the validation function.
        
        Args:
            schemas: Validation schemas for documents
        """
        self.schemas = schemas or {}
        
    def setup(self):
        """Set up the validator."""
        self.validator = DocumentValidator(self.schemas)
        
    def process(self, document: Dict[str, Any]) -> Iterable[Dict[str, Any]]:
        """
        Process a document by validating and transforming it.
        
        Args:
            document: Document to validate
            
        Yields:
            Validated document if successful
        """
        try:
            # Extract stream ID from metadata
            metadata = document.get('_metadata', {})
            stream_id = f"{metadata.get('connection_id', 'unknown')}.{metadata.get('source_name', 'unknown')}"
            
            # Validate the document if a schema exists for this stream
            if stream_id in self.schemas:
                transformed = self.validator.transform_document(stream_id, document)
                yield transformed
            else:
                # If no schema, pass through with minimal formatting
                yield document
                
        except Exception as e:
            logger.error(f"Error validating document: {str(e)}")
            # Don't yield anything for invalid documents
