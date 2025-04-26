"""Message deduplication implementation."""

import time
from typing import Dict, Optional, Set
import logging
from dataclasses import dataclass
from datetime import datetime, timedelta

from google.cloud import firestore
from google.cloud.firestore_v1.base_query import FieldFilter

@dataclass
class DeduplicationConfig:
    """Configuration for message deduplication."""
    # In-memory deduplication settings
    memory_ttl: int = 3600  # Time to keep messages in memory (seconds)
    memory_max_size: int = 10000  # Maximum number of messages to keep in memory
    
    # Persistent deduplication settings
    persistent_enabled: bool = True
    persistent_ttl: int = 86400  # Time to keep messages in persistent storage (seconds)
    cleanup_interval: int = 3600  # How often to clean up old entries (seconds)

class MessageDeduplication:
    """Handles message deduplication using both memory and persistent storage.
    
    This class provides two levels of deduplication:
    1. In-memory cache for fast deduplication of recent messages
    2. Persistent storage (Firestore) for long-term deduplication
    
    The in-memory cache is used first for performance, falling back to
    persistent storage only when necessary.
    """

    def __init__(
        self,
        config: DeduplicationConfig,
        firestore_client: firestore.Client,
        collection_name: str
    ):
        """Initialize the message deduplication service.
        
        Args:
            config: Deduplication configuration
            firestore_client: Firestore client for persistent storage
            collection_name: Name of the MongoDB collection being watched
        """
        self.config = config
        self.firestore_client = firestore_client
        self.collection_name = collection_name
        
        # In-memory deduplication cache
        self._message_cache: Dict[str, float] = {}
        self._last_cleanup = time.time()
        
        # Logger
        self.logger = logging.getLogger(__name__)

    def is_duplicate(
        self,
        message_id: str,
        operation_type: str,
        timestamp: float
    ) -> bool:
        """Check if a message is a duplicate.
        
        Args:
            message_id: Unique identifier for the message (e.g., document ID)
            operation_type: Type of operation (insert, update, delete)
            timestamp: Message timestamp
            
        Returns:
            bool: True if the message is a duplicate
        """
        # Clean up expired entries if needed
        self._cleanup_if_needed()
        
        # Generate a unique key for this message
        dedup_key = self._generate_dedup_key(message_id, operation_type)
        
        # Check in-memory cache first
        if self._check_memory_duplicate(dedup_key, timestamp):
            return True
            
        # If not in memory and persistent storage is enabled, check there
        if self.config.persistent_enabled:
            is_duplicate = self._check_persistent_duplicate(
                dedup_key,
                timestamp
            )
            if not is_duplicate:
                # If not a duplicate, store in persistent storage
                self._store_persistent(dedup_key, timestamp)
            return is_duplicate
            
        return False

    def _generate_dedup_key(self, message_id: str, operation_type: str) -> str:
        """Generate a unique deduplication key.
        
        Args:
            message_id: Message identifier
            operation_type: Operation type
            
        Returns:
            str: Unique deduplication key
        """
        return f"{self.collection_name}:{message_id}:{operation_type}"

    def _check_memory_duplicate(self, dedup_key: str, timestamp: float) -> bool:
        """Check for duplicates in memory cache.
        
        Args:
            dedup_key: Deduplication key
            timestamp: Message timestamp
            
        Returns:
            bool: True if duplicate found in memory
        """
        if dedup_key in self._message_cache:
            existing_timestamp = self._message_cache[dedup_key]
            if timestamp <= existing_timestamp:
                self.logger.debug(
                    "Duplicate message detected in memory cache",
                    extra={
                        "dedup_key": dedup_key,
                        "existing_timestamp": existing_timestamp,
                        "new_timestamp": timestamp
                    }
                )
                return True
                
        # Not a duplicate, add to cache
        self._message_cache[dedup_key] = timestamp
        return False

    def _check_persistent_duplicate(
        self,
        dedup_key: str,
        timestamp: float
    ) -> bool:
        """Check for duplicates in persistent storage.
        
        Args:
            dedup_key: Deduplication key
            timestamp: Message timestamp
            
        Returns:
            bool: True if duplicate found in persistent storage
        """
        try:
            # Query Firestore for existing message
            doc_ref = self._get_firestore_ref(dedup_key)
            doc = doc_ref.get()
            
            if doc.exists:
                existing_data = doc.to_dict()
                if timestamp <= existing_data.get("timestamp", 0):
                    self.logger.debug(
                        "Duplicate message detected in persistent storage",
                        extra={
                            "dedup_key": dedup_key,
                            "existing_timestamp": existing_data.get("timestamp"),
                            "new_timestamp": timestamp
                        }
                    )
                    return True
                    
            return False
            
        except Exception as e:
            self.logger.error(
                "Error checking persistent storage for duplicates",
                extra={
                    "error": str(e),
                    "dedup_key": dedup_key
                }
            )
            # If we can't check persistent storage, assume not duplicate
            return False

    def _store_persistent(self, dedup_key: str, timestamp: float) -> None:
        """Store message information in persistent storage.
        
        Args:
            dedup_key: Deduplication key
            timestamp: Message timestamp
        """
        try:
            doc_ref = self._get_firestore_ref(dedup_key)
            doc_ref.set({
                "timestamp": timestamp,
                "created_at": datetime.utcnow(),
                "collection": self.collection_name,
                "ttl": datetime.utcnow() + timedelta(
                    seconds=self.config.persistent_ttl
                )
            })
        except Exception as e:
            self.logger.error(
                "Error storing message in persistent storage",
                extra={
                    "error": str(e),
                    "dedup_key": dedup_key
                }
            )

    def _get_firestore_ref(self, dedup_key: str) -> firestore.DocumentReference:
        """Get Firestore document reference for a deduplication key.
        
        Args:
            dedup_key: Deduplication key
            
        Returns:
            firestore.DocumentReference: Document reference
        """
        return (
            self.firestore_client
            .collection("message_deduplication")
            .document(dedup_key)
        )

    def _cleanup_if_needed(self) -> None:
        """Clean up expired entries from memory cache if needed."""
        current_time = time.time()
        
        # Check if it's time to clean up
        if (
            current_time - self._last_cleanup >
            self.config.cleanup_interval
        ):
            self._cleanup_memory()
            if self.config.persistent_enabled:
                self._cleanup_persistent()
            self._last_cleanup = current_time

    def _cleanup_memory(self) -> None:
        """Clean up expired entries from memory cache."""
        current_time = time.time()
        expired_keys = []
        
        for key, (timestamp, _) in self._message_cache.items():
            # Convert MongoDB Timestamp to float if needed
            if hasattr(timestamp, 'time'):
                timestamp = float(timestamp.time)
            if current_time - timestamp > self.config.memory_ttl:
                expired_keys.append(key)
                
        for key in expired_keys:
            del self._message_cache[key]
            
        # If still too many entries, remove oldest
        if len(self._message_cache) > self.config.memory_max_size:
            sorted_items = sorted(
                self._message_cache.items(),
                key=lambda x: x[1]
            )
            to_remove = len(self._message_cache) - self.config.memory_max_size
            for key, _ in sorted_items[:to_remove]:
                del self._message_cache[key]

    def _cleanup_persistent(self) -> None:
        """Clean up expired entries from persistent storage."""
        try:
            # Query for expired documents
            expired_time = datetime.utcnow()
            
            # Delete expired documents in batches
            while True:
                # Get batch of expired documents
                query = (
                    self.firestore_client
                    .collection("message_deduplication")
                    .where(filter=FieldFilter("ttl", "<", expired_time))
                    .limit(500)
                )
                
                docs = query.stream()
                batch = self.firestore_client.batch()
                count = 0
                
                # Add deletes to batch
                for doc in docs:
                    batch.delete(doc.reference)
                    count += 1
                
                if count == 0:
                    break
                    
                # Commit the batch
                batch.commit()
                
        except Exception as e:
            self.logger.error(
                "Error cleaning up persistent storage",
                extra={"error": str(e)}
            ) 