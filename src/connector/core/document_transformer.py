"""Document transformer for applying configurable transformations to documents."""

from typing import Any, Callable, Dict, List, Optional, Tuple
from enum import Enum
from ..logging.logging_config import get_logger
from datetime import datetime, UTC
import hashlib
import json
from functools import lru_cache

from .schema_registry import DocumentType

# Type alias for transformation functions
TransformFunc = Callable[[Dict[str, Any]], Dict[str, Any]]

class TransformStage(Enum):
    """Stages where transformations can be applied."""
    PRE_VALIDATION = "pre_validation"
    POST_VALIDATION = "post_validation"
    PRE_PUBLISH = "pre_publish"

class TransformationError(Exception):
    """Exception raised when a transformation fails."""
    pass

class DocumentTransformer:
    """Manages and applies document transformations."""

    def __init__(self, cache_size: int = 1000):
        """Initialize the document transformer.
        
        Args:
            cache_size: Maximum number of cached transformation results to keep.
        """
        # Dictionary to store transformation functions by stage and document type
        self._transforms: Dict[TransformStage, Dict[DocumentType, List[TransformFunc]]] = {
            stage: {doc_type: [] for doc_type in DocumentType}
            for stage in TransformStage
        }

        # Cache configuration
        self._cache_size = cache_size
        
        # Initialize logger
        self.logger = get_logger(__name__)

    def _generate_cache_key(
        self,
        document: Dict[str, Any],
        doc_type: DocumentType,
        stage: TransformStage
    ) -> str:
        """Generate a cache key for a document transformation.
        
        Args:
            document: The document to transform
            doc_type: The type of document
            stage: The transformation stage
            
        Returns:
            A unique cache key string
        """
        # Create a deterministic string representation of the document
        doc_str = json.dumps(document, sort_keys=True)
        
        # Include document type and stage in the key
        key_parts = [
            doc_str,
            doc_type.value,
            stage.value
        ]
        
        # Generate a hash of the combined string
        return hashlib.sha256('|'.join(key_parts).encode()).hexdigest()

    @lru_cache(maxsize=1000)
    def _cached_transform(
        self,
        cache_key: str,
        transform_func: TransformFunc,
        document: str
    ) -> str:
        """Apply a single transformation with caching.
        
        Args:
            cache_key: Unique key for caching
            transform_func: The transformation function to apply
            document: JSON string representation of the document
            
        Returns:
            JSON string of transformed document
        """
        doc_dict = json.loads(document)
        transformed = transform_func(doc_dict)
        return json.dumps(transformed, sort_keys=True)

    def register_transform(
        self,
        stage: TransformStage,
        doc_type: DocumentType,
        transform_func: TransformFunc,
        position: Optional[int] = None
    ) -> None:
        """Register a transformation function.
        
        Args:
            stage: The stage at which to apply the transformation.
            doc_type: The type of document this transformation applies to.
            transform_func: The transformation function to register.
            position: Optional position in the transformation chain (default: append).
            
        Example:
            ```python
            def add_timestamp(doc):
                return {**doc, "processed_at": datetime.utcnow().isoformat()}
            
            transformer.register_transform(
                TransformStage.PRE_PUBLISH,
                DocumentType.INVENTORY,
                add_timestamp
            )
            ```
        """
        transforms = self._transforms[stage][doc_type]
        if position is not None:
            transforms.insert(min(position, len(transforms)), transform_func)
        else:
            transforms.append(transform_func)

        self.logger.info(
            f"Registered transformation for {doc_type.value} at {stage.value} stage"
            f"{f' at position {position}' if position is not None else ''}"
        )

    def apply_transforms(
        self,
        document: Dict[str, Any],
        doc_type: DocumentType,
        stage: TransformStage
    ) -> Dict[str, Any]:
        """Apply all registered transformations for a stage.
        
        Args:
            document: The document to transform.
            doc_type: The type of document being transformed.
            stage: The transformation stage.
            
        Returns:
            The transformed document.
            
        Raises:
            TransformationError: If any transformation fails.
        """
        transformed = document.copy()
        transforms = self._transforms[stage][doc_type]

        # Generate base cache key for this document
        base_cache_key = self._generate_cache_key(document, doc_type, stage)
        
        for transform in transforms:
            try:
                # Create unique cache key for this specific transformation
                transform_key = f"{base_cache_key}_{transform.__name__}"
                
                # Convert document to JSON string for caching
                doc_str = json.dumps(transformed, sort_keys=True)
                
                # Apply cached transformation
                result_str = self._cached_transform(transform_key, transform, doc_str)
                transformed = json.loads(result_str)
                
                if not isinstance(transformed, dict):
                    raise TransformationError(
                        f"Transform {transform.__name__} returned {type(transformed)}, "
                        "expected dict"
                    )
            except Exception as e:
                error_msg = (
                    f"Transform {transform.__name__} failed for {doc_type.value} "
                    f"at {stage.value} stage: {str(e)}"
                )
                self.logger.error(error_msg)
                raise TransformationError(error_msg) from e

        return transformed

    def clear_transforms(
        self,
        stage: Optional[TransformStage] = None,
        doc_type: Optional[DocumentType] = None
    ) -> None:
        """Clear registered transformations and their cached results.
        
        Args:
            stage: Optional stage to clear. If None, clears all stages.
            doc_type: Optional document type to clear. If None, clears all types.
        """
        if stage is None and doc_type is None:
            # Clear all transforms and cache
            self._transforms = {
                stage: {doc_type: [] for doc_type in DocumentType}
                for stage in TransformStage
            }
            self._cached_transform.cache_clear()
        elif stage is None:
            # Clear all transforms for a specific document type
            for stage_transforms in self._transforms.values():
                stage_transforms[doc_type] = []
            self._cached_transform.cache_clear()
        elif doc_type is None:
            # Clear all transforms for a specific stage
            for doc_type_transforms in self._transforms[stage].values():
                doc_type_transforms.clear()
            self._cached_transform.cache_clear()
        else:
            # Clear transforms for specific stage and document type
            self._transforms[stage][doc_type] = []
            self._cached_transform.cache_clear()

        self.logger.info(
            "Cleared transformations and cache"
            f"{f' for {doc_type.value}' if doc_type else ''}"
            f"{f' at {stage.value} stage' if stage else ''}"
        )

    @staticmethod
    def compose_transforms(*transforms: TransformFunc) -> TransformFunc:
        """Compose multiple transformation functions into a single function.
        
        Args:
            *transforms: Transform functions to compose.
            
        Returns:
            A single function that applies all transforms in sequence.
            
        Example:
            ```python
            add_timestamp = lambda doc: {**doc, "timestamp": datetime.utcnow().isoformat()}
            add_version = lambda doc: {**doc, "version": "1.0"}
            
            combined = DocumentTransformer.compose_transforms(add_timestamp, add_version)
            transformer.register_transform(
                TransformStage.PRE_PUBLISH,
                DocumentType.INVENTORY,
                combined
            )
            ```
        """
        def composed(doc: Dict[str, Any]) -> Dict[str, Any]:
            result = doc.copy()
            for transform in transforms:
                result = transform(result)
            return result
        
        # Set a meaningful name for the composed function
        composed.__name__ = f"composed_{'_'.join(t.__name__ for t in transforms)}"
        return composed

# Common transformation functions
def add_processing_metadata(doc: Dict[str, Any]) -> Dict[str, Any]:
    """Add processing metadata to a document.
    
    Args:
        doc: The document to transform.
        
    Returns:
        Document with added metadata.
    """
    return {
        **doc,
        "_processing_metadata": {
            "processed_at": datetime.now(UTC).isoformat(),
            "processor_version": "1.0"
        }
    }

def remove_sensitive_fields(fields_to_remove: List[str]) -> TransformFunc:
    """Create a transformation function that removes sensitive fields.
    
    Args:
        fields_to_remove: List of field names to remove from the document.
        
    Returns:
        A transformation function that removes specified fields.
        
    Example:
        ```python
        remove_pii = remove_sensitive_fields(['customer_email', 'phone_number'])
        transformer.register_transform(
            TransformStage.PRE_PUBLISH,
            DocumentType.TRANSACTION,
            remove_pii
        )
        ```
    """
    def transform(doc: Dict[str, Any]) -> Dict[str, Any]:
        result = doc.copy()
        for field in fields_to_remove:
            # Handle nested fields using dot notation
            parts = field.split('.')
            current = result
            for i, part in enumerate(parts[:-1]):
                if part in current and isinstance(current[part], dict):
                    current = current[part]
                else:
                    break
            else:
                # Remove the field if it exists
                if parts[-1] in current:
                    del current[parts[-1]]
        
        # Add metadata about removed fields
        if "_security_metadata" not in result:
            result["_security_metadata"] = {}
        result["_security_metadata"]["removed_fields"] = fields_to_remove
        result["_security_metadata"]["sanitized_at"] = datetime.now(UTC).isoformat()
        
        return result
    
    transform.__name__ = f"remove_sensitive_fields_{','.join(fields_to_remove)}"
    return transform

def add_field_prefix(prefix: str, fields: Optional[List[str]] = None) -> TransformFunc:
    """Create a transformation function that adds a prefix to field names.

    Args:
        prefix (str): The prefix to add to field names.
        fields (Optional[List[str]], optional): List of fields to prefix. If None, all fields are prefixed.

    Returns:
        TransformFunc: A function that prefixes specified fields in documents.

    Example:
        ```python
        add_warehouse_prefix = add_field_prefix('wh_123_')
        transformer.register_transform(
            TransformStage.PRE_PUBLISH,
            DocumentType.INVENTORY,
            add_warehouse_prefix
        )
        ```
    """
    def transform(doc: Dict[str, Any]) -> Dict[str, Any]:
        # Initialize security metadata
        metadata = {
            "field_prefix": prefix,
            "prefixed_at": datetime.now(UTC).isoformat(),
            "prefixed_fields": []
        }

        def add_prefix_recursive(obj: Dict[str, Any], current_path: str = "") -> Dict[str, Any]:
            if not isinstance(obj, dict):
                return obj

            result = {}
            for key, value in obj.items():
                # Skip metadata fields
                if key.startswith("_"):
                    result[key] = value
                    continue

                field_path = f"{current_path}.{key}" if current_path else key

                # Check if field should be prefixed
                should_prefix = fields is None or field_path in fields
                
                # Handle nested dictionaries
                if isinstance(value, dict):
                    result[key if not should_prefix else f"{prefix}_{key}"] = add_prefix_recursive(
                        value, field_path
                    )
                    if should_prefix:
                        metadata["prefixed_fields"].append(field_path)
                else:
                    if should_prefix:
                        result[f"{prefix}_{key}"] = value
                        metadata["prefixed_fields"].append(field_path)
                    else:
                        result[key] = value

            return result

        result = add_prefix_recursive(doc.copy())
        result["_security_metadata"] = metadata
        return result

    transform.__name__ = f"add_field_prefix_{prefix}"
    return transform 