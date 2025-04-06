"""MongoDB document validation and enrichment module.

This module handles:
1. Schema validation for MongoDB documents
2. Document enrichment with metadata
3. Field validation and formatting
"""
from datetime import datetime, UTC
from typing import Any, Dict, List, Optional, Union
import json
import logging

logger = logging.getLogger(__name__)

class DocumentValidator:
    """Handles document validation and enrichment for MongoDB change stream events."""
    
    def __init__(self, schemas: Dict[str, Dict[str, Any]]):
        """
        Initialize the document validator.
        
        Args:
            schemas: Dictionary of stream-specific schemas
                    Format: {
                        "connection_id.source_name": {
                            "field_name": {
                                "type": "string|number|boolean|date|object|array",
                                "required": bool,
                                "nullable": bool,
                                "validation": Optional[callable]
                            }
                        }
                    }
        """
        self.schemas = schemas
        self._validate_schemas()

    def _validate_schemas(self) -> None:
        """Validate all schema definitions."""
        valid_types = {"string", "number", "boolean", "date", "object", "array"}
        
        for stream_id, schema in self.schemas.items():
            if not isinstance(stream_id, str) or "." not in stream_id:
                raise ValueError(f"Invalid stream_id format: {stream_id}")
                
            for field, config in schema.items():
                if not isinstance(field, str):
                    raise ValueError(f"Field name must be string in {stream_id}, got {type(field)}")
                
                if not isinstance(config, dict):
                    raise ValueError(f"Field config must be dict in {stream_id}, got {type(config)}")
                    
                if "type" not in config:
                    raise ValueError(f"Field {field} missing required 'type' key in {stream_id}")
                    
                if config["type"] not in valid_types:
                    raise ValueError(f"Field {field} has invalid type in {stream_id}: {config['type']}")

    def transform_document(self, stream_id: str, document: Dict[str, Any]) -> Dict[str, Any]:
        """
        Transform and validate a document from a specific stream.
        
        Args:
            stream_id: Stream identifier (format: connection_id.source_name)
            document: MongoDB document to transform
            
        Returns:
            Transformed document with added metadata
            
        Raises:
            ValueError: If document fails validation or stream_id not found
        """
        if stream_id not in self.schemas:
            raise ValueError(f"No schema defined for stream: {stream_id}")
            
        # Parse stream context
        connection_id, source_name = stream_id.split(".")
        
        # Validate document structure
        self._validate_document(stream_id, document)
        
        # Add metadata
        transformed = self._add_metadata(document, connection_id, source_name)
        
        # Format fields consistently
        transformed = self._format_fields(transformed)
        
        return transformed

    def _validate_document(self, stream_id: str, document: Dict[str, Any]) -> None:
        """
        Validate document against stream-specific schema.
        
        Args:
            stream_id: Stream identifier
            document: Document to validate
            
        Raises:
            ValueError: If validation fails
        """
        schema = self.schemas[stream_id]
        
        for field, config in schema.items():
            # Check required fields
            if config.get("required", False):
                if field not in document:
                    raise ValueError(f"Required field missing in {stream_id}: {field}")
                    
                if not config.get("nullable", True) and document[field] is None:
                    raise ValueError(f"Required field cannot be null in {stream_id}: {field}")
            
            # Validate field type if present
            if field in document and document[field] is not None:
                self._validate_field_type(stream_id, field, document[field], config["type"])
                
                # Run custom validation if defined
                if "validation" in config and callable(config["validation"]):
                    try:
                        if not config["validation"](document[field]):
                            raise ValueError("Validation function returned False")
                    except Exception as e:
                        raise ValueError(
                            f"Validation failed for field {field} in {stream_id}: {str(e)}"
                        )

    def _validate_field_type(self, stream_id: str, field: str, value: Any, expected_type: str) -> None:
        """
        Validate field type.
        
        Args:
            stream_id: Stream identifier
            field: Field name
            value: Field value
            expected_type: Expected type from schema
            
        Raises:
            ValueError: If type validation fails
        """
        type_map = {
            "string": str,
            "number": (int, float),
            "boolean": bool,
            "date": datetime,
            "object": dict,
            "array": list
        }
        
        expected_python_type = type_map[expected_type]
        
        if not isinstance(value, expected_python_type):
            raise ValueError(
                f"Invalid type for field {field} in {stream_id}: "
                f"expected {expected_type}, got {type(value).__name__}"
            )

    def _add_metadata(self, document: Dict[str, Any], connection_id: str, source_name: str) -> Dict[str, Any]:
        """
        Add metadata to document.
        
        Args:
            document: Original document
            connection_id: MongoDB connection identifier
            source_name: Source collection name
            
        Returns:
            Document with added metadata
        """
        transformed = document.copy()
        transformed["_metadata"] = {
            "processed_at": datetime.now(UTC).isoformat(),
            "source": "mongodb_change_stream",
            "connection_id": connection_id,
            "source_name": source_name,
            "version": "1.0"
        }
        return transformed

    def _format_fields(self, document: Dict[str, Any]) -> Dict[str, Any]:
        """
        Format fields consistently.
        
        Args:
            document: Document to format
            
        Returns:
            Formatted document
        """
        formatted = {}
        
        for key, value in document.items():
            if isinstance(value, datetime):
                formatted[key] = value.isoformat()
            elif isinstance(value, (dict, list)):
                formatted[key] = json.loads(json.dumps(value))
            else:
                formatted[key] = value
                
        return formatted

    def prepare_for_pubsub(self, document: Dict[str, Any], operation_type: str) -> Dict[str, Any]:
        """
        Prepare document for Pub/Sub.
        
        Args:
            document: Transformed document
            operation_type: MongoDB change stream operation type
            
        Returns:
            Document prepared for Pub/Sub
        """
        metadata = document.get("_metadata", {})
        return {
            "operation": operation_type,
            "timestamp": datetime.now(UTC).isoformat(),
            "connection_id": metadata.get("connection_id"),
            "source_name": metadata.get("source_name"),
            "document": document
        }
