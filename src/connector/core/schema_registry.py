"""Schema registry for managing versioned document schemas."""

from typing import Dict, Any, Optional
from enum import Enum

class DocumentType(Enum):
    """Supported document types."""
    INVENTORY = "inventory"
    TRANSACTION = "transaction"

# Base schema that all versions must include
BASE_SCHEMA = {
    "type": "object",
    "required": ["_schema_version"],
    "properties": {
        "_schema_version": {"type": "string", "pattern": "^v\\d+$"}
    }
}

# Inventory schema versions
INVENTORY_SCHEMAS: Dict[str, Dict[str, Any]] = {
    "v1": {
        "type": "object",
        "required": [
            "_schema_version",
            "product_id",
            "warehouse_id",
            "quantity",
            "last_updated"
        ],
        "properties": {
            "_schema_version": {"type": "string", "enum": ["v1"]},
            "product_id": {"type": "string"},
            "warehouse_id": {"type": "string"},
            "quantity": {"type": "integer", "minimum": 0},
            "last_updated": {"type": "string", "format": "date-time"},
            "category": {"type": "string"},
            "brand": {"type": "string"},
            "sku": {"type": "string"},
            "threshold_min": {"type": "integer", "minimum": 0},
            "threshold_max": {"type": "integer", "minimum": 0}
        },
        "additionalProperties": True
    }
}

# Transaction schema versions
TRANSACTION_SCHEMAS: Dict[str, Dict[str, Any]] = {
    "v1": {
        "type": "object",
        "required": [
            "_schema_version",
            "transaction_id",
            "product_id",
            "warehouse_id",
            "quantity",
            "transaction_type",
            "timestamp"
        ],
        "properties": {
            "_schema_version": {"type": "string", "enum": ["v1"]},
            "transaction_id": {"type": "string"},
            "product_id": {"type": "string"},
            "warehouse_id": {"type": "string"},
            "quantity": {"type": "integer"},
            "transaction_type": {
                "type": "string",
                "enum": ["sale", "restock", "return", "adjustment"]
            },
            "timestamp": {"type": "string", "format": "date-time"},
            "order_id": {"type": "string"},
            "customer_id": {"type": "string"},
            "notes": {"type": "string"}
        },
        "additionalProperties": True
    }
}

class SchemaRegistry:
    """Registry for managing document schemas and their versions."""

    @staticmethod
    def get_latest_version(doc_type: DocumentType) -> str:
        """Get the latest schema version for a document type.
        
        Args:
            doc_type: The document type to get the latest version for.
            
        Returns:
            The latest schema version string (e.g., 'v1').
        """
        schemas = (
            INVENTORY_SCHEMAS if doc_type == DocumentType.INVENTORY
            else TRANSACTION_SCHEMAS
        )
        return max(schemas.keys())

    @staticmethod
    def get_schema(doc_type: DocumentType, version: Optional[str] = None) -> Dict[str, Any]:
        """Get the schema for a specific document type and version.
        
        Args:
            doc_type: The document type to get the schema for.
            version: Optional version string. If not provided, returns the latest version.
            
        Returns:
            The schema dictionary.
            
        Raises:
            ValueError: If the version doesn't exist for the document type.
        """
        schemas = (
            INVENTORY_SCHEMAS if doc_type == DocumentType.INVENTORY
            else TRANSACTION_SCHEMAS
        )
        
        if version is None:
            version = SchemaRegistry.get_latest_version(doc_type)
            
        if version not in schemas:
            raise ValueError(
                f"Schema version {version} not found for document type {doc_type.value}"
            )
            
        return schemas[version]

    @staticmethod
    def is_valid_version(doc_type: DocumentType, version: str) -> bool:
        """Check if a schema version exists for a document type.
        
        Args:
            doc_type: The document type to check.
            version: The version string to check.
            
        Returns:
            True if the version exists, False otherwise.
        """
        schemas = (
            INVENTORY_SCHEMAS if doc_type == DocumentType.INVENTORY
            else TRANSACTION_SCHEMAS
        )
        return version in schemas 