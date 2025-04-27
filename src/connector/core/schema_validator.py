"""
Schema validator for MongoDB documents in the GCP connector pipeline.

This module provides schema validation functionality for inventory and transaction
documents before they are processed and sent to Google Cloud Pub/Sub.

All schema definitions are managed by the SchemaRegistry class. This validator
uses those schemas to validate documents against their respective versions.
"""

from typing import Dict, Any, Optional, Tuple
from jsonschema import validate, ValidationError
import logging

from .schema_registry import SchemaRegistry, DocumentType

# Schema for inventory documents
INVENTORY_SCHEMA = {
    "type": "object",
    "required": [
        "product_id",
        "warehouse_id",
        "quantity",
        "last_updated"
    ],
    "properties": {
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

# Schema for transaction documents
TRANSACTION_SCHEMA = {
    "type": "object",
    "required": [
        "transaction_id",
        "product_id",
        "warehouse_id",
        "quantity",
        "transaction_type",
        "timestamp"
    ],
    "properties": {
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

class SchemaValidator:
    """Validates MongoDB documents against predefined schemas."""

    @staticmethod
    def get_schema(doc_type: DocumentType, version: str) -> Dict[str, Any]:
        """
        Get the schema for a specific document type and version.

        Args:
            doc_type: Type of document (INVENTORY or TRANSACTION)
            version: Schema version

        Returns:
            The schema definition

        Raises:
            ValueError: If the schema version is invalid
        """
        return SchemaRegistry.get_schema(doc_type, version)

    @staticmethod
    def validate_document(document: Dict[str, Any], doc_type: str) -> Optional[str]:
        """
        Validates a document against its schema.

        Args:
            document: The MongoDB document to validate
            doc_type: Type of document ('inventory' or 'transaction')

        Returns:
            None if validation succeeds, error message string if validation fails
        """
        try:
            # Convert string doc_type to enum
            doc_type_enum = (
                DocumentType.INVENTORY if doc_type == 'inventory'
                else DocumentType.TRANSACTION
            )

            # Get document version, default to latest if not specified
            version = document.get('_schema_version')
            
            # If no version is specified, add the latest version
            if not version:
                version = SchemaRegistry.get_latest_version(doc_type_enum)
                document['_schema_version'] = version
                logging.info(
                    f"Added schema version {version} to {doc_type} document"
                )

            # Get and validate against schema
            schema = SchemaValidator.get_schema(doc_type_enum, version)
            validate(instance=document, schema=schema)
            return None

        except ValidationError as e:
            error_msg = f"Schema validation failed for {doc_type} document: {str(e)}"
            logging.error(error_msg)
            return error_msg
        except ValueError as e:
            error_msg = f"Invalid schema version for {doc_type} document: {str(e)}"
            logging.error(error_msg)
            return error_msg

    @staticmethod
    def is_valid_inventory(document: Dict[str, Any]) -> bool:
        """
        Validates an inventory document.

        Args:
            document: The inventory document to validate

        Returns:
            bool: True if document is valid, False otherwise
        """
        return SchemaValidator.validate_document(document, 'inventory') is None

    @staticmethod
    def is_valid_transaction(document: Dict[str, Any]) -> bool:
        """
        Validates a transaction document.

        Args:
            document: The transaction document to validate

        Returns:
            bool: True if document is valid, False otherwise
        """
        return SchemaValidator.validate_document(document, 'transaction') is None 