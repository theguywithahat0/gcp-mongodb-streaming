"""
Schema validator for MongoDB documents in the GCP connector pipeline.

This module provides schema validation functionality for inventory and transaction
documents before they are processed and sent to Google Cloud Pub/Sub.
"""

from typing import Dict, Any, Optional
from jsonschema import validate, ValidationError
import logging

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
            schema = INVENTORY_SCHEMA if doc_type == 'inventory' else TRANSACTION_SCHEMA
            validate(instance=document, schema=schema)
            return None
        except ValidationError as e:
            error_msg = f"Schema validation failed for {doc_type} document: {str(e)}"
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