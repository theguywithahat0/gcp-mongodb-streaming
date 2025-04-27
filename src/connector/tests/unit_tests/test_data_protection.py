"""Tests for the data_protection module."""

import os
import unittest
import base64
from unittest.mock import patch, MagicMock

from ...security import (
    DataProtector,
    FieldProtectionConfig,
    ProtectionLevel,
    create_pii_protection_rules,
    create_payment_protection_rules
)

class TestDataProtection(unittest.TestCase):
    """Test cases for the DataProtector class."""

    def setUp(self):
        """Set up test environment."""
        # Use a test key - in real environments, use a secure key management system
        self.test_key = Fernet.generate_key().decode()
        os.environ["ENCRYPTION_KEY"] = self.test_key
        self.data_protector = DataProtector()

    def test_hash_protection(self):
        """Test hash protection of sensitive fields."""
        # Configure a hash protection rule
        rule = FieldProtectionConfig(
            field_matcher="customer.email",
            protection_level=ProtectionLevel.HASH,
            options={"algorithm": "sha256", "include_salt": True}
        )
        self.data_protector.add_protection_rule(rule)
        
        # Test document
        doc = {
            "customer": {
                "email": "test@example.com",
                "name": "Test User"
            }
        }
        
        # Apply protection
        protected_doc = self.data_protector.apply_field_protection(doc)
        
        # Verify the email was hashed
        self.assertNotEqual(
            protected_doc["customer"]["email"], 
            doc["customer"]["email"]
        )
        self.assertTrue(
            isinstance(protected_doc["customer"]["email"], str)
        )
        # Name should be untouched
        self.assertEqual(
            protected_doc["customer"]["name"], 
            doc["customer"]["name"]
        )
        
        # Verify the protection is deterministic
        protected_doc2 = self.data_protector.apply_field_protection(doc)
        self.assertEqual(
            protected_doc["customer"]["email"],
            protected_doc2["customer"]["email"]
        )
        
        # Verify metadata
        self.assertIn("_security_metadata", protected_doc)
        self.assertIn("protected_fields", protected_doc["_security_metadata"])
        self.assertEqual(
            protected_doc["_security_metadata"]["protected_fields"][0]["field"],
            "customer.email"
        )

    def test_mask_protection(self):
        """Test masking of sensitive fields."""
        # Configure a mask protection rule
        rule = FieldProtectionConfig(
            field_matcher="payment.card_number",
            protection_level=ProtectionLevel.MASK,
            options={"visible_right": 4, "mask_char": "X"}
        )
        self.data_protector.add_protection_rule(rule)
        
        # Test document
        doc = {
            "payment": {
                "card_number": "4111111111111111",
                "cvv": "123"
            }
        }
        
        # Apply protection
        protected_doc = self.data_protector.apply_field_protection(doc)
        
        # Verify the card number was masked properly
        self.assertEqual(
            protected_doc["payment"]["card_number"],
            "XXXXXXXXXXXX1111"
        )
        # CVV should be untouched
        self.assertEqual(
            protected_doc["payment"]["cvv"],
            doc["payment"]["cvv"]
        )

    def test_remove_protection(self):
        """Test removal of sensitive fields."""
        # Configure a removal rule
        rule = FieldProtectionConfig(
            field_matcher="payment.cvv",
            protection_level=ProtectionLevel.REMOVE
        )
        self.data_protector.add_protection_rule(rule)
        
        # Test document
        doc = {
            "payment": {
                "card_number": "4111111111111111",
                "cvv": "123"
            }
        }
        
        # Apply protection
        protected_doc = self.data_protector.apply_field_protection(doc)
        
        # Verify CVV was removed
        self.assertNotIn("cvv", protected_doc["payment"])
        # Card number should be untouched
        self.assertEqual(
            protected_doc["payment"]["card_number"],
            doc["payment"]["card_number"]
        )

    def test_encryption_protection(self):
        """Test encryption of sensitive fields."""
        # Configure an encryption rule
        rule = FieldProtectionConfig(
            field_matcher="user.ssn",
            protection_level=ProtectionLevel.ENCRYPT,
            options={"use_context": False}
        )
        self.data_protector.add_protection_rule(rule)
        
        # Test document
        doc = {
            "user": {
                "ssn": "123-45-6789",
                "name": "Test User"
            }
        }
        
        # Apply protection
        protected_doc = self.data_protector.apply_field_protection(doc)
        
        # Verify the SSN was encrypted
        self.assertNotEqual(
            protected_doc["user"]["ssn"],
            doc["user"]["ssn"]
        )
        self.assertTrue(
            protected_doc["user"]["ssn"].startswith("enc:")
        )
        # Name should be untouched
        self.assertEqual(
            protected_doc["user"]["name"],
            doc["user"]["name"]
        )

    def test_nested_document_protection(self):
        """Test protection of nested documents and arrays."""
        # Configure rules
        email_rule = FieldProtectionConfig(
            field_matcher="user.email",
            protection_level=ProtectionLevel.MASK,
            options={"visible_left": 2, "mask_char": "*"}
        )
        card_rule = FieldProtectionConfig(
            field_matcher="user.payment_methods.card_number",
            protection_level=ProtectionLevel.MASK,
            options={"visible_right": 4, "mask_char": "X"}
        )
        self.data_protector.add_protection_rule(email_rule)
        self.data_protector.add_protection_rule(card_rule)
        
        # Test document with nested array
        doc = {
            "user": {
                "email": "test@example.com",
                "payment_methods": [
                    {
                        "type": "credit_card",
                        "card_number": "4111111111111111"
                    },
                    {
                        "type": "credit_card",
                        "card_number": "5555555555554444"
                    }
                ]
            }
        }
        
        # Apply protection
        protected_doc = self.data_protector.apply_field_protection(doc)
        
        # Verify email was masked
        self.assertEqual(
            protected_doc["user"]["email"],
            "te*************"
        )
        
        # Verify card numbers in array were masked
        self.assertEqual(
            protected_doc["user"]["payment_methods"][0]["card_number"],
            "XXXXXXXXXXXX1111"
        )
        self.assertEqual(
            protected_doc["user"]["payment_methods"][1]["card_number"],
            "XXXXXXXXXXXX4444"
        )

    def test_regex_field_matcher(self):
        """Test using regex patterns for field matching."""
        import re
        
        # Configure a rule with regex pattern
        rule = FieldProtectionConfig(
            field_matcher=re.compile(r'.*email.*', re.IGNORECASE),
            protection_level=ProtectionLevel.MASK,
            options={"visible_left": 2, "mask_char": "*"}
        )
        self.data_protector.add_protection_rule(rule)
        
        # Test document with various email fields
        doc = {
            "customer": {
                "email": "customer@example.com",
                "alternate_email": "alternate@example.com",
                "contact": {
                    "work_email": "work@example.com"
                }
            }
        }
        
        # Apply protection
        protected_doc = self.data_protector.apply_field_protection(doc)
        
        # Verify all email fields were masked
        self.assertEqual(
            protected_doc["customer"]["email"],
            "cu*******************"
        )
        self.assertEqual(
            protected_doc["customer"]["alternate_email"],
            "al*******************"
        )
        self.assertEqual(
            protected_doc["customer"]["contact"]["work_email"],
            "wo****************"
        )

    def test_predefined_rules(self):
        """Test the predefined protection rule sets."""
        # Create a data protector with predefined PII rules
        data_protector = DataProtector()
        for rule in create_pii_protection_rules():
            data_protector.add_protection_rule(rule)
        
        # Test document with various PII fields
        doc = {
            "customer": {
                "email": "customer@example.com",
                "phone": "555-123-4567",
                "address": "123 Main St, Apt 4B, Springfield, IL 62701",
                "payment": {
                    "card_number": "4111111111111111",
                    "cvv": "123"
                },
                "ssn": "123-45-6789"
            }
        }
        
        # Apply protection
        protected_doc = data_protector.apply_field_protection(doc)
        
        # Verify fields were protected appropriately
        self.assertTrue(protected_doc["customer"]["email"].startswith("cu"))
        self.assertTrue(protected_doc["customer"]["phone"].endswith("4567"))
        self.assertNotIn("123", protected_doc["customer"]["address"])
        self.assertNotIn("cvv", protected_doc["customer"]["payment"])
        self.assertNotIn("ssn", protected_doc["customer"])

    def test_transform_func_integration(self):
        """Test integration with DocumentTransformer."""
        # Mock the DocumentTransformer
        mock_transformer = MagicMock()
        
        # Create data protector with a rule
        data_protector = DataProtector()
        rule = FieldProtectionConfig(
            field_matcher="customer.email",
            protection_level=ProtectionLevel.HASH
        )
        data_protector.add_protection_rule(rule)
        
        # Get transform function
        transform_func = data_protector.create_transform_func()
        
        # Test document
        doc = {
            "customer": {
                "email": "test@example.com"
            }
        }
        
        # Apply transformation directly
        transformed_doc = transform_func(doc)
        
        # Verify email was hashed
        self.assertNotEqual(
            transformed_doc["customer"]["email"],
            doc["customer"]["email"]
        )
        
        # Test with mock transformer
        transform_func(doc)  # This would be called by DocumentTransformer


# Import here to avoid circular imports in the test
from cryptography.fernet import Fernet

if __name__ == "__main__":
    unittest.main() 