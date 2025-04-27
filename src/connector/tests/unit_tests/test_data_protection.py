"""Tests for the data_protection module."""

import os
import unittest
import base64
from unittest.mock import patch, MagicMock
from cryptography.fernet import Fernet

from src.connector.security import (
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
        self.test_key = base64.urlsafe_b64encode(os.urandom(32)).decode()
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
        self.assertTrue(
            protected_doc["customer"]["email"].startswith("hash:")
        )
        
        # Name should be untouched
        self.assertEqual(
            protected_doc["customer"]["name"], 
            doc["customer"]["name"]
        )
        
        # Since we use a deterministic salt based on field path, hashing the same value twice should produce the same result
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
        self.assertEqual(
            protected_doc["_security_metadata"]["protected_fields"][0]["protection"],
            "hash"
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
        encrypted_ssn = protected_doc["user"]["ssn"]
        self.assertTrue(encrypted_ssn.startswith("enc:"))
        self.assertNotEqual(encrypted_ssn, doc["user"]["ssn"])
        
        # Name should be untouched
        self.assertEqual(
            protected_doc["user"]["name"],
            doc["user"]["name"]
        )
        
        # Verify encryption is non-deterministic (different each time)
        protected_doc2 = self.data_protector.apply_field_protection(doc)
        self.assertNotEqual(
            protected_doc["user"]["ssn"],
            protected_doc2["user"]["ssn"]
        )

    def test_field_pattern_matching(self):
        """Test various field pattern matching scenarios."""
        # Test exact matches
        self.assertTrue(self.data_protector._matches_field("user.email", "user.email"))
        self.assertFalse(self.data_protector._matches_field("user.name", "user.email"))

        # Test wildcards in field names
        self.assertTrue(self.data_protector._matches_field("user.email", "user.*"))
        self.assertTrue(self.data_protector._matches_field("user.profile.email", "user.*.email"))
        self.assertFalse(self.data_protector._matches_field("user.profile.name", "user.*.email"))

        # Test array indices
        self.assertTrue(self.data_protector._matches_field("users[0].email", "users[*].email"))
        self.assertTrue(self.data_protector._matches_field("users[42].profile.email", "users[*].profile.email"))
        self.assertFalse(self.data_protector._matches_field("users.0.email", "users[*].email"))

        # Test nested arrays
        self.assertTrue(self.data_protector._matches_field("users[0].addresses[1].street", "users[*].addresses[*].street"))
        self.assertTrue(self.data_protector._matches_field("data[0][1].value", "data[*][*].value"))

        # Test complex patterns
        self.assertTrue(self.data_protector._matches_field(
            "customers[0].orders[1].items[2].price",
            "customers[*].orders[*].items[*].price"
        ))
        self.assertTrue(self.data_protector._matches_field(
            "users[0].profile.addresses[1].details.postcode",
            "users[*].*.addresses[*].*.postcode"
        ))

        # Test invalid patterns
        with self.assertLogs(self.data_protector.logger, level='WARNING') as log:
            # Test empty pattern
            self.assertFalse(self.data_protector._matches_field("test.field", ""))
            # Test invalid regex pattern
            self.assertFalse(self.data_protector._matches_field("test.field", "[invalid"))
            # Test invalid pattern type
            self.assertFalse(self.data_protector._matches_field("test.field", 123))
            
            # Verify warning messages
            log_messages = '\n'.join(log.output)
            self.assertIn("Invalid pattern", log_messages)

    def test_regex_field_matcher(self):
        """Test regex field matching with various patterns."""
        # Test simple field match
        self.assertTrue(self.data_protector._matches_field("user.email", "user.email"))
        
        # Test wildcard matches
        self.assertTrue(self.data_protector._matches_field("user.email", "user.*"))
        self.assertTrue(self.data_protector._matches_field("user.contacts[0].email", "user.contacts[*].email"))
        self.assertTrue(self.data_protector._matches_field("data.items[2].details.value", "data.items[*].details.*"))
        
        # Test non-matches
        self.assertFalse(self.data_protector._matches_field("user.phone", "user.email"))
        self.assertFalse(self.data_protector._matches_field("customer.email", "user.*"))
        self.assertFalse(self.data_protector._matches_field("user.contacts.email", "user.contacts[*].email"))
        
        # Test empty inputs
        self.assertFalse(self.data_protector._matches_field("", "user.email"))
        self.assertFalse(self.data_protector._matches_field("user.email", ""))
        
        # Test complex nested patterns
        self.assertTrue(self.data_protector._matches_field(
            "orders[0].items[1].details.price",
            "orders[*].items[*].details.*"
        ))
        self.assertTrue(self.data_protector._matches_field(
            "user.addresses[0].unit[2].number",
            "user.addresses[*].unit[*].*"
        ))
        
        # Test dots in field names
        self.assertTrue(self.data_protector._matches_field(
            "metadata.user.ip.address",
            "metadata.user.ip.address"
        ))
        self.assertTrue(self.data_protector._matches_field(
            "data.field.with.dots",
            "data.field.with.dots"
        ))
        
        # Test multiple wildcards
        self.assertTrue(self.data_protector._matches_field(
            "data[0].items[1].details[2].value",
            "data[*].items[*].details[*].*"
        ))
        
        # Test partial wildcards
        self.assertFalse(self.data_protector._matches_field(
            "user.contacts[0].details.email",
            "user.*.email"  # This won't match because there are more levels
        ))
        
        # Test invalid patterns (should not raise exceptions)
        self.assertFalse(self.data_protector._matches_field(
            "user.email",
            "user.[invalid].*"
        ))
        self.assertFalse(self.data_protector._matches_field(
            "user.email",
            "user.(invalid).*"
        ))

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
        hashed_email = transformed_doc["customer"]["email"]
        self.assertTrue(hashed_email.startswith("hash:"))
        self.assertNotEqual(hashed_email, doc["customer"]["email"])
        
        # Verify transformation is deterministic
        transformed_doc2 = transform_func(doc)
        self.assertEqual(
            transformed_doc["customer"]["email"],
            transformed_doc2["customer"]["email"]
        )

    def test_metadata_collection(self):
        """Test collection and propagation of protection metadata."""
        data_protector = DataProtector()
        data_protector.add_protection_rule(FieldProtectionConfig(
            field_matcher="user.*.email",
            protection_level=ProtectionLevel.MASK
        ))
        data_protector.add_protection_rule(FieldProtectionConfig(
            field_matcher="user.payment_info.*.card_number",
            protection_level=ProtectionLevel.ENCRYPT
        ))

        doc = {
            "user": {
                "personal": {"email": "test@example.com"},
                "work": {"email": "work@company.com"},
                "payment_info": {
                    "primary": {"card_number": "4111111111111111"},
                    "backup": {"card_number": "5555555555554444"}
                }
            }
        }

        protected_doc = data_protector.apply_field_protection(doc)

        # Verify metadata structure
        metadata = protected_doc.get("_protection_metadata", {})
        self.assertIn("fields", metadata)
        
        # Check email protection metadata
        email_paths = ["user.personal.email", "user.work.email"]
        for path in email_paths:
            self.assertIn(path, metadata["fields"])
            self.assertEqual(metadata["fields"][path]["type"], "mask")

        # Check card number protection metadata
        card_paths = ["user.payment_info.primary.card_number", 
                     "user.payment_info.backup.card_number"]
        for path in card_paths:
            self.assertIn(path, metadata["fields"])
            self.assertEqual(metadata["fields"][path]["type"], "encrypt")

    def test_complex_nested_arrays(self):
        """Test protection of deeply nested arrays with mixed content."""
        data_protector = DataProtector()
        data_protector.add_protection_rule(FieldProtectionConfig(
            field_matcher="orders[*].items[*].details.price",
            protection_level=ProtectionLevel.MASK
        ))
        data_protector.add_protection_rule(FieldProtectionConfig(
            field_matcher="orders[*].customer.contact[*].email",
            protection_level=ProtectionLevel.HASH
        ))

        doc = {
            "orders": [
                {
                    "items": [
                        {"details": {"price": 99.99, "name": "Item 1"}},
                        {"details": {"price": 49.99, "name": "Item 2"}}
                    ],
                    "customer": {
                        "contact": [
                            {"email": "customer1@example.com"},
                            {"email": "customer2@example.com"}
                        ]
                    }
                },
                {
                    "items": [
                        {"details": {"price": 199.99, "name": "Item 3"}}
                    ],
                    "customer": {
                        "contact": [
                            {"email": "customer3@example.com"}
                        ]
                    }
                }
            ]
        }

        protected_doc = data_protector.apply_field_protection(doc)

        # Verify prices are masked
        self.assertNotEqual(
            protected_doc["orders"][0]["items"][0]["details"]["price"],
            99.99
        )
        self.assertNotEqual(
            protected_doc["orders"][0]["items"][1]["details"]["price"],
            49.99
        )
        self.assertNotEqual(
            protected_doc["orders"][1]["items"][0]["details"]["price"],
            199.99
        )

        # Verify emails are hashed
        for order in protected_doc["orders"]:
            for contact in order["customer"]["contact"]:
                self.assertTrue(contact["email"].startswith("hash:"))

        # Verify non-protected fields remain unchanged
        self.assertEqual(
            protected_doc["orders"][0]["items"][0]["details"]["name"],
            "Item 1"
        )
        self.assertEqual(
            protected_doc["orders"][1]["items"][0]["details"]["name"],
            "Item 3"
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
            field_matcher="user.payment_methods[*].card_number",
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
        masked_email = protected_doc["user"]["email"]
        self.assertTrue(masked_email.startswith("te"))
        self.assertTrue(all(c == "*" for c in masked_email[2:]))
        
        # Verify card numbers in array were masked
        self.assertEqual(
            protected_doc["user"]["payment_methods"][0]["card_number"],
            "XXXXXXXXXXXX1111"
        )
        self.assertEqual(
            protected_doc["user"]["payment_methods"][1]["card_number"],
            "XXXXXXXXXXXX4444"
        )


if __name__ == "__main__":
    unittest.main() 