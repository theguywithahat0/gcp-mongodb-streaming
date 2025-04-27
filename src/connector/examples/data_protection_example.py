"""Example demonstrating the usage of the data protection module."""

import json
import os
import sys

# Add the parent directory to the path to import the connector package
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

from connector.core.data_protection import (
    DataProtector,
    FieldProtectionConfig,
    ProtectionLevel,
    create_pii_protection_rules,
    create_payment_protection_rules
)

# Create a sample document with sensitive data
sample_doc = {
    "customer": {
        "id": "cust-12345",
        "name": "John Doe",
        "email": "john.doe@example.com",
        "phone": "555-123-4567",
        "address": {
            "street": "123 Main St",
            "city": "Springfield",
            "state": "IL",
            "zip": "62701"
        },
        "ssn": "123-45-6789",
        "date_of_birth": "1980-01-01"
    },
    "payment": {
        "card_number": "4111111111111111",
        "expiry_date": "12/25",
        "cvv": "123",
        "card_holder": "John Doe"
    },
    "transaction": {
        "id": "txn-67890",
        "amount": 99.99,
        "date": "2023-09-15",
        "items": [
            {"name": "Product A", "quantity": 2, "price": 29.99},
            {"name": "Product B", "quantity": 1, "price": 40.01}
        ]
    }
}

def main():
    """Demonstrate various data protection methods."""
    # Set up encryption key for the example (in production, use a secure key management system)
    from cryptography.fernet import Fernet
    key = Fernet.generate_key()
    os.environ["ENCRYPTION_KEY"] = key.decode()
    
    print("Original document:")
    print(json.dumps(sample_doc, indent=2))
    print("\n" + "-" * 80 + "\n")

    # Example 1: Using predefined protection rules for PII data
    print("Example 1: Applying predefined PII protection rules")
    protector = DataProtector()
    for rule in create_pii_protection_rules():
        protector.add_protection_rule(rule)
    
    protected_doc = protector.apply_field_protection(sample_doc.copy())
    print(json.dumps(protected_doc, indent=2))
    print("\n" + "-" * 80 + "\n")

    # Example 2: Using predefined protection rules for payment data
    print("Example 2: Applying predefined payment protection rules")
    protector = DataProtector()
    for rule in create_payment_protection_rules():
        protector.add_protection_rule(rule)
    
    protected_doc = protector.apply_field_protection(sample_doc.copy())
    print(json.dumps(protected_doc, indent=2))
    print("\n" + "-" * 80 + "\n")

    # Example 3: Custom protection rules
    print("Example 3: Applying custom protection rules")
    protector = DataProtector()
    
    # Add custom rules
    protector.add_protection_rule(
        FieldProtectionConfig(
            field_matcher="customer.email",
            protection_level=ProtectionLevel.ENCRYPT,
            options={"use_context": True}
        )
    )
    
    protector.add_protection_rule(
        FieldProtectionConfig(
            field_matcher="customer.phone",
            protection_level=ProtectionLevel.MASK,
            options={"visible_right": 4, "mask_char": "X"}
        )
    )
    
    protector.add_protection_rule(
        FieldProtectionConfig(
            field_matcher="payment.card_number",
            protection_level=ProtectionLevel.MASK,
            options={"visible_right": 4, "mask_char": "X"}
        )
    )
    
    protector.add_protection_rule(
        FieldProtectionConfig(
            field_matcher="payment.cvv",
            protection_level=ProtectionLevel.REMOVE
        )
    )
    
    protector.add_protection_rule(
        FieldProtectionConfig(
            field_matcher="customer.ssn",
            protection_level=ProtectionLevel.HASH,
            options={"algorithm": "sha256", "include_salt": True}
        )
    )
    
    protected_doc = protector.apply_field_protection(sample_doc.copy())
    print(json.dumps(protected_doc, indent=2))
    print("\n" + "-" * 80 + "\n")

    # Example 4: Using pattern matching for field selection
    print("Example 4: Using pattern matching for field selection")
    import re
    
    protector = DataProtector()
    protector.add_protection_rule(
        FieldProtectionConfig(
            field_matcher=re.compile(r".*\.card_.*"),
            protection_level=ProtectionLevel.MASK,
            options={"visible_right": 4, "mask_char": "X"}
        )
    )
    
    protector.add_protection_rule(
        FieldProtectionConfig(
            field_matcher=re.compile(r".*\.cvv"),
            protection_level=ProtectionLevel.REMOVE
        )
    )
    
    protected_doc = protector.apply_field_protection(sample_doc.copy())
    print(json.dumps(protected_doc, indent=2))


if __name__ == "__main__":
    main() 