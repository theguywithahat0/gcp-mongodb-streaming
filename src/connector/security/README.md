# Security Module

The security module provides comprehensive tools for protecting sensitive data in MongoDB documents as they flow through the connector. This module ensures that sensitive information can be properly masked, hashed, encrypted, or removed before publishing to downstream systems.

## Features

- **Field-level protection** with multiple protection levels:
  - `HASH`: One-way hashing for identifiers that need referential integrity
  - `MASK`: Partial masking (e.g., showing only last 4 digits of credit cards)
  - `REMOVE`: Complete removal of sensitive fields
  - `ENCRYPT`: Reversible encryption for data that needs to be recovered
  - `TRUNCATE`: Shortening values to reduce specificity
  - `GENERALIZE`: Reducing specificity of values
  - `TOKENIZE`: Replacing values with consistent tokens

- **Flexible field selection** using:
  - Exact field paths (e.g., "customer.email")
  - Regular expressions (e.g., all fields containing "card")
  - Custom matcher functions

- **Built-in protection rule sets** for common data types:
  - PII (Personally Identifiable Information)
  - Payment card data (PCI DSS compliance)

- **Document transformation integration** for seamless use in the data pipeline

## Usage

### Basic Usage

```python
from connector.security import (
    DataProtector,
    FieldProtectionConfig,
    ProtectionLevel
)

# Create a data protector
protector = DataProtector(encryption_key="your-encryption-key")

# Add protection rules
protector.add_protection_rule(
    FieldProtectionConfig(
        field_matcher="customer.email",
        protection_level=ProtectionLevel.HASH,
        options={"algorithm": "sha256", "include_salt": True}
    )
)

protector.add_protection_rule(
    FieldProtectionConfig(
        field_matcher="payment.card_number",
        protection_level=ProtectionLevel.MASK,
        options={"visible_right": 4, "mask_char": "X"}
    )
)

# Apply protection to a document
protected_doc = protector.apply_field_protection(original_doc)
```

### Using Predefined Rules

```python
from connector.security import (
    DataProtector,
    create_pii_protection_rules,
    create_payment_protection_rules
)

# Create a data protector with PII protection rules
protector = DataProtector()
for rule in create_pii_protection_rules():
    protector.add_protection_rule(rule)

# Apply protection to a document
protected_doc = protector.apply_field_protection(original_doc)
```

### Pattern Matching for Field Selection

```python
import re
from connector.security import (
    DataProtector,
    FieldProtectionConfig,
    ProtectionLevel
)

# Create a data protector
protector = DataProtector()

# Add rule using regex pattern matching
protector.add_protection_rule(
    FieldProtectionConfig(
        field_matcher=re.compile(r".*\.card_.*"),
        protection_level=ProtectionLevel.MASK,
        options={"visible_right": 4, "mask_char": "X"}
    )
)

# Apply protection to a document
protected_doc = protector.apply_field_protection(original_doc)
```

### Integration with Document Transformer

```python
from connector.security import DataProtector
from connector.core.document_transformer import (
    DocumentTransformer,
    TransformStage,
    DocumentType
)

# Create a data protector with rules
protector = DataProtector()
# ... add rules ...

# Get transform function
transform_func = protector.create_transform_func()

# Create document transformer
transformer = DocumentTransformer()

# Register the data protection transform
transformer.register_transform(
    TransformStage.PRE_PUBLISH,  # Apply before publishing
    DocumentType.ALL,            # Apply to all document types
    transform_func
)
```

## Security Considerations

- **Key Management**: The encryption key should be securely managed outside of code, preferably using a dedicated key management system.
- **Salt Management**: For hashing operations, a consistent salt can be provided for deterministic hashing.
- **PCI DSS Compliance**: When handling payment card data, ensure your implementation follows PCI DSS guidelines, which typically require at minimum masking or removal of sensitive card data.
- **Rule Order**: Rules are applied in the order they are added, with the first matching rule taking precedence.
- **Nested Document Handling**: The protector will recursively process nested objects and arrays to protect sensitive data at any depth.

## Examples

See [`connector/examples/data_protection_example.py`](../examples/data_protection_example.py) for comprehensive examples of different data protection scenarios. 