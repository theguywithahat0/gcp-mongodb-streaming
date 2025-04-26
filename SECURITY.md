# Security Documentation for MongoDB Change Stream Connector

This document outlines security considerations and best practices for the MongoDB Change Stream Connector, particularly regarding sensitive data handling, field prefixing, and isolation strategies.

## 1. Sensitive Data Handling

The connector provides mechanisms to handle sensitive data through the document transformer pipeline.

### 1.1 Sensitive Fields

The following fields are considered sensitive and should be handled with care:

| Field | Description | Handling Recommendation |
|-------|-------------|-------------------------|
| `customer_email` | Customer email address | Remove or hash |
| `customer_phone` | Customer phone number | Remove or mask partially |
| `customer.address` | Customer physical address | Remove or generalize to region/city level |
| `payment.card_number` | Credit card number | Remove or mask (e.g., "XXXX-XXXX-XXXX-1234") |
| `payment.cvv` | Card verification value | Always remove completely |
| `shipping.address` | Shipping address details | Remove or generalize to region/city level |
| `personal_identifiers` | SSN, ID numbers, etc. | Always remove completely |

### 1.2 Data Transformation Process

The connector uses the `DocumentTransformer` class to apply security transformations before documents are published to Pub/Sub:

```python
# Example from ChangeStreamListener initialization
self.transformer.register_transform(
    TransformStage.PRE_PUBLISH,
    DocumentType.TRANSACTION,
    remove_sensitive_fields([
        "customer_email",
        "customer_phone",
        "customer.address",
        "payment.card_number",
        "payment.cvv",
        "shipping.address"
    ])
)
```

### 1.3 Implementation Examples

#### Removing Sensitive Fields

```python
from connector.core.document_transformer import remove_sensitive_fields, TransformStage, DocumentType

# Register a transform to remove sensitive fields
transformer.register_transform(
    TransformStage.PRE_PUBLISH,
    DocumentType.TRANSACTION,
    remove_sensitive_fields([
        "customer_email",
        "payment.card_number",
        "payment.cvv"
    ])
)
```

#### Field Masking (Custom Implementation)

```python
def mask_card_number(doc: Dict[str, Any]) -> Dict[str, Any]:
    """Mask credit card numbers, showing only last 4 digits."""
    result = doc.copy()
    
    if "payment" in result and "card_number" in result["payment"]:
        card_num = result["payment"]["card_number"]
        if isinstance(card_num, str) and len(card_num) >= 4:
            masked = "X" * (len(card_num) - 4) + card_num[-4:]
            result["payment"]["card_number"] = masked
            
    return result

# Register the custom masking transformer
transformer.register_transform(
    TransformStage.PRE_PUBLISH,
    DocumentType.TRANSACTION,
    mask_card_number
)
```

## 2. Data Organization

### 2.1 Warehouse Prefixing

The connector uses warehouse prefixing to maintain data isolation between different environments and data sources.

#### Prefixing Convention

The standard format is: `wh_{warehouse_id}_{field_name}`

Example:
- Original field: `product_id`
- Prefixed field: `wh_123_product_id`

#### Implementation

```python
from connector.core.document_transformer import add_field_prefix, TransformStage, DocumentType

# Add warehouse prefix for data isolation
transformer.register_transform(
    TransformStage.PRE_PUBLISH,
    DocumentType.INVENTORY,
    add_field_prefix(f"wh_{config.mongodb.warehouse_id}_")
)
```

### 2.2 Protected Fields

The following fields are automatically protected from prefixing:

```
_id
_processing_metadata
_security_metadata
_schema_version
document_type
```

### 2.3 Selective Field Prefixing

For transaction documents, only specific fields are prefixed:

```python
# Add warehouse prefix only to specific fields
transformer.register_transform(
    TransformStage.PRE_PUBLISH,
    DocumentType.TRANSACTION,
    add_field_prefix(f"wh_{config.mongodb.warehouse_id}_", [
        "product_id",
        "warehouse_id",
        "location",
        "shelf"
    ])
)
```

## 3. Environment Isolation Strategies

### 3.1 Multi-Environment Isolation

When operating the connector in multiple environments (development, staging, production), use these isolation strategies:

1. **Separate Warehouse IDs**: Assign unique warehouse IDs for each environment
   - Development: `wh_dev_`
   - Staging: `wh_stg_`
   - Production: `wh_prod_`

2. **Separate Pub/Sub Topics**: Use environment-specific topics
   - Example: `dev-inventory-updates`, `stg-inventory-updates`, `prod-inventory-updates`

3. **Access Control**: Configure proper IAM permissions for each environment

### 3.2 Example Configuration

```yaml
mongodb:
  uri: "mongodb://localhost:27017"
  database: "inventory"
  warehouse_id: "prod_main_warehouse"  # Environment-specific ID
  collections:
    - name: "products"
      topic: "prod-inventory-updates-topic"  # Environment-specific topic
```

## 4. Security Best Practices

### 4.1 Authentication & Authorization

- Use MongoDB connection strings with username/password authentication
- Store credentials in environment variables, not in configuration files
- Use service accounts with minimal required permissions for GCP resources

### 4.2 Network Security

- Use private IP connections to MongoDB when possible
- Configure firewall rules to restrict access to MongoDB
- Use VPC Service Controls to restrict GCP resource access

### 4.3 Data Protection

- Enable MongoDB TLS connections
- Enable Pub/Sub topic encryption
- Consider applying field-level encryption for highly sensitive fields

### 4.4 Logging & Monitoring

- Monitor the connector status topic for operational issues
- Set up alerts for transformation errors
- Periodically audit the sensitive data handling procedures

## 5. Compliance Considerations

When handling personal data, ensure compliance with relevant regulations such as:

- GDPR: Consider data minimization principles when determining which fields to transform
- PCI DSS: Always remove or properly mask payment card data
- HIPAA: Apply appropriate controls for any health-related information

## 6. Testing Security Transformations

Before deploying to production, verify security transformations with:

```python
# Test document with sensitive data
test_doc = {
    "customer_email": "example@example.com",
    "payment": {
        "card_number": "4111-1111-1111-1111",
        "cvv": "123"
    },
    "product_id": "ABC123"
}

# Apply all pre-publish transformations
transformed = transformer.apply_transforms(
    test_doc,
    DocumentType.TRANSACTION,
    TransformStage.PRE_PUBLISH
)

# Verify sensitive fields are removed or transformed
assert "customer_email" not in transformed
assert "payment" not in transformed or "card_number" not in transformed["payment"]
assert "wh_warehouse_id_product_id" in transformed
``` 