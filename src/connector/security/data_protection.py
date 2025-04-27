"""Data protection module for field-level encryption and sensitive data handling."""

import base64
import hashlib
import hmac
import os
import re
from typing import Any, Callable, Dict, List, Optional, Pattern, Set, Tuple, Union
from dataclasses import dataclass
from enum import Enum
from functools import lru_cache
from datetime import datetime

from cryptography.fernet import Fernet
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC

from ..logging.logging_config import get_logger
from ..core.document_transformer import TransformFunc

# Type definitions
FieldMatcher = Union[str, Pattern, Callable[[str, Any], bool]]
EncryptedValue = str  # Base64-encoded encrypted data

logger = get_logger(__name__)

class ProtectionLevel(Enum):
    """Protection levels for sensitive data."""
    NONE = "none"             # No protection
    REMOVE = "remove"         # Remove field completely
    HASH = "hash"             # One-way hash
    MASK = "mask"             # Partial masking
    TRUNCATE = "truncate"     # Truncate value
    GENERALIZE = "generalize" # Reduce specificity
    ENCRYPT = "encrypt"       # Reversible encryption
    TOKENIZE = "tokenize"     # Replace with token

@dataclass
class FieldProtectionConfig:
    """Configuration for a field protection rule."""
    field_matcher: FieldMatcher
    protection_level: ProtectionLevel
    options: Dict[str, Any] = None
    description: Optional[str] = None
    
    def __post_init__(self):
        """Initialize field protection config."""
        self.options = self.options or {}
        
        # Validate configuration based on protection level
        if self.protection_level == ProtectionLevel.MASK:
            if "mask_char" not in self.options:
                self.options["mask_char"] = "X"
            if "visible_left" not in self.options and "visible_right" not in self.options:
                self.options["visible_right"] = 4  # Default: show last 4 chars
                
        elif self.protection_level == ProtectionLevel.TRUNCATE:
            if "max_length" not in self.options:
                self.options["max_length"] = 10  # Default max length
                
        elif self.protection_level == ProtectionLevel.GENERALIZE:
            if "generalization_rules" not in self.options:
                raise ValueError("Must specify generalization_rules for GENERALIZE protection")
                
        elif self.protection_level == ProtectionLevel.ENCRYPT:
            # Default to encryption without context
            if "use_context" not in self.options:
                self.options["use_context"] = False
                
        elif self.protection_level == ProtectionLevel.HASH:
            if "algorithm" not in self.options:
                self.options["algorithm"] = "sha256"  # Default hash algorithm
            if "include_salt" not in self.options:
                self.options["include_salt"] = True


class FieldProtectionError(Exception):
    """Exception raised when field protection operations fail."""
    pass


class DataProtector:
    """Handles sensitive data protection with field-level encryption and data masking."""

    def __init__(
        self,
        encryption_key: Optional[str] = None,
        salt: Optional[bytes] = None,
        cache_size: int = 1000
    ):
        """Initialize the data protector.
        
        Args:
            encryption_key: Base64-encoded 32-byte key for Fernet encryption, 
                           or a string to derive a key from.
                           If None, will look for ENCRYPTION_KEY env var.
            salt: Salt bytes for key derivation.
                 If None, will use a default salt or look for ENCRYPTION_SALT env var.
            cache_size: Size of LRU cache for encryption operations.
        """
        self.logger = get_logger(__name__)
        self._cache_size = cache_size
        
        # Set up encryption
        self._initialize_encryption(encryption_key, salt)
        
        # Initialize protection rules
        self.protection_rules: List[FieldProtectionConfig] = []
        
    def _initialize_encryption(
        self, 
        encryption_key: Optional[str], 
        salt: Optional[bytes]
    ) -> None:
        """Initialize the encryption engine.
        
        Args:
            encryption_key: Encryption key or string to derive key from
            salt: Salt for key derivation
            
        Raises:
            ValueError: If unable to set up encryption
        """
        # Try to get key from parameter, then environment
        key_material = encryption_key or os.environ.get("ENCRYPTION_KEY")
        if not key_material:
            self.logger.warning(
                "No encryption key provided. Encryption operations will fail."
            )
            self._fernet = None
            return
            
        # Check if key is already a valid Fernet key (base64-encoded 32 bytes)
        try:
            self._fernet = Fernet(key_material.encode())
            # Test the key
            test_data = b"test"
            assert self._fernet.decrypt(self._fernet.encrypt(test_data)) == test_data
            self.logger.info("Using provided Fernet key for encryption")
            return
        except Exception:
            # Not a valid Fernet key, need to derive one
            pass
            
        # Get or create salt
        if salt:
            salt_bytes = salt
        else:
            salt_env = os.environ.get("ENCRYPTION_SALT")
            if salt_env:
                try:
                    salt_bytes = base64.b64decode(salt_env)
                except Exception:
                    salt_bytes = salt_env.encode()
            else:
                # Use a constant salt - not ideal but better than failing
                salt_bytes = b'mongodb-connector-salt'
                
        # Derive key using PBKDF2
        try:
            kdf = PBKDF2HMAC(
                algorithm=hashes.SHA256(),
                length=32,  # 32 bytes = 256 bits
                salt=salt_bytes,
                iterations=100000
            )
            derived_key = base64.urlsafe_b64encode(kdf.derive(key_material.encode()))
            self._fernet = Fernet(derived_key)
            
            # Test the derived key
            test_data = b"test"
            assert self._fernet.decrypt(self._fernet.encrypt(test_data)) == test_data
            self.logger.info("Successfully derived encryption key")
        except Exception as e:
            self.logger.error(f"Failed to initialize encryption: {str(e)}")
            self._fernet = None
            raise ValueError(f"Failed to initialize encryption: {str(e)}") from e

    def add_protection_rule(self, rule: FieldProtectionConfig) -> None:
        """Add a field protection rule.
        
        Args:
            rule: Field protection configuration
        """
        self.protection_rules.append(rule)
        self.logger.info(
            f"Added {rule.protection_level.value} protection rule "
            f"for {rule.field_matcher if isinstance(rule.field_matcher, str) else 'pattern'}"
        )

    def clear_protection_rules(self) -> None:
        """Remove all protection rules."""
        self.protection_rules.clear()
        self.logger.info("Cleared all protection rules")

    def _matches_field(
        self, 
        field_path: str, 
        value: Any, 
        matcher: FieldMatcher
    ) -> bool:
        """Check if a field matches a field matcher.
        
        Args:
            field_path: Dot notation path of the field
            value: Field value
            matcher: Field matcher (string, regex, or callable)
            
        Returns:
            True if field matches, False otherwise
        """
        if isinstance(matcher, str):
            # Direct string comparison with dot notation
            return field_path == matcher
        elif hasattr(matcher, 'match') and callable(matcher.match):
            # Regular expression
            return bool(matcher.match(field_path))
        elif callable(matcher):
            # Custom matcher function
            return matcher(field_path, value)
        return False

    def _apply_protection(
        self, 
        value: Any, 
        rule: FieldProtectionConfig,
        field_path: str,
        doc_context: Optional[Dict[str, Any]] = None
    ) -> Optional[Any]:
        """Apply protection to a value based on the protection rule.
        
        Args:
            value: The value to protect
            rule: Protection rule configuration
            field_path: Path of the field being protected
            doc_context: Optional full document for context-based protection
            
        Returns:
            Protected value or None if field should be removed
            
        Raises:
            FieldProtectionError: If protection operation fails
        """
        # Skip None values
        if value is None:
            return None
            
        # Skip protection for values that can't be properly handled
        if not isinstance(value, (str, int, float, bool, bytes)):
            self.logger.warning(
                f"Skipping protection for {field_path}: "
                f"Unsupported type {type(value).__name__}"
            )
            return value
            
        # Convert to string for processing
        str_value = str(value) if not isinstance(value, bytes) else value.decode('utf-8')
        
        try:
            if rule.protection_level == ProtectionLevel.NONE:
                return value
                
            elif rule.protection_level == ProtectionLevel.REMOVE:
                return None  # Will be removed by caller
                
            elif rule.protection_level == ProtectionLevel.HASH:
                return self._hash_value(str_value, rule.options, field_path)
                
            elif rule.protection_level == ProtectionLevel.MASK:
                return self._mask_value(str_value, rule.options)
                
            elif rule.protection_level == ProtectionLevel.TRUNCATE:
                return self._truncate_value(str_value, rule.options)
                
            elif rule.protection_level == ProtectionLevel.GENERALIZE:
                return self._generalize_value(str_value, rule.options)
                
            elif rule.protection_level == ProtectionLevel.ENCRYPT:
                return self._encrypt_value(
                    str_value, 
                    rule.options, 
                    doc_context if rule.options.get("use_context") else None
                )
                
            elif rule.protection_level == ProtectionLevel.TOKENIZE:
                return self._tokenize_value(str_value, rule.options, field_path)
            
            else:
                self.logger.warning(
                    f"Unknown protection level {rule.protection_level} for {field_path}"
                )
                return value
                
        except Exception as e:
            raise FieldProtectionError(
                f"Failed to apply {rule.protection_level.value} "
                f"protection to {field_path}: {str(e)}"
            ) from e

    @lru_cache(maxsize=1000)
    def _hash_value(
        self, 
        value: str, 
        options: Dict[str, Any],
        field_path: str
    ) -> str:
        """Create a cryptographic hash of a value.
        
        Args:
            value: Value to hash
            options: Hashing options: {algorithm, include_salt}
            field_path: Field path for salting
            
        Returns:
            Hashed value as a hex string
        """
        algorithm = options.get("algorithm", "sha256").lower()
        include_salt = options.get("include_salt", True)
        
        if algorithm == "sha256":
            hasher = hashlib.sha256()
        elif algorithm == "sha512":
            hasher = hashlib.sha512()
        elif algorithm == "md5":
            hasher = hashlib.md5()  # Not recommended for security
        else:
            raise ValueError(f"Unsupported hash algorithm: {algorithm}")
            
        # Add salt based on field path if requested
        if include_salt:
            salt = hmac.new(
                b"field_salt", 
                field_path.encode(), 
                digestmod=hashlib.sha256
            ).hexdigest()
            hasher.update(salt.encode())
            
        hasher.update(value.encode())
        return hasher.hexdigest()

    def _mask_value(self, value: str, options: Dict[str, Any]) -> str:
        """Mask portions of a value, showing only specific parts.
        
        Args:
            value: Value to mask
            options: Masking options: {mask_char, visible_left, visible_right}
            
        Returns:
            Masked value
        """
        mask_char = options.get("mask_char", "X")
        visible_left = options.get("visible_left", 0)
        visible_right = options.get("visible_right", 0)
        
        if len(value) <= (visible_left + visible_right):
            return value  # Too short to mask effectively
            
        masked_part = mask_char * (len(value) - visible_left - visible_right)
        return value[:visible_left] + masked_part + value[-visible_right:] if visible_right else value[:visible_left] + masked_part

    def _truncate_value(self, value: str, options: Dict[str, Any]) -> str:
        """Truncate a value to a maximum length.
        
        Args:
            value: Value to truncate
            options: Truncation options: {max_length, add_ellipsis}
            
        Returns:
            Truncated value
        """
        max_length = options.get("max_length", 10)
        add_ellipsis = options.get("add_ellipsis", True)
        
        if len(value) <= max_length:
            return value
            
        if add_ellipsis and max_length > 3:
            return value[:max_length-3] + "..."
        else:
            return value[:max_length]

    def _generalize_value(self, value: str, options: Dict[str, Any]) -> str:
        """Generalize a value using specified rules.
        
        Args:
            value: Value to generalize
            options: Generalization options: {generalization_rules}
            
        Returns:
            Generalized value
            
        The generalization_rules should be a list of (pattern, replacement) tuples.
        """
        rules = options.get("generalization_rules", [])
        result = value
        
        for pattern, replacement in rules:
            if isinstance(pattern, str):
                # Direct string replacement
                result = result.replace(pattern, replacement)
            else:
                # Regex replacement
                result = pattern.sub(replacement, result)
                
        return result

    def _encrypt_value(
        self, 
        value: str, 
        options: Dict[str, Any],
        context: Optional[Dict[str, Any]] = None
    ) -> str:
        """Encrypt a value using Fernet symmetric encryption.
        
        Args:
            value: Value to encrypt
            options: Encryption options: {use_context}
            context: Optional document context for associated data
            
        Returns:
            Base64-encoded encrypted value with 'enc:' prefix
            
        Raises:
            FieldProtectionError: If encryption is not initialized or fails
        """
        if not self._fernet:
            raise FieldProtectionError(
                "Encryption not initialized. Provide a valid encryption key."
            )
        
        try:
            # Add context metadata if requested
            if context and options.get("use_context"):
                # We're not implementing true authenticated encryption with
                # associated data here, but we can include context in the payload
                context_str = context.get("_id", "unknown_context")
                value_with_context = f"{context_str}:{value}"
                encrypted = self._fernet.encrypt(value_with_context.encode())
            else:
                encrypted = self._fernet.encrypt(value.encode())
                
            # Return base64 with 'enc:' prefix to identify encrypted values
            return f"enc:{base64.b64encode(encrypted).decode()}"
        except Exception as e:
            raise FieldProtectionError(f"Encryption failed: {str(e)}") from e

    @lru_cache(maxsize=1000)
    def _tokenize_value(
        self, 
        value: str, 
        options: Dict[str, Any],
        field_path: str
    ) -> str:
        """Replace a value with a consistent token based on a hash.
        
        Args:
            value: Value to tokenize
            options: Tokenization options: {prefix, token_length}
            field_path: Field path for context
            
        Returns:
            Tokenized value
        """
        prefix = options.get("prefix", "TOK")
        token_length = options.get("token_length", 10)
        
        # Generate deterministic hash based on value and field path
        hash_options = {"algorithm": "sha256", "include_salt": True}
        hash_value = self._hash_value(value, hash_options, field_path)
        
        # Use a portion of the hash as the token
        token = hash_value[:token_length]
        return f"{prefix}_{token}"

    def apply_field_protection(
        self, 
        document: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Apply all protection rules to a document.
        
        Args:
            document: Document to protect
            
        Returns:
            Protected document with sensitive data handled according to rules
            
        Raises:
            FieldProtectionError: If protection operations fail
        """
        if not self.protection_rules:
            return document.copy()
            
        result = document.copy()
        protected_fields = []
        
        def process_value(
            obj: Dict[str, Any], 
            parent_path: str = "", 
            doc_context: Dict[str, Any] = None
        ) -> None:
            """Recursively process a dictionary applying protection rules.
            
            Args:
                obj: Dictionary to process
                parent_path: Dot notation path to this object
                doc_context: Full document for context
            """
            # Only process dictionaries
            if not isinstance(obj, dict):
                return

            # Fields to process in a second pass (after all other processing)
            # This is needed for fields that might be removed
            deferred_keys = []
                
            # First pass: process all fields
            for key, value in list(obj.items()):
                current_path = f"{parent_path}.{key}" if parent_path else key
                
                # Handle nested dictionaries
                if isinstance(value, dict):
                    process_value(value, current_path, doc_context)
                    continue
                    
                # Handle nested lists (process each item)
                if isinstance(value, list):
                    for i, item in enumerate(value):
                        if isinstance(item, dict):
                            process_value(item, f"{current_path}[{i}]", doc_context)
                    continue
                
                # Check all protection rules
                for rule in self.protection_rules:
                    if self._matches_field(current_path, value, rule.field_matcher):
                        try:
                            # Apply the protection rule
                            protected_value = self._apply_protection(
                                value, rule, current_path, doc_context
                            )
                            
                            # Handle removal (None result)
                            if protected_value is None:
                                deferred_keys.append(key)
                            else:
                                obj[key] = protected_value
                                
                            # Record that we protected this field
                            protected_fields.append({
                                "field": current_path,
                                "protection": rule.protection_level.value,
                                "description": rule.description
                            })
                            
                            # Only apply the first matching rule
                            break
                        except Exception as e:
                            self.logger.error(
                                f"Failed to apply protection to {current_path}: {str(e)}"
                            )
                            # Continue with other fields
            
            # Second pass: Remove fields marked for removal
            for key in deferred_keys:
                if key in obj:
                    del obj[key]
        
        # Process the document
        process_value(result, "", result)
        
        # Add metadata about protection
        if protected_fields:
            if "_security_metadata" not in result:
                result["_security_metadata"] = {}
                
            result["_security_metadata"]["protected_fields"] = protected_fields
            result["_security_metadata"]["protected_at"] = datetime.utcnow().isoformat()
        
        return result

    def create_transform_func(self) -> TransformFunc:
        """Create a transformation function for use with DocumentTransformer.
        
        Returns:
            A function suitable for use with register_transform
        """
        def transform_func(document: Dict[str, Any]) -> Dict[str, Any]:
            return self.apply_field_protection(document)
            
        transform_func.__name__ = "data_protection_transform"
        return transform_func


# Predefined protection rules for common data types
def create_pii_protection_rules() -> List[FieldProtectionConfig]:
    """Create a standard set of PII protection rules.
    
    Returns:
        List of FieldProtectionConfig objects for common PII fields
    """
    # Regular expressions for field matching
    email_pattern = re.compile(r'.*email.*', re.IGNORECASE)
    phone_pattern = re.compile(r'.*phone.*', re.IGNORECASE)
    address_pattern = re.compile(r'.*address.*', re.IGNORECASE)
    card_pattern = re.compile(r'.*card.*number.*', re.IGNORECASE)
    cvv_pattern = re.compile(r'.*(cvv|cvc|security_code).*', re.IGNORECASE)
    ssn_pattern = re.compile(r'.*(ssn|social_security).*', re.IGNORECASE)
    
    return [
        # Email addresses - hash with first two chars visible
        FieldProtectionConfig(
            field_matcher=email_pattern,
            protection_level=ProtectionLevel.MASK,
            options={"visible_left": 2, "visible_right": 0, "mask_char": "*"},
            description="Email addresses masked, first 2 chars visible"
        ),
        
        # Phone numbers - last 4 digits visible
        FieldProtectionConfig(
            field_matcher=phone_pattern,
            protection_level=ProtectionLevel.MASK,
            options={"visible_right": 4, "mask_char": "*"},
            description="Phone numbers masked, last 4 digits visible"
        ),
        
        # Addresses - generalize to remove specific details
        FieldProtectionConfig(
            field_matcher=address_pattern,
            protection_level=ProtectionLevel.GENERALIZE,
            options={
                "generalization_rules": [
                    # Remove street numbers
                    (re.compile(r'\b\d+\b'), "[NUMBER]"),
                    # Remove zip/postal codes
                    (re.compile(r'\b\d{5}(-\d{4})?\b'), "[POSTAL]"),
                    # Remove apartment/unit numbers
                    (re.compile(r'\b(apt|unit|suite|#)\s*[-#]?\s*\w+\b', re.IGNORECASE), "[UNIT]")
                ]
            },
            description="Addresses generalized, specific identifiers removed"
        ),
        
        # Credit card numbers - show only last 4 digits
        FieldProtectionConfig(
            field_matcher=card_pattern,
            protection_level=ProtectionLevel.MASK,
            options={"visible_right": 4, "mask_char": "X"},
            description="Card numbers masked, only last 4 digits visible"
        ),
        
        # CVV/security codes - completely remove
        FieldProtectionConfig(
            field_matcher=cvv_pattern,
            protection_level=ProtectionLevel.REMOVE,
            description="Security codes/CVV completely removed"
        ),
        
        # SSN - completely remove
        FieldProtectionConfig(
            field_matcher=ssn_pattern,
            protection_level=ProtectionLevel.REMOVE,
            description="Social Security Numbers completely removed"
        )
    ]


def create_payment_protection_rules() -> List[FieldProtectionConfig]:
    """Create protection rules for payment card data (PCI DSS).
    
    Returns:
        List of FieldProtectionConfig objects for payment card data
    """
    return [
        # Credit card numbers - show only last 4 digits
        FieldProtectionConfig(
            field_matcher="payment.card_number",
            protection_level=ProtectionLevel.MASK,
            options={"visible_right": 4, "mask_char": "X"},
            description="PCI DSS: Card numbers masked to last 4 digits"
        ),
        
        # CVV - always remove
        FieldProtectionConfig(
            field_matcher="payment.cvv",
            protection_level=ProtectionLevel.REMOVE,
            description="PCI DSS: CVV values must be removed"
        ),
        
        # Expiration dates - tokenize
        FieldProtectionConfig(
            field_matcher=re.compile(r'payment\.(expiry|expiration).*'),
            protection_level=ProtectionLevel.TOKENIZE,
            options={"prefix": "EXP", "token_length": 8},
            description="PCI DSS: Expiration dates tokenized"
        ),
        
        # Cardholder name - mask except first letter of each name
        FieldProtectionConfig(
            field_matcher="payment.cardholder_name",
            protection_level=ProtectionLevel.GENERALIZE,
            options={
                "generalization_rules": [
                    # Keep first letter of each name part, mask the rest
                    (re.compile(r'\b(\w)\w+\b'), r'\1***')
                ]
            },
            description="PCI DSS: Cardholder names generalized to first letter"
        )
    ] 