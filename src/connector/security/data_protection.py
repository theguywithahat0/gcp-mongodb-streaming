"""Module for field-level encryption and sensitive data handling."""

import base64
import hashlib
import os
import re
import json
import copy
import time
from typing import (
    Any, Callable, Dict, List, Optional, Pattern, Union
)
from dataclasses import dataclass
from enum import Enum
from functools import lru_cache
from datetime import datetime, timezone

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
    NONE = "none"              # No protection
    REMOVE = "remove"          # Remove field completely
    HASH = "hash"              # One-way hash
    MASK = "mask"              # Partial masking
    TRUNCATE = "truncate"      # Truncate value
    GENERALIZE = "generalize"  # Reduce specificity
    ENCRYPT = "encrypt"        # Reversible encryption
    TOKENIZE = "tokenize"      # Replace with token


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
            if ("visible_left" not in self.options and
                    "visible_right" not in self.options):
                # Default: show last 4 chars
                self.options["visible_right"] = 4

        elif self.protection_level == ProtectionLevel.TRUNCATE:
            if "max_length" not in self.options:
                self.options["max_length"] = 10  # Default max length

        elif self.protection_level == ProtectionLevel.GENERALIZE:
            if "generalization_rules" not in self.options:
                raise ValueError(
                    "Must specify generalization_rules for "
                    "GENERALIZE protection"
                )

        elif self.protection_level == ProtectionLevel.ENCRYPT:
            # Default to encryption without context
            if "use_context" not in self.options:
                self.options["use_context"] = False

        elif self.protection_level == ProtectionLevel.HASH:
            if "algorithm" not in self.options:
                # Default hash algorithm
                self.options["algorithm"] = "sha256"
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

    def _matches_field(self, field_path: str, pattern: FieldMatcher) -> bool:
        """Check if a field path matches a pattern.

        Args:
            field_path: Field path to check (e.g. "address.street", "orders[0].items[2].name")
            pattern: Pattern to match against (string with wildcards, regex pattern, or callable)

        Returns:
            bool: True if the field path matches the pattern

        Examples:
            >>> _matches_field("user.email", "user.email")  # True
            >>> _matches_field("user.name", "user.*")  # True
            >>> _matches_field("users[0].email", "users[*].email")  # True
            >>> _matches_field("users[0].addresses[1].street", "users[*].addresses[*].street")  # True
        """
        if not field_path or not pattern:
            return False

        try:
            # Handle different pattern types
            if isinstance(pattern, str):
                # Split pattern and field path into segments
                pattern_segments = []
                field_segments = []
                
                # Parse pattern segments
                try:
                    pattern_matches = list(re.finditer(r'([^.\[\]]+)|\[([^.\[\]]+)\]', pattern))
                    if not pattern_matches:
                        self.logger.warning(f"Invalid pattern '{pattern}': No valid segments found")
                        return False
                        
                    for segment in pattern_matches:
                        if segment.group(1):  # Normal field name
                            pattern_segments.append(segment.group(1))
                        else:  # Array index
                            pattern_segments.append(f"[{segment.group(2)}]")
                except re.error as e:
                    self.logger.warning(f"Invalid pattern '{pattern}': {str(e)}")
                    return False
                
                # Parse field path segments
                try:
                    for segment in re.finditer(r'([^.\[\]]+)|\[([^.\[\]]+)\]', field_path):
                        if segment.group(1):  # Normal field name
                            field_segments.append(segment.group(1))
                        else:  # Array index
                            field_segments.append(f"[{segment.group(2)}]")
                except re.error as e:
                    self.logger.warning(f"Invalid field path '{field_path}': {str(e)}")
                    return False
                
                # Check if segments match
                if len(pattern_segments) != len(field_segments):
                    return False
                
                for pattern_seg, field_seg in zip(pattern_segments, field_segments):
                    # Handle array wildcards
                    if pattern_seg == "[*]" and re.match(r'\[\d+\]', field_seg):
                        continue
                    # Handle field wildcards
                    elif pattern_seg == "*" and not field_seg.startswith("["):
                        continue
                    # Handle exact matches
                    elif pattern_seg != field_seg:
                        return False
                
                return True
                
            elif isinstance(pattern, Pattern):
                # Use regex pattern directly
                return bool(pattern.match(field_path))
            elif callable(pattern):
                # Use callable matcher
                return pattern(field_path)
            else:
                self.logger.warning(f"Invalid pattern type: {type(pattern)}")
                return False
            
        except Exception as e:
            self.logger.warning(f"Error matching field: {str(e)}")
            return False

    def _apply_protection(
        self,
        value: Any,
        rule: FieldProtectionConfig,
        field_path: str,
        doc_context: Optional[Dict[str, Any]] = None
    ) -> Optional[Any]:
        """Apply the specified protection rule to a value.

        Args:
            value: Value to protect
            rule: Protection rule to apply
            field_path: Path to the field being protected
            doc_context: Full document for context

        Returns:
            Protected value or None if field should be removed
        """
        try:
            if rule.protection_level == ProtectionLevel.REMOVE:
                return None
            elif rule.protection_level == ProtectionLevel.ENCRYPT:
                return self._encrypt_value(value, rule.options, doc_context)
            elif rule.protection_level == ProtectionLevel.HASH:
                return self._hash_value(value, field_path, rule.options)
            elif rule.protection_level == ProtectionLevel.MASK:
                return self._mask_value(str(value), rule.options)
            elif rule.protection_level == ProtectionLevel.TRUNCATE:
                return self._truncate_value(value, rule.options)
            elif rule.protection_level == ProtectionLevel.GENERALIZE:
                return self._generalize_value(value, rule.options)
            elif rule.protection_level == ProtectionLevel.TOKENIZE:
                return self._tokenize_value(value, rule.options, field_path)
            else:
                return value
        except Exception as e:
            raise FieldProtectionError(f"Failed to apply {rule.protection_level.value} protection to {field_path}: {str(e)}") from e

    def _hash_value(
        self,
        value: Any,
        field_path: str,
        options: Optional[Dict[str, Any]] = None
    ) -> str:
        """Hash a value using the specified algorithm.

        Args:
            value: Value to hash
            field_path: Path to the field being hashed
            options: Hash options including algorithm and salt

        Returns:
            Base64-encoded hash value with 'hash:' prefix
        """
        try:
            # Convert value to string representation
            if isinstance(value, (dict, list)):
                value_str = json.dumps(value, sort_keys=True)
            else:
                value_str = str(value)

            # Get hash algorithm
            options = options or {}
            algorithm = options.get("algorithm", "sha256").lower()
            hash_func = getattr(hashlib, algorithm, None)
            if not hash_func:
                raise ValueError(f"Unsupported hash algorithm: {algorithm}")

            # Create hash with optional salt
            hasher = hash_func()
            if options.get("include_salt", True):
                # Use field path as salt to ensure consistent hashing
                hasher.update(field_path.encode())
            hasher.update(value_str.encode())

            # Return base64-encoded hash with prefix
            hash_bytes = hasher.digest()
            return f"hash:{base64.b64encode(hash_bytes).decode()}"

        except Exception as e:
            raise FieldProtectionError(
                f"Failed to hash value: {str(e)}"
            ) from e

    def _mask_value(self, value: str, options: Dict[str, Any]) -> str:
        """Mask a value, showing only specified portions.

        Args:
            value: Value to mask
            options: Masking options including visible parts and mask character

        Returns:
            Masked value
        """
        if not isinstance(value, str):
            value = str(value)

        mask_char = options.get("mask_char", "*")
        visible_left = options.get("visible_left", 0)
        visible_right = options.get("visible_right", 0)

        if not value:
            return value

        # Calculate number of characters to mask
        value_len = len(value)
        visible_left = min(visible_left, value_len)
        visible_right = min(
            visible_right,
            value_len - visible_left
        )

        # Build masked value
        left_part = value[:visible_left] if visible_left > 0 else ""
        right_part = value[-visible_right:] if visible_right > 0 else ""

        # Calculate mask length based on original string length
        mask_len = len(value) - len(left_part) - len(right_part)

        # Build final masked value
        return left_part + (mask_char * mask_len) + right_part

    def _truncate_value(self, value: Any, options: Dict[str, Any]) -> str:
        """Truncate a value to a maximum length.

        Args:
            value: Value to truncate
            options: Truncation options including max_length and suffix

        Returns:
            Truncated value
        """
        if value is None:
            return None

        str_value = str(value)
        max_length = options.get("max_length", 50)  # Default to 50 chars
        suffix = options.get("suffix", "...")  # Default suffix

        if len(str_value) <= max_length:
            return str_value

        # Reserve space for suffix
        suffix_len = len(suffix)
        truncated_length = max_length - suffix_len
        if truncated_length <= 0:
            return suffix  # Edge case where max_length is too small

        return f"{str_value[:truncated_length]}{suffix}"

    def _generalize_value(self, value: Any, options: Dict[str, Any]) -> Any:
        """Generalize a value by rounding numbers or using categories.

        Args:
            value: Value to generalize
            options: Generalization options including:
                - number_precision: Decimal places for numbers
                - ranges: List of range boundaries for numeric values
                - categories: Mapping of values to categories
                - date_format: Format for dates (year, month, etc.)
                - generalization_rules: List of (pattern, replacement) tuples

        Returns:
            Generalized value
        """
        if value is None:
            return None

        # Handle numeric values
        if isinstance(value, (int, float)):
            # Check if we should use ranges
            ranges = options.get("ranges")
            if ranges:
                for range_start, range_end, label in ranges:
                    if range_start <= value <= range_end:
                        return label
                return "other"  # Default if no range matches

            # Otherwise use precision
            precision = options.get("number_precision", 0)
            if precision == 0:
                return round(value)
            return round(value, precision)

        # Handle string values
        if isinstance(value, str):
            # Check for category mapping
            categories = options.get("categories")
            if categories and value in categories:
                return categories[value]

            # Apply generalization rules if present
            rules = options.get("generalization_rules", [])
            result = value
            for pattern, replacement in rules:
                if isinstance(pattern, str):
                    result = result.replace(pattern, replacement)
                else:  # Assume regex pattern
                    result = pattern.sub(replacement, result)
            return result

        # Handle date values (assuming ISO format string)
        if options.get("date_format"):
            try:
                date_format = options["date_format"]
                date_obj = datetime.fromisoformat(str(value))
                if date_format == "year":
                    return date_obj.strftime("%Y")
                elif date_format == "month":
                    return date_obj.strftime("%Y-%m")
                elif date_format == "quarter":
                    quarter = (date_obj.month - 1) // 3 + 1
                    return f"{date_obj.year}-Q{quarter}"
            except (ValueError, TypeError):
                pass  # Not a valid date string

        # Return original value if no generalization rule matches
        return value

    def _encrypt_value(
        self,
        value: Any,
        options: Dict[str, Any],
        context: Optional[Dict[str, Any]] = None
    ) -> str:
        """Encrypt a value using the configured encryption key.
        
        Args:
            value: The value to encrypt
            options: Optional encryption options including:
                - context: Additional context to include in encryption
            
        Returns:
            The encrypted value with 'enc:' prefix
        """
        if not self._fernet:
            raise FieldProtectionError("Encryption not initialized")
            
        try:
            # Add timestamp to ensure non-deterministic encryption
            timestamp = str(time.time())
            context_str = str(context) if context else ''
            
            # Convert value to string and combine with timestamp and context
            value_str = str(value)
            data = f"{timestamp}:{context_str}:{value_str}".encode('utf-8')
            
            # Encrypt using Fernet
            encrypted = self._fernet.encrypt(data)
            return f"enc:{base64.b64encode(encrypted).decode('utf-8')}"
        except Exception as e:
            raise FieldProtectionError(f"Failed to encrypt value: {str(e)}")

    @lru_cache(maxsize=1000)
    def _tokenize_value(
        self,
        value: Any,
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
        hash_value = self._hash_value(value, field_path, hash_options)

        # Use a portion of the hash as the token
        token = hash_value[:token_length]
        return f"{prefix}_{token}"

    def apply_field_protection(self, document: Dict[str, Any], current_path: str = "") -> Dict[str, Any]:
        """Apply protection rules to a document.
        
        Args:
            document: The document to protect
            current_path: The current field path in dot notation (for nested processing)
            
        Returns:
            The protected document with security metadata
        """
        if not isinstance(document, dict):
            return document

        protected_doc = document.copy()
        metadata = {"protected_fields": [], "fields": {}}

        for key, value in document.items():
            field_path = f"{current_path}.{key}" if current_path else key
            
            if isinstance(value, dict):
                protected_value = self.apply_field_protection(value, field_path)
                if isinstance(protected_value, dict):
                    if "_security_metadata" in protected_value:
                        metadata["protected_fields"].extend(
                            protected_value["_security_metadata"]["protected_fields"]
                        )
                        del protected_value["_security_metadata"]
                    if "_protection_metadata" in protected_value:
                        metadata["fields"].update(
                            protected_value["_protection_metadata"]["fields"]
                        )
                        del protected_value["_protection_metadata"]
                protected_doc[key] = protected_value
                
            elif isinstance(value, list):
                protected_list = []
                for i, item in enumerate(value):
                    array_path = f"{field_path}[{i}]"
                    if isinstance(item, dict):
                        protected_item = self.apply_field_protection(item, array_path)
                        if isinstance(protected_item, dict):
                            if "_security_metadata" in protected_item:
                                metadata["protected_fields"].extend(
                                    protected_item["_security_metadata"]["protected_fields"]
                                )
                                del protected_item["_security_metadata"]
                            if "_protection_metadata" in protected_item:
                                metadata["fields"].update(
                                    protected_item["_protection_metadata"]["fields"]
                                )
                                del protected_item["_protection_metadata"]
                        protected_list.append(protected_item)
                    else:
                        protected_item = self._process_value(item, array_path)
                        if protected_item != item:
                            field_info = {
                                "field": array_path,
                                "protection": self._get_protection_type(array_path)
                            }
                            metadata["protected_fields"].append(field_info)
                            metadata["fields"][array_path] = {
                                "type": field_info["protection"]
                            }
                        protected_list.append(protected_item)
                protected_doc[key] = protected_list
                
            else:
                protected_value = self._process_value(value, field_path)
                if protected_value != value:
                    field_info = {
                        "field": field_path,
                        "protection": self._get_protection_type(field_path)
                    }
                    metadata["protected_fields"].append(field_info)
                    metadata["fields"][field_path] = {
                        "type": field_info["protection"]
                    }
                if protected_value is None and value is not None:
                    del protected_doc[key]
                elif protected_value is not None:
                    protected_doc[key] = protected_value

        if metadata["protected_fields"] or metadata["fields"]:
            protected_doc["_security_metadata"] = {
                "protected_fields": metadata["protected_fields"]
            }
            protected_doc["_protection_metadata"] = {
                "fields": metadata["fields"]
            }

        return protected_doc

    def _process_value(self, value: Any, field_path: str) -> Any:
        """Process a single value according to matching protection rules.
        
        Args:
            value: The value to protect
            field_path: The full field path in dot notation
            
        Returns:
            The protected value
        """
        if value is None:
            return None

        matching_rules = [
            rule for rule in self.protection_rules
            if self._matches_field(field_path, rule.field_matcher)
        ]

        if not matching_rules:
            return value

        # Apply the most restrictive protection level
        rule = max(matching_rules, key=lambda r: r.protection_level.value)
        
        try:
            if rule.protection_level == ProtectionLevel.REMOVE:
                return None
            elif rule.protection_level == ProtectionLevel.ENCRYPT:
                return self._encrypt_value(value, rule.options or {})
            elif rule.protection_level == ProtectionLevel.HASH:
                return self._hash_value(value, field_path, rule.options)
            elif rule.protection_level == ProtectionLevel.MASK:
                return self._mask_value(str(value), rule.options or {})
            elif rule.protection_level == ProtectionLevel.TRUNCATE:
                return self._truncate_value(value, rule.options or {})
            elif rule.protection_level == ProtectionLevel.GENERALIZE:
                return self._generalize_value(value, rule.options or {})
            elif rule.protection_level == ProtectionLevel.TOKENIZE:
                return self._tokenize_value(value, rule.options or {}, field_path)
            else:
                return value
        except Exception as e:
            self.logger.warning(
                f"Failed to apply {rule.protection_level.name} protection to field {field_path}: {str(e)}"
            )
            return value

    def _get_protection_type(self, field_path: str) -> str:
        """Get the protection type applied to a field."""
        matching_rules = [
            rule for rule in self.protection_rules
            if self._matches_field(field_path, rule.field_matcher)
        ]
        if not matching_rules:
            return "none"
        rule = max(matching_rules, key=lambda r: r.protection_level.value)
        return rule.protection_level.name.lower()

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
    email_pattern = re.compile(
        r'.*email.*',
        re.IGNORECASE
    )
    phone_pattern = re.compile(
        r'.*phone.*',
        re.IGNORECASE
    )
    address_pattern = re.compile(
        r'.*address.*',
        re.IGNORECASE
    )
    card_pattern = re.compile(
        r'.*card.*number.*',
        re.IGNORECASE
    )
    cvv_pattern = re.compile(
        r'.*(cvv|cvc|security_code).*',
        re.IGNORECASE
    )
    ssn_pattern = re.compile(
        r'.*(ssn|social_security).*',
        re.IGNORECASE
    )

    return [
        # Email addresses - hash with first two chars visible
        FieldProtectionConfig(
            field_matcher=email_pattern,
            protection_level=ProtectionLevel.MASK,
            options={
                "visible_left": 2,
                "visible_right": 0,
                "mask_char": "*"
            },
            description=(
                "Email addresses masked, first 2 chars visible"
            )
        ),

        # Phone numbers - last 4 digits visible
        FieldProtectionConfig(
            field_matcher=phone_pattern,
            protection_level=ProtectionLevel.MASK,
            options={
                "visible_right": 4,
                "mask_char": "*"
            },
            description=(
                "Phone numbers masked, last 4 digits visible"
            )
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
                    (
                        re.compile(r'\b\d{5}(-\d{4})?\b'),
                        "[POSTAL]"
                    ),
                    # Remove apartment/unit numbers
                    (
                        re.compile(
                            r'\b(apt|unit|suite|#)\s*[-#]?\s*\w+\b',
                            re.IGNORECASE
                        ),
                        "[UNIT]"
                    )
                ]
            },
            description=(
                "Addresses generalized, specific identifiers removed"
            )
        ),

        # Credit card numbers - show only last 4 digits
        FieldProtectionConfig(
            field_matcher="*.card_number",  # Use wildcard pattern for arrays
            protection_level=ProtectionLevel.MASK,
            options={
                "visible_right": 4,
                "mask_char": "X"
            },
            description=(
                "Card numbers masked, last 4 digits visible"
            )
        ),

        # CVV/CVC - remove completely
        FieldProtectionConfig(
            field_matcher=cvv_pattern,
            protection_level=ProtectionLevel.REMOVE,
            description="Security codes removed"
        ),

        # SSN - remove completely
        FieldProtectionConfig(
            field_matcher=ssn_pattern,
            protection_level=ProtectionLevel.REMOVE,
            description="Social security numbers removed"
        )
    ]


def create_payment_protection_rules() -> List[FieldProtectionConfig]:
    """Create a standard set of payment data protection rules.

    Returns:
        List of FieldProtectionConfig objects for payment fields
    """
    # Regular expressions for field matching
    card_pattern = re.compile(r'.*card.*number.*', re.IGNORECASE)
    cvv_pattern = re.compile(r'.*(cvv|cvc|security_code).*', re.IGNORECASE)
    expiry_pattern = re.compile(r'.*(expiry|expiration).*', re.IGNORECASE)
    amount_pattern = re.compile(r'.*(amount|total|price).*', re.IGNORECASE)

    return [
        # Card numbers - show only last 4 digits
        FieldProtectionConfig(
            field_matcher=card_pattern,
            protection_level=ProtectionLevel.MASK,
            options={"visible_right": 4, "mask_char": "X"},
            description="Card numbers masked, last 4 digits visible"
        ),

        # CVV/CVC - remove completely
        FieldProtectionConfig(
            field_matcher=cvv_pattern,
            protection_level=ProtectionLevel.REMOVE,
            description="Security codes removed"
        ),

        # Expiry dates - show only month/year
        FieldProtectionConfig(
            field_matcher=expiry_pattern,
            protection_level=ProtectionLevel.GENERALIZE,
            options={"date_format": "month"},
            description="Expiry dates generalized to month/year"
        ),

        # Amounts - round to nearest whole number
        FieldProtectionConfig(
            field_matcher=amount_pattern,
            protection_level=ProtectionLevel.GENERALIZE,
            options={"number_precision": 0},
            description="Amounts rounded to whole numbers"
        )
    ]


if __name__ == "__main__":
    import unittest
    unittest.main() 