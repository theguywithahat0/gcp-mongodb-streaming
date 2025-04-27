"""Data protection module for field-level encryption and sensitive data handling.

This file provides backward compatibility by importing from the security module.
In future code, please import directly from connector.security instead.
"""

# Re-export all symbols from the security module
from ..security.data_protection import (
    base64,
    hashlib,
    hmac,
    os,
    re,
    Any, Callable, Dict, List, Optional, Pattern, Set, Tuple, Union,
    dataclass,
    Enum,
    lru_cache,
    datetime,
    Fernet,
    hashes,
    PBKDF2HMAC,
    get_logger,
    TransformFunc,
    FieldMatcher,
    EncryptedValue,
    logger,
    ProtectionLevel,
    FieldProtectionConfig,
    FieldProtectionError,
    DataProtector,
    create_pii_protection_rules,
    create_payment_protection_rules
)

# For backward compatibility
__all__ = [
    'base64',
    'hashlib',
    'hmac',
    'os',
    're',
    'Any', 'Callable', 'Dict', 'List', 'Optional', 'Pattern', 'Set', 'Tuple', 'Union',
    'dataclass',
    'Enum',
    'lru_cache',
    'datetime',
    'Fernet',
    'hashes',
    'PBKDF2HMAC',
    'get_logger',
    'TransformFunc',
    'FieldMatcher',
    'EncryptedValue',
    'logger',
    'ProtectionLevel',
    'FieldProtectionConfig',
    'FieldProtectionError',
    'DataProtector',
    'create_pii_protection_rules',
    'create_payment_protection_rules'
] 