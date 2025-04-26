"""Message serialization utilities for the connector."""

import json
import msgpack
from datetime import datetime
from typing import Any, Dict, Union
from bson import ObjectId, Timestamp
from enum import Enum

class SerializationFormat(Enum):
    """Available serialization formats."""
    JSON = "json"
    MSGPACK = "msgpack"

class MongoDBEncoder(json.JSONEncoder):
    """JSON encoder that handles MongoDB-specific types."""
    
    def default(self, obj: Any) -> Any:
        if isinstance(obj, ObjectId):
            return str(obj)
        elif isinstance(obj, Timestamp):
            return obj.time
        elif isinstance(obj, datetime):
            return obj.isoformat()
        elif isinstance(obj, Enum):
            return obj.value
        return super().default(obj)

def _encode_mongodb_types(obj: Any) -> Any:
    """Encode MongoDB types for msgpack serialization."""
    if isinstance(obj, ObjectId):
        return {"__type__": "ObjectId", "value": str(obj)}
    elif isinstance(obj, Timestamp):
        return {"__type__": "Timestamp", "value": obj.time}
    elif isinstance(obj, datetime):
        return {"__type__": "datetime", "value": obj.isoformat()}
    elif isinstance(obj, Enum):
        return {"__type__": "Enum", "value": obj.value}
    return obj

def _decode_mongodb_types(obj: Any) -> Any:
    """Decode MongoDB types from msgpack serialization."""
    if isinstance(obj, dict) and "__type__" in obj:
        type_name = obj["__type__"]
        value = obj["value"]
        if type_name == "ObjectId":
            return ObjectId(value)
        elif type_name == "Timestamp":
            return Timestamp(value, 0)
        elif type_name == "datetime":
            return datetime.fromisoformat(value)
        elif type_name == "Enum":
            return value
    return obj

def serialize_message(
    message: Dict[str, Any],
    format: SerializationFormat = SerializationFormat.MSGPACK
) -> bytes:
    """Serialize a message to bytes.
    
    Args:
        message: The message to serialize
        format: The serialization format to use
        
    Returns:
        The serialized message as bytes
    """
    if format == SerializationFormat.JSON:
        return json.dumps(message, cls=MongoDBEncoder).encode('utf-8')
    else:  # MSGPACK
        return msgpack.packb(message, default=_encode_mongodb_types, use_bin_type=True)

def deserialize_message(
    data: bytes,
    format: SerializationFormat = SerializationFormat.MSGPACK
) -> Dict[str, Any]:
    """Deserialize a message from bytes.
    
    Args:
        data: The serialized message
        format: The serialization format to use
        
    Returns:
        The deserialized message
    """
    if format == SerializationFormat.JSON:
        return json.loads(data.decode('utf-8'))
    else:  # MSGPACK
        return msgpack.unpackb(data, object_hook=_decode_mongodb_types, raw=False) 