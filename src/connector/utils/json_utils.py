"""JSON utilities for MongoDB data types."""

import json
from datetime import datetime
from bson import ObjectId, Timestamp

class MongoDBJSONEncoder(json.JSONEncoder):
    """JSON encoder that handles MongoDB data types.
    
    This encoder handles the following types:
    - ObjectId: Converted to string
    - Timestamp: Converted to Unix timestamp
    - datetime: Converted to ISO format string
    
    Example:
        ```python
        data = {"_id": ObjectId(), "timestamp": Timestamp(1, 1)}
        json_str = json.dumps(data, cls=MongoDBJSONEncoder)
        ```
    """
    def default(self, obj):
        if isinstance(obj, ObjectId):
            return str(obj)
        if isinstance(obj, Timestamp):
            return obj.time
        if isinstance(obj, datetime):
            return obj.isoformat()
        return super().default(obj) 