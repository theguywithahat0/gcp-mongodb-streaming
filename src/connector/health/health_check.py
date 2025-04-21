"""Health check endpoints implementation."""

from fastapi import FastAPI, Response, status
from typing import Dict, Optional
from ..core.connector import MongoDBConnector

app = FastAPI()

class HealthCheck:
    """Health check implementation."""
    
    def __init__(self):
        self._connector: Optional[MongoDBConnector] = None

    def register_connector(self, connector: MongoDBConnector) -> None:
        """Register the connector instance for health checks.
        
        Args:
            connector: The MongoDB connector instance.
        """
        self._connector = connector

    async def check_health(self) -> Dict[str, bool]:
        """Check basic health status.
        
        Returns:
            Dict with health status.
        """
        is_healthy = True
        if self._connector:
            is_healthy = self._connector.running
        return {"healthy": is_healthy}

    async def check_readiness(self) -> Dict[str, bool]:
        """Check if the service is ready to handle requests.
        
        Returns:
            Dict with readiness status and component statuses.
        """
        if not self._connector:
            return {
                "ready": False,
                "components": {
                    "connector": False,
                    "mongo": False,
                    "pubsub": False,
                    "firestore": False
                }
            }

        # Check MongoDB connection
        mongo_ready = all(
            listener.mongo_client.admin.command('ping')
            for listener in self._connector.listeners.values()
        ) if self._connector.listeners else False

        # Check Pub/Sub (basic client check)
        pubsub_ready = bool(self._connector.publisher._transport)

        # Check Firestore (basic client check)
        firestore_ready = bool(self._connector.firestore_client._client)

        all_ready = all([
            self._connector.running,
            mongo_ready,
            pubsub_ready,
            firestore_ready
        ])

        return {
            "ready": all_ready,
            "components": {
                "connector": self._connector.running,
                "mongo": mongo_ready,
                "pubsub": pubsub_ready,
                "firestore": firestore_ready
            }
        }

# Create health check instance
health_check = HealthCheck()

@app.get("/health")
async def health() -> Response:
    """Basic health check endpoint."""
    status_info = await health_check.check_health()
    return Response(
        content=str(status_info),
        status_code=status.HTTP_200_OK if status_info["healthy"] else status.HTTP_503_SERVICE_UNAVAILABLE,
        media_type="application/json"
    )

@app.get("/readiness")
async def readiness() -> Response:
    """Readiness probe endpoint."""
    status_info = await health_check.check_readiness()
    return Response(
        content=str(status_info),
        status_code=status.HTTP_200_OK if status_info["ready"] else status.HTTP_503_SERVICE_UNAVAILABLE,
        media_type="application/json"
    ) 