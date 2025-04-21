"""Configuration manager for the MongoDB Change Stream Connector."""

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional

import yaml
from dotenv import load_dotenv

@dataclass
class MongoDBCollectionConfig:
    """Configuration for a MongoDB collection to watch."""
    name: str
    topic: str
    watch_filter: Optional[Dict[str, Any]] = None

@dataclass
class MongoDBConfig:
    """MongoDB connection configuration."""
    uri: str
    database: str
    collections: List[MongoDBCollectionConfig]
    options: Dict[str, Any]

@dataclass
class PubSubPublisherConfig:
    """Configuration for Pub/Sub publisher."""
    batch_settings: Dict[str, Any]
    retry_settings: Dict[str, Any]

@dataclass
class PubSubConfig:
    """Google Cloud Pub/Sub configuration."""
    project_id: str
    default_topic_settings: Dict[str, Any]
    publisher: PubSubPublisherConfig

@dataclass
class FirestoreConfig:
    """Firestore configuration for resume token storage."""
    collection: str
    ttl: int

@dataclass
class MonitoringConfig:
    """Monitoring and logging configuration."""
    logging: Dict[str, Any]
    tracing: Dict[str, Any]

@dataclass
class HealthConfig:
    """Health check configuration."""
    endpoints: Dict[str, str]
    readiness: Dict[str, Any]

@dataclass
class RetryConfig:
    """Retry configuration."""
    initial_delay: float
    max_delay: float
    multiplier: float
    jitter: float
    max_retries: int

@dataclass
class ConnectorConfig:
    """Main configuration class for the connector."""
    mongodb: MongoDBConfig
    pubsub: PubSubConfig
    firestore: FirestoreConfig
    monitoring: MonitoringConfig
    health: HealthConfig
    retry: RetryConfig

class ConfigurationManager:
    """Manages loading and validation of configuration."""

    def __init__(self, config_path: Optional[str] = None):
        """Initialize the configuration manager.
        
        Args:
            config_path: Path to the YAML configuration file. If not provided,
                        will look for config.yaml in the default locations.
        """
        self.config_path = config_path
        self._load_environment()
        self.config = self._load_config()

    def _load_environment(self) -> None:
        """Load environment variables from .env file."""
        load_dotenv()

    def _load_config(self) -> ConnectorConfig:
        """Load configuration from YAML file and environment variables.
        
        Returns:
            ConnectorConfig: The loaded and validated configuration.
        
        Raises:
            FileNotFoundError: If the configuration file cannot be found.
            ValueError: If the configuration is invalid.
        """
        # Load from file
        config_dict = self._load_yaml_config()
        
        # Override with environment variables
        self._override_from_env(config_dict)
        
        # Validate and create config objects
        return self._create_config_objects(config_dict)

    def _load_yaml_config(self) -> Dict[str, Any]:
        """Load configuration from YAML file.
        
        Returns:
            Dict[str, Any]: The loaded configuration dictionary.
        
        Raises:
            FileNotFoundError: If no configuration file can be found.
        """
        search_paths = [
            self.config_path,
            "config.yaml",
            "config/config.yaml",
            "/etc/mongo-connector/config.yaml",
        ]

        for path in search_paths:
            if path and Path(path).is_file():
                with open(path, 'r') as f:
                    return yaml.safe_load(f)

        raise FileNotFoundError(
            f"Could not find configuration file. Searched: {search_paths}"
        )

    def _override_from_env(self, config: Dict[str, Any]) -> None:
        """Override configuration values with environment variables.
        
        Args:
            config: The configuration dictionary to update.
        """
        # MongoDB settings
        if mongo_uri := os.getenv("MONGODB_URI"):
            config["mongodb"]["uri"] = mongo_uri
        if mongo_db := os.getenv("MONGODB_DATABASE"):
            config["mongodb"]["database"] = mongo_db

        # Pub/Sub settings
        if project_id := os.getenv("GOOGLE_CLOUD_PROJECT"):
            config["pubsub"]["project_id"] = project_id

        # Firestore settings
        if collection := os.getenv("FIRESTORE_COLLECTION"):
            config["firestore"]["collection"] = collection

        # Logging settings
        if log_level := os.getenv("LOG_LEVEL"):
            config["monitoring"]["logging"]["level"] = log_level

    def _create_config_objects(self, config: Dict[str, Any]) -> ConnectorConfig:
        """Create configuration objects from the loaded dictionary.
        
        Args:
            config: The configuration dictionary.
            
        Returns:
            ConnectorConfig: The created configuration object.
            
        Raises:
            ValueError: If the configuration is invalid.
        """
        try:
            # Create MongoDB collection configs
            collections = [
                MongoDBCollectionConfig(**coll)
                for coll in config["mongodb"]["collections"]
            ]

            # Create MongoDB config
            mongodb = MongoDBConfig(
                uri=config["mongodb"]["uri"],
                database=config["mongodb"]["database"],
                collections=collections,
                options=config["mongodb"]["options"]
            )

            # Create Pub/Sub config
            pubsub = PubSubConfig(
                project_id=config["pubsub"]["project_id"],
                default_topic_settings=config["pubsub"]["default_topic_settings"],
                publisher=PubSubPublisherConfig(**config["pubsub"]["publisher"])
            )

            # Create other configs
            firestore = FirestoreConfig(**config["firestore"])
            monitoring = MonitoringConfig(**config["monitoring"])
            health = HealthConfig(**config["health"])
            retry = RetryConfig(**config["retry"])

            return ConnectorConfig(
                mongodb=mongodb,
                pubsub=pubsub,
                firestore=firestore,
                monitoring=monitoring,
                health=health,
                retry=retry
            )

        except KeyError as e:
            raise ValueError(f"Missing required configuration key: {e}")
        except TypeError as e:
            raise ValueError(f"Invalid configuration value: {e}")

    def get_config(self) -> ConnectorConfig:
        """Get the loaded configuration.
        
        Returns:
            ConnectorConfig: The loaded configuration.
        """
        return self.config 