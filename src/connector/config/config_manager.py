"""Configuration manager for the MongoDB Change Stream Connector."""

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional
from enum import Enum

import yaml
from dotenv import load_dotenv
from google.api_core.retry import Retry

@dataclass
class LogSamplingConfig:
    """Log sampling configuration."""
    enabled: bool = False
    rate: float = 0.1
    strategy: str = "random"  # random, systematic, or reservoir

@dataclass
class BatchSettings:
    """Batch settings for message publishing."""
    max_messages: int = 100
    max_bytes: int = 1048576  # 1MB
    max_latency: float = 0.1  # seconds

@dataclass
class DeduplicationConfig:
    """Message deduplication configuration."""
    enabled: bool = False
    # In-memory settings
    memory_ttl: int = 3600  # 1 hour
    memory_max_size: int = 10000
    # Persistent storage settings
    persistent_enabled: bool = True
    persistent_ttl: int = 86400  # 24 hours
    cleanup_interval: int = 3600  # 1 hour

@dataclass
class CircuitBreakerSettings:
    """Configuration for circuit breaker."""
    failure_threshold: int = 5
    reset_timeout: float = 60.0
    half_open_max_calls: int = 3

@dataclass
class PublisherConfig:
    """Publisher configuration."""
    batch_settings: BatchSettings = field(default_factory=BatchSettings)
    retry: Optional[Retry] = field(default_factory=lambda: Retry(deadline=30))
    circuit_breaker: CircuitBreakerSettings = field(default_factory=CircuitBreakerSettings)

@dataclass
class PubSubConfig:
    """Pub/Sub configuration."""
    project_id: str
    publisher: PublisherConfig = field(default_factory=PublisherConfig)
    status_topic: Optional[str] = None
    default_topic_settings: Dict[str, Any] = field(default_factory=dict)

@dataclass
class MongoDBCollectionConfig:
    """MongoDB collection configuration."""
    name: str
    topic: str
    watch_filter: Optional[List[Dict[str, Any]]] = None
    watch_full_document: bool = True
    resume_token_file: Optional[str] = None
    deduplication: DeduplicationConfig = field(default_factory=DeduplicationConfig)
    log_sampling: LogSamplingConfig = field(default_factory=LogSamplingConfig)

@dataclass
class MongoDBConfig:
    """MongoDB configuration."""
    uri: str
    database: str
    collections: List[MongoDBCollectionConfig]
    warehouse_id: Optional[str] = None
    options: Dict[str, Any] = field(default_factory=dict)

@dataclass
class FirestoreConfig:
    """Firestore configuration for resume token storage."""
    collection: str
    ttl: int

@dataclass
class LogSamplingRule:
    """Configuration for a log sampling rule."""
    rate: float
    strategy: str
    ttl: Optional[int] = None

@dataclass
class LoggingConfig:
    """Logging configuration."""
    level: str = "INFO"
    format: str = "json"
    handlers: List[Dict[str, Any]] = field(default_factory=list)
    sampling: LogSamplingConfig = field(default_factory=LogSamplingConfig)

@dataclass
class MonitoringConfig:
    """Monitoring and logging configuration."""
    logging: LoggingConfig
    tracing: Dict[str, Any]

@dataclass
class HeartbeatConfig:
    """Heartbeat configuration."""
    interval: int = 30  # seconds
    enabled: bool = True

@dataclass
class ReadinessConfig:
    """Readiness probe configuration."""
    timeout: int = 5
    interval: int = 30
    failure_threshold: int = 3

@dataclass
class HealthEndpointsConfig:
    """Health check endpoints configuration."""
    health: str = "/health"
    readiness: str = "/readiness"
    metrics: str = "/metrics"

@dataclass
class HealthConfig:
    """Health check configuration."""
    endpoints: HealthEndpointsConfig = field(default_factory=HealthEndpointsConfig)
    readiness: ReadinessConfig = field(default_factory=ReadinessConfig)
    heartbeat: HeartbeatConfig = field(default_factory=HeartbeatConfig)

@dataclass
class RetryConfig:
    """Retry configuration."""
    initial_delay: float = 1.0
    max_delay: float = 60.0
    multiplier: float = 2.0
    max_attempts: int = 5

@dataclass
class ConnectorConfig:
    """Main configuration class for the connector."""
    mongodb: MongoDBConfig
    pubsub: PubSubConfig
    firestore: Optional[FirestoreConfig] = None
    monitoring: Optional[MonitoringConfig] = None
    health: Optional[HealthConfig] = None
    retry: Optional[RetryConfig] = None

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
            FileNotFoundError: If the configuration file cannot be found.
        """
        if self.config_path:
            config_path = Path(self.config_path)
        else:
            # Look in default locations
            config_path = self._find_config_file()
            
        if not config_path.exists():
            raise FileNotFoundError(f"Configuration file not found: {config_path}")
            
        with open(config_path) as f:
            return yaml.safe_load(f)

    def _find_config_file(self) -> Path:
        """Find the configuration file in default locations.
        
        Returns:
            Path: Path to the configuration file.
            
        Raises:
            FileNotFoundError: If no configuration file is found.
        """
        # Default locations to look for config file
        default_locations = [
            Path("config.yaml"),
            Path("config/config.yaml"),
            Path(os.environ.get("CONFIG_PATH", "config.yaml")),
        ]
        
        for path in default_locations:
            if path.exists():
                return path
                
        raise FileNotFoundError("No configuration file found in default locations")

    def _override_from_env(self, config: Dict[str, Any]) -> None:
        """Override configuration values with environment variables.
        
        Args:
            config: Configuration dictionary to update.
        """
        # MongoDB settings
        if mongo_uri := os.getenv("MONGODB_URI"):
            config["mongodb"]["uri"] = mongo_uri
            
        if mongo_db := os.getenv("MONGODB_DATABASE"):
            config["mongodb"]["database"] = mongo_db
            
        # Pub/Sub settings
        if project_id := os.getenv("GOOGLE_CLOUD_PROJECT"):
            config["pubsub"]["project_id"] = project_id
            
        if status_topic := os.getenv("PUBSUB_STATUS_TOPIC"):
            config["pubsub"]["status_topic"] = status_topic

    def _create_config_objects(self, config: Dict[str, Any]) -> ConnectorConfig:
        """Create configuration objects from dictionary.
        
        Args:
            config: Configuration dictionary.
            
        Returns:
            ConnectorConfig: The created configuration object.
            
        Raises:
            ValueError: If the configuration is invalid.
        """
        try:
            # Create MongoDB collection configs
            collection_configs = []
            for coll in config["mongodb"]["collections"]:
                collection_configs.append(
                    MongoDBCollectionConfig(
                        name=coll["name"],
                        topic=coll["topic"],
                        watch_filter=coll.get("watch_filter"),
                        watch_full_document=coll.get("watch_full_document", True),
                        resume_token_file=coll.get("resume_token_file"),
                        deduplication=DeduplicationConfig(**coll.get("deduplication", {})),
                        log_sampling=LogSamplingConfig(**coll.get("log_sampling", {}))
                    )
                )
            
            # Create MongoDB config
            mongodb_config = MongoDBConfig(
                uri=config["mongodb"]["uri"],
                database=config["mongodb"]["database"],
                collections=collection_configs,
                warehouse_id=config["mongodb"].get("warehouse_id"),
                options=config["mongodb"].get("options", {})
            )
            
            # Create Pub/Sub config
            pubsub_config = PubSubConfig(
                project_id=config["pubsub"]["project_id"],
                publisher=PublisherConfig(**config["pubsub"].get("publisher", {})),
                status_topic=config["pubsub"].get("status_topic"),
                default_topic_settings=config["pubsub"].get("default_topic_settings", {})
            )
            
            # Create optional configs if present
            firestore_config = None
            if "firestore" in config:
                firestore_config = FirestoreConfig(**config["firestore"])
                
            monitoring_config = None
            if "monitoring" in config:
                monitoring_config = MonitoringConfig(**config["monitoring"])
                
            health_config = None
            if "health" in config:
                health_config = HealthConfig(**config["health"])
                
            retry_config = None
            if "retry" in config:
                retry_config = RetryConfig(**config["retry"])
            
            return ConnectorConfig(
                mongodb=mongodb_config,
                pubsub=pubsub_config,
                firestore=firestore_config,
                monitoring=monitoring_config,
                health=health_config,
                retry=retry_config
            )
            
        except (KeyError, TypeError, ValueError) as e:
            raise ValueError(f"Invalid configuration: {str(e)}")

    def get_config(self) -> ConnectorConfig:
        """Get the loaded configuration.
        
        Returns:
            ConnectorConfig: The loaded configuration.
        """
        return self.config 