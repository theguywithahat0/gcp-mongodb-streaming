"""Configuration manager for the MongoDB Change Stream Connector."""

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional, Set
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
class BackpressureConfig:
    """Configuration for backpressure handling."""
    # Token bucket settings
    tokens_per_second: float = 1000.0  # Base rate limit
    bucket_size: int = 2000  # Maximum burst size
    min_tokens_per_second: float = 100.0  # Minimum rate during heavy backpressure
    max_tokens_per_second: float = 5000.0  # Maximum rate during light load
    
    # Adaptive rate adjustment
    error_decrease_factor: float = 0.8  # Multiply rate by this on errors
    success_increase_factor: float = 1.1  # Multiply rate by this on success
    rate_update_interval: float = 5.0  # Seconds between rate adjustments
    
    # Monitoring
    metrics_window_size: int = 100  # Number of operations to track for metrics

@dataclass
class PublisherConfig:
    """Publisher configuration."""
    batch_settings: BatchSettings = field(default_factory=BatchSettings)
    retry: Optional[Retry] = field(default_factory=lambda: Retry(deadline=30))
    circuit_breaker: CircuitBreakerSettings = field(default_factory=CircuitBreakerSettings)
    backpressure: BackpressureConfig = field(default_factory=BackpressureConfig)

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
class ResourceConfig:
    """Configuration for resource monitoring and optimization."""
    # Monitoring settings
    enabled: bool = True
    monitoring_interval: float = 30.0  # Seconds between resource checks
    log_interval: float = 300.0  # Seconds between detailed resource logs
    
    # Memory thresholds
    memory_warning_threshold: float = 0.70  # 70% of available memory
    memory_critical_threshold: float = 0.85  # 85% of available memory
    memory_emergency_threshold: float = 0.95  # 95% of available memory
    
    # CPU thresholds
    cpu_warning_threshold: float = 0.70  # 70% CPU utilization
    cpu_critical_threshold: float = 0.85  # 85% CPU utilization
    cpu_emergency_threshold: float = 0.95  # 95% CPU utilization
    
    # Optimization settings
    enable_gc_optimization: bool = True  # Enable garbage collection optimization
    gc_threshold_adjustment: bool = True  # Adjust GC thresholds based on memory pressure
    enable_object_tracking: bool = False  # Track object counts by type (can be expensive)
    tracked_types: List[str] = field(default_factory=lambda: ["dict", "ChangeStream", "Message"])
    
    # Response actions
    enable_auto_response: bool = True  # Enable automatic resource optimization
    
    # Circuit breaker
    circuit_breaker_enabled: bool = True  # Enable circuit breaker for resource exhaustion
    circuit_open_time: float = 30.0  # Time in seconds to keep circuit open

@dataclass
class ConnectorConfig:
    """Main configuration class for the connector."""
    mongodb: MongoDBConfig
    pubsub: PubSubConfig
    firestore: Optional[FirestoreConfig] = None
    monitoring: Optional[MonitoringConfig] = None
    health: Optional[HealthConfig] = None
    retry: Optional[RetryConfig] = None
    resource: ResourceConfig = field(default_factory=ResourceConfig)

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
        
        # Create and validate config objects
        config = self._create_config_objects(config_dict)
        self._validate_config(config)
        return config

    def _validate_config(self, config: ConnectorConfig) -> None:
        """Validate configuration values.
        
        Args:
            config: The configuration to validate.
            
        Raises:
            ValueError: If configuration values are invalid.
        """
        # Validate MongoDB configuration
        if not config.mongodb.uri:
            raise ValueError("MongoDB URI is required")
            
        # Validate MongoDB URI format
        if not config.mongodb.uri.startswith(("mongodb://", "mongodb+srv://")):
            raise ValueError("MongoDB URI must start with 'mongodb://' or 'mongodb+srv://'")
        
        if not config.mongodb.database:
            raise ValueError("MongoDB database name is required")
            
        # Validate database name format
        if not all(c.isalnum() or c in '_-' for c in config.mongodb.database):
            raise ValueError("MongoDB database name contains invalid characters")
        
        if not config.mongodb.collections:
            raise ValueError("At least one collection must be configured")
            
        # Validate MongoDB options
        if config.mongodb.options:
            self._validate_mongodb_options(config.mongodb.options)
        
        # Validate collection configurations
        for collection in config.mongodb.collections:
            if not collection.name:
                raise ValueError("Collection name is required")
                
            # Validate collection name format
            if not all(c.isalnum() or c in '_-' for c in collection.name):
                raise ValueError(f"Collection name '{collection.name}' contains invalid characters")
                
            if not collection.topic:
                raise ValueError("Collection topic is required")
                
            # Validate Pub/Sub topic format
            if not collection.topic.replace('-', '').isalnum():
                raise ValueError(f"Invalid Pub/Sub topic name format: {collection.topic}")
                
            # Validate watch filter if present
            if collection.watch_filter:
                try:
                    # Basic syntax check for aggregation pipeline stages
                    if not isinstance(collection.watch_filter, list):
                        raise ValueError("Watch filter must be a list of pipeline stages")
                    for stage in collection.watch_filter:
                        if not isinstance(stage, dict):
                            raise ValueError("Each watch filter stage must be a dictionary")
                except Exception as e:
                    raise ValueError(f"Invalid watch filter syntax: {str(e)}")
                
            # Validate deduplication settings
            if collection.deduplication.enabled:
                if collection.deduplication.memory_ttl <= 0:
                    raise ValueError("Deduplication memory TTL must be positive")
                if collection.deduplication.memory_max_size <= 0:
                    raise ValueError("Deduplication memory max size must be positive")
                if collection.deduplication.memory_max_size > 1000000:
                    raise ValueError("Deduplication memory max size cannot exceed 1,000,000")
                if collection.deduplication.persistent_enabled:
                    if collection.deduplication.persistent_ttl <= 0:
                        raise ValueError("Deduplication persistent TTL must be positive")
                    if collection.deduplication.cleanup_interval <= 0:
                        raise ValueError("Deduplication cleanup interval must be positive")
                    if collection.deduplication.cleanup_interval > collection.deduplication.persistent_ttl:
                        raise ValueError("Cleanup interval cannot be greater than persistent TTL")
                        
            # Validate log sampling configuration
            if collection.log_sampling.enabled:
                if not 0 < collection.log_sampling.rate <= 1:
                    raise ValueError("Log sampling rate must be between 0 and 1")
                if collection.log_sampling.strategy not in ["random", "systematic", "reservoir"]:
                    raise ValueError("Invalid log sampling strategy")
        
        # Validate Pub/Sub configuration
        if not config.pubsub.project_id:
            raise ValueError("Google Cloud project ID is required")
            
        # Validate project ID format
        if not config.pubsub.project_id.replace('-', '').isalnum():
            raise ValueError("Invalid Google Cloud project ID format")
            
        # Validate status topic if present
        if config.pubsub.status_topic and not config.pubsub.status_topic.replace('-', '').isalnum():
            raise ValueError("Invalid status topic name format")
        
        # Validate batch settings
        batch_settings = config.pubsub.publisher.batch_settings
        if batch_settings.max_messages <= 0:
            raise ValueError("Publisher batch max_messages must be positive")
        if batch_settings.max_messages > 1000:
            raise ValueError("Publisher batch max_messages cannot exceed 1000")
        if batch_settings.max_bytes <= 0:
            raise ValueError("Publisher batch max_bytes must be positive")
        if batch_settings.max_bytes > 10 * 1024 * 1024:  # 10MB
            raise ValueError("Publisher batch max_bytes cannot exceed 10MB")
        if batch_settings.max_latency <= 0:
            raise ValueError("Publisher batch max_latency must be positive")
        if batch_settings.max_latency > 600:  # 10 minutes
            raise ValueError("Publisher batch max_latency cannot exceed 600 seconds")
        
        # Validate circuit breaker settings
        cb_settings = config.pubsub.publisher.circuit_breaker
        if cb_settings.failure_threshold <= 0:
            raise ValueError("Circuit breaker failure threshold must be positive")
        if cb_settings.reset_timeout <= 0:
            raise ValueError("Circuit breaker reset timeout must be positive")
        if cb_settings.half_open_max_calls <= 0:
            raise ValueError("Circuit breaker half-open max calls must be positive")
        if cb_settings.half_open_max_calls > cb_settings.failure_threshold:
            raise ValueError("Half-open max calls cannot exceed failure threshold")
        
        # Validate retry configuration if present
        if config.retry:
            if config.retry.initial_delay <= 0:
                raise ValueError("Retry initial delay must be positive")
            if config.retry.max_delay <= 0:
                raise ValueError("Retry max delay must be positive")
            if config.retry.multiplier <= 0:
                raise ValueError("Retry multiplier must be positive")
            if config.retry.max_attempts <= 0:
                raise ValueError("Retry max attempts must be positive")
            if config.retry.max_delay < config.retry.initial_delay:
                raise ValueError("Retry max delay must be greater than or equal to initial delay")
            if config.retry.max_attempts > 100:
                raise ValueError("Retry max attempts cannot exceed 100")
        
        # Validate Firestore configuration if present
        if config.firestore:
            if not config.firestore.collection:
                raise ValueError("Firestore collection is required")
            if not all(c.isalnum() or c in '_-' for c in config.firestore.collection):
                raise ValueError("Invalid Firestore collection format")
            if config.firestore.ttl <= 0:
                raise ValueError("Firestore TTL must be positive")
            if config.firestore.ttl > 365 * 24 * 3600:  # 1 year
                raise ValueError("Firestore TTL cannot exceed 1 year")
        
        # Validate monitoring configuration if present
        if config.monitoring:
            if config.monitoring.logging:
                if config.monitoring.logging.level not in ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]:
                    raise ValueError("Invalid logging level")
                if config.monitoring.logging.format not in ["json", "text"]:
                    raise ValueError("Invalid logging format")
                    
            if config.monitoring.tracing:
                if "service_name" not in config.monitoring.tracing:
                    raise ValueError("Tracing service name is required")
                if "endpoint" not in config.monitoring.tracing:
                    raise ValueError("Tracing endpoint is required")
        
        # Validate health configuration if present
        if config.health:
            if config.health.heartbeat.interval <= 0:
                raise ValueError("Heartbeat interval must be positive")
            if config.health.heartbeat.interval > 300:  # 5 minutes
                raise ValueError("Heartbeat interval cannot exceed 300 seconds")
            if config.health.readiness.timeout <= 0:
                raise ValueError("Readiness probe timeout must be positive")
            if config.health.readiness.interval <= 0:
                raise ValueError("Readiness probe interval must be positive")
            if config.health.readiness.failure_threshold <= 0:
                raise ValueError("Readiness probe failure threshold must be positive")
            if config.health.readiness.timeout >= config.health.readiness.interval:
                raise ValueError("Readiness probe timeout must be less than interval")

        # Validate backpressure settings
        bp_settings = config.pubsub.publisher.backpressure
        if bp_settings.tokens_per_second <= 0:
            raise ValueError("Backpressure tokens_per_second must be positive")
        if bp_settings.bucket_size <= 0:
            raise ValueError("Backpressure bucket_size must be positive")
        if bp_settings.min_tokens_per_second <= 0:
            raise ValueError("Backpressure min_tokens_per_second must be positive")
        if bp_settings.max_tokens_per_second <= bp_settings.min_tokens_per_second:
            raise ValueError("Backpressure max_tokens_per_second must be greater than min_tokens_per_second")
        if bp_settings.error_decrease_factor <= 0 or bp_settings.error_decrease_factor >= 1:
            raise ValueError("Backpressure error_decrease_factor must be between 0 and 1")
        if bp_settings.success_increase_factor <= 1:
            raise ValueError("Backpressure success_increase_factor must be greater than 1")
        if bp_settings.rate_update_interval <= 0:
            raise ValueError("Backpressure rate_update_interval must be positive")
        if bp_settings.metrics_window_size <= 0:
            raise ValueError("Backpressure metrics_window_size must be positive")

        # Validate resource configuration
        self._validate_resource_config(config.resource)

    def _validate_mongodb_options(self, options: Dict[str, Any]) -> None:
        """Validate MongoDB connection options.
        
        Args:
            options: MongoDB connection options dictionary.
            
        Raises:
            ValueError: If options are invalid.
        """
        # Validate pool sizes
        if "max_pool_size" in options:
            if not isinstance(options["max_pool_size"], int) or options["max_pool_size"] <= 0:
                raise ValueError("max_pool_size must be a positive integer")
            if options["max_pool_size"] > 1000:
                raise ValueError("max_pool_size cannot exceed 1000")
                
        if "min_pool_size" in options:
            if not isinstance(options["min_pool_size"], int) or options["min_pool_size"] < 0:
                raise ValueError("min_pool_size must be a non-negative integer")
            if "max_pool_size" in options and options["min_pool_size"] > options["max_pool_size"]:
                raise ValueError("min_pool_size cannot exceed max_pool_size")
        
        # Validate timeouts
        for timeout_option in ["connect_timeout_ms", "server_selection_timeout_ms", "socket_timeout_ms"]:
            if timeout_option in options:
                if not isinstance(options[timeout_option], int) or options[timeout_option] <= 0:
                    raise ValueError(f"{timeout_option} must be a positive integer")
                if options[timeout_option] > 300000:  # 5 minutes
                    raise ValueError(f"{timeout_option} cannot exceed 300000ms (5 minutes)")
        
        # Validate write concern
        if "w" in options:
            valid_w_values = ["majority"] + [str(i) for i in range(1, 6)]
            if str(options["w"]) not in valid_w_values:
                raise ValueError("Invalid write concern value")
        
        # Validate read preference
        if "readPreference" in options:
            valid_read_prefs = ["primary", "primaryPreferred", "secondary", "secondaryPreferred", "nearest"]
            if options["readPreference"] not in valid_read_prefs:
                raise ValueError("Invalid read preference value")
        
        # Validate SSL options
        if "ssl" in options and not isinstance(options["ssl"], bool):
            raise ValueError("ssl must be a boolean value")
            
        if "ssl_cert_reqs" in options:
            valid_cert_reqs = ["CERT_NONE", "CERT_OPTIONAL", "CERT_REQUIRED"]
            if options["ssl_cert_reqs"] not in valid_cert_reqs:
                raise ValueError("Invalid ssl_cert_reqs value")

    def _validate_resource_config(self, config: ResourceConfig) -> None:
        """Validate resource monitoring configuration.
        
        Args:
            config: Resource monitoring configuration object.
            
        Raises:
            ValueError: If the configuration is invalid.
        """
        # Validate thresholds
        for threshold in ["memory_warning_threshold", "memory_critical_threshold", "memory_emergency_threshold",
                          "cpu_warning_threshold", "cpu_critical_threshold", "cpu_emergency_threshold"]:
            value = getattr(config, threshold)
            if not isinstance(value, (int, float)):
                raise ValueError(f"{threshold} must be a number")
            if value < 0 or value > 1.0:
                raise ValueError(f"{threshold} must be between 0 and 1.0")
        
        # Validate threshold ordering
        if not (config.memory_warning_threshold <= config.memory_critical_threshold <= config.memory_emergency_threshold):
            raise ValueError("Memory thresholds must be in ascending order: warning <= critical <= emergency")
        
        if not (config.cpu_warning_threshold <= config.cpu_critical_threshold <= config.cpu_emergency_threshold):
            raise ValueError("CPU thresholds must be in ascending order: warning <= critical <= emergency")
        
        # Validate intervals
        for interval in ["monitoring_interval", "log_interval", "circuit_open_time"]:
            value = getattr(config, interval)
            if not isinstance(value, (int, float)):
                raise ValueError(f"{interval} must be a number")
            if value <= 0:
                raise ValueError(f"{interval} must be positive")
        
        # Validate tracked_types
        if not isinstance(config.tracked_types, list):
            raise ValueError("tracked_types must be a list")
        for item in config.tracked_types:
            if not isinstance(item, str):
                raise ValueError("tracked_types must contain strings")

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
        if project_id := os.getenv("PUBSUB_PROJECT_ID"):
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
            
            # Create Pub/Sub config with proper batch settings
            pubsub_config_dict = config["pubsub"]
            publisher_config_dict = pubsub_config_dict.get("publisher", {})
            batch_settings = BatchSettings(**publisher_config_dict.get("batch_settings", {}))
            
            # Create retry settings
            retry_dict = publisher_config_dict.get("retry", {})
            initial = retry_dict.get("initial_delay", 1.0)
            maximum = retry_dict.get("max_delay", 60.0)
            multiplier = retry_dict.get("multiplier", 2.0)
            deadline = retry_dict.get("deadline", 30.0)
            
            # Validate retry settings
            if initial <= 0:
                raise ValueError("Retry initial delay must be positive")
            if maximum <= 0:
                raise ValueError("Retry max delay must be positive")
            if multiplier <= 0:
                raise ValueError("Retry multiplier must be positive")
            if maximum < initial:
                raise ValueError("Retry max delay must be greater than or equal to initial delay")
            
            retry_settings = Retry(
                initial=initial,
                maximum=maximum,
                multiplier=multiplier,
                deadline=deadline
            )
            
            backpressure_config = BackpressureConfig(**publisher_config_dict.get("backpressure", {}))
            circuit_breaker_config = CircuitBreakerSettings(**publisher_config_dict.get("circuit_breaker", {}))
            
            publisher_config = PublisherConfig(
                batch_settings=batch_settings,
                retry=retry_settings,
                backpressure=backpressure_config,
                circuit_breaker=circuit_breaker_config
            )
            
            pubsub_config = PubSubConfig(
                project_id=pubsub_config_dict["project_id"],
                status_topic=pubsub_config_dict["status_topic"],
                publisher=publisher_config
            )
            
            # Create retry config
            retry_config = RetryConfig(**config.get("retry", {}))
            
            # Create monitoring config
            monitoring_config_dict = config.get("monitoring", {})
            logging_config_dict = monitoring_config_dict.get("logging", {})
            sampling_config_dict = logging_config_dict.get("sampling", {})
            
            sampling_config = LogSamplingConfig(
                enabled=sampling_config_dict.get("enabled", False),
                rate=sampling_config_dict.get("rate", 1.0),
                strategy=sampling_config_dict.get("strategy", "random")
            )
            
            logging_config = LoggingConfig(
                level=logging_config_dict.get("level", "INFO"),
                format=logging_config_dict.get("format", "json"),
                sampling=sampling_config
            )
            
            monitoring_config = MonitoringConfig(
                logging=logging_config,
                tracing=monitoring_config_dict.get("tracing", {})
            )
            
            # Create resource config
            resource_config_dict = config.get("resource", {})
            resource_config = ResourceConfig(
                enabled=resource_config_dict.get("enabled", True),
                monitoring_interval=resource_config_dict.get("monitoring_interval", 30.0),
                log_interval=resource_config_dict.get("log_interval", 300.0),
                memory_warning_threshold=resource_config_dict.get("memory_warning_threshold", 0.7),
                memory_critical_threshold=resource_config_dict.get("memory_critical_threshold", 0.85),
                memory_emergency_threshold=resource_config_dict.get("memory_emergency_threshold", 0.95),
                cpu_warning_threshold=resource_config_dict.get("cpu_warning_threshold", 0.7),
                cpu_critical_threshold=resource_config_dict.get("cpu_critical_threshold", 0.85),
                cpu_emergency_threshold=resource_config_dict.get("cpu_emergency_threshold", 0.95),
                enable_gc_optimization=resource_config_dict.get("enable_gc_optimization", True),
                gc_threshold_adjustment=resource_config_dict.get("gc_threshold_adjustment", True),
                enable_object_tracking=resource_config_dict.get("enable_object_tracking", False),
                tracked_types=resource_config_dict.get("tracked_types", ["dict", "ChangeStream", "Message"]),
                enable_auto_response=resource_config_dict.get("enable_auto_response", True),
                circuit_breaker_enabled=resource_config_dict.get("circuit_breaker_enabled", True),
                circuit_open_time=resource_config_dict.get("circuit_open_time", 30.0)
            )
            
            # Create Firestore config if present
            firestore_config = None
            if "firestore" in config:
                if "collection" not in config["firestore"]:
                    raise ValueError("Firestore collection is required")
                firestore_config = FirestoreConfig(
                    collection=config["firestore"]["collection"],
                    ttl=config["firestore"]["ttl"]
                )
            
            # Create main config
            connector_config = ConnectorConfig(
                mongodb=mongodb_config,
                pubsub=pubsub_config,
                firestore=firestore_config,
                monitoring=monitoring_config,
                health=config.get("health"),
                retry=retry_config,
                resource=resource_config
            )
            
            return connector_config
            
        except Exception as e:
            if "Firestore collection is required" in str(e):
                raise
            raise ValueError(f"Failed to create configuration objects: {str(e)}")

    def get_config(self) -> ConnectorConfig:
        """Get the loaded configuration.
        
        Returns:
            ConnectorConfig: The loaded configuration.
        """
        return self.config 