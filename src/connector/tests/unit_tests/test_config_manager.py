"""Unit tests for the configuration manager."""

import os
import tempfile
from pathlib import Path
from typing import Dict, Any
import pytest
import yaml
from google.api_core.retry import Retry

from connector.config.config_manager import (
    ConfigurationManager,
    BackpressureConfig,
    BatchSettings,
    CircuitBreakerSettings,
    ConnectorConfig,
    MongoDBConfig,
    PubSubConfig,
    MongoDBCollectionConfig,
    FirestoreConfig,
    LogSamplingConfig,
    DeduplicationConfig,
    HealthConfig,
    RetryConfig,
    MonitoringConfig,
    LoggingConfig,
    ResourceConfig,
    PublisherConfig
)

@pytest.fixture
def valid_config_dict() -> Dict[str, Any]:
    """Create a valid configuration dictionary."""
    return {
        "mongodb": {
            "uri": "mongodb://localhost:27017",
            "database": "test_db",
            "warehouse_id": "TEST_WH",
            "collections": [
                {
                    "name": "test_collection",
                    "topic": "test-topic",
                    "watch_full_document": True
                }
            ]
        },
        "pubsub": {
            "project_id": "test-project",
            "status_topic": "test-status-topic",
            "publisher": {
                "batch_settings": {
                    "max_messages": 100,
                    "max_bytes": 1048576,
                    "max_latency": 0.1
                },
                "retry": {
                    "initial_delay": 1.0,
                    "max_delay": 60.0,
                    "multiplier": 2.0,
                    "max_attempts": 5
                },
                "circuit_breaker": {
                    "failure_threshold": 5,
                    "reset_timeout": 60.0,
                    "half_open_max_calls": 3
                },
                "backpressure": {
                    "tokens_per_second": 1000.0,
                    "bucket_size": 2000,
                    "min_tokens_per_second": 100.0,
                    "max_tokens_per_second": 5000.0
                }
            }
        },
        "firestore": {
            "collection": "resume_tokens",
            "ttl": 86400
        },
        "monitoring": {
            "logging": {
                "level": "INFO",
                "format": "json",
                "sampling": {
                    "enabled": True,
                    "rate": 0.1,
                    "strategy": "random"
                }
            },
            "tracing": {
                "enabled": True,
                "sampling_rate": 0.1,
                "exporter": "cloud_trace",
                "service_name": "test-service",
                "endpoint": "localhost:4317"
            }
        }
    }

@pytest.fixture
def config_file(valid_config_dict: Dict[str, Any]) -> Path:
    """Create a temporary config file with valid configuration."""
    with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
        yaml.dump(valid_config_dict, f)
        return Path(f.name)

def test_load_valid_config(config_file: Path):
    """Test loading a valid configuration file."""
    manager = ConfigurationManager(str(config_file))
    config = manager.get_config()
    
    assert isinstance(config, ConnectorConfig)
    assert isinstance(config.mongodb, MongoDBConfig)
    assert isinstance(config.pubsub, PubSubConfig)
    assert isinstance(config.firestore, FirestoreConfig)
    
    # Check MongoDB config
    assert config.mongodb.uri == "mongodb://localhost:27017"
    assert config.mongodb.database == "test_db"
    assert len(config.mongodb.collections) == 1
    assert isinstance(config.mongodb.collections[0], MongoDBCollectionConfig)
    
    # Check Pub/Sub config
    assert config.pubsub.project_id == "test-project"
    assert config.pubsub.status_topic == "test-status-topic"
    assert isinstance(config.pubsub.publisher, PublisherConfig)
    
    # Check Firestore config
    assert config.firestore.collection == "resume_tokens"
    assert config.firestore.ttl == 86400

def test_environment_override(config_file: Path, monkeypatch: pytest.MonkeyPatch):
    """Test overriding configuration with environment variables."""
    monkeypatch.setenv("MONGODB_URI", "mongodb://testhost:27017")
    monkeypatch.setenv("PUBSUB_PROJECT_ID", "env-test-project")
    
    manager = ConfigurationManager(str(config_file))
    config = manager.get_config()
    
    assert config.mongodb.uri == "mongodb://testhost:27017"
    assert config.pubsub.project_id == "env-test-project"

def test_default_values():
    """Test that default values are set correctly when not specified."""
    minimal_config = {
        "mongodb": {
            "uri": "mongodb://localhost:27017",
            "database": "test_db",
            "collections": [
                {
                    "name": "test_collection",
                    "topic": "test-topic"
                }
            ]
        },
        "pubsub": {
            "project_id": "test-project",
            "status_topic": "test-status-topic"
        }
    }
    
    with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
        yaml.dump(minimal_config, f)
        manager = ConfigurationManager(f.name)
        config = manager.get_config()
    
    # Check default values
    assert config.mongodb.collections[0].watch_full_document is True
    assert isinstance(config.pubsub.publisher.batch_settings, BatchSettings)
    assert isinstance(config.pubsub.publisher.retry, Retry)
    assert isinstance(config.pubsub.publisher.circuit_breaker, CircuitBreakerSettings)
    assert isinstance(config.pubsub.publisher.backpressure, BackpressureConfig)

def test_invalid_mongodb_uri():
    """Test that invalid MongoDB URI raises an error."""
    invalid_config = {
        "mongodb": {
            "uri": "invalid://uri",
            "database": "test_db",
            "collections": [
                {
                    "name": "test_collection",
                    "topic": "test-topic"
                }
            ]
        },
        "pubsub": {
            "project_id": "test-project",
            "status_topic": "test-status-topic"
        }
    }
    
    with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
        yaml.dump(invalid_config, f)
        with pytest.raises(ValueError, match="MongoDB URI must start with 'mongodb://' or 'mongodb\\+srv://'"):
            ConfigurationManager(f.name).get_config()

def test_invalid_collection_config():
    """Test that invalid collection configuration raises an error."""
    invalid_config = {
        "mongodb": {
            "uri": "mongodb://localhost:27017",
            "database": "test_db",
            "collections": [
                {
                    "name": "test_collection"  # Missing required 'topic' field
                }
            ]
        },
        "pubsub": {
            "project_id": "test-project",
            "status_topic": "test-status-topic"
        }
    }
    
    with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
        yaml.dump(invalid_config, f)
        with pytest.raises(ValueError, match="Failed to create configuration objects: 'topic'"):
            ConfigurationManager(f.name).get_config()

def test_log_sampling_config():
    """Test log sampling configuration."""
    config_with_sampling = {
        "mongodb": {
            "uri": "mongodb://localhost:27017",
            "database": "test_db",
            "collections": [
                {
                    "name": "test_collection",
                    "topic": "test-topic",
                    "log_sampling": {
                        "enabled": True,
                        "rate": 0.5,
                        "strategy": "random"
                    }
                }
            ]
        },
        "pubsub": {
            "project_id": "test-project",
            "status_topic": "test-status-topic"
        }
    }
    
    with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
        yaml.dump(config_with_sampling, f)
        manager = ConfigurationManager(f.name)
        config = manager.get_config()
    
    sampling_config = config.mongodb.collections[0].log_sampling
    assert isinstance(sampling_config, LogSamplingConfig)
    assert sampling_config.enabled is True
    assert sampling_config.rate == 0.5
    assert sampling_config.strategy == "random"

def test_deduplication_config():
    """Test deduplication configuration."""
    config_with_dedup = {
        "mongodb": {
            "uri": "mongodb://localhost:27017",
            "database": "test_db",
            "collections": [
                {
                    "name": "test_collection",
                    "topic": "test-topic",
                    "deduplication": {
                        "enabled": True,
                        "memory_ttl": 7200,
                        "persistent_enabled": True,
                        "persistent_ttl": 172800
                    }
                }
            ]
        },
        "pubsub": {
            "project_id": "test-project",
            "status_topic": "test-status-topic"
        }
    }
    
    with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
        yaml.dump(config_with_dedup, f)
        manager = ConfigurationManager(f.name)
        config = manager.get_config()
    
    dedup_config = config.mongodb.collections[0].deduplication
    assert isinstance(dedup_config, DeduplicationConfig)
    assert dedup_config.enabled is True
    assert dedup_config.memory_ttl == 7200
    assert dedup_config.persistent_enabled is True
    assert dedup_config.persistent_ttl == 172800

def test_monitoring_config():
    """Test monitoring configuration."""
    config_dict = {
        "mongodb": {
            "uri": "mongodb://localhost:27017",
            "database": "test_db",
            "collections": [{"name": "test", "topic": "test-topic"}]
        },
        "pubsub": {
            "project_id": "test-project",
            "status_topic": "test-status-topic"
        },
        "monitoring": {
            "logging": {
                "level": "INFO",
                "format": "json",
                "sampling": {
                    "enabled": True,
                    "rate": 0.1,
                    "strategy": "random"
                }
            },
            "tracing": {
                "enabled": True,
                "sampling_rate": 0.1,
                "exporter": "cloud_trace",
                "service_name": "test-service",
                "endpoint": "localhost:4317"
            }
        }
    }
    
    with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
        yaml.dump(config_dict, f)
        manager = ConfigurationManager(f.name)
        config = manager.get_config()
        
        assert config.monitoring is not None
        assert isinstance(config.monitoring, MonitoringConfig)
        assert config.monitoring.logging.level == "INFO"
        assert config.monitoring.logging.format == "json"
        assert config.monitoring.logging.sampling.enabled is True
        assert config.monitoring.logging.sampling.rate == 0.1
        assert config.monitoring.logging.sampling.strategy == "random"
        assert config.monitoring.tracing["enabled"] is True
        assert config.monitoring.tracing["sampling_rate"] == 0.1
        assert config.monitoring.tracing["exporter"] == "cloud_trace"
        assert config.monitoring.tracing["service_name"] == "test-service"
        assert config.monitoring.tracing["endpoint"] == "localhost:4317"

def test_resource_config():
    """Test resource configuration."""
    config_with_resource = {
        "mongodb": {
            "uri": "mongodb://localhost:27017",
            "database": "test_db",
            "collections": [
                {
                    "name": "test_collection",
                    "topic": "test-topic"
                }
            ]
        },
        "pubsub": {
            "project_id": "test-project",
            "status_topic": "test-status-topic"
        }
    }
    
    with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
        yaml.dump(config_with_resource, f)
        manager = ConfigurationManager(f.name)
        config = manager.get_config()
    
    assert isinstance(config.resource, ResourceConfig)
    # Test default values
    assert config.resource.enabled is True
    assert config.resource.monitoring_interval == 30.0
    assert config.resource.log_interval == 300.0
    assert config.resource.memory_warning_threshold == 0.7
    assert config.resource.memory_critical_threshold == 0.85
    assert config.resource.memory_emergency_threshold == 0.95
    assert config.resource.cpu_warning_threshold == 0.7
    assert config.resource.cpu_critical_threshold == 0.85
    assert config.resource.cpu_emergency_threshold == 0.95
    assert config.resource.enable_gc_optimization is True
    assert config.resource.gc_threshold_adjustment is True
    assert config.resource.enable_object_tracking is False
    assert config.resource.tracked_types == ["dict", "ChangeStream", "Message"]
    assert config.resource.enable_auto_response is True
    assert config.resource.circuit_breaker_enabled is True
    assert config.resource.circuit_open_time == 30.0

    # Now test with custom values
    config_with_resource["resource"] = {
        "enabled": False,
        "monitoring_interval": 15.0,
        "log_interval": 150.0,
        "memory_warning_threshold": 0.8,
        "memory_critical_threshold": 0.85,
        "memory_emergency_threshold": 0.9,
        "cpu_warning_threshold": 0.8,
        "cpu_critical_threshold": 0.85,
        "cpu_emergency_threshold": 0.9,
        "enable_gc_optimization": False,
        "gc_threshold_adjustment": False,
        "enable_object_tracking": True,
        "tracked_types": ["dict", "Message"],
        "enable_auto_response": False,
        "circuit_breaker_enabled": True,
        "circuit_open_time": 45.0
    }

    with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
        yaml.dump(config_with_resource, f)
        manager = ConfigurationManager(f.name)
        config = manager.get_config()

    assert isinstance(config.resource, ResourceConfig)
    assert config.resource.enabled is False
    assert config.resource.monitoring_interval == 15.0
    assert config.resource.log_interval == 150.0
    assert config.resource.memory_warning_threshold == 0.8
    assert config.resource.memory_critical_threshold == 0.85
    assert config.resource.memory_emergency_threshold == 0.9
    assert config.resource.cpu_warning_threshold == 0.8
    assert config.resource.cpu_critical_threshold == 0.85
    assert config.resource.cpu_emergency_threshold == 0.9
    assert config.resource.enable_gc_optimization is False
    assert config.resource.gc_threshold_adjustment is False
    assert config.resource.enable_object_tracking is True
    assert config.resource.tracked_types == ["dict", "Message"]
    assert config.resource.enable_auto_response is False
    assert config.resource.circuit_breaker_enabled is True
    assert config.resource.circuit_open_time == 45.0

def test_create_config_objects():
    """Test creating configuration objects from valid config."""
    config = {
        "mongodb": {
            "uri": "mongodb://localhost:27017",
            "database": "test_db",
            "collections": [
                {
                    "name": "test_collection",
                    "topic": "test-topic"
                }
            ]
        },
        "pubsub": {
            "project_id": "test-project",
            "status_topic": "test-status-topic",
            "publisher": {
                "batch_settings": {
                    "max_bytes": 1024,
                    "max_latency": 0.5,
                    "max_messages": 100
                },
                "retry": {
                    "initial": 1.0,
                    "maximum": 10.0,
                    "multiplier": 1.3,
                    "deadline": 30.0
                },
                "backpressure": {
                    "tokens_per_second": 1000.0,
                    "bucket_size": 2000,
                    "min_tokens_per_second": 100.0,
                    "max_tokens_per_second": 5000.0
                },
                "circuit_breaker": {
                    "failure_threshold": 5,
                    "reset_timeout": 60.0,
                    "half_open_max_calls": 3
                }
            }
        }
    }

    with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
        yaml.dump(config, f)
        config_manager = ConfigurationManager(f.name)
        config = config_manager.get_config()

    assert isinstance(config.pubsub.publisher, PublisherConfig)
    assert isinstance(config.pubsub.publisher.batch_settings, BatchSettings)
    assert isinstance(config.pubsub.publisher.retry, Retry)
    assert isinstance(config.pubsub.publisher.backpressure, BackpressureConfig)
    assert isinstance(config.pubsub.publisher.circuit_breaker, CircuitBreakerSettings)

    # Verify batch settings values
    assert config.pubsub.publisher.batch_settings.max_bytes == 1024
    assert config.pubsub.publisher.batch_settings.max_latency == 0.5
    assert config.pubsub.publisher.batch_settings.max_messages == 100

def test_retry_config_creation():
    """Test that retry settings are correctly created from config."""
    config = {
        "mongodb": {
            "uri": "mongodb://localhost:27017",
            "database": "test_db",
            "collections": [
                {
                    "name": "test_collection",
                    "topic": "test-topic"
                }
            ]
        },
        "pubsub": {
            "project_id": "test-project",
            "status_topic": "test-status-topic",
            "publisher": {
                "retry": {
                    "initial_delay": 1.0,
                    "max_delay": 10.0,
                    "multiplier": 1.3,
                    "deadline": 30.0
                }
            }
        }
    }

    with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
        yaml.dump(config, f)
        config_manager = ConfigurationManager(f.name)
        config = config_manager.get_config()

    retry_settings = config.pubsub.publisher.retry
    assert isinstance(retry_settings, Retry)
    assert retry_settings._initial == 1.0
    assert retry_settings._maximum == 10.0
    assert retry_settings._multiplier == 1.3
    assert retry_settings.deadline == 30.0

def test_retry_config_defaults():
    """Test that default retry settings are used when not specified."""
    config = {
        "mongodb": {
            "uri": "mongodb://localhost:27017",
            "database": "test_db",
            "collections": [
                {
                    "name": "test_collection",
                    "topic": "test-topic"
                }
            ]
        },
        "pubsub": {
            "project_id": "test-project",
            "status_topic": "test-status-topic",
            "publisher": {}
        }
    }

    with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
        yaml.dump(config, f)
        config_manager = ConfigurationManager(f.name)
        config = config_manager.get_config()

    retry_settings = config.pubsub.publisher.retry
    assert isinstance(retry_settings, Retry)
    assert retry_settings.deadline == 30.0  # Default value from our configuration

def test_retry_config_validation():
    """Test that invalid retry settings raise appropriate errors."""
    config = {
        "mongodb": {
            "uri": "mongodb://localhost:27017",
            "database": "test_db",
            "collections": [
                {
                    "name": "test_collection",
                    "topic": "test-topic"
                }
            ]
        },
        "pubsub": {
            "project_id": "test-project",
            "status_topic": "test-status-topic",
            "publisher": {
                "retry": {
                    "initial_delay": -1.0,  # Invalid: negative value
                    "max_delay": 60.0,
                    "multiplier": 2.0,
                    "max_attempts": 5
                }
            }
        }
    }

    with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
        yaml.dump(config, f)
        with pytest.raises(ValueError, match="Retry initial delay must be positive"):
            ConfigurationManager(f.name).get_config()

def test_circuit_breaker_config():
    """Test circuit breaker configuration."""
    # Test valid configuration
    config_dict = {
        "mongodb": {
            "uri": "mongodb://localhost:27017",
            "database": "test_db",
            "collections": [{"name": "test", "topic": "test-topic"}]
        },
        "pubsub": {
            "project_id": "test-project",
            "status_topic": "test-status-topic",
            "publisher": {
                "circuit_breaker": {
                    "failure_threshold": 5,
                    "reset_timeout": 60.0,
                    "half_open_max_calls": 3
                }
            }
        }
    }
    
    with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
        yaml.dump(config_dict, f)
        manager = ConfigurationManager(f.name)
        config = manager.get_config()
        
        assert config.pubsub.publisher.circuit_breaker is not None
        assert isinstance(config.pubsub.publisher.circuit_breaker, CircuitBreakerSettings)
        assert config.pubsub.publisher.circuit_breaker.failure_threshold == 5
        assert config.pubsub.publisher.circuit_breaker.reset_timeout == 60.0
        assert config.pubsub.publisher.circuit_breaker.half_open_max_calls == 3

    # Test invalid failure threshold
    config_dict["pubsub"]["publisher"]["circuit_breaker"]["failure_threshold"] = 0
    with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
        yaml.dump(config_dict, f)
        with pytest.raises(ValueError, match="Circuit breaker failure threshold must be positive"):
            ConfigurationManager(f.name).get_config()

    # Test invalid reset timeout
    config_dict["pubsub"]["publisher"]["circuit_breaker"].update({
        "failure_threshold": 5,
        "reset_timeout": -1.0
    })
    with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
        yaml.dump(config_dict, f)
        with pytest.raises(ValueError, match="Circuit breaker reset timeout must be positive"):
            ConfigurationManager(f.name).get_config()

    # Test invalid half_open_max_calls
    config_dict["pubsub"]["publisher"]["circuit_breaker"].update({
        "reset_timeout": 60.0,
        "half_open_max_calls": 0
    })
    with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
        yaml.dump(config_dict, f)
        with pytest.raises(ValueError, match="Circuit breaker half-open max calls must be positive"):
            ConfigurationManager(f.name).get_config()

    # Test half_open_max_calls exceeding failure threshold
    config_dict["pubsub"]["publisher"]["circuit_breaker"].update({
        "failure_threshold": 3,
        "half_open_max_calls": 5
    })
    with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
        yaml.dump(config_dict, f)
        with pytest.raises(ValueError, match="Half-open max calls cannot exceed failure threshold"):
            ConfigurationManager(f.name).get_config()

def test_cleanup():
    """Clean up temporary files after tests."""
    def cleanup_temp_files():
        for file in Path(".").glob("*.yaml"):
            if file.is_file() and "tmp" in str(file):
                file.unlink()
    
    cleanup_temp_files()

def test_firestore_config():
    """Test Firestore configuration."""
    config_dict = {
        "mongodb": {
            "uri": "mongodb://localhost:27017",
            "database": "test_db",
            "collections": [{"name": "test", "topic": "test-topic"}]
        },
        "pubsub": {
            "project_id": "test-project",
            "status_topic": "test-status-topic"
        },
        "firestore": {
            "collection": "resume_tokens",
            "ttl": 86400
        }
    }
    
    with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
        yaml.dump(config_dict, f)
        manager = ConfigurationManager(f.name)
        config = manager.get_config()
        
        assert config.firestore is not None
        assert isinstance(config.firestore, FirestoreConfig)
        assert config.firestore.collection == "resume_tokens"
        assert config.firestore.ttl == 86400

    # Test with missing collection
    config_dict["firestore"] = {
        "ttl": 86400
    }
    with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
        yaml.dump(config_dict, f)
        with pytest.raises(ValueError, match="Firestore collection is required"):
            ConfigurationManager(f.name).get_config()

    # Test with invalid TTL
    config_dict["firestore"] = {
        "collection": "resume_tokens",
        "ttl": -1
    }
    with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
        yaml.dump(config_dict, f)
        with pytest.raises(ValueError, match="Firestore TTL must be positive"):
            ConfigurationManager(f.name).get_config() 