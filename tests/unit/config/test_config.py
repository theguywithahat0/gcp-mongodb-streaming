import pytest
from pathlib import Path
import os
import yaml
from src.pipeline.utils.config import ConfigManager

@pytest.fixture
def sample_config_file(tmp_path):
    config_data = {
        'mongodb': {
            'database': 'test_db',
            'collection': 'test_collection'
        },
        'gcp': {
            'project_id': 'test-project',
            'region': 'us-central1',
            'pubsub': {
                'topic_name': 'test-topic',
                'subscription_name': 'test-sub'
            }
        }
    }
    config_file = tmp_path / "config.yaml"
    with open(config_file, 'w') as f:
        yaml.dump(config_data, f)
    return config_file

@pytest.fixture
def sample_env_vars():
    env_vars = {
        'MONGODB_URI': 'mongodb+srv://test:test@cluster.mongodb.net/',
        'GCP_PROJECT_ID': 'test-project-id',
        'GCP_BUCKET': 'test-bucket'
    }
    # Store original env vars
    original_vars = {key: os.environ.get(key) for key in env_vars}
    # Set test env vars
    for key, value in env_vars.items():
        os.environ[key] = value
    yield env_vars
    # Restore original env vars
    for key, value in original_vars.items():
        if value is None:
            del os.environ[key]
        else:
            os.environ[key] = value

def test_config_load_yaml(sample_config_file):
    """Test loading configuration from YAML file"""
    config = ConfigManager(config_path=sample_config_file)
    assert config.get('mongodb.database') == 'test_db'
    assert config.get('gcp.pubsub.topic_name') == 'test-topic'

def test_config_env_override(sample_config_file, sample_env_vars):
    """Test environment variable overrides"""
    config = ConfigManager(config_path=sample_config_file)
    assert config.get('gcp.project_id') == 'test-project-id'  # Should use env var
    assert config.get('mongodb.database') == 'test_db'  # Should use yaml value

def test_config_missing_key():
    """Test handling of missing configuration keys"""
    config = ConfigManager(config_path=Path('nonexistent.yaml'))
    with pytest.raises(KeyError):
        config.get('nonexistent.key')

def test_config_default_value(sample_config_file):
    """Test default value handling"""
    config = ConfigManager(config_path=sample_config_file)
    assert config.get('nonexistent.key', default='default_value') == 'default_value'

def test_config_validation():
    """Test configuration validation"""
    with pytest.raises(ValueError):
        ConfigManager(config_path=Path('nonexistent.yaml'), required_keys=['mongodb.uri']) 