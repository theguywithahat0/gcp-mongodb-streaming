"""Shared test configuration and fixtures."""
import os
import pytest
from pathlib import Path
from dotenv import load_dotenv

def pytest_configure(config):
    """Configure test environment."""
    # Load environment variables from config files
    config_dir = Path(__file__).parent / "config"
    
    # Load test environment variables
    test_env = config_dir / ".env.test"
    if test_env.exists():
        load_dotenv(test_env)
    
    # Load integration test environment variables for integration tests
    if any(item.get_marker("integration") for item in config.items):
        integration_env = config_dir / ".env.integration"
        if integration_env.exists():
            load_dotenv(integration_env)

@pytest.fixture(scope="session")
def project_root():
    """Return the project root directory."""
    return Path(__file__).parent.parent

@pytest.fixture(scope="session")
def test_config_dir():
    """Return the test config directory."""
    return Path(__file__).parent / "config"

@pytest.fixture(scope="session")
def mongodb_uri():
    """Return the MongoDB URI from environment variables."""
    uri = os.getenv("MONGODB_TEST_URI")
    if not uri:
        pytest.skip("MongoDB URI not configured")
    return uri

@pytest.fixture(scope="session")
def mongodb_database():
    """Return the MongoDB database name from environment variables."""
    db = os.getenv("MONGODB_TEST_DB", "test_db")
    return db

@pytest.fixture(scope="session")
def mongodb_collection():
    """Return the MongoDB collection name from environment variables."""
    collection = os.getenv("MONGODB_TEST_COLLECTION", "test_collection")
    return collection
