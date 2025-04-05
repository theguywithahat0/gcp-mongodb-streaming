from pathlib import Path
from typing import Any, Dict, List, Optional
import os
import yaml


class ConfigManager:
    """Configuration management system that handles YAML files and environment variables."""

    def __init__(self, config_path: Path, required_keys: Optional[List[str]] = None):
        """
        Initialize the configuration manager.

        Args:
            config_path: Path to the YAML configuration file
            required_keys: List of required configuration keys (dot notation supported)
        
        Raises:
            ValueError: If a required key is missing
            FileNotFoundError: If the config file doesn't exist and is required
        """
        self.config_path = Path(config_path)
        self.config: Dict[str, Any] = {}
        self._load_config()
        
        if required_keys:
            self._validate_required_keys(required_keys)

    def _load_config(self) -> None:
        """Load configuration from YAML file and environment variables."""
        # Load YAML config if it exists
        if self.config_path.exists():
            with open(self.config_path) as f:
                self.config = yaml.safe_load(f) or {}
        
        # TODO: Implement environment variable override

    def _validate_required_keys(self, required_keys: List[str]) -> None:
        """
        Validate that all required keys are present in the configuration.

        Args:
            required_keys: List of required configuration keys

        Raises:
            ValueError: If any required key is missing
        """
        for key in required_keys:
            try:
                self.get(key)
            except KeyError:
                raise ValueError(f"Required configuration key missing: {key}")

    def _get_env_key(self, key: str) -> str:
        """
        Convert dot notation key to environment variable format.
        Example: 'mongodb.uri' -> 'MONGODB_URI'

        Args:
            key: Configuration key in dot notation

        Returns:
            Environment variable name
        """
        return key.replace('.', '_').upper()

    def _get_nested_value(self, keys: List[str], config_dict: Dict[str, Any]) -> Any:
        """
        Get a value from nested dictionary using a list of keys.

        Args:
            keys: List of keys to traverse
            config_dict: Dictionary to search in

        Returns:
            Value from the dictionary

        Raises:
            KeyError: If the key path doesn't exist
        """
        current = config_dict
        for key in keys:
            if isinstance(current, dict):
                if key in current:
                    current = current[key]
                else:
                    raise KeyError(f"Key not found: {key}")
            else:
                raise KeyError(f"Cannot traverse further at key: {key}")
        return current

    def get(self, key: str, default: Any = None) -> Any:
        """
        Get a configuration value using dot notation.
        Checks environment variables first, then YAML config.

        Args:
            key: Configuration key in dot notation (e.g., 'mongodb.database')
            default: Default value if key doesn't exist

        Returns:
            Configuration value

        Raises:
            KeyError: If the key doesn't exist and no default is provided
        """
        try:
            # Check environment variables first
            env_key = self._get_env_key(key)
            env_value = os.environ.get(env_key)
            if env_value is not None:
                return env_value

            # If not in environment, check YAML config
            keys = key.split('.')
            return self._get_nested_value(keys, self.config)

        except KeyError as e:
            if default is not None:
                return default
            raise KeyError(f"Configuration key not found: {key}") from e
