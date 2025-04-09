"""
Dynamic transform loader for the MongoDB to Pub/Sub pipeline.

This module provides:
1. Functions to dynamically load transforms from configuration
2. Error handling for missing or invalid transforms
3. Configuration validation for transforms
"""

import importlib
import logging
import inspect
from typing import List, Dict, Any, Type, Optional

from .base import MessageTransform, ConfigurableTransform, SchemaAware

logger = logging.getLogger(__name__)

def _is_valid_transform_class(cls) -> bool:
    """
    Check if a class is a valid MessageTransform.
    
    Args:
        cls: Class to check
        
    Returns:
        True if class is a valid transform, False otherwise
    """
    return (inspect.isclass(cls) and 
            issubclass(cls, MessageTransform) and
            cls != MessageTransform)

def _get_transform_class(module_path: str, class_name: str) -> Type[MessageTransform]:
    """
    Dynamically import and return a transform class.
    
    Args:
        module_path: Full module path (e.g., 'package.module')
        class_name: Name of the transform class
        
    Returns:
        Transform class
        
    Raises:
        ImportError: If module cannot be imported
        AttributeError: If class does not exist in module
        TypeError: If class is not a valid MessageTransform
    """
    # Import the module
    module = importlib.import_module(module_path)
    
    # Get the class
    cls = getattr(module, class_name)
    
    # Validate it's a MessageTransform
    if not _is_valid_transform_class(cls):
        raise TypeError(f"Class {class_name} in {module_path} is not a valid MessageTransform")
    
    return cls

def load_transforms(transform_config: Dict[str, Any], 
                    schemas: Optional[Dict[str, Dict[str, Any]]] = None) -> List[MessageTransform]:
    """
    Dynamically load transforms based on configuration.
    
    Args:
        transform_config: Transform configuration dictionary
        schemas: Optional schemas to pass to SchemaAware transforms
        
    Returns:
        List of instantiated transform objects
        
    Config format:
    {
        "transforms": [
            {
                "name": "MyTransform",
                "module": "my_package.transforms.my_module",
                "config": {
                    "param1": "value1"
                },
                "enabled": true
            }
        ]
    }
    """
    transforms = []
    transform_configs = transform_config.get("transforms", [])
    
    for config in transform_configs:
        # Skip disabled transforms
        if not config.get("enabled", True):
            logger.info(f"Skipping disabled transform: {config.get('name')}")
            continue
        
        try:
            # Get transform details
            transform_name = config.get("name")
            module_path = config.get("module")
            transform_params = config.get("config", {})
            
            if not transform_name or not module_path:
                logger.warning(f"Skipping transform with missing name or module: {config}")
                continue
                
            # Import the transform class
            transform_class = _get_transform_class(module_path, transform_name)
            
            # Instantiate the transform
            try:
                transform = transform_class(**transform_params)
            except TypeError as e:
                logger.error(f"Failed to instantiate {transform_name}: {str(e)}")
                continue
            
            # Configure if it's a ConfigurableTransform
            if isinstance(transform, ConfigurableTransform) and "config" in config:
                transform.configure(config["config"])
            
            # Set schemas if it's SchemaAware
            if isinstance(transform, SchemaAware) and schemas is not None:
                transform.set_schemas(schemas)
            
            # Add to list of transforms
            transforms.append(transform)
            logger.info(f"Loaded transform: {transform_name} from {module_path}")
            
        except (ImportError, AttributeError, TypeError) as e:
            logger.error(f"Failed to load transform {config.get('name', 'unknown')}: {str(e)}")
    
    return transforms

def validate_transform_config(transform_config: Dict[str, Any]) -> List[str]:
    """
    Validate transform configuration.
    
    Args:
        transform_config: Transform configuration dictionary
        
    Returns:
        List of validation error messages, empty if valid
    """
    errors = []
    
    # Check transforms section exists
    if "transforms" not in transform_config:
        errors.append("Missing 'transforms' section in configuration")
        return errors
    
    # Validate each transform
    for i, config in enumerate(transform_config.get("transforms", [])):
        # Check required fields
        if "name" not in config:
            errors.append(f"Transform {i+1} is missing 'name' field")
        
        if "module" not in config:
            errors.append(f"Transform {i+1} ({config.get('name', 'unknown')}) is missing 'module' field")
    
    return errors 