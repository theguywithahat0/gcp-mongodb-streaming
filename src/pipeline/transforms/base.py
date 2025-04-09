"""
Base classes for transformations in the MongoDB to Pub/Sub pipeline.

This module provides:
1. Base classes for implementing custom transforms
2. Standard interfaces for transform integration
3. Utilities for transform loading and configuration
"""

import apache_beam as beam
from abc import ABC, abstractmethod
from typing import Dict, Any, List, Optional

class MessageTransform(beam.PTransform, ABC):
    """
    Base class for all message transforms in the pipeline.
    
    All custom transforms should inherit from this class and implement
    the expand method to define their transformation logic.
    """
    
    @abstractmethod
    def expand(self, pcoll):
        """
        Implement the transformation logic.
        
        Args:
            pcoll: Input PCollection of messages
            
        Returns:
            Transformed PCollection
        """
        pass
    
    def describe(self) -> Dict[str, Any]:
        """
        Return descriptive information about this transform.
        
        Returns:
            Dictionary with transform metadata
        """
        return {
            "name": self.__class__.__name__,
            "module": self.__class__.__module__,
            "description": self.__doc__ or "No description provided"
        }

class MetricsProvider(ABC):
    """
    Interface for transforms that provide metrics.
    
    Transforms implementing this interface can report metrics
    for monitoring and analysis.
    """
    
    @abstractmethod
    def get_metrics(self) -> Dict[str, Any]:
        """
        Get metrics produced by this transform.
        
        Returns:
            Dictionary of metric names and values
        """
        pass
    
class SchemaAware(ABC):
    """
    Interface for transforms that are aware of document schemas.
    
    Transforms implementing this interface can validate or transform
    documents based on their schema definitions.
    """
    
    @abstractmethod
    def set_schemas(self, schemas: Dict[str, Dict[str, Any]]):
        """
        Set schemas for this transform to use.
        
        Args:
            schemas: Dictionary of schema definitions
        """
        pass

class ConfigurableTransform(ABC):
    """
    Interface for transforms that can be configured dynamically.
    
    Transforms implementing this interface can accept configuration
    parameters at runtime.
    """
    
    @abstractmethod
    def configure(self, config: Dict[str, Any]):
        """
        Configure this transform with the provided parameters.
        
        Args:
            config: Configuration parameters
        """
        pass 