"""Resource monitoring and optimization for the MongoDB change stream connector."""

import os
import sys
import time
import asyncio
import psutil
import threading
import gc
from dataclasses import dataclass
from typing import Dict, List, Optional, Set, Callable, Any
import logging
from functools import wraps

from ..logging.logging_config import get_logger, add_context_to_logger
from ..utils.error_utils import with_retries, wrap_error, ErrorCategory, ConnectorError

@dataclass
class ResourceConfig:
    """Configuration for resource monitoring and optimization."""
    # Monitoring settings
    monitoring_interval: float = 10.0  # Seconds between resource checks
    log_interval: float = 60.0  # Seconds between detailed resource logs
    
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
    enable_object_tracking: bool = True  # Track object counts by type
    tracked_types: List[str] = None  # List of type names to track
    
    # Response actions
    enable_auto_response: bool = True  # Enable automatic resource optimization
    
    # Circuit breaker
    circuit_breaker_enabled: bool = True  # Enable circuit breaker for resource exhaustion
    circuit_open_time: float = 30.0  # Time in seconds to keep circuit open


class ResourceMonitor:
    """Monitors and optimizes resource usage for the MongoDB connector.
    
    Features:
    - Memory and CPU usage monitoring
    - Automatic garbage collection optimization
    - Object tracking for memory leak detection
    - Configurable thresholds and actions
    - Circuit breaker pattern for resource exhaustion
    - Detailed metrics and reporting
    """

    def __init__(self, config: ResourceConfig = None):
        """Initialize the resource monitor.
        
        Args:
            config: Resource monitoring configuration
        """
        self.config = config or ResourceConfig()
        self.logger = get_logger(__name__)
        self.logger = add_context_to_logger(
            self.logger,
            {
                "component": "resource_monitor",
                "service": "mongodb-connector"
            }
        )
        
        # Initialize state
        self.process = psutil.Process(os.getpid())
        self.running = False
        self.monitoring_task: Optional[asyncio.Task] = None
        self.last_log_time = 0
        self.last_gc_collection = 0
        
        # Initialize metrics
        self.metrics = {
            'cpu_percent': 0.0,
            'memory_percent': 0.0,
            'memory_mb': 0.0,
            'memory_rss_mb': 0.0,
            'gc_collections': [0, 0, 0],  # Count of collections by generation
            'thread_count': 0,
            'file_descriptors': 0,
            'start_time': time.time()
        }
        
        # Circuit breaker state
        self.circuit_open = False
        self.circuit_open_until = 0
        
        # Object tracking if enabled
        self.type_counts: Dict[str, int] = {}
        self.tracked_types: Set[str] = set(self.config.tracked_types or [])

        # Register optimizers
        self.optimizers = {
            'memory': [
                self._optimize_garbage_collection,
                self._optimize_object_cache,
                self._optimize_memory_allocator
            ],
            'cpu': [
                self._optimize_thread_pool,
                self._optimize_io_operations
            ]
        }
        
        # Register callbacks
        self.resource_callbacks: Dict[str, List[Callable]] = {
            'memory_warning': [],
            'memory_critical': [],
            'memory_emergency': [],
            'cpu_warning': [],
            'cpu_critical': [],
            'cpu_emergency': [],
            'metrics_updated': []
        }

    async def start(self) -> None:
        """Start the resource monitoring service."""
        if self.running:
            return
            
        self.running = True
        self.logger.info(
            "resource_monitoring_started",
            extra={
                "monitoring_interval": self.config.monitoring_interval,
                "log_interval": self.config.log_interval,
                "memory_thresholds": {
                    "warning": self.config.memory_warning_threshold,
                    "critical": self.config.memory_critical_threshold,
                    "emergency": self.config.memory_emergency_threshold
                },
                "cpu_thresholds": {
                    "warning": self.config.cpu_warning_threshold,
                    "critical": self.config.cpu_critical_threshold,
                    "emergency": self.config.cpu_emergency_threshold
                }
            }
        )
        
        # Start monitoring task
        self.monitoring_task = asyncio.create_task(self._monitor_resources())

    async def stop(self) -> None:
        """Stop the resource monitoring service."""
        if not self.running:
            return
            
        self.running = False
        if self.monitoring_task:
            self.monitoring_task.cancel()
            try:
                await self.monitoring_task
            except asyncio.CancelledError:
                pass
        
        self.logger.info("resource_monitoring_stopped")

    async def _monitor_resources(self) -> None:
        """Main monitoring loop for resources."""
        try:
            while self.running:
                # Update metrics
                await self._update_metrics()
                
                # Check thresholds and trigger events
                await self._check_thresholds()
                
                # Run optimizations if enabled
                if self.config.enable_auto_response:
                    await self._run_optimizations()
                
                # Log metrics if interval reached
                now = time.time()
                if now - self.last_log_time >= self.config.log_interval:
                    self._log_metrics()
                    self.last_log_time = now
                
                # Wait for next interval
                await asyncio.sleep(self.config.monitoring_interval)
        except asyncio.CancelledError:
            raise
        except Exception as e:
            wrapped_error = wrap_error(e, {
                "component": "resource_monitor",
                "operation": "monitor_resources"
            })
            self.logger.error(
                "resource_monitoring_error",
                extra={
                    "error": str(wrapped_error),
                    "category": wrapped_error.category.value
                }
            )
            raise

    async def _update_metrics(self) -> None:
        """Update all resource metrics."""
        try:
            # Update CPU metrics
            self.metrics['cpu_percent'] = self.process.cpu_percent(interval=0.1)
            
            # Update memory metrics
            memory_info = self.process.memory_info()
            self.metrics['memory_rss_mb'] = memory_info.rss / (1024 * 1024)  # RSS in MB
            self.metrics['memory_percent'] = self.process.memory_percent()
            
            # Update thread count
            self.metrics['thread_count'] = threading.active_count()
            
            # Update file descriptor count
            try:
                self.metrics['file_descriptors'] = self.process.num_fds()
            except AttributeError:
                # Not available on Windows
                pass
            
            # Update GC stats
            self.metrics['gc_collections'] = gc.get_count()
            
            # Update object tracking if enabled
            if self.config.enable_object_tracking:
                self._update_object_counts()
            
            # Notify metrics updated callbacks
            for callback in self.resource_callbacks.get('metrics_updated', []):
                try:
                    await callback(self.metrics)
                except Exception as e:
                    wrapped_error = wrap_error(e, {
                        "component": "resource_monitor",
                        "operation": "metrics_callback",
                        "callback": str(callback.__name__)
                    })
                    self.logger.error(
                        "metrics_callback_error",
                        extra={
                            "error": str(wrapped_error),
                            "category": wrapped_error.category.value
                        }
                    )
        except Exception as e:
            wrapped_error = wrap_error(e, {
                "component": "resource_monitor",
                "operation": "update_metrics"
            })
            self.logger.error(
                "metrics_update_error",
                extra={
                    "error": str(wrapped_error),
                    "category": wrapped_error.category.value
                }
            )

    def _update_object_counts(self) -> None:
        """Update counts of tracked object types."""
        if not self.config.enable_object_tracking:
            return
            
        # Reset counts
        self.type_counts = {}
        
        # Count objects by type
        for obj in gc.get_objects():
            try:
                obj_type = type(obj).__name__
                
                # Only track specified types if provided
                if not self.tracked_types or obj_type in self.tracked_types:
                    self.type_counts[obj_type] = self.type_counts.get(obj_type, 0) + 1
            except Exception:
                # Skip problematic objects
                pass

    async def _check_thresholds(self) -> None:
        """Check resource thresholds and trigger appropriate events."""
        # Check memory thresholds
        memory_percent = self.metrics['memory_percent']
        
        if memory_percent >= self.config.memory_emergency_threshold:
            await self._trigger_callbacks('memory_emergency')
            
            # Run emergency optimization
            if self.config.enable_auto_response:
                await self._emergency_memory_optimization()
                
        elif memory_percent >= self.config.memory_critical_threshold:
            await self._trigger_callbacks('memory_critical')
            
        elif memory_percent >= self.config.memory_warning_threshold:
            await self._trigger_callbacks('memory_warning')
        
        # Check CPU thresholds
        cpu_percent = self.metrics['cpu_percent']
        
        if cpu_percent >= self.config.cpu_emergency_threshold:
            await self._trigger_callbacks('cpu_emergency')
            
            # Run emergency optimization
            if self.config.enable_auto_response:
                await self._emergency_cpu_optimization()
                
        elif cpu_percent >= self.config.cpu_critical_threshold:
            await self._trigger_callbacks('cpu_critical')
            
        elif cpu_percent >= self.config.cpu_warning_threshold:
            await self._trigger_callbacks('cpu_warning')

    async def _trigger_callbacks(self, event_type: str) -> None:
        """Trigger callbacks for an event type.
        
        Args:
            event_type: The type of event (e.g., memory_warning)
        """
        for callback in self.resource_callbacks.get(event_type, []):
            try:
                await callback(self.metrics)
            except Exception as e:
                wrapped_error = wrap_error(e, {
                    "component": "resource_monitor",
                    "operation": "trigger_callback",
                    "event_type": event_type,
                    "callback": str(callback.__name__ if hasattr(callback, "__name__") else callback)
                })
                self.logger.error(
                    "callback_error",
                    extra={
                        "error": str(wrapped_error),
                        "category": wrapped_error.category.value,
                        "event_type": event_type
                    }
                )

    async def _run_optimizations(self) -> None:
        """Run resource optimizations based on current metrics."""
        memory_percent = self.metrics['memory_percent']
        cpu_percent = self.metrics['cpu_percent']
        
        # Run memory optimizations if under pressure
        if memory_percent >= self.config.memory_warning_threshold:
            optimization_level = 0  # Warning level
            if memory_percent >= self.config.memory_critical_threshold:
                optimization_level = 1  # Critical level
            if memory_percent >= self.config.memory_emergency_threshold:
                optimization_level = 2  # Emergency level
                
            await self._optimize_resource('memory', optimization_level)
        
        # Run CPU optimizations if under pressure
        if cpu_percent >= self.config.cpu_warning_threshold:
            optimization_level = 0  # Warning level
            if cpu_percent >= self.config.cpu_critical_threshold:
                optimization_level = 1  # Critical level
            if cpu_percent >= self.config.cpu_emergency_threshold:
                optimization_level = 2  # Emergency level
                
            await self._optimize_resource('cpu', optimization_level)

    async def _optimize_resource(self, resource_type: str, level: int) -> None:
        """Run optimizations for a specific resource type.
        
        Args:
            resource_type: Type of resource ('memory' or 'cpu')
            level: Optimization level (0=warning, 1=critical, 2=emergency)
        """
        optimizers = self.optimizers.get(resource_type, [])
        if not optimizers:
            return
            
        self.logger.info(
            "optimization_started",
            extra={
                "resource_type": resource_type,
                "level": level,
                "optimizer_count": len(optimizers)
            }
        )
        
        # Run each optimizer with the current level
        for optimizer in optimizers:
            try:
                await optimizer(level)
            except Exception as e:
                wrapped_error = wrap_error(e, {
                    "component": "resource_monitor",
                    "operation": "optimize_resource",
                    "resource_type": resource_type,
                    "optimizer": optimizer.__name__,
                    "level": level
                })
                self.logger.error(
                    "optimizer_error",
                    extra={
                        "error": str(wrapped_error),
                        "category": wrapped_error.category.value,
                        "optimizer": optimizer.__name__,
                        "resource_type": resource_type,
                        "level": level
                    }
                )

    async def _emergency_memory_optimization(self) -> None:
        """Run emergency memory optimization."""
        self.logger.warning(
            "emergency_memory_optimization",
            extra={
                "memory_percent": self.metrics['memory_percent'],
                "memory_mb": self.metrics['memory_rss_mb']
            }
        )
        
        # Force garbage collection
        gc.collect(2)  # Collect all generations
        
        # Open circuit breaker if enabled
        if self.config.circuit_breaker_enabled:
            self._open_circuit_breaker()

    async def _emergency_cpu_optimization(self) -> None:
        """Run emergency CPU optimization."""
        self.logger.warning(
            "emergency_cpu_optimization",
            extra={
                "cpu_percent": self.metrics['cpu_percent']
            }
        )
        
        # Open circuit breaker if enabled
        if self.config.circuit_breaker_enabled:
            self._open_circuit_breaker()

    def _open_circuit_breaker(self) -> None:
        """Open the circuit breaker to temporarily reject operations."""
        if self.circuit_open:
            return
            
        self.circuit_open = True
        self.circuit_open_until = time.time() + self.config.circuit_open_time
        
        self.logger.warning(
            "circuit_breaker_opened",
            extra={
                "open_duration": self.config.circuit_open_time,
                "memory_percent": self.metrics['memory_percent'],
                "cpu_percent": self.metrics['cpu_percent'],
                "open_until": self.circuit_open_until
            }
        )

    def check_circuit_breaker(self) -> bool:
        """Check if the circuit breaker is open.
        
        Returns:
            bool: True if operations should be allowed, False if rejected
        """
        # Allow operations if circuit breaker is disabled
        if not self.config.circuit_breaker_enabled:
            return True
            
        # Check if circuit is open
        if not self.circuit_open:
            return True
            
        # Check if circuit should be closed again
        now = time.time()
        if now >= self.circuit_open_until:
            self.circuit_open = False
            self.logger.info("circuit_breaker_closed")
            return True
            
        return False

    def circuit_breaker(self, func):
        """Decorator for functions that should respect the circuit breaker.
        
        Args:
            func: The function to decorate
            
        Returns:
            Callable: The decorated function
        """
        @wraps(func)
        async def wrapper(*args, **kwargs):
            if not self.check_circuit_breaker():
                error = ResourceExhaustionError("Circuit breaker open: resource exhaustion")
                wrapped_error = ConnectorError(
                    message=str(error),
                    category=ErrorCategory.RETRYABLE_RESOURCE,
                    context={
                        "component": "resource_monitor",
                        "operation": "circuit_breaker",
                        "memory_percent": self.metrics['memory_percent'],
                        "cpu_percent": self.metrics['cpu_percent']
                    }
                )
                self.logger.warning(
                    "operation_rejected_by_circuit_breaker",
                    extra={
                        "function": func.__name__,
                        "circuit_open_until": self.circuit_open_until,
                        "seconds_remaining": self.circuit_open_until - time.time()
                    }
                )
                raise wrapped_error
            return await func(*args, **kwargs)
        return wrapper

    def _log_metrics(self) -> None:
        """Log current resource metrics."""
        self.logger.info(
            "resource_metrics",
            extra={
                "cpu_percent": self.metrics['cpu_percent'],
                "memory_percent": self.metrics['memory_percent'],
                "memory_mb": self.metrics['memory_rss_mb'],
                "thread_count": self.metrics['thread_count'],
                "file_descriptors": self.metrics.get('file_descriptors', 0),
                "gc_collections": self.metrics['gc_collections'],
                "uptime_seconds": time.time() - self.metrics['start_time']
            }
        )
        
        # Log object counts if tracking enabled
        if self.config.enable_object_tracking and self.type_counts:
            # Find top 10 object types by count
            top_types = sorted(
                self.type_counts.items(),
                key=lambda x: x[1],
                reverse=True
            )[:10]
            
            self.logger.info(
                "object_count_metrics",
                extra={
                    "top_types": {t[0]: t[1] for t in top_types},
                    "tracked_type_count": len(self.type_counts)
                }
            )

    # Optimization implementations
    async def _optimize_garbage_collection(self, level: int) -> None:
        """Optimize garbage collection based on memory pressure.
        
        Args:
            level: Optimization level (0=warning, 1=critical, 2=emergency)
        """
        if not self.config.enable_gc_optimization:
            return
            
        # Run GC based on severity
        if level >= 2:  # Emergency
            # Full collection of all generations
            gc.collect(2)
            self.logger.info(
                "gc_optimization_emergency",
                extra={"generation": 2, "level": level}
            )
        elif level >= 1:  # Critical
            # Collect generation 1 and 0
            gc.collect(1)
            self.logger.info(
                "gc_optimization_critical", 
                extra={"generation": 1, "level": level}
            )
        elif level >= 0:  # Warning
            # Only run if we haven't had a collection recently
            collections_since_last = sum(gc.get_count()) - self.last_gc_collection
            if collections_since_last < 3:
                gc.collect(0)
                self.logger.info(
                    "gc_optimization_warning",
                    extra={"generation": 0, "level": level}
                )
                
        # Adjust GC thresholds based on memory pressure if enabled
        if self.config.gc_threshold_adjustment:
            current_thresholds = gc.get_threshold()
            
            if level >= 2:  # Emergency
                # Make GC very aggressive
                gc.set_threshold(100, 10, 5)
                self.logger.info(
                    "gc_thresholds_emergency",
                    extra={
                        "old_thresholds": current_thresholds,
                        "new_thresholds": (100, 10, 5)
                    }
                )
            elif level >= 1:  # Critical
                # Make GC somewhat aggressive
                gc.set_threshold(500, 50, 25)
                self.logger.info(
                    "gc_thresholds_critical",
                    extra={
                        "old_thresholds": current_thresholds,
                        "new_thresholds": (500, 50, 25)
                    }
                )
            elif current_thresholds != (700, 10, 10):  # Warning, restore defaults
                gc.set_threshold(700, 10, 10)
                self.logger.info(
                    "gc_thresholds_default",
                    extra={
                        "old_thresholds": current_thresholds,
                        "new_thresholds": (700, 10, 10)
                    }
                )
                
        # Update last GC collection count
        self.last_gc_collection = sum(gc.get_count())

    async def _optimize_object_cache(self, level: int) -> None:
        """Optimize memory usage by clearing object caches.
        
        Args:
            level: Optimization level
        """
        pass  # Placeholder for implementation

    async def _optimize_memory_allocator(self, level: int) -> None:
        """Optimize memory allocator behavior.
        
        Args:
            level: Optimization level
        """
        pass  # Placeholder for implementation

    async def _optimize_thread_pool(self, level: int) -> None:
        """Optimize thread pool settings based on CPU pressure.
        
        Args:
            level: Optimization level
        """
        pass  # Placeholder for implementation

    async def _optimize_io_operations(self, level: int) -> None:
        """Optimize I/O operations based on CPU pressure.
        
        Args:
            level: Optimization level
        """
        pass  # Placeholder for implementation

    # Public API
    def register_callback(self, event_type: str, callback: Callable) -> None:
        """Register a callback for resource events.
        
        Args:
            event_type: Event type ('memory_warning', 'cpu_critical', etc.)
            callback: Async callback function taking metrics dict as parameter
        """
        if event_type not in self.resource_callbacks:
            self.resource_callbacks[event_type] = []
        self.resource_callbacks[event_type].append(callback)
        
        self.logger.debug(
            "callback_registered",
            extra={
                "event_type": event_type,
                "callback": callback.__name__ if hasattr(callback, "__name__") else str(callback),
                "callback_count": len(self.resource_callbacks[event_type])
            }
        )
        
    def unregister_callback(self, event_type: str, callback: Callable) -> None:
        """Unregister a callback for resource events.
        
        Args:
            event_type: Event type
            callback: Callback to remove
        """
        if event_type in self.resource_callbacks:
            if callback in self.resource_callbacks[event_type]:
                self.resource_callbacks[event_type].remove(callback)
                self.logger.debug(
                    "callback_unregistered",
                    extra={
                        "event_type": event_type,
                        "callback": callback.__name__ if hasattr(callback, "__name__") else str(callback),
                        "callback_count": len(self.resource_callbacks[event_type])
                    }
                )

    def get_metrics(self) -> Dict[str, Any]:
        """Get current resource metrics.
        
        Returns:
            Dict[str, Any]: Current metrics
        """
        return self.metrics.copy()
        
    def add_tracked_type(self, type_name: str) -> None:
        """Add a type to track for object counting.
        
        Args:
            type_name: Name of the type to track
        """
        self.tracked_types.add(type_name)
        self.logger.debug(
            "tracked_type_added",
            extra={
                "type_name": type_name,
                "tracked_types_count": len(self.tracked_types)
            }
        )

    async def force_optimization(self, resource_type: str, level: int) -> None:
        """Force optimization for a specific resource type.
        
        Args:
            resource_type: Resource type ('memory' or 'cpu')
            level: Optimization level (0-2)
        """
        self.logger.info(
            "forced_optimization",
            extra={
                "resource_type": resource_type,
                "level": level
            }
        )
        await self._optimize_resource(resource_type, level)

    async def __aenter__(self) -> 'ResourceMonitor':
        """Context manager entry."""
        await self.start()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        """Context manager exit."""
        await self.stop()


class ResourceExhaustionError(Exception):
    """Exception raised when resource exhaustion is detected."""
    pass 