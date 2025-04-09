"""
StockPositionMonitor - A transform for monitoring stock positions from trading data.

This transform monitors and aggregates stock trading activity to track positions
and generate alerts when position thresholds are exceeded.
"""

import logging
import time
from typing import Dict, List, Any, Optional, Tuple

import apache_beam as beam
from apache_beam.metrics import Metrics
from apache_beam.transforms import window
from apache_beam import pvalue


class StockPositionMonitor(beam.PTransform):
    """
    A beam transform that monitors stock positions from a stream of trade data.
    
    The transform performs the following operations:
    1. Filters for valid stock trade documents.
    2. Groups trade events by site and stock within a sliding time window.
    3. Calculates net positions based on buy/sell activities.
    4. Compares positions against thresholds to generate alerts.
    
    Args:
        thresholds: List of threshold configurations with site, stock, and limit values.
        window_size: Time window size in seconds for position calculation.
    """
    
    def __init__(self, thresholds: List[Dict[str, Any]], window_size: int = 300):
        """Initialize the transform with thresholds and window configuration."""
        super().__init__()
        self.thresholds = thresholds
        self.window_size = window_size
        
        # Create a lookup dictionary for faster threshold checking
        self.threshold_map = {}
        for threshold in self.thresholds:
            key = f"{threshold['site']}:{threshold['stock']}"
            self.threshold_map[key] = threshold['limit']
        
        # Metrics for monitoring the transform's performance
        self.documents_processed_counter = Metrics.counter(
            self.__class__.__name__, 'documents_processed')
        self.valid_trades_counter = Metrics.counter(
            self.__class__.__name__, 'valid_trades')
        self.positions_monitored_counter = Metrics.counter(
            self.__class__.__name__, 'positions_monitored')
        self.alerts_generated_counter = Metrics.counter(
            self.__class__.__name__, 'alerts_generated')
    
    def expand(self, input_pcoll):
        """Process the input PCollection and produce position and alert outputs."""
        
        # Filter valid trade documents
        trades = (
            input_pcoll
            | "Count All Documents" >> beam.Map(self._count_document)
            | "Filter Valid Trades" >> beam.Filter(self._is_valid_trade)
            | "Format Trade Data" >> beam.Map(self._format_trade)
        )
        
        # Apply windowing to group trades in time windows
        windowed_trades = (
            trades
            | "Apply Sliding Window" >> beam.WindowInto(
                window.SlidingWindows(self.window_size, self.window_size // 2),
                timestamp_fn=lambda x: self._get_timestamp(x)
            )
        )
        
        # Group by site and stock
        grouped_trades = (
            windowed_trades
            | "Group By Site and Stock" >> beam.GroupBy(
                lambda x: (x['site'], x['stock'])
            )
            .aggregate_field(
                lambda x: x['quantity'] * (1 if x['operation'] == 'buy' else -1),
                sum,
                'position'
            )
        )
        
        # Calculate positions and check thresholds
        positions = (
            grouped_trades
            | "Count Positions" >> beam.Map(self._count_position)
            | "Add Window Info" >> beam.Map(self._add_window_info)
        )
        
        # Generate alerts for positions exceeding thresholds
        alerts = (
            positions
            | "Check Thresholds" >> beam.Map(self._check_threshold)
            | "Filter Alerts" >> beam.Filter(lambda x: x.get('alert', False))
            | "Count Alerts" >> beam.Map(self._count_alert)
        )
        
        # Return combined positions and alerts
        return (positions, alerts) | "Combine Results" >> beam.Flatten()
    
    def _is_valid_trade(self, document: Dict[str, Any]) -> bool:
        """Check if the document is a valid stock trade."""
        is_valid = (
            isinstance(document, dict) and
            'site' in document and
            'symbol' in document and
            'quantity' in document and
            'operation' in document and
            document.get('operation') in ['buy', 'sell']
        )
        
        if is_valid:
            self.valid_trades_counter.inc()
        
        return is_valid
    
    def _format_trade(self, trade: Dict[str, Any]) -> Dict[str, Any]:
        """Standardize trade document format."""
        return {
            'site': trade['site'],
            'stock': trade['symbol'],
            'quantity': trade['quantity'],
            'operation': trade['operation'],
            'timestamp': trade.get('timestamp', time.time())
        }
    
    def _get_timestamp(self, trade: Dict[str, Any]) -> float:
        """Extract timestamp from trade data for windowing."""
        timestamp_str = trade.get('timestamp')
        if isinstance(timestamp_str, str):
            try:
                # Convert ISO string to Unix timestamp for beam windowing
                from datetime import datetime
                dt = datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))
                return dt.timestamp()
            except (ValueError, TypeError):
                logging.warning(f"Invalid timestamp format: {timestamp_str}")
                return time.time()
        return float(timestamp_str) if timestamp_str else time.time()
    
    def _count_document(self, document: Dict[str, Any]) -> Dict[str, Any]:
        """Count all processed documents for metrics."""
        self.documents_processed_counter.inc()
        return document
    
    def _count_position(self, position: Dict[str, Any]) -> Dict[str, Any]:
        """Count monitored positions for metrics."""
        self.positions_monitored_counter.inc()
        return position
    
    def _count_alert(self, alert: Dict[str, Any]) -> Dict[str, Any]:
        """Count generated alerts for metrics."""
        self.alerts_generated_counter.inc()
        return alert
    
    def _add_window_info(self, position: Dict[str, Any]) -> Dict[str, Any]:
        """Add window end time information to position data."""
        from apache_beam.transforms.window import TimestampedValue
        from datetime import datetime
        
        # Get window end time from beam's window context if available
        try:
            window_end = beam.window.WindowFn.current_window().end.to_utc_datetime()
            position['window_end'] = window_end.isoformat() + 'Z'
        except (AttributeError, RuntimeError):
            # If not available (e.g., in tests), use current time
            position['window_end'] = datetime.utcnow().isoformat() + 'Z'
            
        return position
    
    def _check_threshold(self, position: Dict[str, Any]) -> Dict[str, Any]:
        """Check if a position exceeds its threshold and generate alert if needed."""
        site = position.get('site')
        stock = position.get('stock')
        pos_value = position.get('position', 0)
        
        key = f"{site}:{stock}"
        threshold = self.threshold_map.get(key)
        
        if threshold and abs(pos_value) > threshold:
            return {
                **position,
                'threshold': threshold,
                'alert': True
            }
        
        return position
    
    def get_metrics(self) -> Dict[str, int]:
        """Retrieve metrics collected by the transform."""
        # Note: In a real deployment, we would access the metrics through the monitoring system.
        # This method is primarily for testing purposes.
        total_trades = self.valid_trades_counter.value
        total_alerts = self.alerts_generated_counter.value
        
        alert_rate = total_alerts / total_trades if total_trades > 0 else 0
        
        return {
            'documents_processed': self.documents_processed_counter.value,
            'valid_trades': total_trades,
            'positions_monitored': self.positions_monitored_counter.value,
            'alerts_generated': total_alerts,
            'alert_rate': alert_rate
        }