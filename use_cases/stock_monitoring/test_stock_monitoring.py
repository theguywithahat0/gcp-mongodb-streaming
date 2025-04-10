"""Tests for the StockPositionMonitor transform."""

import unittest
import apache_beam as beam
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that, equal_to

from use_cases.stock_monitoring.stock_monitoring import StockPositionMonitor


class StockPositionMonitorTest(unittest.TestCase):
    """Test cases for the StockPositionMonitor transform."""

    def test_position_calculation(self):
        """Test the transform's ability to calculate stock positions correctly."""
        # Sample input data for positions calculation
        input_data = [
            # Fidelity trades for AAPL
            {"site": "fidelity", "symbol": "AAPL", "quantity": 100, "operation": "buy", "timestamp": "2023-05-01T10:00:00Z"},
            {"site": "fidelity", "symbol": "AAPL", "quantity": 50, "operation": "buy", "timestamp": "2023-05-01T10:05:00Z"},
            {"site": "fidelity", "symbol": "AAPL", "quantity": 25, "operation": "sell", "timestamp": "2023-05-01T10:10:00Z"},
            
            # ETrade trades for GOOGL
            {"site": "etrade", "symbol": "GOOGL", "quantity": 200, "operation": "buy", "timestamp": "2023-05-01T10:02:00Z"},
            {"site": "etrade", "symbol": "GOOGL", "quantity": 300, "operation": "buy", "timestamp": "2023-05-01T10:07:00Z"},
            
            # Non-stock document to be filtered out
            {"type": "system_log", "message": "Database maintenance", "timestamp": "2023-05-01T10:15:00Z"},
        ]

        # Expected net positions (after windowing and grouping)
        expected_positions = [
            {"site": "fidelity", "stock": "AAPL", "position": 125, "window_end": "2023-05-01T10:15:00Z"},
            {"site": "etrade", "stock": "GOOGL", "position": 500, "window_end": "2023-05-01T10:15:00Z"},
        ]
        
        # Configure thresholds to trigger alerts for GOOGL but not AAPL
        thresholds = [
            {"site": "fidelity", "stock": "AAPL", "limit": 500},
            {"site": "etrade", "stock": "GOOGL", "limit": 400},
        ]

        # The alert should only be triggered for GOOGL which exceeds its threshold
        expected_alerts = [
            {
                "site": "etrade", 
                "stock": "GOOGL", 
                "position": 500, 
                "threshold": 400, 
                "alert": True, 
                "window_end": "2023-05-01T10:15:00Z"
            }
        ]

        # Create and run test pipeline
        with TestPipeline() as p:
            # Create PCollection from input data
            input_pcoll = p | "Create Input" >> beam.Create(input_data)
            
            # Apply the transform
            monitor = StockPositionMonitor(thresholds=thresholds, window_size=900)
            results = input_pcoll | "Monitor Stock Positions" >> monitor
            
            # Split results into positions and alerts
            positions = results | "Filter Positions" >> beam.Filter(
                lambda x: "alert" not in x or not x["alert"]
            )
            
            alerts = results | "Filter Alerts" >> beam.Filter(
                lambda x: "alert" in x and x["alert"]
            )
            
            # Assert the expected outputs
            assert_that(positions, equal_to(expected_positions), label="CheckPositions")
            assert_that(alerts, equal_to(expected_alerts), label="CheckAlerts")
    
    def test_metrics(self):
        """Test that metrics are accurately tracked."""
        input_data = [
            {"site": "fidelity", "symbol": "AAPL", "quantity": 100, "operation": "buy", "timestamp": "2023-05-01T10:00:00Z"},
            {"site": "etrade", "symbol": "GOOGL", "quantity": 600, "operation": "buy", "timestamp": "2023-05-01T10:02:00Z"},
            {"type": "system_log", "message": "Database maintenance", "timestamp": "2023-05-01T10:15:00Z"},
        ]
        
        thresholds = [
            {"site": "etrade", "stock": "GOOGL", "limit": 500},
        ]
        
        with TestPipeline() as p:
            input_pcoll = p | "Create Input" >> beam.Create(input_data)
            monitor = StockPositionMonitor(thresholds=thresholds, window_size=900)
            _ = input_pcoll | "Monitor Stock Positions" >> monitor
            
            # Pipeline will run and metrics will be collected
            
        # Get metrics after pipeline execution
        metrics = monitor.get_metrics()
        
        # Verify metrics are populated
        self.assertEqual(metrics["documents_processed"], 3)  # All documents counted
        self.assertEqual(metrics["valid_trades"], 2)  # Two stock trades
        self.assertEqual(metrics["positions_monitored"], 2)  # Two positions monitored
        self.assertEqual(metrics["alerts_generated"], 1)  # One alert for GOOGL
        self.assertGreaterEqual(metrics["alert_rate"], 0.3)  # At least 30% alert rate


if __name__ == "__main__":
    unittest.main() 