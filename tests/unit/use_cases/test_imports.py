"""
Test imports from the use_cases package.

This ensures that after moving code from src/pipeline/transforms/examples
to the use_cases directory, imports continue to work correctly.
"""

import unittest


class ImportTests(unittest.TestCase):
    """Test case for verifying imports from the use_cases package."""
    
    def test_import_stock_monitoring(self):
        """Test that StockPositionMonitor can be imported from use_cases."""
        try:
            from use_cases.stock_monitoring import StockPositionMonitor
            self.assertTrue(True, "Successfully imported StockPositionMonitor")
        except ImportError as e:
            self.fail(f"Failed to import StockPositionMonitor: {e}")
    
    def test_instantiate_stock_monitor(self):
        """Test that StockPositionMonitor can be instantiated."""
        from use_cases.stock_monitoring import StockPositionMonitor
        
        # Simple threshold configuration
        thresholds = [
            {"site": "test_site", "stock": "TEST", "limit": 100}
        ]
        
        # Instantiate the transform
        try:
            monitor = StockPositionMonitor(thresholds=thresholds, window_size=60)
            self.assertIsNotNone(monitor, "StockPositionMonitor was instantiated")
        except Exception as e:
            self.fail(f"Failed to instantiate StockPositionMonitor: {e}")


if __name__ == "__main__":
    unittest.main() 