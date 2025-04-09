#!/usr/bin/env python3
"""
Integration Test Runner for Stock Position Monitoring

This script coordinates the execution of the stock monitoring integration test,
including:
1. Generating stock trade data
2. Running the pipeline with the StockPositionMonitor transform
3. Monitoring the Pub/Sub output
4. Analyzing the results and reporting success/failure
"""

import os
import sys
import json
import asyncio
import logging
import subprocess
import time
import argparse
import datetime
from pathlib import Path
from typing import Dict, Any, Optional
from dataclasses import dataclass
from dotenv import load_dotenv

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Add project root to the Python path
current_dir = Path(__file__).resolve().parent
project_root = current_dir.parent.parent.parent.parent
sys.path.append(str(project_root))

# Load environment variables
env_path = project_root / 'tests' / '.env.test'
if not env_path.exists():
    raise FileNotFoundError(f"Test environment file not found at {env_path}")
load_dotenv(str(env_path))

# Default test configuration
DEFAULT_TIMEOUT = int(os.getenv('TEST_DURATION', '60'))
PIPELINE_RUNNER = os.getenv('PIPELINE_RUNNER', 'DirectRunner')
GCP_PROJECT_ID = os.getenv('GCP_PROJECT_ID')
PUBSUB_TOPIC = os.getenv('PUBSUB_TOPIC')
MIN_EXPECTED_DOCS = int(os.getenv('TEST_MIN_DOCS', '50'))
POSITION_THRESHOLDS = os.getenv('TEST_POSITION_THRESHOLDS')
WINDOW_SIZE = int(os.getenv('TEST_WINDOW_SIZE', '10'))

@dataclass
class TestResult:
    """Test result data."""
    start_time: datetime.datetime
    end_time: datetime.datetime
    duration: float
    documents_generated: int
    documents_processed: int
    alerts_generated: int
    success: bool
    error_message: Optional[str] = None

    def to_dict(self):
        """Convert test result to dictionary for JSON serialization."""
        return {
            "start_time": self.start_time.isoformat(),
            "end_time": self.end_time.isoformat(),
            "duration": self.duration,
            "documents_generated": self.documents_generated,
            "documents_processed": self.documents_processed,
            "alerts_generated": self.alerts_generated,
            "success": self.success,
            "error_message": self.error_message
        }

class IntegrationTestRunner:
    """
    Runner for the Stock Position Monitoring integration test.
    
    Coordinates the execution of the data generator, pipeline, and monitor.
    """
    
    def __init__(self, timeout: int = DEFAULT_TIMEOUT):
        """Initialize the test runner."""
        self.timeout = timeout
        self.reports_dir = current_dir.parent / 'reports'
        self.reports_dir.mkdir(exist_ok=True)
        self.processes = []
        self.result = None
    
    async def _read_stream(self, stream, prefix=""):
        """Read from a stream and log output with a prefix."""
        while True:
            try:
                line = await stream.readline()
                if not line:  # EOF
                    break
                text = line.decode().strip()
                if text:
                    logger.info(f"{prefix}: {text}")
            except Exception as e:
                logger.error(f"Error reading stream: {e}")
                break
    
    async def _monitor_process(self, process, name):
        """Monitor a subprocess's stdout and stderr."""
        try:
            await asyncio.gather(
                self._read_stream(process.stdout, f"{name} [stdout]"),
                self._read_stream(process.stderr, f"{name} [stderr]")
            )
        except Exception as e:
            logger.error(f"Error monitoring {name} process: {e}")
    
    async def _run_process(self, cmd, name, cwd=None):
        """Run a subprocess and monitor its output."""
        process = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            cwd=cwd
        )
        
        self.processes.append((process, name))
        monitor_task = asyncio.create_task(self._monitor_process(process, name))
        
        return process, monitor_task
    
    async def _run_generator(self):
        """Run the stock trade generator."""
        logger.info("Starting stock trade generator...")
        
        cmd = [
            sys.executable,
            str(current_dir / 'stock_trade_generator.py')
        ]
        
        generator_process, monitor_task = await self._run_process(cmd, "GENERATOR")
        
        # Wait for generator to complete
        await generator_process.wait()
        exit_code = generator_process.returncode
        
        if exit_code != 0:
            logger.error(f"Generator process failed with exit code {exit_code}")
            raise RuntimeError(f"Generator process failed with exit code {exit_code}")
        
        logger.info("Trade generator completed successfully")
        
        # Read generation details to get document count
        generation_details_file = self.reports_dir / 'trade_generation_details.json'
        try:
            with open(generation_details_file) as f:
                generation_details = json.load(f)
                return len(generation_details)
        except (FileNotFoundError, json.JSONDecodeError) as e:
            logger.error(f"Error reading generator output: {e}")
            return 0
    
    async def _run_pipeline(self):
        """Run the pipeline with the StockPositionMonitor transform."""
        logger.info("Starting pipeline with StockPositionMonitor transform...")
        
        # Create a temprary config file for the stock monitor
        config_path = current_dir / 'temp_stock_monitor_config.json'
        pipeline_config = {
            "transforms": [
                {
                    "name": "StockPositionMonitor",
                    "module": "pipeline.transforms.examples.stock_monitoring",
                    "enabled": True,
                    "config": {
                        "thresholds": json.loads(POSITION_THRESHOLDS)["thresholds"],
                        "window_size": WINDOW_SIZE
                    }
                }
            ]
        }
        
        with open(config_path, 'w') as f:
            json.dump(pipeline_config, f, indent=2)
        
        # Run the pipeline
        cmd = [
            sys.executable,
            str(project_root / 'src' / 'scripts' / 'run_pipeline.py'),
            '--project', GCP_PROJECT_ID,
            '--runner', PIPELINE_RUNNER,
            '--transform_config', str(config_path),
            '--streaming'
        ]
        
        pipeline_process, monitor_task = await self._run_process(
            cmd, "PIPELINE", cwd=str(project_root)
        )
        
        # We'll let the pipeline run in the background and stop it later
        logger.info("Pipeline started")
        return pipeline_process
    
    async def _run_monitor(self):
        """Run the Pub/Sub monitor to capture pipeline output."""
        logger.info("Starting Pub/Sub monitor...")
        
        cmd = [
            sys.executable,
            str(current_dir / 'pubsub_monitor.py')
        ]
        
        monitor_process, monitor_task = await self._run_process(cmd, "MONITOR")
        
        # Wait for monitor to complete
        await monitor_process.wait()
        exit_code = monitor_process.returncode
        
        if exit_code != 0:
            logger.error(f"Monitor process failed with exit code {exit_code}")
            raise RuntimeError(f"Monitor process failed with exit code {exit_code}")
        
        logger.info("Monitor completed successfully")
        
        # Read monitor results
        monitor_results_file = self.reports_dir / 'pubsub_monitor_results.json'
        try:
            with open(monitor_results_file) as f:
                monitor_results = json.load(f)
                return monitor_results
        except (FileNotFoundError, json.JSONDecodeError) as e:
            logger.error(f"Error reading monitor results: {e}")
            return {"total_messages": 0, "alert_messages": 0}
    
    def _cleanup_processes(self):
        """Clean up all subprocesses."""
        for process, name in self.processes:
            if process.returncode is None:
                try:
                    logger.info(f"Terminating {name} process...")
                    process.terminate()
                except Exception as e:
                    logger.error(f"Error terminating {name} process: {e}")
                    try:
                        process.kill()
                    except:
                        pass
    
    async def run_test(self) -> TestResult:
        """Run the full integration test."""
        start_time = datetime.datetime.now(datetime.UTC)
        documents_generated = 0
        documents_processed = 0
        alerts_generated = 0
        error_message = None
        
        try:
            # Step 1: Generate test data
            documents_generated = await self._run_generator()
            logger.info(f"Generated {documents_generated} test documents")
            
            # Step 2: Start the pipeline
            pipeline_process = await self._run_pipeline()
            logger.info("Pipeline running, waiting for processing...")
            
            # Give the pipeline a moment to initialize
            await asyncio.sleep(5)
            
            # Step 3: Monitor Pub/Sub output
            monitor_results = await self._run_monitor()
            documents_processed = monitor_results.get('total_messages', 0)
            alerts_generated = monitor_results.get('alert_messages', 0)
            
            logger.info(f"Processed {documents_processed} documents, generated {alerts_generated} alerts")
            
            # Step 4: Verify results
            if documents_processed < MIN_EXPECTED_DOCS:
                error_message = (
                    f"Too few documents processed: {documents_processed} "
                    f"(expected at least {MIN_EXPECTED_DOCS})"
                )
                logger.error(error_message)
            elif alerts_generated == 0:
                error_message = "No alerts were generated, threshold logic may be incorrect"
                logger.error(error_message)
            
        except Exception as e:
            error_message = str(e)
            logger.error(f"Test failed: {error_message}")
        finally:
            # Clean up
            self._cleanup_processes()
        
        end_time = datetime.datetime.now(datetime.UTC)
        duration = (end_time - start_time).total_seconds()
        
        # Create test result
        result = TestResult(
            start_time=start_time,
            end_time=end_time,
            duration=duration,
            documents_generated=documents_generated,
            documents_processed=documents_processed,
            alerts_generated=alerts_generated,
            success=error_message is None,
            error_message=error_message
        )
        
        # Save test result
        with open(self.reports_dir / 'integration_test_result.json', 'w') as f:
            json.dump(result.to_dict(), f, indent=2)
        
        if result.success:
            logger.info(f"Test PASSED in {duration:.1f} seconds")
        else:
            logger.error(f"Test FAILED in {duration:.1f} seconds: {result.error_message}")
        
        self.result = result
        return result

async def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(description='Run stock position monitoring integration test')
    parser.add_argument('--timeout', type=int, default=DEFAULT_TIMEOUT,
                        help='Test timeout in seconds')
    args = parser.parse_args()
    
    if not GCP_PROJECT_ID:
        raise ValueError("GCP_PROJECT_ID environment variable is required")
    
    runner = IntegrationTestRunner(timeout=args.timeout)
    
    try:
        result = await runner.run_test()
        sys.exit(0 if result.success else 1)
    except KeyboardInterrupt:
        logger.info("Test interrupted by user")
        runner._cleanup_processes()
        sys.exit(2)
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        runner._cleanup_processes()
        sys.exit(3)

if __name__ == '__main__':
    asyncio.run(main()) 