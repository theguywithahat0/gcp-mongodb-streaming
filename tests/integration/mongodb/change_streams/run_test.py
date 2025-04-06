#!/usr/bin/env python3
"""
Integration test runner for MongoDB change streams.
Coordinates test data generation and stream monitoring, then generates a report.
"""

import os
import sys
import json
import asyncio
import logging
import datetime
import subprocess
from datetime import UTC
from pathlib import Path
from typing import Dict, List, Any, Tuple
from dataclasses import dataclass, asdict
from dotenv import load_dotenv

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Add the project root to the Python path
current_dir = Path(__file__).resolve().parent
project_root = current_dir.parent.parent.parent.parent
sys.path.append(str(project_root))

# Add the integration tests directory to the Python path for fixtures
integration_dir = current_dir.parent.parent
sys.path.append(str(integration_dir))

# Load environment variables from .env.integration
env_path = project_root / 'tests' / 'config' / '.env.integration'
if not env_path.exists():
    raise FileNotFoundError(
        f"Integration test environment file not found at {env_path}. "
        "Please ensure .env.integration exists in the tests/config directory."
    )
load_dotenv(str(env_path))

@dataclass
class TestConfig:
    """Test configuration parameters."""
    mongodb_uri: str = os.getenv('MONGODB_TEST_URI')
    database: str = os.getenv('MONGODB_TEST_DB', 'test')
    collection: str = os.getenv('MONGODB_TEST_COLLECTION', 'stream_test')
    duration: int = int(os.getenv('TEST_DURATION', '30'))
    write_interval: int = int(os.getenv('TEST_WRITE_INTERVAL', '2'))
    min_expected_docs: int = int(os.getenv('TEST_MIN_DOCS', '10'))

@dataclass
class TestResult:
    """Test result data."""
    start_time: datetime.datetime
    end_time: datetime.datetime
    duration: float
    documents_generated: int
    documents_processed: int
    success: bool
    error_message: str = None

    def to_dict(self):
        """Convert the test result to a dictionary with serializable values."""
        return {
            "start_time": self.start_time.isoformat(),
            "end_time": self.end_time.isoformat(),
            "duration": self.duration,
            "documents_generated": self.documents_generated,
            "documents_processed": self.documents_processed,
            "success": self.success,
            "error_message": self.error_message
        }

class IntegrationTestRunner:
    def __init__(self, config: TestConfig):
        self.config = config
        self.result = None
        self.monitor_process = None
        self.generator_process = None

    async def run_test(self) -> TestResult:
        """Run the integration test suite."""
        start_time = datetime.datetime.now(UTC)
        documents_generated = 0
        documents_processed = 0
        error_message = None

        try:
            # Start the change stream monitor
            monitor_process = await self._start_monitor()
            await asyncio.sleep(2)  # Give monitor time to initialize

            # Start the test data generator
            generator_process = await self._start_generator()

            # Wait for processes to complete with timeout
            monitor_task = asyncio.create_task(self._wait_for_process(monitor_process))
            generator_task = asyncio.create_task(self._wait_for_process(generator_process))

            try:
                # Wait for both tasks with timeout
                monitor_result, generator_result = await asyncio.wait_for(
                    asyncio.gather(monitor_task, generator_task),
                    timeout=self.config.duration + 10
                )
                monitor_output, monitor_error = monitor_result
                generator_output, generator_error = generator_result

                # Process outputs
                documents_processed = self._extract_document_count(monitor_output + monitor_error)
                documents_generated = self._extract_document_count(generator_output + generator_error)

                # Verify results
                if documents_generated < self.config.min_expected_docs or documents_processed < self.config.min_expected_docs:
                    error_message = (
                        f"Insufficient documents processed. "
                        f"Generated: {documents_generated}, "
                        f"Processed: {documents_processed}, "
                        f"Expected minimum: {self.config.min_expected_docs}"
                    )

            except asyncio.TimeoutError:
                error_message = f"Test timed out after {self.config.duration + 10} seconds"
                self._cleanup_processes()

        except Exception as e:
            error_message = str(e)
            logger.error(f"Test failed: {error_message}")
        finally:
            self._cleanup_processes()

        end_time = datetime.datetime.now(UTC)
        duration = (end_time - start_time).total_seconds()

        result = TestResult(
            start_time=start_time,
            end_time=end_time,
            duration=duration,
            documents_generated=documents_generated,
            documents_processed=documents_processed,
            success=error_message is None,
            error_message=error_message
        )

        # Generate test report
        self._save_report(result)
        return result

    def _cleanup_processes(self):
        """Clean up any running processes."""
        for process in [self.monitor_process, self.generator_process]:
            if process and process.returncode is None:
                try:
                    process.terminate()
                    process.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait()

    async def _start_monitor(self):
        """Start the change stream monitor process."""
        monitor_script = os.path.join(os.path.dirname(__file__), 'stream_monitor.py')
        monitor_env = os.environ.copy()
        
        self.monitor_process = await asyncio.create_subprocess_exec(
            sys.executable,
            monitor_script,
            env=monitor_env,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE
        )
        return self.monitor_process

    async def _start_generator(self):
        """Start the test data generator process."""
        generator_script = os.path.join(os.path.dirname(__file__), 'data_generator.py')
        generator_env = os.environ.copy()
        
        self.generator_process = await asyncio.create_subprocess_exec(
            sys.executable,
            generator_script,
            env=generator_env,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE
        )
        return self.generator_process

    async def _wait_for_process(self, process) -> Tuple[str, str]:
        """Wait for a process to complete and return its output."""
        stdout, stderr = await process.communicate()
        return stdout.decode(), stderr.decode()

    def _extract_document_count(self, output: str) -> int:
        """Extract the number of documents processed from process output."""
        for line in output.splitlines():
            if "Test completed. Generated" in line:
                try:
                    return int(line.split()[-2])
                except (IndexError, ValueError):
                    continue
            if "Monitor completed. Processed" in line:
                try:
                    return int(line.split()[-2])
                except (IndexError, ValueError):
                    continue
        return 0

    def _save_report(self, result: TestResult):
        """Save the test report to files."""
        report = {
            "test_config": asdict(self.config),
            "test_result": result.to_dict(),
            "summary": {
                "status": "PASS" if result.success else "FAIL",
                "duration_seconds": result.duration,
                "documents": {
                    "generated": result.documents_generated,
                    "processed": result.documents_processed,
                    "expected_minimum": self.config.min_expected_docs
                }
            }
        }
        
        # Ensure reports directory exists
        reports_dir = current_dir.parent / 'reports'
        reports_dir.mkdir(exist_ok=True)
        
        # Save both latest and timestamped reports
        report_path = reports_dir / 'latest.json'
        timestamp = result.start_time.strftime('%Y%m%d_%H%M%S')
        timestamped_path = reports_dir / f'report_{timestamp}.json'
        
        for path in [report_path, timestamped_path]:
            with open(path, 'w') as f:
                json.dump(report, f, indent=2, default=str)
        
        logger.info(f"Test reports written to {reports_dir}/")

def main():
    """Main entry point."""
    config = TestConfig()
    runner = IntegrationTestRunner(config)
    
    try:
        result = asyncio.run(runner.run_test())
        sys.exit(0 if result.success else 1)
    except KeyboardInterrupt:
        logger.info("Test interrupted by user")
        sys.exit(130)
    except Exception as e:
        logger.error(f"Test failed: {str(e)}")
        sys.exit(1)

if __name__ == '__main__':
    main() 