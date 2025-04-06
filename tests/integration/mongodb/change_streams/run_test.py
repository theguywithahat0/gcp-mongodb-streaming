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

def setup_logging():
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s [%(levelname)s] %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    # Set MongoDB driver logging to WARNING level to reduce noise
    logging.getLogger('mongodb').setLevel(logging.WARNING)
    logging.getLogger('pymongo').setLevel(logging.WARNING)
    logging.getLogger('motor').setLevel(logging.WARNING)

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
        self.logger = logging.getLogger(__name__)
        setup_logging()

    async def _read_stream(self, stream, prefix=""):
        """Read from a stream and log output with a prefix"""
        try:
            # Use a single reader for the stream
            reader = asyncio.StreamReader()
            protocol = asyncio.StreamReaderProtocol(reader)
            await asyncio.get_event_loop().connect_read_pipe(lambda: protocol, stream)
            
            while True:
                try:
                    line = await reader.readline()
                    if not line:  # EOF
                        break
                    text = line.decode().strip()
                    if text:
                        self.logger.debug(f"{prefix}: {text}")
                except Exception as e:
                    self.logger.error(f"Error decoding output: {e}")
                    break
        finally:
            # Clean up the protocol
            protocol.connection_lost(None)

    async def _monitor_output(self, process, name):
        """Monitor the output of a process"""
        try:
            # Use a single task to read both stdout and stderr
            await asyncio.gather(
                self._read_stream(process.stdout, f"{name} [stdout]"),
                self._read_stream(process.stderr, f"{name} [stderr]")
            )
        except Exception as e:
            self.logger.error(f"Error monitoring {name} output: {e}")

    async def run_test(self) -> TestResult:
        """Run the integration test suite."""
        start_time = datetime.datetime.now(UTC)
        documents_generated = 0
        documents_processed = 0
        error_message = None

        try:
            # Start the change stream monitor with extra time to catch all documents
            monitor_process = await self._start_monitor()
            self.logger.info("Waiting for monitor to initialize...")
            
            # Start monitoring output
            monitor_task = asyncio.create_task(self._monitor_output(monitor_process, "MONITOR"))
            
            await asyncio.sleep(3)  # Give monitor time to initialize

            # Start the test data generator
            generator_process = await self._start_generator()
            generator_task = asyncio.create_task(self._monitor_output(generator_process, "GENERATOR"))

            # Wait for generator to complete
            generator_output, generator_error = await self._wait_for_process(generator_process)
            documents_generated = self._extract_document_count(generator_output + generator_error)
            self.logger.info(f"Generator completed. Generated {documents_generated} documents")

            # Read generation details to find last document timestamp
            reports_dir = Path(__file__).parent.parent / 'reports'
            generation_details_file = reports_dir / 'generation_details.json'
            try:
                with open(generation_details_file) as f:
                    generation_details = json.load(f)
                if generation_details:
                    # Parse timestamp and ensure it's timezone-aware
                    last_generated_str = generation_details[-1]['generated_at']
                    # If the timestamp doesn't have timezone info, assume UTC
                    if 'Z' not in last_generated_str and '+' not in last_generated_str:
                        last_generated_str += 'Z'
                    last_generated = datetime.datetime.fromisoformat(last_generated_str.replace('Z', '+00:00'))
                    now = datetime.datetime.now(UTC)
                    # Calculate how long to wait from now to ensure we're 15 seconds past the last document
                    wait_time = max(0, 15 - (now - last_generated).total_seconds())
                    if wait_time > 0:
                        self.logger.info(f"Waiting {wait_time:.1f} seconds from last document generation...")
                        await asyncio.sleep(wait_time)
            except Exception as e:
                self.logger.warning(f"Could not read generation details, using default grace period: {e}")
                # Fallback to fixed grace period
                grace_period = 15
                self.logger.info(f"Using fallback grace period of {grace_period} seconds...")
                await asyncio.sleep(grace_period)

            # Wait for monitor to complete
            monitor_output, monitor_error = await self._wait_for_process(monitor_process)
            documents_processed = self._extract_document_count(monitor_output + monitor_error)
            self.logger.info(f"Monitor completed. Processed {documents_processed} documents")

            # Verify results
            if documents_generated < self.config.min_expected_docs or documents_processed < self.config.min_expected_docs:
                error_message = (
                    f"Insufficient documents processed. "
                    f"Generated: {documents_generated}, "
                    f"Processed: {documents_processed}, "
                    f"Expected minimum: {self.config.min_expected_docs}"
                )

        except Exception as e:
            error_message = str(e)
            self.logger.error(f"Test failed: {error_message}")
        finally:
            # Simple process cleanup
            for process in [self.monitor_process, self.generator_process]:
                if process and process.returncode is None:
                    try:
                        process.terminate()
                        await process.wait()
                    except:
                        process.kill()

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
                    # Send SIGTERM for graceful shutdown
                    process.terminate()
                    try:
                        # Give process time to cleanup and report final counts
                        asyncio.get_event_loop().run_until_complete(
                            asyncio.wait_for(process.wait(), timeout=10.0)
                        )
                    except (asyncio.TimeoutError, RuntimeError):
                        self.logger.warning("Process did not terminate gracefully, forcing kill")
                        process.kill()
                        try:
                            # Final attempt to wait without timeout
                            asyncio.get_event_loop().run_until_complete(process.wait())
                        except RuntimeError:
                            # Event loop might be closed, fall back to sync wait
                            process.wait()
                except Exception as e:
                    self.logger.error(f"Error during process cleanup: {e}")
                    # Ensure process is killed even if wait fails
                    try:
                        process.kill()
                    except ProcessLookupError:
                        pass  # Process already terminated

    async def _start_monitor(self):
        """Start the change stream monitor process."""
        monitor_script = os.path.join(os.path.dirname(__file__), 'stream_monitor.py')
        monitor_env = os.environ.copy()
        monitor_env['PYTHONUNBUFFERED'] = '1'  # Ensure output is not buffered
        
        self.monitor_process = await asyncio.create_subprocess_exec(
            sys.executable,
            monitor_script,
            env=monitor_env,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            bufsize=0  # No buffering
        )
        return self.monitor_process

    async def _start_generator(self):
        """Start the test data generator process."""
        generator_script = os.path.join(os.path.dirname(__file__), 'data_generator.py')
        generator_env = os.environ.copy()
        generator_env['PYTHONUNBUFFERED'] = '1'  # Ensure output is not buffered
        
        self.generator_process = await asyncio.create_subprocess_exec(
            sys.executable,
            generator_script,
            env=generator_env,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            bufsize=0  # No buffering
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
        # Load generation and processing details if available
        reports_dir = Path(__file__).parent.parent / 'reports'
        generation_details = {}
        processing_details = {}
        
        try:
            with open(reports_dir / 'generation_details.json', 'r') as f:
                generation_details = json.load(f)
        except FileNotFoundError:
            self.logger.warning("Generation details not found")
            
        try:
            with open(reports_dir / 'processing_details.json', 'r') as f:
                processing_details = json.load(f)
        except FileNotFoundError:
            self.logger.warning("Processing details not found")

        report = {
            "test_config": asdict(self.config),
            "test_result": result.to_dict(),
            "summary": {
                "status": "PASS" if result.success else "FAIL",
                "duration_seconds": result.duration,
                "documents": {
                    "generated": result.documents_generated,
                    "processed": result.documents_processed,
                    "expected_minimum": self.config.min_expected_docs,
                    "generation_details": generation_details,
                    "processing_details": processing_details
                }
            }
        }
        
        # Ensure reports directory exists
        reports_dir.mkdir(exist_ok=True)
        
        # Save both latest and timestamped reports
        report_path = reports_dir / 'latest.json'
        timestamp = result.start_time.strftime('%Y%m%d_%H%M%S')
        timestamped_path = reports_dir / f'report_{timestamp}.json'
        
        for path in [report_path, timestamped_path]:
            with open(path, 'w') as f:
                json.dump(report, f, indent=2, default=str)
        
        self.logger.info(f"Test reports written to {reports_dir}/")

    async def _cleanup_processes_async(self):
        """Async version of process cleanup."""
        for process in [self.monitor_process, self.generator_process]:
            if process and process.returncode is None:
                try:
                    # Send SIGTERM for graceful shutdown
                    process.terminate()
                    try:
                        # Give process time to cleanup and report final counts
                        await asyncio.wait_for(process.wait(), timeout=10.0)
                    except asyncio.TimeoutError:
                        self.logger.warning("Process did not terminate gracefully, forcing kill")
                        process.kill()
                        await process.wait()
                except Exception as e:
                    self.logger.error(f"Error during async process cleanup: {e}")
                    try:
                        process.kill()
                    except ProcessLookupError:
                        pass

def main():
    """Main entry point."""
    config = TestConfig()
    runner = IntegrationTestRunner(config)
    
    try:
        result = asyncio.run(runner.run_test())
        sys.exit(0 if result.success else 1)
    except KeyboardInterrupt:
        runner.logger.info("Test interrupted by user")
        sys.exit(130)
    except Exception as e:
        runner.logger.error(f"Test failed: {str(e)}")
        sys.exit(1)

if __name__ == '__main__':
    main() 