#!/usr/bin/env python3
"""
Integration test runner for MongoDB document validation.
Coordinates test data generation and validation, then generates a report.
"""

import os
import sys
import json
import asyncio
import logging
import datetime
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

logger = logging.getLogger(__name__)

# Add the project root to the Python path
current_dir = Path(__file__).resolve().parent
project_root = current_dir.parent.parent.parent.parent
sys.path.append(str(project_root))

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
    expected_valid_docs: int = 8
    expected_invalid_docs: int = 4

@dataclass
class TestResult:
    """Test result data."""
    start_time: datetime.datetime
    end_time: datetime.datetime
    duration: float
    documents_generated: int
    documents_processed: int
    valid_documents: int
    invalid_documents: int
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
            "valid_documents": self.valid_documents,
            "invalid_documents": self.invalid_documents,
            "success": self.success,
            "error_message": self.error_message
        }

class IntegrationTestRunner:
    def __init__(self, config: TestConfig):
        self.config = config
        self.result = None
        self.validator_process = None
        self.generator_process = None
        self.logger = logging.getLogger(__name__)
        setup_logging()

    async def _read_stream(self, stream, prefix=""):
        """Read from a stream and log output with a prefix"""
        try:
            while True:
                try:
                    line = await stream.readline()
                    if not line:  # EOF
                        break
                    text = line.decode().strip()
                    if text:
                        self.logger.info(f"{prefix}: {text}")  # Changed to INFO for better visibility
                except Exception as e:
                    self.logger.error(f"Error reading stream: {e}")
                    break
        except asyncio.CancelledError:
            pass  # Allow clean cancellation

    async def _monitor_output(self, process, name):
        """Monitor the output of a process"""
        stdout_task = asyncio.create_task(self._read_stream(process.stdout, f"{name} [stdout]"))
        stderr_task = asyncio.create_task(self._read_stream(process.stderr, f"{name} [stderr]"))
        try:
            await asyncio.gather(stdout_task, stderr_task)
        except asyncio.CancelledError:
            stdout_task.cancel()
            stderr_task.cancel()
            try:
                await asyncio.gather(stdout_task, stderr_task, return_exceptions=True)
            except asyncio.CancelledError:
                pass

    async def run_test(self) -> TestResult:
        """Run the integration test suite."""
        start_time = datetime.datetime.now(UTC)
        documents_generated = 0
        documents_processed = 0
        valid_documents = 0
        invalid_documents = 0
        error_message = None
        validator_task = None
        generator_task = None

        # Verify environment variables
        required_vars = ['MONGODB_TEST_URI', 'MONGODB_TEST_DB', 'MONGODB_TEST_COLLECTION']
        missing_vars = [var for var in required_vars if not os.getenv(var)]
        if missing_vars:
            error_message = f"Missing required environment variables: {', '.join(missing_vars)}"
            self.logger.error(error_message)
            return TestResult(
                start_time=start_time,
                end_time=datetime.datetime.now(UTC),
                duration=0,
                documents_generated=0,
                documents_processed=0,
                valid_documents=0,
                invalid_documents=0,
                success=False,
                error_message=error_message
            )

        try:
            # Start the validator monitor
            validator_process = await self._start_validator()
            self.validator_process = validator_process
            self.logger.info("Waiting for validator to initialize...")
            
            # Start monitoring output
            validator_task = asyncio.create_task(self._monitor_output(validator_process, "VALIDATOR"))
            
            await asyncio.sleep(3)  # Give validator time to initialize

            # Start the test data generator
            generator_process = await self._start_generator()
            self.generator_process = generator_process
            generator_task = asyncio.create_task(self._monitor_output(generator_process, "GENERATOR"))

            # Wait for generator to complete
            await generator_process.wait()
            generator_output = await self._collect_output(generator_process)
            documents_generated = self._extract_document_count(generator_output)
            self.logger.info(f"Generator completed. Generated {documents_generated} documents")

            # Read generation details to find last document timestamp
            reports_dir = Path(__file__).parent.parent / 'reports'
            generation_details_file = reports_dir / 'generation_details.json'
            try:
                with open(generation_details_file) as f:
                    generation_details = json.load(f)
                if generation_details:
                    last_generated_str = generation_details[-1]['generated_at']
                    if 'Z' not in last_generated_str and '+' not in last_generated_str:
                        last_generated_str += 'Z'
                    last_generated = datetime.datetime.fromisoformat(last_generated_str.replace('Z', '+00:00'))
                    now = datetime.datetime.now(UTC)
                    wait_time = max(0, 15 - (now - last_generated).total_seconds())
                    if wait_time > 0:
                        self.logger.info(f"Waiting {wait_time:.1f} seconds from last document generation...")
                        await asyncio.sleep(wait_time)
            except Exception as e:
                self.logger.warning(f"Could not read generation details, using default grace period: {e}")
                grace_period = 15
                self.logger.info(f"Using fallback grace period of {grace_period} seconds...")
                await asyncio.sleep(grace_period)

            # Send SIGTERM to validator and wait for it to finish
            validator_process.terminate()
            try:
                await asyncio.wait_for(validator_process.wait(), timeout=5.0)
            except asyncio.TimeoutError:
                validator_process.kill()
                await validator_process.wait()

            # Get final validator output
            validator_output = await self._collect_output(validator_process)
            validation_results = self._extract_validation_results(validator_output)
            documents_processed = validation_results['total']
            valid_documents = validation_results['valid']
            invalid_documents = validation_results['invalid']
            self.logger.info(f"Validator completed. Processed {documents_processed} documents ({valid_documents} valid, {invalid_documents} invalid)")

            # Cancel output monitoring tasks
            if validator_task:
                validator_task.cancel()
            if generator_task:
                generator_task.cancel()
            try:
                await asyncio.gather(validator_task, generator_task, return_exceptions=True)
            except asyncio.CancelledError:
                pass

            # If we couldn't get counts from output, try to get from files
            if documents_generated == 0:
                try:
                    with open(reports_dir / 'generation_details.json', 'r') as f:
                        generation_details = json.load(f)
                        documents_generated = len(generation_details)
                except:
                    pass

            if documents_processed == 0:
                try:
                    with open(reports_dir / 'processing_details.json', 'r') as f:
                        processing_details = json.load(f)
                        documents_processed = len(processing_details)
                        valid_documents = sum(1 for d in processing_details if d.get('validation') == 'success')
                        invalid_documents = sum(1 for d in processing_details if d.get('validation') == 'failed')
                except:
                    pass

            # Verify results
            if documents_processed != documents_generated:
                error_message = (
                    f"Document count mismatch. "
                    f"Generated: {documents_generated}, "
                    f"Processed: {documents_processed}"
                )
            elif valid_documents != self.config.expected_valid_docs:
                error_message = (
                    f"Valid document count mismatch. "
                    f"Expected: {self.config.expected_valid_docs}, "
                    f"Got: {valid_documents}"
                )
            elif invalid_documents != self.config.expected_invalid_docs:
                error_message = (
                    f"Invalid document count mismatch. "
                    f"Expected: {self.config.expected_invalid_docs}, "
                    f"Got: {invalid_documents}"
                )

        except Exception as e:
            error_message = str(e)
            self.logger.error(f"Test failed: {error_message}")
        finally:
            # Clean up processes
            for process in [self.validator_process, self.generator_process]:
                if process and process.returncode is None:
                    try:
                        process.terminate()
                        await asyncio.wait_for(process.wait(), timeout=5.0)
                    except asyncio.TimeoutError:
                        process.kill()
                        await process.wait()
                    except Exception as e:
                        self.logger.error(f"Error during process cleanup: {e}")
                        try:
                            process.kill()
                        except:
                            pass

            # Cancel any remaining tasks
            if validator_task and not validator_task.done():
                validator_task.cancel()
            if generator_task and not generator_task.done():
                generator_task.cancel()

        end_time = datetime.datetime.now(UTC)
        duration = (end_time - start_time).total_seconds()

        result = TestResult(
            start_time=start_time,
            end_time=end_time,
            duration=duration,
            documents_generated=documents_generated,
            documents_processed=documents_processed,
            valid_documents=valid_documents,
            invalid_documents=invalid_documents,
            success=error_message is None,
            error_message=error_message
        )

        # Generate test report
        self._save_report(result)
        return result

    async def _start_validator(self):
        """Start the validator monitor process."""
        validator_script = os.path.join(os.path.dirname(__file__), 'validator_monitor.py')
        validator_env = os.environ.copy()
        validator_env['PYTHONUNBUFFERED'] = '1'  # Ensure output is not buffered
        
        self.validator_process = await asyncio.create_subprocess_exec(
            sys.executable,
            validator_script,
            env=validator_env,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            bufsize=0  # No buffering
        )
        return self.validator_process

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

    async def _collect_output(self, process) -> str:
        """Collect all output from a process's stdout and stderr."""
        stdout, stderr = await process.communicate()
        return stdout.decode() + stderr.decode()

    def _extract_document_count(self, output: str) -> int:
        """Extract the number of documents generated from process output."""
        # First try to get from output
        for line in output.splitlines():
            if "Generated" in line and "valid" in line and "invalid" in line:
                try:
                    # Format: "Test completed. Generated 12 documents (8 valid, 4 invalid)"
                    parts = line.split()
                    return int(parts[2])  # Get the total count
                except (IndexError, ValueError):
                    continue
        
        # If not found in output, try to get from generation details
        try:
            reports_dir = Path(__file__).parent.parent / 'reports'
            with open(reports_dir / 'generation_details.json', 'r') as f:
                generation_details = json.load(f)
                return len(generation_details)
        except:
            pass
        
        return 0

    def _extract_validation_results(self, output: str) -> Dict[str, int]:
        """Extract validation results from process output."""
        results = {'total': 0, 'valid': 0, 'invalid': 0}
        
        # First try to get from output
        for line in output.splitlines():
            if "Validation stats" in line:
                try:
                    # Format: "Validation stats - Processed: X, Valid: Y, Invalid: Z"
                    parts = line.split()
                    results['total'] = int(parts[4].rstrip(','))
                    results['valid'] = int(parts[6].rstrip(','))
                    results['invalid'] = int(parts[8])
                    return results
                except (IndexError, ValueError):
                    continue
        
        # If not found in output, try to get from processing details
        try:
            reports_dir = Path(__file__).parent.parent / 'reports'
            with open(reports_dir / 'processing_details.json', 'r') as f:
                processing_details = json.load(f)
                results['total'] = len(processing_details)
                results['valid'] = sum(1 for d in processing_details if d.get('validation') == 'success')
                results['invalid'] = sum(1 for d in processing_details if d.get('validation') == 'failed')
        except:
            pass
        
        return results

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
                    "valid": result.valid_documents,
                    "invalid": result.invalid_documents,
                    "expected_valid": self.config.expected_valid_docs,
                    "expected_invalid": self.config.expected_invalid_docs,
                    "generation_details": generation_details,
                    "processing_details": processing_details
                }
            }
        }
        
        # Write latest report
        reports_dir.mkdir(exist_ok=True)
        with open(reports_dir / 'latest.json', 'w') as f:
            json.dump(report, f, indent=2, default=str)
        
        self.logger.info(f"Test reports written to {reports_dir}/")

if __name__ == '__main__':
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