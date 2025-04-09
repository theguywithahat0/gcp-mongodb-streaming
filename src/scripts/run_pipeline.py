#!/usr/bin/env python3
"""
Runner script for the MongoDB to Pub/Sub pipeline.

This script:
1. Loads the configuration from config/config.yaml
2. Sets up pipeline options
3. Runs the pipeline either locally or on Dataflow
"""

import argparse
import logging
import os
import sys
import yaml
import json
from datetime import datetime
from typing import Dict, Any, List, Optional

# Add the src directory to the Python path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from pipeline.beam.pipeline import MongoDBToPubSubOptions, run_pipeline
from pipeline.utils.config import load_config
from pipeline.transforms.loader import validate_transform_config

def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description='Run the MongoDB to Pub/Sub pipeline')
    
    # Basic configuration
    parser.add_argument(
        '--config', 
        type=str, 
        default='config/config.yaml',
        help='Path to the configuration file (default: config/config.yaml)'
    )
    parser.add_argument(
        '--project', 
        type=str, 
        help='Google Cloud project ID (overrides config)'
    )
    
    # Runner configuration
    parser.add_argument(
        '--runner', 
        type=str, 
        choices=['DirectRunner', 'DataflowRunner'],
        default='DirectRunner',
        help='Pipeline runner (default: DirectRunner)'
    )
    parser.add_argument(
        '--streaming', 
        action='store_true', 
        default=True,
        help='Run in streaming mode (default: True)'
    )
    
    # Transform configuration
    parser.add_argument(
        '--disable_transforms',
        action='store_true',
        help='Disable all custom transforms'
    )
    parser.add_argument(
        '--transform_config',
        type=str,
        help='Path to additional transform configuration file (JSON format)'
    )
    
    # Dataflow-specific options
    dataflow_group = parser.add_argument_group('Dataflow options')
    dataflow_group.add_argument(
        '--region', 
        type=str, 
        help='Google Cloud region for Dataflow (overrides config)'
    )
    dataflow_group.add_argument(
        '--temp_location', 
        type=str,
        help='GCS location for temporary files'
    )
    dataflow_group.add_argument(
        '--staging_location', 
        type=str,
        help='GCS location for staging files'
    )
    dataflow_group.add_argument(
        '--job_name', 
        type=str, 
        default=f'mongodb-to-pubsub-{datetime.now().strftime("%Y%m%d-%H%M%S")}',
        help='Dataflow job name (default: mongodb-to-pubsub-YYYYMMDD-HHMMSS)'
    )
    dataflow_group.add_argument(
        '--machine_type', 
        type=str, 
        default='n1-standard-2',
        help='Dataflow worker machine type (default: n1-standard-2)'
    )
    dataflow_group.add_argument(
        '--max_workers', 
        type=int, 
        default=2,
        help='Maximum number of Dataflow workers (default: 2)'
    )
    
    # Monitoring configuration
    monitoring_group = parser.add_argument_group('Monitoring options')
    monitoring_group.add_argument(
        '--enable_monitoring',
        action='store_true',
        help='Enable pipeline monitoring'
    )
    monitoring_group.add_argument(
        '--monitoring_project',
        type=str,
        help='Project ID for monitoring metrics (defaults to --project)'
    )
    
    # Logging configuration
    parser.add_argument(
        '--log_level', 
        type=str, 
        choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'],
        default='INFO',
        help='Logging level (default: INFO)'
    )
    
    # Advanced options
    parser.add_argument(
        '--setup_file', 
        type=str, 
        default='./setup.py',
        help='Path to setup.py file for Dataflow dependencies (default: ./setup.py)'
    )
    parser.add_argument(
        '--validation_only', 
        action='store_true',
        help='Validate configuration without running the pipeline'
    )
    
    return parser.parse_args()

def setup_logging(log_level: str):
    """Set up logging configuration."""
    numeric_level = getattr(logging, log_level.upper(), None)
    if not isinstance(numeric_level, int):
        raise ValueError(f'Invalid log level: {log_level}')
    
    logging.basicConfig(
        level=numeric_level,
        format='%(asctime)s [%(levelname)s] %(name)s: %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )

def validate_config(config: Dict[str, Any]) -> List[str]:
    """
    Validate the pipeline configuration.
    
    Args:
        config: Pipeline configuration
        
    Returns:
        List of validation error messages, empty if valid
    """
    errors = []
    
    # Check MongoDB configuration
    if 'mongodb' not in config:
        errors.append("Missing MongoDB configuration section")
    else:
        if 'connections' not in config['mongodb']:
            errors.append("Missing MongoDB connections configuration")
        elif not config['mongodb']['connections']:
            errors.append("No MongoDB connections defined")
    
    # Check Pub/Sub configuration
    if 'pubsub' not in config:
        errors.append("Missing Pub/Sub configuration section")
    else:
        if 'topics' not in config['pubsub']:
            errors.append("Missing Pub/Sub topics configuration")
        elif not config['pubsub']['topics']:
            errors.append("No Pub/Sub topics defined")
        
        # Check for default topic if no routing is defined
        if ('routing' not in config['pubsub'] or not config['pubsub']['routing']) and \
           ('default' not in config['pubsub']['topics']):
            errors.append("No routing defined and no default topic found")
    
    # Check transforms configuration (optional)
    if 'transforms' in config:
        transform_errors = validate_transform_config(config['transforms'])
        errors.extend(transform_errors)
    
    return errors

def load_transform_config(config_path: str) -> Dict[str, Any]:
    """
    Load transform configuration from a JSON file.
    
    Args:
        config_path: Path to JSON configuration file
        
    Returns:
        Transform configuration dictionary
    """
    with open(config_path, 'r') as f:
        return json.load(f)

def merge_configs(base_config: Dict[str, Any], 
                override_config: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """
    Merge configuration dictionaries.
    
    Args:
        base_config: Base configuration
        override_config: Override configuration (optional)
        
    Returns:
        Merged configuration
    """
    if not override_config:
        return base_config.copy()
    
    result = base_config.copy()
    
    # Deep merge dictionaries
    for key, value in override_config.items():
        if key in result and isinstance(result[key], dict) and isinstance(value, dict):
            result[key] = merge_configs(result[key], value)
        else:
            result[key] = value
            
    return result

def create_pipeline_options(args, config: Dict[str, Any]):
    """
    Create pipeline options from command line arguments and configuration.
    
    Args:
        args: Command line arguments
        config: Configuration dictionary
        
    Returns:
        MongoDBToPubSubOptions instance
    """
    # Get project ID from args or config
    project = args.project or config.get('gcp', {}).get('project_id')
    if not project:
        raise ValueError("Project ID is required. Provide it via --project or in config file")
    
    # Get region from args or config
    region = args.region or config.get('gcp', {}).get('region')
    
    pipeline_args = [
        f'--project={project}',
        f'--runner={args.runner}',
        f'--streaming={str(args.streaming).lower()}',
        f'--config_file={args.config}'
    ]
    
    # Add monitoring options if enabled
    if args.enable_monitoring or config.get('monitoring', {}).get('metrics', {}).get('enable', False):
        pipeline_args.append('--enable_streaming_engine')
        
        # Use monitoring project if specified, otherwise use main project
        monitoring_project = args.monitoring_project or project
        pipeline_args.append(f'--monitoring_project={monitoring_project}')
    
    # Add Dataflow-specific options if using DataflowRunner
    if args.runner == 'DataflowRunner':
        pipeline_args.extend([
            f'--job_name={args.job_name}',
            f'--machine_type={args.machine_type}',
            f'--max_num_workers={args.max_workers}',
            f'--setup_file={args.setup_file}'
        ])
        
        # Add region if provided
        if region:
            pipeline_args.append(f'--region={region}')
        
        # Add locations if provided
        if args.temp_location:
            pipeline_args.append(f'--temp_location={args.temp_location}')
        
        if args.staging_location:
            pipeline_args.append(f'--staging_location={args.staging_location}')
    
    return MongoDBToPubSubOptions(pipeline_args)

def main():
    """Main entry point."""
    args = parse_args()
    setup_logging(args.log_level)
    logger = logging.getLogger(__name__)
    
    logger.info(f"Loading configuration from {args.config}")
    try:
        config = load_config(args.config)
    except Exception as e:
        logger.error(f"Failed to load configuration: {str(e)}")
        return 1
    
    # Handle transform configuration
    if args.disable_transforms:
        logger.info("Custom transforms are disabled by command line flag")
        if 'transforms' in config:
            del config['transforms']
    elif args.transform_config:
        logger.info(f"Loading additional transform configuration from {args.transform_config}")
        try:
            transform_config = load_transform_config(args.transform_config)
            config = merge_configs(config, {'transforms': transform_config})
        except Exception as e:
            logger.error(f"Failed to load transform configuration: {str(e)}")
            return 1
    
    logger.info("Validating configuration")
    errors = validate_config(config)
    if errors:
        for error in errors:
            logger.error(f"Configuration error: {error}")
        return 1
    
    if args.validation_only:
        logger.info("Configuration is valid. Exiting due to --validation_only flag")
        return 0
    
    logger.info(f"Creating pipeline with runner: {args.runner}")
    try:
        pipeline_options = create_pipeline_options(args, config)
    except ValueError as e:
        logger.error(f"Error creating pipeline options: {str(e)}")
        return 1
    
    logger.info("Starting pipeline")
    try:
        run_pipeline(pipeline_options, config)
        logger.info("Pipeline started successfully")
        return 0
    except Exception as e:
        logger.error(f"Failed to run pipeline: {str(e)}")
        return 1

if __name__ == '__main__':
    sys.exit(main())
