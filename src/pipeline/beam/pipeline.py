"""
Main Apache Beam pipeline for streaming MongoDB changes to Google Cloud Pub/Sub.

This module provides:
1. A complete pipeline that reads from MongoDB change streams
2. Transformation and enrichment of documents
3. Publishing to Google Cloud Pub/Sub topics
4. Monitoring and metrics
"""

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions, GoogleCloudOptions
from apache_beam.runners.runner import PipelineState
import json
import logging
from typing import Dict, Any, Optional, List, Tuple

from .sources.mongodb import ReadFromMongoDB
from ..mongodb.validator import DocumentValidator

logger = logging.getLogger(__name__)

class MongoDBToPubSubOptions(PipelineOptions):
    """Custom options for the MongoDB to Pub/Sub pipeline."""
    
    @classmethod
    def _add_argparse_args(cls, parser):
        """Add custom arguments."""
        parser.add_argument(
            '--config_file',
            help='Path to configuration file',
            default='config/config.yaml'
        )
        parser.add_argument(
            '--project',
            help='Google Cloud project ID',
            required=True
        )
        parser.add_argument(
            '--streaming',
            help='Whether to run in streaming mode',
            action='store_true',
            default=True
        )

class FormatForPubSubFn(beam.DoFn):
    """
    DoFn to format MongoDB documents for Pub/Sub.
    Extracts metadata and prepares message attributes.
    """
    
    def process(self, document: Dict[str, Any]) -> List[Tuple[bytes, Dict[str, str]]]:
        """
        Process a document, converting it to Pub/Sub message format.
        
        Args:
            document: MongoDB document
            
        Returns:
            List of tuples (message_data, message_attributes)
        """
        try:
            # Extract metadata for message attributes
            attributes = {}
            metadata = document.get('_metadata', {})
            
            # Add common metadata fields as attributes
            for key in ['source', 'connection_id', 'source_name', 'version']:
                if key in metadata:
                    attributes[key] = str(metadata[key])
            
            # Convert document to JSON bytes
            message_data = json.dumps(document).encode('utf-8')
            
            return [(message_data, attributes)]
            
        except Exception as e:
            logger.error(f"Error formatting document for Pub/Sub: {str(e)}")
            return []

class RouteToTopicFn(beam.DoFn):
    """
    DoFn to route documents to different topics based on content.
    """
    
    def __init__(self, routing_config: Dict[str, Dict[str, Any]]):
        """
        Initialize with routing configuration.
        
        Args:
            routing_config: Configuration for routing documents to topics
        """
        self.routing_config = routing_config
        
    def process(self, element: Tuple[bytes, Dict[str, str]]) -> List[beam.pvalue.TaggedOutput]:
        """
        Route a document to one or more topics.
        
        Args:
            element: Tuple of (message_data, attributes)
            
        Returns:
            Tagged outputs for different topics
        """
        message_data, attributes = element
        
        # Default topic (if defined)
        if 'default' in self.routing_config:
            yield beam.pvalue.TaggedOutput('default', element)
            
        # Try to decode JSON for content-based routing
        try:
            # We only need to decode if we're doing content-based routing
            if any(rule.get('field') for rule in self.routing_config.values()):
                document = json.loads(message_data)
                
                # Check each routing rule
                for topic, rule in self.routing_config.items():
                    if topic == 'default':
                        continue  # Already handled
                        
                    # Source-based routing
                    if 'source' in rule:
                        source_match = attributes.get('source_name') == rule['source']
                        if not source_match:
                            continue
                    
                    # Field-based routing
                    if 'field' in rule and 'value' in rule:
                        field_path = rule['field'].split('.')
                        value = document
                        
                        # Navigate nested fields
                        for part in field_path:
                            if isinstance(value, dict) and part in value:
                                value = value[part]
                            else:
                                value = None
                                break
                                
                        # Check if value matches
                        if value != rule['value']:
                            continue
                            
                    # If we got here, all rules matched
                    yield beam.pvalue.TaggedOutput(topic, element)
                    
        except Exception as e:
            logger.error(f"Error routing document: {str(e)}")
            # Fall back to default topic if available
            if 'default' in self.routing_config:
                yield beam.pvalue.TaggedOutput('default', element)

def build_pipeline(options: PipelineOptions, config: Dict[str, Any]) -> beam.Pipeline:
    """
    Build the MongoDB to Pub/Sub pipeline.
    
    Args:
        options: Pipeline options
        config: Pipeline configuration
        
    Returns:
        Configured pipeline
    """
    # Configure pipeline options
    if options.view_as(StandardOptions).streaming:
        options.view_as(StandardOptions).streaming = True
    
    gcp_options = options.view_as(GoogleCloudOptions)
    if not gcp_options.project:
        gcp_options.project = config.get('gcp', {}).get('project_id')
    
    # Create pipeline
    pipeline = beam.Pipeline(options=options)
    
    # Extract configuration sections
    mongodb_config = config.get('mongodb', {})
    pubsub_config = config.get('pubsub', {})
    schemas = config.get('schemas', {})
    topics = pubsub_config.get('topics', {})
    routing_config = pubsub_config.get('routing', {})
    
    # Build topic paths
    project_id = gcp_options.project
    topic_paths = {
        name: f"projects/{project_id}/topics/{details['name']}"
        for name, details in topics.items()
    }
    
    # Build pipeline
    documents = (
        pipeline
        | 'ReadFromMongoDB' >> ReadFromMongoDB(mongodb_config, schemas)
        | 'FormatForPubSub' >> beam.ParDo(FormatForPubSubFn())
    )
    
    # Apply routing if configured
    if routing_config:
        # Route to different topics
        routes = documents | 'RouteToTopics' >> beam.ParDo(
            RouteToTopicFn(routing_config)
        ).with_outputs()
        
        # Write to each topic
        for topic_name in routing_config:
            if topic_name in topic_paths and hasattr(routes, topic_name):
                routes[topic_name] | f'WriteTo{topic_name}' >> beam.io.WriteToPubSub(
                    topic=topic_paths[topic_name],
                    with_attributes=True
                )
    else:
        # Simple case - write all to default topic
        default_topic = topic_paths.get('default')
        if default_topic:
            documents | 'WriteToDefaultTopic' >> beam.io.WriteToPubSub(
                topic=default_topic,
                with_attributes=True
            )
        else:
            raise ValueError("No default topic specified in configuration")
    
    return pipeline

def run_pipeline(options: PipelineOptions, config: Dict[str, Any]) -> PipelineState:
    """
    Run the MongoDB to Pub/Sub pipeline.
    
    Args:
        options: Pipeline options
        config: Pipeline configuration
        
    Returns:
        Pipeline state after execution
    """
    # Build and run pipeline
    pipeline = build_pipeline(options, config)
    result = pipeline.run()
    
    # Wait for pipeline to finish if not streaming
    if not options.view_as(StandardOptions).streaming:
        result.wait_until_finish()
    
    return result.state
