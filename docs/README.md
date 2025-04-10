# MongoDB Streaming Pipeline Documentation

This directory contains documentation for the MongoDB Streaming Pipeline project.

## Available Documentation

- [Creating New Use Cases](creating_new_use_cases.md) - A guide to creating custom, domain-specific implementations using the pipeline framework

## Project Overview

The MongoDB Streaming Pipeline is a framework for streaming data from MongoDB to Google Cloud Pub/Sub using Apache Beam. It provides:

- Flexible configuration for connecting to multiple MongoDB sources
- Customizable transforms for data processing
- Integration with Google Cloud Pub/Sub for real-time data streaming
- Domain-specific implementations for various use cases

## Getting Started

1. See the [project README](../README.md) for installation and basic setup
2. Follow the [Creating New Use Cases](creating_new_use_cases.md) guide to implement your own domain logic
3. Check out the [existing use cases](../use_cases) for examples and inspiration 