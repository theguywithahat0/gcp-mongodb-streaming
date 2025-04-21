"""Main entry point for the MongoDB Change Stream Connector."""

import argparse
import sys

from .core.connector import main

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="MongoDB Change Stream Connector"
    )
    parser.add_argument(
        "--config",
        help="Path to configuration file",
        default=None
    )
    
    args = parser.parse_args()
    sys.exit(main()) 