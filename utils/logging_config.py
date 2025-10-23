"""
Logging configuration for development and production environments.
"""

import logging
import sys
from typing import Optional


def setup_development_logging(level: str = "DEBUG") -> None:
    """
    Configure logging for development environment with detailed output.

    Args:
        level: Logging level (DEBUG, INFO, WARNING, ERROR)
    """
    # Configure root logger
    logging.basicConfig(
        level=getattr(logging, level.upper()),
        format="%(asctime)s - %(name)s - %(levelname)s - %(filename)s:%(lineno)d - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        handlers=[logging.StreamHandler(sys.stdout)],
        force=True,
    )

    # Set specific loggers
    logging.getLogger("uvicorn.access").setLevel(logging.INFO)
    logging.getLogger("snowflake.connector").setLevel(logging.WARNING)
    logging.getLogger("googleapiclient.discovery").setLevel(logging.WARNING)
    logging.getLogger("google.auth").setLevel(logging.WARNING)
    logging.getLogger("utils.fetch_external").setLevel(logging.DEBUG)


def setup_production_logging(level: str = "INFO") -> None:
    """
    Configure logging for production environment.

    Args:
        level: Logging level (INFO, WARNING, ERROR)
    """
    logging.basicConfig(
        level=getattr(logging, level.upper()),
        format="%(asctime)s - %(name)s - %(levelname)s - %(filename)s:%(lineno)d - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        force=True,
    )

    # Reduce noise from external libraries
    logging.getLogger("snowflake.connector").setLevel(logging.ERROR)
    logging.getLogger("googleapiclient.discovery").setLevel(logging.ERROR)
    logging.getLogger("google.auth").setLevel(logging.ERROR)
    logging.getLogger("utils.fetch_external").setLevel(logging.DEBUG)


def get_logger(name: Optional[str] = None) -> logging.Logger:
    """
    Get a logger instance with the specified name.

    Args:
        name: Logger name, defaults to calling module name

    Returns:
        Logger instance
    """
    return logging.getLogger(name)
