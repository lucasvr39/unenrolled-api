"""
Unenrolled API

A containerized FastAPI application that finds unenrolled users by comparing
external data sources with Snowflake enrollment data for data reconciliation.
"""

__version__ = "1.0.0"
__author__ = "lucasvr39"

# Import main application for easy access
from app.api import app

__all__ = ["app"]
