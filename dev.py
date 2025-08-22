#!/usr/bin/env python3
"""
Development server script with uvicorn configured for debugging.
Provides auto-reload, detailed logging, and debug mode for local development.
"""

import os

import uvicorn
from dotenv import load_dotenv

from utils.logging_config import setup_development_logging

load_dotenv()

setup_development_logging()

if __name__ == "__main__":
    port = int(os.getenv("PORT", 8000))
    host = os.getenv("HOST", "127.0.0.1")

    uvicorn.run(
        "app.api:app",
        host=host,
        port=port,
        reload=True,
        log_level="debug",
        access_log=True,
        reload_dirs=["app", "utils"],  # Watch directories for changes
    )
