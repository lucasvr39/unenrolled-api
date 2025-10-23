#!/bin/bash
# Entrypoint script to support environment variable configuration and debug mode

PORT=${PORT:-8000}
HOST=${HOST:-0.0.0.0}
DEBUG=${DEBUG:-false}

# Configure uvicorn based on debug mode
if [[ "${DEBUG,,}" =~ ^(true|1|yes)$ ]]; then
    echo "Starting in DEBUG mode with detailed logging..."
    exec uvicorn app.api:app --host "$HOST" --port "$PORT" --proxy-headers --log-level debug --access-log
else
    echo "Starting in PRODUCTION mode..."
    exec uvicorn app.api:app --host "$HOST" --port "$PORT" --proxy-headers --log-level info
fi