#!/bin/bash
# Entrypoint script to support environment variable configuration

PORT=${PORT:-8000}
HOST=${HOST:-0.0.0.0}

exec uvicorn app.api:app --host "$HOST" --port "$PORT" --proxy-headers