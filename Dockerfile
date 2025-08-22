FROM python:3.11-slim

WORKDIR /app

# Create non-root user for security
RUN groupadd -r appuser && useradd -r -g appuser appuser

RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app/ ./app/
COPY utils/ ./utils/
COPY entrypoint.sh ./entrypoint.sh

# Make entrypoint script executable and change ownership to non-root user
RUN chmod +x ./entrypoint.sh && chown -R appuser:appuser /app

ENV PYTHONPATH=/app

# Switch to non-root user
USER appuser

EXPOSE 8000

CMD ["./entrypoint.sh"]