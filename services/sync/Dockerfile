FROM python:3.11-slim

WORKDIR /app

# Install system dependencies
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    postgresql-client \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY . .

# Create cache and log directories
RUN mkdir -p /parquet_cache /app/logs

# Set environment
ENV PYTHONUNBUFFERED=1

# Default command (can be overridden)
CMD ["python", "sync.py"]
