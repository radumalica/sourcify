FROM python:3.11-slim

WORKDIR /app

# Install system dependencies
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements and install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY . .

# Create logs directory
RUN mkdir -p /app/logs

# Make sync scripts executable
RUN chmod +x  sync_bigquery_optimized.py sync_parquet.py

# Default command (use optimized version for better performance)
CMD ["python", "sync_bigquery_optimized.py"]