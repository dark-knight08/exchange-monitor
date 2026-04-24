FROM python:3.11-slim

WORKDIR /app

# Install curl for health checks and any build dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application
COPY . .

# Create data directory
RUN mkdir -p data

# Expose API port
EXPOSE 8000

# Environment
ENV PYTHONUNBUFFERED=1
ENV DATABASE_PATH=/app/data/hkex_monitor.db

# Start the full system (API + scheduler)
CMD ["python", "main.py", "run", "--host", "0.0.0.0", "--port", "8000"]
