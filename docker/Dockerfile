FROM python:3.11-slim

# Set working directory inside container
WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    wget \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy ETL
COPY pipeline/ ./pipeline/
COPY sql/ ./sql/

# Create output directories in container
RUN mkdir -p data insights

# Default command: run your ETL
CMD ["python", "pipeline/etl.py"]
