FROM python:3.11-slim

WORKDIR /app

# Install system dependencies for MongoDB TLS + DNS
RUN apt-get update && apt-get install -y \
    gcc \
    ca-certificates \
    openssl \
    dnsutils \
    curl \
    && update-ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements and install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY . .

CMD ["python", "--version"]   # replace with your actual entrypoint
