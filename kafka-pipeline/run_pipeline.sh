#!/bin/bash
set -e

echo "======================================="
echo " Kafka Pipeline – Hybrid Run Script"
echo "======================================="

# ---------- venv ----------
if [ -d "venv" ]; then
  echo "🔹 Activating virtual environment"
  source ../venv/bin/activate
fi

# ---------- STEP 1: Start Kafka ----------
echo "🚀 Starting Kafka (Docker)..."
docker compose up -d kafka

# ---------- STEP 2: Wait for Kafka ----------
echo "⏳ Waiting for Kafka to be ready..."
until echo > /dev/tcp/localhost/9092 2>/dev/null; do
  sleep 3
  echo "   Kafka not ready yet..."
done
echo "✅ Kafka is ready"

# ---------- STEP 3: Initializer ----------
echo "🚀 Running initializer..."
python initializer.py
echo "✅ Initializer finished"

# ---------- STEP 4: Producer ----------
echo "🚀 Starting producer..."
python producer/produce.py &
PRODUCER_PID=$!

# ---------- STEP 5: Consumer ----------
echo "🚀 Starting consumer..."
python consumer/consume.py &
CONSUMER_PID=$!

echo "======================================="
echo " Producer PID: $PRODUCER_PID"
echo " Consumer PID: $CONSUMER_PID"
echo "======================================="

# ---------- Keep script alive ----------
wait
