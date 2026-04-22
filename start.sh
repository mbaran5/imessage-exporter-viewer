#!/bin/bash
set -e

echo "=== iMessage Search ==="
echo "Running indexer..."
python3 /app/indexer.py

echo ""
echo "Starting web server on port 6333..."
exec python3 /app/app.py
