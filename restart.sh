#!/bin/bash

# Restart script for RemoteMedia gRPC server

echo "🔄 Restarting RemoteMedia gRPC Server..."

# Stop existing server if running
if [ -f server.pid ]; then
    PID=$(cat server.pid)
    if ps -p $PID > /dev/null 2>&1; then
        echo "🛑 Stopping existing server (PID: $PID)..."
        kill $PID
        sleep 2
        
        # Force kill if still running
        if ps -p $PID > /dev/null 2>&1; then
            echo "⚡ Force killing server..."
            kill -9 $PID
            sleep 1
        fi
    fi
    rm -f server.pid
fi

# Activate conda environment and start server
echo "🚀 Starting new server..."
source ~/miniconda3/etc/profile.d/conda.sh
conda activate remote_media
PYTHONPATH=. nohup python remote_service/src/server.py > server.log 2>&1 & 
NEW_PID=$!
echo $NEW_PID > server.pid

# Wait a moment and check if server started successfully
sleep 3

if ps -p $NEW_PID > /dev/null 2>&1; then
    echo "✅ Server started successfully (PID: $NEW_PID)"
    echo "📋 Server logs: tail -f server.log"
    echo "🔗 gRPC endpoint: localhost:50052"
else
    echo "❌ Server failed to start. Check server.log for details:"
    tail -10 server.log
    exit 1
fi