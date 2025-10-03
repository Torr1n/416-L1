#!/bin/bash
# Simple test script
rm -f mr-out-* mr-[0-9]*-[0-9]*

echo "Starting coordinator..."
go run mrcoordinator.go pg-being_ernest.txt &
COORD_PID=$!

sleep 3

echo "Starting worker..."
go run mrworker.go wc.so &
WORKER_PID=$!

# Wait for worker to finish or timeout
sleep 10

# Check if still running
if kill -0 $WORKER_PID 2>/dev/null; then
    echo "Worker still running, killing..."
    kill $WORKER_PID
fi

if kill -0 $COORD_PID 2>/dev/null; then
    echo "Coordinator still running, killing..."
    kill $COORD_PID
fi

echo "Files created:"
ls mr-* 2>/dev/null || echo "No mr files found"
