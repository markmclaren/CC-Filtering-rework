#!/bin/bash

echo "=========================================="
echo "Real-time Disk Usage Monitor"
echo "=========================================="

# Use JOB_WORKDIR if set, otherwise current directory
WORK_DIR="${JOB_WORKDIR:-$(pwd)}"

while true; do
    clear
    echo "$(date): Monitoring disk usage for running jobs"
    echo "Working directory: $WORK_DIR"
    echo "----------------------------------------"
    
    # Current disk usage
    df -h "$WORK_DIR" | head -2
    echo ""
    
    # Count running jobs
    RUNNING_JOBS=$(squeue -u $USER -t R | wc -l)
    echo "🏃 Running jobs: $((RUNNING_JOBS-1))"
    
    # Check for wet.gz files
    WET_FILES=$(find "$WORK_DIR" -name "*.wet.gz" 2>/dev/null | wc -l)
    WET_SIZE=$(find "$WORK_DIR" -name "*.wet.gz" -exec du -ch {} + 2>/dev/null | tail -1 | cut -f1)
    echo "📁 Active wet.gz files: $WET_FILES ($WET_SIZE)"
    
    # Recent output activity
    RECENT_FILES=$(find "$WORK_DIR/output/" "$WORK_DIR/202104-output/" -name "*.parquet" -mmin -5 2>/dev/null | wc -l)
    echo "📊 Files created in last 5 min: $RECENT_FILES"
    
    echo ""
    echo "Press Ctrl+C to stop monitoring"
    sleep 30
done
