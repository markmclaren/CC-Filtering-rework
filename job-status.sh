#!/bin/bash

# SLURM Job Status Monitor
# Provides comprehensive status report for running Common Crawl processing jobs

echo "=========================================="
echo "SLURM Job Status Report"
echo "=========================================="
echo "Generated: $(date)"
echo ""

# Use JOB_WORKDIR if set, otherwise current directory
WORK_DIR="${JOB_WORKDIR:-$(pwd)}"
echo "Working directory: $WORK_DIR"
echo ""

# Check if user has any jobs running
JOBS=$(squeue -u $USER -h | wc -l)
if [ $JOBS -eq 0 ]; then
    echo "❌ No jobs currently running for user $USER"
    exit 0
fi

echo "📊 CURRENT JOB STATUS"
echo "----------------------------------------"

# Get main orchestrator job info
MAIN_JOB=$(squeue -u $USER -h | grep -E "run-fast|run-ultra|run-optimized" | head -1)
if [ ! -z "$MAIN_JOB" ]; then
    MAIN_ID=$(echo $MAIN_JOB | awk '{print $1}')
    MAIN_TIME=$(echo $MAIN_JOB | awk '{print $6}')
    echo "🎯 Main orchestrator job: $MAIN_ID (Runtime: $MAIN_TIME)"
fi

# Find the current array job
ARRAY_ID=$(squeue -u $USER -h | grep "_" | head -1 | awk '{print $1}' | cut -d'_' -f1)

if [ ! -z "$ARRAY_ID" ]; then
    echo "🎯 Array job ID: $ARRAY_ID"
    
    # Count running array tasks
    RUNNING_TASKS=$(squeue -u $USER -h | grep "${ARRAY_ID}_" | wc -l)
    echo "🚀 Array tasks running: $RUNNING_TASKS"
    
    echo ""
    echo "📈 COMPLETION STATISTICS"
    echo "----------------------------------------"
    
    # Get completion stats from sacct (this was working well before)
    COMPLETED=$(sacct -j $ARRAY_ID --format=JobID,State -n 2>/dev/null | grep "COMPLETED" | wc -l)
    FAILED=$(sacct -j $ARRAY_ID --format=JobID,State -n 2>/dev/null | grep "FAILED" | wc -l)
    
    echo "✅ Completed tasks: $COMPLETED"
    
    # Get total tasks from pending job range
    PENDING_JOB=$(squeue -u $USER -h | grep "${ARRAY_ID}\[" | head -1)
    if [ ! -z "$PENDING_JOB" ]; then
        # Extract the range like [421-2559]
        RANGE=$(echo $PENDING_JOB | awk '{print $1}' | grep -o '\[[0-9-]*\]' | tr -d '[]')
        if [[ "$RANGE" =~ ^[0-9]+-[0-9]+$ ]]; then
            START=$(echo $RANGE | cut -d'-' -f1)
            END=$(echo $RANGE | cut -d'-' -f2)
            TOTAL_TASKS=$((END + 1))  # Add 1 because arrays are 0-indexed
        fi
    fi
    
    # If we couldn't get total from pending range, try a different approach
    if [ -z "$TOTAL_TASKS" ]; then
        # Use a reasonable default based on your setup
        TOTAL_TASKS=2643
        echo "ℹ️  Using default total tasks: $TOTAL_TASKS"
    else
        echo "📊 Total tasks: $TOTAL_TASKS"
    fi
    
    # Calculate progress
    PROCESSED=$((COMPLETED + RUNNING_TASKS))
    REMAINING=$((TOTAL_TASKS - PROCESSED))
    
    echo "🏃 Currently processing: $PROCESSED tasks"
    echo "⏰ Remaining in queue: $REMAINING tasks"
    
    if [ $TOTAL_TASKS -gt 0 ]; then
        PROGRESS=$(echo "scale=1; $PROCESSED * 100 / $TOTAL_TASKS" | bc -l)
        echo "📊 Overall progress: $PROGRESS% complete"
    fi
    
    # Calculate completion rate and time estimate
    if [ ! -z "$MAIN_TIME" ] && [ $COMPLETED -gt 0 ]; then
        # Convert runtime to minutes
        if [[ "$MAIN_TIME" =~ ^([0-9]+):([0-9]+):([0-9]+)$ ]]; then
            HOURS=${BASH_REMATCH[1]}
            MINS=${BASH_REMATCH[2]}
            TOTAL_MINUTES=$((HOURS * 60 + MINS))
        elif [[ "$MAIN_TIME" =~ ^([0-9]+):([0-9]+)$ ]]; then
            MINS=${BASH_REMATCH[1]}
            TOTAL_MINUTES=$MINS
        fi
        
        if [ ! -z "$TOTAL_MINUTES" ] && [ $TOTAL_MINUTES -gt 0 ]; then
            RATE=$(echo "scale=2; $COMPLETED * 60 / $TOTAL_MINUTES" | bc -l)
            if [ $REMAINING -gt 0 ] && [ $(echo "$RATE > 0" | bc -l) -eq 1 ]; then
                TIME_REMAINING_HOURS=$(echo "scale=1; $REMAINING / $RATE" | bc -l)
                TIME_REMAINING_DAYS=$(echo "scale=1; $TIME_REMAINING_HOURS / 24" | bc -l)
                
                echo ""
                echo "⏰ ESTIMATED COMPLETION"
                echo "----------------------------------------"
                echo "📈 Completion rate: $RATE tasks/hour"
                echo "⏳ Estimated time remaining: $TIME_REMAINING_HOURS hours (~$TIME_REMAINING_DAYS days)"
            fi
        fi
    fi
    
else
    echo "⚠️  No array job found"
fi

echo ""
echo "🖥️  RESOURCE UTILIZATION"
echo "----------------------------------------"

# Count unique nodes in use
NODES=$(squeue -u $USER -h | awk '{print $8}' | grep -o 'bp1-compute[0-9]*' | sort -u | wc -l)
echo "🏗️  Compute nodes in use: $NODES"

# Calculate CPU usage
TOTAL_RUNNING=$(squeue -u $USER -h | wc -l)
CPUS=$((TOTAL_RUNNING * 8))
echo "⚡ Total CPUs in use: $CPUS"

# Show node distribution
if [ $NODES -gt 0 ]; then
    echo ""
    echo "📍 NODE DISTRIBUTION"
    echo "----------------------------------------"
    squeue -u $USER -h | awk '{print $8}' | grep -o 'bp1-compute[0-9]*' | sort | uniq -c | sort -nr | head -10
fi

echo ""
echo "⏱️  RUNTIME ANALYSIS"
echo "----------------------------------------"
echo "📋 Current task runtimes:"
squeue -u $USER -h | awk '{print $6}' | sort | uniq -c | sort -nr | head -5

echo ""
echo "🔍 DETAILED QUEUE STATUS"
echo "----------------------------------------"
squeue -u $USER

echo ""
echo "=========================================="
echo "💡 To refresh this report, run: source runme.sh && ./job-status.sh"
echo "🛠️  To cancel jobs, run: scancel JOBID"
echo "📊 For detailed job info: scontrol show job JOBID"
echo "🔍 Check logs: ls -la $WORK_DIR/*.{out,err,log}"
echo "=========================================="