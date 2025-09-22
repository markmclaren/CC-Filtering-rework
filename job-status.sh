#!/bin/bash

# SLURM Job Status Monitor
# Provides comprehensive status report for running Common Crawl processing jobs

echo "=========================================="
echo "SLURM Job Status Report"
echo "=========================================="
echo "Generated: $(date)"
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
MAIN_JOB=$(squeue -u $USER -h | grep "run-fast\|run-ultra\|run-optimized" | head -1)
if [ ! -z "$MAIN_JOB" ]; then
    MAIN_ID=$(echo $MAIN_JOB | awk '{print $1}')
    MAIN_TIME=$(echo $MAIN_JOB | awk '{print $6}')
    echo "🎯 Main orchestrator job: $MAIN_ID (Runtime: $MAIN_TIME)"
else
    echo "⚠️  No main orchestrator job found"
fi

# Count running array tasks
RUNNING_TASKS=$(squeue -u $USER -h | grep -E "13539494_|crawl_jo" | wc -l)
echo "🚀 Array tasks running: $RUNNING_TASKS"

# Get array job details
ARRAY_JOB=$(squeue -u $USER -h | grep "13539494\[" | head -1)
if [ ! -z "$ARRAY_JOB" ]; then
    PENDING_RANGE=$(echo $ARRAY_JOB | awk '{print $1}' | grep -o '\[[0-9-]*%' | tr -d '[%')
    echo "⏳ Pending tasks range: $PENDING_RANGE"
fi

# Count completed tasks
echo ""
echo "📈 COMPLETION STATISTICS"
echo "----------------------------------------"

# Get array job ID (assumes format 13539494)
ARRAY_ID=$(squeue -u $USER -h | grep -o "13539494" | head -1)
if [ ! -z "$ARRAY_ID" ]; then
    COMPLETED=$(sacct -j $ARRAY_ID --format=JobID,State -n 2>/dev/null | grep "COMPLETED" | wc -l)
    echo "✅ Completed tasks: $COMPLETED"
    
    # Calculate progress
    TOTAL_TASKS=2643  # Update this if your total changes
    PROCESSED=$((COMPLETED + RUNNING_TASKS))
    REMAINING=$((TOTAL_TASKS - PROCESSED))
    PROGRESS=$(echo "scale=1; $PROCESSED * 100 / $TOTAL_TASKS" | bc -l 2>/dev/null || echo "N/A")
    
    echo "🏃 Currently processing: $PROCESSED tasks"
    echo "⏰ Remaining in queue: $REMAINING tasks"
    echo "📊 Overall progress: $PROGRESS% complete"
else
    echo "⚠️  Array job ID not found"
fi

echo ""
echo "🖥️  RESOURCE UTILIZATION"
echo "----------------------------------------"

# Count unique nodes in use
NODES=$(squeue -u $USER -h | awk '{print $8}' | grep -o 'bp1-compute[0-9]*' | sort -u | wc -l)
echo "🏗️  Compute nodes in use: $NODES"

# Calculate CPU usage (assuming 2 CPUs per task)
CPUS=$((RUNNING_TASKS * 2))
echo "⚡ Total CPUs in use: $CPUS"

# Show node distribution
echo ""
echo "📍 NODE DISTRIBUTION"
echo "----------------------------------------"
squeue -u $USER -h | awk '{print $8}' | grep -o 'bp1-compute[0-9]*' | sort | uniq -c | sort -nr | head -10

echo ""
echo "⏱️  RUNTIME ANALYSIS"
echo "----------------------------------------"

# Show runtime distribution
echo "📋 Current task runtimes:"
squeue -u $USER -h | grep "13539494_" | awk '{print $6}' | sort | uniq -c | sort -nr | head -5

# Calculate estimated completion
if [ ! -z "$COMPLETED" ] && [ $COMPLETED -gt 0 ]; then
    # Rough calculation based on current completion rate
    HOURS_RUNNING=$(echo $MAIN_TIME | awk -F: '{if(NF==3) print $1 + $2/60 + $3/3600; else print $1/60 + $2/3600}')
    if [ ! -z "$HOURS_RUNNING" ] && [ ! -z "$REMAINING" ]; then
        RATE=$(echo "scale=2; $COMPLETED / $HOURS_RUNNING" | bc -l 2>/dev/null)
        if [ ! -z "$RATE" ] && [ "$RATE" != "0" ]; then
            EST_HOURS=$(echo "scale=1; $REMAINING / $RATE" | bc -l 2>/dev/null)
            EST_DAYS=$(echo "scale=1; $EST_HOURS / 24" | bc -l 2>/dev/null)
            echo ""
            echo "⏰ ESTIMATED COMPLETION"
            echo "----------------------------------------"
            echo "📈 Completion rate: $RATE tasks/hour"
            echo "⏳ Estimated time remaining: $EST_HOURS hours (~$EST_DAYS days)"
        fi
    fi
fi

echo ""
echo "🔍 DETAILED QUEUE STATUS"
echo "----------------------------------------"
squeue -u $USER

echo ""
echo "=========================================="
echo "💡 To refresh this report, run: ./job-status.sh"
echo "🛠️  To cancel jobs, run: scancel JOBID"
echo "📊 For detailed job info: scontrol show job JOBID"
echo "=========================================="