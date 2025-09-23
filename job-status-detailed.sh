#!/bin/bash

echo "=========================================="
echo "DETAILED JOB Analysis"
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
    
    # Look for recent job IDs from log files
    echo ""
    echo "🔍 ANALYZING RECENT JOBS FROM LOG FILES"
    echo "----------------------------------------"
    
    LATEST_LOG=$(ls -t "$WORK_DIR"/*.out 2>/dev/null | head -1)
    if [ ! -z "$LATEST_LOG" ]; then
        # Extract job ID from any .out filename pattern
        ARRAY_JOB=$(basename "$LATEST_LOG" | grep -o '[0-9]\{7,\}' | head -1)
        echo "📋 Most recent job found in logs: $ARRAY_JOB"
        
        # Analyze completed job
        echo ""
        echo "📊 COMPLETED JOB ANALYSIS"
        echo "----------------------------------------"
        
        # Count log files containing this job ID
        OUT_FILES=$(ls "$WORK_DIR"/*.out 2>/dev/null | grep -c "$ARRAY_JOB")
        ERR_FILES=$(ls "$WORK_DIR"/*.err 2>/dev/null | grep -c "$ARRAY_JOB")
        echo "📝 Output files (.out): $OUT_FILES"
        echo "⚠️  Error files (.err): $ERR_FILES"
        
        # Try to get job stats from sacct
        if [ ! -z "$ARRAY_JOB" ]; then
            SACCT_OUTPUT=$(sacct -j $ARRAY_JOB --format=State --parsable2 --noheader 2>/dev/null)
            if [ $? -eq 0 ] && [ ! -z "$SACCT_OUTPUT" ]; then
                COMPLETED=$(echo "$SACCT_OUTPUT" | grep -c "COMPLETED")
                FAILED=$(echo "$SACCT_OUTPUT" | grep -c "FAILED")
                CANCELLED=$(echo "$SACCT_OUTPUT" | grep -c "CANCELLED")
                
                echo "✅ Completed tasks: $COMPLETED"
                echo "❌ Failed tasks: $FAILED"
                echo "🚫 Cancelled tasks: $CANCELLED"
                
                TOTAL_TASKS=$((COMPLETED + FAILED + CANCELLED))
                if [ $TOTAL_TASKS -gt 0 ]; then
                    SUCCESS_RATE=$(echo "scale=1; $COMPLETED * 100 / $TOTAL_TASKS" | bc -l)
                    echo "📊 Success rate: $SUCCESS_RATE%"
                fi
            else
                echo "⚠️  Cannot retrieve job statistics (job may be too old)"
            fi
        fi
        
    else
        echo "❌ No SLURM log files found in $WORK_DIR"
    fi
    
    exit 0
fi

# Find current array job dynamically
echo "🔍 DETECTING CURRENT JOBS"
echo "----------------------------------------"

# Show current jobs
squeue -u $USER

# Try to find array job ID
ARRAY_JOB=""
MAIN_JOB=""

# Look for running array tasks
RUNNING_TASK=$(squeue -u $USER -h | grep "_" | head -1)
if [ ! -z "$RUNNING_TASK" ]; then
    ARRAY_JOB=$(echo "$RUNNING_TASK" | awk '{print $1}' | cut -d'_' -f1)
fi

# Look for pending array jobs
if [ -z "$ARRAY_JOB" ]; then
    PENDING_TASK=$(squeue -u $USER -h | grep "\[" | head -1)
    if [ ! -z "$PENDING_TASK" ]; then
        ARRAY_JOB=$(echo "$PENDING_TASK" | awk '{print $1}' | cut -d'[' -f1)
    fi
fi

# Look for main orchestrator job
MAIN_TASK=$(squeue -u $USER -h | grep -v "_" | grep -v "\[" | head -1)
if [ ! -z "$MAIN_TASK" ]; then
    MAIN_JOB=$(echo "$MAIN_TASK" | awk '{print $1}')
fi

# If no running jobs, try to extract from most recent log files
if [ -z "$ARRAY_JOB" ]; then
    LATEST_LOG=$(ls -t "$WORK_DIR"/*.out 2>/dev/null | head -1)
    if [ ! -z "$LATEST_LOG" ]; then
        ARRAY_JOB=$(basename "$LATEST_LOG" | grep -o '[0-9]\{7,\}' | head -1)
    fi
fi

if [ ! -z "$ARRAY_JOB" ]; then
    echo "🎯 Detected array job: $ARRAY_JOB"
else
    echo "⚠️  No array job detected"
fi

if [ ! -z "$MAIN_JOB" ]; then
    echo "🎯 Detected main job: $MAIN_JOB"
fi

echo ""
echo "📊 CURRENT STATUS ANALYSIS"
echo "----------------------------------------"

if [ ! -z "$ARRAY_JOB" ]; then
    # Current status (only check if job exists)
    RUNNING=$(squeue -j $ARRAY_JOB -t R -h 2>/dev/null | wc -l)
    PENDING=$(squeue -j $ARRAY_JOB -t PD -h 2>/dev/null | wc -l)
    
    # Check completed from sacct
    SACCT_OUTPUT=$(sacct -j $ARRAY_JOB --format=State --parsable2 --noheader 2>/dev/null)
    if [ $? -eq 0 ] && [ ! -z "$SACCT_OUTPUT" ]; then
        COMPLETED=$(echo "$SACCT_OUTPUT" | grep -c "COMPLETED")
        FAILED=$(echo "$SACCT_OUTPUT" | grep -c "FAILED")
    else
        COMPLETED="N/A"
        FAILED="N/A"
    fi
    
    echo "✅ Completed tasks: $COMPLETED"
    echo "🏃 Currently running: $RUNNING"
    echo "⏰ Pending in queue: $PENDING"
    echo "❌ Failed tasks: $FAILED"
    
    # Estimate total tasks from log files containing this job ID
    TOTAL_TASKS=$(ls "$WORK_DIR"/*.out 2>/dev/null | grep -c "$ARRAY_JOB")
    if [ $TOTAL_TASKS -gt 0 ]; then
        echo "📊 Total tasks (estimated): $TOTAL_TASKS"
        
        if [[ "$COMPLETED" =~ ^[0-9]+$ ]] && [ $TOTAL_TASKS -gt 0 ]; then
            PROGRESS=$(echo "scale=1; $COMPLETED * 100 / $TOTAL_TASKS" | bc -l)
            echo "📈 Progress: $PROGRESS%"
        fi
    fi
else
    echo "⚠️  Cannot analyze job status - no valid array job found"
fi

echo ""
echo "📁 LOG FILE ANALYSIS"
echo "----------------------------------------"
echo "📊 SLURM log files in $WORK_DIR:"

# Count all .out and .err files by extension
OUT_FILES=$(ls "$WORK_DIR"/*.out 2>/dev/null | wc -l)
ERR_FILES=$(ls "$WORK_DIR"/*.err 2>/dev/null | wc -l)
LOG_FILES=$(ls "$WORK_DIR"/*.log 2>/dev/null | wc -l)

echo "   SLURM output files (.out): $OUT_FILES"
echo "   SLURM error files (.err): $ERR_FILES"
echo "   Python processor logs (.log): $LOG_FILES"

# Show job ID range from all files
if [ $OUT_FILES -gt 0 ]; then
    ALL_JOB_IDS=$(ls "$WORK_DIR"/*.out 2>/dev/null | grep -o '[0-9]\{7,\}' | sort -n | uniq)
    MIN_JOB=$(echo "$ALL_JOB_IDS" | head -1)
    MAX_JOB=$(echo "$ALL_JOB_IDS" | tail -1)
    UNIQUE_JOBS=$(echo "$ALL_JOB_IDS" | wc -l)
    echo "   Unique job IDs: $UNIQUE_JOBS"
    echo "   Job ID range: $MIN_JOB to $MAX_JOB"
fi

echo ""
echo "📈 OUTPUT DIRECTORY STATUS"
echo "----------------------------------------"
if [ -d "$WORK_DIR/output" ]; then
    OUTPUT_FILES=$(find "$WORK_DIR/output" -name "*.parquet" 2>/dev/null | wc -l)
    OUTPUT_SIZE=$(du -sh "$WORK_DIR/output" 2>/dev/null | cut -f1)
    echo "📂 Output files: $OUTPUT_FILES parquet files"
    echo "💾 Output size: $OUTPUT_SIZE"
    
    # Recent output activity
    RECENT_OUTPUT=$(find "$WORK_DIR/output" -name "*.parquet" -newermt "1 hour ago" 2>/dev/null | wc -l)
    echo "🕐 Files created in last hour: $RECENT_OUTPUT"
else
    echo "❌ Output directory not found at $WORK_DIR/output"
fi

# Check for other output directories
for dir in 202104-output 202110-output; do
    if [ -d "$WORK_DIR/$dir" ]; then
        DIR_FILES=$(find "$WORK_DIR/$dir" -name "*.parquet" 2>/dev/null | wc -l)
        DIR_SIZE=$(du -sh "$WORK_DIR/$dir" 2>/dev/null | cut -f1)
        echo "📂 $dir: $DIR_FILES files ($DIR_SIZE)"
    fi
done

echo ""
echo "🔍 RESOURCE UTILIZATION"
echo "----------------------------------------"

# Count nodes and resources
TOTAL_RUNNING=$(squeue -u $USER -h | wc -l)
if [ $TOTAL_RUNNING -gt 0 ]; then
    NODES=$(squeue -u $USER -h | awk '{print $8}' | grep -o 'bp1-compute[0-9]*' | sort -u | wc -l)
    CPUS=$((TOTAL_RUNNING * 8))
    
    echo "🏗️  Compute nodes in use: $NODES"
    echo "⚡ Estimated CPUs in use: $CPUS"
    echo "🏃 Total running jobs/tasks: $TOTAL_RUNNING"
    
    # Show node distribution
    if [ $NODES -gt 0 ]; then
        echo ""
        echo "📍 NODE DISTRIBUTION"
        echo "----------------------------------------"
        squeue -u $USER -h | awk '{print $8}' | grep -o 'bp1-compute[0-9]*' | sort | uniq -c | sort -nr | head -5
    fi
else
    echo "ℹ️  No jobs currently running"
fi

echo ""
echo "=========================================="
echo "💡 To refresh this report, run: source runme.sh && ./job-status-detailed.sh"
if [ ! -z "$ARRAY_JOB" ]; then
    echo "🔍 Check specific logs: ls -la $WORK_DIR/*$ARRAY_JOB*.{out,err}"
    echo "📊 Monitor job: watch 'squeue -j $ARRAY_JOB'"
fi
echo "🛠️  To cancel jobs, run: scancel JOBID"
echo "📂 Check output: ls -la $WORK_DIR/output/"
echo "📋 Check processor logs: ls -la $WORK_DIR/*.log"
echo "=========================================="

