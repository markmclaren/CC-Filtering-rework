#!/bin/bash

# Unified Job Monitoring Script for Common Crawl Processing
# Usage: ./monitor-job.sh [JOB_PREFIX] [--dry-run]
# Example: ./monitor-job.sh crawl_job_long
# This script checks Slurm status, progress, time limits, and disk usage.

set -euo pipefail  # Exit on error, undefined vars, pipe failures

source ./runme.sh  # Load environment variables

# Print loaded environment variables for clarity
echo "Loaded environment variables:"
echo "  SLURM_ACCOUNT: $SLURM_ACCOUNT"
echo "  SBATCH_ACCOUNT: $SBATCH_ACCOUNT"
echo "  WORKING_DIR: $WORKING_DIR"
echo "  SCRIPT_DIR: $SCRIPT_DIR"


# Colors for output (optional)
RED=$(tput setaf 1)
GREEN=$(tput setaf 2)
YELLOW=$(tput setaf 3)
BLUE=$(tput setaf 4)
RESET=$(tput sgr0)

# Defaults
JOB_PREFIX="${1:-crawl_job}"  # Default prefix if none provided
DRY_RUN="${2:-}"  # --dry-run flag for testing
OUTPUT_DIR="${WORKING_DIR}/output"  # Parquet and wet.gz files location
WET_PATHS_PATTERN="wet.paths"  # File listing total segments
CRAWL_DATA_FILE="crawl_data.txt"  # List of crawl dates
LOG_DIR="${WORKING_DIR}"  # .out, .log, .err files location
DISK_THRESHOLD_GB=10  # Warn if free < this
OUTPUT_SIZE_THRESHOLD_GB=500  # Warn if total output > this
TIME_WARN_PCT=80  # Warn if elapsed > this % of limit

if [[ "$DRY_RUN" == "--dry-run" ]]; then
    echo "${YELLOW}Dry-run mode: Simulating without real checks.${RESET}"
    exit 0
fi

echo "${BLUE}=== Job Monitoring Report for Prefix: $JOB_PREFIX ===${RESET}"
echo "Report generated: $(date)"
echo ""

# 1. WHAT HAS RUN SO FAR: Slurm Job Status
echo "${GREEN}1. Current Slurm Jobs${RESET}"
echo "-------------------------"
if command -v squeue >/dev/null 2>&1; then
    RUNNING=$(squeue -A "$SLURM_ACCOUNT" -u $USER -j "*${JOB_PREFIX}*" --format="%i %P %j %T %l %D" --state=RUNNING | wc -l)
    PENDING=$(squeue -A "$SLURM_ACCOUNT" -u $USER -j "*${JOB_PREFIX}*" --format="%i %P %j %T %l %D" --state=PENDING | wc -l)
    COMPLETED=$(sacct -A "$SLURM_ACCOUNT" -u $USER -j "*${JOB_PREFIX}*" --format="JobID,JobName,State,Elapsed,MaxRSS" --state=COMPLETED | tail -n +2 | wc -l)

    echo "Running jobs: $((RUNNING - 1))  (includes header row)"
    echo "Pending jobs: $((PENDING - 1))"
    echo "Recent completed jobs: $COMPLETED"
    
    # Detailed running jobs
    echo ""
    echo "Running/Pending Details:"
    squeue -A "$SLURM_ACCOUNT" -u $USER -j "*${JOB_PREFIX}*" --format="%i %P %j %T %l %M %D %R" --states=RUNNING,PENDING || echo "No matching jobs found."
else
    echo "${RED}Warning: squeue not available (not on Slurm cluster?).${RESET}"
fi
echo ""

# 2. PROGRESS: What has run / What's left
echo "${GREEN}2. Processing Progress${RESET}"
echo "---------------------"
TOTAL_SEGMENTS=0
PROCESSED_SEGMENTS=0
CRAWL_DATES=()

if [[ -f "$CRAWL_DATA_FILE" ]]; then
    mapfile -t CRAWL_DATES < <(grep -v '^#' "$CRAWL_DATA_FILE" | cut -d' ' -f1)  # Assume first column is date like 202350
fi

for DATE in "${CRAWL_DATES[@]}"; do
    WET_PATHS="${DATE}_${WET_PATHS_PATTERN}"
    if [[ -f "$WET_PATHS" ]]; then
        TOTAL_SEGMENTS=$((TOTAL_SEGMENTS + $(wc -l < "$WET_PATHS")))
    fi
done

# Count processed (Parquet files exist and non-empty)
for PARQUET in "$OUTPUT_DIR"/crawldata*.parquet; do
    if [[ -f "$PARQUET" && -s "$PARQUET" ]]; then  # Non-empty
        PROCESSED_SEGMENTS=$((PROCESSED_SEGMENTS + 1))
    fi
done

PERCENT_COMPLETE=$(( (PROCESSED_SEGMENTS * 100) / TOTAL_SEGMENTS ))
REMAINING_SEGMENTS=$((TOTAL_SEGMENTS - PROCESSED_SEGMENTS))

echo "Total segments to process: $TOTAL_SEGMENTS (across ${#CRAWL_DATES[@]} crawl dates)"
echo "Processed segments so far: $PROCESSED_SEGMENTS"
echo "Progress: ${PERCENT_COMPLETE}%"
echo "Remaining segments: $REMAINING_SEGMENTS"

if [[ $REMAINING_SEGMENTS -eq 0 ]]; then
    echo "${GREEN}All segments complete!${RESET}"
else
    # Rough time estimate: Average from recent logs (assumes logs have lines like "[TIME] segment X: Y sec")
    EST_TIME_PER_SEG=300  # Default 5 min; parse logs for better estimate
    if [[ -d "$LOG_DIR" ]]; then
        RECENT_LOGS=$(ls -t "$LOG_DIR"/*.log 2>/dev/null | head -5)  # Last 5 logs in WORKING_DIR
        for LOG in $RECENT_LOGS; do
            AVG_TIME=$(grep "Completed processing segment" "$LOG" | awk '{sum+=$NF} END {if (NR>0) print sum/NR}' || echo "$EST_TIME_PER_SEG")
            EST_TIME_PER_SEG=$(( (EST_TIME_PER_SEG + AVG_TIME) / 2 ))  # Simple average
        done
    fi
    EST_TIME_LEFT_HRS=$(( (REMAINING_SEGMENTS * EST_TIME_PER_SEG) / 3600 ))
    echo "Estimated time left: ~${EST_TIME_LEFT_HRS} hours (at ~${EST_TIME_PER_SEG}s per segment)"
fi
echo ""

# 3. TIME CONSTRAINTS
echo "${GREEN}3. Time Constraints${RESET}"
echo "--------------------"
# Get time limit from running jobs (first one)
TIME_LIMIT=$(squeue -A "$SLURM_ACCOUNT" -u $USER -j "*${JOB_PREFIX}*" --format="%l" --states=RUNNING | head -n2 | tail -1 | cut -d: -f1)  # e.g., "14-00:00:00" -> hours
if [[ -n "$TIME_LIMIT" ]]; then
    TIME_HRS=${TIME_LIMIT%%:*}  # Extract hours
    ELAPSED=$(squeue -A "$SLURM_ACCOUNT" -u $USER -j "*${JOB_PREFIX}*" --format="%D" --states=RUNNING | head -n2 | tail -1 | cut -d: -f1)  # Days elapsed
    ELAPSED_HRS=$((ELAPSED * 24))
    PCT_USED=$(( (ELAPSED_HRS * 100) / TIME_HRS ))
    
    echo "Job time limit: ~${TIME_HRS} hours"
    echo "Elapsed time: ~${ELAPSED_HRS} hours (${PCT_USED}%)"
    
    if [[ $PCT_USED -gt $TIME_WARN_PCT ]]; then
        echo "${RED}WARNING: Approaching time limit! Consider extending or checking for issues.${RESET}"
    fi
else
    echo "No running jobs with prefix '$JOB_PREFIX'."
fi

# Project deadline (optional env var)
if [[ -n "${PROJECT_DEADLINE:-}" ]]; then
    DEADLINE_DAYS=$(( ($(date -d "$PROJECT_DEADLINE" +%s) - $(date +%s)) / 86400 ))
    echo "Project deadline: $PROJECT_DEADLINE (~${DEADLINE_DAYS} days left)"
    if [[ $DEADLINE_DAYS -lt 7 ]]; then
        echo "${RED}ALERT: Project deadline approaching!${RESET}"
    fi
fi
echo ""

# 4. FILE SIZE CONSTRAINTS
echo "${GREEN}4. Disk and File Size Status${RESET}"
echo "-----------------------------"

# Disk and file size status for WORKING_DIR/output
FREE_GB=$(df -BG "$WORKING_DIR" | tail -1 | awk '{print $4}' | sed 's/G//')  # Free space in GB
TOTAL_OUTPUT_GB=$(du -sBG "$OUTPUT_DIR" 2>/dev/null | awk '{print $1}' | sed 's/G//') || echo "0"

echo "Free disk space in WORKING_DIR: ${FREE_GB}GB"
echo "Total output size in WORKING_DIR/output: ${TOTAL_OUTPUT_GB}GB"

if [[ $FREE_GB -lt $DISK_THRESHOLD_GB ]]; then
    echo "${RED}WARNING: Low disk space! Free: ${FREE_GB}GB (threshold: ${DISK_THRESHOLD_GB}GB). Run cleanup.sh?${RESET}"
fi

if [[ $TOTAL_OUTPUT_GB -gt $OUTPUT_SIZE_THRESHOLD_GB ]]; then
    echo "${RED}WARNING: Output size exceeding threshold: ${TOTAL_OUTPUT_GB}GB (> ${OUTPUT_SIZE_THRESHOLD_GB}GB). Consider archiving.${RESET}"
fi

echo ""
echo "${BLUE}Monitoring complete. For details, check squeue or logs in $OUTPUT_DIR.${RESET}"
