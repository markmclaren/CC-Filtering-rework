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
# Accept optional job id as first arg; --dry-run can be first or second arg
JOB_ID=""
DRY_RUN=""
if [[ "${1:-}" == "--dry-run" || "${2:-}" == "--dry-run" ]]; then
    DRY_RUN="--dry-run"
fi
if [[ "${1:-}" != "--dry-run" && -n "${1:-}" ]]; then
    JOB_ID="$1"
fi
OUTPUT_DIR="${WORKING_DIR}/output"  # Parquet and wet.gz files location
WET_PATHS_PATTERN="wet.paths"  # File listing total segments
CRAWL_DATA_FILE="crawl_data.txt"  # List of crawl dates
LOG_DIR="${WORKING_DIR}"  # .out, .log, .err files location
DISK_THRESHOLD_GB=10  # Warn if free < this
OUTPUT_SIZE_THRESHOLD_GB=500  # Warn if total output > this
TIME_WARN_PCT=80  # Warn if elapsed > this % of limit

# Try to auto-detect the most recent job ID for the user if not provided
if [[ -z "$JOB_ID" ]] && command -v squeue >/dev/null 2>&1; then
    JOB_ID=$(squeue -u "$USER" --noheader -o "%A %j %T" | awk '{print $1}' | tail -n1 || true)
fi
# Fallback to sacct (recent historical jobs) if still empty
if [[ -z "$JOB_ID" ]] && command -v sacct >/dev/null 2>&1; then
    JOB_ID=$(sacct -u "$USER" --format=JobID%20,JobName%40 --noheader | awk '{print $1}' | tail -n1 || true)
fi

if [[ -n "$JOB_ID" ]]; then
    echo "Using job id: $JOB_ID"
    JOB_SELECTOR_ID="$JOB_ID"
else
    echo "No job id provided or auto-detected; operating on all jobs for user $USER."
    JOB_SELECTOR_ID=""
fi

if [[ "$DRY_RUN" == "--dry-run" ]]; then
    echo "${YELLOW}Dry-run mode: Simulating without real checks.${RESET}"
    exit 0
fi

echo "${BLUE}=== Job Monitoring Report ${JOB_ID:+for Job ID: $JOB_ID} ===${RESET}"
echo "Report generated: $(date)"
echo ""

# 1. WHAT HAS RUN SO FAR: Slurm Job Status
echo "${GREEN}1. Current Slurm Jobs${RESET}"
echo "-------------------------"
if command -v squeue >/dev/null 2>&1; then
    if [[ -n "${JOB_SELECTOR_ID:-}" ]]; then
        RUNNING=$(squeue -A "$SLURM_ACCOUNT" -u $USER -j "$JOB_SELECTOR_ID" --format="%i %P %j %T %l %D" --state=RUNNING | wc -l)
        PENDING=$(squeue -A "$SLURM_ACCOUNT" -u $USER -j "$JOB_SELECTOR_ID" --format="%i %P %j %T %l %D" --state=PENDING | wc -l)
        COMPLETED=$(sacct -A "$SLURM_ACCOUNT" -u $USER -j "$JOB_SELECTOR_ID" --format="JobID,JobName,State,Elapsed,MaxRSS" --state=COMPLETED | tail -n +2 | wc -l || echo 0)
    else
        # No specific job id: list all jobs for user
        RUNNING=$(squeue -A "$SLURM_ACCOUNT" -u $USER --format="%i %P %j %T %l %D" --state=RUNNING | wc -l)
        PENDING=$(squeue -A "$SLURM_ACCOUNT" -u $USER --format="%i %P %j %T %l %D" --state=PENDING | wc -l)
        COMPLETED=$(sacct -A "$SLURM_ACCOUNT" -u $USER --format="JobID,JobName,State,Elapsed,MaxRSS" --state=COMPLETED | tail -n +2 | wc -l || echo 0)
    fi

    echo "Running jobs: $((RUNNING - 1))  (includes header row)"
    echo "Pending jobs: $((PENDING - 1))"
    echo "Recent completed jobs: $COMPLETED"
    
    # Detailed running jobs
    echo ""
    echo "Running/Pending Details:"
    if [[ -n "${JOB_SELECTOR_ID:-}" ]]; then
        squeue -A "$SLURM_ACCOUNT" -u $USER -j "$JOB_SELECTOR_ID" --format="%i %P %j %T %l %M %D %R" --states=RUNNING,PENDING || echo "No matching jobs found."
    else
        squeue -A "$SLURM_ACCOUNT" -u $USER --format="%i %P %j %T %l %M %D %R" --states=RUNNING,PENDING || echo "No matching jobs found."
    fi
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

# Protect against division by zero when no segments are listed
if [[ $TOTAL_SEGMENTS -le 0 ]]; then
    PERCENT_COMPLETE=0
    REMAINING_SEGMENTS=$TOTAL_SEGMENTS  # keep semantics (usually 0)
else
    PERCENT_COMPLETE=$(( (PROCESSED_SEGMENTS * 100) / TOTAL_SEGMENTS ))
    REMAINING_SEGMENTS=$((TOTAL_SEGMENTS - PROCESSED_SEGMENTS))
fi

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

# Convert Slurm time strings (DD-HH:MM:SS, HH:MM:SS, MM:SS, UNLIMITED) to seconds
slurm_time_to_seconds() {
    local t="$1"
    [[ -z "$t" || "$t" == "UNLIMITED" ]] && echo 0 && return
    if [[ "$t" =~ ^([0-9]+)-([0-9]{1,2}):([0-9]{2}):([0-9]{2})$ ]]; then
        local days=${BASH_REMATCH[1]}
        local hrs=${BASH_REMATCH[2]}
        local mins=${BASH_REMATCH[3]}
        local secs=${BASH_REMATCH[4]}
        echo $(( (days*24 + hrs)*3600 + mins*60 + secs ))
        return
    fi
    if [[ "$t" =~ ^([0-9]{1,2}):([0-9]{2}):([0-9]{2})$ ]]; then
        local hrs=${BASH_REMATCH[1]}
        local mins=${BASH_REMATCH[2]}
        local secs=${BASH_REMATCH[3]}
        echo $(( hrs*3600 + mins*60 + secs ))
        return
    fi
    if [[ "$t" =~ ^([0-9]{1,2}):([0-9]{2})$ ]]; then
        local mins=${BASH_REMATCH[1]}
        local secs=${BASH_REMATCH[2]}
        echo $(( mins*60 + secs ))
        return
    fi
    # fallback: try integer seconds
    echo "$t"
}

# Query squeue for elapsed and limit (use job id if set, else first running job for user)
if command -v squeue >/dev/null 2>&1; then
    if [[ -n "${JOB_SELECTOR_ID:-}" ]]; then
        line=$(squeue -A "$SLURM_ACCOUNT" -u $USER -j "$JOB_SELECTOR_ID" -o "%M %l" --noheader --states=RUNNING | head -n1 || true)
    else
        line=$(squeue -A "$SLURM_ACCOUNT" -u $USER -o "%M %l" --noheader --states=RUNNING | head -n1 || true)
    fi

    if [[ -n "$line" ]]; then
        elapsed_str=$(awk '{print $1}' <<<"$line")
        limit_str=$(awk '{print $2}' <<<"$line")
        elapsed_s=$(slurm_time_to_seconds "$elapsed_str")
        limit_s=$(slurm_time_to_seconds "$limit_str")

        limit_hrs=$(( limit_s / 3600 ))
        elapsed_hrs=$(( elapsed_s / 3600 ))

        if [[ $limit_s -le 0 ]]; then
            PCT_USED=0
        else
            PCT_USED=$(( (elapsed_s * 100) / limit_s ))
        fi

        echo "Job time limit: ~${limit_hrs} hours"
        echo "Elapsed time: ~${elapsed_hrs} hours (${PCT_USED}%)"

        if [[ $PCT_USED -gt $TIME_WARN_PCT ]]; then
            echo "${RED}WARNING: Approaching time limit! Consider extending or checking for issues.${RESET}"
        fi
    else
        if [[ -n "${JOB_SELECTOR_ID:-}" ]]; then
            echo "No running job found with id '$JOB_SELECTOR_ID'."
        else
            echo "No running jobs to report time limits."
        fi
    fi
else
    echo "${RED}squeue not available; cannot determine time constraints.${RESET}"
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
