#!/bin/bash

# Simple Status Tool for SLURM Common Crawl Job
# Shows: Work done, work remaining, percentage complete, elapsed time, and ETA

set -euo pipefail

# Colors
RED=$(tput setaf 1 2>/dev/null || echo "")
GREEN=$(tput setaf 2 2>/dev/null || echo "")
YELLOW=$(tput setaf 3 2>/dev/null || echo "")
BLUE=$(tput setaf 4 2>/dev/null || echo "")
BOLD=$(tput bold 2>/dev/null || echo "")
RESET=$(tput sgr0 2>/dev/null || echo "")

# Set default values for variables
WORKING_DIR="${WORKING_DIR:-.}"
SCRIPT_DIR="${SCRIPT_DIR:-.}"
CRAWL_DATA_FILE="${SCRIPT_DIR}/crawl_data.txt"

# Timing functionality
CURRENT_TIME=$(date +%s)

# Source runme.sh first to get SLURM_ACCOUNT
if [[ -f "./runme.sh" ]]; then
    # shellcheck source=/dev/null
    source ./runme.sh
fi

# Try to get the actual SLURM job start time first
START_TIME=""
if [[ -n "${SLURM_ACCOUNT:-}" ]]; then
    # Use the known start time of the main job (2025-09-25T16:31:39)
    START_TIME=$(date -d "2025-09-25T16:31:39" +%s 2>/dev/null || echo "")
fi

# Fallback to timing file if SLURM start time not available
START_TIME_FILE="${WORKING_DIR}/.job_start_time"
if [[ -z "$START_TIME" ]]; then
    # Create working directory if it doesn't exist
    mkdir -p "$WORKING_DIR" 2>/dev/null || true

    # Initialize or read start time from file
    if [[ ! -f "$START_TIME_FILE" ]]; then
        echo "$CURRENT_TIME" > "$START_TIME_FILE"
        START_TIME="$CURRENT_TIME"
        echo "🕐 Job started: $(date '+%Y-%m-%d %H:%M:%S')"
    else
        START_TIME=$(cat "$START_TIME_FILE" 2>/dev/null || echo "$CURRENT_TIME")
    fi
fi

# Calculate elapsed time
ELAPSED_SECONDS=$((CURRENT_TIME - START_TIME))
ELAPSED_HOURS=$((ELAPSED_SECONDS / 3600))
ELAPSED_MINUTES=$(( (ELAPSED_SECONDS % 3600) / 60 ))
ELAPSED_DAYS=$((ELAPSED_HOURS / 24))
ELAPSED_HOURS=$((ELAPSED_HOURS % 24))

# Source runme.sh to load environment variables if present
if [[ -f "./runme.sh" ]]; then
    # shellcheck source=/dev/null
    source ./runme.sh
    echo "✅ Loaded environment from runme.sh"
else
    echo "⚠️  Warning: runme.sh not found — using defaults"
fi

echo "${BOLD}${BLUE}🔍 SLURM Common Crawl Job Status${RESET}"
echo "====================================="

# Display environment information
echo ""
echo "${BOLD}🔧 Environment:${RESET}"
if [[ -n "${SLURM_ACCOUNT:-}" ]]; then
    echo "  SLURM Account: ${GREEN}${BOLD}$SLURM_ACCOUNT${RESET}"
else
    echo "  SLURM Account: ${YELLOW}Not set${RESET}"
fi
if [[ -n "${SCRIPT_DIR:-}" ]]; then
    echo "  Script Directory: ${BLUE}${BOLD}$SCRIPT_DIR${RESET}"
else
    echo "  Script Directory: ${YELLOW}Current directory${RESET}"
fi
if [[ -n "${WORKING_DIR:-}" ]]; then
    echo "  Working Directory: ${BLUE}${BOLD}$WORKING_DIR${RESET}"
else
    echo "  Working Directory: ${YELLOW}Current directory${RESET}"
fi

# Check if crawl data file exists
if [[ ! -f "$CRAWL_DATA_FILE" ]]; then
    echo "${RED}❌ Error: $CRAWL_DATA_FILE not found${RESET}"
    exit 1
fi

# Calculate total work from crawl_data.txt
total_files=0
total_dates=0

echo ""
echo "${BOLD}📊 Total Workload:${RESET}"

while read -r date n_files; do
    # Skip comments and empty lines
    [[ $date =~ ^#.*$ ]] && continue
    [[ -z "$date" ]] && continue

    if [[ "$n_files" =~ ^[0-9]+$ ]]; then
        total_files=$((total_files + n_files))
        total_dates=$((total_dates + 1))
        echo "  ${BLUE}$date${RESET}: ${BOLD}$n_files${RESET} files"
    fi
done < "$CRAWL_DATA_FILE"

echo ""
echo "${BOLD}📈 Progress Summary:${RESET}"
echo "  Total crawl dates: ${BOLD}$total_dates${RESET}"
echo "  Total files to process: ${BOLD}$total_files${RESET}"

# Count completed files
completed_files=0
completed_dates=0
current_date=""

if [[ -d "$WORKING_DIR" ]]; then
    # Count all parquet files
    while IFS= read -r -d '' file; do
        [[ -s "$file" ]] && completed_files=$((completed_files + 1))
    done < <(find "$WORKING_DIR" -name "crawldata*.parquet" -print0 2>/dev/null)
fi

remaining_files=$((total_files - completed_files))
percentage=0

if [[ $total_files -gt 0 ]]; then
    percentage=$((completed_files * 100 / total_files))
fi

echo ""
echo "${BOLD}✅ Completion Status:${RESET}"
echo "  Files completed: ${GREEN}${BOLD}$completed_files${RESET}/${BOLD}$total_files${RESET}"
echo "  Files remaining: ${YELLOW}${BOLD}$remaining_files${RESET}"
echo "  Progress: ${BOLD}${percentage}%${RESET} complete"

# Show progress bar
if [[ $total_files -gt 0 ]]; then
    bar_width=30
    filled=$((percentage * bar_width / 100))
    empty=$((bar_width - filled))

    printf "  ["
    printf "%0.s${GREEN}█${RESET}" $(seq 1 $filled)
    printf "%0.s${YELLOW}░${RESET}" $(seq 1 $empty)
    printf "]\n"
fi

# Show timing information
echo ""
echo "${BOLD}⏱️  Timing Information:${RESET}"

# Format elapsed time
if [[ $ELAPSED_DAYS -gt 0 ]]; then
    echo "  Elapsed time: ${BOLD}${ELAPSED_DAYS}d ${ELAPSED_HOURS}h ${ELAPSED_MINUTES}m${RESET}"
elif [[ $ELAPSED_HOURS -gt 0 ]]; then
    echo "  Elapsed time: ${BOLD}${ELAPSED_HOURS}h ${ELAPSED_MINUTES}m${RESET}"
else
    echo "  Elapsed time: ${BOLD}${ELAPSED_MINUTES}m${RESET}"
fi

# Calculate and display ETA
if [[ $completed_files -gt 0 && $remaining_files -gt 0 && $ELAPSED_SECONDS -gt 0 ]]; then
    # Calculate processing rate (files per minute) - avoid division by zero
    if [[ $ELAPSED_SECONDS -gt 0 ]]; then
        RATE_FILES_PER_MINUTE=$((completed_files * 60 / ELAPSED_SECONDS))
        if [[ $RATE_FILES_PER_MINUTE -gt 0 ]]; then
            # Calculate minutes remaining (remaining_files / files_per_minute)
            MINUTES_REMAINING=$((remaining_files / RATE_FILES_PER_MINUTE))
            ETA_HOURS=$((MINUTES_REMAINING / 60))
            ETA_MINUTES=$((MINUTES_REMAINING % 60))
            ETA_DAYS=$((ETA_HOURS / 24))
            ETA_HOURS=$((ETA_HOURS % 24))

            # Format ETA
            if [[ $ETA_DAYS -gt 0 ]]; then
                echo "  Estimated time remaining: ${YELLOW}${BOLD}${ETA_DAYS}d ${ETA_HOURS}h ${ETA_MINUTES}m${RESET}"
            elif [[ $ETA_HOURS -gt 0 ]]; then
                echo "  Estimated time remaining: ${YELLOW}${BOLD}${ETA_HOURS}h ${ETA_MINUTES}m${RESET}"
            else
                echo "  Estimated time remaining: ${YELLOW}${BOLD}${ETA_MINUTES}m${RESET}"
            fi

            echo "  Current processing rate: ${BOLD}${RATE_FILES_PER_MINUTE}${RESET} files/minute"
        else
            echo "  Estimated time remaining: ${YELLOW}Calculating...${RESET}"
        fi
    else
        echo "  Estimated time remaining: ${YELLOW}Calculating...${RESET}"
    fi
else
    echo "  Estimated time remaining: ${YELLOW}Waiting for progress...${RESET}"
fi

# Show current job status
echo ""
echo "${BOLD}⚙️  Active Jobs:${RESET}"
if [[ -n "${SLURM_ACCOUNT:-}" ]]; then
    squeue -u "$USER" -A "$SLURM_ACCOUNT" -o "%.8i %.9P %.8j %.8T %.10M %.6D %R" 2>/dev/null | head -6 || echo "  No active jobs found for account $SLURM_ACCOUNT"
else
    squeue -u "$USER" -o "%.8i %.9P %.8j %.8T %.10M %.6D %R" 2>/dev/null | head -6 || echo "  No active jobs found"
fi

echo ""
echo "${BOLD}📁 Output Files:${RESET}"
echo "  Completed parquet files: ${BOLD}$completed_files${RESET}"

if [[ $completed_files -gt 0 ]]; then
    echo "  Average processing rate: ${BOLD}$((completed_files / 60))${RESET} files/minute (estimated)"
fi

# Show recent activity
echo ""
echo "${BOLD}📈 Recent Activity:${RESET}"
if [[ -f "${WORKING_DIR}/progress.log" ]]; then
    tail -3 "${WORKING_DIR}/progress.log" 2>/dev/null || echo "  No progress log found"
else
    echo "  No progress log found"
fi

echo ""
echo "Last updated: $(date '+%Y-%m-%d %H:%M:%S')"
