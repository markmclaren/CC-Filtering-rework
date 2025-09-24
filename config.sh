#!/bin/bash

# =============================================================================
# CC-Filtering-rework Monitoring Configuration
# =============================================================================
# This file contains centralized configuration for all monitoring scripts.
# Source this file in your monitoring scripts with: source config.sh

# --- General Settings ---
# Working directory for logs and output files
# Override with JOB_WORKDIR environment variable if set
WORK_DIR="${JOB_WORKDIR:-$(pwd)}"

# --- SLURM Settings ---
# SLURM user for querying jobs (defaults to current user)
SLURM_USER="${USER}"

# Common job name patterns to search for
JOB_NAME_PATTERNS=("run-fast" "run-ultra" "run-optimized" "run-disk-safe" "crawler")

# --- Log File Settings ---
# Naming patterns for SLURM log files
ERROR_LOG_PATTERN="*.err"
OUTPUT_LOG_PATTERN="*.out"

# Maximum number of log files to analyze (for performance)
MAX_LOG_FILES=1000

# --- File Analysis Settings ---
# Directories to analyze for output files (relative to WORK_DIR)
OUTPUT_DIRS=("output" "results" "processed" "filtered")

# File extensions to specifically track
TRACKED_EXTENSIONS=("json" "jsonl" "parquet" "csv" "txt" "log")

# --- Display Settings ---
# Colors for output (set to empty strings to disable colors)
COLOR_GREEN="\033[0;32m"
COLOR_RED="\033[0;31m"
COLOR_YELLOW="\033[1;33m"
COLOR_BLUE="\033[0;34m"
COLOR_PURPLE="\033[0;35m"
COLOR_CYAN="\033[0;36m"
COLOR_RESET="\033[0m"

# Progress bar settings
PROGRESS_BAR_WIDTH=50

# --- Utility Functions ---
# Function to format bytes in human-readable format
format_bytes() {
    local bytes=$1
    if [ "$bytes" -eq 0 ]; then
        echo "0 B"
    elif [ "$bytes" -lt 1024 ]; then
        echo "${bytes} B"
    elif [ "$bytes" -lt 1048576 ]; then
        echo "$(( bytes / 1024 )) KB"
    elif [ "$bytes" -lt 1073741824 ]; then
        echo "$(( bytes / 1048576 )) MB"
    else
        echo "$(( bytes / 1073741824 )) GB"
    fi
}

# Function to print colored output
print_colored() {
    local color=$1
    local text=$2
    echo -e "${color}${text}${COLOR_RESET}"
}

# Function to print a progress bar
print_progress_bar() {
    local current=$1
    local total=$2
    local width=${3:-$PROGRESS_BAR_WIDTH}
    
    if [ "$total" -eq 0 ]; then
        echo "[No data available]"
        return
    fi
    
    local percentage=$(( current * 100 / total ))
    local filled=$(( current * width / total ))
    local empty=$(( width - filled ))
    
    printf "["
    printf "%*s" "$filled" | tr ' ' '='
    printf "%*s" "$empty" | tr ' ' '-'
    printf "] %d%% (%d/%d)\n" "$percentage" "$current" "$total"
}

# Function to validate working directory
validate_work_dir() {
    if [ ! -d "$WORK_DIR" ]; then
        print_colored "$COLOR_RED" "Error: Working directory '$WORK_DIR' does not exist."
        return 1
    fi
    
    if [ ! -r "$WORK_DIR" ]; then
        print_colored "$COLOR_RED" "Error: Cannot read working directory '$WORK_DIR'."
        return 1
    fi
    
    return 0
}

# Function to check if required commands are available
check_dependencies() {
    local missing_commands=()
    
    for cmd in squeue sacct find grep awk; do
        if ! command -v "$cmd" >/dev/null 2>&1; then
            missing_commands+=("$cmd")
        fi
    done
    
    if [ ${#missing_commands[@]} -gt 0 ]; then
        print_colored "$COLOR_RED" "Error: Missing required commands: ${missing_commands[*]}"
        return 1
    fi
    
    return 0
}

# Function to get current timestamp
get_timestamp() {
    date '+%Y-%m-%d %H:%M:%S'
}

# Validation on source
if ! validate_work_dir; then
    print_colored "$COLOR_YELLOW" "Warning: Working directory validation failed. Some scripts may not work correctly."
fi

if ! check_dependencies; then
    print_colored "$COLOR_YELLOW" "Warning: Dependency check failed. Some scripts may not work correctly."
fi
