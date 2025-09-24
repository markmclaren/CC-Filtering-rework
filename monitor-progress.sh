#!/bin/bash

# =============================================================================
# Job Progress Monitor
# =============================================================================
# Provides comprehensive, real-time monitoring of SLURM job progress
# with reliable parsing and accurate time estimates.
#
# Usage: ./monitor-progress.sh [options]
# Options:
#   -h, --help     Show this help message
#   -v, --verbose  Enable verbose output
#   -q, --quiet    Suppress non-essential output
#   -r, --refresh  Auto-refresh every 30 seconds (Ctrl+C to stop)

set -euo pipefail

# Source the centralized configuration
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/config.sh"

# Script-specific variables
VERBOSE=false
QUIET=false
REFRESH=false

# Function to display help
show_help() {
    cat << EOF
Job Progress Monitor

USAGE:
    $0 [OPTIONS]

OPTIONS:
    -h, --help     Show this help message and exit
    -v, --verbose  Enable verbose output with detailed information
    -q, --quiet    Suppress non-essential output (errors only)
    -r, --refresh  Auto-refresh display every 30 seconds

DESCRIPTION:
    This script provides comprehensive monitoring of SLURM jobs, including:
    - Real-time job status (running, pending, completed, failed)
    - Progress calculation and completion percentage
    - Performance metrics (tasks per hour)
    - Estimated time remaining (ETR)
    - Resource utilization summary

EXAMPLES:
    $0                 # Show current job status
    $0 -v              # Show detailed status information
    $0 -r              # Auto-refresh every 30 seconds
    $0 -q              # Show only essential information

EOF
}

# Function to parse command line arguments
parse_args() {
    while [[ $# -gt 0 ]]; do
        case $1 in
            -h|--help)
                show_help
                exit 0
                ;;
            -v|--verbose)
                VERBOSE=true
                shift
                ;;
            -q|--quiet)
                QUIET=true
                shift
                ;;
            -r|--refresh)
                REFRESH=true
                shift
                ;;
            *)
                print_colored "$COLOR_RED" "Unknown option: $1"
                echo "Use -h or --help for usage information."
                exit 1
                ;;
        esac
    done
}

# Function to get current running jobs
get_running_jobs() {
    local job_ids=()
    
    # Try to find jobs by common patterns
    for pattern in "${JOB_NAME_PATTERNS[@]}"; do
        while IFS= read -r job_id; do
            if [[ -n "$job_id" && "$job_id" =~ ^[0-9]+$ ]]; then
                job_ids+=("$job_id")
            fi
        done < <(squeue -u "$SLURM_USER" -h -o "%i" --name="$pattern" 2>/dev/null || true)
    done
    
    # If no jobs found by pattern, get all user jobs
    if [ ${#job_ids[@]} -eq 0 ]; then
        while IFS= read -r job_id; do
            if [[ -n "$job_id" && "$job_id" =~ ^[0-9]+$ ]]; then
                job_ids+=("$job_id")
            fi
        done < <(squeue -u "$SLURM_USER" -h -o "%i" 2>/dev/null || true)
    fi
    
    printf '%s\n' "${job_ids[@]}"
}

# Function to get job statistics from sacct
get_job_stats() {
    local job_id=$1
    local stats_output
    
    # Get comprehensive job statistics
    stats_output=$(sacct -j "$job_id" --format=JobID,State,ExitCode,Start,End,CPUTime,MaxRSS,NodeList --parsable2 --noheader 2>/dev/null || echo "")
    
    if [[ -z "$stats_output" ]]; then
        return 1
    fi
    
    echo "$stats_output"
}

# Function to analyze job array progress
analyze_job_array() {
    local job_id=$1
    local completed=0
    local failed=0
    local running=0
    local pending=0
    local total=0
    
    # Get array job status
    while IFS='|' read -r subjob_id state exit_code start_time end_time cpu_time max_rss node_list; do
        if [[ -z "$subjob_id" ]]; then
            continue
        fi
        
        total=$((total + 1))
        
        case "$state" in
            "COMPLETED")
                completed=$((completed + 1))
                ;;
            "FAILED"|"CANCELLED"|"TIMEOUT"|"OUT_OF_MEMORY")
                failed=$((failed + 1))
                ;;
            "RUNNING")
                running=$((running + 1))
                ;;
            "PENDING"|"CONFIGURING")
                pending=$((pending + 1))
                ;;
        esac
        
        if [[ "$VERBOSE" == true && -n "$state" ]]; then
            printf "  Task %s: %s" "$subjob_id" "$state"
            if [[ -n "$exit_code" && "$exit_code" != "0:0" ]]; then
                printf " (Exit: %s)" "$exit_code"
            fi
            if [[ -n "$node_list" ]]; then
                printf " [%s]" "$node_list"
            fi
            echo
        fi
    done < <(get_job_stats "$job_id")
    
    echo "$completed $failed $running $pending $total"
}

# Function to calculate completion rate and ETA
calculate_eta() {
    local completed=$1
    local total=$2
    local start_time=$3
    
    if [[ $completed -eq 0 || -z "$start_time" ]]; then
        echo "N/A"
        return
    fi
    
    # Calculate elapsed time in seconds
    local current_time
    current_time=$(date +%s)
    local start_timestamp
    start_timestamp=$(date -d "$start_time" +%s 2>/dev/null || echo "$current_time")
    local elapsed=$((current_time - start_timestamp))
    
    if [[ $elapsed -le 0 ]]; then
        echo "N/A"
        return
    fi
    
    # Calculate completion rate (tasks per second)
    local rate
    rate=$(echo "scale=6; $completed / $elapsed" | bc -l 2>/dev/null || echo "0")
    
    if [[ $(echo "$rate <= 0" | bc -l 2>/dev/null || echo "1") -eq 1 ]]; then
        echo "N/A"
        return
    fi
    
    # Calculate remaining time
    local remaining=$((total - completed))
    local eta_seconds
    eta_seconds=$(echo "scale=0; $remaining / $rate" | bc -l 2>/dev/null || echo "0")
    
    # Format ETA
    if [[ $eta_seconds -lt 3600 ]]; then
        printf "%dm" $((eta_seconds / 60))
    elif [[ $eta_seconds -lt 86400 ]]; then
        printf "%dh %dm" $((eta_seconds / 3600)) $(((eta_seconds % 3600) / 60))
    else
        printf "%dd %dh" $((eta_seconds / 86400)) $(((eta_seconds % 86400) / 3600))
    fi
}

# Function to display job progress
display_progress() {
    local job_id=$1
    local job_name=$2
    local completed=$3
    local failed=$4
    local running=$5
    local pending=$6
    local total=$7
    local start_time=$8
    
    if [[ "$QUIET" == true ]]; then
        if [[ $total -gt 0 ]]; then
            local percentage=$((completed * 100 / total))
            echo "Job $job_id: $percentage% complete ($completed/$total)"
        else
            echo "Job $job_id: No tasks found"
        fi
        return
    fi
    
    print_colored "$COLOR_BLUE" "=== Job $job_id${job_name:+ ($job_name)} ==="
    
    if [[ $total -eq 0 ]]; then
        print_colored "$COLOR_YELLOW" "No array tasks found for this job"
        return
    fi
    
    # Display progress bar
    print_colored "$COLOR_CYAN" "Progress:"
    print_progress_bar "$completed" "$total"
    
    # Display status breakdown
    echo
    print_colored "$COLOR_GREEN" "✓ Completed: $completed"
    if [[ $failed -gt 0 ]]; then
        print_colored "$COLOR_RED" "✗ Failed: $failed"
    else
        print_colored "$COLOR_GREEN" "✗ Failed: $failed"
    fi
    print_colored "$COLOR_YELLOW" "⚡ Running: $running"
    print_colored "$COLOR_PURPLE" "⏳ Pending: $pending"
    
    # Calculate and display performance metrics
    if [[ $completed -gt 0 && -n "$start_time" ]]; then
        local eta
        eta=$(calculate_eta "$completed" "$total" "$start_time")
        
        echo
        print_colored "$COLOR_CYAN" "Performance:"
        echo "  Estimated Time Remaining: $eta"
        
        # Calculate completion rate
        local current_time
        current_time=$(date +%s)
        local start_timestamp
        start_timestamp=$(date -d "$start_time" +%s 2>/dev/null || echo "$current_time")
        local elapsed=$((current_time - start_timestamp))
        
        if [[ $elapsed -gt 0 ]]; then
            local rate_per_hour
            rate_per_hour=$(echo "scale=1; $completed * 3600 / $elapsed" | bc -l 2>/dev/null || echo "0")
            echo "  Completion Rate: $rate_per_hour tasks/hour"
        fi
    fi
    
    echo
}

# Function to get job start time
get_job_start_time() {
    local job_id=$1
    
    # Get the earliest start time from the job array
    sacct -j "$job_id" --format=Start --parsable2 --noheader 2>/dev/null | \
        grep -v "Unknown" | \
        head -n 1 | \
        tr -d '|'
}

# Function to get job name
get_job_name() {
    local job_id=$1
    
    squeue -j "$job_id" -h -o "%j" 2>/dev/null | head -n 1
}

# Main monitoring function
monitor_jobs() {
    local job_ids
    readarray -t job_ids < <(get_running_jobs)
    
    if [[ ${#job_ids[@]} -eq 0 ]]; then
        print_colored "$COLOR_YELLOW" "No active jobs found for user $SLURM_USER"
        return
    fi
    
    print_colored "$COLOR_BLUE" "=== SLURM Job Progress Monitor ==="
    print_colored "$COLOR_CYAN" "Timestamp: $(get_timestamp)"
    print_colored "$COLOR_CYAN" "Working Directory: $WORK_DIR"
    print_colored "$COLOR_CYAN" "User: $SLURM_USER"
    echo
    
    for job_id in "${job_ids[@]}"; do
        if [[ -z "$job_id" ]]; then
            continue
        fi
        
        local job_name
        job_name=$(get_job_name "$job_id")
        
        local start_time
        start_time=$(get_job_start_time "$job_id")
        
        local stats
        stats=$(analyze_job_array "$job_id")
        
        if [[ -n "$stats" ]]; then
            read -r completed failed running pending total <<< "$stats"
            display_progress "$job_id" "$job_name" "$completed" "$failed" "$running" "$pending" "$total" "$start_time"
        else
            print_colored "$COLOR_YELLOW" "Could not retrieve statistics for job $job_id"
            echo
        fi
    done
}

# Main execution
main() {
    parse_args "$@"
    
    if [[ "$REFRESH" == true ]]; then
        print_colored "$COLOR_GREEN" "Auto-refresh mode enabled. Press Ctrl+C to stop."
        echo
        
        while true; do
            clear
            monitor_jobs
            sleep 30
        done
    else
        monitor_jobs
    fi
}

# Execute main function with all arguments
main "$@"
