#!/bin/bash

# Enhanced CLI Progress Monitor for Common Crawl Processing
# Pure command-line tool with advanced progress tracking and trend analysis
# Usage: ./progress-monitor-cli.sh [OPTIONS]

set -euo pipefail

# Colors for output
RED=$(tput setaf 1 2>/dev/null || echo "")
GREEN=$(tput setaf 2 2>/dev/null || echo "")
YELLOW=$(tput setaf 3 2>/dev/null || echo "")
BLUE=$(tput setaf 4 2>/dev/null || echo "")
MAGENTA=$(tput setaf 5 2>/dev/null || echo "")
CYAN=$(tput setaf 6 2>/dev/null || echo "")
BOLD=$(tput bold 2>/dev/null || echo "")
DIM=$(tput dim 2>/dev/null || echo "")
RESET=$(tput sgr0 2>/dev/null || echo "")

# Default configuration
MONITOR_INTERVAL=30
LOG_FILE=""
PROGRESS_FILE="progress.log"
HISTORY_FILE="progress_history.log"
QUIET_MODE=false
VERBOSE_MODE=false
JOB_ID=""
AUTO_DETECT=true
OUTPUT_FORMAT="detailed"
NO_COLOR=false
SHOW_TRENDS=false
EXPORT_FILE=""
OUTPUT_FILE=""

# Progress tracking variables
LAST_TOTAL_SEGMENTS=0
LAST_PROCESSED_SEGMENTS=0
START_TIME=0
COMPLETION_TIMES=()
PROGRESS_HISTORY=()

# Function to print usage
usage() {
    cat << EOF
${BOLD}Enhanced CLI Progress Monitor for Common Crawl Processing${RESET}

${BOLD}Usage:${RESET} $0 [OPTIONS]

${BOLD}Options:${RESET}
    ${GREEN}-i, --interval SECONDS${RESET}     Monitoring interval in seconds (default: 30)
    ${GREEN}-j, --job-id JOB_ID${RESET}       Specific SLURM job ID to monitor
    ${GREEN}-f, --format FORMAT${RESET}       Output format: detailed|compact|json (default: detailed)
    ${GREEN}-l, --log-file FILE${RESET}       Log progress to specific file
    ${GREEN}-o, --output FILE${RESET}         Save progress snapshots to file
    ${GREEN}-p, --progress-file FILE${RESET}  Progress state file (default: progress.log)
    ${GREEN}-q, --quiet${RESET}              Minimal output mode
    ${GREEN}-v, --verbose${RESET}            Extra detailed output with trends
    ${GREEN}--no-color${RESET}               Disable colored output
    ${GREEN}--show-trends${RESET}            Show performance trends and rates
    ${GREEN}--export-snapshot FILE${RESET}   Export current state to JSON file
    ${GREEN}-h, --help${RESET}               Show this help message

${BOLD}Output Formats:${RESET}
    ${CYAN}detailed${RESET}    Full progress display with colors and progress bars
    ${CYAN}compact${RESET}     Condensed single-line output
    ${CYAN}json${RESET}        Machine-readable JSON output

${BOLD}Examples:${RESET}
    $0                                    # Monitor with default detailed view
    $0 -i 60 -j 12345 --show-trends      # Monitor job 12345 with trends
    $0 -f compact -q                     # Quiet compact monitoring
    $0 --format json -o progress.json    # JSON output to file
    $0 -v --show-trends                  # Verbose mode with performance analysis

EOF
}

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        -i|--interval)
            MONITOR_INTERVAL="$2"
            shift 2
            ;;
        -j|--job-id)
            JOB_ID="$2"
            AUTO_DETECT=false
            shift 2
            ;;
        -f|--format)
            OUTPUT_FORMAT="$2"
            if [[ ! "$OUTPUT_FORMAT" =~ ^(detailed|compact|json)$ ]]; then
                echo "${RED}Error: Invalid format '$OUTPUT_FORMAT'. Use: detailed, compact, or json${RESET}" >&2
                exit 1
            fi
            shift 2
            ;;
        -l|--log-file)
            LOG_FILE="$2"
            shift 2
            ;;
        -o|--output)
            OUTPUT_FILE="$2"
            shift 2
            ;;
        -p|--progress-file)
            PROGRESS_FILE="$2"
            shift 2
            ;;
        -q|--quiet)
            QUIET_MODE=true
            shift
            ;;
        -v|--verbose)
            VERBOSE_MODE=true
            SHOW_TRENDS=true
            shift
            ;;
        --no-color)
            NO_COLOR=true
            # Clear all color variables
            RED="" GREEN="" YELLOW="" BLUE="" MAGENTA="" CYAN="" BOLD="" DIM="" RESET=""
            shift
            ;;
        --show-trends)
            SHOW_TRENDS=true
            shift
            ;;
        --export-snapshot)
            EXPORT_FILE="$2"
            shift 2
            ;;
        -h|--help)
            usage
            exit 0
            ;;
        *)
            echo "${RED}Unknown option: $1${RESET}" >&2
            usage
            exit 1
            ;;
    esac
done

# Load environment variables if available
if [[ -f "runme.sh" ]]; then
    source runme.sh 2>/dev/null || true
fi

# Set up paths
WORKING_DIR="${WORKING_DIR:-.}"
OUTPUT_DIR="${WORKING_DIR}/output"
CRAWL_DATA_FILE="${WORKING_DIR}/crawl_data.txt"
PROGRESS_LOG="${WORKING_DIR}/${PROGRESS_FILE}"
HISTORY_LOG="${WORKING_DIR}/${HISTORY_FILE}"

# Create output directory if it doesn't exist
mkdir -p "$OUTPUT_DIR"

# Function to log messages
log_message() {
    local level="$1"
    local message="$2"
    local timestamp=$(date '+%Y-%m-%d %H:%M:%S')

    if [[ "$QUIET_MODE" != "true" ]] && [[ "$OUTPUT_FORMAT" != "json" ]]; then
        echo "[$timestamp] $level: $message" >&2
    fi

    if [[ -n "$LOG_FILE" ]]; then
        echo "[$timestamp] $level: $message" >> "${WORKING_DIR}/${LOG_FILE}"
    fi
}

# Function to get SLURM job status
get_job_status() {
    local status="UNKNOWN"
    
    if [[ -n "$JOB_ID" ]]; then
        status=$(squeue -h -j "$JOB_ID" -o "%T" 2>/dev/null || echo "NOT_FOUND")
    else
        # Get the most recent job for this user
        local job_info=$(squeue -h -u "$USER" -o "%i %T" 2>/dev/null | head -1)
        if [[ -n "$job_info" ]]; then
            status=$(echo "$job_info" | awk '{print $2}')
        else
            status="NO_JOBS"
        fi
    fi
    
    echo "$status"
}

# Function to get active job ID
get_active_job_id() {
    if [[ -n "$JOB_ID" ]]; then
        echo "$JOB_ID"
    else
        # Get the most recent running job
        squeue -h -u "$USER" -o "%i" 2>/dev/null | head -1 || echo ""
    fi
}

# Function to calculate total segments
get_total_segments() {
    local total=0

    if [[ -f "$CRAWL_DATA_FILE" ]]; then
        while read -r date n_files; do
            # Skip comments and empty lines
            [[ $date =~ ^#.*$ ]] && continue
            [[ -z "$date" ]] && continue

            if [[ "$n_files" =~ ^[0-9]+$ ]]; then
                total=$((total + n_files))
            fi
        done < "$CRAWL_DATA_FILE"
    fi

    echo "$total"
}

# Function to get processed segments
get_processed_segments() {
    local count=0

    if [[ -d "$OUTPUT_DIR" ]]; then
        # Count non-empty parquet files
        while IFS= read -r -d '' file; do
            [[ -s "$file" ]] && count=$((count + 1))
        done < <(find "$OUTPUT_DIR" -name "crawldata*.parquet" -print0 2>/dev/null)
    fi

    echo "$count"
}

# Function to get current crawl date being processed
get_current_crawl_date() {
    local current_date=""

    if [[ -f "$CRAWL_DATA_FILE" ]]; then
        while read -r date n_files; do
            # Skip comments and empty lines
            [[ $date =~ ^#.*$ ]] && continue
            [[ -z "$date" ]] && continue

            if [[ "$n_files" =~ ^[0-9]+$ ]]; then
                local expected_files=$n_files
                local processed_files=0

                # Count processed files for this date
                while IFS= read -r -d '' file; do
                    [[ -s "$file" ]] && processed_files=$((processed_files + 1))
                done < <(find "$OUTPUT_DIR" -name "crawldata${date}*.parquet" -print0 2>/dev/null)

                if [[ $processed_files -lt $expected_files ]]; then
                    current_date="$date"
                    break
                fi
            fi
        done < "$CRAWL_DATA_FILE"
    fi

    echo "$current_date"
}

# Function to get current date progress details
get_current_date_progress() {
    local current_date="$1"
    local total=0
    local processed=0

    if [[ -n "$current_date" ]] && [[ -f "$CRAWL_DATA_FILE" ]]; then
        # Get expected count for current date
        while read -r date n_files; do
            [[ $date =~ ^#.*$ ]] && continue
            [[ -z "$date" ]] && continue
            if [[ "$date" == "$current_date" && "$n_files" =~ ^[0-9]+$ ]]; then
                total="$n_files"
                break
            fi
        done < "$CRAWL_DATA_FILE"

        # Count processed files for current date
        if [[ -d "$OUTPUT_DIR" ]]; then
            while IFS= read -r -d '' file; do
                [[ -s "$file" ]] && processed=$((processed + 1))
            done < <(find "$OUTPUT_DIR" -name "crawldata${current_date}*.parquet" -print0 2>/dev/null)
        fi
    fi

    echo "$processed $total"
}

# Function to calculate processing rate and trends
calculate_trends() {
    local processed="$1"
    local elapsed_time="$2"
    
    # Calculate current rate (segments per hour)
    local current_rate=0
    if [[ $elapsed_time -gt 0 ]]; then
        current_rate=$(echo "scale=2; $processed * 3600 / $elapsed_time" | bc -l 2>/dev/null || echo "0")
    fi

    # Calculate recent rate (last 10 measurements)
    local recent_rate=0
    if [[ ${#PROGRESS_HISTORY[@]} -gt 1 ]]; then
        local recent_count=10
        local start_idx=$((${#PROGRESS_HISTORY[@]} - recent_count))
        [[ $start_idx -lt 0 ]] && start_idx=0
        
        local recent_start_time=$(echo "${PROGRESS_HISTORY[$start_idx]}" | cut -d',' -f1)
        local recent_start_processed=$(echo "${PROGRESS_HISTORY[$start_idx]}" | cut -d',' -f2)
        local recent_elapsed=$((elapsed_time - recent_start_time))
        local recent_processed_diff=$((processed - recent_start_processed))
        
        if [[ $recent_elapsed -gt 0 ]]; then
            recent_rate=$(echo "scale=2; $recent_processed_diff * 3600 / $recent_elapsed" | bc -l 2>/dev/null || echo "0")
        fi
    fi

    echo "$current_rate $recent_rate"
}

# Function to calculate ETA with improved accuracy
calculate_eta() {
    local processed="$1"
    local total="$2"
    local elapsed_time="$3"

    if [[ $processed -eq 0 ]] || [[ $elapsed_time -eq 0 ]]; then
        echo "Unknown"
        return
    fi

    # Use recent rate if available, otherwise use overall rate
    local rates=($(calculate_trends "$processed" "$elapsed_time"))
    local rate=${rates[1]:-${rates[0]}}  # Use recent rate if available
    
    if [[ $(echo "$rate <= 0" | bc -l 2>/dev/null || echo "1") -eq 1 ]]; then
        echo "Unknown"
        return
    fi

    local remaining=$((total - processed))
    local eta_hours=$(echo "scale=2; $remaining / $rate" | bc -l 2>/dev/null || echo "0")
    local eta_seconds=$(echo "scale=0; $eta_hours * 3600" | bc -l 2>/dev/null || echo "0")

    # Convert to human readable format
    local days=$((eta_seconds / 86400))
    local hours=$(( (eta_seconds % 86400) / 3600 ))
    local minutes=$(( (eta_seconds % 3600) / 60 ))

    if [[ $days -gt 0 ]]; then
        echo "${days}d ${hours}h ${minutes}m"
    elif [[ $hours -gt 0 ]]; then
        echo "${hours}h ${minutes}m"
    else
        echo "${minutes}m"
    fi
}

# Function to create progress bar
create_progress_bar() {
    local percentage="$1"
    local width="${2:-50}"
    local filled=$((percentage * width / 100))
    local empty=$((width - filled))

    printf "["
    printf "%0.s${GREEN}█${RESET}" $(seq 1 $filled)
    printf "%0.s${DIM}░${RESET}" $(seq 1 $empty)
    printf "]"
}

# Function to format numbers with commas
format_number() {
    printf "%'d" "$1" 2>/dev/null || echo "$1"
}

# Function to get status color and symbol
get_status_display() {
    local status="$1"
    case "$status" in
        "RUNNING")
            echo "${GREEN}●${RESET} ${GREEN}RUNNING${RESET}"
            ;;
        "PENDING")
            echo "${YELLOW}●${RESET} ${YELLOW}PENDING${RESET}"
            ;;
        "COMPLETED"|"COMPLETING")
            echo "${BLUE}●${RESET} ${BLUE}COMPLETED${RESET}"
            ;;
        "FAILED"|"CANCELLED"|"TIMEOUT")
            echo "${RED}●${RESET} ${RED}$status${RESET}"
            ;;
        "NO_JOBS")
            echo "${DIM}○${RESET} ${DIM}NO ACTIVE JOBS${RESET}"
            ;;
        *)
            echo "${DIM}?${RESET} ${DIM}$status${RESET}"
            ;;
    esac
}

# Function to save progress state
save_progress_state() {
    local total="$1"
    local processed="$2"
    local current_date="$3"
    local job_status="$4"
    local active_job_id="$5"
    local elapsed_time="$6"
    local eta="$7"

    local percentage=0
    [[ $total -gt 0 ]] && percentage=$((processed * 100 / total))

    cat > "$PROGRESS_LOG" << EOF
TIMESTAMP=$(date '+%Y-%m-%d %H:%M:%S')
TOTAL_SEGMENTS=$total
PROCESSED_SEGMENTS=$processed
CURRENT_CRAWL_DATE=$current_date
JOB_STATUS=$job_status
ACTIVE_JOB_ID=$active_job_id
COMPLETION_PERCENTAGE=$percentage
ELAPSED_TIME=$elapsed_time
ETA=$eta
EOF
}

# Function to log progress to history
log_progress_history() {
    local timestamp="$1"
    local processed="$2"
    local total="$3"
    local percentage="$4"
    local current_date="$5"
    local elapsed_time="$6"

    echo "[$timestamp] PROCESSED=$processed/$total (${percentage}%) CURRENT_DATE=$current_date ELAPSED=${elapsed_time}s" >> "$HISTORY_LOG"
    
    # Add to in-memory history for trend calculation
    PROGRESS_HISTORY+=("$elapsed_time,$processed,$total")
    
    # Keep only last 100 entries in memory
    if [[ ${#PROGRESS_HISTORY[@]} -gt 100 ]]; then
        PROGRESS_HISTORY=("${PROGRESS_HISTORY[@]:50}")
    fi
}

# Function to export JSON snapshot
export_json_snapshot() {
    local file="$1"
    local total="$2"
    local processed="$3"
    local current_date="$4"
    local job_status="$5"
    local active_job_id="$6"
    local elapsed_time="$7"
    local eta="$8"
    
    local percentage=0
    [[ $total -gt 0 ]] && percentage=$((processed * 100 / total))
    
    local current_date_info=($(get_current_date_progress "$current_date"))
    local current_processed=${current_date_info[0]:-0}
    local current_total=${current_date_info[1]:-0}
    
    local rates=($(calculate_trends "$processed" "$elapsed_time"))
    local overall_rate=${rates[0]:-0}
    local recent_rate=${rates[1]:-0}

    cat > "$file" << EOF
{
  "timestamp": "$(date -Iseconds)",
  "overall_progress": {
    "total_segments": $total,
    "processed_segments": $processed,
    "remaining_segments": $((total - processed)),
    "completion_percentage": $percentage,
    "eta": "$eta"
  },
  "current_crawl_date": {
    "date": "$current_date",
    "processed_segments": $current_processed,
    "total_segments": $current_total,
    "completion_percentage": $([[ $current_total -gt 0 ]] && echo "$((current_processed * 100 / current_total))" || echo "0")
  },
  "job_info": {
    "active_job_id": "$active_job_id",
    "status": "$job_status",
    "elapsed_time_seconds": $elapsed_time
  },
  "performance": {
    "overall_rate_per_hour": $overall_rate,
    "recent_rate_per_hour": $recent_rate
  }
}
EOF
}

# Function to display detailed progress
display_detailed_progress() {
    local total="$1"
    local processed="$2"
    local current_date="$3"
    local job_status="$4"
    local active_job_id="$5"
    local elapsed_time="$6"
    local eta="$7"

    # Clear screen and move to top
    echo -e "\033[2J\033[H"

    echo "${BOLD}${BLUE}╔══════════════════════════════════════════════════════════════════════════════╗${RESET}"
    echo "${BOLD}${BLUE}║                    Common Crawl Progress Monitor                             ║${RESET}"
    echo "${BOLD}${BLUE}╚══════════════════════════════════════════════════════════════════════════════╝${RESET}"
    echo "${DIM}Last updated: $(date '+%Y-%m-%d %H:%M:%S')${RESET}"
    echo ""

    # Overall progress
    if [[ $total -gt 0 ]]; then
        local percentage=$((processed * 100 / total))
        local remaining=$((total - processed))

        echo "${BOLD}${CYAN}📊 Overall Progress${RESET}"
        echo "   Total segments: $(format_number $total)"
        echo "   Processed: ${GREEN}$(format_number $processed)${RESET}"
        echo "   Remaining: ${YELLOW}$(format_number $remaining)${RESET}"
        echo "   Progress: ${BOLD}${percentage}%${RESET} $(create_progress_bar $percentage)"
        echo "   ETA: ${MAGENTA}$eta${RESET}"
        echo ""
    fi

    # Current crawl date progress
    if [[ -n "$current_date" ]]; then
        local current_info=($(get_current_date_progress "$current_date"))
        local current_processed=${current_info[0]:-0}
        local current_total=${current_info[1]:-0}

        echo "${BOLD}${CYAN}🔄 Current Crawl Date: ${YELLOW}$current_date${RESET}"
        
        if [[ $current_total -gt 0 ]]; then
            local current_percentage=$((current_processed * 100 / current_total))
            echo "   Date segments: ${GREEN}$(format_number $current_processed)${RESET}/${YELLOW}$(format_number $current_total)${RESET} (${current_percentage}%)"
            echo "   Progress: $(create_progress_bar $current_percentage 30)"
        else
            echo "   Date segments: ${DIM}No data available${RESET}"
        fi
        echo ""
    fi

    # Job status
    echo "${BOLD}${CYAN}⚙️  Job Status${RESET}"
    echo "   Active job ID: ${active_job_id:-${DIM}None${RESET}}"
    echo "   Status: $(get_status_display "$job_status")"
    echo "   Elapsed time: ${BLUE}$(format_duration $elapsed_time)${RESET}"
    echo ""

    # Performance trends (if enabled)
    if [[ "$SHOW_TRENDS" == "true" ]] && [[ $processed -gt 0 ]]; then
        local rates=($(calculate_trends "$processed" "$elapsed_time"))
        local overall_rate=${rates[0]:-0}
        local recent_rate=${rates[1]:-0}
        
        echo "${BOLD}${CYAN}📈 Performance Trends${RESET}"
        echo "   Overall rate: ${GREEN}$(printf "%.1f" "$overall_rate")${RESET} segments/hour"
        if [[ $(echo "$recent_rate > 0" | bc -l 2>/dev/null || echo "0") -eq 1 ]]; then
            echo "   Recent rate: ${GREEN}$(printf "%.1f" "$recent_rate")${RESET} segments/hour"
            
            # Show trend direction
            local trend_symbol="→"
            local trend_color="$RESET"
            if [[ $(echo "$recent_rate > $overall_rate * 1.1" | bc -l 2>/dev/null || echo "0") -eq 1 ]]; then
                trend_symbol="↗"
                trend_color="$GREEN"
            elif [[ $(echo "$recent_rate < $overall_rate * 0.9" | bc -l 2>/dev/null || echo "0") -eq 1 ]]; then
                trend_symbol="↘"
                trend_color="$RED"
            fi
            echo "   Trend: ${trend_color}${trend_symbol} $(printf "%.1f%%" "$(echo "scale=1; ($recent_rate - $overall_rate) * 100 / $overall_rate" | bc -l 2>/dev/null || echo "0")")${RESET}"
        fi
        echo ""
    fi

    # System info
    echo "${BOLD}${CYAN}ℹ️  System Info${RESET}"
    echo "   Monitoring interval: ${MONITOR_INTERVAL}s"
    echo "   Output format: $OUTPUT_FORMAT"
    if [[ -n "$LOG_FILE" ]]; then
        echo "   Log file: $LOG_FILE"
    fi
    if [[ -n "$OUTPUT_FILE" ]]; then
        echo "   Output file: $OUTPUT_FILE"
    fi
    echo ""
}

# Function to display compact progress
display_compact_progress() {
    local total="$1"
    local processed="$2"
    local current_date="$3"
    local job_status="$4"
    local active_job_id="$5"
    local elapsed_time="$6"
    local eta="$7"

    local percentage=0
    [[ $total -gt 0 ]] && percentage=$((processed * 100 / total))

    local status_symbol="?"
    case "$job_status" in
        "RUNNING") status_symbol="${GREEN}●${RESET}" ;;
        "PENDING") status_symbol="${YELLOW}●${RESET}" ;;
        "COMPLETED"|"COMPLETING") status_symbol="${BLUE}●${RESET}" ;;
        "FAILED"|"CANCELLED"|"TIMEOUT") status_symbol="${RED}●${RESET}" ;;
        *) status_symbol="${DIM}○${RESET}" ;;
    esac

    printf "[%s] %s %d%% (%s/%s) | %s | ETA: %s | Job: %s %s\n" \
        "$(date '+%H:%M:%S')" \
        "$status_symbol" \
        "$percentage" \
        "$(format_number $processed)" \
        "$(format_number $total)" \
        "${current_date:-N/A}" \
        "$eta" \
        "${active_job_id:-None}" \
        "$job_status"
}

# Function to display JSON progress
display_json_progress() {
    local total="$1"
    local processed="$2"
    local current_date="$3"
    local job_status="$4"
    local active_job_id="$5"
    local elapsed_time="$6"
    local eta="$7"

    export_json_snapshot "/dev/stdout" "$total" "$processed" "$current_date" "$job_status" "$active_job_id" "$elapsed_time" "$eta"
}

# Function to format duration
format_duration() {
    local seconds="$1"
    local days=$((seconds / 86400))
    local hours=$(( (seconds % 86400) / 3600 ))
    local minutes=$(( (seconds % 3600) / 60 ))
    local secs=$((seconds % 60))

    if [[ $days -gt 0 ]]; then
        echo "${days}d ${hours}h ${minutes}m"
    elif [[ $hours -gt 0 ]]; then
        echo "${hours}h ${minutes}m"
    elif [[ $minutes -gt 0 ]]; then
        echo "${minutes}m ${secs}s"
    else
        echo "${secs}s"
    fi
}

# Function to cleanup
cleanup() {
    if [[ "$OUTPUT_FORMAT" == "detailed" ]] && [[ "$QUIET_MODE" != "true" ]]; then
        echo ""
        echo "${DIM}Progress monitoring stopped.${RESET}"
    fi
    exit 0
}

# Trap to cleanup on exit
trap cleanup EXIT INT TERM

# Main monitoring loop
main() {
    log_message "INFO" "Starting enhanced CLI progress monitor (interval: ${MONITOR_INTERVAL}s, format: $OUTPUT_FORMAT)"

    # Initialize start time
    START_TIME=$(date +%s)

    # Load existing history if available
    if [[ -f "$HISTORY_LOG" ]]; then
        # Load last 50 entries for trend calculation
        local history_lines=($(tail -50 "$HISTORY_LOG" | grep -o 'ELAPSED=[0-9]*s' | sed 's/ELAPSED=//;s/s//' || true))
        local processed_lines=($(tail -50 "$HISTORY_LOG" | grep -o 'PROCESSED=[0-9]*' | sed 's/PROCESSED=//' || true))
        
        for i in "${!history_lines[@]}"; do
            if [[ -n "${history_lines[$i]}" ]] && [[ -n "${processed_lines[$i]}" ]]; then
                PROGRESS_HISTORY+=("${history_lines[$i]},${processed_lines[$i]},0")
            fi
        done
    fi

    while true; do
        local timestamp=$(date '+%Y-%m-%d %H:%M:%S')
        local active_job_id=$(get_active_job_id)
        local job_status=$(get_job_status)
        local total_segments=$(get_total_segments)
        local processed_segments=$(get_processed_segments)
        local current_date=$(get_current_crawl_date)
        local elapsed_time=$(( $(date +%s) - START_TIME ))

        # Calculate progress metrics
        local percentage=0
        if [[ $total_segments -gt 0 ]]; then
            percentage=$((processed_segments * 100 / total_segments))
        fi

        # Calculate ETA
        local eta="Unknown"
        if [[ $processed_segments -gt 0 ]] && [[ $elapsed_time -gt 0 ]]; then
            eta=$(calculate_eta "$processed_segments" "$total_segments" "$elapsed_time")
        fi

        # Save progress state
        save_progress_state "$total_segments" "$processed_segments" "$current_date" "$job_status" "$active_job_id" "$elapsed_time" "$eta"

        # Log to history
        log_progress_history "$timestamp" "$processed_segments" "$total_segments" "$percentage" "$current_date" "$elapsed_time"

        # Display progress based on format
        if [[ "$QUIET_MODE" != "true" ]]; then
            case "$OUTPUT_FORMAT" in
                "detailed")
                    display_detailed_progress "$total_segments" "$processed_segments" "$current_date" "$job_status" "$active_job_id" "$elapsed_time" "$eta"
                    ;;
                "compact")
                    display_compact_progress "$total_segments" "$processed_segments" "$current_date" "$job_status" "$active_job_id" "$elapsed_time" "$eta"
                    ;;
                "json")
                    display_json_progress "$total_segments" "$processed_segments" "$current_date" "$job_status" "$active_job_id" "$elapsed_time" "$eta"
                    ;;
            esac
        fi

        # Save to output file if specified
        if [[ -n "$OUTPUT_FILE" ]]; then
            case "$OUTPUT_FORMAT" in
                "json")
                    export_json_snapshot "$OUTPUT_FILE" "$total_segments" "$processed_segments" "$current_date" "$job_status" "$active_job_id" "$elapsed_time" "$eta"
                    ;;
                *)
                    echo "[$timestamp] $percentage% ($processed_segments/$total_segments) | $current_date | $job_status | ETA: $eta" >> "$OUTPUT_FILE"
                    ;;
            esac
        fi

        # Export snapshot if requested
        if [[ -n "$EXPORT_FILE" ]]; then
            export_json_snapshot "$EXPORT_FILE" "$total_segments" "$processed_segments" "$current_date" "$job_status" "$active_job_id" "$elapsed_time" "$eta"
            log_message "INFO" "Progress snapshot exported to $EXPORT_FILE"
            EXPORT_FILE=""  # Only export once
        fi

        # Check if job is complete
        if [[ "$job_status" != "RUNNING" ]] && [[ "$job_status" != "PENDING" ]] && [[ $processed_segments -gt 0 ]]; then
            log_message "INFO" "Job completed or not running. Final status: $job_status. Processed: $processed_segments/$total_segments (${percentage}%)"
            
            # Final display
            if [[ "$QUIET_MODE" != "true" ]] && [[ "$OUTPUT_FORMAT" == "detailed" ]]; then
                echo ""
                echo "${BOLD}${GREEN}✓ Processing Complete!${RESET}"
                echo "   Final progress: ${BOLD}${percentage}%${RESET} ($(format_number $processed_segments)/$(format_number $total_segments) segments)"
                echo "   Total time: ${BLUE}$(format_duration $elapsed_time)${RESET}"
                if [[ $processed_segments -gt 0 ]]; then
                    local final_rate=$(echo "scale=2; $processed_segments * 3600 / $elapsed_time" | bc -l 2>/dev/null || echo "0")
                    echo "   Average rate: ${GREEN}$(printf "%.1f" "$final_rate")${RESET} segments/hour"
                fi
                echo ""
            fi
            break
        fi

        # Wait for next update
        sleep "$MONITOR_INTERVAL"
    done

    log_message "INFO" "Enhanced CLI progress monitoring completed"
}

# Run main function
main "$@"
