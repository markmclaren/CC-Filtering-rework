#!/bin/bash

# Real-time Progress Monitor for Common Crawl Processing
# Monitors SLURM job progress with percentage completion and ETA
# Usage: ./progress-monitor.sh [OPTIONS]

set -euo pipefail

# Colors for output
RED=$(tput setaf 1)
GREEN=$(tput setaf 2)
YELLOW=$(tput setaf 3)
BLUE=$(tput setaf 4)
BOLD=$(tput bold)
RESET=$(tput sgr0)

# Default configuration
MONITOR_INTERVAL=30
LOG_FILE=""
PROGRESS_FILE="progress.log"
HISTORY_FILE="progress_history.log"
DASHBOARD_PORT=8080
QUIET_MODE=false
JOB_ID=""
AUTO_DETECT=true

# Progress tracking variables
LAST_TOTAL_SEGMENTS=0
LAST_PROCESSED_SEGMENTS=0
START_TIME=0
COMPLETION_TIMES=()

# Function to print usage
usage() {
    cat << EOF
Real-time Progress Monitor for Common Crawl Processing

Usage: $0 [OPTIONS]

Options:
    -i, --interval SECONDS    Monitoring interval in seconds (default: 30)
    -j, --job-id JOB_ID       Specific SLURM job ID to monitor
    -l, --log-file FILE       Log progress to specific file
    -p, --progress-file FILE  Progress state file (default: progress.log)
    -q, --quiet              Quiet mode (no console output)
    -d, --dashboard PORT      Start web dashboard on port (default: 8080)
    -h, --help               Show this help message

Examples:
    $0                                    # Monitor with default settings
    $0 -i 60 -j 12345                    # Monitor job 12345 every 60 seconds
    $0 -d 8080 -l monitor.log            # Start dashboard and log to file
    $0 --quiet --interval 120            # Quiet mode, check every 2 minutes

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
        -l|--log-file)
            LOG_FILE="$2"
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
        -d|--dashboard)
            DASHBOARD_PORT="$2"
            shift 2
            ;;
        -h|--help)
            usage
            exit 0
            ;;
        *)
            echo "Unknown option: $1" >&2
            usage
            exit 1
            ;;
    esac
done

# Load environment variables
source runme.sh 2>/dev/null || true

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

    echo "[$timestamp] $level: $message" >&2

    if [[ -n "$LOG_FILE" ]]; then
        echo "[$timestamp] $level: $message" >> "${WORKING_DIR}/${LOG_FILE}"
    fi
}

# Function to get SLURM job status
get_job_status() {
    if [[ -n "$JOB_ID" ]]; then
        squeue -h -j "$JOB_ID" -o "%T" 2>/dev/null || echo "UNKNOWN"
    else
        # Get the most recent job for this user/account
        squeue -h -A "${SLURM_ACCOUNT:-}" -u "$USER" -o "%i %T" 2>/dev/null | head -1 | awk '{print $2}' || echo "NO_JOBS"
    fi
}

# Function to get active job ID
get_active_job_id() {
    if [[ -n "$JOB_ID" ]]; then
        echo "$JOB_ID"
    else
        # Get the most recent running job
        squeue -h -A "${SLURM_ACCOUNT:-}" -u "$USER" -o "%i" 2>/dev/null | head -1 || echo ""
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

# Function to calculate ETA
calculate_eta() {
    local processed="$1"
    local total="$2"
    local elapsed_time="$3"

    if [[ $processed -eq 0 ]] || [[ $elapsed_time -eq 0 ]]; then
        echo "Unknown"
        return
    fi

    local rate=$((processed / elapsed_time))  # segments per second
    local remaining=$((total - processed))

    if [[ $rate -eq 0 ]]; then
        echo "Unknown"
        return
    fi

    local eta_seconds=$((remaining / rate))

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

# Function to save progress state
save_progress_state() {
    local total="$1"
    local processed="$2"
    local current_date="$3"
    local job_status="$4"
    local active_job_id="$5"

    cat > "$PROGRESS_LOG" << EOF
TIMESTAMP=$(date '+%Y-%m-%d %H:%M:%S')
TOTAL_SEGMENTS=$total
PROCESSED_SEGMENTS=$processed
CURRENT_CRAWL_DATE=$current_date
JOB_STATUS=$job_status
ACTIVE_JOB_ID=$active_job_id
COMPLETION_PERCENTAGE=$([[ $total -gt 0 ]] && echo "$((processed * 100 / total))" || echo "0")
EOF
}

# Function to load progress state
load_progress_state() {
    if [[ -f "$PROGRESS_LOG" ]]; then
        source "$PROGRESS_LOG"
    fi
}

# Function to log progress to history
log_progress_history() {
    local timestamp="$1"
    local processed="$2"
    local total="$3"
    local percentage="$4"
    local current_date="$5"

    echo "[$timestamp] PROCESSED=$processed/$total (${percentage}%) CURRENT_DATE=$current_date" >> "$HISTORY_LOG"
}

# Function to display progress
display_progress() {
    local total="$1"
    local processed="$2"
    local current_date="$3"
    local job_status="$4"
    local active_job_id="$5"
    local elapsed_time="$6"
    local eta="$7"

    if [[ "$QUIET_MODE" == "true" ]]; then
        return
    fi

    # Clear screen and move to top
    echo -e "\033[2J\033[H"

    echo "${BOLD}${BLUE}=== Common Crawl Progress Monitor ===${RESET}"
    echo "Last updated: $(date '+%Y-%m-%d %H:%M:%S')"
    echo ""

    # Overall progress
    if [[ $total -gt 0 ]]; then
        local percentage=$((processed * 100 / total))
        local remaining=$((total - processed))

        echo "${BOLD}Overall Progress:${RESET}"
        echo "  Total segments: $total"
        echo "  Processed: $processed"
        echo "  Remaining: $remaining"
        echo -n "  Progress: ${percentage}% "

        # Progress bar
        local bar_width=50
        local filled=$((percentage * bar_width / 100))
        local empty=$((bar_width - filled))

        printf "["
        printf "%0.s=" $((filled))
        printf "%0.s " $((empty))
        printf "]\n"

        echo "  ETA: $eta"
        echo ""
    fi

    # Current crawl date progress
    if [[ -n "$current_date" ]]; then
        echo "${BOLD}Current Crawl Date: $current_date${RESET}"

        # Calculate progress for current date
        local current_total=0
        local current_processed=0

        if [[ -f "$CRAWL_DATA_FILE" ]]; then
            while read -r date n_files; do
                [[ $date =~ ^#.*$ ]] && continue
                [[ -z "$date" ]] && continue
                if [[ "$date" == "$current_date" && "$n_files" =~ ^[0-9]+$ ]]; then
                    current_total="$n_files"
                    break
                fi
            done < "$CRAWL_DATA_FILE"
        fi

        # Count processed files for current date
        while IFS= read -r -d '' file; do
            [[ -s "$file" ]] && current_processed=$((current_processed + 1))
        done < <(find "$OUTPUT_DIR" -name "crawldata${current_date}*.parquet" -print0 2>/dev/null)

        if [[ $current_total -gt 0 ]]; then
            local current_percentage=$((current_processed * 100 / current_total))
            echo "  Date segments: $current_processed/$current_total (${current_percentage}%)"
        fi
        echo ""
    fi

    # Job status
    echo "${BOLD}Job Status:${RESET}"
    echo "  Active job ID: ${active_job_id:-None}"
    echo "  Status: $job_status"
    echo "  Elapsed time: ${elapsed_time}s"
    echo ""

    # System info
    echo "${BOLD}System Info:${RESET}"
    echo "  Monitoring interval: ${MONITOR_INTERVAL}s"
    echo "  Log file: ${LOG_FILE:-$PROGRESS_LOG}"
    echo ""
}

# Function to start web dashboard
start_dashboard() {
    if [[ "$DASHBOARD_PORT" != "0" ]]; then
        log_message "INFO" "Starting web dashboard on port $DASHBOARD_PORT"
        python3 -c "
import http.server
import socketserver
import json
import os
import threading
import time

class ProgressHandler(http.server.SimpleHTTPRequestHandler):
    def do_GET(self):
        if self.path == '/api/progress':
            self.send_response(200)
            self.send_header('Content-type', 'application/json')
            self.send_header('Access-Control-Allow-Origin', '*')
            self.end_headers()

            try:
                if os.path.exists('$PROGRESS_LOG'):
                    with open('$PROGRESS_LOG', 'r') as f:
                        content = f.read()
                        # Parse the progress file
                        data = {}
                        for line in content.split('\n'):
                            if '=' in line:
                                key, value = line.split('=', 1)
                                data[key] = value
                        self.wfile.write(json.dumps(data).encode())
                    return
            except Exception as e:
                pass

            self.wfile.write(b'{\"error\": \"No progress data available\"}')
        elif self.path == '/':
            self.send_response(200)
            self.send_header('Content-type', 'text/html')
            self.end_headers()
            self.wfile.write(b'''
            <!DOCTYPE html>
            <html>
            <head>
                <title>Common Crawl Progress Monitor</title>
                <meta http-equiv=\"refresh\" content=\"30\">
                <style>
                    body { font-family: Arial, sans-serif; margin: 20px; }
                    .progress-bar { width: 100%; height: 30px; background: #eee; border-radius: 5px; overflow: hidden; }
                    .progress-fill { height: 100%; background: linear-gradient(90deg, #4CAF50, #2196F3); transition: width 0.3s; }
                    .info { background: #f5f5f5; padding: 15px; margin: 10px 0; border-radius: 5px; }
                </style>
            </head>
            <body>
                <h1>Common Crawl Progress Monitor</h1>
                <div id=\"progress\">Loading...</div>
                <script>
                    async function updateProgress() {
                        try {
                            const response = await fetch('/api/progress');
                            const data = await response.json();
                            document.getElementById('progress').innerHTML = createProgressHTML(data);
                        } catch (e) {
                            document.getElementById('progress').innerHTML = 'Error loading progress data';
                        }
                    }

                    function createProgressHTML(data) {
                        let html = '<div class=\"info\">';
                        html += '<h2>Overall Progress</h2>';
                        const total = data.TOTAL_SEGMENTS || 0;
                        const processed = data.PROCESSED_SEGMENTS || 0;
                        const percentage = data.COMPLETION_PERCENTAGE || 0;

                        html += '<div class=\"progress-bar\">';
                        html += '<div class=\"progress-fill\" style=\"width: ' + percentage + '%\"></div>';
                        html += '</div>';
                        html += '<p>' + processed + '/' + total + ' segments (' + percentage + '%)</p>';
                        html += '<p>Current date: ' + (data.CURRENT_CRAWL_DATE || 'None') + '</p>';
                        html += '<p>Job status: ' + (data.JOB_STATUS || 'Unknown') + '</p>';
                        html += '<p>Last updated: ' + (data.TIMESTAMP || 'Never') + '</p>';
                        html += '</div>';
                        return html;
                    }

                    updateProgress();
                    setInterval(updateProgress, 30000);
                </script>
            </body>
            </html>
            ''')
        else:
            super().do_GET()

# Start server in background
server = socketserver.TCPServer(('', $DASHBOARD_PORT), ProgressHandler)
print('Dashboard started at http://localhost:$DASHBOARD_PORT')
server.serve_forever()
" > /dev/null 2>&1 &
        DASHBOARD_PID=$!
        log_message "INFO" "Dashboard started with PID $DASHBOARD_PID"
    fi
}

# Function to cleanup
cleanup() {
    if [[ -n "${DASHBOARD_PID:-}" ]]; then
        kill "$DASHBOARD_PID" 2>/dev/null || true
        log_message "INFO" "Dashboard stopped"
    fi
    exit 0
}

# Trap to cleanup on exit
trap cleanup EXIT INT TERM

# Main monitoring loop
main() {
    log_message "INFO" "Starting progress monitor (interval: ${MONITOR_INTERVAL}s)"

    # Start web dashboard if requested
    if [[ "$DASHBOARD_PORT" != "0" ]]; then
        start_dashboard
    fi

    # Initialize start time
    START_TIME=$(date +%s)

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
        save_progress_state "$total_segments" "$processed_segments" "$current_date" "$job_status" "$active_job_id"

        # Log to history
        log_progress_history "$timestamp" "$processed_segments" "$total_segments" "$percentage" "$current_date"

        # Display progress
        display_progress "$total_segments" "$processed_segments" "$current_date" "$job_status" "$active_job_id" "$elapsed_time" "$eta"

        # Check if job is complete
        if [[ "$job_status" != "RUNNING" ]] && [[ "$job_status" != "PENDING" ]] && [[ $processed_segments -gt 0 ]]; then
            log_message "INFO" "Job completed or not running. Processed: $processed_segments/$total_segments (${percentage}%)"
            break
        fi

        # Wait for next update
        sleep "$MONITOR_INTERVAL"
    done

    log_message "INFO" "Progress monitoring completed"
}

# Run main function
main "$@"
