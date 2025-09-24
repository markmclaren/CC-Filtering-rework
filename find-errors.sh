#!/bin/bash

# =============================================================================
# Error Detection and Analysis Script
# =============================================================================
# Comprehensive error analysis tool that scans SLURM error logs for issues,
# categorizes them, and provides actionable insights.
#
# Usage: ./find-errors.sh [options]
# Options:
#   -h, --help          Show this help message
#   -v, --verbose       Enable verbose output with detailed error listings
#   -s, --summary       Show only summary statistics (default)
#   -c, --category CAT  Show only errors from specific category
#   -n, --count N       Limit output to top N most frequent errors
#   -f, --files         Show which files contain each error type
#   -r, --recent HOURS  Only analyze errors from last N hours

set -euo pipefail

# Source the centralized configuration
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/config.sh"

# Script-specific variables
VERBOSE=false
SUMMARY_ONLY=true
SPECIFIC_CATEGORY=""
MAX_COUNT=10
SHOW_FILES=false
RECENT_HOURS=""
ERROR_PATTERNS_FILE="${SCRIPT_DIR}/error_patterns.txt"

# Associative arrays for error tracking
declare -A error_counts
declare -A error_descriptions
declare -A error_files
declare -A category_counts

# Function to display help
show_help() {
    cat << EOF
Error Detection and Analysis Script

USAGE:
    $0 [OPTIONS]

OPTIONS:
    -h, --help              Show this help message and exit
    -v, --verbose           Enable verbose output with detailed error listings
    -s, --summary           Show only summary statistics (default)
    -c, --category CATEGORY Show only errors from specific category
    -n, --count NUMBER      Limit output to top N most frequent errors (default: 10)
    -f, --files             Show which files contain each error type
    -r, --recent HOURS      Only analyze errors from last N hours

CATEGORIES:
    PYTHON_ERROR    Python exceptions and tracebacks
    MEMORY_ERROR    Memory allocation and out-of-memory errors
    FILE_ERROR      File system and I/O errors
    NETWORK_ERROR   Network connectivity and protocol errors
    SLURM_ERROR     SLURM scheduler and job management errors
    PROCESS_ERROR   Process crashes and system signals
    DATA_ERROR      Data parsing and format errors
    TIMEOUT_ERROR   Operation timeouts and deadlines
    CONFIG_ERROR    Configuration and parameter errors
    DATABASE_ERROR  Database connectivity and SQL errors
    GENERIC_ERROR   Generic errors and exceptions

DESCRIPTION:
    This script analyzes SLURM error log files to identify and categorize
    common failure patterns. It provides:
    - Error frequency analysis and ranking
    - Categorization of error types
    - File-specific error reporting
    - Time-based filtering for recent errors
    - Actionable insights for troubleshooting

EXAMPLES:
    $0                          # Show error summary
    $0 -v                       # Show detailed error analysis
    $0 -c PYTHON_ERROR          # Show only Python errors
    $0 -n 5 -f                  # Show top 5 errors with file locations
    $0 -r 24                    # Show errors from last 24 hours
    $0 -c MEMORY_ERROR -v -f    # Detailed memory error analysis with files

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
                SUMMARY_ONLY=false
                shift
                ;;
            -s|--summary)
                SUMMARY_ONLY=true
                VERBOSE=false
                shift
                ;;
            -c|--category)
                if [[ -n "${2:-}" ]]; then
                    SPECIFIC_CATEGORY="$2"
                    shift 2
                else
                    print_colored "$COLOR_RED" "Error: --category requires a category name"
                    exit 1
                fi
                ;;
            -n|--count)
                if [[ -n "${2:-}" && "$2" =~ ^[0-9]+$ ]]; then
                    MAX_COUNT="$2"
                    shift 2
                else
                    print_colored "$COLOR_RED" "Error: --count requires a positive number"
                    exit 1
                fi
                ;;
            -f|--files)
                SHOW_FILES=true
                shift
                ;;
            -r|--recent)
                if [[ -n "${2:-}" && "$2" =~ ^[0-9]+$ ]]; then
                    RECENT_HOURS="$2"
                    shift 2
                else
                    print_colored "$COLOR_RED" "Error: --recent requires a number of hours"
                    exit 1
                fi
                ;;
            *)
                print_colored "$COLOR_RED" "Unknown option: $1"
                echo "Use -h or --help for usage information."
                exit 1
                ;;
        esac
    done
}

# Function to load error patterns from file
load_error_patterns() {
    local patterns_file="$1"
    local -A patterns
    
    if [[ ! -f "$patterns_file" ]]; then
        print_colored "$COLOR_YELLOW" "Warning: Error patterns file not found: $patterns_file"
        print_colored "$COLOR_YELLOW" "Using basic built-in patterns only."
        return 1
    fi
    
    while IFS='|' read -r category pattern description; do
        # Skip comments and empty lines
        if [[ "$category" =~ ^#.*$ || -z "$category" ]]; then
            continue
        fi
        
        patterns["$pattern"]="$category|$description"
    done < "$patterns_file"
    
    # Export patterns for use in other functions
    for pattern in "${!patterns[@]}"; do
        echo "$pattern|${patterns[$pattern]}"
    done
}

# Function to check if file is recent enough
is_file_recent() {
    local file="$1"
    local hours="$2"
    
    if [[ -z "$hours" ]]; then
        return 0  # No time filter
    fi
    
    local file_time
    file_time=$(stat -c %Y "$file" 2>/dev/null || echo "0")
    local current_time
    current_time=$(date +%s)
    local cutoff_time=$((current_time - hours * 3600))
    
    [[ $file_time -gt $cutoff_time ]]
}

# Function to scan a single log file for errors
scan_log_file() {
    local log_file="$1"
    local patterns="$2"
    
    if [[ ! -f "$log_file" || ! -r "$log_file" ]]; then
        return
    fi
    
    # Check if file is recent enough
    if ! is_file_recent "$log_file" "$RECENT_HOURS"; then
        return
    fi
    
    local file_basename
    file_basename=$(basename "$log_file")
    
    # Process each error pattern
    while IFS='|' read -r pattern category description; do
        if [[ -z "$pattern" ]]; then
            continue
        fi
        
        # Skip if filtering by specific category
        if [[ -n "$SPECIFIC_CATEGORY" && "$category" != "$SPECIFIC_CATEGORY" ]]; then
            continue
        fi
        
        # Search for pattern in log file
        local matches
        matches=$(grep -c "$pattern" "$log_file" 2>/dev/null || echo "0")
        
        if [[ $matches -gt 0 ]]; then
            local key="${category}:${pattern}"
            error_counts["$key"]=$((${error_counts["$key"]:-0} + matches))
            error_descriptions["$key"]="$description"
            category_counts["$category"]=$((${category_counts["$category"]:-0} + matches))
            
            if [[ "$SHOW_FILES" == true ]]; then
                if [[ -n "${error_files["$key"]:-}" ]]; then
                    error_files["$key"]="${error_files["$key"]}, $file_basename"
                else
                    error_files["$key"]="$file_basename"
                fi
            fi
        fi
    done <<< "$patterns"
}

# Function to find and scan all error log files
scan_all_logs() {
    local patterns="$1"
    local log_files
    local total_files=0
    local processed_files=0
    
    print_colored "$COLOR_CYAN" "Scanning error logs in: $WORK_DIR"
    
    # Find all error log files
    readarray -t log_files < <(find "$WORK_DIR" -name "$ERROR_LOG_PATTERN" -type f 2>/dev/null | head -n "$MAX_LOG_FILES")
    
    total_files=${#log_files[@]}
    
    if [[ $total_files -eq 0 ]]; then
        print_colored "$COLOR_YELLOW" "No error log files found matching pattern: $ERROR_LOG_PATTERN"
        return 1
    fi
    
    print_colored "$COLOR_CYAN" "Found $total_files error log files"
    
    if [[ -n "$RECENT_HOURS" ]]; then
        print_colored "$COLOR_CYAN" "Filtering for files modified in last $RECENT_HOURS hours"
    fi
    
    # Process each log file
    for log_file in "${log_files[@]}"; do
        if [[ -n "$log_file" ]]; then
            scan_log_file "$log_file" "$patterns"
            processed_files=$((processed_files + 1))
            
            # Show progress for large numbers of files
            if [[ $total_files -gt 50 && $((processed_files % 10)) -eq 0 ]]; then
                echo -n "."
            fi
        fi
    done
    
    if [[ $total_files -gt 50 ]]; then
        echo  # New line after progress dots
    fi
    
    print_colored "$COLOR_GREEN" "Processed $processed_files log files"
    return 0
}

# Function to display category summary
display_category_summary() {
    if [[ ${#category_counts[@]} -eq 0 ]]; then
        print_colored "$COLOR_YELLOW" "No errors found in the analyzed log files."
        return
    fi
    
    print_colored "$COLOR_BLUE" "=== Error Category Summary ==="
    echo
    
    # Sort categories by count (descending)
    local sorted_categories
    sorted_categories=$(for category in "${!category_counts[@]}"; do
        echo "${category_counts[$category]} $category"
    done | sort -rn)
    
    local total_errors=0
    while read -r count category; do
        if [[ -n "$count" && -n "$category" ]]; then
            total_errors=$((total_errors + count))
            printf "%-20s %s\n" "$category:" "$(print_colored "$COLOR_RED" "$count errors")"
        fi
    done <<< "$sorted_categories"
    
    echo
    print_colored "$COLOR_CYAN" "Total Errors Found: $total_errors"
    echo
}

# Function to display detailed error analysis
display_detailed_errors() {
    if [[ ${#error_counts[@]} -eq 0 ]]; then
        return
    fi
    
    print_colored "$COLOR_BLUE" "=== Detailed Error Analysis ==="
    echo
    
    # Sort errors by count (descending)
    local sorted_errors
    sorted_errors=$(for key in "${!error_counts[@]}"; do
        echo "${error_counts[$key]} $key"
    done | sort -rn | head -n "$MAX_COUNT")
    
    local rank=1
    while read -r count key; do
        if [[ -n "$count" && -n "$key" ]]; then
            local category="${key%%:*}"
            local pattern="${key#*:}"
            local description="${error_descriptions[$key]:-Unknown error}"
            
            print_colored "$COLOR_PURPLE" "[$rank] $category"
            print_colored "$COLOR_RED" "    Count: $count occurrences"
            print_colored "$COLOR_YELLOW" "    Description: $description"
            
            if [[ "$VERBOSE" == true ]]; then
                print_colored "$COLOR_CYAN" "    Pattern: $pattern"
            fi
            
            if [[ "$SHOW_FILES" == true && -n "${error_files[$key]:-}" ]]; then
                print_colored "$COLOR_GREEN" "    Files: ${error_files[$key]}"
            fi
            
            echo
            rank=$((rank + 1))
        fi
    done <<< "$sorted_errors"
}

# Function to display recommendations
display_recommendations() {
    if [[ ${#category_counts[@]} -eq 0 ]]; then
        return
    fi
    
    print_colored "$COLOR_BLUE" "=== Troubleshooting Recommendations ==="
    echo
    
    # Provide category-specific recommendations
    for category in "${!category_counts[@]}"; do
        local count="${category_counts[$category]}"
        
        case "$category" in
            "PYTHON_ERROR")
                print_colored "$COLOR_YELLOW" "Python Errors ($count found):"
                echo "  • Check Python environment and dependencies"
                echo "  • Verify input data format and content"
                echo "  • Review Python script logic and error handling"
                ;;
            "MEMORY_ERROR")
                print_colored "$COLOR_YELLOW" "Memory Errors ($count found):"
                echo "  • Increase SLURM memory allocation (--mem parameter)"
                echo "  • Optimize data processing to use less memory"
                echo "  • Consider processing data in smaller chunks"
                ;;
            "FILE_ERROR")
                print_colored "$COLOR_YELLOW" "File System Errors ($count found):"
                echo "  • Check file paths and permissions"
                echo "  • Verify available disk space"
                echo "  • Ensure input files exist and are readable"
                ;;
            "SLURM_ERROR")
                print_colored "$COLOR_YELLOW" "SLURM Errors ($count found):"
                echo "  • Check SLURM job parameters (time, memory, CPUs)"
                echo "  • Verify cluster resource availability"
                echo "  • Review job submission script configuration"
                ;;
            "NETWORK_ERROR")
                print_colored "$COLOR_YELLOW" "Network Errors ($count found):"
                echo "  • Check network connectivity and firewall settings"
                echo "  • Verify external service availability"
                echo "  • Consider implementing retry logic for network operations"
                ;;
        esac
        echo
    done
}

# Main analysis function
analyze_errors() {
    local patterns
    
    print_colored "$COLOR_BLUE" "=== Error Detection and Analysis ==="
    print_colored "$COLOR_CYAN" "Timestamp: $(get_timestamp)"
    print_colored "$COLOR_CYAN" "Working Directory: $WORK_DIR"
    
    if [[ -n "$SPECIFIC_CATEGORY" ]]; then
        print_colored "$COLOR_CYAN" "Category Filter: $SPECIFIC_CATEGORY"
    fi
    
    if [[ -n "$RECENT_HOURS" ]]; then
        print_colored "$COLOR_CYAN" "Time Filter: Last $RECENT_HOURS hours"
    fi
    
    echo
    
    # Load error patterns
    patterns=$(load_error_patterns "$ERROR_PATTERNS_FILE")
    
    if [[ -z "$patterns" ]]; then
        print_colored "$COLOR_RED" "Error: No error patterns loaded. Cannot proceed with analysis."
        exit 1
    fi
    
    # Scan all log files
    if ! scan_all_logs "$patterns"; then
        exit 1
    fi
    
    echo
    
    # Display results
    display_category_summary
    
    if [[ "$SUMMARY_ONLY" == false ]]; then
        display_detailed_errors
        display_recommendations
    fi
}

# Main execution
main() {
    parse_args "$@"
    analyze_errors
}

# Execute main function with all arguments
main "$@"
