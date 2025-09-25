#!/bin/bash

# Unified Job Analysis Script for Common Crawl Outputs
# Usage: ./job-analyzer.sh [JOB_PREFIX] [--dry-run] [--output-dir=DIR]
# Example: ./job-analyzer.sh crawl_job_long --output-dir=output

shopt -s nullglob
set -euo pipefail

# Source runme.sh only if present (avoid silent exit when file missing)
if [[ -f ./runme.sh ]]; then
    # shellcheck disable=SC1091
    source ./runme.sh
else
    echo "Note: runme.sh not found; continuing with defaults" >&2
fi

# Defaults
DRY_RUN="false"
REPORT_FILE="analysis_report.txt"
ERROR_THRESHOLD=5
CONDA_ENV="./.conda_env"
VERBOSE="false"
DEBUG="false"
# Use WORKING_DIR from environment for analysis
JOB_WORKDIR="${WORKING_DIR:-.}"
ANALYSIS_DIR="${JOB_WORKDIR}/output"
# Parse arguments (handle --dry-run and --output-dir=)
for arg in "$@"; do
    case "$arg" in
        --dry-run) DRY_RUN="true" ;;
        --verbose|-v) VERBOSE="true" ;;
        --debug) DEBUG="true" ;;
        --output-dir=*) ANALYSIS_DIR="${arg#*=}" ;;
        *) JOB_PREFIX="${arg}" ;; # optional positional argument kept for compatibility
    esac
done

# Colors for output (with reliable fallback)
if tput colors >/dev/null 2>&1; then
    RED=$(tput setaf 1)
    GREEN=$(tput setaf 2)
    YELLOW=$(tput setaf 3)
    BLUE=$(tput setaf 4)
    RESET=$(tput sgr0)
else
    RED=""
    GREEN=""
    YELLOW=""
    BLUE=""
    RESET=""
fi

# Logging helpers (verbose/debug)
log_info() {
    if [[ "$VERBOSE" == "true" || "$DEBUG" == "true" ]]; then
        echo -e "$@"
    fi
}
log_debug() {
    if [[ "$DEBUG" == "true" ]]; then
        echo -e "$@"
    fi
}

# If debug requested, enable shell tracing with helpful PS4
if [[ "$DEBUG" == "true" ]]; then
    export PS4='+ ${BASH_SOURCE}:${LINENO}:${FUNCNAME[0]}: '
    set -x
fi

if [[ "$DRY_RUN" == "true" ]]; then
    echo "${YELLOW}Dry-run mode: Simulating analysis without real checks.${RESET}"
    exit 0
fi

# Build file lists after args / env are resolved
ERR_FILES=("$JOB_WORKDIR"/*.err)
LOG_FILES=("$JOB_WORKDIR"/*.log)
TEMP_FILES=("$JOB_WORKDIR"/*.out "${LOG_FILES[@]}")
PARQUET_FILES=("$ANALYSIS_DIR"/*.parquet)

# Debug / verbose dump of resolved variables and globs
log_info "${BLUE}--- Debug: resolved environment & globs ---${RESET}"
log_info "JOB_WORKDIR=$JOB_WORKDIR"
log_info "ANALYSIS_DIR=$ANALYSIS_DIR"
log_info "REPORT_FILE=$REPORT_FILE"
log_info "CONDA_ENV=$CONDA_ENV"
log_info "DRY_RUN=$DRY_RUN VERBOSE=$VERBOSE DEBUG=$DEBUG"
log_info "ERR_FILES (count=${#ERR_FILES[@]}):"
log_debug "  ${ERR_FILES[*]:-<none>}"
log_info "LOG_FILES (count=${#LOG_FILES[@]}):"
log_debug "  ${LOG_FILES[*]:-<none>}"
log_info "TEMP_FILES (count=${#TEMP_FILES[@]}):"
log_debug "  ${TEMP_FILES[*]:-<none>}"
log_info "PARQUET_FILES (count=${#PARQUET_FILES[@]}):"
log_debug "  ${PARQUET_FILES[*]:-<none>}"
log_info "${BLUE}--- End debug dump ---${RESET}"

echo "${BLUE}=== Job Analysis Report ===${RESET}"
echo "Analyzing in: $ANALYSIS_DIR"
echo "Report generated: $(date)"
echo "Saving detailed report to: $REPORT_FILE"
echo ""

{
    echo "Job Analysis Report"
    echo "Generated: $(date)"
    echo ""
} > "$REPORT_FILE"

# 1. ERROR AND FAILURE ANALYSIS (*.err and *.log)
echo "${GREEN}1. Error and Failure Summary${RESET}"
echo "-----------------------------"
ERROR_COUNT=0
WARNING_COUNT=0
FAILURE_DETAILS=""

for file in "${ERR_FILES[@]}"; do
    if [[ -f "$file" ]]; then
        ERRORS=$( (grep -i -E "error|failed|exception|timeout" "$file" || true) | wc -l )
        WARNINGS=$( (grep -i -E "warning|low|insufficient" "$file" || true) | wc -l )
        ERROR_COUNT=$((ERROR_COUNT + ERRORS))
        WARNING_COUNT=$((WARNING_COUNT + WARNINGS))
        tmp_fail=$((grep -i -E "error|failed" "$file" || true) | head -3)
        if [[ -n "$tmp_fail" ]]; then
            FAILURE_DETAILS+="$tmp_fail\nFrom file: $file\n\n"
        fi
    fi
done

for file in "${LOG_FILES[@]}"; do
    if [[ -f "$file" ]]; then
        ERRORS=$( (grep -i -E "error|failed to download|error processing|logging.error" "$file" || true) | wc -l )
        WARNINGS=$( (grep -i -E "warning|no records|low disk" "$file" || true) | wc -l )
        ERROR_COUNT=$((ERROR_COUNT + ERRORS))
        WARNING_COUNT=$((WARNING_COUNT + WARNINGS))
        tmp_fail=$((grep -i -E "error|failed" "$file" || true) | head -3)
        if [[ -n "$tmp_fail" ]]; then
            FAILURE_DETAILS+="$tmp_fail\nFrom file: $file\n\n"
        fi
    fi
done

echo "Total errors found: $ERROR_COUNT (across ${#ERR_FILES[@]} + ${#LOG_FILES[@]} files)"
echo "Total warnings: $WARNING_COUNT"
if [[ $ERROR_COUNT -gt $ERROR_THRESHOLD ]]; then
    echo "${RED}WARNING: High error count! Review failures below.${RESET}"
    echo "Top failure details:"
    echo "$FAILURE_DETAILS"
else
    echo "${GREEN}Error count acceptable.${RESET}"
fi

{
    echo "1. Error and Failure Summary"
    echo "Total errors: $ERROR_COUNT"
    echo "Total warnings: $WARNING_COUNT"
    echo "Details:"
    echo "$FAILURE_DETAILS"
    echo ""
} >> "$REPORT_FILE"
echo ""

# 2. LOG SUMMARY (*.log and *.out)
echo "${GREEN}2. Log Summary (Successes and Metrics)${RESET}"
echo "-----------------------------------"
SUCCESSFUL_SEGMENTS=0
TOTAL_RECORDS=0
AVG_TIME_PER_SEG=0
LOG_DETAILS=""

for file in "${TEMP_FILES[@]}"; do
    if [[ -f "$file" ]]; then
        SUCC=$( (grep -o "Completed processing segment" "$file" || true) | wc -l )
        SUCCESSFUL_SEGMENTS=$((SUCCESSFUL_SEGMENTS + SUCC))
        RECS=$( (grep "Completed processing segment" "$file" || true) | awk '{sum += $NF} END {print sum + 0}' )
        TOTAL_RECORDS=$((TOTAL_RECORDS + RECS))
        TIMES=$( (grep -i -E "processing|heartbeat" "$file" || true) | awk 'BEGIN{cnt=0;sum=0} { if ($0 ~ /sec/) { sum += $NF; cnt++ } } END { print (cnt>0 ? int(sum/cnt) : 0) }' )
        if [[ $AVG_TIME_PER_SEG -eq 0 ]]; then
            AVG_TIME_PER_SEG=$TIMES
        else
            AVG_TIME_PER_SEG=$(( (AVG_TIME_PER_SEG + TIMES) / 2 ))
        fi
        LOG_DETAILS+="Successes: $SUCC segments, ~${RECS} records from $file\n"
    fi
done

if [[ $((SUCCESSFUL_SEGMENTS + ERROR_COUNT)) -gt 0 ]]; then
    SUCCESS_RATE=$(( (SUCCESSFUL_SEGMENTS * 100) / (SUCCESSFUL_SEGMENTS + ERROR_COUNT) ))
else
    SUCCESS_RATE=0
fi

echo "Successful segments processed: $SUCCESSFUL_SEGMENTS"
echo "Total records extracted: $TOTAL_RECORDS"
echo "Average time per segment: ~${AVG_TIME_PER_SEG} seconds"
echo "Overall success rate: ${SUCCESS_RATE}%"

if [[ $SUCCESS_RATE -lt 90 ]]; then
    echo "${YELLOW}Note: Success rate below 90%—consider re-running failed segments.${RESET}"
fi

{
    echo "2. Log Summary"
    echo "Successful segments: $SUCCESSFUL_SEGMENTS"
    echo "Total records: $TOTAL_RECORDS"
    echo "Avg time per segment: ${AVG_TIME_PER_SEG}s"
    echo "Success rate: ${SUCCESS_RATE}%"
    echo "Details:"
    echo "$LOG_DETAILS"
    echo ""
} >> "$REPORT_FILE"
echo ""

# 3. OUTPUT FILE ANALYSIS (*.parquet)
echo "${GREEN}3. Output File Analysis (*.parquet)${RESET}"
echo "-----------------------------------"
PARQUET_COUNT=0
TOTAL_PARQUET_SIZE=0
TOTAL_ROWS=0
UNIQUE_POSTCODES=0
QUALITY_NOTES=""
PCT_WITH_PC="0"

if [[ ${#PARQUET_FILES[@]} -eq 0 ]]; then
    echo "${YELLOW}No Parquet files found. Skipping analysis.${RESET}"
else
    echo "Found ${#PARQUET_FILES[@]} Parquet files. Analyzing..."
    for file in "${PARQUET_FILES[@]}"; do
        if [[ -f "$file" ]]; then
            SIZE_MB=$(du -m "$file" | cut -f1)
            TOTAL_PARQUET_SIZE=$((TOTAL_PARQUET_SIZE + SIZE_MB))
            PARQUET_COUNT=$((PARQUET_COUNT + 1))
        fi
    done
    echo "Total Parquet files: $PARQUET_COUNT"
    echo "Total output size: ${TOTAL_PARQUET_SIZE}MB"
    if [[ -d "$CONDA_ENV" ]]; then
        PYTHON_SCRIPT=$(cat << 'EOF'
import os
import sys
import pandas as pd
from glob import glob

analysis_dir = sys.argv[1] if len(sys.argv) > 1 else '.'
parquet_files = glob(os.path.join(analysis_dir, 'crawldata*.parquet'))

total_rows = 0
unique_postcodes = set()
with_postcodes_pct = 0.0
file_count = 0

for file in parquet_files:
    if os.path.exists(file) and os.path.getsize(file) > 0:
        try:
            df = pd.read_parquet(file)
            rows = len(df)
            total_rows += rows
            file_count += 1
            if 'postcodes' in df.columns:
                all_postcodes = [pc for sublist in df['postcodes'].dropna() for pc in sublist]
                unique_postcodes.update(all_postcodes)
            pct_with_pc = (df['postcodes'].notna().sum() / rows * 100) if rows > 0 else 0
            with_postcodes_pct += pct_with_pc
        except Exception as e:
            print(f"Error reading {file}: {e}", file=sys.stderr)

if file_count > 0:
    with_postcodes_pct /= file_count
    print(f"Total rows across Parquets: {total_rows}")
    print(f"Unique postcodes: {len(unique_postcodes)}")
    print(f"Avg % records with postcodes: {with_postcodes_pct:.1f}%")
else:
    print("No valid Parquet files to analyze.")
EOF
        )
        TEMP_PY=$(mktemp)
        echo "$PYTHON_SCRIPT" > "$TEMP_PY"
        if [[ -x "$CONDA_ENV/bin/python" ]]; then
            STATS=$("$CONDA_ENV/bin/python" "$TEMP_PY" "$ANALYSIS_DIR" 2>&1)
            rm "$TEMP_PY"
            echo "$STATS"
            TOTAL_ROWS=$(echo "$STATS" | grep "Total rows" | awk '{print $5}' || echo "0")
            UNIQUE_POSTCODES=$(echo "$STATS" | grep "Unique postcodes" | awk '{print $3}' || echo "0")
            PCT_WITH_PC=$(echo "$STATS" | grep "Avg % records" | sed 's/.*: \([0-9.]*\)%/\1/' || echo "0")
            QUALITY_NOTES="Avg % records with postcodes: ${PCT_WITH_PC}%"
            if [[ $(echo "$PCT_WITH_PC < 10" | awk '{print ($1 < 10) ? 1 : 0}') -eq 1 ]]; then
                echo "${YELLOW}Note: Low postcode coverage—check regex in common-crawl-processor.py.${RESET}"
            fi
        else
            echo "${RED}Conda env not found or unusable. Skipping detailed Parquet stats. Install pandas/pyarrow and retry.${RESET}"
            echo "Basic stats only: $PARQUET_COUNT files, ${TOTAL_PARQUET_SIZE}MB total."
        fi
    else
        echo "${RED}No Conda env at $CONDA_ENV. Skipping Python-based Parquet analysis.${RESET}"
        echo "Basic stats only: $PARQUET_COUNT files, ${TOTAL_PARQUET_SIZE}MB total."
    fi
fi

{
    echo "3. Output File Analysis"
    echo "Parquet files: $PARQUET_COUNT"
    echo "Total size: ${TOTAL_PARQUET_SIZE}MB"
    echo "Total rows: $TOTAL_ROWS"
    echo "Unique postcodes: $UNIQUE_POSTCODES"
    echo "Quality notes: $QUALITY_NOTES"
    echo ""
} >> "$REPORT_FILE"
echo ""

# 4. OVERALL REVIEW AND RECOMMENDATIONS
echo "${GREEN}4. Overall Review and Recommendations${RESET}"
echo "------------------------------------"
if [[ $ERROR_COUNT -eq 0 && $SUCCESS_RATE -gt 95 ]]; then
    echo "${GREEN}SUCCESS: Job looks healthy! High success rate, low errors.${RESET}"
elif [[ $ERROR_COUNT -gt 0 ]]; then
    echo "${YELLOW}RECOMMENDATION: Re-run failed segments (grep logs for 'segment X failed').${RESET}"
fi
if [[ $PARQUET_COUNT -gt 0 && $TOTAL_ROWS -gt 0 ]]; then
    echo "${GREEN}Data quality good: ${TOTAL_ROWS} records with ${UNIQUE_POSTCODES} unique postcodes.${RESET}"
else
    echo "${YELLOW}RECOMMENDATION: No outputs? Check job submission and wet.paths generation.${RESET}"
fi
echo "Full report saved to $REPORT_FILE. For CSV export, add a Python step if needed."

{
    echo "4. Overall Review"
    echo "Success rate: ${SUCCESS_RATE}%"
    echo "Recommendations: See console or review high-error files."
    echo ""
    echo "End of Report"
} >> "$REPORT_FILE"

echo "${BLUE}Analysis complete. Review $REPORT_FILE for details.${RESET}"