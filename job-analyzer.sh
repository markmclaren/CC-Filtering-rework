#!/bin/bash

# Unified Job Analysis Script for Common Crawl Outputs
# Usage: ./job-analyzer.sh [JOB_PREFIX] [--dry-run] [--output-dir=DIR]
# Example: ./job-analyzer.sh crawl_job_long --output-dir=output
# Analyzes *.out, *.err, *.log, and *.parquet files for errors, summaries, and data quality.

# Safe globs: Expand to empty if no matches (replaces 2>/dev/null for silence)
shopt -s nullglob

source ./runme.sh  # Load environment variables

# Source config for dual-dir setup (JOB_WORKDIR, SLURM_ACCOUNT, etc.)
if [[ -f "slurm-config.sh" ]]; then
    source slurm-config.sh 2>/dev/null || echo "Warning: Sourcing slurm-config.sh had unexpected output."
else
    echo "Warning: slurm-config.sh not found. Set JOB_WORKDIR manually if needed."
fi

# Ensure JOB_WORKDIR is set (fallback to current dir)
if [[ -z "${JOB_WORKDIR:-}" ]]; then
    echo "Note: JOB_WORKDIR not set. Using current dir for analysis."
    JOB_WORKDIR="."
fi

set -euo pipefail  # Exit on error, undefined vars, pipe failures

# Colors for output (with fallback if tput not available)
RED=$(tput setaf 1 2>/dev/null || true; echo -n ${RED:-})
GREEN=$(tput setaf 2 2>/dev/null || true; echo -n ${GREEN:-})
YELLOW=$(tput setaf 3 2>/dev/null || true; echo -n ${YELLOW:-})
BLUE=$(tput setaf 4 2>/dev/null || true; echo -n ${BLUE:-})
RESET=$(tput sgr0 2>/dev/null || true; echo -n ${RESET:-})

# Defaults
JOB_PREFIX="${1:-crawl_job}"
DRY_RUN="${2:-}"
ANALYSIS_DIR="${JOB_WORKDIR}/output"  # Default to WORKING_DIR/output for dual-dir
REPORT_FILE="${JOB_WORKDIR}/analysis_report_${JOB_PREFIX}.txt"  # Save report in WORKING_DIR
ERROR_THRESHOLD=5  # Warn if errors > this
CONDA_ENV="./.conda_env"  # Path to env for Parquet analysis

if [[ "$DRY_RUN" == "--dry-run" ]]; then
    echo "${YELLOW}Dry-run mode: Simulating analysis without real checks.${RESET}"
    exit 0
fi

# Override ANALYSIS_DIR if --output-dir provided (simple arg parsing)
for arg in "$@"; do
    if [[ $arg == --output-dir=* ]]; then
        ANALYSIS_DIR="${arg#*=}"
    fi
done

echo "${BLUE}=== Job Analysis Report for Prefix: $JOB_PREFIX ===${RESET}"
echo "Analyzing in: $ANALYSIS_DIR"
echo "Report generated: $(date)"
echo "Saving detailed report to: $REPORT_FILE"
echo ""

# Create report file header
{
    echo "Job Analysis Report for $JOB_PREFIX"
    echo "Generated: $(date)"
    echo ""
} > "$REPORT_FILE"

# 1. ERROR AND FAILURE ANALYSIS (*.err and *.log)
echo "${GREEN}1. Error and Failure Summary${RESET}"
echo "-----------------------------"
ERROR_COUNT=0
WARNING_COUNT=0
FAILURE_DETAILS=""

# Scan *.err files (nullglob makes empty array if no files)
ERR_FILES=("$ANALYSIS_DIR"/*_${JOB_PREFIX}*.err "$ANALYSIS_DIR"/*.err)
for file in "${ERR_FILES[@]}"; do
    if [[ -f "$file" ]]; then
        ERRORS=$(grep -i "error\|failed\|exception\|timeout" "$file" | wc -l)
        WARNINGS=$(grep -i "warning\|low\|insufficient" "$file" | wc -l)
        ERROR_COUNT=$((ERROR_COUNT + ERRORS))
        WARNING_COUNT=$((WARNING_COUNT + WARNINGS))
        FAILURE_DETAILS+=$(grep -i "error\|failed" "$file" | head -3)  # Top 3 details
        FAILURE_DETAILS+="\nFrom file: $file\n\n"
    fi
done

# Scan *.log files for failures
LOG_FILES=("$ANALYSIS_DIR"/*_${JOB_PREFIX}*.log "$ANALYSIS_DIR"/*.log)
for file in "${LOG_FILES[@]}"; do
    if [[ -f "$file" ]]; then
        ERRORS=$(grep -i "error\|failed to download\|error processing|logging.error" "$file" | wc -l)
        WARNINGS=$(grep -i "warning\|no records|low disk" "$file" | wc -l)
        ERROR_COUNT=$((ERROR_COUNT + ERRORS))
        WARNING_COUNT=$((WARNING_COUNT + WARNINGS))
        FAILURE_DETAILS+=$(grep -i "error\|failed" "$file" | head -3)
        FAILURE_DETAILS+="\nFrom file: $file\n\n"
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

# Append to report
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

# Capture globs in temp array for safe iteration
TEMP_FILES=("${LOG_FILES[@]}" "$ANALYSIS_DIR"/*_${JOB_PREFIX}*.out "$ANALYSIS_DIR"/*.out)
for file in "${TEMP_FILES[@]}"; do
    if [[ -f "$file" ]]; then
        # Count successful segments (e.g., "Completed processing segment X: Y records")
        SUCC=$(grep -o "Completed processing segment" "$file" | wc -l)
        SUCCESSFUL_SEGMENTS=$((SUCCESSFUL_SEGMENTS + SUCC))
        
        # Sum records (extract Y from "Y records")
        RECS=$(grep "Completed processing segment" "$file" | awk '{sum += $NF} END {print sum + 0}' || echo "0")
        TOTAL_RECORDS=$((TOTAL_RECORDS + RECS))
        
        # Average time: Assume logs have "Processing time: X sec" or estimate from heartbeats (heuristic)
        TIMES=$(grep -i "processing\|heartbeat" "$file" | awk '{if ($0 ~ /sec/) sum += $NF} END {print (sum > 0 ? sum / NR : 0)}' || echo "0")
        if [[ $AVG_TIME_PER_SEG -eq 0 ]]; then
            AVG_TIME_PER_SEG=$TIMES
        else
            AVG_TIME_PER_SEG=$(( (AVG_TIME_PER_SEG + TIMES) / 2 ))
        fi
        
        LOG_DETAILS+="Successes: $SUCC segments, ~${RECS} records from $file\n"
    fi
done

# Avoid div-by-zero
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

# Append to report
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

PARQUET_FILES=("$ANALYSIS_DIR"/crawldata*.parquet)

if [[ ${#PARQUET_FILES[@]} -eq 0 ]]; then
    echo "${YELLOW}No Parquet files found. Skipping analysis.${RESET}"
else
    echo "Found ${#PARQUET_FILES[@]} Parquet files. Analyzing..."
    
    # Simple size check (shell)
    for file in "${PARQUET_FILES[@]}"; do
        if [[ -f "$file" ]]; then
            SIZE_MB=$(du -m "$file" | cut -f1)
            TOTAL_PARQUET_SIZE=$((TOTAL_PARQUET_SIZE + SIZE_MB))
            PARQUET_COUNT=$((PARQUET_COUNT + 1))
        fi
    done
    echo "Total Parquet files: $PARQUET_COUNT"
    echo "Total output size: ${TOTAL_PARQUET_SIZE}MB"
    
    # Python for detailed stats (if env exists)
    if [[ -d "$CONDA_ENV" ]]; then
        PYTHON_SCRIPT=$(cat << 'EOF'
import os
import sys
import pandas as pd
from glob import glob

job_workdir = os.environ.get('JOB_WORKDIR', '.')
analysis_dir = sys.argv[1] if len(sys.argv) > 1 else os.path.join(job_workdir, 'output')
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
            # Simple low coverage check (integer for bash; assumes PCT_WITH_PC is numeric)
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

# Append to report
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

# Append recommendations to report
{
    echo "4. Overall Review"
    echo "Success rate: ${SUCCESS_RATE}%"
    echo "Recommendations: See console or review high-error files."
    echo ""
    echo "End of Report"
} >> "$REPORT_FILE"

echo "${BLUE}Analysis complete. Review $REPORT_FILE for details.${RESET}"#!/bin/bash

# Unified Job Analysis Script for Common Crawl Outputs
# Usage: ./job-analyzer.sh [JOB_PREFIX] [--dry-run] [--output-dir=DIR]
# Example: ./job-analyzer.sh crawl_job_long --output-dir=output
# Analyzes *.out, *.err, *.log, and *.parquet files for errors, summaries, and data quality.

# Safe globs: Expand to empty if no matches (replaces 2>/dev/null for silence)
shopt -s nullglob

# Source config for dual-dir setup (JOB_WORKDIR, SLURM_ACCOUNT, etc.)
if [[ -f "slurm-config.sh" ]]; then
    source slurm-config.sh 2>/dev/null || echo "Warning: Sourcing slurm-config.sh had unexpected output."
else
    echo "Warning: slurm-config.sh not found. Set JOB_WORKDIR manually if needed."
fi

# Ensure JOB_WORKDIR is set (fallback to current dir)
if [[ -z "${JOB_WORKDIR:-}" ]]; then
    echo "Note: JOB_WORKDIR not set. Using current dir for analysis."
    JOB_WORKDIR="."
fi

set -euo pipefail  # Exit on error, undefined vars, pipe failures

# Colors for output (with fallback if tput not available)
RED=$(tput setaf 1 2>/dev/null || true; echo -n ${RED:-})
GREEN=$(tput setaf 2 2>/dev/null || true; echo -n ${GREEN:-})
YELLOW=$(tput setaf 3 2>/dev/null || true; echo -n ${YELLOW:-})
BLUE=$(tput setaf 4 2>/dev/null || true; echo -n ${BLUE:-})
RESET=$(tput sgr0 2>/dev/null || true; echo -n ${RESET:-})

# Defaults
JOB_PREFIX="${1:-crawl_job}"
DRY_RUN="${2:-}"
ANALYSIS_DIR="${JOB_WORKDIR}/output"  # Default to WORKING_DIR/output for dual-dir
REPORT_FILE="${JOB_WORKDIR}/analysis_report_${JOB_PREFIX}.txt"  # Save report in WORKING_DIR
ERROR_THRESHOLD=5  # Warn if errors > this
CONDA_ENV="./.conda_env"  # Path to env for Parquet analysis

if [[ "$DRY_RUN" == "--dry-run" ]]; then
    echo "${YELLOW}Dry-run mode: Simulating analysis without real checks.${RESET}"
    exit 0
fi

# Override ANALYSIS_DIR if --output-dir provided (simple arg parsing)
for arg in "$@"; do
    if [[ $arg == --output-dir=* ]]; then
        ANALYSIS_DIR="${arg#*=}"
    fi
done

echo "${BLUE}=== Job Analysis Report for Prefix: $JOB_PREFIX ===${RESET}"
echo "Analyzing in: $ANALYSIS_DIR"
echo "Report generated: $(date)"
echo "Saving detailed report to: $REPORT_FILE"
echo ""

# Create report file header
{
    echo "Job Analysis Report for $JOB_PREFIX"
    echo "Generated: $(date)"
    echo ""
} > "$REPORT_FILE"

# 1. ERROR AND FAILURE ANALYSIS (*.err and *.log)
echo "${GREEN}1. Error and Failure Summary${RESET}"
echo "-----------------------------"
ERROR_COUNT=0
WARNING_COUNT=0
FAILURE_DETAILS=""

# Scan *.err files (nullglob makes empty array if no files)
ERR_FILES=("$ANALYSIS_DIR"/*_${JOB_PREFIX}*.err "$ANALYSIS_DIR"/*.err)
for file in "${ERR_FILES[@]}"; do
    if [[ -f "$file" ]]; then
        ERRORS=$(grep -i "error\|failed\|exception\|timeout" "$file" | wc -l)
        WARNINGS=$(grep -i "warning\|low\|insufficient" "$file" | wc -l)
        ERROR_COUNT=$((ERROR_COUNT + ERRORS))
        WARNING_COUNT=$((WARNING_COUNT + WARNINGS))
        FAILURE_DETAILS+=$(grep -i "error\|failed" "$file" | head -3)  # Top 3 details
        FAILURE_DETAILS+="\nFrom file: $file\n\n"
    fi
done

# Scan *.log files for failures
LOG_FILES=("$ANALYSIS_DIR"/*_${JOB_PREFIX}*.log "$ANALYSIS_DIR"/*.log)
for file in "${LOG_FILES[@]}"; do
    if [[ -f "$file" ]]; then
        ERRORS=$(grep -i "error\|failed to download\|error processing|logging.error" "$file" | wc -l)
        WARNINGS=$(grep -i "warning\|no records|low disk" "$file" | wc -l)
        ERROR_COUNT=$((ERROR_COUNT + ERRORS))
        WARNING_COUNT=$((WARNING_COUNT + WARNINGS))
        FAILURE_DETAILS+=$(grep -i "error\|failed" "$file" | head -3)
        FAILURE_DETAILS+="\nFrom file: $file\n\n"
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

# Append to report
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

# Capture globs in temp array for safe iteration
TEMP_FILES=("${LOG_FILES[@]}" "$ANALYSIS_DIR"/*_${JOB_PREFIX}*.out "$ANALYSIS_DIR"/*.out)
for file in "${TEMP_FILES[@]}"; do
    if [[ -f "$file" ]]; then
        # Count successful segments (e.g., "Completed processing segment X: Y records")
        SUCC=$(grep -o "Completed processing segment" "$file" | wc -l)
        SUCCESSFUL_SEGMENTS=$((SUCCESSFUL_SEGMENTS + SUCC))
        
        # Sum records (extract Y from "Y records")
        RECS=$(grep "Completed processing segment" "$file" | awk '{sum += $NF} END {print sum + 0}' || echo "0")
        TOTAL_RECORDS=$((TOTAL_RECORDS + RECS))
        
        # Average time: Assume logs have "Processing time: X sec" or estimate from heartbeats (heuristic)
        TIMES=$(grep -i "processing\|heartbeat" "$file" | awk '{if ($0 ~ /sec/) sum += $NF} END {print (sum > 0 ? sum / NR : 0)}' || echo "0")
        if [[ $AVG_TIME_PER_SEG -eq 0 ]]; then
            AVG_TIME_PER_SEG=$TIMES
        else
            AVG_TIME_PER_SEG=$(( (AVG_TIME_PER_SEG + TIMES) / 2 ))
        fi
        
        LOG_DETAILS+="Successes: $SUCC segments, ~${RECS} records from $file\n"
    fi
done

# Avoid div-by-zero
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

# Append to report
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

PARQUET_FILES=("$ANALYSIS_DIR"/crawldata*.parquet)

if [[ ${#PARQUET_FILES[@]} -eq 0 ]]; then
    echo "${YELLOW}No Parquet files found. Skipping analysis.${RESET}"
else
    echo "Found ${#PARQUET_FILES[@]} Parquet files. Analyzing..."
    
    # Simple size check (shell)
    for file in "${PARQUET_FILES[@]}"; do
        if [[ -f "$file" ]]; then
            SIZE_MB=$(du -m "$file" | cut -f1)
            TOTAL_PARQUET_SIZE=$((TOTAL_PARQUET_SIZE + SIZE_MB))
            PARQUET_COUNT=$((PARQUET_COUNT + 1))
        fi
    done
    echo "Total Parquet files: $PARQUET_COUNT"
    echo "Total output size: ${TOTAL_PARQUET_SIZE}MB"
    
    # Python for detailed stats (if env exists)
    if [[ -d "$CONDA_ENV" ]]; then
        PYTHON_SCRIPT=$(cat << 'EOF'
import os
import sys
import pandas as pd
from glob import glob

job_workdir = os.environ.get('JOB_WORKDIR', '.')
analysis_dir = sys.argv[1] if len(sys.argv) > 1 else os.path.join(job_workdir, 'output')
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
            # Simple low coverage check (integer for bash; assumes PCT_WITH_PC is numeric)
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

# Append to report
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

# Append recommendations to report
{
    echo "4. Overall Review"
    echo "Success rate: ${SUCCESS_RATE}%"
    echo "Recommendations: See console or review high-error files."
    echo ""
    echo "End of Report"
} >> "$REPORT_FILE"

echo "${BLUE}Analysis complete. Review $REPORT_FILE for details.${RESET}"