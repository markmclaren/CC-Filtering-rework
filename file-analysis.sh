#!/bin/bash

# Output Files Analysis Script
# Analyzes generated files from Common Crawl processing jobs

echo "=========================================="
echo "OUTPUT FILES ANALYSIS REPORT"
echo "=========================================="
echo "Generated: $(date)"
echo ""

# Function to format bytes in human readable format
format_bytes() {
    local bytes=$1
    if [ $bytes -ge 1073741824 ]; then
        echo "$(echo "scale=2; $bytes / 1073741824" | bc -l) GB"
    elif [ $bytes -ge 1048576 ]; then
        echo "$(echo "scale=2; $bytes / 1048576" | bc -l) MB"
    elif [ $bytes -ge 1024 ]; then
        echo "$(echo "scale=2; $bytes / 1024" | bc -l) KB"
    else
        echo "$bytes bytes"
    fi
}

echo "📁 OUTPUT DIRECTORIES ANALYSIS"
echo "----------------------------------------"

# Check common output directories
OUTPUT_DIRS=("output" "202104-output" "generated_scripts" "miniconda3" ".conda_env")

TOTAL_FILES=0
TOTAL_SIZE=0

for dir in "${OUTPUT_DIRS[@]}"; do
    if [ -d "$dir" ]; then
        echo ""
        echo "📂 Directory: $dir"
        echo "   ├── Exists: ✅"
        
        # Count files
        FILE_COUNT=$(find "$dir" -type f 2>/dev/null | wc -l)
        echo "   ├── Files: $FILE_COUNT"
        
        # Calculate total size
        DIR_SIZE=$(find "$dir" -type f -exec stat -c%s {} + 2>/dev/null | awk '{sum+=$1} END {print sum+0}')
        READABLE_SIZE=$(format_bytes $DIR_SIZE)
        echo "   ├── Size: $READABLE_SIZE"
        
        # Show file types
        echo "   └── File types:"
        find "$dir" -type f 2>/dev/null | grep -o '\.[^.]*$' | sort | uniq -c | sort -nr | head -5 | while read count ext; do
            echo "       • $ext: $count files"
        done
        
        TOTAL_FILES=$((TOTAL_FILES + FILE_COUNT))
        TOTAL_SIZE=$((TOTAL_SIZE + DIR_SIZE))
    else
        echo ""
        echo "📂 Directory: $dir"
        echo "   └── Status: ❌ Not found"
    fi
done

echo ""
echo "📊 SUMMARY STATISTICS"
echo "----------------------------------------"
echo "🗂️  Total output files: $TOTAL_FILES"
echo "💾 Total output size: $(format_bytes $TOTAL_SIZE)"

# Check for SLURM output files
echo ""
echo "📋 SLURM LOG FILES"
echo "----------------------------------------"

SLURM_OUT=$(find . -maxdepth 1 -name "*.out" 2>/dev/null | wc -l)
SLURM_ERR=$(find . -maxdepth 1 -name "*.err" 2>/dev/null | wc -l)
SLURM_SIZE=$(find . -maxdepth 1 -name "*.out" -o -name "*.err" 2>/dev/null | xargs stat -c%s 2>/dev/null | awk '{sum+=$1} END {print sum+0}')

echo "📄 SLURM output files (.out): $SLURM_OUT"
echo "📄 SLURM error files (.err): $SLURM_ERR"
echo "💾 SLURM logs total size: $(format_bytes $SLURM_SIZE)"

# Recent files analysis
echo ""
echo "⏰ RECENT FILE ACTIVITY"
echo "----------------------------------------"

echo "📅 Files created in last hour:"
RECENT_1H=$(find . -type f -newermt "1 hour ago" 2>/dev/null | wc -l)
echo "   └── Count: $RECENT_1H files"

echo "📅 Files created in last 6 hours:"
RECENT_6H=$(find . -type f -newermt "6 hours ago" 2>/dev/null | wc -l)
echo "   └── Count: $RECENT_6H files"

echo "📅 Files created today:"
RECENT_TODAY=$(find . -type f -newermt "today" 2>/dev/null | wc -l)
echo "   └── Count: $RECENT_TODAY files"

# Check for specific output patterns
echo ""
echo "🔍 OUTPUT FILE PATTERNS"
echo "----------------------------------------"

# Look for parquet files
PARQUET_COUNT=$(find . -name "*.parquet" 2>/dev/null | wc -l)
PARQUET_SIZE=$(find . -name "*.parquet" -exec stat -c%s {} + 2>/dev/null | awk '{sum+=$1} END {print sum+0}')
echo "📊 Parquet files: $PARQUET_COUNT ($(format_bytes $PARQUET_SIZE))"

# Look for CSV files
CSV_COUNT=$(find . -name "*.csv" 2>/dev/null | wc -l)
CSV_SIZE=$(find . -name "*.csv" -exec stat -c%s {} + 2>/dev/null | awk '{sum+=$1} END {print sum+0}')
echo "📈 CSV files: $CSV_COUNT ($(format_bytes $CSV_SIZE))"

# Look for JSON files
JSON_COUNT=$(find . -name "*.json" 2>/dev/null | wc -l)
JSON_SIZE=$(find . -name "*.json" -exec stat -c%s {} + 2>/dev/null | awk '{sum+=$1} END {print sum+0}')
echo "📋 JSON files: $JSON_COUNT ($(format_bytes $JSON_SIZE))"

# Show largest files
echo ""
echo "📏 LARGEST OUTPUT FILES"
echo "----------------------------------------"
find . -path "./output*" -name "*.parquet" 2>/dev/null | \
    xargs ls -la 2>/dev/null | \
    sort -k5 -nr | \
    head -10 | \
    while read perm links owner group size month day time filename; do
        readable=$(format_bytes $size)
        echo "💎 $(basename $filename) ($readable)"
    done

# If no parquet files found, show CSV files
if [ $(find . -path "./output*" -name "*.parquet" 2>/dev/null | wc -l) -eq 0 ]; then
    echo "No parquet files found in output directories"
    find . -path "./output*" -name "*.csv" 2>/dev/null | \
        xargs ls -la 2>/dev/null | \
        sort -k5 -nr | \
        head -5 | \
        while read perm links owner group size month day time filename; do
            readable=$(format_bytes $size)
            echo "💎 $(basename $filename) ($readable)"
        done
fi

# Disk space analysis
echo ""
echo "💿 DISK SPACE ANALYSIS"
echo "----------------------------------------"

# Current directory usage
CURRENT_SIZE=$(du -sb . 2>/dev/null | awk '{print $1}')
echo "📁 Current directory total: $(format_bytes $CURRENT_SIZE)"

# Available space
AVAIL_SPACE=$(df . | tail -1 | awk '{print $4 * 1024}')
echo "💽 Available space: $(format_bytes $AVAIL_SPACE)"

# Calculate percentage used
USED_PERCENT=$(df . | tail -1 | awk '{print $5}' | tr -d '%')
echo "📊 Disk usage: $USED_PERCENT%"

echo ""
echo "=========================================="
echo "💡 To refresh this report, run: ./file-analysis.sh"
echo "🗑️  To clean up logs, run: ./cleanup.sh"
echo "📂 To check specific directory: ls -lah DIRNAME"
echo "=========================================="