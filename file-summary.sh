#!/bin/bash

# Quick Output Files Summary
# Simple analysis of generated files from Common Crawl processing

echo "=========================================="
echo "📊 QUICK OUTPUT SUMMARY"
echo "=========================================="
echo "Generated: $(date)"
echo ""

# Function to format bytes
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

echo "🎯 KEY METRICS"
echo "----------------------------------------"

# Count output files and size
OUTPUT_FILES=$(find output* -name "*.parquet" 2>/dev/null | wc -l)
OUTPUT_SIZE=$(find output* -name "*.parquet" 2>/dev/null | xargs stat -c%s 2>/dev/null | awk '{sum+=$1} END {print sum+0}')

echo "📊 Parquet output files: $OUTPUT_FILES"
echo "💾 Total parquet size: $(format_bytes $OUTPUT_SIZE)"

# SLURM logs
SLURM_OUT=$(ls *.out 2>/dev/null | wc -l)
SLURM_ERR=$(ls *.err 2>/dev/null | wc -l)

echo "📋 SLURM output logs: $SLURM_OUT"
echo "📋 SLURM error logs: $SLURM_ERR"

# Recent activity
RECENT_1H=$(find output* -name "*.parquet" -newermt "1 hour ago" 2>/dev/null | wc -l)
RECENT_6H=$(find output* -name "*.parquet" -newermt "6 hours ago" 2>/dev/null | wc -l)

echo "🕐 Files created last hour: $RECENT_1H"
echo "🕕 Files created last 6 hours: $RECENT_6H"

# Output directories
echo ""
echo "📁 OUTPUT DIRECTORIES"
echo "----------------------------------------"
for dir in output*; do
    if [ -d "$dir" ]; then
        files=$(find "$dir" -name "*.parquet" 2>/dev/null | wc -l)
        size=$(find "$dir" -name "*.parquet" 2>/dev/null | xargs stat -c%s 2>/dev/null | awk '{sum+=$1} END {print sum+0}')
        echo "📂 $dir: $files files ($(format_bytes $size))"
    fi
done

# Disk usage
echo ""
echo "💿 STORAGE"
echo "----------------------------------------"
TOTAL_SIZE=$(du -sb . 2>/dev/null | awk '{print $1}')
AVAIL_SPACE=$(df . | tail -1 | awk '{print $4 * 1024}')
USED_PERCENT=$(df . | tail -1 | awk '{print $5}' | tr -d '%')

echo "📁 Project total size: $(format_bytes $TOTAL_SIZE)"
echo "💽 Available space: $(format_bytes $AVAIL_SPACE)"
echo "📊 Disk usage: $USED_PERCENT%"

echo ""
echo "=========================================="