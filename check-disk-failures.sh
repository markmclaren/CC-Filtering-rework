#!/bin/bash

echo "=========================================="
echo "Disk Space Failure Analysis"
echo "=========================================="
echo "Generated: $(date)"
echo ""

# Use JOB_WORKDIR if set, otherwise current directory
WORK_DIR="${JOB_WORKDIR:-$(pwd)}"
echo "Working directory: $WORK_DIR"
echo ""

echo "💾 CURRENT DISK USAGE"
echo "----------------------------------------"
df -h "$WORK_DIR" | head -2
echo ""
du -sh "$WORK_DIR" 2>/dev/null || echo "Cannot calculate directory size"

echo ""
echo "📁 OUTPUT DIRECTORY ANALYSIS"
echo "----------------------------------------"
for dir in output; do
    full_dir="$WORK_DIR/$dir"
    if [ -d "$full_dir" ]; then
        echo "📂 $dir:"
        echo "   Size: $(du -sh $full_dir 2>/dev/null | cut -f1)"
        echo "   Files: $(find $full_dir -type f 2>/dev/null | wc -l)"
        echo "   Empty files: $(find $full_dir -size 0 2>/dev/null | wc -l)"
    fi
done

echo ""
echo "🚨 DISK SPACE ERROR ANALYSIS"
echo "----------------------------------------"
echo "🔍 Checking SLURM logs for disk space errors..."

SPACE_ERRORS=$(grep -l -i "space\|disk.*full\|quota.*exceeded\|filesystem.*full" "$WORK_DIR"/*.err 2>/dev/null | wc -l)
echo "📊 Jobs with disk space errors: $SPACE_ERRORS"

if [ $SPACE_ERRORS -gt 0 ]; then
    echo ""
    echo "📋 Sample disk space errors:"
    grep -i -A2 -B1 "space\|disk.*full\|quota" "$WORK_DIR"/*.err 2>/dev/null | head -10
fi

echo ""
echo "🔍 Checking for exit code 28 (No space left):"
EXIT_28_COUNT=$(grep -c "exit.*28\|Exit.*28" "$WORK_DIR"/*.{out,err} 2>/dev/null | awk -F: '{sum+=$2} END{print sum+0}')
echo "📊 Jobs with exit code 28: $EXIT_28_COUNT"

echo ""
echo "📈 TEMPORARY FILE ANALYSIS"
echo "----------------------------------------"
echo "🗂️ Checking for large temporary files..."
find "$WORK_DIR" -name "*.tmp" -o -name "temp*" -o -name "*.temp" 2>/dev/null | head -5
find "$WORK_DIR" -name "*.tmp" -o -name "temp*" -o -name "*.temp" 2>/dev/null | wc -l | xargs echo "Temporary files found:"

echo ""
echo "📊 LOG FILE DISK USAGE"
echo "----------------------------------------"
echo "💾 SLURM log file usage:"
OUT_SIZE=$(find "$WORK_DIR" -name "*.out" -exec du -ch {} + 2>/dev/null | tail -1 | cut -f1)
ERR_SIZE=$(find "$WORK_DIR" -name "*.err" -exec du -ch {} + 2>/dev/null | tail -1 | cut -f1)
LOG_SIZE=$(find "$WORK_DIR" -name "*.log" -exec du -ch {} + 2>/dev/null | tail -1 | cut -f1)

echo "   Output files (.out): $OUT_SIZE"
echo "   Error files (.err): $ERR_SIZE"
echo "   Python logs (.log): $LOG_SIZE"

echo ""
echo "🧹 CLEANUP RECOMMENDATIONS"
echo "----------------------------------------"
echo "🗑️ If disk space is low, consider:"
echo "   • Remove old SLURM logs: rm $WORK_DIR/*.{out,err}"
echo "   • Remove Python logs: rm $WORK_DIR/*.log"
echo "   • Clean conda cache: ./miniconda3/bin/conda clean --all"
echo "   • Remove temporary files: find $WORK_DIR -name '*.tmp' -delete"
echo "   • Compress old output: tar -czf $WORK_DIR/output_backup.tar.gz $WORK_DIR/output/"
echo "   • Archive old logs: tar -czf $WORK_DIR/logs_backup.tar.gz $WORK_DIR/*.{out,err,log} && rm $WORK_DIR/*.{out,err,log}"

echo ""
echo "=========================================="
echo "💡 Useful commands:"
echo "   📊 Check largest files: find $WORK_DIR -type f -exec du -h {} + | sort -hr | head -20"
echo "   🔍 Find files over 100MB: find $WORK_DIR -size +100M -exec ls -lh {} \;"
echo "   📁 Check inode usage: df -i $WORK_DIR"
echo "   🗂️ Count files by type: find $WORK_DIR -type f | sed 's/.*\.//' | sort | uniq -c | sort -nr"
echo "=========================================="

