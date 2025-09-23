#!/bin/bash

echo "=========================================="
echo "Disk Space Failure Analysis"
echo "=========================================="
echo "Generated: $(date)"
echo ""

echo "💾 CURRENT DISK USAGE"
echo "----------------------------------------"
df -h . | head -2
echo ""
du -sh . 2>/dev/null || echo "Cannot calculate directory size"

echo ""
echo "📁 OUTPUT DIRECTORY ANALYSIS"
echo "----------------------------------------"
for dir in output 202104-output 202110-output; do
    if [ -d "$dir" ]; then
        echo "📂 $dir:"
        echo "   Size: $(du -sh $dir 2>/dev/null | cut -f1)"
        echo "   Files: $(find $dir -type f 2>/dev/null | wc -l)"
        echo "   Empty files: $(find $dir -size 0 2>/dev/null | wc -l)"
    fi
done

echo ""
echo "🚨 DISK SPACE ERROR ANALYSIS"
echo "----------------------------------------"
echo "🔍 Checking SLURM logs for disk space errors..."

SPACE_ERRORS=$(grep -l -i "space\|disk.*full\|quota.*exceeded\|filesystem.*full" slurm-*.err 2>/dev/null | wc -l)
echo "📊 Jobs with disk space errors: $SPACE_ERRORS"

if [ $SPACE_ERRORS -gt 0 ]; then
    echo ""
    echo "📋 Sample disk space errors:"
    grep -i -A2 -B1 "space\|disk.*full\|quota" slurm-*.err 2>/dev/null | head -10
fi

echo ""
echo "🔍 Checking for exit code 28 (No space left):"
EXIT_28_COUNT=$(grep -c "exit.*28\|Exit.*28" slurm-*.{out,err} 2>/dev/null | awk -F: '{sum+=$2} END{print sum+0}')
echo "📊 Jobs with exit code 28: $EXIT_28_COUNT"

echo ""
echo "📈 TEMPORARY FILE ANALYSIS"
echo "----------------------------------------"
echo "🗂️ Checking for large temporary files..."
find . -name "*.tmp" -o -name "temp*" -o -name "*.temp" 2>/dev/null | head -5
find . -name "*.tmp" -o -name "temp*" -o -name "*.temp" 2>/dev/null | wc -l | xargs echo "Temporary files found:"

echo ""
echo "🧹 CLEANUP RECOMMENDATIONS"
echo "----------------------------------------"
echo "🗑️ If disk space is low, consider:"
echo "   • Remove old SLURM logs: rm slurm-*.{out,err}"
echo "   • Clean conda cache: ./miniconda3/bin/conda clean --all"
echo "   • Remove temporary files: find . -name '*.tmp' -delete"
echo "   • Compress old output: tar -czf output_backup.tar.gz output/"

