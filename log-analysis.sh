#!/bin/bash

echo "=========================================="
echo "SLURM Log File Analysis"
echo "=========================================="
echo "Generated: $(date)"
echo ""

# Count log files
OUT_FILES=$(ls slurm-*.out 2>/dev/null | wc -l)
ERR_FILES=$(ls slurm-*.err 2>/dev/null | wc -l)

echo "📄 LOG FILE SUMMARY"
echo "----------------------------------------"
echo "📝 Output files (.out): $OUT_FILES"
echo "⚠️  Error files (.err): $ERR_FILES"
echo ""

# Check for errors in stderr files
echo "🚨 ERROR ANALYSIS"
echo "----------------------------------------"
if [ $ERR_FILES -gt 0 ]; then
    echo "🔍 Checking error files for issues..."
    
    # Count files with actual errors (non-empty)
    NON_EMPTY_ERRORS=$(find . -name "slurm-*.err" -size +0 | wc -l)
    echo "📊 Files with error output: $NON_EMPTY_ERRORS"
    
    if [ $NON_EMPTY_ERRORS -gt 0 ]; then
        echo ""
        echo "🔍 Sample errors (first 5 files with errors):"
        find . -name "slurm-*.err" -size +0 | head -5 | while read file; do
            echo "📁 $file:"
            head -3 "$file" | sed 's/^/   /'
            echo ""
        done
        
        echo "🏷️  Common error patterns:"
        cat slurm-*.err 2>/dev/null | grep -i "error\|exception\|failed\|timeout" | sort | uniq -c | sort -nr | head -10
    else
        echo "✅ No errors found in stderr files!"
    fi
else
    echo "ℹ️  No error files found"
fi

echo ""

# Check output files for completion messages
echo "✅ COMPLETION ANALYSIS"
echo "----------------------------------------"
if [ $OUT_FILES -gt 0 ]; then
    SUCCESSFUL_COMPLETIONS=$(grep -l "successfully\|completed\|finished" slurm-*.out 2>/dev/null | wc -l)
    echo "🎯 Jobs with completion messages: $SUCCESSFUL_COMPLETIONS"
    
    # Check for timeout or kill messages
    TIMEOUTS=$(grep -l "timeout\|killed\|terminated" slurm-*.out 2>/dev/null | wc -l)
    echo "⏰ Jobs with timeout/kill messages: $TIMEOUTS"
    
    # Check for memory issues
    MEMORY_ISSUES=$(grep -l "memory\|oom\|killed" slurm-*.err 2>/dev/null | wc -l)
    echo "💾 Jobs with memory issues: $MEMORY_ISSUES"
    
    echo ""
    echo "📊 Recent log file activity:"
    ls -lt slurm-*.{out,err} 2>/dev/null | head -5
else
    echo "ℹ️  No output files found"
fi

echo ""

# Performance insights from logs
echo "🚀 PERFORMANCE INSIGHTS FROM LOGS"
echo "----------------------------------------"

# Look for processing time information
if [ $OUT_FILES -gt 0 ]; then
    echo "⏱️  Processing times found in logs:"
    grep -h "processed\|took\|elapsed\|duration" slurm-*.out 2>/dev/null | head -5 | sed 's/^/   /'
fi

echo ""
echo "=========================================="
echo "💡 Additional log analysis commands:"
echo "   🔍 Search for specific error: grep -r 'error_text' slurm-*.err"
echo "   📊 Count specific patterns: grep -c 'pattern' slurm-*.out"
echo "   📝 View specific log: less slurm-JOBID.out"
echo "   🗑️  Clean old logs: rm slurm-*.{out,err}"
echo "=========================================="
