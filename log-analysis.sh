#!/bin/bash

echo "=========================================="
echo "SLURM Log File Analysis"
echo "=========================================="
echo "Generated: $(date)"
echo ""

# Use JOB_WORKDIR if set, otherwise current directory
WORK_DIR="${JOB_WORKDIR:-$(pwd)}"
echo "Working directory: $WORK_DIR"
echo ""

# Count log files by extension
OUT_FILES=$(ls "$WORK_DIR"/*.out 2>/dev/null | wc -l)
ERR_FILES=$(ls "$WORK_DIR"/*.err 2>/dev/null | wc -l)
LOG_FILES=$(ls "$WORK_DIR"/*.log 2>/dev/null | wc -l)

echo "📄 LOG FILE SUMMARY"
echo "----------------------------------------"
echo "📝 Output files (.out): $OUT_FILES"
echo "⚠️  Error files (.err): $ERR_FILES"
echo "📋 Log files (.log): $LOG_FILES"
echo ""

# Check for errors in stderr files
echo "🚨 ERROR ANALYSIS"
echo "----------------------------------------"
if [ $ERR_FILES -gt 0 ]; then
    echo "🔍 Checking error files for issues..."
    
    # Count files with actual errors (non-empty)
    NON_EMPTY_ERRORS=$(find "$WORK_DIR" -name "*.err" -size +0 | wc -l)
    echo "📊 Files with error output: $NON_EMPTY_ERRORS"
    
    if [ $NON_EMPTY_ERRORS -gt 0 ]; then
        echo ""
        echo "🔍 Sample errors (first 5 files with errors):"
        find "$WORK_DIR" -name "*.err" -size +0 | head -5 | while read file; do
            echo "📁 $(basename $file):"
            head -3 "$file" | sed 's/^/   /'
            echo ""
        done
        
        echo "🏷️  Common error patterns:"
        cat "$WORK_DIR"/*.err 2>/dev/null | grep -i "error\|exception\|failed\|timeout" | sort | uniq -c | sort -nr | head -10
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
    SUCCESSFUL_COMPLETIONS=$(grep -l "successfully\|completed\|finished" "$WORK_DIR"/*.out 2>/dev/null | wc -l)
    echo "🎯 Jobs with completion messages: $SUCCESSFUL_COMPLETIONS"
    
    # Check for timeout or kill messages
    TIMEOUTS=$(grep -l "timeout\|killed\|terminated" "$WORK_DIR"/*.out 2>/dev/null | wc -l)
    echo "⏰ Jobs with timeout/kill messages: $TIMEOUTS"
    
    # Check for memory issues
    MEMORY_ISSUES=$(grep -l "memory\|oom\|killed" "$WORK_DIR"/*.err 2>/dev/null | wc -l)
    echo "💾 Jobs with memory issues: $MEMORY_ISSUES"
    
    echo ""
    echo "📊 Recent log file activity:"
    ls -lt "$WORK_DIR"/*.{out,err,log} 2>/dev/null | head -5
else
    echo "ℹ️  No output files found"
fi

echo ""

# Performance insights from logs
echo "🚀 PERFORMANCE INSIGHTS FROM LOGS"
echo "----------------------------------------"

# Look for processing time information in both .out and .log files
if [ $OUT_FILES -gt 0 ] || [ $LOG_FILES -gt 0 ]; then
    echo "⏱️  Processing times found in logs:"
    
    # Check .out files
    if [ $OUT_FILES -gt 0 ]; then
        grep -h "processed\|took\|elapsed\|duration" "$WORK_DIR"/*.out 2>/dev/null | head -3 | sed 's/^/   OUT: /'
    fi
    
    # Check .log files (your Python processor logs)
    if [ $LOG_FILES -gt 0 ]; then
        grep -h "processed\|took\|elapsed\|duration\|time" "$WORK_DIR"/*.log 2>/dev/null | head -3 | sed 's/^/   LOG: /'
    fi
fi

echo ""

# Analysis of Python processor logs
if [ $LOG_FILES -gt 0 ]; then
    echo "🐍 PYTHON PROCESSOR LOG ANALYSIS"
    echo "----------------------------------------"
    
    # Look for successful processing patterns
    SUCCESS_PATTERNS=$(grep -c "success\|complete\|finished\|processed.*files" "$WORK_DIR"/*.log 2>/dev/null | grep -v ":0" | wc -l)
    echo "📊 Logs with success indicators: $SUCCESS_PATTERNS"
    
    # Look for common error patterns in Python logs
    PYTHON_ERRORS=$(grep -l "ERROR\|Exception\|Traceback\|Failed" "$WORK_DIR"/*.log 2>/dev/null | wc -l)
    echo "🐛 Logs with Python errors: $PYTHON_ERRORS"
    
    if [ $PYTHON_ERRORS -gt 0 ]; then
        echo ""
        echo "🔍 Sample Python errors:"
        grep -h "ERROR\|Exception" "$WORK_DIR"/*.log 2>/dev/null | head -3 | sed 's/^/   /'
    fi
fi

echo ""

# File size analysis
echo "📊 FILE SIZE ANALYSIS"
echo "----------------------------------------"
if [ $OUT_FILES -gt 0 ] || [ $ERR_FILES -gt 0 ] || [ $LOG_FILES -gt 0 ]; then
    echo "💾 Total log space usage:"
    du -sh "$WORK_DIR"/*.{out,err,log} 2>/dev/null | awk '{total+=$1} END {print "   Total: " total}'
    
    echo ""
    echo "📏 Largest log files:"
    ls -lSh "$WORK_DIR"/*.{out,err,log} 2>/dev/null | head -5 | awk '{print "   " $9 " (" $5 ")"}'
fi

echo ""
echo "=========================================="
echo "💡 Additional log analysis commands:"
echo "   🔍 Search for specific error: grep -r 'error_text' $WORK_DIR/*.err"
echo "   📊 Count specific patterns: grep -c 'pattern' $WORK_DIR/*.out"
echo "   📝 View specific log: less $WORK_DIR/FILENAME.{out,err,log}"
echo "   🗑️  Clean old logs: rm $WORK_DIR/*.{out,err,log}"
echo "   🐍 Check Python logs: grep -i error $WORK_DIR/*.log"
echo "   📈 Monitor real-time: tail -f $WORK_DIR/*.log"
echo "=========================================="
