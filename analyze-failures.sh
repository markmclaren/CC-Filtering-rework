#!/bin/bash

echo "=========================================="
echo "Exit Code 120 Failure Analysis"
echo "=========================================="
echo "Generated: $(date)"
echo ""

# Use JOB_WORKDIR if set, otherwise current directory
WORK_DIR="${JOB_WORKDIR:-$(pwd)}"
echo "Working directory: $WORK_DIR"
echo ""

echo "🔍 ANALYZING FAILED JOBS WITH EXIT CODE 120"
echo "----------------------------------------"

# Count total failures by looking at all .err files
TOTAL_FAILURES=$(ls "$WORK_DIR"/*.err 2>/dev/null | wc -l)
echo "📊 Total failure logs found: $TOTAL_FAILURES"

echo ""
echo "🚨 COMMON ERROR PATTERNS"
echo "----------------------------------------"

# Look for common error patterns in stderr files
echo "🔍 Python errors:"
grep -h -i "python\|traceback\|exception" "$WORK_DIR"/*.err 2>/dev/null | sort | uniq -c | sort -nr | head -5

echo ""
echo "🔍 File/Data errors:"
grep -h -i "file\|data\|key.*error\|not found" "$WORK_DIR"/*.err 2>/dev/null | sort | uniq -c | sort -nr | head -5

echo ""
echo "🔍 Network/Download errors:"
grep -h -i "download\|http\|connection\|timeout\|fetch" "$WORK_DIR"/*.err 2>/dev/null | sort | uniq -c | sort -nr | head -5

echo ""
echo "🔍 Memory/Resource errors:"
grep -h -i "memory\|killed\|oom" "$WORK_DIR"/*.err 2>/dev/null | sort | uniq -c | sort -nr | head -5

echo ""
echo "📝 SAMPLE ERROR FROM RECENT FAILED JOB"
echo "----------------------------------------"
# Find the most recent .err file with content
RECENT_ERR=$(find "$WORK_DIR" -name "*.err" -size +0 -printf '%T@ %p\n' 2>/dev/null | sort -n | tail -1 | cut -d' ' -f2-)
if [ ! -z "$RECENT_ERR" ]; then
    echo "📁 From $(basename "$RECENT_ERR"):"
    head -10 "$RECENT_ERR"
else
    echo "❌ No error files with content found"
fi

echo ""
echo "📊 SAMPLE OUTPUT FROM CORRESPONDING JOB"
echo "----------------------------------------"
if [ ! -z "$RECENT_ERR" ]; then
    # Extract job ID from error filename and look for corresponding .out file
    JOB_ID=$(basename "$RECENT_ERR" | grep -o '[0-9]\{7,\}')
    CORRESPONDING_OUT=$(ls "$WORK_DIR"/*${JOB_ID}*.out 2>/dev/null | head -1)
    
    if [ ! -z "$CORRESPONDING_OUT" ]; then
        echo "📁 From $(basename "$CORRESPONDING_OUT"):"
        head -10 "$CORRESPONDING_OUT"
    else
        echo "❌ No corresponding output file found for job $JOB_ID"
    fi
else
    echo "❌ No job ID could be determined"
fi

echo ""
echo "🔢 ERROR FILE SIZE ANALYSIS"
echo "----------------------------------------"
# Show files with largest error output (likely have more detailed errors)
echo "📊 Largest error files (most verbose errors):"
ls -lSh "$WORK_DIR"/*.err 2>/dev/null | head -5 | awk '{print "   " $9 " (" $5 ")"}'

echo ""
echo "📈 ERROR FREQUENCY BY JOB"
echo "----------------------------------------"
# Count error files per job ID pattern
if [ $TOTAL_FAILURES -gt 0 ]; then
    echo "📊 Jobs with errors:"
    ls "$WORK_DIR"/*.err 2>/dev/null | grep -o '[0-9]\{7,\}' | sort | uniq -c | sort -nr | head -10 | \
    awk '{printf "   Job %s: %s error files\n", $2, $1}'
fi

echo ""
echo "=========================================="
echo "💡 Next steps:"
echo "   📝 Check specific error: less [specific_error_file.err]"
echo "   🔍 Search pattern: grep -r 'specific_error' $WORK_DIR/*.err"
echo "   📊 Count pattern: grep -c 'pattern' $WORK_DIR/*.err | sort -t: -k2 -nr"
echo "   🔍 Find largest errors: find $WORK_DIR -name '*.err' -size +1k -exec ls -lh {} \;"
echo "   📋 Check corresponding logs: ls $WORK_DIR/*.log"
echo "=========================================="
