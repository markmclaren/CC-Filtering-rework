#!/bin/bash

echo "=========================================="
echo "Exit Code 120 Failure Analysis"
echo "=========================================="
echo "Generated: $(date)"
echo ""

echo "🔍 ANALYZING FAILED JOBS WITH EXIT CODE 120"
echo "----------------------------------------"

# Count total failures
TOTAL_FAILURES=$(ls slurm-13539494_*.err 2>/dev/null | wc -l)
echo "📊 Total failure logs found: $TOTAL_FAILURES"

echo ""
echo "🚨 COMMON ERROR PATTERNS"
echo "----------------------------------------"

# Look for common error patterns in stderr files
echo "🔍 Python errors:"
grep -h -i "python\|traceback\|exception" slurm-13539494_*.err 2>/dev/null | sort | uniq -c | sort -nr | head -5

echo ""
echo "🔍 File/Data errors:"
grep -h -i "file\|data\|key.*error\|not found" slurm-13539494_*.err 2>/dev/null | sort | uniq -c | sort -nr | head -5

echo ""
echo "🔍 Network/Download errors:"
grep -h -i "download\|http\|connection\|timeout\|fetch" slurm-13539494_*.err 2>/dev/null | sort | uniq -c | sort -nr | head -5

echo ""
echo "🔍 Memory/Resource errors:"
grep -h -i "memory\|killed\|oom" slurm-13539494_*.err 2>/dev/null | sort | uniq -c | sort -nr | head -5

echo ""
echo "📝 SAMPLE ERROR FROM FIRST FAILED JOB"
echo "----------------------------------------"
if [ -f "slurm-13539494_2434.err" ]; then
    echo "📁 From slurm-13539494_2434.err:"
    head -10 slurm-13539494_2434.err
else
    echo "❌ No error file found for job 2434"
fi

echo ""
echo "📊 SAMPLE OUTPUT FROM FIRST FAILED JOB"
echo "----------------------------------------"
if [ -f "slurm-13539494_2434.out" ]; then
    echo "📁 From slurm-13539494_2434.out:"
    head -10 slurm-13539494_2434.out
else
    echo "❌ No output file found for job 2434"
fi

echo ""
echo "=========================================="
echo "💡 Next steps:"
echo "   📝 Check specific job: less slurm-13539494_2434.err"
echo "   🔍 Search pattern: grep -r 'specific_error' slurm-*.err"
echo "   📊 Count pattern: grep -c 'pattern' slurm-*.err | sort -t: -k2 -nr"
echo "=========================================="
