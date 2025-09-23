#!/bin/bash

echo "=========================================="
echo "SLURM Job Completion Review"
echo "=========================================="
echo "Generated: $(date)"
echo ""

# Use JOB_WORKDIR if set, otherwise current directory
WORK_DIR="${JOB_WORKDIR:-$(pwd)}"
echo "Working directory: $WORK_DIR"
echo ""

# Get the most recent completed job array (assuming it's your main job)
RECENT_JOB=$(sacct -u $USER --format=JobID,JobName,State,ExitCode,Start,End,Elapsed --parsable2 | grep "crawl_jo" | head -1 | cut -d'|' -f1)
MAIN_JOB=$(echo $RECENT_JOB | cut -d'_' -f1)

echo "🔍 ANALYZING JOB: $MAIN_JOB"
echo "----------------------------------------"

# Overall job summary
echo "📊 JOB SUMMARY"
echo "----------------------------------------"
sacct -j $MAIN_JOB --format=JobID,JobName,State,ExitCode,Start,End,Elapsed,ReqMem,MaxRSS,ReqCPUS --parsable2 | \
awk -F'|' 'NR==1{print $0} NR>1{printf "%-15s %-12s %-10s %-8s %-19s %-19s %-10s %-8s %-10s %-6s\n", $1,$2,$3,$4,$5,$6,$7,$8,$9,$10}'

echo ""

# Success/Failure analysis
echo "✅ SUCCESS/FAILURE ANALYSIS"
echo "----------------------------------------"
COMPLETED=$(sacct -j $MAIN_JOB --format=State --parsable2 --noheader | grep -c "COMPLETED")
FAILED=$(sacct -j $MAIN_JOB --format=State --parsable2 --noheader | grep -c "FAILED")
CANCELLED=$(sacct -j $MAIN_JOB --format=State --parsable2 --noheader | grep -c "CANCELLED")
TIMEOUT=$(sacct -j $MAIN_JOB --format=State --parsable2 --noheader | grep -c "TIMEOUT")
TOTAL_TASKS=$(sacct -j $MAIN_JOB --format=State --parsable2 --noheader | wc -l)

echo "✅ Completed tasks: $COMPLETED"
echo "❌ Failed tasks: $FAILED"
echo "🚫 Cancelled tasks: $CANCELLED"
echo "⏰ Timeout tasks: $TIMEOUT"
echo "📊 Total tasks: $TOTAL_TASKS"
if [ $TOTAL_TASKS -gt 0 ]; then
    echo "📈 Success rate: $(echo "scale=1; $COMPLETED * 100 / $TOTAL_TASKS" | bc)%"
fi

echo ""

# Runtime statistics
echo "⏱️  RUNTIME STATISTICS"
echo "----------------------------------------"
sacct -j $MAIN_JOB --format=Elapsed --parsable2 --noheader | \
awk '{
    split($1, time, ":")
    if(length(time)==3) seconds = time[1]*3600 + time[2]*60 + time[3]
    else if(length(time)==2) seconds = time[1]*60 + time[2]
    else seconds = time[1]
    
    total += seconds
    count++
    if(seconds > max) max = seconds
    if(min == 0 || seconds < min) min = seconds
}
END {
    if(count > 0) {
        avg = total/count
        printf "⏰ Average runtime: %02d:%02d:%02d\n", avg/3600, (avg%3600)/60, avg%60
        printf "🚀 Fastest task: %02d:%02d:%02d\n", min/3600, (min%3600)/60, min%60
        printf "🐌 Slowest task: %02d:%02d:%02d\n", max/3600, (max%3600)/60, max%60
        printf "📊 Total compute time: %02d:%02d:%02d\n", total/3600, (total%3600)/60, total%60
    }
}'

echo ""

# Node usage summary
echo "🖥️  NODE USAGE SUMMARY"
echo "----------------------------------------"
sacct -j $MAIN_JOB --format=NodeList --parsable2 --noheader | grep -v "^$" | \
sort | uniq -c | sort -nr | head -10 | \
awk '{printf "%-6s tasks on %s\n", $1, $2}'

echo ""

# Output file analysis
echo "📁 OUTPUT FILE ANALYSIS"
echo "----------------------------------------"
if [ -d "$WORK_DIR/output" ]; then
    OUTPUT_FILES=$(find "$WORK_DIR/output" -name "*.parquet" 2>/dev/null | wc -l)
    OUTPUT_SIZE=$(du -sh "$WORK_DIR/output" 2>/dev/null | cut -f1)
    echo "📂 Output files: $OUTPUT_FILES parquet files"
    echo "💾 Output size: $OUTPUT_SIZE"
else
    echo "❌ Output directory not found at $WORK_DIR/output"
fi

# Check for additional output directories
for dir in 202104-output 202110-output; do
    if [ -d "$WORK_DIR/$dir" ]; then
        DIR_FILES=$(find "$WORK_DIR/$dir" -name "*.parquet" 2>/dev/null | wc -l)
        DIR_SIZE=$(du -sh "$WORK_DIR/$dir" 2>/dev/null | cut -f1)
        echo "📂 $dir: $DIR_FILES files ($DIR_SIZE)"
    fi
done

echo ""

# Log file summary
echo "📄 LOG FILE SUMMARY"
echo "----------------------------------------"
OUT_FILES=$(ls "$WORK_DIR"/*.out 2>/dev/null | wc -l)
ERR_FILES=$(ls "$WORK_DIR"/*.err 2>/dev/null | wc -l)
LOG_FILES=$(ls "$WORK_DIR"/*.log 2>/dev/null | wc -l)
echo "📝 SLURM output files: $OUT_FILES"
echo "⚠️  SLURM error files: $ERR_FILES"
echo "📋 Python processor logs: $LOG_FILES"

echo ""
echo "=========================================="
echo "💡 Additional commands:"
echo "   📊 Detailed accounting: sacct -j $MAIN_JOB --long"
echo "   📝 Job details: scontrol show job $MAIN_JOB"
echo "   📂 Check output: ls -la $WORK_DIR/output/ $WORK_DIR/202104-output/"
echo "   🗂️  Check logs: ls -la $WORK_DIR/*.{out,err,log}"
echo "   🔍 Analyze logs: ./log-analysis.sh"
echo "   🧹 Clean logs: rm $WORK_DIR/*.{out,err,log}"
echo "=========================================="
