# Common Crawl Progress Monitoring System

A comprehensive monitoring system for tracking SLURM job progress in Common Crawl data processing pipelines.

## Overview

This monitoring system provides real-time progress tracking for your Common Crawl processing jobs with:

- **Real-time progress updates** with percentage completion
- **ETA calculations** based on processing rates
- **Web-based dashboard** with visual progress indicators
- **Historical progress tracking** for trend analysis
- **Integration** with existing SLURM infrastructure

## Components

### 1. Progress Monitor Script (`progress-monitor.sh`)

A bash script that provides real-time console-based progress monitoring.

**Features:**
- Configurable monitoring intervals
- Progress bar visualization
- ETA calculations
- Job status tracking
- Progress logging
- Optional web dashboard

**Usage:**
```bash
# Basic monitoring with default settings (30-second intervals)
./progress-monitor.sh

# Monitor specific job with custom interval
./progress-monitor.sh --job-id 12345 --interval 60

# Start with web dashboard on port 8080
./progress-monitor.sh --dashboard 8080

# Quiet mode with logging
./progress-monitor.sh --quiet --log-file monitor.log --interval 120

# Show help
./progress-monitor.sh --help
```

**Options:**
- `-i, --interval SECONDS`: Monitoring interval (default: 30)
- `-j, --job-id JOB_ID`: Specific SLURM job ID to monitor
- `-l, --log-file FILE`: Log progress to specific file
- `-p, --progress-file FILE`: Progress state file (default: progress.log)
- `-q, --quiet`: Quiet mode (no console output)
- `-d, --dashboard PORT`: Start web dashboard on port (default: 8080)
- `-h, --help`: Show help message

### 2. Progress Dashboard (`progress-dashboard.py`)

A Python-based web dashboard with advanced visualization features.

**Features:**
- Modern web interface with real-time updates
- Interactive progress charts
- Comprehensive metrics display
- Historical trend analysis
- Mobile-responsive design

**Installation Requirements:**
```bash
pip install flask matplotlib pandas
```

**Usage:**
```bash
# Start dashboard on default port 8080
python3 progress-dashboard.py

# Custom port and working directory
python3 progress-dashboard.py --port 9000 --working-dir /path/to/project

# Debug mode without browser auto-open
python3 progress-dashboard.py --debug --no-browser

# Show help
python3 progress-dashboard.py --help
```

**Options:**
- `--port, -p PORT`: Port for web dashboard (default: 8080)
- `--working-dir, -w DIR`: Working directory (default: current)
- `--no-browser`: Don't open browser automatically
- `--debug`: Run in debug mode

## Quick Start

### Method 1: Using the Bash Monitor
```bash
# Start monitoring your current job
./progress-monitor.sh --dashboard 8080

# Open http://localhost:8080 in your browser
```

### Method 2: Using the Python Dashboard
```bash
# Start the web dashboard
python3 progress-dashboard.py

# Open http://localhost:8080 in your browser
```

### Method 3: Integration with Job Launch
```bash
# Start monitoring in background before launching job
./progress-monitor.sh --dashboard 8080 &

# Launch your job
./run-fast-parallel.sh

# Monitor will continue tracking until job completes
```

## Progress Calculation

The system calculates progress based on:

1. **Total Work Units**: Sum of all segments from `crawl_data.txt`
2. **Completed Work**: Count of non-empty `crawldata*.parquet` files in output directory
3. **Current Date Tracking**: Identifies which crawl date is currently being processed
4. **ETA Calculation**: Based on rolling average of completion rates

## Output Files

The monitoring system creates several output files:

- `progress.log`: Current progress state
- `progress_history.log`: Historical progress data for trend analysis
- `charts/progress_chart.png`: Generated progress visualization (dashboard only)
- Custom log files (if specified with `--log-file`)

## Integration with Existing System

The monitoring system integrates seamlessly with your existing:

- **SLURM Jobs**: Automatically detects and tracks job status
- **Output Structure**: Works with your existing `output/` directory structure
- **Configuration**: Uses existing `crawl_data.txt` and environment variables
- **Existing Monitor**: Complements your current `monitor-job.sh` script

## Example Output

### Console Output
```
=== Common Crawl Progress Monitor ===
Last updated: 2024-01-15 14:30:25

Overall Progress:
  Total segments: 640000
  Processed: 128000
  Remaining: 512000
  Progress: 20% [==========                    ]
  ETA: 2h 15m

Current Crawl Date: 202104
  Date segments: 16000/79840 (20%)

Job Status:
  Active job ID: 12345
  Status: RUNNING
  Elapsed time: 1350s

System Info:
  Monitoring interval: 30s
  Log file: progress.log
```

### Web Dashboard Features

The web dashboard provides:
- **Real-time metrics** with auto-refresh every 30 seconds
- **Visual progress bars** with smooth animations
- **Interactive charts** showing progress trends over time
- **Current date tracking** with separate progress indicators
- **Job status indicators** with color-coded status
- **Mobile-responsive design** for monitoring on any device

## Troubleshooting

### Common Issues

1. **No progress detected:**
   - Ensure `crawl_data.txt` exists and has correct format
   - Check that output files are being created in the expected location
   - Verify the working directory is correct

2. **Job status shows "Unknown":**
   - Ensure you're running on a SLURM cluster
   - Check that SLURM commands are available in your PATH
   - Verify your user has permission to query job status

3. **Dashboard not accessible:**
   - Check if the port is already in use
   - Ensure no firewall is blocking the connection
   - Try a different port with `--port` option

4. **ETA shows "Unknown":**
   - Need more progress data points for calculation
   - Wait for at least a few monitoring intervals
   - Check that segments are being processed consistently

### Debug Mode

For troubleshooting, run the dashboard in debug mode:
```bash
python3 progress-dashboard.py --debug --no-browser
```

This will show detailed logging information to help identify issues.

## Performance Considerations

- **Lightweight**: Minimal system resource usage
- **Non-intrusive**: Doesn't interfere with job execution
- **Configurable**: Adjustable monitoring frequency
- **Efficient**: Uses file system monitoring rather than active polling of jobs

## Best Practices

1. **Start monitoring before launching jobs** for complete progress tracking
2. **Use consistent working directories** for proper file detection
3. **Configure appropriate intervals** based on job duration (30-120 seconds typical)
4. **Keep progress logs** for historical analysis and optimization
5. **Monitor system resources** to ensure adequate disk space for output files

## Support

The monitoring system is designed to work with your existing Common Crawl processing pipeline. If you encounter issues:

1. Check the troubleshooting section above
2. Verify your `crawl_data.txt` format matches expected structure
3. Ensure output directory has proper write permissions
4. Check that required dependencies are installed for the dashboard

The system will automatically adapt to your specific configuration and provide accurate progress tracking for your SLURM-based Common Crawl processing jobs.
