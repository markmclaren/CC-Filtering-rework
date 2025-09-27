#!/usr/bin/env python3
"""
Advanced Progress Dashboard for Common Crawl Processing
Provides a web-based interface with real-time progress visualization
"""

import argparse
import asyncio
import json
import os
import threading
import time
import webbrowser
from datetime import datetime
from http.server import HTTPServer, SimpleHTTPRequestHandler
from urllib.parse import urlparse, parse_qs

import matplotlib.pyplot as plt
import pandas as pd
from flask import Flask, jsonify, render_template_string, request


class ProgressTracker:
    """Tracks progress data and calculates metrics"""

    def __init__(self, working_dir="."):
        self.working_dir = working_dir
        self.output_dir = os.path.join(working_dir, "output")
        self.crawl_data_file = os.path.join(working_dir, "crawl_data.txt")
        self.progress_log = os.path.join(working_dir, "progress.log")
        self.history_log = os.path.join(working_dir, "progress_history.log")

        # Progress history for trend analysis
        self.progress_history = []
        self.start_time = time.time()

    def get_total_segments(self):
        """Get total number of segments from crawl_data.txt"""
        total = 0
        if os.path.exists(self.crawl_data_file):
            with open(self.crawl_data_file, 'r') as f:
                for line in f:
                    line = line.strip()
                    if line and not line.startswith('#'):
                        parts = line.split()
                        if len(parts) >= 2 and parts[1].isdigit():
                            total += int(parts[1])
        return total

    def get_processed_segments(self):
        """Count processed segments (non-empty parquet files)"""
        count = 0
        if os.path.exists(self.output_dir):
            for filename in os.listdir(self.output_dir):
                if filename.startswith('crawldata') and filename.endswith('.parquet'):
                    filepath = os.path.join(self.output_dir, filename)
                    if os.path.exists(filepath) and os.path.getsize(filepath) > 0:
                        count += 1
        return count

    def get_current_crawl_date(self):
        """Determine which crawl date is currently being processed"""
        if not os.path.exists(self.crawl_data_file):
            return None

        with open(self.crawl_data_file, 'r') as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith('#'):
                    parts = line.split()
                    if len(parts) >= 2:
                        date, expected_count = parts[0], int(parts[1])

                        # Count processed files for this date
                        processed_count = 0
                        if os.path.exists(self.output_dir):
                            for filename in os.listdir(self.output_dir):
                                if filename.startswith(f'crawldata{date}') and filename.endswith('.parquet'):
                                    filepath = os.path.join(self.output_dir, filename)
                                    if os.path.exists(filepath) and os.path.getsize(filepath) > 0:
                                        processed_count += 1

                        # If not all files are processed, this is likely the current date
                        if processed_count < expected_count:
                            return date
        return None

    def get_job_status(self):
        """Get SLURM job status"""
        try:
            # Try to get the most recent job
            result = os.popen("squeue -u $USER -o '%i %T' -h | head -1").read().strip()
            if result:
                job_id, status = result.split()
                return status
        except:
            pass
        return "Unknown"

    def get_progress_data(self):
        """Get comprehensive progress data"""
        total_segments = self.get_total_segments()
        processed_segments = self.get_processed_segments()
        current_date = self.get_current_crawl_date()
        job_status = self.get_job_status()

        percentage = 0
        if total_segments > 0:
            percentage = (processed_segments / total_segments) * 100

        elapsed_time = time.time() - self.start_time

        # Calculate ETA
        eta = "Unknown"
        if processed_segments > 0 and elapsed_time > 0:
            rate = processed_segments / elapsed_time
            remaining = total_segments - processed_segments
            if rate > 0:
                eta_seconds = remaining / rate
                eta = self.format_duration(eta_seconds)

        # Get current date progress
        current_date_total = 0
        current_date_processed = 0
        if current_date:
            # Find expected count for current date
            with open(self.crawl_data_file, 'r') as f:
                for line in f:
                    line = line.strip()
                    if line and not line.startswith('#'):
                        parts = line.split()
                        if len(parts) >= 2 and parts[0] == current_date:
                            current_date_total = int(parts[1])
                            break

            # Count processed files for current date
            if os.path.exists(self.output_dir):
                for filename in os.listdir(self.output_dir):
                    if filename.startswith(f'crawldata{current_date}') and filename.endswith('.parquet'):
                        filepath = os.path.join(self.output_dir, filename)
                        if os.path.exists(filepath) and os.path.getsize(filepath) > 0:
                            current_date_processed += 1

        return {
            'timestamp': datetime.now().isoformat(),
            'total_segments': total_segments,
            'processed_segments': processed_segments,
            'percentage': round(percentage, 2),
            'current_crawl_date': current_date,
            'current_date_total': current_date_total,
            'current_date_processed': current_date_processed,
            'job_status': job_status,
            'elapsed_time': elapsed_time,
            'eta': eta,
            'remaining_segments': total_segments - processed_segments
        }

    def format_duration(self, seconds):
        """Format duration in human-readable format"""
        days = int(seconds // 86400)
        hours = int((seconds % 86400) // 3600)
        minutes = int((seconds % 3600) // 60)

        if days > 0:
            return f"{days}d {hours}h {minutes}m"
        elif hours > 0:
            return f"{hours}h {minutes}m"
        else:
            return f"{minutes}m"

    def get_progress_history(self):
        """Get progress history for trend analysis"""
        if not os.path.exists(self.history_log):
            return []

        history = []
        try:
            with open(self.history_log, 'r') as f:
                for line in f:
                    line = line.strip()
                    if line and 'PROCESSED=' in line:
                        # Parse line like: [2024-01-01 12:00:00] PROCESSED=100/1000 (10%) CURRENT_DATE=202350
                        parts = line.split(' PROCESSED=')
                        if len(parts) == 2:
                            timestamp_str = parts[0].strip('[]')
                            processed_info = parts[1]

                            try:
                                timestamp = datetime.strptime(timestamp_str, '%Y-%m-%d %H:%M:%S')
                                # Extract numbers
                                numbers = re.findall(r'\d+', processed_info)
                                if len(numbers) >= 2:
                                    processed = int(numbers[0])
                                    total = int(numbers[1])
                                    percentage = (processed / total * 100) if total > 0 else 0

                                    history.append({
                                        'timestamp': timestamp,
                                        'processed': processed,
                                        'total': total,
                                        'percentage': percentage
                                    })
                            except ValueError:
                                continue
        except Exception as e:
            print(f"Error reading history: {e}")

        return history

    def generate_progress_chart(self):
        """Generate a progress chart and return the file path"""
        history = self.get_progress_history()

        if not history:
            return None

        # Create plot
        timestamps = [h['timestamp'] for h in history]
        percentages = [h['percentage'] for h in history]

        plt.figure(figsize=(10, 6))
        plt.plot(timestamps, percentages, 'b-', linewidth=2, markersize=4, marker='o')
        plt.title('Processing Progress Over Time')
        plt.xlabel('Time')
        plt.ylabel('Completion Percentage (%)')
        plt.grid(True, alpha=0.3)
        plt.ylim(0, 100)
        plt.xticks(rotation=45)
        plt.tight_layout()

        # Save plot
        chart_dir = os.path.join(self.working_dir, 'charts')
        os.makedirs(chart_dir, exist_ok=True)
        chart_path = os.path.join(chart_dir, 'progress_chart.png')
        plt.savefig(chart_path, dpi=100, bbox_inches='tight')
        plt.close()

        return chart_path


class ProgressDashboard:
    """Web dashboard for progress monitoring"""

    def __init__(self, tracker, port=8080, debug=False):
        self.tracker = tracker
        self.port = port
        self.debug = debug

        self.app = Flask(__name__)
        self.setup_routes()

    def setup_routes(self):
        """Set up Flask routes"""

        @self.app.route('/')
        def dashboard():
            """Main dashboard page"""
            return render_template_string(DASHBOARD_HTML)

        @self.app.route('/api/progress')
        def api_progress():
            """API endpoint for progress data"""
            try:
                data = self.tracker.get_progress_data()
                return jsonify(data)
            except Exception as e:
                return jsonify({'error': str(e)}), 500

        @self.app.route('/api/history')
        def api_history():
            """API endpoint for progress history"""
            try:
                history = self.tracker.get_progress_history()
                return jsonify(history)
            except Exception as e:
                return jsonify({'error': str(e)}), 500

        @self.app.route('/api/chart')
        def api_chart():
            """Generate and return progress chart"""
            try:
                chart_path = self.tracker.generate_progress_chart()
                if chart_path and os.path.exists(chart_path):
                    return jsonify({'chart_url': f'/charts/progress_chart.png'})
                else:
                    return jsonify({'error': 'No chart data available'}), 404
            except Exception as e:
                return jsonify({'error': str(e)}), 500

        @self.app.route('/charts/<filename>')
        def serve_chart(filename):
            """Serve chart images"""
            from flask import send_file
            chart_dir = os.path.join(self.tracker.working_dir, 'charts')
            return send_file(os.path.join(chart_dir, filename))

    def run(self, open_browser=True):
        """Run the dashboard server"""
        print(f"Starting progress dashboard at http://localhost:{self.port}")

        if open_browser:
            # Open browser after a short delay
            def open_browser_delayed():
                time.sleep(1)
                webbrowser.open(f"http://localhost:{self.port}")
            threading.Thread(target=open_browser_delayed, daemon=True).start()

        self.app.run(host='0.0.0.0', port=self.port, debug=self.debug)


# HTML template for the dashboard
DASHBOARD_HTML = """
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Common Crawl Progress Dashboard</title>
    <meta http-equiv="refresh" content="30">
    <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
    <style>
        body {
            font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
            margin: 0;
            padding: 20px;
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            color: #333;
        }
        .container {
            max-width: 1200px;
            margin: 0 auto;
            background: white;
            border-radius: 15px;
            box-shadow: 0 10px 30px rgba(0,0,0,0.2);
            overflow: hidden;
        }
        .header {
            background: linear-gradient(135deg, #4CAF50, #45a049);
            color: white;
            padding: 30px;
            text-align: center;
        }
        .header h1 {
            margin: 0;
            font-size: 2.5em;
            font-weight: 300;
        }
        .header p {
            margin: 10px 0 0 0;
            opacity: 0.9;
            font-size: 1.1em;
        }
        .content {
            padding: 30px;
        }
        .metrics-grid {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(250px, 1fr));
            gap: 20px;
            margin-bottom: 30px;
        }
        .metric-card {
            background: #f8f9fa;
            border-radius: 10px;
            padding: 25px;
            text-align: center;
            border-left: 5px solid #4CAF50;
            transition: transform 0.2s;
        }
        .metric-card:hover {
            transform: translateY(-5px);
        }
        .metric-value {
            font-size: 2.5em;
            font-weight: bold;
            color: #4CAF50;
            margin-bottom: 10px;
        }
        .metric-label {
            color: #666;
            font-size: 0.9em;
            text-transform: uppercase;
            letter-spacing: 1px;
        }
        .progress-section {
            background: #f8f9fa;
            border-radius: 10px;
            padding: 25px;
            margin-bottom: 30px;
        }
        .progress-bar {
            width: 100%;
            height: 30px;
            background: #e0e0e0;
            border-radius: 15px;
            overflow: hidden;
            margin: 20px 0;
        }
        .progress-fill {
            height: 100%;
            background: linear-gradient(90deg, #4CAF50, #2196F3);
            width: 0%;
            transition: width 0.8s ease-in-out;
            border-radius: 15px;
        }
        .chart-container {
            position: relative;
            height: 400px;
            margin-top: 30px;
        }
        .status-indicator {
            display: inline-block;
            width: 12px;
            height: 12px;
            border-radius: 50%;
            margin-right: 8px;
        }
        .status-running { background-color: #4CAF50; }
        .status-pending { background-color: #FF9800; }
        .status-completed { background-color: #2196F3; }
        .status-unknown { background-color: #9E9E9E; }
        .last-updated {
            text-align: center;
            color: #666;
            margin-top: 20px;
            font-style: italic;
        }
        .current-date-section {
            background: linear-gradient(135deg, #2196F3, #21CBF3);
            color: white;
            border-radius: 10px;
            padding: 20px;
            margin-bottom: 20px;
        }
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>🚀 Common Crawl Progress Dashboard</h1>
            <p>Real-time monitoring of your data processing pipeline</p>
        </div>

        <div class="content">
            <div id="loading">Loading progress data...</div>
            <div id="dashboard" style="display: none;">
                <!-- Current Date Section -->
                <div class="current-date-section" id="current-date-section" style="display: none;">
                    <h3>🔄 Currently Processing: <span id="current-date-value"></span></h3>
                    <div class="progress-bar">
                        <div class="progress-fill" id="current-date-progress"></div>
                    </div>
                    <p><span id="current-date-processed">0</span> / <span id="current-date-total">0</span> segments completed</p>
                </div>

                <!-- Overall Metrics -->
                <div class="metrics-grid">
                    <div class="metric-card">
                        <div class="metric-value" id="overall-percentage">0%</div>
                        <div class="metric-label">Overall Progress</div>
                    </div>
                    <div class="metric-card">
                        <div class="metric-value" id="processed-count">0</div>
                        <div class="metric-label">Segments Processed</div>
                    </div>
                    <div class="metric-card">
                        <div class="metric-value" id="remaining-count">0</div>
                        <div class="metric-label">Segments Remaining</div>
                    </div>
                    <div class="metric-card">
                        <div class="metric-value" id="eta-value">Unknown</div>
                        <div class="metric-label">Estimated Time</div>
                    </div>
                </div>

                <!-- Overall Progress Bar -->
                <div class="progress-section">
                    <h3>📊 Overall Progress</h3>
                    <div class="progress-bar">
                        <div class="progress-fill" id="overall-progress"></div>
                    </div>
                    <p><span id="processed-text">0</span> / <span id="total-text">0</span> total segments processed</p>
                </div>

                <!-- Job Status -->
                <div class="progress-section">
                    <h3>⚙️ Job Status</h3>
                    <p>
                        <span class="status-indicator" id="status-indicator"></span>
                        Status: <strong id="job-status">Unknown</strong>
                    </p>
                    <p>Elapsed Time: <strong id="elapsed-time">0s</strong></p>
                </div>

                <!-- Progress Chart -->
                <div class="progress-section">
                    <h3>📈 Progress Trend</h3>
                    <div class="chart-container">
                        <canvas id="progressChart"></canvas>
                    </div>
                </div>

                <div class="last-updated">
                    Last updated: <span id="last-updated">Never</span>
                </div>
            </div>
        </div>
    </div>

    <script>
        let progressChart = null;

        async function updateDashboard() {
            try {
                const response = await fetch('/api/progress');
                const data = await response.json();

                if (data.error) {
                    document.getElementById('loading').innerText = 'Error: ' + data.error;
                    return;
                }

                document.getElementById('loading').style.display = 'none';
                document.getElementById('dashboard').style.display = 'block';

                // Update metrics
                document.getElementById('overall-percentage').innerText = data.percentage + '%';
                document.getElementById('processed-count').innerText = data.processed_segments.toLocaleString();
                document.getElementById('remaining-count').innerText = data.remaining_segments.toLocaleString();
                document.getElementById('eta-value').innerText = data.eta;
                document.getElementById('processed-text').innerText = data.processed_segments.toLocaleString();
                document.getElementById('total-text').innerText = data.total_segments.toLocaleString();
                document.getElementById('last-updated').innerText = new Date(data.timestamp).toLocaleString();

                // Update progress bars
                document.getElementById('overall-progress').style.width = data.percentage + '%';
                document.getElementById('overall-progress').style.background = `linear-gradient(90deg, #4CAF50, #2196F3)`;

                // Update current date section
                if (data.current_crawl_date) {
                    document.getElementById('current-date-section').style.display = 'block';
                    document.getElementById('current-date-value').innerText = data.current_crawl_date;
                    const currentPercentage = data.current_date_total > 0 ?
                        (data.current_date_processed / data.current_date_total * 100) : 0;
                    document.getElementById('current-date-progress').style.width = currentPercentage + '%';
                    document.getElementById('current-date-processed').innerText = data.current_date_processed;
                    document.getElementById('current-date-total').innerText = data.current_date_total;
                } else {
                    document.getElementById('current-date-section').style.display = 'none';
                }

                // Update job status
                const statusIndicator = document.getElementById('status-indicator');
                const jobStatus = document.getElementById('job-status');
                const elapsedTime = document.getElementById('elapsed-time');

                jobStatus.innerText = data.job_status;
                elapsedTime.innerText = formatDuration(data.elapsed_time);

                // Update status indicator color
                statusIndicator.className = 'status-indicator status-' +
                    data.job_status.toLowerCase();

                // Update chart
                await updateChart();

            } catch (error) {
                document.getElementById('loading').innerText = 'Error loading data: ' + error.message;
            }
        }

        async function updateChart() {
            try {
                const response = await fetch('/api/history');
                const history = await response.json();

                if (history.error || history.length === 0) {
                    return;
                }

                const ctx = document.getElementById('progressChart').getContext('2d');

                if (progressChart) {
                    progressChart.destroy();
                }

                progressChart = new Chart(ctx, {
                    type: 'line',
                    data: {
                        labels: history.map(h => new Date(h.timestamp).toLocaleTimeString()),
                        datasets: [{
                            label: 'Progress (%)',
                            data: history.map(h => h.percentage),
                            borderColor: '#4CAF50',
                            backgroundColor: 'rgba(76, 175, 80, 0.1)',
                            borderWidth: 3,
                            fill: true,
                            tension: 0.4
                        }]
                    },
                    options: {
                        responsive: true,
                        maintainAspectRatio: false,
                        scales: {
                            y: {
                                beginAtZero: true,
                                max: 100
                            }
                        },
                        plugins: {
                            legend: {
                                display: false
                            }
                        }
                    }
                });
            } catch (error) {
                console.error('Error updating chart:', error);
            }
        }

        function formatDuration(seconds) {
            const days = Math.floor(seconds / 86400);
            const hours = Math.floor((seconds % 86400) / 3600);
            const minutes = Math.floor((seconds % 3600) / 60);

            if (days > 0) {
                return `${days}d ${hours}h ${minutes}m`;
            } else if (hours > 0) {
                return `${hours}h ${minutes}m`;
            } else {
                return `${minutes}m`;
            }
        }

        // Update every 30 seconds
        updateDashboard();
        setInterval(updateDashboard, 30000);
    </script>
</body>
</html>
"""


def main():
    parser = argparse.ArgumentParser(description="Progress Dashboard for Common Crawl Processing")
    parser.add_argument('--port', '-p', type=int, default=8080,
                       help='Port for the web dashboard (default: 8080)')
    parser.add_argument('--working-dir', '-w', type=str, default='.',
                       help='Working directory (default: current directory)')
    parser.add_argument('--no-browser', action='store_true',
                       help='Do not open browser automatically')
    parser.add_argument('--debug', action='store_true',
                       help='Run in debug mode')

    args = parser.parse_args()

    # Create progress tracker
    tracker = ProgressTracker(args.working_dir)

    # Create and run dashboard
    dashboard = ProgressDashboard(tracker, args.port, args.debug)
    dashboard.run(open_browser=not args.no_browser)


if __name__ == '__main__':
    main()
