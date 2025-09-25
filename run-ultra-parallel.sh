#!/bin/bash
#SBATCH --partition=compute       # Match your partition
#SBATCH --time=168:00:00          # 7 days for parent (increase if needed, e.g., 200:00:00)
#SBATCH --mem=2G                  # Low for launcher (it's mostly waiting)
#SBATCH --cpus-per-task=1         # Minimal for launcher

# Ultra-high parallelism - takes advantage of available resources
# Uses 100 concurrent array tasks for maximum throughput

source runme.sh  # Set SLURM account

./.conda_env/bin/python slurm-sequential-runner.py \
  --template-file job-template.sh \
  --crawl-dates-file crawl_data.txt \
  --partition compute \
  --time 168 \
  --mem 2G \
  --cpus 2 \
  --segments-per-task 10 \
  --throttle 100 \
  --job-prefix crawl_job_ultra
  # segments-per-task 10 + throttle 100 = up to 100 parallel tasks