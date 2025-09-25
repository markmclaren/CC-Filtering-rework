#!/bin/bash
#SBATCH --partition=compute       # Match your partition
#SBATCH --time=168:00:00          # 7 days for parent (increase if needed, e.g., 200:00:00)
#SBATCH --mem=2G                  # Low for launcher (it's mostly waiting)
#SBATCH --cpus-per-task=1         # Minimal for launcher

# High-parallelism configuration for faster processing
# Runs 50 concurrent array tasks for quicker completion

source runme.sh  # Set SLURM account

./.conda_env/bin/python slurm-sequential-runner.py \
  --template-file job-template.sh \
  --crawl-dates-file crawl_data.txt \
  --partition compute \
  --time 168 \
  --mem 2G \
  --cpus 2 \
  --segments-per-task 25 \
  --throttle 50 \
  --job-prefix crawl_job_fast
  # 50 concurrent tasks = 5x more than the old default of 10