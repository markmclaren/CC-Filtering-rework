#!/bin/bash

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