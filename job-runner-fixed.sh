#!/bin/bash
#SBATCH --job-name=runner

# FIXED VERSION: Addresses timeout and resource issues
# Original failed after 24h with 5-day workload

./.conda_env/bin/python slurm-sequential-runner.py \
  --template-file job-template.sh \
  --crawl-dates-file crawl_data.txt \
  --partition compute \
  --time 168 \
  --mem 2G \
  --cpus 2 \
  --segments-per-task 25 \
  --job-prefix crawl_job_fixed
  # This should complete in hours instead of days