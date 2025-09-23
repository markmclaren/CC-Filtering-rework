#!/bin/bash
#SBATCH --partition=compute
#SBATCH --time=168:00:00
#SBATCH --mem=2G
#SBATCH --cpus-per-task=2

# Set account
source runme.sh

# Use lower throttle to prevent disk space issues
# 25 concurrent tasks instead of 50
./.conda_env/bin/python slurm-sequential-runner.py \
  --template-file job-template.sh \
  --crawl-dates-file crawl_data.txt \
  --partition compute \
  --time 168 \
  --mem 2G \
  --cpus 2 \
  --segments-per-task 25 \
  --throttle 25 \
  --job-prefix crawl_job_safe
