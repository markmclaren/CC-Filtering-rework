#!/bin/bash

# Test configuration - generates scripts without submitting
# Use this to verify your SLURM configuration before real submission

./.conda_env/bin/python slurm-sequential-runner.py \
  --template-file job-template.sh \
  --crawl-dates-file crawl_data.txt \
  --partition compute \
  --time 336 \
  --mem 2G \
  --cpus 2 \
  --segments-per-task 50 \
  --job-prefix crawl_job_test \
  --dry-run

echo "Check generated_scripts/ directory for the SLURM scripts that would be submitted"