#!/bin/bash

# Optimized job runner for long-running Common Crawl processing
# This configuration is optimized for 5+ day jobs without GPU

./.conda_env/bin/python slurm-sequential-runner.py \
  --template-file job-template.sh \
  --crawl-dates-file crawl_data.txt \
  --partition compute \
  --time 336 \
  --mem 2G \
  --cpus 2 \
  --segments-per-task 50 \
  --job-prefix crawl_job_long
  # Remove --dry-run when ready to submit real jobs