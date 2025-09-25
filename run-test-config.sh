#!/bin/bash
#SBATCH --partition=compute       # Match your partition
#SBATCH --time=168:00:00          # 7 days for parent (increase if needed, e.g., 200:00:00)
#SBATCH --mem=2G                  # Low for launcher (it's mostly waiting)
#SBATCH --cpus-per-task=1         # Minimal for launcher

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