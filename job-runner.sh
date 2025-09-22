#!/bin/bash
#SBATCH --job-name=runner

# Environment created with: ./miniconda3/bin/conda create -p ./.conda_env python=3.9 -y

./.conda_env/bin/python slurm-sequential-runner.py \
  --template-file job-template.sh \
  --crawl-dates-file crawl_data.txt 
  #--dry-run