#!/bin/bash
#SBATCH --job-name=runner

PLATFORM=$(uname -m)

if [ "$PLATFORM" = "x86_64" ]; then
  URL="https://micro.mamba.pm/api/micromamba/linux-64/latest"
elif [ "$PLATFORM" = "aarch64" ]; then
  URL="https://micro.mamba.pm/api/micromamba/linux-aarch64/latest"
elif [ "$PLATFORM" = "arm64" ]; then
  URL="https://micro.mamba.pm/api/micromamba/osx-arm64/latest"
else
  echo "Unsupported platform: $PLATFORM"
  exit 1
fi

curl -Ls $URL | tar -xvj bin/micromamba

# Set up the micromamba environment
eval "$(bin/micromamba shell hook --shell bash)"

# Create a Micromamba environment
micromamba create -n myenv python=3.9 simple_slurm -y --quiet

# Activate the environment
micromamba activate myenv

python slurm-sequential-runner.py \
  --template-file job-template.sh \
  --crawl-dates-file crawl_data.txt
  # --dry-run