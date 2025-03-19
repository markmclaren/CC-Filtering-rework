# Set the crawl date - adjust as needed
CRAWL_DATE=$date

# Number of segments to process per task
SEGMENTS_PER_TASK=$segments_per_task

echo "Processing crawl date: $date"
echo "Total files in this crawl: $n_files"

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
micromamba create -n myenv python=3.9 pandas=2.2.0 pyarrow=19.0.1 warcio=1.7.5 requests -y --quiet

# Activate the environment
micromamba activate myenv

# Print task information for logging
echo "Running array task ${SLURM_ARRAY_TASK_ID} (Job ID: ${SLURM_JOB_ID})"

# Run the python script with explicitly passed task ID
python common-crawl-processor.py \
  --crawl-date ${CRAWL_DATE} \
  --task-id ${SLURM_ARRAY_TASK_ID} \
  --job-id "${SLURM_JOB_ID}_${SLURM_ARRAY_TASK_ID}" \
  --segments-per-task ${SEGMENTS_PER_TASK} \
  --wet-paths wet.paths \
  --output-dir output \
  --postcode-lookup BristolPostcodeLookup.parquet
