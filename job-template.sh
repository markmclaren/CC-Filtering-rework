# Set the crawl date - adjust as needed
CRAWL_DATE=$date

# Number of segments to process per task
SEGMENTS_PER_TASK=$segments_per_task

echo "Processing crawl date: $date"
echo "Total files in this crawl: $n_files"

# Print task information for logging
echo "Running array task ${SLURM_ARRAY_TASK_ID} (Job ID: ${SLURM_JOB_ID})"

# Run the python script with explicitly passed task ID using conda environment
./.conda_env/bin/python common-crawl-processor.py \
  --crawl-date ${CRAWL_DATE} \
  --task-id ${SLURM_ARRAY_TASK_ID} \
  --job-id "${SLURM_JOB_ID}_${SLURM_ARRAY_TASK_ID}" \
  --segments-per-task ${SEGMENTS_PER_TASK} \
  --wet-paths wet.paths \
  --output-dir output \
  --postcode-lookup BristolPostcodeLookup.parquet
