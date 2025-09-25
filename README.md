# CC-Filtering-rework

A tool for processing and filtering Common Crawl data using Slurm-based parallel processing.

Derived from original work here: [CC-Filtering](https://github.com/0emerald/CC-Filtering)

## Overview

This repository contains scripts for efficiently downloading and processing Common Crawl data using a Slurm computing cluster. The workflow uses SLURM array jobs for parallel processing within each crawl date, submitted sequentially across dates via a Python runner. This allows scalable handling of large datasets while controlling concurrency to avoid overwhelming resources.

## Scripts

### Core Scripts
- **setup-conda-env.sh** - Sets up the conda environment (`.conda_env`) for processing with required Python packages.
- **slurm-sequential-runner.py** - Python script that reads crawl dates from `crawl_data.txt` and sequentially submits/manages a SLURM array job for each date (waiting for completion before the next).
- **job-template.sh** - Template for individual SLURM array jobs; configures the environment and runs the processor.
- **common-crawl-processor.py** - Core Python script that downloads and processes Common Crawl WET files for a given date and task ID.

### Run Scripts (Launchers/Optimization)
These launcher scripts configure and submit the sequential runner for different parallelism levels. Run with `sbatch <script.sh>`.

- **run-fast-parallel.sh** - High-parallelism mode: 50 concurrent array tasks, 25 files per task, 7-day timeout.
- **run-ultra-parallel.sh** - Maximum throughput: 100 concurrent array tasks, 10 files per task, 7-day timeout.
- **run-disk-safe-parallel.sh** - Conservative mode: 20 concurrent tasks, 50 files per task, focuses on I/O safety.
- **run-optimized-job.sh** - Balanced default: 30 concurrent tasks, 25 files per task.
- **run-test-config.sh** - Dry-run mode: Generates scripts without submitting jobs for validation.

### Monitoring & Analysis Scripts
- **monitor-job.sh** - Real-time monitoring of running jobs: progress, resource usage, estimated completion time.
- **job-analyzer.sh** - Post-completion analysis: success rates, runtimes, failures, and performance metrics from SLURM logs.

### Data & Utility Files
- **BristolPostcodeLookup.parquet** - Lookup table for Bristol postcodes used in data filtering.
- **crawl_data.txt** - List of crawl dates and file counts (format: `date num_files`, e.g., `202104 79840`).
- **wet.paths** - Paths to Common Crawl WET files.
- **scripts.txt** - Quick reference to script purposes.
- **slurm-config-guide.txt** - SLURM configuration tips and best practices.
- **script-comparison.md** - Comparison of run script configurations.

## Usage

### Quick Start
1. **Setup Environment**:
   ```bash
   bash setup-conda-env.sh  # Creates .conda_env
   source runme.sh          # Sets SLURM account (create if needed: export SLURM_ACCOUNT=your_account)
   ```

2. **Run Pipeline** (submit via SLURM):
   ```bash
   # Recommended: Fast parallel mode
   sbatch run-fast-parallel.sh

   # Or ultra-parallel for max speed (higher resource use)
   sbatch run-ultra-parallel.sh

   # Disk-safe mode (lower concurrency)
   sbatch run-disk-safe-parallel.sh

   # Dry-run test (no submission)
   sbatch run-test-config.sh
   ```

   Each launcher runs `slurm-sequential-runner.py` with tailored args, e.g.:
   ```bash
   ./.conda_env/bin/python slurm-sequential-runner.py \
     --template-file job-template.sh \
     --crawl-dates-file crawl_data.txt \
     --partition compute \
     --time 168 \                # 7 days
     --mem 2G \
     --cpus 2 \
     --segments-per-task 25 \    # Files per array task
     --throttle 50 \             # Max concurrent tasks
     --job-prefix crawl_job_fast
   ```

### Monitoring & Management
While jobs run (parent launcher + child arrays):
```bash
# Monitor running jobs (parent and children)
./monitor-job.sh

# SLURM commands for details
squeue -u $USER  # All jobs
squeue -u $USER | grep crawl_job  # Child jobs
sinfo -p compute  # Partition status
```

### Post-Completion Analysis
After the sequential runner finishes all dates:
```bash
# Analyze completed jobs (success, runtime, failures)
./job-analyzer.sh

# Check logs/output
sacct -j <JOB_ID> --format=JobID,State,ExitCode,MaxRSS,Elapsed  # Parent job
ls *_%j.out *_%j.err  # Child job logs
```

**Analysis Features** (via `job-analyzer.sh`):
- 📊 Success/failure rates and exit codes.
- ⏱️ Runtime stats (min/max/avg per task/date).
- 💾 Resource utilization (memory, CPU).
- 📈 Performance insights and tuning recommendations.

## Workflow Explanation
1. Launcher script (e.g., `run-fast-parallel.sh`) runs `slurm-sequential-runner.py`.
2. The Python runner loads `crawl_data.txt` and, for each date:
   - Calculates array size: `n_files / segments_per_task`.
   - Submits a SLURM array job (e.g., `0-3199%50` for ~80k files, 25 per task, 50 concurrent).
   - Uses `job-template.sh` to generate the job script, which activates `.conda_env` and calls `common-crawl-processor.py --task-id $SLURM_ARRAY_TASK_ID`.
   - Waits (polls `squeue`) for the array to complete before next date.
3. Each array task processes its file segments: downloads WET files (from `wet.paths`), filters (using `BristolPostcodeLookup.parquet`), outputs Parquet/CSV.

Processed files save to `./output/<date>/` (configurable in `common-crawl-processor.py`).

## Requirements
- SLURM cluster access.
- Bash, Python 3.x (via `.conda_env`: pandas, pyarrow, requests, etc.—installed by `setup-conda-env.sh`).
- Git LFS for large files (setup below).

Note: Uses stable conda env in `.conda_env` (no Micromamba due to HPC compatibility issues).

## Configuration
Edit these for customization:
- **crawl_data.txt**: Add/remove dates and file counts.
- **job-template.sh**: Tweak SLURM params or env setup.
- **common-crawl-processor.py**: Adjust filtering logic or output paths.
- **run-*.sh**: Modify Python args for your needs (e.g., `--throttle 30` for medium clusters).

### SLURM Tuning
- **Time**: `--time 168` (7 days) for large dates; check partition max with `sinfo`.
- **Parallelism**: Lower `--segments-per-task` = more tasks (higher parallelism); use `--throttle` to limit concurrency.
- **Account**: Set in `runme.sh` and source before `sbatch`.

## Performance Optimization Features
- **Configurable Parallelism**: `--throttle` controls concurrent array tasks (default 50; up to 100+ on large clusters).
- **Sequential Safety**: Processes dates one-by-one to avoid overload, but parallel within dates via arrays.
- **Extended Timeouts**: 7-day limits prevent failures on big crawls.
- **Resource Balance**: 2G mem, 2 CPUs per task; low overhead for launcher.
- **Dry-Run**: Test with `run-test-config.sh`—generates scripts in `./generated_scripts/`.
- **Improvements**: 5-10x faster than original sequential (via arrays + throttling); optimized for compute partitions.

**Performance Comparison**:
- **Sequential (old)**: 10 tasks max, 24h timeout → frequent failures.
- **Array (now)**: 50-100 concurrent, 7-day timeout → full dataset completion.

## Best Practices
1. **Account Setup**: Use `runme.sh` for portability.
2. **Throttle Tuning**: 10-20 for small clusters; 50+ for large.
3. **Timeouts**: 24h for tests; 168h for production.
4. **Balance Workload**: `--segments-per-task 10-50` (trade parallelism vs. overhead).
5. **Test First**: Always dry-run.
6. **Parquet Output**: Half the size of CSV; better for large data.
7. **SLURM Arrays**: Use `%throttle` (e.g., `--array=0-899%50`) for control.
8. **Monitor Resources**: `sacct` for usage; adjust mem/CPUs if needed.
9. **Git Hygiene**: `.gitignore` excludes outputs/logs; commit only templates/scripts.

## Understanding the Transition to SLURM Array Jobs
(See original README for detailed equivalence to old manual chunking—unchanged, but now fully integrated via `slurm-sequential-runner.py`.)

## Troubleshooting
### Common Issues
**Parent Job Times Out**: Launcher (e.g., `run-fast-parallel.sh`) needs `#SBATCH --time=168:00:00` for full sequential wait. Add to scripts if missing.

**Child Arrays Stop Early**: Due to parent kill; ensure parent time > total estimated runtime (~1-2 days per date × num_dates).

**PENDING "PartitionConfig"**: Source `runme.sh` for account.

**Low Concurrency**: Increase `--throttle`; check `sinfo` for resources.

**Exit Code 120**: App errors (e.g., download fails)—use `./job-analyzer.sh`.

**Array Not Starting**: Normal queuing; SLURM throttles via `%`.

### Monitoring Commands
```bash
squeue -u $USER  # Running jobs
sacct -j <ID> --format=JobID,State,Elapsed,MaxRSS  # Completed
./monitor-job.sh  # Custom progress
./job-analyzer.sh  # Analysis
scontrol show job <ID>  # Details
```

### Job Completion Insights (from `job-analyzer.sh`)
- ✅ Success rate (e.g., 95% tasks complete).
- ⏱️ Avg runtime per date.
- 🚨 Failures by code/pattern.
- 🎯 Recommendations (e.g., "Increase throttle to 75").

## Git LFS Setup and Data Download
Uses Git LFS for large files (e.g., `.parquet`).

### Install/Use
```bash
git lfs install  # If needed
git lfs pull     # Download all
git lfs pull --include="*.parquet"  # Specific
```

### Check
```bash
git lfs ls-files
git lfs track
```

Modify/commit as usual: `git add <file> && git commit && git push`. `.gitattributes` auto-tracks large files.