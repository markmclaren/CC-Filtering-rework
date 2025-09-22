# CC-Filtering-rework

A tool for processing and filtering Common Crawl data using Slurm-based parallel processing.

Derived from original work here: [CC-Filtering](https://github.com/0emerald/CC-Filtering)

## Overview

This repository contains scripts for efficiently downloading and processing Common Crawl data using a Slurm computing cluster. The workflow automatically creates and submits batch jobs for each crawl date, allowing for parallel processing of multiple crawl segments.

## Scripts

### Core Scripts
- **setup-conda.sh** - Sets up the conda environment for processing
- **job-runner.sh** - Main entry point that configures the Micromamba environment and initiates the job creation process
- **job-runner-fixed.sh** - Improved version with optimized SLURM configuration and proper timeouts
- **slurm-sequential-runner.py** - Creates individual Slurm jobs for each crawl date based on the provided template
- **common-crawl-processor.py** - Downloads and processes Common Crawl data for a specific crawl date
- **job-template.sh** - Template script for Slurm array jobs that runs the processor with appropriate configurations

### Optimization Scripts
- **run-fast-parallel.sh** - High-performance configuration with 50 concurrent array tasks
- **run-ultra-parallel.sh** - Maximum throughput configuration with 100 concurrent array tasks  
- **test-config.sh** - Dry-run validation script to test configurations without submitting jobs

## Data Files

- **BristolPostcodeLookup.parquet** - Lookup table for Bristol postcodes used in data filtering
- **crawl_data.txt** - List of Common Crawl dates to process
- **wet.paths** - Path information for Common Crawl WET files

## Usage

### Quick Start

To run the entire pipeline with optimized performance:

```bash
# Set your SLURM account (required for compute partition access)
source runme.sh

# High-performance mode (50 concurrent tasks)
sbatch run-fast-parallel.sh

# Maximum throughput mode (100 concurrent tasks)  
sbatch run-ultra-parallel.sh

# Test configuration without submitting jobs
./test-config.sh
```

### Performance Optimization Features

**Recent improvements (September 2025):**
- ✅ **Configurable Throttle Limits**: Added `--throttle` parameter to control concurrent array tasks
- ✅ **Increased Default Parallelism**: Raised default from 10 to 50 concurrent tasks (5x improvement)
- ✅ **Ultra-High Throughput Mode**: Support for 100+ concurrent tasks on large clusters
- ✅ **Proper Account Management**: Environment variable support for SLURM account configuration
- ✅ **Extended Time Limits**: 7-day (168h) timeout prevents job failures on large datasets
- ✅ **Optimized Resource Allocation**: 2G memory, 2 CPUs per task for balanced performance
- ✅ **Dry-Run Validation**: Test configurations before submitting production jobs

**Performance Comparison:**
- **Before**: 10 concurrent tasks maximum, 24h timeout (caused failures)
- **After**: 50-100 concurrent tasks, 7-day timeout, 5-10x faster processing

### Workflow Explanation

1. **job-runner.sh** configures the Miniconda environment and calls `slurm-sequential-runner.py`
2. **slurm-sequential-runner.py** reads crawl dates from `crawl_data.txt` and creates a Slurm job for each date
3. Each job uses **job-template.sh** to configure the environment and run **common-crawl-processor.py**
4. **common-crawl-processor.py** downloads and processes the Common Crawl data for its assigned date

### Requirements

- Slurm cluster access
- Miniconda environment (configured in the scripts)
- Python 3.x with required packages (listed in environment setup)

**Note**: The project has migrated from Micromamba to Miniconda3 due to segmentation fault issues encountered with Micromamba in the HPC environment. All scripts now use the more stable Miniconda3 installation.

## Configuration

### SLURM Performance Tuning

The `slurm-sequential-runner.py` script now supports extensive configuration options:

```bash
python slurm-sequential-runner.py \
  --template-file job-template.sh \
  --crawl-dates-file crawl_data.txt \
  --partition compute \
  --time 168 \                      # Time limit in hours (168h = 7 days)
  --mem 2G \                        # Memory per task  
  --cpus 2 \                        # CPUs per task
  --segments-per-task 25 \          # Files per task (lower = more parallelism)
  --throttle 50 \                   # Max concurrent array tasks
  --job-prefix crawl_job \          # Job name prefix
  --dry-run                         # Test mode (don't submit jobs)
```

### Account Configuration

Create a `runme.sh` file to set your SLURM account:

```bash
export SLURM_ACCOUNT=your_account_name
export SBATCH_ACCOUNT=your_account_name
```

Then source it before submitting jobs:
```bash
source runme.sh
```

Edit the following files to customize your processing:

- **job-template.sh**: Modify resource allocations and environment setup
- **crawl_data.txt**: Add or remove Common Crawl dates to process
- **common-crawl-processor.py**: Adjust filtering parameters as needed

## Output

Processed data will be saved in the output directory specified in the scripts, with one subdirectory per crawl date.

# Understanding the Transition to SLURM Array Jobs

## The Original Approach
The original script used these parameters:
- `crawlDate="202350"` - Identifies the data crawl (2023 week 50)
- `n=90000` - Total number of .wet files to process
- `c=10` - Number of chunks to manually divide the work into
- `account="blah"` - SLURM account for resource charging

With this approach:
- The script manually divided 90,000 files into 10 chunks
- Each chunk would process 9,000 files
- Processing was likely sequential within each chunk
- The work couldn't be easily distributed across multiple nodes

## The New SLURM Array Approach
The new script is designed to work with SLURM's array job functionality:

### Key Parameters
- `--crawl-date` - Specifies which crawl to process (e.g., 202350)
- `--segments-per-task` - How many files each array task should process
- `--task-id` - Automatically provided by SLURM as `$SLURM_ARRAY_TASK_ID`

### How It Works
1. The script is submitted as a SLURM array job
2. SLURM creates multiple independent tasks and assigns each a unique ID
3. Each task calculates which segments to process based on its ID:
   ```
   start_segment = task_id * segments_per_task
   end_segment = start_segment + segments_per_task
   ```
4. Tasks run in parallel across the cluster

### Example SLURM Job Submission
```bash
#!/bin/bash
#SBATCH --account=math026082
#SBATCH --array=0-899%10       # 900 tasks, max 10 running at once
#SBATCH --job-name=crawl_process

python common-crawl-processor.py \
  --crawl-date 202350 \
  --task-id $SLURM_ARRAY_TASK_ID \
  --segments-per-task 100
```

### Parameter Equivalence
- For 90,000 files with 100 segments per task:
  - Need 900 tasks (90,000 ÷ 100)
  - SLURM array range: 0-899
- For 90,000 files with 10 segments per task:
  - Need 9,000 tasks (90,000 ÷ 10)
  - SLURM array range: 0-8999

The `%10` in `--array=0-899%10` means run at most 10 tasks simultaneously.

## Advantages of the SLURM Array Approach

1. **Automatic Distribution**: SLURM distributes tasks across available nodes
2. **Better Fault Tolerance**: If a task fails, only its segments need to be reprocessed
3. **Improved Resource Utilization**: The scheduler optimizes node usage
4. **Scalability**: Easy to scale by changing array size or segments per task
5. **Built-in Management Tools**: Monitor progress with SLURM commands
6. **Concurrency Control**: Throttle concurrent tasks to avoid overwhelming resources

The new approach leverages SLURM's built-in capabilities rather than implementing parallel processing manually, resulting in better performance, reliability, and resource utilization across the entire computing cluster.

## Best Practices

1. **Use Environment Variables for Account**: Instead of hardcoding the account name, use the `$SBATCH_ACCOUNT` environment variable. This makes scripts more portable and secure.

2. **Optimize Throttle Settings**: Control concurrent task limits based on cluster size:
   - **Small clusters**: `--throttle 10-20` 
   - **Medium clusters**: `--throttle 50` (default)
   - **Large clusters**: `--throttle 100+`

3. **Configure Appropriate Timeouts**: Use extended time limits for large datasets:
   - **Short jobs**: `--time 24` (24 hours)
   - **Medium jobs**: `--time 72` (3 days) 
   - **Large datasets**: `--time 168` (7 days)

4. **Balance segments-per-task**: Optimize the trade-off between parallelism and overhead:
   - **More parallelism**: `--segments-per-task 10-25` (more tasks, faster completion)
   - **Less overhead**: `--segments-per-task 50-100` (fewer tasks, reduced scheduling load)

5. **Test Before Production**: Always validate configurations with dry-run mode:
   ```bash
   ./test-config.sh  # Generates scripts without submitting
   ```

6. **Use Parquet Output Format**: Parquet files are approximately half the size of equivalent CSV files, with better compression and query performance.

7. **Leverage SLURM Arrays**: Distribute computation across multiple nodes by using SLURM array jobs. Control parallel execution with the `%` throttling parameter (e.g., `--array=0-899%50`).

8. **Monitor Resource Usage**: Check job efficiency and adjust resource requests:
   ```bash
   squeue -u $USER                    # Check running jobs
   sacct -j JOBID --format=JobID,MaxRSS,MaxVMSize,CPUTime  # Check resource usage
   ```

9. **Repository Management**: Don't commit generated code to Git. Instead:
   - Include templates and generation scripts in the repository
   - Document the generation process in the README
   - Add generated files to .gitignore

By following these practices, you'll create a more efficient, maintainable, and scalable pipeline for processing Common Crawl data across a computing cluster.

## Troubleshooting

### Common Issues and Solutions

**Job stays in PENDING state with "PartitionConfig" reason:**
- **Cause**: Default account is denied access to compute partition
- **Solution**: Set proper account with `source runme.sh` before submitting

**Jobs timeout after 24 hours:**
- **Cause**: Default time limit too short for large datasets  
- **Solution**: Use `--time 168` for 7-day limit

**Low parallelism (only 10 tasks running):**
- **Cause**: Default throttle limit of 10 concurrent tasks
- **Solution**: Use `--throttle 50` or higher based on cluster capacity

**Git LFS errors during push:**
- **Cause**: Git LFS not installed but repository configured for it
- **Solution**: Add miniconda git to PATH: `export PATH="./miniconda3/bin:$PATH"`

**Micromamba segmentation faults:**
- **Cause**: Micromamba instability in HPC environments
- **Solution**: Use Miniconda3 instead - more stable and reliable for cluster computing
- **Migration**: Update scripts to use `./miniconda3/bin/python` instead of micromamba commands
- **Solution**: Add miniconda git to PATH: `export PATH="./miniconda3/bin:$PATH"`

**Array jobs not starting:**
- **Cause**: JobArrayTaskLimit reached
- **Solution**: This is normal - SLURM queues tasks and starts them as resources become available

### Performance Monitoring

```bash
# Check job status
squeue -u $USER

# Monitor resource usage
sacct -j JOBID --format=JobID,MaxRSS,MaxVMSize,CPUTime,State

# Check partition availability  
sinfo -p compute

# View detailed job information
scontrol show job JOBID
```

## Git LFS Setup and Data Download

This repository uses Git LFS (Large File Storage) for managing large data files like parquet files and datasets.

### Installing Git LFS

If you don't have Git LFS installed, you can use the version included with Miniconda3:

```bash
# Add miniconda3 git (includes LFS support) to your PATH
export PATH="./miniconda3/bin:$PATH"

# Or install git-lfs separately if needed
# conda install git-lfs
# git lfs install
```

### Downloading LFS Content

After cloning the repository, download the large files:

```bash
# Download all LFS files
git lfs pull

# Or download specific files
git lfs pull --include="*.parquet"
git lfs pull --include="wet.paths"
```

### Checking LFS Status

```bash
# View which files are stored in LFS
git lfs ls-files

# Check LFS tracking patterns
git lfs track

# View LFS file info
git lfs pointer --file=BristolPostcodeLookup.parquet
```

### Working with LFS Files

When you modify large data files, Git LFS automatically handles them:

```bash
# Add and commit LFS files normally
git add BristolPostcodeLookup.parquet
git commit -m "Update lookup data"
git push
```

**Note**: Large files (>100MB) are automatically tracked by LFS. The `.gitattributes` file defines which file types use LFS storage.