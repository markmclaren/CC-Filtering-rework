# CC-Filtering-rework

A tool for processing and filtering Common Crawl data using Slurm-based parallel processing.

Derived from original work here: [CC-Filtering](https://github.com/0emerald/CC-Filtering)

## Overview

This repository contains scripts for efficiently downloading and processing Common Crawl data using a Slurm computing cluster. The workflow automatically creates and submits batch jobs for each crawl date, allowing for parallel processing of multiple crawl segments.

## Scripts

### Core Scripts
- **job-runner.sh** - Main entry point that configures the Micromamba environment and initiates the job creation process
- **slurm-sequential-runner.py** - Creates individual Slurm jobs for each crawl date based on the provided template
- **common-crawl-processor.py** - Downloads and processes Common Crawl data for a specific crawl date
- **job-template.sh** - Template script for Slurm array jobs that runs the processor with appropriate configurations

## Data Files

- **BristolPostcodeLookup.parquet** - Lookup table for Bristol postcodes used in data filtering
- **crawl_data.txt** - List of Common Crawl dates to process
- **wet.paths** - Path information for Common Crawl WET files

## Usage

### Quick Start

To run the entire pipeline:

```bash
sbatch job-runner.sh
```

### Workflow Explanation

1. **job-runner.sh** configures the Micromamba environment and calls `slurm-sequential-runner.py`
2. **slurm-sequential-runner.py** reads crawl dates from `crawl_data.txt` and creates a Slurm job for each date
3. Each job uses **job-template.sh** to configure the environment and run **common-crawl-processor.py**
4. **common-crawl-processor.py** downloads and processes the Common Crawl data for its assigned date

### Requirements

- Slurm cluster access
- Micromamba environment (configured in the scripts)
- Python 3.x with required packages (listed in environment setup)

## Configuration

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

2. **Use Parquet Output Format**: Parquet files are approximately half the size of equivalent CSV files, with better compression and query performance.

3. **Leverage SLURM Arrays**: Distribute computation across multiple nodes by using SLURM array jobs. Control parallel execution with the `%` throttling parameter (e.g., `--array=0-899%10`).

4. **Minimize Shell Script Complexity**: Keep shell scripts simple by using Python to generate them:
   - Create a template SLURM script
   - Use Python to populate the template with specific parameters
   - Generate the final SLURM scripts programmatically

5. **Sequential Crawl Processing**: Use a Python script to submit SLURM jobs for each crawl date sequentially:
   ```python
   for crawl_date in crawl_dates:
       subprocess.run(["sbatch", f"process_crawl_{crawl_date}.sh"])
       # Wait for completion if needed
   ```

6. **Repository Management**: Don't commit generated code to Git. Instead:
   - Include templates and generation scripts in the repository
   - Document the generation process in the README
   - Add generated files to .gitignore

By following these practices, you'll create a more efficient, maintainable, and scalable pipeline for processing Common Crawl data across a computing cluster.