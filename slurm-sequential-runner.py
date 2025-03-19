import datetime
from simple_slurm import Slurm
import subprocess
import time
import string
import argparse
import os

def load_template_file(template_file):
    """Load command template from a file."""
    if not os.path.exists(template_file):
        raise FileNotFoundError(f"Template file not found: {template_file}")
    
    with open(template_file, 'r') as f:
        template_content = f.read()
    
    return template_content

def create_template_command(template_content, variables):
    """Create a command from a template string and variables dictionary."""
    template = string.Template(template_content)
    return template.safe_substitute(variables)

def submit_and_wait_for_job(date, n_files, template_content, job_name_prefix="batch_job",
                          partition=None, time_limit=None, mem=None, cpus_per_task=None,
                          check_interval=60, segments_per_task=100, throttle=10, dry_run=False):
    """
    Submit a job for one crawl date and wait for it to complete before returning.
    
    Args:
        date: Crawl date string
        n_files: Number of files for this crawl date
        template_content: Template content loaded from file
        job_name_prefix: Prefix for job name
        partition: SLURM partition (optional)
        time_limit: Time limit as datetime.timedelta (optional)
        mem: Memory request (optional)
        cpus_per_task: CPUs per task (optional)
        check_interval: How often to check job status in seconds
        segments_per_task: Number of segments per task
        dry_run: If True, do not submit the job; just output the generated script
    
    Returns:
        Job ID of the completed job (or None in dry-run mode)
    """

    array_size = n_files // segments_per_task
    
    # Create variables dictionary for template substitution
    variables = {
        "date": date,
        "n_files": n_files,
        "segments_per_task": segments_per_task,
    }
    
    # Create command from template
    command = create_template_command(template_content, variables)
    
    # Define job name
    job_name = f"{job_name_prefix}_{date}"
    
    # Create Slurm object with parameters
    slurm_params = {
        "job_name": job_name,
        "output": f"{job_name}_%j.out",
        "error": f"{job_name}_%j.err",
    }
    
    # Add optional parameters if provided
    if partition:
        slurm_params["partition"] = partition
    if time_limit:
        slurm_params["time"] = time_limit
    if mem:
        slurm_params["mem"] = mem
    if cpus_per_task:
        slurm_params["cpus_per_task"] = cpus_per_task
        
    # Create array based on n_files - each array task will process one file
    if array_size > 1:
        slurm_params["array"] = f"0-{array_size - 1}%{throttle}"
    
    # Create Slurm object
    slurm = Slurm(**slurm_params)
    slurm.add_cmd(command)
    
    if dry_run:
        # Output the generated SLURM script to a file instead of submitting it
        os.makedirs("generated_scripts", exist_ok=True)
        script_path = os.path.join("generated_scripts", f"{job_name}.sh")
        with open(script_path, 'w') as script_file:
            script_file.write(slurm.script())
        print(f"\n[DRY-RUN] Generated SLURM script for job {job_name} saved to {script_path}\n")
        return None
    
    # Submit the job
    job_id = slurm.sbatch()
    print(f"Submitted job {job_id} for date {date}")
    
    # Wait for this job to complete before continuing
    job_completed = False
    while not job_completed:
        try:
            result = subprocess.run(
                ["squeue", "-j", str(job_id)],
                capture_output=True,
                text=True,
                check=True,
            )
            if str(job_id) not in result.stdout:
                print(f"Job {job_id} for date {date} completed.")
                job_completed = True
            else:
                print(f"Job {job_id} for date {date} still running. Checking again in {check_interval} seconds...")
                time.sleep(check_interval)
        except subprocess.CalledProcessError:
            print(f"Job {job_id} for date {date} completed. (squeue error)")
            job_completed = True
        except Exception as e:
            print(f"Error checking job status: {e}")
            time.sleep(check_interval)
    
    return job_id

def process_crawl_data_sequentially(crawl_dates, n_files_list, template_content, 
                                  job_name_prefix="batch_job", partition=None, 
                                  time_limit=None, mem=None, cpus_per_task=None,
                                  check_interval=60, segments_per_task=100, dry_run=False):
    """
    Process each crawl date sequentially, waiting for each job to finish before starting the next.
    
    Args:
        crawl_dates: List of crawl date strings
        n_files_list: List of number of files for each crawl date
        template_content: Template content loaded from file
        job_name_prefix: Prefix for job names
        partition: SLURM partition (optional)
        time_limit: Time limit as datetime.timedelta (optional)
        mem: Memory request (optional)
        cpus_per_task: CPUs per task (optional)
        check_interval: How often to check job status in seconds
        segments_per_task: Number of segments per task
        dry_run: If True, do not submit the jobs; just output the generated scripts
    
    Returns:
        List of completed job IDs
    """
    completed_job_ids = []
    
    for i, (date, n_files) in enumerate(zip(crawl_dates, n_files_list)):
        print(f"\n[{i+1}/{len(crawl_dates)}] Processing crawl date {date} with {n_files} files")
        
        job_id = submit_and_wait_for_job(
            date=date,
            n_files=n_files,
            template_content=template_content,
            job_name_prefix=job_name_prefix,
            partition=partition,
            time_limit=time_limit,
            mem=mem,
            cpus_per_task=cpus_per_task,
            check_interval=check_interval,
            segments_per_task=segments_per_task,
            dry_run=dry_run
        )
        
        completed_job_ids.append(job_id)
        print(f"Completed job {job_id} for date {date}")
    
    return completed_job_ids

def main():
    # Parse command line arguments
    parser = argparse.ArgumentParser(description="Submit sequential SLURM jobs with template file")
    parser.add_argument("--template-file", required=True, help="Path to command template file")
    parser.add_argument("--partition", help="SLURM partition")
    parser.add_argument("--mem", default="100m", help="Memory request (e.g., '100m')")
    parser.add_argument("--time", type=int, default=24, help="Time limit in hours")
    parser.add_argument("--cpus", type=int, default=1, help="CPUs per task")
    parser.add_argument("--job-prefix", default="crawl_job", help="Job name prefix")
    parser.add_argument("--check-interval", type=int, default=60,
                      help="How often to check job status (seconds)")
    parser.add_argument("--crawl-dates-file", help="Path to file with crawl dates and file counts")
    parser.add_argument("--segments-per-task", type=int, default=100, help="Segments per task")
    parser.add_argument("--dry-run", action="store_true", help="Run in dry-run mode (do not submit jobs)")
    args = parser.parse_args()
    
    # Load template from file
    template_content = load_template_file(args.template_file)
    
    # Define crawl dates and corresponding number of files
    # Either use the default values or load from a file if provided
    if args.crawl_dates_file and os.path.exists(args.crawl_dates_file):
        crawl_dates = []
        n_files_list = []
        with open(args.crawl_dates_file, 'r') as f:
            for line in f:
                if line.strip() and not line.startswith('#'):
                    parts = line.strip().split()
                    if len(parts) >= 2:
                        crawl_dates.append(parts[0])
                        n_files_list.append(int(parts[1]))
    else:
        # Default values
        crawl_dates = ["202104", "202110", "202117", "202121", "202125", "202131", "202139", "202143", "202149"]
        n_files_list = [79840, 64000, 64000, 64000, 64000, 72000, 72000, 72000, 64000]
    
    # Convert time to timedelta
    time_limit = datetime.timedelta(hours=args.time)
    
    # Process the jobs sequentially
    completed_job_ids = process_crawl_data_sequentially(
        crawl_dates=crawl_dates, 
        n_files_list=n_files_list,
        template_content=template_content,
        job_name_prefix=args.job_prefix,
        partition=args.partition,
        time_limit=time_limit,
        mem=args.mem,
        cpus_per_task=args.cpus,
        check_interval=args.check_interval,
        segments_per_task=args.segments_per_task,
        dry_run=args.dry_run
    )
    
    print("\nAll jobs completed successfully!")
    print(f"Completed jobs: {completed_job_ids}")

if __name__ == "__main__":
    main()



