import os
import gzip
import shutil
import pandas as pd
import requests
import argparse
import logging
import time
from warcio.archiveiterator import ArchiveIterator
from urllib.parse import urljoin
import re

def setup_logging(job_id=None):
    """Configure logging for the application"""
    # Include job ID in log filename if provided
    log_filename = f"commoncrawl_processor_{job_id}.log" if job_id else "commoncrawl_processor.log"
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_filename),
            logging.StreamHandler()
        ]
    )
    return logging.getLogger(__name__)

def download_file(url, output_path, max_retries=5, retry_delay=1):
    """Download a file with retry logic"""
    for attempt in range(max_retries):
        try:
            with requests.get(url, stream=True) as response:
                response.raise_for_status()
                with open(output_path, 'wb') as f:
                    shutil.copyfileobj(response.raw, f)
            logging.info(f"Successfully downloaded {url} to {output_path}")
            return True
        except Exception as e:
            if attempt < max_retries - 1:
                logging.warning(f"Attempt {attempt+1} failed: {e}. Retrying in {retry_delay} seconds...")
                time.sleep(retry_delay)
                retry_delay *= 2  # Exponential backoff
            else:
                logging.error(f"Failed to download {url} after {max_retries} attempts: {e}")
                return False

def postcode_finder(text):
    postcodes = re.findall(r'\b[A-Z]{1,2}[0-9][A-Z0-9]? [0-9][ABD-HJLNP-UW-Z]{2}\b', text)
    return list(set(postcodes))

def Bristol_postcode_finder(text, postcode_lookup):
    postcodes = re.findall(r'\b[A-Z]{1,2}[0-9][A-Z0-9]? [0-9][ABD-HJLNP-UW-Z]{2}\b', text)
    postcodes = list(set(postcodes))
    postcodes = [postcode for postcode in postcodes if postcode.startswith("BS")]
    matches = [postcode for postcode in postcodes if postcode in postcode_lookup['pcds'].values]
    if matches:
        return matches

def extract_website(url):
    website = re.sub(r'\.uk/.*', ".uk", url)
    website = website.replace("https://", "").replace("http://", "")
    return website

def process_wet_file(wet_file_path, output_path, postcode_lookup=None):
    """Process a WET file and extract relevant information"""
    results = []
    
    try:
        with gzip.open(wet_file_path, 'rb') as f:
            for record in ArchiveIterator(f):
                if record.rec_type == 'conversion':
                    uri = record.rec_headers.get_header('WARC-Target-URI')
                    language = record.rec_headers.get_header('WARC-Identified-Content-Language')

                    if ('.co.uk/' in uri) & (language == 'eng'):
                        website = extract_website(uri)
                        text = record.content_stream().read().decode('utf-8', 'ignore')
                        postcodes = Bristol_postcode_finder(text, postcode_lookup)
                        text = text.lower()

                        if postcodes is not None:
                            record_data = {
                                'uri': uri,
                                'website': website,
                                'postcodes': postcodes,
                                'text': text
                            }
                            results.append(record_data)
    except Exception as e:
        logging.error(f"Error processing {wet_file_path}: {e}")
    
    if results:
        df = pd.DataFrame(results)
        df.to_parquet(output_path, engine='pyarrow', compression='snappy')
        logging.info(f"Processed {len(results)} records from {wet_file_path}")
    else:
        logging.warning(f"No records extracted from {wet_file_path}")
    
    return len(results)

def process_segment(segment_number, wet_paths_file, server_url, output_dir, postcode_lookup=None):
    """Process a single segment"""
    try:
        with open(wet_paths_file, 'r') as f:
            paths = f.readlines()
        
        if segment_number >= len(paths):
            logging.error(f"Segment number {segment_number} exceeds available paths ({len(paths)})")
            return False
        
        wet_path = paths[segment_number].strip()
        file_url = urljoin(server_url, wet_path)
        
        segment_str = f"{segment_number:05d}"
        gz_filename = f"crawldata{args.crawl_date}segment{segment_str}.wet.gz"
        wet_filename = f"crawldata{args.crawl_date}segment{segment_str}.wet"
        parquet_filename = f"crawldata{args.crawl_date}segment{segment_str}.parquet"
        
        gz_filepath = os.path.join(output_dir, gz_filename)
        wet_filepath = os.path.join(output_dir, wet_filename)
        parquet_filepath = os.path.join(output_dir, parquet_filename)
        
        logging.info(f"Downloading {file_url} to {gz_filepath}")
        if not download_file(file_url, gz_filepath):
            logging.error(f"Failed to download {file_url}")
            return False
        
        logging.info(f"Processing {gz_filepath}")
        records_processed = process_wet_file(gz_filepath, parquet_filepath, postcode_lookup)
        
        if os.path.exists(gz_filepath):
            os.remove(gz_filepath)
            logging.info(f"Removed {gz_filepath}")
                
        logging.info(f"Completed processing segment {segment_number}: {records_processed} records")
        return True
        
    except Exception as e:
        logging.error(f"Error processing segment {segment_number}: {e}")
        return False

def load_postcode_lookup(file_path):
    """Load postcode lookup data if available"""
    if os.path.exists(file_path):
        try:
            return pd.read_parquet(file_path)
        except Exception as e:
            logging.error(f"Error loading postcode lookup file: {e}")
    return None

def main():
    parser = argparse.ArgumentParser(description="Process CommonCrawl WET files")
    parser.add_argument("--crawl-date", type=str, required=True, help="Crawl date identifier (e.g., 202350)")
    parser.add_argument("--server-url", type=str, default="https://data.commoncrawl.org/", help="CommonCrawl server URL")
    parser.add_argument("--wet-paths", type=str, default="wet.paths", help="Path to wet.paths file")
    parser.add_argument("--output-dir", type=str, default="output", help="Output directory")
    parser.add_argument("--task-id", type=int, help="Task ID for SLURM array jobs")
    parser.add_argument("--segments-per-task", type=int, default=1, help="Number of segments to process per task")
    parser.add_argument("--segment", type=int, help="Specific segment to process (for direct invocation)")
    parser.add_argument("--postcode-lookup", type=str, default="BristolPostcodeLookup.parquet", help="Path to postcode lookup file")
    parser.add_argument("--job-id", type=str, help="Job ID for logging purposes")

    global args
    args = parser.parse_args()
    
    global logger
    logger = setup_logging(args.job_id)
    
    # Create output directory if it doesn't exist
    os.makedirs(args.output_dir, exist_ok=True)
    
    # Load postcode lookup data
    postcode_lookup = load_postcode_lookup(args.postcode_lookup)
    
    # Determine which segment(s) to process
    if args.segment is not None:
        # Direct invocation with specific segment
        segment_numbers = [args.segment]
        logger.info(f"Processing single segment {args.segment}")
    elif args.task_id is not None:
        # Array job mode with explicitly provided task ID
        start_segment = args.task_id * args.segments_per_task
        end_segment = start_segment + args.segments_per_task
        segment_numbers = list(range(start_segment, end_segment))
        logger.info(f"Task {args.task_id} processing segments {start_segment} to {end_segment-1}")
    else:
        logger.error("No segment specified and no task ID provided")
        return
    
    logger.info(f"Using server URL: {args.server_url}")
    logger.info(f"Output directory: {args.output_dir}")
    
    successful_segments = 0
    for segment_number in segment_numbers:
        if process_segment(segment_number, args.wet_paths, args.server_url, args.output_dir, postcode_lookup):
            successful_segments += 1
    
    logger.info(f"Processing completed: {successful_segments}/{len(segment_numbers)} segments successful")

if __name__ == "__main__":
    main()
