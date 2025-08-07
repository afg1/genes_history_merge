#!/usr/bin/env python3
"""
Slurm-optimized version for parallel fetching across multiple nodes
This script processes a subset of releases based on SLURM_ARRAY_TASK_ID
"""

import os
import sys
import json
import logging
import subprocess
import gzip
import shutil
from datetime import datetime
from pathlib import Path
import psycopg2
from psycopg2.extras import RealDictCursor
import time
import re

import config

def setup_logging(task_id=None):
    """Configure logging for the application"""
    # Create log directory if it doesn't exist
    os.makedirs(config.LOG_DIR, exist_ok=True)
    
    # Modify log filename for array tasks
    if task_id:
        log_file = os.path.join(config.LOG_DIR, f'download_task_{task_id}.log')
        error_file = os.path.join(config.LOG_DIR, f'errors_task_{task_id}.log')
    else:
        log_file = config.MAIN_LOG_FILE
        error_file = config.ERROR_LOG_FILE
    
    # Configure root logger
    logging.basicConfig(
        level=getattr(logging, config.LOG_LEVEL),
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler(sys.stdout)
        ]
    )
    
    # Create error file handler
    error_handler = logging.FileHandler(error_file)
    error_handler.setLevel(logging.ERROR)
    error_handler.setFormatter(logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    ))
    logging.getLogger().addHandler(error_handler)
    
    return logging.getLogger(__name__)


def get_organisms_from_db():
    """Query the database to get list of organisms with their taxids"""
    logger = logging.getLogger(__name__)
    
    # Get database connection string from environment
    db_conn_str = os.environ.get(config.DB_CONNECTION_ENV)
    if not db_conn_str:
        raise ValueError(f"Database connection string not found in environment variable {config.DB_CONNECTION_ENV}")
    
    logger.info(f"Connecting to database...")
    
    try:
        # Connect to database
        conn = psycopg2.connect(db_conn_str)
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        
        # Execute query
        logger.info("Executing query to fetch organisms...")
        cursor.execute(config.DB_QUERY)
        
        # Fetch results
        organisms = cursor.fetchall()
        logger.info(f"Found {len(organisms)} organisms in database")
        
        # Close connection
        cursor.close()
        conn.close()
        
        return organisms
    
    except Exception as e:
        logger.error(f"Database query failed: {str(e)}")
        raise


def transform_organism_name(name: str) -> str:
    """Transform organism name to match RNAcentral URL format"""
    # Convert to lowercase
    transformed = name.lower()
    # Replace spaces and special characters with underscores
    transformed = re.sub(r'[^a-z0-9]+', '_', transformed)
    # Remove leading/trailing underscores
    transformed = transformed.strip('_')
    return transformed


def download_and_process_file(organism_name, taxid, release, output_path):
    """Download and decompress a single GFF file"""
    logger = logging.getLogger(__name__)
    
    # Create output directory
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    
    # Generate URL
    transformed_name = transform_organism_name(organism_name)
    base_url = f"{config.FTP_BASE_URL}/{release}.0/genome_coordinates/gff3/"
    
    # Try to find the actual file
    try:
        # List directory to find matching file
        result = subprocess.run(
            ['curl', '-s', base_url],
            capture_output=True,
            text=True,
            timeout=30
        )
        
        if result.returncode == 0:
            # Parse HTML to find matching files
            pattern = rf'href="({transformed_name}\.[^"]+\.gff3\.gz)"'
            matches = re.findall(pattern, result.stdout)
            
            if not matches:
                logger.debug(f"No file found for {organism_name} in release {release}")
                return {'status': 'not_found', 'organism': organism_name, 'release': release}
            
            # Use first match
            url = base_url + matches[0]
        else:
            return {'status': 'error', 'organism': organism_name, 'release': release}
            
    except Exception as e:
        logger.error(f"Failed to list directory: {str(e)}")
        return {'status': 'error', 'organism': organism_name, 'release': release}
    
    # Download file
    try:
        result = subprocess.run(
            ['wget', '--timeout=300', '--tries=3', '-O', output_path, url],
            capture_output=True,
            text=True,
            timeout=330
        )
        
        if result.returncode != 0:
            logger.error(f"Download failed for {organism_name} release {release}")
            return {'status': 'failed', 'organism': organism_name, 'release': release}
        
        # Decompress file
        decompressed_path = output_path[:-3]  # Remove .gz
        with gzip.open(output_path, 'rb') as f_in:
            with open(decompressed_path, 'wb') as f_out:
                shutil.copyfileobj(f_in, f_out)
        
        logger.info(f"Successfully processed {organism_name} release {release}")
        return {
            'status': 'success',
            'organism': organism_name,
            'release': release,
            'compressed_path': output_path,
            'decompressed_path': decompressed_path
        }
        
    except Exception as e:
        logger.error(f"Error processing {organism_name} release {release}: {str(e)}")
        return {'status': 'error', 'organism': organism_name, 'release': release}


def main():
    """Main function for Slurm array job"""
    
    # Get Slurm array task ID (if running as array job)
    task_id = os.environ.get('SLURM_ARRAY_TASK_ID')
    array_size = os.environ.get('SLURM_ARRAY_TASK_COUNT')
    
    if task_id:
        task_id = int(task_id)
        logger = setup_logging(task_id)
        logger.info(f"Running as Slurm array task {task_id}")
    else:
        logger = setup_logging()
        logger.info("Running as standalone script")
        task_id = 0
        array_size = 1
    
    if array_size:
        array_size = int(array_size)
    else:
        array_size = 1
    
    # Create data directory
    os.makedirs(config.DATA_DIR, exist_ok=True)
    
    # Get organisms from database
    organisms = get_organisms_from_db()
    
    # Determine which releases this task should process
    all_releases = list(range(config.RELEASE_START, config.RELEASE_END + 1))
    
    if array_size > 1:
        # Distribute releases among array tasks
        releases_per_task = len(all_releases) // array_size
        remainder = len(all_releases) % array_size
        
        if task_id < remainder:
            # First 'remainder' tasks get one extra release
            start_idx = task_id * (releases_per_task + 1)
            end_idx = start_idx + releases_per_task + 1
        else:
            # Remaining tasks get base number of releases
            start_idx = remainder * (releases_per_task + 1) + (task_id - remainder) * releases_per_task
            end_idx = start_idx + releases_per_task
        
        my_releases = all_releases[start_idx:end_idx]
    else:
        # Process all releases
        my_releases = all_releases
    
    logger.info(f"Task {task_id} processing releases: {my_releases}")
    
    # Process downloads
    results = []
    total_tasks = len(organisms) * len(my_releases)
    completed = 0
    
    for release in my_releases:
        logger.info(f"Processing release {release}...")
        
        for organism in organisms:
            organism_name = organism['organism_name']
            taxid = organism['taxid']
            
            # Generate output path
            transformed_name = transform_organism_name(organism_name)
            output_dir = os.path.join(config.DATA_DIR, f"release_{release}", transformed_name)
            output_filename = f"{transformed_name}.gff3.gz"
            output_path = os.path.join(output_dir, output_filename)
            
            # Download and process
            result = download_and_process_file(organism_name, taxid, release, output_path)
            results.append(result)
            
            completed += 1
            if completed % 10 == 0:
                logger.info(f"Progress: {completed}/{total_tasks} tasks completed")
    
    # Save task results
    summary = {
        'task_id': task_id,
        'releases': my_releases,
        'total_organisms': len(organisms),
        'total_tasks': total_tasks,
        'results': results,
        'statistics': {
            'successful': sum(1 for r in results if r['status'] == 'success'),
            'not_found': sum(1 for r in results if r['status'] == 'not_found'),
            'failed': sum(1 for r in results if r['status'] in ['failed', 'error'])
        }
    }
    
    # Save summary
    summary_file = os.path.join(config.LOG_DIR, f'summary_task_{task_id}.json')
    with open(summary_file, 'w') as f:
        json.dump(summary, f, indent=2)
    
    logger.info(f"Task {task_id} completed. Summary saved to {summary_file}")
    logger.info(f"Statistics: {summary['statistics']}")


if __name__ == "__main__":
    main()
