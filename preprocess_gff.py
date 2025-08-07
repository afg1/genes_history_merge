#!/usr/bin/env python3
"""
Preprocess GFF files using singularity container
Converts GFF files to gene format for each organism/release
"""

import os
import sys
import json
import logging
import subprocess
from pathlib import Path
from typing import Dict, List, Optional, Tuple
import psycopg2
from psycopg2.extras import RealDictCursor
from concurrent.futures import ThreadPoolExecutor, as_completed
import time
from datetime import datetime
import glob

import config

# Preprocessing-specific configuration
SINGULARITY_IMAGE = "/hps/nobackup/agb/rnacentral/genes-testing/gff2genes/rnacentral-rnacentral-import-pipeline-latest.sif"
SINGULARITY_ENV_PATH = "/hps/nobackup/agb/rnacentral/genes-testing/bin"
PREPROCESSING_LOG_DIR = os.path.join(config.LOG_DIR, "preprocessing")
MAX_PARALLEL_PREPROCESSING = 4  # Adjust based on available resources

def setup_preprocessing_logging(task_id=None):
    """Configure logging for preprocessing"""
    os.makedirs(PREPROCESSING_LOG_DIR, exist_ok=True)
    
    if task_id is not None:
        log_file = os.path.join(PREPROCESSING_LOG_DIR, f'preprocess_task_{task_id}.log')
    else:
        log_file = os.path.join(PREPROCESSING_LOG_DIR, 'preprocess_main.log')
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler(sys.stdout)
        ]
    )
    
    return logging.getLogger(__name__)


def get_organism_taxid_mapping() -> Dict[str, int]:
    """
    Query database to get mapping of organism names to taxids
    Returns: Dictionary mapping transformed organism names to taxids
    """
    logger = logging.getLogger(__name__)
    
    db_conn_str = os.environ.get(config.DB_CONNECTION_ENV)
    if not db_conn_str:
        raise ValueError(f"Database connection string not found in environment variable {config.DB_CONNECTION_ENV}")
    
    logger.info("Fetching organism-taxid mapping from database...")
    
    try:
        conn = psycopg2.connect(db_conn_str)
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        
        cursor.execute(config.DB_QUERY)
        organisms = cursor.fetchall()
        
        # Create mapping of transformed names to taxids
        mapping = {}
        for org in organisms:
            transformed_name = transform_organism_name(org['organism_name'])
            mapping[transformed_name] = org['taxid']
        
        logger.info(f"Created mapping for {len(mapping)} organisms")
        
        cursor.close()
        conn.close()
        
        return mapping
    
    except Exception as e:
        logger.error(f"Database query failed: {str(e)}")
        raise


def transform_organism_name(name: str) -> str:
    """Transform organism name to match directory format"""
    import re
    transformed = name.lower()
    transformed = re.sub(r'[^a-z0-9]+', '_', transformed)
    transformed = transformed.strip('_')
    return transformed


def find_gff_files(base_dir: str, release: Optional[int] = None) -> List[Tuple[str, int, str]]:
    """
    Find all GFF files that need preprocessing
    Returns: List of tuples (gff_path, release_number, organism_dir_name)
    """
    logger = logging.getLogger(__name__)
    gff_files = []
    
    if release:
        # Process specific release
        release_dirs = [f"release_{release}"]
    else:
        # Process all releases
        release_dirs = [d for d in os.listdir(base_dir) 
                       if d.startswith('release_') and os.path.isdir(os.path.join(base_dir, d))]
    
    for release_dir in sorted(release_dirs):
        release_num = int(release_dir.replace('release_', ''))
        release_path = os.path.join(base_dir, release_dir)
        
        # Find all organism directories
        for organism_dir in os.listdir(release_path):
            organism_path = os.path.join(release_path, organism_dir)
            
            if os.path.isdir(organism_path):
                # Find decompressed GFF files
                for gff_file in glob.glob(os.path.join(organism_path, "*.gff3")):
                    gff_files.append((gff_file, release_num, organism_dir))
    
    logger.info(f"Found {len(gff_files)} GFF files to process")
    return gff_files


def run_singularity_conversion(gff_path: str, taxid: int, working_dir: Optional[str] = None) -> Dict:
    """
    Run singularity container to convert a single GFF file
    """
    logger = logging.getLogger(__name__)
    
    if working_dir is None:
        working_dir = os.path.dirname(gff_path)
    
    gff_filename = os.path.basename(gff_path)
    
    # Set up environment
    env = os.environ.copy()
    env['SINGULARITYENV_APPEND_PATH'] = SINGULARITY_ENV_PATH
    
    # Build command
    cmd = [
        'singularity', 'exec',
        SINGULARITY_IMAGE,
        'rnac', 'genes', 'convert',
        '--gff_file', gff_filename,
        '--taxid', str(taxid)
    ]
    
    logger.info(f"Processing {gff_filename} with taxid {taxid}")
    logger.debug(f"Command: {' '.join(cmd)}")
    logger.debug(f"Working directory: {working_dir}")
    
    try:
        # Run singularity command
        result = subprocess.run(
            cmd,
            cwd=working_dir,
            env=env,
            capture_output=True,
            text=True,
            timeout=600  # 10 minute timeout per file
        )
        
        if result.returncode == 0:
            logger.info(f"Successfully processed {gff_filename}")
            return {
                'status': 'success',
                'gff_file': gff_path,
                'taxid': taxid,
                'stdout': result.stdout,
                'stderr': result.stderr
            }
        else:
            logger.error(f"Failed to process {gff_filename}: {result.stderr}")
            return {
                'status': 'failed',
                'gff_file': gff_path,
                'taxid': taxid,
                'error': result.stderr,
                'return_code': result.returncode
            }
    
    except subprocess.TimeoutExpired:
        logger.error(f"Timeout processing {gff_filename}")
        return {
            'status': 'timeout',
            'gff_file': gff_path,
            'taxid': taxid
        }
    
    except Exception as e:
        logger.error(f"Error processing {gff_filename}: {str(e)}")
        return {
            'status': 'error',
            'gff_file': gff_path,
            'taxid': taxid,
            'error': str(e)
        }


def process_single_file(task: Tuple[str, int, str, Dict[str, int]]) -> Dict:
    """
    Process a single GFF file
    """
    gff_path, release, organism_dir, taxid_mapping = task
    
    # Look up taxid
    taxid = taxid_mapping.get(organism_dir)
    
    if not taxid:
        logger = logging.getLogger(__name__)
        logger.warning(f"No taxid found for organism directory: {organism_dir}")
        return {
            'status': 'skipped',
            'gff_file': gff_path,
            'release': release,
            'organism': organism_dir,
            'reason': 'no_taxid'
        }
    
    # Check if output already exists (optional - for resuming)
    output_pattern = os.path.join(os.path.dirname(gff_path), "*.genes.json")
    existing_output = glob.glob(output_pattern)
    if existing_output:
        logger = logging.getLogger(__name__)
        logger.info(f"Output already exists for {gff_path}, skipping")
        return {
            'status': 'skipped',
            'gff_file': gff_path,
            'release': release,
            'organism': organism_dir,
            'taxid': taxid,
            'reason': 'already_processed'
        }
    
    # Run conversion
    result = run_singularity_conversion(gff_path, taxid)
    result['release'] = release
    result['organism'] = organism_dir
    
    return result


def main():
    """
    Main preprocessing function
    """
    # Check for Slurm array task
    task_id = os.environ.get('SLURM_ARRAY_TASK_ID')
    array_size = os.environ.get('SLURM_ARRAY_TASK_COUNT')
    
    if task_id:
        task_id = int(task_id)
        logger = setup_preprocessing_logging(task_id)
        logger.info(f"Running as Slurm array task {task_id}")
    else:
        logger = setup_preprocessing_logging()
        logger.info("Running as standalone script")
        task_id = 0
        array_size = 1
    
    if array_size:
        array_size = int(array_size)
    else:
        array_size = 1
    
    # Check if singularity image exists
    if not os.path.exists(SINGULARITY_IMAGE):
        logger.error(f"Singularity image not found: {SINGULARITY_IMAGE}")
        sys.exit(1)
    
    # Get organism-taxid mapping
    try:
        taxid_mapping = get_organism_taxid_mapping()
    except Exception as e:
        logger.error(f"Failed to get taxid mapping: {str(e)}")
        sys.exit(1)
    
    # Find all GFF files
    gff_files = find_gff_files(config.DATA_DIR)
    
    if not gff_files:
        logger.warning("No GFF files found to process")
        return
    
    # Distribute work among array tasks
    if array_size > 1:
        # Split files among tasks
        files_per_task = len(gff_files) // array_size
        remainder = len(gff_files) % array_size
        
        if task_id < remainder:
            start_idx = task_id * (files_per_task + 1)
            end_idx = start_idx + files_per_task + 1
        else:
            start_idx = remainder * (files_per_task + 1) + (task_id - remainder) * files_per_task
            end_idx = start_idx + files_per_task
        
        my_files = gff_files[start_idx:end_idx]
        logger.info(f"Task {task_id} processing {len(my_files)} files (indices {start_idx}-{end_idx})")
    else:
        my_files = gff_files
        logger.info(f"Processing all {len(my_files)} files")
    
    # Process files
    results = []
    start_time = time.time()
    
    # Prepare tasks with taxid mapping
    tasks = [(gff_path, release, organism, taxid_mapping) 
             for gff_path, release, organism in my_files]
    
    # Process with limited parallelism (singularity can be resource-intensive)
    with ThreadPoolExecutor(max_workers=MAX_PARALLEL_PREPROCESSING) as executor:
        future_to_task = {executor.submit(process_single_file, task): task 
                         for task in tasks}
        
        completed = 0
        for future in as_completed(future_to_task):
            completed += 1
            task = future_to_task[future]
            
            try:
                result = future.result()
                results.append(result)
                
                # Progress update
                if completed % 10 == 0 or completed == len(tasks):
                    elapsed = time.time() - start_time
                    rate = completed / elapsed if elapsed > 0 else 0
                    remaining = (len(tasks) - completed) / rate if rate > 0 else 0
                    logger.info(f"Progress: {completed}/{len(tasks)} files processed "
                              f"({rate:.1f} files/min, ~{remaining/60:.1f} hours remaining)")
            
            except Exception as e:
                logger.error(f"Task failed: {str(e)}")
                results.append({
                    'status': 'error',
                    'gff_file': task[0],
                    'error': str(e)
                })
    
    # Generate summary
    summary = {
        'task_id': task_id if task_id else 'main',
        'start_time': datetime.fromtimestamp(start_time).isoformat(),
        'end_time': datetime.now().isoformat(),
        'total_files': len(my_files),
        'processed': len(results),
        'statistics': {
            'successful': sum(1 for r in results if r.get('status') == 'success'),
            'failed': sum(1 for r in results if r.get('status') == 'failed'),
            'timeout': sum(1 for r in results if r.get('status') == 'timeout'),
            'skipped': sum(1 for r in results if r.get('status') == 'skipped'),
            'error': sum(1 for r in results if r.get('status') == 'error')
        },
        'results': results
    }
    
    # Save summary
    summary_file = os.path.join(PREPROCESSING_LOG_DIR, f'preprocess_summary_task_{task_id}.json')
    with open(summary_file, 'w') as f:
        json.dump(summary, f, indent=2)
    
    # Print summary
    logger.info("="*60)
    logger.info("PREPROCESSING SUMMARY")
    logger.info("="*60)
    logger.info(f"Total files: {summary['total_files']}")
    logger.info(f"Successful: {summary['statistics']['successful']}")
    logger.info(f"Failed: {summary['statistics']['failed']}")
    logger.info(f"Timeout: {summary['statistics']['timeout']}")
    logger.info(f"Skipped: {summary['statistics']['skipped']}")
    logger.info(f"Errors: {summary['statistics']['error']}")
    logger.info(f"Summary saved to: {summary_file}")
    
    # Exit with error if too many failures
    failure_rate = (summary['statistics']['failed'] + summary['statistics']['error']) / len(my_files) if my_files else 0
    if failure_rate > 0.5:
        logger.error(f"High failure rate: {failure_rate:.1%}")
        sys.exit(1)


if __name__ == "__main__":
    main()
