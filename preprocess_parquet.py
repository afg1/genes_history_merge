#!/usr/bin/env python3
"""
Preprocess transcript parquet files using singularity container
Converts transcript parquet files to feature parquet files for classification model
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
SO_MODEL_PATH = "/hps/nobackup/agb/rnacentral/genes-testing/gff2genes/so_embedding_model.emb"
FEATURE_LOG_DIR = os.path.join(config.LOG_DIR, "feature_preprocessing")
MAX_PARALLEL_FEATURE_PROCESSING = 2  # Adjust based on available resources - lower due to memory requirements

def setup_feature_logging(task_id=None):
    """Configure logging for feature preprocessing"""
    os.makedirs(FEATURE_LOG_DIR, exist_ok=True)
    
    if task_id is not None:
        log_file = os.path.join(FEATURE_LOG_DIR, f'feature_preprocess_task_{task_id}.log')
    else:
        log_file = os.path.join(FEATURE_LOG_DIR, 'feature_preprocess_main.log')
    
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


def find_transcript_parquet_files(base_dir: str, release: Optional[int] = None) -> List[Tuple[str, int, str]]:
    """
    Find all transcript parquet files that need feature preprocessing
    Returns: List of tuples (parquet_path, release_number, organism_dir_name)
    """
    logger = logging.getLogger(__name__)
    parquet_files = []
    
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
                # Find transcript parquet files (not feature files)
                # Look for parquet files that don't already have "_features" in the name
                for parquet_file in glob.glob(os.path.join(organism_path, "*.parquet")):
                    if "_features" not in os.path.basename(parquet_file):
                        parquet_files.append((parquet_file, release_num, organism_dir))
    
    logger.info(f"Found {len(parquet_files)} transcript parquet files to process")
    return parquet_files


def extract_organism_assembly_from_filename(filename: str) -> Tuple[str, str]:
    """
    Extract organism and assembly from filename
    Expected format: organism.assembly.parquet or organism.assembly_transcripts.parquet
    Returns: (organism, assembly)
    """
    base_name = os.path.splitext(filename)[0]  # Remove .parquet
    
    # Remove common suffixes like _transcripts
    base_name = base_name.replace('_transcripts', '')
    
    # Split on dots - expect organism.assembly format
    parts = base_name.split('.')
    if len(parts) >= 2:
        organism = parts[0]
        assembly = parts[1]
    else:
        # Fallback - use whole name as organism, empty assembly
        organism = base_name
        assembly = ""
    
    return organism, assembly


def run_singularity_feature_preprocessing(parquet_path: str, task_id: int = 0, working_dir: Optional[str] = None) -> Dict:
    """
    Run singularity container to preprocess a single transcript parquet file
    """
    logger = logging.getLogger(__name__)
    
    if working_dir is None:
        working_dir = os.path.dirname(parquet_path)
    
    parquet_filename = os.path.basename(parquet_path)
    
    # Extract organism and assembly from filename
    organism, assembly = extract_organism_assembly_from_filename(parquet_filename)
    
    # Generate output filename using the specified pattern
    if assembly:
        output_filename = f"{organism}.{assembly}_{task_id}_features.parquet"
    else:
        output_filename = f"{organism}_{task_id}_features.parquet"
    
    # Set up environment
    env = os.environ.copy()
    env['SINGULARITYENV_APPEND_PATH'] = SINGULARITY_ENV_PATH
    
    # Build command exactly as specified
    cmd = [
        'singularity', 'exec',
        SINGULARITY_IMAGE,
        'rnac', 'genes', 'preprocess',
        '--transcripts_file', parquet_filename,
        '--so_model_path', SO_MODEL_PATH,
        '--output', output_filename,
        '--no-parallel'
    ]
    
    logger.info(f"Processing {parquet_filename} -> {output_filename}")
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
            timeout=1800  # 30 minute timeout per file (feature generation can be slower)
        )
        
        if result.returncode == 0:
            logger.info(f"Successfully processed {parquet_filename}")
            return {
                'status': 'success',
                'input_file': parquet_path,
                'output_file': os.path.join(working_dir, output_filename),
                'stdout': result.stdout,
                'stderr': result.stderr
            }
        else:
            logger.error(f"Failed to process {parquet_filename}: {result.stderr}")
            return {
                'status': 'failed',
                'input_file': parquet_path,
                'error': result.stderr,
                'return_code': result.returncode
            }
    
    except subprocess.TimeoutExpired:
        logger.error(f"Timeout processing {parquet_filename}")
        return {
            'status': 'timeout',
            'input_file': parquet_path
        }
    
    except Exception as e:
        logger.error(f"Error processing {parquet_filename}: {str(e)}")
        return {
            'status': 'error',
            'input_file': parquet_path,
            'error': str(e)
        }


def process_single_file(task: Tuple[str, int, str, int]) -> Dict:
    """
    Process a single transcript parquet file
    """
    parquet_path, release, organism_dir, array_task_id = task
    
    # Check if output already exists (optional - for resuming)
    working_dir = os.path.dirname(parquet_path)
    parquet_filename = os.path.basename(parquet_path)
    
    organism, assembly = extract_organism_assembly_from_filename(parquet_filename)
    
    if assembly:
        expected_output = f"{organism}.{assembly}_{array_task_id}_features.parquet"
    else:
        expected_output = f"{organism}_{array_task_id}_features.parquet"
    
    output_path = os.path.join(working_dir, expected_output)
    
    if os.path.exists(output_path):
        logger = logging.getLogger(__name__)
        logger.info(f"Output already exists for {parquet_path}, skipping")
        return {
            'status': 'skipped',
            'input_file': parquet_path,
            'output_file': output_path,
            'release': release,
            'organism': organism_dir,
            'reason': 'already_processed'
        }
    
    # Run feature preprocessing
    result = run_singularity_feature_preprocessing(parquet_path, array_task_id)
    result['release'] = release
    result['organism'] = organism_dir
    
    return result


def main():
    """
    Main feature preprocessing function
    """
    # Check for Slurm array task
    task_id = os.environ.get('SLURM_ARRAY_TASK_ID')
    array_size = os.environ.get('SLURM_ARRAY_TASK_COUNT')
    
    if task_id:
        task_id = int(task_id)
        logger = setup_feature_logging(task_id)
        logger.info(f"Running as Slurm array task {task_id}")
    else:
        logger = setup_feature_logging()
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
    
    # Check if SO embedding model exists
    if not os.path.exists(SO_MODEL_PATH):
        logger.error(f"SO embedding model not found: {SO_MODEL_PATH}")
        sys.exit(1)
    
    # Find all transcript parquet files
    parquet_files = find_transcript_parquet_files(config.DATA_DIR)
    
    if not parquet_files:
        logger.warning("No transcript parquet files found to process")
        return
    
    # Distribute work among array tasks
    if array_size > 1:
        # Split files among tasks
        files_per_task = len(parquet_files) // array_size
        remainder = len(parquet_files) % array_size
        
        if task_id < remainder:
            start_idx = task_id * (files_per_task + 1)
            end_idx = start_idx + files_per_task + 1
        else:
            start_idx = remainder * (files_per_task + 1) + (task_id - remainder) * files_per_task
            end_idx = start_idx + files_per_task
        
        my_files = parquet_files[start_idx:end_idx]
        logger.info(f"Task {task_id} processing {len(my_files)} files (indices {start_idx}-{end_idx})")
    else:
        my_files = parquet_files
        logger.info(f"Processing all {len(my_files)} files")
    
    # Process files
    results = []
    start_time = time.time()
    
    # Prepare tasks with array task ID
    tasks = [(parquet_path, release, organism, task_id) 
             for parquet_path, release, organism in my_files]
    
    # Process with limited parallelism (feature generation can be memory-intensive)
    with ThreadPoolExecutor(max_workers=MAX_PARALLEL_FEATURE_PROCESSING) as executor:
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
                if completed % 5 == 0 or completed == len(tasks):
                    elapsed = time.time() - start_time
                    rate = completed / elapsed if elapsed > 0 else 0
                    remaining = (len(tasks) - completed) / rate if rate > 0 else 0
                    logger.info(f"Progress: {completed}/{len(tasks)} files processed "
                              f"({rate:.2f} files/min, ~{remaining/60:.1f} hours remaining)")
            
            except Exception as e:
                logger.error(f"Task failed: {str(e)}")
                results.append({
                    'status': 'error',
                    'input_file': task[0],
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
    summary_file = os.path.join(FEATURE_LOG_DIR, f'feature_preprocess_summary_task_{task_id}.json')
    with open(summary_file, 'w') as f:
        json.dump(summary, f, indent=2)
    
    # Print summary
    logger.info("="*60)
    logger.info("FEATURE PREPROCESSING SUMMARY")
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
