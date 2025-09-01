#!/usr/bin/env python3
"""
Classify genes using a pre-trained model in a singularity container.
This script takes transcript and feature parquet files, runs them through a classification model,
and produces an output directory with the classified genes.
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

# Configuration
SINGULARITY_IMAGE = "/hps/nobackup/agb/rnacentral/genes-testing/gff2genes/rnacentral-rnacentral-import-pipeline-latest.sif"
SINGULARITY_ENV_PATH = "/hps/nobackup/agb/rnacentral/genes-testing/bin"
MODEL_PATH = "/hps/nobackup/agb/rnacentral/genes-testing/gff2genes/genes_rf_model.onnx"
CLASSIFICATION_LOG_DIR = os.path.join(config.LOG_DIR, "gene_classification")
MAX_PARALLEL_CLASSIFICATION = 4 # Can be higher than preprocessing as it's less memory intensive

def setup_classification_logging(task_id=None):
    """Configure logging for gene classification"""
    os.makedirs(CLASSIFICATION_LOG_DIR, exist_ok=True)

    if task_id is not None:
        log_file = os.path.join(CLASSIFICATION_LOG_DIR, f'classification_task_{task_id}.log')
    else:
        log_file = os.path.join(CLASSIFICATION_LOG_DIR, 'classification_main.log')

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


def find_files_for_classification(base_dir: str, task_id: int, release: Optional[int] = None) -> List[Tuple[str, str, int, str]]:
    """
    Find pairs of transcript and feature parquet files for classification.
    Returns: List of tuples (transcript_path, feature_path, release_number, organism_dir_name)
    """
    logger = logging.getLogger(__name__)
    file_pairs = []

    if release:
        release_dirs = [f"release_{release}"]
    else:
        release_dirs = [d for d in os.listdir(base_dir)
                       if d.startswith('release_') and os.path.isdir(os.path.join(base_dir, d))]

    for release_dir in sorted(release_dirs):
        release_num = int(release_dir.replace('release_', ''))
        release_path = os.path.join(base_dir, release_dir)

        for organism_dir in os.listdir(release_path):
            organism_path = os.path.join(release_path, organism_dir)

            if os.path.isdir(organism_path):
                # Find transcript files
                for transcript_file in glob.glob(os.path.join(organism_path, "*_transcripts.parquet")):
                    base_name = os.path.basename(transcript_file).replace('_transcripts.parquet', '')

                    # Construct expected feature file name
                    feature_file_pattern = f"{base_name}_{task_id}_features.parquet"
                    feature_file_path = os.path.join(organism_path, feature_file_pattern)

                    if os.path.exists(feature_file_path):
                        file_pairs.append((transcript_file, feature_file_path, release_num, organism_dir))
                    else:
                         # Fallback for filenames that might not have task_id
                        feature_file_pattern_no_task = f"{base_name}_features.parquet"
                        feature_file_path_no_task = os.path.join(organism_path, feature_file_pattern_no_task)
                        if os.path.exists(feature_file_path_no_task):
                             file_pairs.append((transcript_file, feature_file_path_no_task, release_num, organism_dir))
                        else:
                            logger.warning(f"Feature file not found for {transcript_file}")


    logger.info(f"Found {len(file_pairs)} pairs of files for classification")
    return file_pairs


def run_singularity_classification(transcript_path: str, feature_path: str, taxid: int, task_id: int) -> Dict:
    """
    Run singularity container to classify genes for a single organism.
    """
    logger = logging.getLogger(__name__)

    working_dir = os.path.dirname(transcript_path)
    transcript_filename = os.path.basename(transcript_path)
    feature_filename = os.path.basename(feature_path)

    output_dir = f"release_{task_id}_genes_output"
    output_path = os.path.join(working_dir, output_dir)

    # Set up environment
    env = os.environ.copy()
    env['SINGULARITYENV_APPEND_PATH'] = SINGULARITY_ENV_PATH

    # Build command
    cmd = [
        'singularity', 'exec',
        SINGULARITY_IMAGE,
        'rnac', 'genes', 'classify',
        '--transcripts_file', transcript_filename,
        '--features_file', feature_filename,
        '--taxid', str(taxid),
        '--output_dir', output_dir,
        '--model_path', MODEL_PATH
    ]

    logger.info(f"Classifying genes for {transcript_filename}")
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
            timeout=3600  # 1 hour timeout
        )

        if result.returncode == 0:
            logger.info(f"Successfully classified {transcript_filename}")
            return {
                'status': 'success',
                'transcript_file': transcript_path,
                'feature_file': feature_path,
                'output_dir': output_path,
                'stdout': result.stdout,
                'stderr': result.stderr
            }
        else:
            logger.error(f"Failed to classify {transcript_filename}: {result.stderr}")
            return {
                'status': 'failed',
                'transcript_file': transcript_path,
                'feature_file': feature_path,
                'error': result.stderr,
                'return_code': result.returncode
            }

    except subprocess.TimeoutExpired:
        logger.error(f"Timeout classifying {transcript_filename}")
        return {
            'status': 'timeout',
            'transcript_file': transcript_path,
            'feature_file': feature_path,
        }

    except Exception as e:
        logger.error(f"Error classifying {transcript_filename}: {str(e)}")
        return {
            'status': 'error',
            'transcript_file': transcript_path,
            'feature_file': feature_path,
            'error': str(e)
        }

def process_single_classification(task: Tuple[str, str, int, str, int]) -> Dict:
    """
    Process a single classification task.
    """
    transcript_path, feature_path, release, organism_dir, task_id = task

    # Get taxid
    taxid_mapping = get_organism_taxid_mapping()
    taxid = taxid_mapping.get(organism_dir)

    if not taxid:
        return {
            'status': 'error',
            'transcript_file': transcript_path,
            'error': f"Could not find taxid for organism {organism_dir}"
        }

    # Run classification
    result = run_singularity_classification(transcript_path, feature_path, taxid, task_id)
    result['release'] = release
    result['organism'] = organism_dir

    return result

def main():
    """
    Main gene classification function
    """
    task_id = os.environ.get('SLURM_ARRAY_TASK_ID')
    array_size = os.environ.get('SLURM_ARRAY_TASK_COUNT')

    if task_id:
        task_id = int(task_id)
        logger = setup_classification_logging(task_id)
        logger.info(f"Running as Slurm array task {task_id}")
    else:
        logger = setup_classification_logging()
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

    # Check if classification model exists
    if not os.path.exists(MODEL_PATH):
        logger.error(f"Classification model not found: {MODEL_PATH}")
        sys.exit(1)

    # Find all file pairs for classification
    file_pairs = find_files_for_classification(config.DATA_DIR, task_id)

    if not file_pairs:
        logger.warning("No transcript/feature file pairs found to process")
        return

    # Distribute work among array tasks
    if array_size > 1:
        files_per_task = len(file_pairs) // array_size
        remainder = len(file_pairs) % array_size

        if task_id < remainder:
            start_idx = task_id * (files_per_task + 1)
            end_idx = start_idx + files_per_task + 1
        else:
            start_idx = remainder * (files_per_task + 1) + (task_id - remainder) * files_per_task
            end_idx = start_idx + files_per_task

        my_files = file_pairs[start_idx:end_idx]
        logger.info(f"Task {task_id} processing {len(my_files)} file pairs (indices {start_idx}-{end_idx})")
    else:
        my_files = file_pairs
        logger.info(f"Processing all {len(my_files)} file pairs")

    # Process files
    results = []
    start_time = time.time()

    tasks = [(transcript_path, feature_path, release, organism, task_id)
             for transcript_path, feature_path, release, organism in my_files]

    with ThreadPoolExecutor(max_workers=MAX_PARALLEL_CLASSIFICATION) as executor:
        future_to_task = {executor.submit(process_single_classification, task): task
                         for task in tasks}

        completed = 0
        for future in as_completed(future_to_task):
            completed += 1
            task = future_to_task[future]

            try:
                result = future.result()
                results.append(result)

                if completed % 5 == 0 or completed == len(tasks):
                    elapsed = time.time() - start_time
                    rate = completed / elapsed if elapsed > 0 else 0
                    remaining = (len(tasks) - completed) / rate if rate > 0 else 0
                    logger.info(f"Progress: {completed}/{len(tasks)} file pairs processed "
                              f"({rate:.2f} pairs/min, ~{remaining/60:.1f} hours remaining)")

            except Exception as e:
                logger.error(f"Task failed: {str(e)}")
                results.append({
                    'status': 'error',
                    'transcript_file': task[0],
                    'feature_file': task[1],
                    'error': str(e)
                })

    # Generate summary
    summary = {
        'task_id': task_id if task_id else 'main',
        'start_time': datetime.fromtimestamp(start_time).isoformat(),
        'end_time': datetime.now().isoformat(),
        'total_pairs': len(my_files),
        'processed': len(results),
        'statistics': {
            'successful': sum(1 for r in results if r.get('status') == 'success'),
            'failed': sum(1 for r in results if r.get('status') == 'failed'),
            'timeout': sum(1 for r in results if r.get('status') == 'timeout'),
            'error': sum(1 for r in results if r.get('status') == 'error')
        },
        'results': results
    }

    # Save summary
    summary_file = os.path.join(CLASSIFICATION_LOG_DIR, f'classification_summary_task_{task_id}.json')
    with open(summary_file, 'w') as f:
        json.dump(summary, f, indent=2)

    logger.info("="*60)
    logger.info("GENE CLASSIFICATION SUMMARY")
    logger.info("="*60)
    logger.info(f"Total file pairs: {summary['total_pairs']}")
    logger.info(f"Successful: {summary['statistics']['successful']}")
    logger.info(f"Failed: {summary['statistics']['failed']}")
    logger.info(f"Timeout: {summary['statistics']['timeout']}")
    logger.info(f"Errors: {summary['statistics']['error']}")
    logger.info(f"Summary saved to: {summary_file}")

    failure_rate = (summary['statistics']['failed'] + summary.get('statistics', {}).get('error', 0)) / len(my_files) if my_files else 0
    if failure_rate > 0.5:
        logger.error(f"High failure rate: {failure_rate:.1%}")
        sys.exit(1)

if __name__ == "__main__":
    main()
