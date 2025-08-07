#!/usr/bin/env python3
"""
Main script to fetch RNAcentral GFF files for multiple organisms and releases
"""

import os
import sys
import json
import logging
import subprocess
import gzip
import shutil
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Tuple, Dict, Optional
import psycopg2
from psycopg2.extras import RealDictCursor
import time
import re
from pathlib import Path

import config

# Set up logging
def setup_logging():
    """Configure logging for the application"""
    # Create log directory if it doesn't exist
    os.makedirs(config.LOG_DIR, exist_ok=True)
    
    # Configure root logger
    logging.basicConfig(
        level=getattr(logging, config.LOG_LEVEL),
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(config.MAIN_LOG_FILE),
            logging.StreamHandler(sys.stdout)
        ]
    )
    
    # Create error file handler
    error_handler = logging.FileHandler(config.ERROR_LOG_FILE)
    error_handler.setLevel(logging.ERROR)
    error_handler.setFormatter(logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    ))
    logging.getLogger().addHandler(error_handler)
    
    return logging.getLogger(__name__)


def get_organisms_from_db() -> List[Dict[str, any]]:
    """
    Query the database to get list of organisms with their taxids
    """
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
    """
    Transform organism name to match RNAcentral URL format
    Examples:
        "Felis catus" -> "felis_catus"
        "Homo sapiens" -> "homo_sapiens"
        "Escherichia coli K-12" -> "escherichia_coli_k_12"
    """
    # Convert to lowercase
    transformed = name.lower()
    
    # Replace spaces and special characters with underscores
    transformed = re.sub(r'[^a-z0-9]+', '_', transformed)
    
    # Remove leading/trailing underscores
    transformed = transformed.strip('_')
    
    return transformed


def list_available_files(release: int) -> Optional[List[str]]:
    """
    Try to list available GFF files for a release by fetching the directory listing
    Returns list of available filenames or None if listing fails
    """
    logger = logging.getLogger(__name__)
    
    # Construct URL to the GFF directory
    list_url = f"{config.FTP_BASE_URL}/{release}.0/genome_coordinates/gff3/"
    
    try:
        # Try to fetch directory listing using curl
        result = subprocess.run(
            ['curl', '-s', '--list-only', list_url],
            capture_output=True,
            text=True,
            timeout=30
        )
        
        if result.returncode == 0 and result.stdout:
            # Parse the file listing
            files = [line.strip() for line in result.stdout.splitlines() 
                    if line.strip().endswith('.gff3.gz')]
            logger.info(f"Found {len(files)} GFF files in release {release}")
            return files
        else:
            logger.warning(f"Could not list files for release {release}")
            return None
            
    except Exception as e:
        logger.warning(f"Failed to list files for release {release}: {str(e)}")
        return None


def generate_download_url(organism_name: str, release: int, available_files: Optional[List[str]] = None) -> Optional[str]:
    """
    Generate download URL for an organism and release
    If available_files is provided, tries to find matching file
    """
    transformed_name = transform_organism_name(organism_name)
    
    if available_files:
        # Try to find a matching file in the available files list
        matching_files = [f for f in available_files if f.startswith(transformed_name + '.')]
        if matching_files:
            # Use the first matching file
            filename = matching_files[0]
            url = f"{config.FTP_BASE_URL}/{release}.0/genome_coordinates/gff3/{filename}"
            return url
    
    # If no file list or no match, generate URL with pattern
    # We'll try without assembly version first - the download script will handle 404s
    filename = f"{transformed_name}.*.gff3.gz"
    url = f"{config.FTP_BASE_URL}/{release}.0/genome_coordinates/gff3/"
    
    # Return base URL and name for pattern matching attempt
    return (url, transformed_name)


def download_single_file(task: Dict) -> Dict:
    """
    Download a single GFF file using the bash script
    """
    logger = logging.getLogger(__name__)
    
    organism = task['organism']
    release = task['release']
    url_info = task['url_info']
    output_path = task['output_path']
    
    # Create output directory
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    
    if isinstance(url_info, tuple):
        # Need to find the actual file
        base_url, organism_prefix = url_info
        
        # Try to get file listing and find matching file
        try:
            result = subprocess.run(
                ['curl', '-s', base_url],
                capture_output=True,
                text=True,
                timeout=30
            )
            
            if result.returncode == 0:
                # Parse HTML to find matching files
                import re
                pattern = rf'href="({organism_prefix}\.[^"]+\.gff3\.gz)"'
                matches = re.findall(pattern, result.stdout)
                
                if matches:
                    # Use first match
                    url = base_url + matches[0]
                else:
                    return {
                        'organism': organism,
                        'release': release,
                        'status': 'not_found',
                        'message': f'No matching file found for {organism_prefix}'
                    }
            else:
                return {
                    'organism': organism,
                    'release': release,
                    'status': 'error',
                    'message': 'Failed to list directory'
                }
                
        except Exception as e:
            return {
                'organism': organism,
                'release': release,
                'status': 'error',
                'message': str(e)
            }
    else:
        url = url_info
    
    # Run download script
    try:
        result = subprocess.run(
            ['./download_gff.sh', url, output_path, str(config.MAX_RETRIES), str(config.DOWNLOAD_TIMEOUT)],
            capture_output=True,
            text=True,
            timeout=config.DOWNLOAD_TIMEOUT + 30  # Add buffer to script timeout
        )
        
        if result.returncode == 0:
            logger.info(f"Successfully downloaded: {organism} (release {release})")
            return {
                'organism': organism,
                'release': release,
                'status': 'success',
                'path': output_path,
                'url': url
            }
        elif result.returncode == 8:
            logger.debug(f"File not found: {organism} (release {release})")
            return {
                'organism': organism,
                'release': release,
                'status': 'not_found',
                'message': 'File not available in this release'
            }
        else:
            logger.error(f"Download failed: {organism} (release {release})")
            return {
                'organism': organism,
                'release': release,
                'status': 'failed',
                'message': result.stderr or 'Download failed'
            }
            
    except subprocess.TimeoutExpired:
        logger.error(f"Download timeout: {organism} (release {release})")
        return {
            'organism': organism,
            'release': release,
            'status': 'timeout',
            'message': 'Download exceeded timeout'
        }
    except Exception as e:
        logger.error(f"Download error for {organism} (release {release}): {str(e)}")
        return {
            'organism': organism,
            'release': release,
            'status': 'error',
            'message': str(e)
        }


def decompress_file(filepath: str) -> bool:
    """
    Decompress a .gz file
    """
    logger = logging.getLogger(__name__)
    
    try:
        output_path = filepath[:-3]  # Remove .gz extension
        
        with gzip.open(filepath, 'rb') as f_in:
            with open(output_path, 'wb') as f_out:
                shutil.copyfileobj(f_in, f_out)
        
        logger.info(f"Decompressed: {filepath}")
        
        # Optionally remove compressed file to save space
        # os.remove(filepath)
        
        return True
        
    except Exception as e:
        logger.error(f"Failed to decompress {filepath}: {str(e)}")
        return False


def main():
    """
    Main orchestration function
    """
    logger = setup_logging()
    logger.info("="*60)
    logger.info("Starting RNAcentral GFF download process")
    logger.info("="*60)
    
    # Create data directory
    os.makedirs(config.DATA_DIR, exist_ok=True)
    
    # Initialize summary
    summary = {
        'start_time': datetime.now().isoformat(),
        'config': {
            'releases': f"{config.RELEASE_START}-{config.RELEASE_END}",
            'parallel_downloads': config.MAX_PARALLEL_DOWNLOADS
        },
        'organisms': [],
        'downloads': [],
        'statistics': {
            'total_organisms': 0,
            'total_tasks': 0,
            'successful': 0,
            'not_found': 0,
            'failed': 0,
            'decompressed': 0
        }
    }
    
    try:
        # Step 1: Get organisms from database
        logger.info("Fetching organisms from database...")
        organisms = get_organisms_from_db()
        summary['statistics']['total_organisms'] = len(organisms)
        summary['organisms'] = [{'taxid': o['taxid'], 'name': o['organism_name']} for o in organisms]
        
        # Step 2: Generate download tasks
        logger.info("Generating download tasks...")
        download_tasks = []
        
        for release in range(config.RELEASE_START, config.RELEASE_END + 1):
            logger.info(f"Preparing release {release}...")
            
            # Try to get file listing for this release
            available_files = list_available_files(release)
            
            for organism in organisms:
                organism_name = organism['organism_name']
                taxid = organism['taxid']
                
                # Generate URL
                url_info = generate_download_url(organism_name, release, available_files)
                
                if url_info:
                    # Generate output path
                    transformed_name = transform_organism_name(organism_name)
                    output_dir = os.path.join(config.DATA_DIR, f"release_{release}", transformed_name)
                    output_filename = f"{transformed_name}.gff3.gz"
                    output_path = os.path.join(output_dir, output_filename)
                    
                    task = {
                        'organism': organism_name,
                        'taxid': taxid,
                        'release': release,
                        'url_info': url_info,
                        'output_path': output_path
                    }
                    download_tasks.append(task)
        
        summary['statistics']['total_tasks'] = len(download_tasks)
        logger.info(f"Created {len(download_tasks)} download tasks")
        
        # Step 3: Execute downloads in parallel
        logger.info(f"Starting downloads with {config.MAX_PARALLEL_DOWNLOADS} parallel workers...")
        
        with ThreadPoolExecutor(max_workers=config.MAX_PARALLEL_DOWNLOADS) as executor:
            # Submit all tasks
            future_to_task = {executor.submit(download_single_file, task): task 
                             for task in download_tasks}
            
            # Process completed tasks
            completed = 0
            for future in as_completed(future_to_task):
                completed += 1
                task = future_to_task[future]
                
                try:
                    result = future.result()
                    summary['downloads'].append(result)
                    
                    # Update statistics
                    if result['status'] == 'success':
                        summary['statistics']['successful'] += 1
                    elif result['status'] == 'not_found':
                        summary['statistics']['not_found'] += 1
                    else:
                        summary['statistics']['failed'] += 1
                    
                    # Progress update
                    if completed % 10 == 0:
                        logger.info(f"Progress: {completed}/{len(download_tasks)} tasks completed")
                        
                except Exception as e:
                    logger.error(f"Task failed: {str(e)}")
                    summary['statistics']['failed'] += 1
        
        logger.info("All downloads completed")
        
        # Step 4: Decompress successful downloads
        logger.info("Decompressing downloaded files...")
        successful_downloads = [d for d in summary['downloads'] if d['status'] == 'success']
        
        for download in successful_downloads:
            if decompress_file(download['path']):
                summary['statistics']['decompressed'] += 1
        
        # Step 5: Generate summary
        summary['end_time'] = datetime.now().isoformat()
        
        # Save summary to file
        with open(config.SUMMARY_FILE, 'w') as f:
            json.dump(summary, f, indent=2)
        
        # Print summary
        logger.info("="*60)
        logger.info("DOWNLOAD SUMMARY")
        logger.info("="*60)
        logger.info(f"Total organisms: {summary['statistics']['total_organisms']}")
        logger.info(f"Total download tasks: {summary['statistics']['total_tasks']}")
        logger.info(f"Successful downloads: {summary['statistics']['successful']}")
        logger.info(f"Files not found: {summary['statistics']['not_found']}")
        logger.info(f"Failed downloads: {summary['statistics']['failed']}")
        logger.info(f"Files decompressed: {summary['statistics']['decompressed']}")
        logger.info(f"Summary saved to: {config.SUMMARY_FILE}")
        
    except Exception as e:
        logger.error(f"Process failed: {str(e)}")
        raise


if __name__ == "__main__":
    main()
