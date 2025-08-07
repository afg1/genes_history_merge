#!/usr/bin/env python3
"""
Retry failed downloads based on merged summary
"""

import os
import json
import sys
from pathlib import Path
import config
from slurm_fetch_parallel import download_and_process_file, transform_organism_name, setup_logging

def get_failed_downloads():
    """Extract failed downloads from merged summary"""
    
    summary_file = os.path.join(config.LOG_DIR, 'merged_summary.json')
    
    if not os.path.exists(summary_file):
        print("No merged summary found. Run merge_slurm_results.py first.")
        return None
    
    with open(summary_file, 'r') as f:
        data = json.load(f)
    
    failed_tasks = []
    
    for organism, info in data.get('by_organism', {}).items():
        # Get failed releases
        for release in info.get('releases_failed', []):
            failed_tasks.append({
                'organism': organism,
                'release': release
            })
    
    return failed_tasks

def retry_downloads(max_retries=3):
    """Retry all failed downloads"""
    
    logger = setup_logging()
    logger.info("Starting retry of failed downloads")
    
    failed_tasks = get_failed_downloads()
    
    if not failed_tasks:
        print("No failed downloads to retry!")
        return
    
    print(f"Found {len(failed_tasks)} failed downloads to retry")
    print("-" * 60)
    
    results = {
        'retried': len(failed_tasks),
        'successful': 0,
        'still_failed': 0,
        'not_found': 0,
        'details': []
    }
    
    for i, task in enumerate(failed_tasks, 1):
        organism = task['organism']
        release = task['release']
        
        print(f"[{i}/{len(failed_tasks)}] Retrying {organism} (release {release})...")
        
        # Generate output path
        transformed_name = transform_organism_name(organism)
        output_dir = os.path.join(config.DATA_DIR, f"release_{release}", transformed_name)
        output_filename = f"{transformed_name}.gff3.gz"
        output_path = os.path.join(output_dir, output_filename)
        
        # Retry download with specified number of retries
        attempt = 0
        success = False
        last_result = None
        
        while attempt < max_retries and not success:
            attempt += 1
            logger.info(f"Retry attempt {attempt}/{max_retries} for {organism} release {release}")
            
            result = download_and_process_file(organism, None, release, output_path)
            last_result = result
            
            if result['status'] == 'success':
                success = True
                results['successful'] += 1
                logger.info(f"Successfully downloaded {organism} release {release} on retry")
                break
            elif result['status'] == 'not_found':
                # No point retrying if file doesn't exist
                results['not_found'] += 1
                logger.info(f"File not found for {organism} release {release}")
                break
            else:
                if attempt < max_retries:
                    logger.warning(f"Retry {attempt} failed for {organism} release {release}, trying again...")
                    import time
                    time.sleep(5)  # Wait before next retry
        
        if not success and last_result and last_result['status'] != 'not_found':
            results['still_failed'] += 1
            logger.error(f"Failed to download {organism} release {release} after {max_retries} retries")
        
        results['details'].append({
            'organism': organism,
            'release': release,
            'final_status': last_result['status'] if last_result else 'unknown',
            'attempts': attempt
        })
    
    # Save retry results
    retry_summary_file = os.path.join(config.LOG_DIR, 'retry_summary.json')
    with open(retry_summary_file, 'w') as f:
        json.dump(results, f, indent=2)
    
    # Print summary
    print("\n" + "=" * 60)
    print("RETRY SUMMARY")
    print("=" * 60)
    print(f"Total retried: {results['retried']}")
    print(f"Successful: {results['successful']}")
    print(f"Not found: {results['not_found']}")
    print(f"Still failed: {results['still_failed']}")
    
    if results['successful'] > 0:
        success_rate = (results['successful'] / results['retried']) * 100
        print(f"Success rate: {success_rate:.1f}%")
    
    print(f"\nDetailed results saved to: {retry_summary_file}")
    
    return results

def main():
    """Main function"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Retry failed RNAcentral GFF downloads')
    parser.add_argument('--max-retries', type=int, default=3,
                        help='Maximum number of retry attempts per file (default: 3)')
    parser.add_argument('--list-only', action='store_true',
                        help='Only list failed downloads without retrying')
    
    args = parser.parse_args()
    
    if args.list_only:
        failed_tasks = get_failed_downloads()
        if not failed_tasks:
            print("No failed downloads found!")
        else:
            print(f"Found {len(failed_tasks)} failed downloads:")
            print("-" * 60)
            for task in failed_tasks:
                print(f"  - {task['organism']} (release {task['release']})")
    else:
        retry_downloads(max_retries=args.max_retries)

if __name__ == "__main__":
    main()
