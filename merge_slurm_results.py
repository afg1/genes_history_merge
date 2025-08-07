#!/usr/bin/env python3
"""
Merge results from Slurm array job tasks
"""

import os
import json
import glob
from datetime import datetime
import config

def merge_array_results():
    """Merge summary files from all array tasks"""
    
    # Find all task summary files
    summary_pattern = os.path.join(config.LOG_DIR, 'summary_task_*.json')
    summary_files = glob.glob(summary_pattern)
    
    if not summary_files:
        print("No task summary files found. Make sure the array jobs have completed.")
        return
    
    print(f"Found {len(summary_files)} task summary files")
    
    # Merge results
    merged = {
        'merge_time': datetime.now().isoformat(),
        'tasks': [],
        'all_releases': set(),
        'all_organisms': 0,
        'total_downloads': 0,
        'statistics': {
            'successful': 0,
            'not_found': 0,
            'failed': 0
        },
        'by_release': {},
        'by_organism': {}
    }
    
    for summary_file in sorted(summary_files):
        print(f"Processing {summary_file}...")
        
        with open(summary_file, 'r') as f:
            task_data = json.load(f)
        
        # Add task info
        merged['tasks'].append({
            'task_id': task_data.get('task_id'),
            'releases': task_data.get('releases'),
            'statistics': task_data.get('statistics')
        })
        
        # Update releases
        for release in task_data.get('releases', []):
            merged['all_releases'].add(release)
        
        # Update organism count (will be same for all tasks)
        merged['all_organisms'] = task_data.get('total_organisms', 0)
        
        # Aggregate statistics
        stats = task_data.get('statistics', {})
        merged['statistics']['successful'] += stats.get('successful', 0)
        merged['statistics']['not_found'] += stats.get('not_found', 0)
        merged['statistics']['failed'] += stats.get('failed', 0)
        
        # Process results for detailed analysis
        for result in task_data.get('results', []):
            organism = result.get('organism')
            release = result.get('release')
            status = result.get('status')
            
            # By release
            if release not in merged['by_release']:
                merged['by_release'][release] = {
                    'successful': 0,
                    'not_found': 0,
                    'failed': 0
                }
            
            if status == 'success':
                merged['by_release'][release]['successful'] += 1
            elif status == 'not_found':
                merged['by_release'][release]['not_found'] += 1
            else:
                merged['by_release'][release]['failed'] += 1
            
            # By organism
            if organism not in merged['by_organism']:
                merged['by_organism'][organism] = {
                    'releases_found': [],
                    'releases_missing': [],
                    'releases_failed': []
                }
            
            if status == 'success':
                merged['by_organism'][organism]['releases_found'].append(release)
            elif status == 'not_found':
                merged['by_organism'][organism]['releases_missing'].append(release)
            else:
                merged['by_organism'][organism]['releases_failed'].append(release)
    
    # Convert set to sorted list
    merged['all_releases'] = sorted(list(merged['all_releases']))
    merged['total_downloads'] = sum(merged['statistics'].values())
    
    # Save merged results
    output_file = os.path.join(config.LOG_DIR, 'merged_summary.json')
    with open(output_file, 'w') as f:
        json.dump(merged, f, indent=2)
    
    # Print summary
    print("\n" + "="*70)
    print("MERGED RESULTS SUMMARY")
    print("="*70)
    print(f"Total tasks processed: {len(summary_files)}")
    print(f"Releases covered: {min(merged['all_releases'])} - {max(merged['all_releases'])}")
    print(f"Total organisms: {merged['all_organisms']}")
    print(f"Total download attempts: {merged['total_downloads']}")
    print("\nDownload Statistics:")
    print(f"  Successful: {merged['statistics']['successful']:,}")
    print(f"  Not found: {merged['statistics']['not_found']:,}")
    print(f"  Failed: {merged['statistics']['failed']:,}")
    print(f"\nSuccess rate: {merged['statistics']['successful']/merged['total_downloads']*100:.1f}%")
    
    print("\nPer-release breakdown:")
    for release in sorted(merged['by_release'].keys(), key=int):
        stats = merged['by_release'][release]
        total = sum(stats.values())
        success_rate = stats['successful'] / total * 100 if total > 0 else 0
        print(f"  Release {release}: {stats['successful']}/{total} successful ({success_rate:.1f}%)")
    
    # Find organisms with complete coverage
    complete_coverage = []
    partial_coverage = []
    no_coverage = []
    
    for organism, data in merged['by_organism'].items():
        found_count = len(data['releases_found'])
        total_releases = len(merged['all_releases'])
        
        if found_count == total_releases:
            complete_coverage.append(organism)
        elif found_count > 0:
            partial_coverage.append((organism, found_count))
        else:
            no_coverage.append(organism)
    
    print(f"\nOrganism coverage:")
    print(f"  Complete coverage (all {len(merged['all_releases'])} releases): {len(complete_coverage)} organisms")
    print(f"  Partial coverage: {len(partial_coverage)} organisms")
    print(f"  No data found: {len(no_coverage)} organisms")
    
    if complete_coverage:
        print(f"\nExample organisms with complete coverage:")
        for org in sorted(complete_coverage)[:5]:
            print(f"    - {org}")
    
    print(f"\nMerged summary saved to: {output_file}")
    print("="*70)

if __name__ == "__main__":
    merge_array_results()
