#!/usr/bin/env python3
"""
Monitor preprocessing progress and check for generated genes.json files
"""

import os
import json
import glob
from datetime import datetime
from collections import defaultdict
import config

PREPROCESSING_LOG_DIR = os.path.join(config.LOG_DIR, "preprocessing")

def count_files_by_pattern(base_dir, pattern):
    """Count files matching a pattern recursively"""
    files = glob.glob(os.path.join(base_dir, "**", pattern), recursive=True)
    return len(files)

def analyze_preprocessing_progress():
    """Analyze preprocessing progress by checking for output files"""
    
    if not os.path.exists(config.DATA_DIR):
        print("Data directory not found")
        return None
    
    stats = {
        'by_release': defaultdict(lambda: {'gff': 0, 'processed': 0, 'organisms': set()}),
        'by_organism': defaultdict(lambda: {'releases': [], 'processed': []}),
        'total_gff': 0,
        'total_processed': 0,
        'total_organisms': set()
    }
    
    # Scan data directory
    for release_dir in sorted(os.listdir(config.DATA_DIR)):
        if not release_dir.startswith('release_'):
            continue
        
        release_num = release_dir.replace('release_', '')
        release_path = os.path.join(config.DATA_DIR, release_dir)
        
        for organism_dir in os.listdir(release_path):
            organism_path = os.path.join(release_path, organism_dir)
            
            if not os.path.isdir(organism_path):
                continue
            
            # Count GFF files
            gff_files = glob.glob(os.path.join(organism_path, "*.gff3"))
            genes_files = glob.glob(os.path.join(organism_path, "*.genes.json"))
            
            if gff_files:
                stats['by_release'][release_num]['gff'] += len(gff_files)
                stats['by_release'][release_num]['organisms'].add(organism_dir)
                stats['by_organism'][organism_dir]['releases'].append(release_num)
                stats['total_gff'] += len(gff_files)
                stats['total_organisms'].add(organism_dir)
                
                if genes_files:
                    stats['by_release'][release_num]['processed'] += len(genes_files)
                    stats['by_organism'][organism_dir]['processed'].append(release_num)
                    stats['total_processed'] += len(genes_files)
    
    return stats

def check_preprocessing_logs():
    """Check preprocessing log files for errors"""
    
    if not os.path.exists(PREPROCESSING_LOG_DIR):
        return None
    
    log_stats = {
        'summary_files': [],
        'total_success': 0,
        'total_failed': 0,
        'total_timeout': 0,
        'errors': []
    }
    
    # Find summary files
    summary_files = glob.glob(os.path.join(PREPROCESSING_LOG_DIR, "preprocess_summary_*.json"))
    
    for summary_file in summary_files:
        try:
            with open(summary_file, 'r') as f:
                data = json.load(f)
            
            log_stats['summary_files'].append(os.path.basename(summary_file))
            
            if 'statistics' in data:
                stats = data['statistics']
                log_stats['total_success'] += stats.get('successful', 0)
                log_stats['total_failed'] += stats.get('failed', 0)
                log_stats['total_timeout'] += stats.get('timeout', 0)
                
                # Extract errors
                for result in data.get('results', []):
                    if result.get('status') in ['failed', 'error', 'timeout']:
                        log_stats['errors'].append({
                            'file': result.get('gff_file', 'unknown'),
                            'status': result.get('status'),
                            'error': result.get('error', 'No error message')[:200]  # Truncate long errors
                        })
        
        except Exception as e:
            print(f"Error reading {summary_file}: {str(e)}")
    
    return log_stats

def main():
    """Main monitoring function"""
    
    print("="*70)
    print("PREPROCESSING PROGRESS MONITOR")
    print("="*70)
    print(f"Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("-"*70)
    
    # Analyze file progress
    stats = analyze_preprocessing_progress()
    
    if stats:
        print("\nOVERALL PROGRESS")
        print("-"*70)
        print(f"Total GFF files: {stats['total_gff']}")
        print(f"Total processed (genes.json created): {stats['total_processed']}")
        print(f"Total organisms: {len(stats['total_organisms'])}")
        
        if stats['total_gff'] > 0:
            progress_pct = (stats['total_processed'] / stats['total_gff']) * 100
            print(f"Overall progress: {progress_pct:.1f}%")
            print(f"Remaining: {stats['total_gff'] - stats['total_processed']} files")
        
        print("\nPROGRESS BY RELEASE")
        print("-"*70)
        print(f"{'Release':<10} {'GFF Files':<12} {'Processed':<12} {'Progress':<12} {'Organisms':<12}")
        print("-"*70)
        
        for release in sorted(stats['by_release'].keys(), key=int):
            release_stats = stats['by_release'][release]
            gff_count = release_stats['gff']
            processed = release_stats['processed']
            organism_count = len(release_stats['organisms'])
            
            if gff_count > 0:
                progress = (processed / gff_count) * 100
                progress_str = f"{progress:.1f}%"
            else:
                progress_str = "N/A"
            
            print(f"{release:<10} {gff_count:<12} {processed:<12} {progress_str:<12} {organism_count:<12}")
        
        # Find organisms with incomplete processing
        incomplete = []
        complete = []
        
        for organism, org_stats in stats['by_organism'].items():
            total_releases = len(org_stats['releases'])
            processed_releases = len(org_stats['processed'])
            
            if processed_releases == total_releases and total_releases > 0:
                complete.append(organism)
            elif processed_releases < total_releases:
                incomplete.append((organism, processed_releases, total_releases))
        
        if complete:
            print(f"\nCOMPLETELY PROCESSED ORGANISMS: {len(complete)}")
            print("-"*70)
            for i, organism in enumerate(sorted(complete)[:5]):
                print(f"  {organism}")
            if len(complete) > 5:
                print(f"  ... and {len(complete) - 5} more")
        
        if incomplete:
            print(f"\nINCOMPLETE ORGANISMS: {len(incomplete)}")
            print("-"*70)
            print(f"{'Organism':<40} {'Processed':<12} {'Total':<12}")
            for organism, processed, total in sorted(incomplete)[:10]:
                print(f"{organism:<40} {processed:<12} {total:<12}")
            if len(incomplete) > 10:
                print(f"... and {len(incomplete) - 10} more")
    
    else:
        print("No data found in data directory")
    
    # Check preprocessing logs
    log_stats = check_preprocessing_logs()
    
    if log_stats:
        print("\nPREPROCESSING LOG SUMMARY")
        print("-"*70)
        print(f"Summary files found: {len(log_stats['summary_files'])}")
        print(f"Total successful: {log_stats['total_success']}")
        print(f"Total failed: {log_stats['total_failed']}")
        print(f"Total timeout: {log_stats['total_timeout']}")
        
        if log_stats['errors']:
            print(f"\nRECENT ERRORS (showing first 5):")
            print("-"*70)
            for error in log_stats['errors'][:5]:
                print(f"File: {os.path.basename(error['file'])}")
                print(f"Status: {error['status']}")
                print(f"Error: {error['error']}")
                print("-"*40)
    
    # Check for running Slurm jobs
    print("\nSLURM JOB STATUS")
    print("-"*70)
    
    import subprocess
    try:
        result = subprocess.run(
            ['squeue', '-u', os.environ.get('USER', 'unknown'), '--name=preprocess_gff'],
            capture_output=True,
            text=True,
            timeout=5
        )
        
        if result.returncode == 0:
            lines = result.stdout.strip().split('\n')
            if len(lines) > 1:  # Header + at least one job
                print(f"Active preprocessing jobs: {len(lines) - 1}")
                print(result.stdout)
            else:
                print("No active preprocessing jobs")
        else:
            print("Could not query Slurm queue")
    
    except (subprocess.TimeoutExpired, FileNotFoundError):
        print("Slurm commands not available")
    
    print("="*70)

if __name__ == "__main__":
    main()
