#!/usr/bin/env python3
"""
Monitor download progress and statistics
"""

import os
import json
import sys
from datetime import datetime
from pathlib import Path
import config

def get_directory_stats(path):
    """Get statistics for a directory"""
    if not os.path.exists(path):
        return {'files': 0, 'size': 0}
    
    total_size = 0
    file_count = 0
    
    for root, dirs, files in os.walk(path):
        for file in files:
            filepath = os.path.join(root, file)
            if os.path.exists(filepath):
                total_size += os.path.getsize(filepath)
                file_count += 1
    
    return {'files': file_count, 'size': total_size}

def format_bytes(bytes):
    """Format bytes to human readable string"""
    for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
        if bytes < 1024.0:
            return f"{bytes:.2f} {unit}"
        bytes /= 1024.0
    return f"{bytes:.2f} PB"

def main():
    print("RNAcentral GFF Download Monitor")
    print("="*60)
    
    # Check if download is running
    if os.path.exists(config.MAIN_LOG_FILE):
        # Get last modified time
        log_mtime = os.path.getmtime(config.MAIN_LOG_FILE)
        log_age = datetime.now().timestamp() - log_mtime
        
        if log_age < 60:  # Log updated in last minute
            print("✓ Download process appears to be running")
        else:
            print("⚠ Download process may not be running (log not recently updated)")
    else:
        print("No log file found - download may not have started")
    
    print("-"*60)
    
    # Check data directory
    if os.path.exists(config.DATA_DIR):
        print("\nData Directory Statistics:")
        print("-"*60)
        
        # Overall stats
        overall_stats = get_directory_stats(config.DATA_DIR)
        print(f"Total files: {overall_stats['files']:,}")
        print(f"Total size: {format_bytes(overall_stats['size'])}")
        print()
        
        # Per-release stats
        releases = sorted([d for d in os.listdir(config.DATA_DIR) 
                          if d.startswith('release_') and os.path.isdir(os.path.join(config.DATA_DIR, d))])
        
        if releases:
            print("Per-release breakdown:")
            for release_dir in releases:
                release_path = os.path.join(config.DATA_DIR, release_dir)
                stats = get_directory_stats(release_path)
                
                # Count organisms
                organisms = [d for d in os.listdir(release_path) 
                           if os.path.isdir(os.path.join(release_path, d))]
                
                print(f"  {release_dir}: {len(organisms)} organisms, "
                      f"{stats['files']} files, {format_bytes(stats['size'])}")
    else:
        print("Data directory not found")
    
    print("-"*60)
    
    # Check summary file
    if os.path.exists(config.SUMMARY_FILE):
        print("\nDownload Summary:")
        print("-"*60)
        
        with open(config.SUMMARY_FILE, 'r') as f:
            summary = json.load(f)
        
        stats = summary.get('statistics', {})
        print(f"Total organisms: {stats.get('total_organisms', 'N/A')}")
        print(f"Total tasks: {stats.get('total_tasks', 'N/A')}")
        print(f"Successful: {stats.get('successful', 0)}")
        print(f"Not found: {stats.get('not_found', 0)}")
        print(f"Failed: {stats.get('failed', 0)}")
        print(f"Decompressed: {stats.get('decompressed', 0)}")
        
        if 'start_time' in summary:
            print(f"\nStart time: {summary['start_time']}")
        if 'end_time' in summary:
            print(f"End time: {summary['end_time']}")
            
            # Calculate duration
            try:
                start = datetime.fromisoformat(summary['start_time'])
                end = datetime.fromisoformat(summary['end_time'])
                duration = end - start
                print(f"Duration: {duration}")
            except:
                pass
    else:
        print("No summary file found")
    
    print("="*60)
    
    # Recent log entries
    if os.path.exists(config.MAIN_LOG_FILE):
        print("\nRecent log entries:")
        print("-"*60)
        
        # Get last 10 lines of log
        with open(config.MAIN_LOG_FILE, 'r') as f:
            lines = f.readlines()
            recent_lines = lines[-10:] if len(lines) > 10 else lines
            
            for line in recent_lines:
                print(line.rstrip())

if __name__ == "__main__":
    main()
