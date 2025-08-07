#!/usr/bin/env python3
"""
Analyze downloaded GFF files to show coverage across releases
"""

import os
import json
from pathlib import Path
from collections import defaultdict
import config

def analyze_downloads():
    """Analyze which organisms have files in which releases"""
    
    if not os.path.exists(config.DATA_DIR):
        print("No data directory found. Run the download script first.")
        return None
    
    # Build coverage matrix
    organism_releases = defaultdict(set)
    release_organisms = defaultdict(set)
    
    # Scan data directory
    for release_dir in os.listdir(config.DATA_DIR):
        if not release_dir.startswith('release_'):
            continue
            
        release_num = release_dir.replace('release_', '')
        release_path = os.path.join(config.DATA_DIR, release_dir)
        
        if os.path.isdir(release_path):
            for organism_dir in os.listdir(release_path):
                organism_path = os.path.join(release_path, organism_dir)
                
                if os.path.isdir(organism_path):
                    # Check for actual GFF files
                    gff_files = [f for f in os.listdir(organism_path) 
                               if f.endswith('.gff3') or f.endswith('.gff3.gz')]
                    
                    if gff_files:
                        organism_releases[organism_dir].add(release_num)
                        release_organisms[release_num].add(organism_dir)
    
    return organism_releases, release_organisms

def print_coverage_report(organism_releases, release_organisms):
    """Print a coverage report"""
    
    print("RNAcentral GFF Coverage Analysis")
    print("="*70)
    
    # Overall statistics
    total_organisms = len(organism_releases)
    total_releases = len(release_organisms)
    
    print(f"\nTotal unique organisms with data: {total_organisms}")
    print(f"Total releases with data: {total_releases}")
    
    if not organism_releases:
        print("\nNo data found. Please run the download script first.")
        return
    
    # Releases summary
    print("\n" + "-"*70)
    print("Releases Summary:")
    print("-"*70)
    
    sorted_releases = sorted(release_organisms.keys(), key=lambda x: int(x))
    for release in sorted_releases:
        count = len(release_organisms[release])
        print(f"Release {release}: {count} organisms")
    
    # Coverage distribution
    print("\n" + "-"*70)
    print("Coverage Distribution:")
    print("-"*70)
    
    coverage_dist = defaultdict(int)
    for organism, releases in organism_releases.items():
        coverage_dist[len(releases)] += 1
    
    for num_releases in sorted(coverage_dist.keys()):
        count = coverage_dist[num_releases]
        percentage = (count / total_organisms) * 100
        print(f"{count} organisms ({percentage:.1f}%) have data in {num_releases} release(s)")
    
    # Organisms with complete coverage
    all_releases = set(release_organisms.keys())
    complete_coverage = [org for org, rels in organism_releases.items() 
                        if rels == all_releases]
    
    if complete_coverage:
        print("\n" + "-"*70)
        print(f"Organisms with complete coverage (all {len(all_releases)} releases):")
        print("-"*70)
        for org in sorted(complete_coverage)[:10]:  # Show first 10
            print(f"  - {org}")
        if len(complete_coverage) > 10:
            print(f"  ... and {len(complete_coverage) - 10} more")
    
    # Organisms with sparse coverage
    sparse_coverage = [(org, rels) for org, rels in organism_releases.items() 
                      if len(rels) == 1]
    
    if sparse_coverage:
        print("\n" + "-"*70)
        print("Organisms with data in only 1 release:")
        print("-"*70)
        for org, rels in sorted(sparse_coverage)[:10]:  # Show first 10
            release = list(rels)[0]
            print(f"  - {org} (release {release})")
        if len(sparse_coverage) > 10:
            print(f"  ... and {len(sparse_coverage) - 10} more")
    
    # Export detailed report
    report_file = os.path.join(config.LOG_DIR, 'coverage_report.json')
    report_data = {
        'summary': {
            'total_organisms': total_organisms,
            'total_releases': total_releases,
            'releases': sorted_releases
        },
        'organism_coverage': {org: sorted(list(rels)) 
                             for org, rels in organism_releases.items()},
        'release_counts': {rel: len(orgs) 
                          for rel, orgs in release_organisms.items()}
    }
    
    os.makedirs(config.LOG_DIR, exist_ok=True)
    with open(report_file, 'w') as f:
        json.dump(report_data, f, indent=2, sort_keys=True)
    
    print("\n" + "-"*70)
    print(f"Detailed report saved to: {report_file}")

def main():
    result = analyze_downloads()
    if result:
        organism_releases, release_organisms = result
        print_coverage_report(organism_releases, release_organisms)

if __name__ == "__main__":
    main()
