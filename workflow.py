#!/usr/bin/env python3
"""
Complete workflow orchestrator for RNAcentral GFF processing pipeline
Manages the entire process from download to preprocessing
"""

import os
import sys
import argparse
import subprocess
import json
from datetime import datetime
from pathlib import Path

def run_command(cmd, description, check=True):
    """Run a command and handle output"""
    print(f"\n{'='*60}")
    print(f"Running: {description}")
    print(f"Command: {cmd}")
    print(f"{'='*60}")
    
    result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
    
    if result.stdout:
        print(result.stdout)
    
    if result.returncode != 0:
        print(f"Error: {result.stderr}")
        if check:
            sys.exit(1)
    
    return result.returncode == 0

def check_environment():
    """Check that all required environment variables are set"""
    print("\nChecking environment...")
    
    required_vars = {
        'PGDATABASE': 'PostgreSQL connection string',
        'USER': 'Username for Slurm commands'
    }
    
    missing = []
    for var, description in required_vars.items():
        if not os.environ.get(var):
            missing.append(f"  - {var}: {description}")
    
    if missing:
        print("Missing required environment variables:")
        for item in missing:
            print(item)
        return False
    
    print("✓ All required environment variables set")
    return True

def main():
    """Main workflow orchestrator"""
    
    parser = argparse.ArgumentParser(
        description='RNAcentral GFF Processing Pipeline',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Workflow stages:
  1. setup     - Install dependencies and test connections
  2. download  - Download GFF files from RNAcentral
  3. preprocess - Convert GFF files to genes.json format
  4. monitor   - Monitor progress of current operations
  5. analyze   - Analyze results and generate reports
  6. all       - Run complete pipeline (setup + download + preprocess)

Examples:
  # Run complete pipeline
  python workflow.py all
  
  # Run only download step
  python workflow.py download --slurm
  
  # Monitor current progress
  python workflow.py monitor
  
  # Run preprocessing after download
  python workflow.py preprocess --slurm
        """
    )
    
    parser.add_argument(
        'stage',
        choices=['setup', 'download', 'preprocess', 'monitor', 'analyze', 'all'],
        help='Pipeline stage to run'
    )
    
    parser.add_argument(
        '--slurm',
        action='store_true',
        help='Use Slurm for parallel processing (recommended for HPC)'
    )
    
    parser.add_argument(
        '--skip-tests',
        action='store_true',
        help='Skip test steps (not recommended)'
    )
    
    parser.add_argument(
        '--releases',
        type=str,
        help='Specify release range (e.g., "20-25")'
    )
    
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Show what would be done without executing'
    )
    
    args = parser.parse_args()
    
    print("="*70)
    print("RNACENTRAL GFF PROCESSING PIPELINE")
    print("="*70)
    print(f"Stage: {args.stage}")
    print(f"Mode: {'Slurm (parallel)' if args.slurm else 'Local (sequential)'}")
    print(f"Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("="*70)
    
    # Check environment
    if not check_environment():
        print("\nPlease set required environment variables and try again.")
        sys.exit(1)
    
    # Handle dry run
    if args.dry_run:
        print("\n[DRY RUN MODE - Commands will not be executed]")
    
    # Execute requested stage
    success = True
    
    if args.stage in ['all', 'setup']:
        print("\n" + "="*70)
        print("STAGE 1: SETUP")
        print("="*70)
        
        if not args.skip_tests:
            # Install dependencies
            if not args.dry_run:
                success = run_command(
                    "pip install -q -r requirements.txt",
                    "Installing Python dependencies",
                    check=False
                )
            
            # Test database connection
            if not args.dry_run:
                success = run_command(
                    "python test_setup.py",
                    "Testing database connection and download capability",
                    check=False
                )
            
            if not success:
                print("\n⚠ Setup tests failed. Please fix issues before continuing.")
                if args.stage == 'setup':
                    sys.exit(1)
    
    if args.stage in ['all', 'download']:
        print("\n" + "="*70)
        print("STAGE 2: DOWNLOAD")
        print("="*70)
        
        if args.slurm:
            # Submit Slurm array job for downloading
            cmd = "sbatch --parsable submit_slurm_array.sh"
            if not args.dry_run:
                result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
                if result.returncode == 0:
                    job_id = result.stdout.strip()
                    print(f"✓ Submitted download job: {job_id}")
                    print(f"  Monitor with: squeue -j {job_id}")
                    print(f"  Logs in: logs/slurm_{job_id}_*.out")
                    
                    # Wait for completion if running all stages
                    if args.stage == 'all':
                        print("\nWaiting for download to complete...")
                        print("(This may take several hours)")
                        
                        # Check job status periodically
                        import time
                        while True:
                            check_cmd = f"squeue -j {job_id} -h"
                            check_result = subprocess.run(check_cmd, shell=True, capture_output=True, text=True)
                            if not check_result.stdout.strip():
                                print("Download job completed")
                                break
                            time.sleep(60)  # Check every minute
                        
                        # Merge results
                        run_command(
                            "python merge_slurm_results.py",
                            "Merging download results"
                        )
                else:
                    print(f"✗ Failed to submit download job: {result.stderr}")
                    success = False
            else:
                print(f"[DRY RUN] Would execute: {cmd}")
        else:
            # Run download locally
            cmd = "python fetch_rnacentral_gff.py"
            if not args.dry_run:
                success = run_command(cmd, "Running download locally")
            else:
                print(f"[DRY RUN] Would execute: {cmd}")
    
    if args.stage in ['all', 'preprocess']:
        print("\n" + "="*70)
        print("STAGE 3: PREPROCESSING")
        print("="*70)
        
        # Test preprocessing setup first
        if not args.skip_tests and not args.dry_run:
            test_success = run_command(
                "python test_preprocessing_setup.py",
                "Testing preprocessing setup",
                check=False
            )
            
            if not test_success:
                print("\n⚠ Preprocessing setup test failed.")
                response = input("Continue anyway? (y/N): ")
                if response.lower() != 'y':
                    sys.exit(1)
        
        if args.slurm:
            # Submit Slurm array job for preprocessing
            cmd = "sbatch --parsable submit_preprocessing_slurm.sh"
            if not args.dry_run:
                result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
                if result.returncode == 0:
                    job_id = result.stdout.strip()
                    print(f"✓ Submitted preprocessing job: {job_id}")
                    print(f"  Monitor with: squeue -j {job_id}")
                    print(f"  Progress: python monitor_preprocessing.py")
                else:
                    print(f"✗ Failed to submit preprocessing job: {result.stderr}")
                    success = False
            else:
                print(f"[DRY RUN] Would execute: {cmd}")
        else:
            # Generate scripts for local execution
            print("Generating preprocessing scripts...")
            if not args.dry_run:
                run_command(
                    "python generate_preprocessing_scripts.py",
                    "Generating preprocessing scripts"
                )
                
                # Run batch script
                run_command(
                    "bash preprocessing_scripts/batch_all.sh",
                    "Running preprocessing locally"
                )
            else:
                print("[DRY RUN] Would generate and run preprocessing scripts")
    
    if args.stage == 'monitor':
        print("\n" + "="*70)
        print("MONITORING")
        print("="*70)
        
        # Monitor downloads
        print("\nDownload Status:")
        run_command("python monitor.py", "Checking download progress", check=False)
        
        # Monitor preprocessing
        print("\nPreprocessing Status:")
        run_command("python monitor_preprocessing.py", "Checking preprocessing progress", check=False)
        
        # Check Slurm queue
        print("\nSlurm Queue Status:")
        run_command(f"squeue -u {os.environ.get('USER', 'unknown')}", "Checking Slurm jobs", check=False)
    
    if args.stage == 'analyze':
        print("\n" + "="*70)
        print("ANALYSIS")
        print("="*70)
        
        # Analyze download coverage
        print("\nDownload Coverage Analysis:")
        run_command("python analyze_coverage.py", "Analyzing download coverage", check=False)
        
        # Count files
        print("\nFile Statistics:")
        if not args.dry_run:
            gff_count = subprocess.run(
                "find data/ -name '*.gff3' 2>/dev/null | wc -l",
                shell=True, capture_output=True, text=True
            ).stdout.strip()
            
            genes_count = subprocess.run(
                "find data/ -name '*.genes.json' 2>/dev/null | wc -l",
                shell=True, capture_output=True, text=True
            ).stdout.strip()
            
            print(f"  GFF files: {gff_count}")
            print(f"  Genes.json files: {genes_count}")
            
            if int(gff_count) > 0:
                completion = (int(genes_count) / int(gff_count)) * 100
                print(f"  Preprocessing completion: {completion:.1f}%")
        
        # Disk usage
        print("\nDisk Usage:")
        run_command("du -sh data/ 2>/dev/null || echo 'No data directory'", "Checking disk usage", check=False)
    
    # Final summary
    print("\n" + "="*70)
    print("WORKFLOW SUMMARY")
    print("="*70)
    
    if args.stage == 'all':
        if success:
            print("✓ Pipeline completed successfully!")
            print("\nNext steps:")
            print("  1. Review results: python analyze_coverage.py")
            print("  2. Check logs: ls -la logs/")
            print("  3. Verify output: find data/ -name '*.genes.json' | head")
        else:
            print("⚠ Pipeline completed with some errors")
            print("  Check logs for details: grep ERROR logs/*.log")
    else:
        if success:
            print(f"✓ Stage '{args.stage}' completed successfully")
        else:
            print(f"⚠ Stage '{args.stage}' completed with errors")
    
    print("="*70)

if __name__ == "__main__":
    main()
