#!/usr/bin/env python3
"""
Generate individual preprocessing scripts for each organism/release pair
This allows for more granular control and easier debugging
"""

import os
import sys
import json
from pathlib import Path
import psycopg2
from psycopg2.extras import RealDictCursor
import glob
import re

import config

# Configuration
SINGULARITY_IMAGE = "/hps/nobackup/agb/rnacentral/genes-testing/gff2genes/rnacentral-rnacentral-import-pipeline-latest.sif"
SINGULARITY_ENV_PATH = "/hps/nobackup/agb/rnacentral/genes-testing/bin"
SCRIPTS_OUTPUT_DIR = "preprocessing_scripts"

def get_organism_taxid_mapping():
    """Query database to get mapping of organism names to taxids"""
    
    db_conn_str = os.environ.get(config.DB_CONNECTION_ENV)
    if not db_conn_str:
        raise ValueError(f"Database connection string not found in environment variable {config.DB_CONNECTION_ENV}")
    
    print("Fetching organism-taxid mapping from database...")
    
    try:
        conn = psycopg2.connect(db_conn_str)
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        
        cursor.execute(config.DB_QUERY)
        organisms = cursor.fetchall()
        
        # Create mapping of transformed names to taxids
        mapping = {}
        for org in organisms:
            transformed_name = transform_organism_name(org['organism_name'])
            mapping[transformed_name] = {
                'taxid': org['taxid'],
                'original_name': org['organism_name']
            }
        
        print(f"Created mapping for {len(mapping)} organisms")
        
        cursor.close()
        conn.close()
        
        return mapping
    
    except Exception as e:
        print(f"Database query failed: {str(e)}")
        raise


def transform_organism_name(name: str) -> str:
    """Transform organism name to match directory format"""
    transformed = name.lower()
    transformed = re.sub(r'[^a-z0-9]+', '_', transformed)
    transformed = transformed.strip('_')
    return transformed


def generate_single_script(gff_path, taxid, organism_name, release):
    """Generate a single preprocessing script"""
    
    script_content = f"""#!/bin/bash
# Preprocessing script for {organism_name} (taxid: {taxid}) - Release {release}
# Generated automatically - DO NOT EDIT

# Set up environment
export SINGULARITYENV_APPEND_PATH={SINGULARITY_ENV_PATH}

# Change to GFF directory
cd {os.path.dirname(gff_path)}

# Get GFF filename
GFF_FILE="{os.path.basename(gff_path)}"

# Check if GFF file exists
if [ ! -f "$GFF_FILE" ]; then
    echo "ERROR: GFF file not found: $GFF_FILE"
    exit 1
fi

# Check if already processed
if ls *.genes.json 1> /dev/null 2>&1; then
    echo "Already processed - genes.json file exists"
    exit 0
fi

echo "Processing $GFF_FILE with taxid {taxid}"
echo "Start time: $(date)"

# Run singularity container
singularity exec \\
    {SINGULARITY_IMAGE} \\
    rnac genes convert \\
    --gff_file "$GFF_FILE" \\
    --taxid {taxid}

# Check exit code
if [ $? -eq 0 ]; then
    echo "Successfully processed $GFF_FILE"
    echo "End time: $(date)"
    
    # List output files
    echo "Generated files:"
    ls -la *.genes.json 2>/dev/null || echo "Warning: No genes.json file found"
else
    echo "ERROR: Failed to process $GFF_FILE"
    exit 1
fi
"""
    
    return script_content


def generate_batch_script(scripts_info, batch_size=10):
    """Generate a batch script that runs multiple preprocessing scripts"""
    
    batch_content = """#!/bin/bash
# Batch preprocessing script
# Runs multiple organism preprocessing scripts in sequence

FAILED_COUNT=0
SUCCESS_COUNT=0
SKIPPED_COUNT=0

echo "Starting batch preprocessing of {count} organisms"
echo "="*60

""".format(count=len(scripts_info))
    
    for script_path, organism, release in scripts_info:
        batch_content += f"""
echo "Processing {organism} (release {release})..."
bash {script_path}
STATUS=$?

if [ $STATUS -eq 0 ]; then
    ((SUCCESS_COUNT++))
    echo "✓ Success: {organism}"
elif [ $STATUS -eq 2 ]; then
    ((SKIPPED_COUNT++))
    echo "○ Skipped: {organism} (already processed)"
else
    ((FAILED_COUNT++))
    echo "✗ Failed: {organism}"
fi

echo "-"*40
"""
    
    batch_content += """
echo "="*60
echo "Batch processing complete"
echo "Success: $SUCCESS_COUNT"
echo "Skipped: $SKIPPED_COUNT"  
echo "Failed: $FAILED_COUNT"

if [ $FAILED_COUNT -gt 0 ]; then
    exit 1
fi
"""
    
    return batch_content


def generate_slurm_array_script(total_scripts):
    """Generate a Slurm array submission script"""
    
    slurm_content = f"""#!/bin/bash
#SBATCH --job-name=preprocess_gff
#SBATCH --array=0-{total_scripts-1}%10    # Process {total_scripts} scripts, max 10 concurrent
#SBATCH --time=2:00:00                    # 2 hours per task
#SBATCH --mem=8G                          # 8GB per task
#SBATCH --cpus-per-task=2                 # 2 CPUs for singularity
#SBATCH --output=logs/preprocess_%A_%a.out
#SBATCH --error=logs/preprocess_%A_%a.err

# Load required modules (adjust for your cluster)
# module load singularity/3.8.0

# Create logs directory
mkdir -p logs

# Get list of scripts
SCRIPTS=({SCRIPTS_OUTPUT_DIR}/release_*/organism_*/*.sh)

# Get the script for this array task
SCRIPT="${{SCRIPTS[$SLURM_ARRAY_TASK_ID]}}"

echo "Array task $SLURM_ARRAY_TASK_ID of $SLURM_ARRAY_TASK_COUNT"
echo "Running script: $SCRIPT"
echo "Start time: $(date)"

# Run the preprocessing script
bash "$SCRIPT"

echo "End time: $(date)"
"""
    
    return slurm_content


def main():
    """Generate all preprocessing scripts"""
    
    print("="*60)
    print("Generating preprocessing scripts")
    print("="*60)
    
    # Get organism-taxid mapping
    try:
        taxid_mapping = get_organism_taxid_mapping()
    except Exception as e:
        print(f"Failed to get taxid mapping: {str(e)}")
        sys.exit(1)
    
    # Create output directory
    os.makedirs(SCRIPTS_OUTPUT_DIR, exist_ok=True)
    
    # Find all GFF files
    all_scripts = []
    scripts_by_release = {}
    missing_taxids = set()
    
    for release_dir in sorted(os.listdir(config.DATA_DIR)):
        if not release_dir.startswith('release_'):
            continue
        
        release_num = release_dir.replace('release_', '')
        release_path = os.path.join(config.DATA_DIR, release_dir)
        scripts_by_release[release_num] = []
        
        # Create release script directory
        release_script_dir = os.path.join(SCRIPTS_OUTPUT_DIR, release_dir)
        os.makedirs(release_script_dir, exist_ok=True)
        
        for organism_dir in sorted(os.listdir(release_path)):
            organism_path = os.path.join(release_path, organism_dir)
            
            if not os.path.isdir(organism_path):
                continue
            
            # Find decompressed GFF files
            gff_files = glob.glob(os.path.join(organism_path, "*.gff3"))
            
            if not gff_files:
                continue
            
            # Get taxid for this organism
            organism_info = taxid_mapping.get(organism_dir)
            
            if not organism_info:
                print(f"Warning: No taxid found for {organism_dir}")
                missing_taxids.add(organism_dir)
                continue
            
            taxid = organism_info['taxid']
            original_name = organism_info['original_name']
            
            # Create organism script directory
            organism_script_dir = os.path.join(release_script_dir, f"organism_{organism_dir}")
            os.makedirs(organism_script_dir, exist_ok=True)
            
            for gff_file in gff_files:
                # Generate script
                script_content = generate_single_script(
                    gff_file, taxid, original_name, release_num
                )
                
                # Save script
                script_name = f"preprocess_{organism_dir}_r{release_num}.sh"
                script_path = os.path.join(organism_script_dir, script_name)
                
                with open(script_path, 'w') as f:
                    f.write(script_content)
                
                # Make executable
                os.chmod(script_path, 0o755)
                
                all_scripts.append((script_path, organism_dir, release_num))
                scripts_by_release[release_num].append((script_path, organism_dir, release_num))
    
    print(f"\nGenerated {len(all_scripts)} preprocessing scripts")
    
    # Generate batch scripts for each release
    for release_num, scripts in scripts_by_release.items():
        if scripts:
            batch_script = generate_batch_script(scripts)
            batch_path = os.path.join(SCRIPTS_OUTPUT_DIR, f"batch_release_{release_num}.sh")
            
            with open(batch_path, 'w') as f:
                f.write(batch_script)
            
            os.chmod(batch_path, 0o755)
            print(f"Generated batch script for release {release_num}: {len(scripts)} organisms")
    
    # Generate master batch script
    master_batch = generate_batch_script(all_scripts)
    master_path = os.path.join(SCRIPTS_OUTPUT_DIR, "batch_all.sh")
    
    with open(master_path, 'w') as f:
        f.write(master_batch)
    
    os.chmod(master_path, 0o755)
    
    # Generate Slurm array script
    slurm_script = generate_slurm_array_script(len(all_scripts))
    slurm_path = os.path.join(SCRIPTS_OUTPUT_DIR, "submit_preprocessing_array.sh")
    
    with open(slurm_path, 'w') as f:
        f.write(slurm_script)
    
    os.chmod(slurm_path, 0o755)
    
    # Generate summary
    summary = {
        'total_scripts': len(all_scripts),
        'releases': list(scripts_by_release.keys()),
        'missing_taxids': list(missing_taxids),
        'scripts_by_release': {k: len(v) for k, v in scripts_by_release.items()},
        'output_directory': SCRIPTS_OUTPUT_DIR
    }
    
    summary_path = os.path.join(SCRIPTS_OUTPUT_DIR, "generation_summary.json")
    with open(summary_path, 'w') as f:
        json.dump(summary, f, indent=2)
    
    # Print summary
    print("\n" + "="*60)
    print("SCRIPT GENERATION SUMMARY")
    print("="*60)
    print(f"Total scripts generated: {len(all_scripts)}")
    print(f"Output directory: {SCRIPTS_OUTPUT_DIR}/")
    print(f"Missing taxids: {len(missing_taxids)} organisms")
    
    print("\nScripts by release:")
    for release, count in sorted(scripts_by_release.items(), key=lambda x: int(x[0])):
        print(f"  Release {release}: {len(scripts_by_release[release])} scripts")
    
    print("\n" + "-"*60)
    print("Usage options:")
    print("\n1. Run single organism:")
    print(f"   bash {SCRIPTS_OUTPUT_DIR}/release_12/organism_homo_sapiens/preprocess_homo_sapiens_r12.sh")
    
    print("\n2. Run all organisms for a release:")
    print(f"   bash {SCRIPTS_OUTPUT_DIR}/batch_release_12.sh")
    
    print("\n3. Run all organisms for all releases:")
    print(f"   bash {SCRIPTS_OUTPUT_DIR}/batch_all.sh")
    
    print("\n4. Submit as Slurm array job:")
    print(f"   sbatch {SCRIPTS_OUTPUT_DIR}/submit_preprocessing_array.sh")
    
    print("\nSummary saved to:", summary_path)
    print("="*60)


if __name__ == "__main__":
    main()
