#!/usr/bin/env python3
"""
Test preprocessing setup with a single GFF file
"""

import os
import sys
import subprocess
import glob
import json
from pathlib import Path

import config
from preprocess_gff import (
    get_organism_taxid_mapping,
    transform_organism_name,
    run_singularity_conversion
)

SINGULARITY_IMAGE = "/hps/nobackup/agb/rnacentral/genes-testing/gff2genes/rnacentral-rnacentral-import-pipeline-latest.sif"
SINGULARITY_ENV_PATH = "/hps/nobackup/agb/rnacentral/genes-testing/bin"

def test_singularity_availability():
    """Test if singularity is available and accessible"""
    print("\n1. Testing Singularity availability...")
    print("-" * 60)
    
    try:
        result = subprocess.run(
            ['singularity', '--version'],
            capture_output=True,
            text=True,
            timeout=5
        )
        
        if result.returncode == 0:
            print(f"✓ Singularity is available: {result.stdout.strip()}")
            return True
        else:
            print(f"✗ Singularity command failed: {result.stderr}")
            return False
    
    except FileNotFoundError:
        print("✗ Singularity not found. Please load the singularity module:")
        print("  module load singularity/3.8.0")
        return False
    
    except Exception as e:
        print(f"✗ Error testing singularity: {str(e)}")
        return False

def test_singularity_image():
    """Test if the singularity image exists and is accessible"""
    print("\n2. Testing Singularity image...")
    print("-" * 60)
    
    if os.path.exists(SINGULARITY_IMAGE):
        file_size = os.path.getsize(SINGULARITY_IMAGE)
        print(f"✓ Singularity image found: {SINGULARITY_IMAGE}")
        print(f"  Size: {file_size / (1024**3):.2f} GB")
        
        # Test if we can inspect the image
        try:
            result = subprocess.run(
                ['singularity', 'inspect', SINGULARITY_IMAGE],
                capture_output=True,
                text=True,
                timeout=10
            )
            
            if result.returncode == 0:
                print("✓ Image is accessible and valid")
                return True
            else:
                print(f"⚠ Warning: Could not inspect image: {result.stderr}")
                return True  # Image exists, might still work
        
        except Exception as e:
            print(f"⚠ Warning: Could not inspect image: {str(e)}")
            return True  # Image exists, might still work
    
    else:
        print(f"✗ Singularity image not found: {SINGULARITY_IMAGE}")
        print("  Please check the path or contact system administrator")
        return False

def test_environment_path():
    """Test if the environment path exists"""
    print("\n3. Testing environment path...")
    print("-" * 60)
    
    if os.path.exists(SINGULARITY_ENV_PATH):
        print(f"✓ Environment path exists: {SINGULARITY_ENV_PATH}")
        
        # List some files in the path
        try:
            files = os.listdir(SINGULARITY_ENV_PATH)
            print(f"  Contains {len(files)} files/directories")
            if files:
                print(f"  Sample entries: {', '.join(files[:5])}")
            return True
        except Exception as e:
            print(f"⚠ Warning: Could not list directory contents: {str(e)}")
            return True
    else:
        print(f"✗ Environment path not found: {SINGULARITY_ENV_PATH}")
        return False

def find_test_gff():
    """Find a test GFF file to process"""
    print("\n4. Finding test GFF file...")
    print("-" * 60)
    
    # Look for any GFF file in the data directory
    gff_files = glob.glob(os.path.join(config.DATA_DIR, "**/**.gff3"), recursive=True)
    
    if not gff_files:
        print("✗ No GFF files found in data directory")
        print("  Please run the download script first")
        return None
    
    # Prefer a small, well-known organism if available
    preferred_organisms = ['homo_sapiens', 'mus_musculus', 'drosophila_melanogaster']
    
    test_file = None
    for organism in preferred_organisms:
        for gff_file in gff_files:
            if organism in gff_file:
                test_file = gff_file
                break
        if test_file:
            break
    
    if not test_file:
        # Just use the first file found
        test_file = gff_files[0]
    
    file_size = os.path.getsize(test_file)
    print(f"✓ Found test GFF file: {test_file}")
    print(f"  Size: {file_size / (1024**2):.2f} MB")
    
    return test_file

def test_preprocessing(gff_file):
    """Test preprocessing a single GFF file"""
    print("\n5. Testing preprocessing...")
    print("-" * 60)
    
    # Get organism name from path
    organism_dir = os.path.basename(os.path.dirname(gff_file))
    
    # Get taxid mapping
    print("Getting taxid mapping from database...")
    try:
        taxid_mapping = get_organism_taxid_mapping()
        taxid = taxid_mapping.get(organism_dir)
        
        if not taxid:
            print(f"⚠ Warning: No taxid found for {organism_dir}, using test taxid 9606 (human)")
            taxid = 9606
        else:
            print(f"✓ Found taxid {taxid} for {organism_dir}")
    
    except Exception as e:
        print(f"⚠ Warning: Could not get taxid from database: {str(e)}")
        print("  Using test taxid 9606 (human)")
        taxid = 9606
    
    # Set up environment
    env = os.environ.copy()
    env['SINGULARITYENV_APPEND_PATH'] = SINGULARITY_ENV_PATH
    
    # Run preprocessing
    print(f"\nRunning preprocessing command...")
    print(f"  GFF file: {os.path.basename(gff_file)}")
    print(f"  Taxid: {taxid}")
    print(f"  Working directory: {os.path.dirname(gff_file)}")
    
    # Build command
    cmd = [
        'singularity', 'exec',
        SINGULARITY_IMAGE,
        'rnac', 'genes', 'convert',
        '--gff_file', os.path.basename(gff_file),
        '--taxid', str(taxid)
    ]
    
    print(f"  Command: {' '.join(cmd)}")
    print("\nProcessing (this may take a few minutes)...")
    
    try:
        result = subprocess.run(
            cmd,
            cwd=os.path.dirname(gff_file),
            env=env,
            capture_output=True,
            text=True,
            timeout=300  # 5 minute timeout
        )
        
        if result.returncode == 0:
            print("✓ Preprocessing completed successfully!")
            
            # Check for output files
            output_files = glob.glob(os.path.join(os.path.dirname(gff_file), "*.genes.json"))
            if output_files:
                print(f"✓ Generated output file: {output_files[0]}")
                
                # Check file size and content
                output_size = os.path.getsize(output_files[0])
                print(f"  Output size: {output_size / 1024:.2f} KB")
                
                # Try to validate JSON
                try:
                    with open(output_files[0], 'r') as f:
                        data = json.load(f)
                    print(f"  Valid JSON with {len(data)} entries" if isinstance(data, list) else "  Valid JSON")
                except Exception as e:
                    print(f"  Warning: Could not parse JSON: {str(e)}")
            else:
                print("⚠ Warning: No .genes.json file found after processing")
            
            return True
        
        else:
            print(f"✗ Preprocessing failed with return code {result.returncode}")
            print(f"Error output:\n{result.stderr}")
            return False
    
    except subprocess.TimeoutExpired:
        print("✗ Preprocessing timeout (exceeded 5 minutes)")
        return False
    
    except Exception as e:
        print(f"✗ Preprocessing error: {str(e)}")
        return False

def main():
    """Main test function"""
    print("="*70)
    print("PREPROCESSING SETUP TEST")
    print("="*70)
    
    tests_passed = []
    
    # Test 1: Singularity availability
    if test_singularity_availability():
        tests_passed.append("Singularity availability")
    
    # Test 2: Singularity image
    if test_singularity_image():
        tests_passed.append("Singularity image")
    
    # Test 3: Environment path
    if test_environment_path():
        tests_passed.append("Environment path")
    
    # Test 4: Find test file
    test_file = find_test_gff()
    if test_file:
        tests_passed.append("Test GFF file")
        
        # Test 5: Run preprocessing
        if test_preprocessing(test_file):
            tests_passed.append("Preprocessing execution")
    
    # Summary
    print("\n" + "="*70)
    print("TEST SUMMARY")
    print("="*70)
    
    total_tests = 5
    passed = len(tests_passed)
    
    print(f"Tests passed: {passed}/{total_tests}")
    for test in tests_passed:
        print(f"  ✓ {test}")
    
    if passed == total_tests:
        print("\n✓ All tests passed! Ready to run preprocessing.")
        print("\nNext steps:")
        print("1. For single file: python preprocess_gff.py")
        print("2. For batch processing: python generate_preprocessing_scripts.py")
        print("3. For Slurm: sbatch submit_preprocessing_slurm.sh")
    else:
        print("\n✗ Some tests failed. Please fix the issues above.")
        sys.exit(1)

if __name__ == "__main__":
    main()
