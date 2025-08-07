# GFF Preprocessing with Singularity

This module processes downloaded GFF files using a Singularity container to convert them to gene format.

## Overview

The preprocessing step:
1. Queries the database to get taxid mappings for organisms
2. Runs a Singularity container for each GFF file
3. Converts GFF format to genes.json format
4. Handles parallel processing for efficiency

## Prerequisites

- Singularity installed and available
- Access to the Singularity image at `/hps/nobackup/agb/rnacentral/genes-testing/gff2genes/`
- Downloaded and decompressed GFF files (from the download step)
- Database connection for taxid lookup

## Setup

1. **Set environment variables:**
```bash
export PGDATABASE='postgresql://user:pass@host/db'
export SINGULARITYENV_APPEND_PATH=/hps/nobackup/agb/rnacentral/genes-testing/bin
```

2. **Load required modules (on HPC):**
```bash
module load singularity/3.8.0
module load python/3.9
```

3. **Test the setup:**
```bash
python test_preprocessing_setup.py
```

## Usage Options

### Option 1: Direct Python Processing (Recommended for Slurm)

Run preprocessing with automatic parallelization:

```bash
# Single node processing
python preprocess_gff.py

# Slurm array job (parallel across nodes)
sbatch submit_preprocessing_slurm.sh
```

**Features:**
- Automatic taxid lookup from database
- Parallel processing within each node
- Comprehensive logging
- Resume capability (skips already processed files)

### Option 2: Generated Scripts (More Control)

Generate individual scripts for each organism/release:

```bash
# Generate all scripts
python generate_preprocessing_scripts.py

# This creates:
# - Individual scripts for each organism/release
# - Batch scripts for each release
# - Master batch script for all files
# - Slurm array submission script
```

**Run generated scripts:**
```bash
# Single organism
bash preprocessing_scripts/release_12/organism_homo_sapiens/preprocess_homo_sapiens_r12.sh

# All organisms in a release
bash preprocessing_scripts/batch_release_12.sh

# Everything
bash preprocessing_scripts/batch_all.sh

# As Slurm array
sbatch preprocessing_scripts/submit_preprocessing_array.sh
```

**Advantages:**
- Full control over individual processes
- Easy to debug specific failures
- Can manually edit scripts if needed
- Scripts are self-contained

## Monitoring Progress

### Real-time monitoring:
```bash
python monitor_preprocessing.py
```

Shows:
- Overall progress percentage
- Progress by release
- Incomplete organisms
- Recent errors
- Active Slurm jobs

### Check logs:
```bash
# Main preprocessing log
tail -f logs/preprocessing/preprocess_main.log

# Slurm array task logs
tail -f logs/preprocess_*_*.out

# Error summaries
grep ERROR logs/preprocessing/*.log
```

### Verify output:
```bash
# Count generated genes.json files
find data/ -name "*.genes.json" | wc -l

# Check file sizes
find data/ -name "*.genes.json" -exec ls -lh {} \;

# Validate JSON format
python -c "import json; json.load(open('data/release_12/homo_sapiens/homo_sapiens.genes.json'))"
```

## Slurm Configuration

### Resource Requirements

Per GFF file processing:
- **CPU**: 2-4 cores (Singularity can use multiple threads)
- **Memory**: 8-16GB (depends on GFF size)
- **Time**: 5-30 minutes per file
- **Disk**: Output is typically 10-50% of input GFF size

### Recommended Slurm Settings

For array job processing:
```bash
#SBATCH --array=0-13%4       # 14 releases, max 4 concurrent
#SBATCH --cpus-per-task=4    # 4 CPUs per task
#SBATCH --mem=16G            # 16GB RAM
#SBATCH --time=24:00:00      # 24 hours per release
```

Adjust based on your cluster and data:
- Increase `%N` for more parallel tasks
- Increase memory for larger GFF files
- Adjust time based on organism count per release

## Troubleshooting

### Singularity not found
```bash
# Load module
module load singularity/3.8.0

# Or use full path
/usr/bin/singularity --version
```

### Image not accessible
```bash
# Check image exists
ls -la /hps/nobackup/agb/rnacentral/genes-testing/gff2genes/rnacentral-rnacentral-import-pipeline-latest.sif

# Test image
singularity inspect <image_path>
```

### Database connection errors
```bash
# Verify connection
psql $PGDATABASE -c "SELECT 1"

# Test taxid query
python -c "from preprocess_gff import get_organism_taxid_mapping; print(len(get_organism_taxid_mapping()))"
```

### Processing failures

1. **Check specific error:**
```bash
grep -A5 "ERROR.*organism_name" logs/preprocessing/*.log
```

2. **Retry single organism:**
```bash
cd data/release_12/organism_name/
singularity exec <image> rnac genes convert --gff_file file.gff3 --taxid 12345
```

3. **Common issues:**
- Invalid GFF format
- Missing taxid in database
- Insufficient memory
- Corrupted GFF file

### Resume after failure

The scripts automatically skip already processed files (those with .genes.json output).

To force reprocessing:
```bash
# Remove output file
rm data/release_*/organism_name/*.genes.json

# Rerun processing
python preprocess_gff.py
```

## Output Files

For each GFF file, the preprocessing generates:
- `*.genes.json` - Gene information in JSON format

Example structure:
```
data/
└── release_12/
    └── homo_sapiens/
        ├── homo_sapiens.gff3          # Original GFF
        └── homo_sapiens.genes.json    # Generated output
```

## Performance Optimization

### Parallel Processing

1. **Within-node parallelism:**
```python
# In preprocess_gff.py, adjust:
MAX_PARALLEL_PREPROCESSING = 4  # Number of concurrent singularity processes
```

2. **Across-node parallelism:**
```bash
# Use more array tasks
#SBATCH --array=0-265%20  # Process by organism instead of release
```

3. **Local scratch for I/O:**
```bash
# Copy to local scratch
cp $DATA_DIR/release_*/organism/* $TMPDIR/
# Process
cd $TMPDIR && singularity exec...
# Copy back
cp *.genes.json $DATA_DIR/release_*/organism/
```

## Summary Statistics

After preprocessing completes:

```bash
# Generate statistics
python -c "
import glob, json
files = glob.glob('data/**/*.genes.json', recursive=True)
print(f'Total genes.json files: {len(files)}')
total_genes = 0
for f in files[:10]:  # Sample first 10
    with open(f) as fh:
        data = json.load(fh)
        total_genes += len(data) if isinstance(data, list) else 1
print(f'Average genes per file (sample): {total_genes/10:.0f}')
"
```

## Next Steps

After preprocessing is complete:
1. Verify all expected organisms have been processed
2. Check for any failed conversions in logs
3. Proceed with downstream analysis of genes.json files
4. Archive or remove original GFF files if no longer needed

## Support

For issues with:
- **Singularity container**: Contact the RNAcentral team
- **Database/taxid issues**: Check database schema and connectivity
- **Script bugs**: Review logs in `logs/preprocessing/`
