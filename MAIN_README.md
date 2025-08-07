# RNAcentral GFF File Processing Pipeline

This is a complete pipeline for downloading and preprocessing GFF files from RNAcentral for multiple organisms across different releases.

## Overview

The pipeline consists of two main stages:
1. **Download**: Fetch GFF files from RNAcentral FTP (releases 12-25)
2. **Preprocessing**: Convert GFF files to genes.json format using Singularity

## Quick Start

```bash
# Set up environment
export PGDATABASE='postgresql://user:password@host:port/database'
export SINGULARITYENV_APPEND_PATH=/hps/nobackup/agb/rnacentral/genes-testing/bin

# Run complete pipeline with Slurm
python workflow.py all --slurm

# Or run stages individually
python workflow.py download --slurm
python workflow.py preprocess --slurm
python workflow.py monitor
```

## Project Structure

```
.
├── Core Scripts
│   ├── workflow.py                    # Main orchestrator
│   ├── config.py                      # Central configuration
│   └── requirements.txt               # Python dependencies
│
├── Download Scripts
│   ├── fetch_rnacentral_gff.py       # Main download script
│   ├── slurm_fetch_parallel.py       # Slurm-optimized downloader
│   ├── download_gff.sh               # Bash download helper
│   └── test_setup.py                 # Test database and downloads
│
├── Preprocessing Scripts
│   ├── preprocess_gff.py             # Main preprocessing script
│   ├── generate_preprocessing_scripts.py  # Script generator
│   ├── test_preprocessing_setup.py   # Test preprocessing setup
│   └── monitor_preprocessing.py      # Monitor preprocessing progress
│
├── Utility Scripts
│   ├── monitor.py                    # Monitor downloads
│   ├── analyze_coverage.py           # Analyze coverage
│   ├── merge_slurm_results.py        # Merge Slurm results
│   └── retry_failed_downloads.py     # Retry failures
│
├── Slurm Scripts
│   ├── submit_slurm_array.sh         # Download array job
│   ├── submit_slurm_single.sh        # Single node download
│   └── submit_preprocessing_slurm.sh # Preprocessing array job
│
└── Documentation
    ├── README.md                      # This file
    ├── SLURM_README.md               # Slurm usage guide
    └── PREPROCESSING_README.md       # Preprocessing details
```

## Prerequisites

- Python 3.6+
- PostgreSQL database with organism data
- wget for downloading
- Singularity for preprocessing
- Access to RNAcentral FTP
- Access to preprocessing Singularity image

## Installation

```bash
# Clone or download the scripts
# Install Python dependencies
pip install -r requirements.txt

# Test setup
python test_setup.py
python test_preprocessing_setup.py
```

## Configuration

Edit `config.py` to adjust:
- Release range (default: 12-25)
- Parallel download settings
- Directory paths
- Timeout and retry settings

Set environment variables:
```bash
export PGDATABASE='postgresql://username:password@host:port/database'
export SINGULARITYENV_APPEND_PATH=/hps/nobackup/agb/rnacentral/genes-testing/bin
```

## Usage

### Complete Pipeline (Recommended)

```bash
# Run everything with Slurm parallelization
python workflow.py all --slurm

# Run everything locally (slower)
python workflow.py all
```

### Stage-by-Stage Execution

#### 1. Download Stage
```bash
# With Slurm (parallel across 14 nodes)
sbatch submit_slurm_array.sh

# Or locally
python fetch_rnacentral_gff.py

# Monitor progress
python monitor.py
```

#### 2. Preprocessing Stage
```bash
# With Slurm
sbatch submit_preprocessing_slurm.sh

# Or generate individual scripts
python generate_preprocessing_scripts.py
bash preprocessing_scripts/batch_all.sh

# Monitor progress
python monitor_preprocessing.py
```

### Monitoring and Analysis

```bash
# Check overall progress
python workflow.py monitor

# Analyze download coverage
python analyze_coverage.py

# Check preprocessing status
python monitor_preprocessing.py

# View Slurm jobs
squeue -u $USER
```

## Output Structure

```
data/
├── release_12/
│   ├── homo_sapiens/
│   │   ├── homo_sapiens.gff3.gz      # Downloaded compressed
│   │   ├── homo_sapiens.gff3         # Decompressed
│   │   └── homo_sapiens.genes.json   # Preprocessed output
│   └── mus_musculus/
│       └── ...
├── release_13/
│   └── ...
└── ...

logs/
├── download.log                       # Main download log
├── errors.log                        # Error log
├── preprocessing/                    # Preprocessing logs
│   ├── preprocess_main.log
│   └── preprocess_summary_*.json
└── slurm_*.out                      # Slurm output files
```

## Performance

### Expected Runtime
- **Download**: ~12 hours per release with good network
- **Preprocessing**: ~5-30 minutes per organism
- **Total**: 24-48 hours for complete pipeline with Slurm

### Resource Requirements
- **Download**: 1 CPU, 4-8GB RAM per task
- **Preprocessing**: 2-4 CPUs, 8-16GB RAM per task
- **Storage**: ~200-400GB for all data

### Slurm Optimization
- Downloads: 14 parallel tasks (one per release)
- Preprocessing: 4-10 parallel tasks
- Adjust `--array` parameters based on cluster availability

## Troubleshooting

### Common Issues

1. **Database connection errors**
   ```bash
   # Test connection
   psql $PGDATABASE -c "SELECT 1"
   ```

2. **Singularity not found**
   ```bash
   module load singularity/3.8.0
   ```

3. **Download failures**
   ```bash
   # Retry failed downloads
   python retry_failed_downloads.py
   ```

4. **Preprocessing failures**
   ```bash
   # Check specific errors
   grep ERROR logs/preprocessing/*.log
   ```

### Resume After Failure

Both download and preprocessing stages automatically skip completed work:
- Downloads skip existing GFF files
- Preprocessing skips existing genes.json files

To force re-processing, remove the output files and run again.

## Advanced Usage

### Custom Release Range
```bash
# Edit config.py
RELEASE_START = 20
RELEASE_END = 25

# Or for single release
python fetch_rnacentral_gff.py --release 25
```

### Filter Organisms
```sql
-- Edit query in config.py
WHERE rnc_taxonomy.name LIKE 'Homo%'
```

### Parallel Optimization
```python
# In config.py
MAX_PARALLEL_DOWNLOADS = 10  # Increase for faster network
MAX_PARALLEL_PREPROCESSING = 8  # Increase for more CPUs
```

## Support and Debugging

### Log Files
- `logs/download.log` - Main download activity
- `logs/errors.log` - Download errors only
- `logs/preprocessing/*.log` - Preprocessing logs
- `logs/slurm_*.out` - Slurm job outputs

### Generate Reports
```bash
# Download summary
cat logs/download_summary.json | python -m json.tool

# Preprocessing summary
cat logs/preprocessing/preprocess_summary_*.json | python -m json.tool

# Coverage analysis
python analyze_coverage.py > coverage_report.txt
```

## Citation

If you use this pipeline, please cite:
- RNAcentral: [https://rnacentral.org](https://rnacentral.org)
- The RNAcentral Consortium (2021) Nucleic Acids Research

## License

This pipeline is provided as-is for research purposes.

## Contact

For issues with:
- RNAcentral data: Contact RNAcentral support
- Singularity container: Contact your system administrator
- Pipeline scripts: Check logs and documentation