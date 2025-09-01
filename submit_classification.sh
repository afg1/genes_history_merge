#!/bin/bash
#SBATCH --job-name=classify_genes
#SBATCH --array=0-15%4
#SBATCH --cpus-per-task=4
#SBATCH --mem=16G
#SBATCH --time=48:00:00
#SBATCH --partition=standard
#SBATCH --output=logs/gene_classification_%A_%a.out
#SBATCH --error=logs/gene_classification_%A_%a.err

# Gene classification using a pre-trained model
# This script runs the classification for pairs of transcript and feature parquet files

echo "Starting gene classification array job"
echo "Array task ID: $SLURM_ARRAY_TASK_ID"
echo "Job ID: $SLURM_JOB_ID"
echo "Node: $SLURMD_NODENAME"
echo "Working directory: $(pwd)"

# Load required modules
module load python/3.11.2

# Activate virtual environment if it exists
if [ -d "venv" ]; then
    source venv/bin/activate
fi

# Source environment variables if .env file exists
if [ -f ".env" ]; then
    source .env
fi

# Set environment variables
export SINGULARITYENV_APPEND_PATH=/hps/nobackup/agb/rnacentral/genes-testing/bin

# Verify database connection is set
if [ -z "$PGDATABASE" ]; then
    echo "ERROR: PGDATABASE environment variable not set"
    echo "Please set: export PGDATABASE='postgresql://user:pass@host/db'"
    exit 1
fi

# Create logs directory if it doesn't exist
mkdir -p logs/gene_classification

# Check if singularity image exists
SINGULARITY_IMAGE="/hps/nobackup/agb/rnacentral/genes-testing/gff2genes/rnacentral-rnacentral-import-pipeline-latest.sif"
if [ ! -f "$SINGULARITY_IMAGE" ]; then
    echo "ERROR: Singularity image not found: $SINGULARITY_IMAGE"
    exit 1
fi

# Check if classification model exists
MODEL_PATH="/hps/nobackup/agb/rnacentral/genes-testing/gff2genes/genes_rf_model.onnx"
if [ ! -f "$MODEL_PATH" ]; then
    echo "ERROR: Classification model not found: $MODEL_PATH"
    exit 1
fi

# Print resource information
echo "Resources allocated:"
echo "  CPUs: $SLURM_CPUS_PER_TASK"
echo "  Memory: $SLURM_MEM_PER_NODE MB"
echo "  Time limit: $SLURM_TIMELIMIT"
echo "  Partition: $SLURM_JOB_PARTITION"

# Run the classification
echo "Running gene classification..."
echo "Command: python classify_genes.py"

python classify_genes.py

exit_code=$?

echo "Gene classification completed with exit code: $exit_code"
echo "End time: $(date)"

# Print summary if successful
if [ $exit_code -eq 0 ]; then
    echo "Checking for generated classification output directories..."
    output_dir_count=$(find data/ -name "*_genes_output" 2>/dev/null | wc -l)
    echo "Total output directories found: $output_dir_count"

    if [ $output_dir_count -gt 0 ]; then
        echo "Recent output directories:"
        find data/ -name "*_genes_output" -exec ls -ldh {} \; | tail -5
    fi
fi

exit $exit_code
