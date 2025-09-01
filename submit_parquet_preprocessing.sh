#!/bin/bash
#SBATCH --job-name=preprocess_parquet
#SBATCH --array=0-15%4
#SBATCH --cpus-per-task=4
#SBATCH --mem=24G
#SBATCH --time=48:00:00
#SBATCH --partition=standard
#SBATCH --output=logs/feature_preprocess_%A_%a.out
#SBATCH --error=logs/feature_preprocess_%A_%a.err

# Feature preprocessing for transcript parquet files
# This script processes transcript parquet files to generate features for classification

echo "Starting feature preprocessing array job"
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
mkdir -p logs/feature_preprocessing

# Check if singularity image exists
SINGULARITY_IMAGE="/hps/nobackup/agb/rnacentral/genes-testing/gff2genes/rnacentral-rnacentral-import-pipeline-latest.sif"
if [ ! -f "$SINGULARITY_IMAGE" ]; then
    echo "ERROR: Singularity image not found: $SINGULARITY_IMAGE"
    exit 1
fi

# Check if SO embedding model exists
SO_MODEL_PATH="/hps/nobackup/agb/rnacentral/genes-testing/gff2genes/so_embedding_model.emb"
if [ ! -f "$SO_MODEL_PATH" ]; then
    echo "ERROR: SO embedding model not found: $SO_MODEL_PATH"
    exit 1
fi

# Print resource information
echo "Resources allocated:"
echo "  CPUs: $SLURM_CPUS_PER_TASK"
echo "  Memory: $SLURM_MEM_PER_NODE MB"
echo "  Time limit: $SLURM_TIMELIMIT"
echo "  Partition: $SLURM_JOB_PARTITION"

# Run the preprocessing
echo "Running feature preprocessing..."
echo "Command: python preprocess_parquet.py"

python preprocess_parquet.py

exit_code=$?

echo "Feature preprocessing completed with exit code: $exit_code"
echo "End time: $(date)"

# Print summary if successful
if [ $exit_code -eq 0 ]; then
    echo "Checking for generated feature files in data/ directories..."
    feature_count=$(find data/ -name "*_features.parquet" 2>/dev/null | wc -l)
    echo "Total feature files found: $feature_count"
    
    if [ $feature_count -gt 0 ]; then
        echo "Recent feature files:"
        find data/ -name "*_features.parquet" -exec ls -lh {} \; | tail -5
    fi
fi

exit $exit_code
