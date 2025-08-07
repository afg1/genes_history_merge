#!/bin/bash
#SBATCH --job-name=preprocess_gff
#SBATCH --array=0-13%4              # 14 tasks (one per release), max 4 concurrent
#SBATCH --time=24:00:00             # 24 hours per release
#SBATCH --mem=16G                   # 16GB for singularity processing
#SBATCH --cpus-per-task=4           # 4 CPUs for parallel processing within task
#SBATCH --output=logs/preprocess_%A_%a.out
#SBATCH --error=logs/preprocess_%A_%a.err

# Slurm submission script for preprocessing GFF files with singularity

# Load required modules (adjust for your cluster)
# module load python/3.9
# module load singularity/3.8.0

# Create log directory
mkdir -p logs/preprocessing

# Activate virtual environment if it exists
if [ -d "venv" ]; then
    source venv/bin/activate
fi

# Ensure database connection is set
if [ -z "$PGDATABASE" ]; then
    echo "ERROR: PGDATABASE environment variable not set!"
    exit 1
fi

# Set singularity environment
export SINGULARITYENV_APPEND_PATH=/hps/nobackup/agb/rnacentral/genes-testing/bin

echo "Starting preprocessing array task $SLURM_ARRAY_TASK_ID of $SLURM_ARRAY_TASK_COUNT"
echo "Job ID: $SLURM_JOB_ID"
echo "Running on node: $SLURM_NODELIST"
echo "CPUs allocated: $SLURM_CPUS_PER_TASK"
echo "Memory allocated: ${SLURM_MEM_PER_NODE}MB"
echo "Start time: $(date)"

# Run the preprocessing script
python preprocess_gff.py

echo "End time: $(date)"
echo "Task completed"
