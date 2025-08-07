#!/bin/bash
#SBATCH --job-name=rnacentral_single
#SBATCH --time=48:00:00        # 48 hours for all downloads
#SBATCH --mem=8G               # 8GB memory
#SBATCH --cpus-per-task=1      # 1 CPU is sufficient
#SBATCH --output=logs/slurm_%j.out
#SBATCH --error=logs/slurm_%j.err

# Single node version - runs the original script on one node
# Use this if you prefer simplicity over parallelization

# Load required modules (adjust for your cluster)
# module load python/3.9
# module load postgresql/13

# Create log directory
mkdir -p logs

source .env

# Activate virtual environment if it exists
if [ -d "venv" ]; then
    source venv/bin/activate
fi

# Ensure database connection is set
if [ -z "$PGDATABASE" ]; then
    echo "ERROR: PGDATABASE environment variable not set!"
    exit 1
fi

echo "Starting single-node download job"
echo "Job ID: $SLURM_JOB_ID"
echo "Running on node: $SLURM_NODELIST"
echo "Start time: $(date)"

# Run the original Python script
python fetch_rnacentral_gff.py

echo "End time: $(date)"
echo "Job completed"
