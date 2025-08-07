#!/bin/bash
#SBATCH --job-name=rnacentral_gff
#SBATCH --array=0-13%4         # 14 tasks (releases 12-25), max 4 running at once
#SBATCH --time=12:00:00        # 12 hours per task
#SBATCH --mem=4G               # 4GB per task (mostly for decompression)
#SBATCH --cpus-per-task=1      # 1 CPU is sufficient
#SBATCH --output=logs/slurm_%A_%a.out
#SBATCH --error=logs/slurm_%A_%a.err

# Load required modules (adjust for your cluster)
# module load python/3.9
# module load postgresql/13

# Create log directory
mkdir -p logs

# Activate virtual environment if it exists
if [ -d "venv" ]; then
    source venv/bin/activate
fi

# Ensure database connection is set
if [ -z "$PGDATABASE" ]; then
    echo "ERROR: PGDATABASE environment variable not set!"
    exit 1
fi

echo "Starting array task $SLURM_ARRAY_TASK_ID of $SLURM_ARRAY_TASK_COUNT"
echo "Job ID: $SLURM_JOB_ID"
echo "Running on node: $SLURM_NODELIST"
echo "Start time: $(date)"

# Run the Python script
python slurm_fetch_parallel.py

echo "End time: $(date)"
echo "Task completed"
