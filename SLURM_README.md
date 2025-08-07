# Slurm Cluster Usage Guide

## Overview

This project includes optimized scripts for running on a Slurm cluster. The main benefit is **parallelization across multiple nodes** rather than using multiple CPUs on a single node.

## Why 1 CPU per Task is Sufficient

The workload is **I/O bound**, not CPU bound:
- **Downloading files**: ~0% CPU (waiting for network)
- **Decompressing GFF files**: ~5-10% CPU (single-threaded gzip)
- **Database query**: <1% CPU

**Recommendation**: Use `--cpus-per-task=1` and parallelize across multiple nodes instead.

## Two Approaches

### Option 1: Slurm Array Job (Recommended for Large Clusters)

Distributes releases across multiple nodes for parallel processing.

```bash
# Submit array job (14 tasks for releases 12-25)
sbatch submit_slurm_array.sh

# Monitor progress
squeue -u $USER

# After completion, merge results
python merge_slurm_results.py
```

**Advantages:**
- Parallel processing across multiple nodes
- Faster completion (14x speedup with 14 nodes)
- Automatic load distribution
- Resilient to node failures

**Resource usage per task:**
- CPUs: 1
- Memory: 4GB
- Time: ~12 hours per release
- Network: ~20GB download per release

### Option 2: Single Node Job (Simple Approach)

Runs the original script on a single node.

```bash
# Submit single-node job
sbatch submit_slurm_single.sh

# Monitor progress
python monitor.py
```

**Advantages:**
- Simpler to manage
- Single log file
- No result merging needed

**Resource usage:**
- CPUs: 1
- Memory: 8GB
- Time: ~48 hours for all releases
- Network: ~280GB total download

## Customizing Slurm Parameters

### Modify Array Job Size

Edit `submit_slurm_array.sh`:

```bash
# Process 14 releases with max 4 concurrent tasks
#SBATCH --array=0-13%4

# Process all at once (no limit)
#SBATCH --array=0-13

# Process 7 releases (e.g., 12-18)
#SBATCH --array=0-6
```

Then adjust `config.py` accordingly:
```python
RELEASE_START = 12
RELEASE_END = 18  # Adjusted for 7 releases
```

### Adjust Time Limits

Based on your network speed and organism count:

```bash
# Fast network, fewer organisms
#SBATCH --time=6:00:00

# Slow network, many organisms
#SBATCH --time=24:00:00
```

### Memory Requirements

Memory is mainly used for:
- File decompression buffers
- Python script overhead
- Temporary storage

```bash
# Minimal memory (if disk I/O is fast)
#SBATCH --mem=2G

# Conservative memory allocation
#SBATCH --mem=8G
```

## Monitoring Array Jobs

### Check Job Status
```bash
# View all array tasks
squeue -u $USER

# View specific array job
squeue -j <job_id>

# Check completed tasks
sacct -j <job_id> --format=JobID,State,ExitCode,Elapsed
```

### Monitor Individual Task Logs
```bash
# View output for task 0
tail -f logs/slurm_<job_id>_0.out

# View errors for task 5
cat logs/slurm_<job_id>_5.err

# Check task-specific download log
tail -f logs/download_task_0.log
```

### Real-time Progress
```bash
# Count successfully downloaded files
find data/ -name "*.gff3.gz" | wc -l

# Check disk usage
du -sh data/

# Monitor network usage (if available)
sstat -j <job_id> --format=JobID,AveRSS,MaxRSS,ConsumedEnergy
```

## Post-Processing

After array jobs complete:

1. **Merge results:**
   ```bash
   python merge_slurm_results.py
   ```

2. **Analyze coverage:**
   ```bash
   python analyze_coverage.py
   ```

3. **Check for failed downloads:**
   ```bash
   grep -l "failed" logs/summary_task_*.json
   ```

4. **Retry failed downloads:**
   ```bash
   # Extract failed organisms/releases from merged_summary.json
   # Create a custom script to retry only failures
   ```

## Performance Optimization Tips

### Network Optimization
- **Use nodes with good network connectivity** to the external internet
- **Avoid peak hours** when possible
- **Consider using a proxy** if your cluster has one configured

### Storage Optimization
- **Use local scratch space** for temporary files:
  ```bash
  #SBATCH --tmp=100G  # Request local scratch space
  ```
  Then modify scripts to use `$TMPDIR`

- **Compress completed downloads** to save space:
  ```bash
  # After processing, keep only compressed files
  find data/ -name "*.gff3" -exec gzip {} \;
  ```

### Scheduling Optimization
- **Use job dependencies** for sequential processing:
  ```bash
  # Submit download job
  DOWNLOAD_JOB=$(sbatch --parsable submit_slurm_array.sh)
  
  # Submit merge job to run after downloads complete
  sbatch --dependency=afterok:$DOWNLOAD_JOB merge_results.sh
  ```

## Troubleshooting

### Array Task Failures

If specific tasks fail:
```bash
# Check which tasks failed
sacct -j <job_id> --format=JobID,State,ExitCode | grep FAILED

# Rerun specific tasks
sbatch --array=3,7,11 submit_slurm_array.sh  # Rerun tasks 3, 7, and 11
```

### Database Connection Issues

Ensure database is accessible from compute nodes:
```bash
# Test from compute node
srun --pty bash
export PGDATABASE='your_connection_string'
python test_setup.py
```

### Disk Quota Issues

Monitor disk usage:
```bash
# Check quota
quota -s

# Find large files
find data/ -size +1G -ls

# Clean up old logs
rm logs/slurm_*.out logs/slurm_*.err
```

## Example Workflow

Complete workflow for a typical run:

```bash
# 1. Set up environment
export PGDATABASE='postgresql://user:pass@host/db'
module load python/3.9

# 2. Install dependencies
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt

# 3. Test setup
python test_setup.py

# 4. Submit array job
JOB_ID=$(sbatch --parsable submit_slurm_array.sh)
echo "Submitted job: $JOB_ID"

# 5. Monitor progress
watch -n 60 squeue -j $JOB_ID

# 6. Wait for completion and merge
while squeue -j $JOB_ID | grep -q $JOB_ID; do
    sleep 300
done
python merge_slurm_results.py

# 7. Analyze results
python analyze_coverage.py

# 8. Clean up if needed
# gzip data/release_*/\*/*.gff3
```

## Resource Estimation

For ~265 organisms across 14 releases:

| Approach | Nodes | CPUs/Node | Total Time | Total CPU Hours |
|----------|-------|-----------|------------|-----------------|
| Array Job | 14 | 1 | ~12 hours | 168 |
| Array Job | 7 | 1 | ~24 hours | 168 |
| Single Node | 1 | 1 | ~48 hours | 48 |

**Note**: Array jobs use more CPU hours but complete faster through parallelization.
