#!/bin/bash
#SBATCH --job-name=virac_shard
#SBATCH --output=virac_shard_%A_%a.out
#SBATCH --error=virac_shard_%A_%a.err
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=16    # Updated to match core96 architecture 
#SBATCH --mem=0            # Kept high memory for safety (Node has 256GB )
#SBATCH --time=7-00:00:00     # Updated to match the 7-day walltime of the main job 
#SBATCH --partition=core96    # Added to target the correct queue 
#SBATCH --array=0-19           # <-- CHANGE THIS: shards 1,2,3 (keep 0 for your running job)

# ==============================================================================
# VIRAC Sharded Extraction (Core96 Version)
# ==============================================================================
# Your original job (shard 0) is already running.
# This launches additional shards (1, 2, 3, ...) in parallel.
#
# Each shard processes every Nth tile:
#   Shard 0: tiles 0, 4, 8, 12, ...
#   Shard 1: tiles 1, 5, 9, 13, ...
#   Shard 2: tiles 2, 6, 10, 14, ...
#   Shard 3: tiles 3, 7, 11, 15, ...
#
# All shards share the same checkpoint file (with locking).
# ==============================================================================

# Use modulecmd for batch scripts [cite: 8]
eval $(/usr/bin/modulecmd bash purge)
eval $(/usr/bin/modulecmd bash load python/3.9)
eval $(/usr/bin/modulecmd bash load hdf5)

export HDF5_USE_FILE_LOCKING=FALSE
export OMP_NUM_THREADS=1

cd $SLURM_SUBMIT_DIR

# Total shards = your running job + these new ones
# If running job is shard 0 and you launch array=1-3, total = 4
TOTAL_SHARDS=20
WORKERS=16 #"$SLURM_CPUS_PER_TASK" # Dynamically set workers to 96 [cite: 14]

echo "=============================================="
echo "Shard $SLURM_ARRAY_TASK_ID of $TOTAL_SHARDS"
echo "Node:    $SLURM_NODELIST"
echo "CPUs:    $SLURM_CPUS_PER_TASK"
echo "Workers: $WORKERS"
echo "Start:   $(date)"
echo "=============================================="

python virac_extractor_sharded.py \
    --shard $SLURM_ARRAY_TASK_ID \
    --total-shards $TOTAL_SHARDS \
    --workers "$WORKERS" \
    --output-dir /beegfs/car/njm/virac_lightcurves/

echo "=============================================="
echo "End: $(date)"
echo "=============================================="
