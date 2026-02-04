#!/bin/bash
#SBATCH --job-name=virac_lc
#SBATCH --output=virac_lc_%j.out
#SBATCH --error=virac_lc_%j.err
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=64
#SBATCH --mem=256G
#SBATCH --time=24:00:00
#SBATCH --partition=compute

# ==============================================================================
# VIRAC Light Curve Extraction Job
# ==============================================================================
# This job can be resubmitted multiple times - it automatically resumes
# from where it left off using checkpoint files.
#
# Usage:
#   sbatch run_extraction.slurm
#
# Monitor progress:
#   python monitor_progress.py --watch
# ==============================================================================

# Load required modules (adjust for your system)
module purge
module load python/3.9
module load hdf5

# Set environment variables for better HDF5 performance
export HDF5_USE_FILE_LOCKING=FALSE
export OMP_NUM_THREADS=1

# Move to submission directory
cd $SLURM_SUBMIT_DIR

# Configuration - adjust these paths as needed
INPUT_DIR="/beegfs/car/lsmith/virac_v2/data/output/ts_tables/"
OUTPUT_DIR="/beegfs/car/njm/virac_lightcurves/"
MIN_KS=20
WORKERS=64

# Print job info
echo "=============================================="
echo "VIRAC Light Curve Extraction"
echo "=============================================="
echo "Job ID:     $SLURM_JOB_ID"
echo "Node:       $SLURM_NODELIST"
echo "CPUs:       $SLURM_CPUS_PER_TASK"
echo "Start time: $(date)"
echo "Input:      $INPUT_DIR"
echo "Output:     $OUTPUT_DIR"
echo "Min Ks:     $MIN_KS"
echo "Workers:    $WORKERS"
echo "=============================================="

# Run the extractor
python virac_lightcurve_extractor.py \
    --input-dir "$INPUT_DIR" \
    --output-dir "$OUTPUT_DIR" \
    --workers "$WORKERS" \
    --min-ks "$MIN_KS"

# Capture exit status
EXIT_STATUS=$?

# Print completion info
echo "=============================================="
echo "End time:   $(date)"
echo "Exit status: $EXIT_STATUS"
echo "=============================================="

# If job didn't complete all tiles, suggest resubmission
if [ $EXIT_STATUS -eq 0 ]; then
    echo "Job completed successfully."
else
    echo "Job exited with status $EXIT_STATUS"
    echo "You can resubmit with: sbatch run_extraction.slurm"
fi

exit $EXIT_STATUS
