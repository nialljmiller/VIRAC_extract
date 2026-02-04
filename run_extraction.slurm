#!/bin/bash
#SBATCH --job-name=virac_lc
#SBATCH --output=virac_lc_%j.out
#SBATCH --error=virac_lc_%j.err
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=96
# The 96‑core EPYC nodes in the core96 queue each have 256 GB of RAM.  Requesting
# one of these nodes allows us to run the maximum number of parallel workers
# available on a single compute node.  See the cluster documentation on queues
# and architecture for details【663148948527848†L6-L14】【549210836745242†L24-L31】.
# In accordance with the fair‑share policy, jobs may run up to one week and
# users may consume up to 256 cores × 1 week of processor time【344885532347030†L23-L29】.  We
# therefore set the walltime to 7 days to allow the extractor to run for the
# maximum permitted duration【663148948527848†L18-L22】.
#SBATCH --mem=2G
#SBATCH --time=7-00:00:00
#SBATCH --partition=core96

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
##
# The cluster uses environment modules, but the standard `module` function is
# defined only for login shells.  In batch scripts, call the module command via
# modulecmd so that the appropriate environment variables are set【917054095551098†L24-L33】.
# First purge any previously loaded modules, then load Python and HDF5.
eval $(/usr/bin/modulecmd bash purge)
eval $(/usr/bin/modulecmd bash load python/3.9)
eval $(/usr/bin/modulecmd bash load hdf5)

# Set environment variables for better HDF5 performance
export HDF5_USE_FILE_LOCKING=FALSE
export OMP_NUM_THREADS=1

# Move to submission directory
cd $SLURM_SUBMIT_DIR

# Configuration - adjust these paths as needed
INPUT_DIR="/beegfs/car/lsmith/virac_v2/data/output/ts_tables/"
OUTPUT_DIR="/beegfs/car/njm/virac_lightcurves/"
MIN_KS=20
# Use all of the CPUs allocated to this job for the worker processes.  The
# extractor script uses the `--workers` option to set the number of parallel
# processes; setting this to the Slurm cpus‑per‑task means we utilise the full
# capacity of the allocated core96 node.  If you modify `--ntasks` and
# `--cpus-per-task` in the directives above, adjust this accordingly.
WORKERS="$SLURM_CPUS_PER_TASK"

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
