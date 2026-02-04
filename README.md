# VIRAC Light Curve Extractor

Extracts light curves from VIRAC HDF5 tiles into individual CSV files per source.
Designed for supercomputer execution with robust checkpoint/restart capabilities.

## Features

- **All bands**: Extracts Ks, Z, Y, J, H photometry into unified CSV files
- **Checkpoint/restart**: Survives wall-time limits, tracks completed tiles
- **Multiprocessing**: Configurable parallel workers (32-64 recommended)
- **Quality filter**: Only keeps sources with >20 Ks-band detections
- **Robust**: Detailed error logging, handles corrupted files gracefully

## Installation

No special installation required. Dependencies:
- Python 3.7+
- numpy
- h5py

```bash
# On most HPC systems these are available via module
module load python/3.9
module load hdf5
```

## Usage

### Basic Run

```bash
python virac_lightcurve_extractor.py \
    --input-dir /beegfs/car/lsmith/virac_v2/data/output/ts_tables/ \
    --output-dir /beegfs/car/njm/virac_lightcurves/ \
    --workers 32 \
    --min-ks 20
```

### SLURM Job Script

Create `run_extraction.slurm`:

```bash
#!/bin/bash
#SBATCH --job-name=virac_extract
#SBATCH --output=virac_extract_%j.out
#SBATCH --error=virac_extract_%j.err
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=64
#SBATCH --mem=128G
#SBATCH --time=24:00:00

module load python/3.9
module load hdf5

cd $SLURM_SUBMIT_DIR

python virac_lightcurve_extractor.py \
    --workers 64 \
    --output-dir /beegfs/car/njm/virac_lightcurves/
```

Submit with:
```bash
sbatch run_extraction.slurm
```

### Resuming After Wall-Time

Simply resubmit the same job. The extractor automatically:
1. Reads the checkpoint file
2. Skips already-completed tiles
3. Continues where it left off

```bash
# Just resubmit - it resumes automatically
sbatch run_extraction.slurm
```

### Retrying Failed Tiles

If some tiles failed due to transient errors:

```bash
python virac_lightcurve_extractor.py --retry-failed
```

### Monitoring Progress

While the job is running:

```bash
# One-time status check
python monitor_progress.py

# Continuous monitoring
python monitor_progress.py --watch --interval 30
```

## Output Format

Each source gets a CSV file named `{sourceid}.csv`:

```csv
mjd,ks_mag,ks_err,z_mag,z_err,y_mag,y_err,j_mag,j_err,h_mag,h_err,seeing,exptime,skylevel,ellipticity,chi,ast_res_chisq,detected,filter
55123.456789,12.345,0.012,,,,,,,,,0.85,30.00,1234.56,0.0512,1.23,0.98,1,Ks
55124.567890,12.367,0.015,,,,,,,,,0.92,30.00,1298.23,0.0489,1.15,1.02,1,Ks
55200.123456,,,15.234,0.045,,,,,,,0.78,60.00,2345.67,0.0534,0.95,0.87,1,Z
```

**Columns:**
- `mjd`: Modified Julian Date of observation
- `ks_mag`, `ks_err`: Ks-band magnitude and error (only filled for Ks observations)
- `z_mag`, `z_err`: Z-band magnitude and error
- `y_mag`, `y_err`: Y-band magnitude and error
- `j_mag`, `j_err`: J-band magnitude and error
- `h_mag`, `h_err`: H-band magnitude and error
- `seeing`: Seeing in arcseconds
- `exptime`: Exposure time in seconds
- `skylevel`: Sky background level
- `ellipticity`: PSF ellipticity
- `chi`: Chi-squared of PSF fit
- `ast_res_chisq`: Astrometric residual chi-squared
- `detected`: 1 if detected, 0 if upper limit
- `filter`: Filter name (Ks, Z, Y, J, H)

## Directory Structure

```
/beegfs/car/njm/virac_lightcurves/
├── 123456789012.csv      # Light curve files (named by sourceid)
├── 234567890123.csv
├── ...
├── checkpoints/
│   ├── completed_tiles.json   # Tracks which tiles are done
│   ├── failed_tiles.json      # Tracks failures for retry
│   ├── progress.json          # Overall progress stats
│   └── .checkpoint.lock       # Lock file for concurrent access
└── logs/
    └── extraction_20240101_120000.log
```

## Checkpoint Files

### completed_tiles.json
```json
{
  "completed": ["n1024_12345", "n1024_12346", ...],
  "stats": {
    "n1024_12345": {
      "n_sources": 50000,
      "n_valid": 12500,
      "timestamp": "2024-01-01T12:00:00"
    }
  }
}
```

### progress.json
```json
{
  "total_tiles": 1500,
  "processed_tiles": 750,
  "total_sources": 37500000,
  "valid_sources": 9375000,
  "last_update": "2024-01-01T18:00:00",
  "percent_complete": 50.0
}
```

## Tips for Large Runs

1. **Memory**: Each worker needs ~2-4GB. For 64 workers, request 128-256GB.

2. **Storage**: Expect ~10KB per CSV file. For 10M sources ≈ 100GB.

3. **Time estimate**: ~50-100 tiles/minute with 64 workers. 
   1500 tiles ≈ 15-30 minutes (excluding I/O bottlenecks).

4. **Multiple jobs**: Safe to run multiple jobs simultaneously - 
   checkpoint file uses locking.

5. **Check progress**: Use `monitor_progress.py` to track status.

## Troubleshooting

### Job keeps timing out
- Increase wall time
- Reduce workers (I/O might be saturated)
- Check if specific tiles are slow (large tiles)

### Many failed tiles
- Check `failed_tiles.json` for error patterns
- Use `--retry-failed` after fixing issues
- Check disk space

### CSV files missing
- Check `checkpoints/completed_tiles.json` vs actual files
- A tile might be marked complete but CSV writing failed
- Manually delete checkpoint and rerun that tile

### Lock timeout errors
- Previous job may have crashed holding the lock
- Delete `.checkpoint.lock` file manually
- Rerun

## Contact

For questions about this extractor, contact Nill.
