#!/usr/bin/env python3
"""
VIRAC Light Curve Extractor
===========================
Extracts light curves from VIRAC HDF5 tiles into individual CSV files per source.
Designed for supercomputer execution with checkpoint/restart capabilities.

Features:
- Processes all bands (Ks, Z, Y, J, H) into unified CSV files
- Checkpoint/restart: tracks completed tiles, survives wall-time limits
- Multiprocessing with configurable worker count
- Only keeps sources with >20 Ks-band detections
- Robust error handling with detailed logging

Output CSV format per source:
    mjd, ks_mag, ks_err, z_mag, z_err, y_mag, y_err, j_mag, j_err, h_mag, h_err,
    seeing, exptime, skylevel, ellipticity, chi, ast_res_chisq, detected

Usage:
    python virac_lightcurve_extractor.py [--workers N] [--output-dir PATH] [--min-ks N]
"""

import os
import sys
import glob
import json
import argparse
import logging
import multiprocessing as mp
from datetime import datetime
from pathlib import Path
from typing import Optional, Tuple, List, Dict
import fcntl
import time
import traceback

import numpy as np
import h5py

# =============================================================================
# Configuration
# =============================================================================

DEFAULT_INPUT_DIR = "/beegfs/car/lsmith/virac_v2/data/output/ts_tables/"
DEFAULT_OUTPUT_DIR = "/beegfs/car/njm/virac_lightcurves/"
DEFAULT_MIN_KS = 20
DEFAULT_WORKERS = 32

# Filters in VIRAC data
FILTERS = ['Ks', 'Z', 'Y', 'J', 'H']

# Columns from catIndex (observation metadata)
CATINDEX_COLS = ["mjdobs", "filter", "seeing", "ellipticity", "exptime", 
                 "skylevel", "tile", "tileloc", "filename"]

# Columns from timeSeries (detection data)
TIMESERIES_COLS = ["hfad_mag", "hfad_emag", "chi", "ast_res_chisq", 
                   "ambiguous_match", "cnf_ctr", "diff_fit_ap", "ext", 
                   "objtype", "sky", "x", "y"]

# CSV header
CSV_HEADER = (
    "mjd,ks_mag,ks_err,z_mag,z_err,y_mag,y_err,j_mag,j_err,h_mag,h_err,"
    "seeing,exptime,skylevel,ellipticity,chi,ast_res_chisq,detected,filter\n"
)

# =============================================================================
# Checkpoint Manager
# =============================================================================

class CheckpointManager:
    """
    Thread-safe checkpoint manager for tracking completed tiles.
    Uses file locking for safe concurrent access.
    """
    
    def __init__(self, checkpoint_dir: str):
        self.checkpoint_dir = Path(checkpoint_dir)
        self.checkpoint_dir.mkdir(parents=True, exist_ok=True)
        
        self.completed_file = self.checkpoint_dir / "completed_tiles.json"
        self.failed_file = self.checkpoint_dir / "failed_tiles.json"
        self.progress_file = self.checkpoint_dir / "progress.json"
        self.lock_file = self.checkpoint_dir / ".checkpoint.lock"
        
    def _acquire_lock(self, timeout: float = 30.0) -> int:
        """Acquire file lock with timeout."""
        fd = os.open(str(self.lock_file), os.O_RDWR | os.O_CREAT)
        start = time.time()
        while True:
            try:
                fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
                return fd
            except (IOError, OSError):
                if time.time() - start > timeout:
                    os.close(fd)
                    raise TimeoutError("Could not acquire checkpoint lock")
                time.sleep(0.1)
    
    def _release_lock(self, fd: int):
        """Release file lock."""
        fcntl.flock(fd, fcntl.LOCK_UN)
        os.close(fd)
    
    def _load_json(self, filepath: Path) -> dict:
        """Load JSON file, return empty dict if not exists."""
        if filepath.exists():
            try:
                with open(filepath, 'r') as f:
                    return json.load(f)
            except (json.JSONDecodeError, IOError):
                return {}
        return {}
    
    def _save_json(self, filepath: Path, data: dict):
        """Save JSON file atomically."""
        temp_file = filepath.with_suffix('.tmp')
        with open(temp_file, 'w') as f:
            json.dump(data, f, indent=2)
        temp_file.replace(filepath)
    
    def get_completed_tiles(self) -> set:
        """Get set of completed tile IDs."""
        fd = self._acquire_lock()
        try:
            data = self._load_json(self.completed_file)
            return set(data.get("completed", []))
        finally:
            self._release_lock(fd)
    
    def mark_tile_completed(self, tile_id: str, n_sources: int, n_valid: int):
        """Mark a tile as completed with stats."""
        fd = self._acquire_lock()
        try:
            data = self._load_json(self.completed_file)
            if "completed" not in data:
                data["completed"] = []
            if "stats" not in data:
                data["stats"] = {}
            
            if tile_id not in data["completed"]:
                data["completed"].append(tile_id)
            
            data["stats"][tile_id] = {
                "n_sources": n_sources,
                "n_valid": n_valid,
                "timestamp": datetime.now().isoformat()
            }
            
            self._save_json(self.completed_file, data)
        finally:
            self._release_lock(fd)
    
    def mark_tile_failed(self, tile_id: str, error: str):
        """Mark a tile as failed with error message."""
        fd = self._acquire_lock()
        try:
            data = self._load_json(self.failed_file)
            if "failed" not in data:
                data["failed"] = {}
            
            data["failed"][tile_id] = {
                "error": error[:500],  # Truncate long errors
                "timestamp": datetime.now().isoformat()
            }
            
            self._save_json(self.failed_file, data)
        finally:
            self._release_lock(fd)
    
    def update_progress(self, total_tiles: int, processed_tiles: int, 
                       total_sources: int, valid_sources: int):
        """Update overall progress."""
        fd = self._acquire_lock()
        try:
            data = {
                "total_tiles": total_tiles,
                "processed_tiles": processed_tiles,
                "total_sources": total_sources,
                "valid_sources": valid_sources,
                "last_update": datetime.now().isoformat(),
                "percent_complete": round(100 * processed_tiles / max(total_tiles, 1), 2)
            }
            self._save_json(self.progress_file, data)
        finally:
            self._release_lock(fd)
    
    def get_failed_tiles(self) -> List[str]:
        """Get list of failed tile IDs for retry."""
        fd = self._acquire_lock()
        try:
            data = self._load_json(self.failed_file)
            return list(data.get("failed", {}).keys())
        finally:
            self._release_lock(fd)


# =============================================================================
# Light Curve Extraction
# =============================================================================

def extract_source_lightcurve(lc: h5py.File, idx: int, 
                              catidx_data: Dict[str, np.ndarray]) -> Optional[np.ndarray]:
    """
    Extract light curve for a single source across all filters.
    
    Parameters
    ----------
    lc : h5py.File
        Open HDF5 file handle
    idx : int
        Source index in the file
    catidx_data : dict
        Pre-loaded catIndex data arrays
    
    Returns
    -------
    np.ndarray or None
        Structured array with light curve data, or None if invalid
    """
    try:
        # Get detection and coverage indices
        ci_idx = lc["timeSeries/catindexid"][idx]
        ci_idx_covered = lc["timeSeries/catindexidcovered"][idx]
        
        if ci_idx.size == 0:
            return None
        
        # Non-detections are in covered but not in detected
        ci_idx_nondet = ci_idx_covered[~np.isin(ci_idx_covered, ci_idx)]
        
        n_det = ci_idx.size
        n_nondet = ci_idx_nondet.size
        n_total = n_det + n_nondet
        
        if n_total == 0:
            return None
        
        # Build output dtype
        dtype = [
            ('mjd', np.float64),
            ('ks_mag', np.float32), ('ks_err', np.float32),
            ('z_mag', np.float32), ('z_err', np.float32),
            ('y_mag', np.float32), ('y_err', np.float32),
            ('j_mag', np.float32), ('j_err', np.float32),
            ('h_mag', np.float32), ('h_err', np.float32),
            ('seeing', np.float32),
            ('exptime', np.float32),
            ('skylevel', np.float32),
            ('ellipticity', np.float32),
            ('chi', np.float32),
            ('ast_res_chisq', np.float32),
            ('detected', np.int8),
            ('filter', 'U2')
        ]
        
        output = np.zeros(n_total, dtype=dtype)
        
        # Initialize with NaN for magnitude columns
        for col in ['ks_mag', 'ks_err', 'z_mag', 'z_err', 'y_mag', 'y_err',
                    'j_mag', 'j_err', 'h_mag', 'h_err', 'chi', 'ast_res_chisq']:
            output[col] = np.nan
        
        # Combined indices: detections first, then non-detections
        ci_all = np.concatenate([ci_idx, ci_idx_nondet])
        
        # Fill observation metadata from catIndex
        output['mjd'] = catidx_data['mjdobs'][ci_all]
        output['seeing'] = catidx_data['seeing'][ci_all]
        output['exptime'] = catidx_data['exptime'][ci_all]
        output['skylevel'] = catidx_data['skylevel'][ci_all]
        output['ellipticity'] = catidx_data['ellipticity'][ci_all]
        
        # Get filter for each observation
        filters_bytes = catidx_data['filter'][ci_all]
        
        # Decode filter bytes to string
        if filters_bytes.dtype.kind == 'S':  # bytes
            filters_str = np.array([f.decode('utf-8', errors='ignore').strip() 
                                   for f in filters_bytes])
        else:
            filters_str = np.array([str(f).strip() for f in filters_bytes])
        
        output['filter'] = filters_str
        
        # Get detection data from timeSeries
        ts_mag = lc["timeSeries/hfad_mag"][idx]
        ts_emag = lc["timeSeries/hfad_emag"][idx]
        ts_chi = lc["timeSeries/chi"][idx]
        ts_ast = lc["timeSeries/ast_res_chisq"][idx]
        
        # Fill detection-related columns (only for detected epochs)
        output['detected'][:n_det] = 1
        output['chi'][:n_det] = ts_chi
        output['ast_res_chisq'][:n_det] = ts_ast
        
        # Map magnitudes to appropriate filter columns for detections
        det_filters = filters_str[:n_det]
        
        filter_col_map = {
            'Ks': ('ks_mag', 'ks_err'),
            'Z': ('z_mag', 'z_err'),
            'Y': ('y_mag', 'y_err'),
            'J': ('j_mag', 'j_err'),
            'H': ('h_mag', 'h_err')
        }
        
        for filt, (mag_col, err_col) in filter_col_map.items():
            mask = (det_filters == filt)
            if np.any(mask):
                indices = np.where(mask)[0]
                output[mag_col][indices] = ts_mag[mask]
                output[err_col][indices] = ts_emag[mask]
        
        return output
        
    except Exception as e:
        return None


def count_ks_detections(lc: h5py.File, idx: int, 
                        cat_filter: np.ndarray) -> int:
    """
    Count Ks-band detections for a source.
    
    Parameters
    ----------
    lc : h5py.File
        Open HDF5 file handle
    idx : int
        Source index
    cat_filter : np.ndarray
        Pre-loaded filter array from catIndex
    
    Returns
    -------
    int
        Number of Ks-band detections
    """
    try:
        ci_idx = lc["timeSeries/catindexid"][idx]
        if ci_idx.size == 0:
            return 0
        return np.count_nonzero(cat_filter[ci_idx] == b'Ks')
    except:
        return 0


def write_lightcurve_csv(output_dir: Path, sourceid: int, 
                         data: np.ndarray) -> bool:
    """
    Write light curve data to CSV file.
    
    Parameters
    ----------
    output_dir : Path
        Output directory
    sourceid : int
        Source ID (used as filename)
    data : np.ndarray
        Structured array with light curve data
    
    Returns
    -------
    bool
        True if successful
    """
    try:
        filepath = output_dir / f"{sourceid}.csv"
        
        with open(filepath, 'w') as f:
            f.write(CSV_HEADER)
            
            for row in data:
                # Format: mjd, ks_mag, ks_err, z_mag, z_err, y_mag, y_err, 
                #         j_mag, j_err, h_mag, h_err, seeing, exptime, skylevel,
                #         ellipticity, chi, ast_res_chisq, detected, filter
                line = (
                    f"{row['mjd']:.6f},"
                    f"{row['ks_mag']:.4f},{row['ks_err']:.4f},"
                    f"{row['z_mag']:.4f},{row['z_err']:.4f},"
                    f"{row['y_mag']:.4f},{row['y_err']:.4f},"
                    f"{row['j_mag']:.4f},{row['j_err']:.4f},"
                    f"{row['h_mag']:.4f},{row['h_err']:.4f},"
                    f"{row['seeing']:.3f},{row['exptime']:.2f},"
                    f"{row['skylevel']:.2f},{row['ellipticity']:.4f},"
                    f"{row['chi']:.4f},{row['ast_res_chisq']:.4f},"
                    f"{row['detected']},{row['filter']}\n"
                )
                # Replace 'nan' with empty string for cleaner CSV
                line = line.replace('nan', '')
                f.write(line)
        
        return True
        
    except Exception as e:
        return False


def process_tile(args: Tuple[str, str, str, int]) -> Tuple[str, int, int, str]:
    """
    Process a single HDF5 tile file.
    
    Parameters
    ----------
    args : tuple
        (h5_path, output_dir, checkpoint_dir, min_ks)
    
    Returns
    -------
    tuple
        (tile_id, n_sources, n_valid, error_msg)
    """
    h5_path, output_dir, checkpoint_dir, min_ks = args
    
    basename = os.path.basename(h5_path)
    tile_id = os.path.splitext(basename)[0]
    output_dir = Path(output_dir)
    
    n_sources = 0
    n_valid = 0
    error_msg = ""
    
    try:
        with h5py.File(h5_path, 'r') as lc:
            # Pre-load catIndex data (loaded once per tile)
            catidx_data = {col: lc[f"catIndex/{col}"][:] for col in CATINDEX_COLS}
            cat_filter = catidx_data['filter']
            
            # Get source list
            sourceids = lc["sourceList/sourceid"][:]
            n_sources = len(sourceids)
            
            # First pass: find sources with enough Ks detections
            valid_indices = []
            for idx in range(n_sources):
                n_ks = count_ks_detections(lc, idx, cat_filter)
                if n_ks > min_ks:
                    valid_indices.append(idx)
            
            # Second pass: extract and write light curves
            for idx in valid_indices:
                sourceid = sourceids[idx]
                
                # Skip if already exists
                csv_path = output_dir / f"{sourceid}.csv"
                if csv_path.exists():
                    n_valid += 1
                    continue
                
                # Extract light curve
                lc_data = extract_source_lightcurve(lc, idx, catidx_data)
                
                if lc_data is not None and len(lc_data) > 0:
                    if write_lightcurve_csv(output_dir, sourceid, lc_data):
                        n_valid += 1
        
        return (tile_id, n_sources, n_valid, "")
        
    except Exception as e:
        error_msg = f"{type(e).__name__}: {str(e)}"
        return (tile_id, n_sources, n_valid, error_msg)


# =============================================================================
# Main Processing Loop
# =============================================================================

def setup_logging(output_dir: str) -> logging.Logger:
    """Setup logging to both file and console."""
    log_dir = Path(output_dir) / "logs"
    log_dir.mkdir(parents=True, exist_ok=True)
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = log_dir / f"extraction_{timestamp}.log"
    
    logger = logging.getLogger("virac_extractor")
    logger.setLevel(logging.INFO)
    
    # File handler
    fh = logging.FileHandler(log_file)
    fh.setLevel(logging.INFO)
    
    # Console handler
    ch = logging.StreamHandler()
    ch.setLevel(logging.INFO)
    
    # Formatter
    formatter = logging.Formatter(
        '%(asctime)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    fh.setFormatter(formatter)
    ch.setFormatter(formatter)
    
    logger.addHandler(fh)
    logger.addHandler(ch)
    
    return logger


def main():
    parser = argparse.ArgumentParser(
        description="Extract VIRAC light curves to individual CSV files",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    parser.add_argument(
        "--input-dir", "-i",
        default=DEFAULT_INPUT_DIR,
        help="Input directory containing HDF5 tile files"
    )
    parser.add_argument(
        "--output-dir", "-o",
        default=DEFAULT_OUTPUT_DIR,
        help="Output directory for CSV files"
    )
    parser.add_argument(
        "--workers", "-w",
        type=int,
        default=DEFAULT_WORKERS,
        help="Number of parallel workers"
    )
    parser.add_argument(
        "--min-ks", "-m",
        type=int,
        default=DEFAULT_MIN_KS,
        help="Minimum number of Ks-band detections required"
    )
    parser.add_argument(
        "--retry-failed",
        action="store_true",
        help="Retry previously failed tiles"
    )
    parser.add_argument(
        "--tile-pattern",
        default="n*_*.hdf5",
        help="Glob pattern for tile files"
    )
    
    args = parser.parse_args()
    
    # Setup directories
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    
    checkpoint_dir = output_dir / "checkpoints"
    
    # Setup logging
    logger = setup_logging(args.output_dir)
    
    # Initialize checkpoint manager
    ckpt = CheckpointManager(str(checkpoint_dir))
    
    # Find all tile files
    tile_files = sorted(glob.glob(os.path.join(args.input_dir, args.tile_pattern)))
    total_tiles = len(tile_files)
    
    logger.info("=" * 70)
    logger.info("VIRAC Light Curve Extractor")
    logger.info("=" * 70)
    logger.info(f"Input directory:  {args.input_dir}")
    logger.info(f"Output directory: {args.output_dir}")
    logger.info(f"Workers:          {args.workers}")
    logger.info(f"Min Ks points:    {args.min_ks}")
    logger.info(f"Total tiles:      {total_tiles}")
    
    if total_tiles == 0:
        logger.error("No tile files found!")
        sys.exit(1)
    
    # Get completed tiles
    completed_tiles = ckpt.get_completed_tiles()
    logger.info(f"Previously completed: {len(completed_tiles)} tiles")
    
    # Optionally retry failed tiles
    if args.retry_failed:
        failed_tiles = ckpt.get_failed_tiles()
        logger.info(f"Retrying {len(failed_tiles)} failed tiles")
        # Remove failed tiles from completed set so they get reprocessed
        completed_tiles -= set(failed_tiles)
    
    # Filter to only pending tiles
    pending_tiles = []
    for tf in tile_files:
        tile_id = os.path.splitext(os.path.basename(tf))[0]
        if tile_id not in completed_tiles:
            pending_tiles.append(tf)
    
    logger.info(f"Tiles to process: {len(pending_tiles)}")
    
    if len(pending_tiles) == 0:
        logger.info("All tiles already processed!")
        sys.exit(0)
    
    # Prepare arguments for workers
    worker_args = [
        (tf, str(output_dir), str(checkpoint_dir), args.min_ks)
        for tf in pending_tiles
    ]
    
    # Process tiles
    total_sources = 0
    total_valid = 0
    processed = 0
    
    start_time = datetime.now()
    
    logger.info(f"Starting processing at {start_time.isoformat()}")
    logger.info("-" * 70)
    
    # Use imap_unordered for better progress reporting
    with mp.Pool(processes=args.workers) as pool:
        for result in pool.imap_unordered(process_tile, worker_args, chunksize=1):
            tile_id, n_sources, n_valid, error_msg = result
            processed += 1
            total_sources += n_sources
            total_valid += n_valid
            
            if error_msg:
                logger.warning(f"[{processed}/{len(pending_tiles)}] {tile_id}: "
                             f"FAILED - {error_msg}")
                ckpt.mark_tile_failed(tile_id, error_msg)
            else:
                logger.info(f"[{processed}/{len(pending_tiles)}] {tile_id}: "
                          f"{n_valid}/{n_sources} sources extracted")
                ckpt.mark_tile_completed(tile_id, n_sources, n_valid)
            
            # Update progress every 10 tiles
            if processed % 10 == 0:
                ckpt.update_progress(
                    total_tiles, 
                    len(completed_tiles) + processed,
                    total_sources,
                    total_valid
                )
    
    # Final summary
    end_time = datetime.now()
    elapsed = end_time - start_time
    
    logger.info("-" * 70)
    logger.info("Processing complete!")
    logger.info(f"Time elapsed:     {elapsed}")
    logger.info(f"Tiles processed:  {processed}")
    logger.info(f"Total sources:    {total_sources}")
    logger.info(f"Valid sources:    {total_valid}")
    logger.info(f"Tiles/minute:     {processed / max(elapsed.total_seconds() / 60, 1):.2f}")
    
    # Final progress update
    ckpt.update_progress(
        total_tiles,
        len(completed_tiles) + processed,
        total_sources,
        total_valid
    )


if __name__ == "__main__":
    main()
