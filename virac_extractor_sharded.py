#!/usr/bin/env python3
"""
VIRAC Light Curve Extractor - Sharded Version
==============================================
Run multiple instances in parallel by partitioning tiles across jobs.

Usage:
    python virac_extractor_sharded.py --shard 0 --total-shards 4
    python virac_extractor_sharded.py --shard 1 --total-shards 4
    python virac_extractor_sharded.py --shard 2 --total-shards 4
    python virac_extractor_sharded.py --shard 3 --total-shards 4

Each shard processes every Nth tile (where N = total-shards).
All shards share the same checkpoint file safely via file locking.
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

import numpy as np
import h5py

# =============================================================================
# Configuration
# =============================================================================

DEFAULT_INPUT_DIR = "/beegfs/car/lsmith/virac_v2/data/output/ts_tables/"
DEFAULT_OUTPUT_DIR = "/beegfs/car/njm/virac_lightcurves/"
DEFAULT_MIN_KS = 20
DEFAULT_WORKERS = 32

CATINDEX_COLS = ["mjdobs", "filter", "seeing", "ellipticity", "exptime", 
                 "skylevel", "tile", "tileloc", "filename"]

CSV_HEADER = (
    "mjd,ks_mag,ks_err,z_mag,z_err,y_mag,y_err,j_mag,j_err,h_mag,h_err,"
    "seeing,exptime,skylevel,ellipticity,chi,ast_res_chisq,detected,filter\n"
)

# =============================================================================
# Checkpoint Manager (same as original)
# =============================================================================

class CheckpointManager:
    def __init__(self, checkpoint_dir: str):
        self.checkpoint_dir = Path(checkpoint_dir)
        self.checkpoint_dir.mkdir(parents=True, exist_ok=True)
        self.completed_file = self.checkpoint_dir / "completed_tiles.json"
        self.failed_file = self.checkpoint_dir / "failed_tiles.json"
        self.progress_file = self.checkpoint_dir / "progress.json"
        self.lock_file = self.checkpoint_dir / ".checkpoint.lock"
        
    def _acquire_lock(self, timeout: float = 30.0) -> int:
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
        fcntl.flock(fd, fcntl.LOCK_UN)
        os.close(fd)
    
    def _load_json(self, filepath: Path) -> dict:
        if filepath.exists():
            try:
                with open(filepath, 'r') as f:
                    return json.load(f)
            except:
                return {}
        return {}
    
    def _save_json(self, filepath: Path, data: dict):
            # Use PID to ensure unique temp file per process
            temp_file = filepath.with_suffix(f'.tmp.{os.getpid()}') 
            with open(temp_file, 'w') as f:
                json.dump(data, f, indent=2)
            temp_file.replace(filepath)
    
    
    def get_completed_tiles(self) -> set:
        fd = self._acquire_lock()
        try:
            data = self._load_json(self.completed_file)
            return set(data.get("completed", []))
        finally:
            self._release_lock(fd)
    
    def mark_tile_completed(self, tile_id: str, n_sources: int, n_valid: int):
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
        fd = self._acquire_lock()
        try:
            data = self._load_json(self.failed_file)
            if "failed" not in data:
                data["failed"] = {}
            data["failed"][tile_id] = {
                "error": error[:500],
                "timestamp": datetime.now().isoformat()
            }
            self._save_json(self.failed_file, data)
        finally:
            self._release_lock(fd)


# =============================================================================
# Light Curve Extraction (same as original)
# =============================================================================

def extract_source_lightcurve(lc: h5py.File, idx: int, 
                              catidx_data: Dict[str, np.ndarray]) -> Optional[np.ndarray]:
    try:
        ci_idx = lc["timeSeries/catindexid"][idx]
        ci_idx_covered = lc["timeSeries/catindexidcovered"][idx]
        
        if ci_idx.size == 0:
            return None
        
        ci_idx_nondet = ci_idx_covered[~np.isin(ci_idx_covered, ci_idx)]
        n_det = ci_idx.size
        n_nondet = ci_idx_nondet.size
        n_total = n_det + n_nondet
        
        if n_total == 0:
            return None
        
        dtype = [
            ('mjd', np.float64),
            ('ks_mag', np.float32), ('ks_err', np.float32),
            ('z_mag', np.float32), ('z_err', np.float32),
            ('y_mag', np.float32), ('y_err', np.float32),
            ('j_mag', np.float32), ('j_err', np.float32),
            ('h_mag', np.float32), ('h_err', np.float32),
            ('seeing', np.float32), ('exptime', np.float32),
            ('skylevel', np.float32), ('ellipticity', np.float32),
            ('chi', np.float32), ('ast_res_chisq', np.float32),
            ('detected', np.int8), ('filter', 'U2')
        ]
        
        output = np.zeros(n_total, dtype=dtype)
        
        for col in ['ks_mag', 'ks_err', 'z_mag', 'z_err', 'y_mag', 'y_err',
                    'j_mag', 'j_err', 'h_mag', 'h_err', 'chi', 'ast_res_chisq']:
            output[col] = np.nan
        
        ci_all = np.concatenate([ci_idx, ci_idx_nondet])
        
        output['mjd'] = catidx_data['mjdobs'][ci_all]
        output['seeing'] = catidx_data['seeing'][ci_all]
        output['exptime'] = catidx_data['exptime'][ci_all]
        output['skylevel'] = catidx_data['skylevel'][ci_all]
        output['ellipticity'] = catidx_data['ellipticity'][ci_all]
        
        filters_bytes = catidx_data['filter'][ci_all]
        if filters_bytes.dtype.kind == 'S':
            filters_str = np.array([f.decode('utf-8', errors='ignore').strip() 
                                   for f in filters_bytes])
        else:
            filters_str = np.array([str(f).strip() for f in filters_bytes])
        
        output['filter'] = filters_str
        
        ts_mag = lc["timeSeries/hfad_mag"][idx]
        ts_emag = lc["timeSeries/hfad_emag"][idx]
        ts_chi = lc["timeSeries/chi"][idx]
        ts_ast = lc["timeSeries/ast_res_chisq"][idx]
        
        output['detected'][:n_det] = 1
        output['chi'][:n_det] = ts_chi
        output['ast_res_chisq'][:n_det] = ts_ast
        
        det_filters = filters_str[:n_det]
        filter_col_map = {
            'Ks': ('ks_mag', 'ks_err'), 'Z': ('z_mag', 'z_err'),
            'Y': ('y_mag', 'y_err'), 'J': ('j_mag', 'j_err'),
            'H': ('h_mag', 'h_err')
        }
        
        for filt, (mag_col, err_col) in filter_col_map.items():
            mask = (det_filters == filt)
            if np.any(mask):
                indices = np.where(mask)[0]
                output[mag_col][indices] = ts_mag[mask]
                output[err_col][indices] = ts_emag[mask]
        
        return output
    except:
        return None


def count_ks_detections(lc: h5py.File, idx: int, cat_filter: np.ndarray) -> int:
    try:
        ci_idx = lc["timeSeries/catindexid"][idx]
        if ci_idx.size == 0:
            return 0
        return np.count_nonzero(cat_filter[ci_idx] == b'Ks')
    except:
        return 0



def get_output_path(source_id, output_dir):
    """Create hierarchical path to avoid directory explosion"""
    # Use first 2-3 digits of source_id as subdirs
    # Example: source_id = 8365035120893
    # -> output_dir/836/503/8365035120893.csv
    
    source_str = str(source_id)
    subdir1 = source_str[:3]  # First 3 digits
    subdir2 = source_str[3:6] # Next 3 digits
    
    subdir_path = os.path.join(output_dir, subdir1, subdir2)
    os.makedirs(subdir_path, exist_ok=True)
    
    return os.path.join(subdir_path, f"{source_id}.csv")



def write_lightcurve_csv(output_dir: Path, sourceid: int, data: np.ndarray) -> bool:
    try:
        filepath = get_output_path(sourceid, output_dir)
        with open(filepath, 'w') as f:
            f.write(CSV_HEADER)
            for row in data:
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
                ).replace('nan', '')
                f.write(line)
        return True
    except:
        return False


def process_tile(args: Tuple[str, str, str, int]) -> Tuple[str, int, int, str]:
    h5_path, output_dir, checkpoint_dir, min_ks = args
    
    basename = os.path.basename(h5_path)
    tile_id = os.path.splitext(basename)[0]
    output_dir = Path(output_dir)
    
    n_sources = 0
    n_valid = 0
    error_msg = ""
    
    try:
        with h5py.File(h5_path, 'r') as lc:
            catidx_data = {col: lc[f"catIndex/{col}"][:] for col in CATINDEX_COLS}
            cat_filter = catidx_data['filter']
            sourceids = lc["sourceList/sourceid"][:]
            n_sources = len(sourceids)
            
            valid_indices = []
            for idx in range(n_sources):
                n_ks = count_ks_detections(lc, idx, cat_filter)
                if n_ks > min_ks:
                    valid_indices.append(idx)
            
            for idx in valid_indices:
                sourceid = sourceids[idx]
                csv_path = Path(get_output_path(sourceid, output_dir))
                if csv_path.exists():
                    n_valid += 1
                    continue
                
                lc_data = extract_source_lightcurve(lc, idx, catidx_data)
                if lc_data is not None and len(lc_data) > 0:
                    if write_lightcurve_csv(output_dir, sourceid, lc_data):
                        n_valid += 1
        
        # CRITICAL: If we had valid candidates but saved nothing, that's a FAILURE
        if len(valid_indices) > 0 and n_valid == 0:
            error_msg = f"SILENT_FAIL: {len(valid_indices)} candidates but 0 saved"
            return (tile_id, n_sources, n_valid, error_msg)

        return (tile_id, n_sources, n_valid, "")
    except Exception as e:
        error_msg = f"{type(e).__name__}: {str(e)}"
        return (tile_id, n_sources, n_valid, error_msg)


# =============================================================================
# Main with Sharding
# =============================================================================

def main():
    parser = argparse.ArgumentParser(
        description="VIRAC Light Curve Extractor - Sharded Version"
    )
    parser.add_argument("--input-dir", "-i", default=DEFAULT_INPUT_DIR)
    parser.add_argument("--output-dir", "-o", default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--workers", "-w", type=int, default=DEFAULT_WORKERS)
    parser.add_argument("--min-ks", "-m", type=int, default=DEFAULT_MIN_KS)
    parser.add_argument("--shard", "-s", type=int, required=True,
                        help="Shard index (0-indexed)")
    parser.add_argument("--total-shards", "-t", type=int, required=True,
                        help="Total number of shards")
    parser.add_argument("--tile-pattern", default="n*_*.hdf5")
    
    args = parser.parse_args()
    
    if args.shard < 0 or args.shard >= args.total_shards:
        print(f"ERROR: shard must be 0 to {args.total_shards - 1}")
        sys.exit(1)
    
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    checkpoint_dir = output_dir / "checkpoints"
    
    # Setup logging for this shard
    log_dir = output_dir / "logs"
    log_dir.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = log_dir / f"shard{args.shard}_{timestamp}.log"
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler()
        ]
    )
    logger = logging.getLogger()
    
    ckpt = CheckpointManager(str(checkpoint_dir))
    
    # Get all tiles and select this shard's portion
    all_tiles = sorted(glob.glob(os.path.join(args.input_dir, args.tile_pattern)))
    shard_tiles = [t for i, t in enumerate(all_tiles) if i % args.total_shards == args.shard]
    
    logger.info("=" * 70)
    logger.info(f"VIRAC Extractor - Shard {args.shard}/{args.total_shards}")
    logger.info("=" * 70)
    logger.info(f"Total tiles in dataset: {len(all_tiles)}")
    logger.info(f"Tiles for this shard:   {len(shard_tiles)}")
    logger.info(f"Workers: {args.workers}")
    
    # Filter completed
    completed_tiles = ckpt.get_completed_tiles()
    pending_tiles = [t for t in shard_tiles 
                     if os.path.splitext(os.path.basename(t))[0] not in completed_tiles]
    
    logger.info(f"Already completed: {len(shard_tiles) - len(pending_tiles)}")
    logger.info(f"Tiles to process:  {len(pending_tiles)}")
    
    if len(pending_tiles) == 0:
        logger.info("All tiles for this shard already processed!")
        sys.exit(0)
    
    worker_args = [(tf, str(output_dir), str(checkpoint_dir), args.min_ks)
                   for tf in pending_tiles]
    
    processed = 0
    start_time = datetime.now()
    
    with mp.Pool(processes=args.workers) as pool:
        for result in pool.imap_unordered(process_tile, worker_args, chunksize=1):
            tile_id, n_sources, n_valid, error_msg = result
            processed += 1
            
            if error_msg:
                logger.warning(f"[{processed}/{len(pending_tiles)}] {tile_id}: FAILED - {error_msg}")
                ckpt.mark_tile_failed(tile_id, error_msg)
            else:
                logger.info(f"[{processed}/{len(pending_tiles)}] {tile_id}: {n_valid}/{n_sources}")
                ckpt.mark_tile_completed(tile_id, n_sources, n_valid)
    
    elapsed = datetime.now() - start_time
    logger.info("-" * 70)
    logger.info(f"Shard {args.shard} complete! Processed {processed} tiles in {elapsed}")


if __name__ == "__main__":
    main()
