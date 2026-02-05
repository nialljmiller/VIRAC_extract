#!/usr/bin/env python3
"""
VIRAC Extraction Progress Monitor (Dashboard Edition)
=====================================================
- Global Stats & Completion %
- Live Status of Each Shard (Active Tile & Speed)
- PRIMVS Target Coverage
- Data Integrity Checks
"""

import os
import sys
import json
import argparse
import subprocess
import random
import csv
import re
import glob
from pathlib import Path
from datetime import datetime
import time

DEFAULT_OUTPUT_DIR = "/beegfs/car/njm/virac_lightcurves/"
PRIMVS_FILENAME = "PRIMVS_ID.csv"

# =============================================================================
# Core Utilities
# =============================================================================

def load_json_safe(filepath: Path) -> dict:
    if not filepath.exists():
        return {}
    try:
        with open(filepath, 'r') as f:
            return json.load(f)
    except:
        return {}

def get_latest_logs(directory: Path) -> dict:
    """
    Finds the most recent .err log file for each shard index.
    Returns: {shard_index: filepath}
    """
    shard_logs = {}
    pattern = re.compile(r"virac_shard_(\d+)_(\d+)\.err")
    
    # glob all .err files in the current directory (where script is running)
    # or look in output_dir/logs if that's where they are.
    # Based on your 'll', they are in the current dir.
    log_files = glob.glob("virac_shard_*_*.err")
    
    for log_file in log_files:
        match = pattern.search(log_file)
        if match:
            job_id = int(match.group(1))
            shard_idx = int(match.group(2))
            
            # Keep only the newest job_id for this shard index
            if shard_idx not in shard_logs:
                shard_logs[shard_idx] = (job_id, log_file)
            else:
                current_best_job = shard_logs[shard_idx][0]
                if job_id > current_best_job:
                    shard_logs[shard_idx] = (job_id, log_file)
    
    return {k: v[1] for k, v in shard_logs.items()}

def parse_shard_status(log_file: str) -> dict:
    """Reads the tail of a log file to extract current status."""
    status = {"tile": "Starting...", "progress": "N/A", "updated": "N/A"}
    
    try:
        fpath = Path(log_file)
        if not fpath.exists():
            return status
            
        status["updated"] = datetime.fromtimestamp(fpath.stat().st_mtime).strftime('%H:%M:%S')
        
        # Read last 2KB
        file_size = fpath.stat().st_size
        read_size = min(2048, file_size)
        
        with open(fpath, 'rb') as f:
            if file_size > read_size:
                f.seek(-read_size, 2)
            lines = f.read().decode('utf-8', errors='ignore').splitlines()
            
        # Find last INFO line with tile progress
        # Format: "INFO - [55/5530] n1024_8954892: 9856/13849"
        prog_pattern = re.compile(r"\[(\d+/\d+)\]\s+(n\d+_\d+):\s+(\d+/\d+)")
        
        for line in reversed(lines):
            match = prog_pattern.search(line)
            if match:
                status["tile"] = match.group(2)
                status["progress"] = match.group(3)
                # Add batch progress too
                status["batch"] = match.group(1)
                break
                
    except Exception:
        pass
    return status

# =============================================================================
# Coverage & QC
# =============================================================================

def get_primvs_coverage(output_dir: Path) -> dict:
    script_dir = Path(__file__).parent.resolve()
    primvs_path = script_dir / PRIMVS_FILENAME
    
    result = {"found": False, "hits": 0, "checked": 0, "pct": 0.0, "note": ""}
    if not primvs_path.exists(): return result
    result["found"] = True

    try:
        with open(primvs_path, 'r') as f:
            # Quick check for header
            sample = f.read(1024); f.seek(0)
            has_header = csv.Sniffer().has_header(sample)
            
            if has_header:
                reader = csv.DictReader(f)
                headers = [h.strip().lower() for h in reader.fieldnames]
                id_col = next((h for h in headers if "sourceid" in h), reader.fieldnames[0])
                f.seek(0); reader = csv.DictReader(f)
                ids = [row[id_col].strip() for row in reader if row[id_col]]
            else:
                ids = [line.split(',')[0].strip() for line in f if line.strip()]
                
        if len(ids) > 5000:
            check_ids = random.sample(ids, 2000)
            result["note"] = "(Sampled 2k)"
        else:
            check_ids = ids
            result["note"] = "(Exact)"
            
        result["checked"] = len(check_ids)
        hits = sum(1 for sid in check_ids if (output_dir / f"{sid}.csv").exists())
        result["hits"] = hits
        if result["checked"] > 0:
            result["pct"] = (hits / result["checked"]) * 100
            
    except Exception: pass
    return result

def inspect_sample(output_dir: Path) -> dict:
    try:
        # Fast scandir for 1 file
        with os.scandir(output_dir) as entries:
            for entry in entries:
                if entry.name.endswith('.csv'):
                    fpath = Path(entry.path)
                    sz = fpath.stat().st_size
                    with open(fpath, 'r') as f:
                        head = f.readline().strip()
                        row = f.readline().strip()
                    return {"name": entry.name, "size": sz, "valid": sz > 0 and "mjd" in head, "row": row}
    except: pass
    return None

# =============================================================================
# Main Display
# =============================================================================

def display_progress(output_dir: str, clear: bool = True):
    output_dir = Path(output_dir)
    checkpoint_dir = output_dir / "checkpoints"
    
    if clear: os.system('clear' if os.name == 'posix' else 'cls')
    
    print("=" * 80)
    print(f"VIRAC EXTRACTOR DASHBOARD  |  {datetime.now().strftime('%H:%M:%S')}")
    print("=" * 80)
    
    # --- Global Stats ---
    stats = load_json_safe(checkpoint_dir / "completed_tiles.json").get("stats", {})
    completed_len = len(stats)
    total_len = 22585
    pct = (completed_len / total_len * 100)
    
    n_src = sum(x.get("n_sources", 0) for x in stats.values())
    n_csv = sum(x.get("n_valid", 0) for x in stats.values())
    
    print(f" GLOBAL PROGRESS:  {pct:5.1f}%  [{completed_len:,} / {total_len:,} Tiles]")
    print(f" FILES WRITTEN:    {n_csv:,} light curves ({n_src:,} scanned)")
    print("-" * 80)

    # --- Live Shard Status ---
    print(f" {'SHARD':<6} | {'STATUS':<12} | {'CURRENT TILE':<15} | {'PROGRESS':<15} | {'UPDATED'}")
    print("-" * 80)
    
    logs = get_latest_logs(Path("."))
    active_shards = sorted(logs.keys())
    
    # Only show relevant shards (0-3 usually, ignoring cancelled ones if possible)
    # If list is huge, maybe slice it. For now show all found.
    for shard_idx in active_shards:
        if shard_idx > 3 and shard_idx < 90: continue # Skip the dead ones if they exist
        
        info = parse_shard_status(logs[shard_idx])
        
        # Color coding status roughly
        state = "ACTIVE"
        if "Starting" in info['tile']: state = "INIT"
        
        # Clean print
        tile_str = info.get('tile', '-')
        prog_str = info.get('progress', '-')
        
        print(f" {shard_idx:<6} | {state:<12} | {tile_str:<15} | {prog_str:<15} | {info['updated']}")

    print("-" * 80)

    # --- Targets & QC ---
    primvs = get_primvs_coverage(output_dir)
    if primvs["found"]:
        print(f" TARGETS: {primvs['pct']:5.2f}% found in PRIMVS_ID.csv {primvs['note']}")
        
    qc = inspect_sample(output_dir)
    if qc:
        status = "PASS" if qc['valid'] else "FAIL"
        print(f" QC CHECK: {status} on {qc['name']} ({qc['size']} bytes)")

    print("=" * 80)

def watch_progress(output_dir: str, interval: int = 10):
    try:
        while True:
            display_progress(output_dir)
            time.sleep(interval)
    except KeyboardInterrupt:
        print("\nExiting.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--output-dir", "-o", default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--watch", "-w", action="store_true")
    parser.add_argument("--interval", "-i", type=int, default=10)
    args = parser.parse_args()
    
    if args.watch: watch_progress(args.output_dir, args.interval)
    else: display_progress(args.output_dir, clear=False)
