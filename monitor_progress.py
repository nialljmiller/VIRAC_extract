#!/usr/bin/env python3
"""
VIRAC Extraction Monitor (Ultimate Edition)
===========================================
Combines real-time log parsing (Dashboard) with deep data integrity checks (QC).
Features:
- Global Progress Stats
- Live per-shard status table (Tile ID, Progress, Speed)
- PRIMVS Target List Coverage
- Deep CSV Integrity Inspection (Header & Row checks)
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
    """Finds the most recent .err log file for each shard index."""
    shard_logs = {}
    pattern = re.compile(r"virac_shard_(\d+)_(\d+)\.err")
    
    # Look in current directory for log files
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
                if job_id > shard_logs[shard_idx][0]:
                    shard_logs[shard_idx] = (job_id, log_file)
    
    return {k: v[1] for k, v in shard_logs.items()}

def parse_shard_status(log_file: str) -> dict:
    """Reads the tail of a log file to extract current status."""
    status = {"tile": "Starting...", "progress": "-", "batch": "-", "updated": "N/A"}
    
    try:
        fpath = Path(log_file)
        if not fpath.exists(): return status
            
        status["updated"] = datetime.fromtimestamp(fpath.stat().st_mtime).strftime('%H:%M:%S')
        
        # Read last 4KB to catch recent updates
        file_size = fpath.stat().st_size
        read_size = min(4096, file_size)
        
        with open(fpath, 'rb') as f:
            if file_size > read_size:
                f.seek(-read_size, 2)
            lines = f.read().decode('utf-8', errors='ignore').splitlines()
            
        # Parse: "INFO - [55/5530] n1024_8954892: 9856/13849"
        prog_pattern = re.compile(r"\[(\d+/\d+)\]\s+(n\d+_\d+):\s+(\d+/\d+)")
        
        for line in reversed(lines):
            match = prog_pattern.search(line)
            if match:
                status["batch"] = match.group(1)
                status["tile"] = match.group(2)
                status["progress"] = match.group(3)
                break
    except: pass
    return status

# =============================================================================
# Coverage & QC Checks
# =============================================================================

def get_primvs_coverage(output_dir: Path) -> dict:
    script_dir = Path(__file__).parent.resolve()
    primvs_path = script_dir / PRIMVS_FILENAME
    
    result = {"found": False, "hits": 0, "checked": 0, "pct": 0.0, "note": ""}
    if not primvs_path.exists(): return result
    result["found"] = True

    try:
        with open(primvs_path, 'r') as f:
            # Check for header
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
            
    except: pass
    return result

def get_sample_file(directory: Path) -> Path:
    """Fast scandir to find one random CSV file."""
    try:
        with os.scandir(directory) as entries:
            for entry in entries:
                if entry.name.endswith('.csv'):
                    return Path(entry.path)
    except: pass
    return None

def inspect_file_health(filepath: Path) -> dict:
    """Deep inspection of CSV integrity."""
    result = {"valid": False, "head": [], "error": None, "size": 0, "name": filepath.name}
    try:
        result["size"] = filepath.stat().st_size
        if result["size"] == 0:
            result["error"] = "Empty file (0B)"
            return result
        
        with open(filepath, 'r') as f:
            lines = [f.readline() for _ in range(3)]
            result["head"] = [L.strip() for L in lines if L]
            
        if not result["head"] or "mjd" not in result["head"][0].lower():
            result["error"] = "Invalid Header"
        elif len(result["head"]) < 2:
            result["error"] = "No Data Rows"
        else:
            result["valid"] = True
    except Exception as e:
        result["error"] = str(e)
    return result

# =============================================================================
# Main Dashboard
# =============================================================================

def display_progress(output_dir: str, clear: bool = True):
    output_dir = Path(output_dir)
    checkpoint_dir = output_dir / "checkpoints"
    
    if clear: os.system('clear' if os.name == 'posix' else 'cls')
    
    print("=" * 80)
    print(f"VIRAC EXTRACTOR DASHBOARD  |  {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 80)
    
    # --- 1. Global Stats ---
    stats = load_json_safe(checkpoint_dir / "completed_tiles.json").get("stats", {})
    failed = load_json_safe(checkpoint_dir / "failed_tiles.json").get("failed", {})
    
    completed_len = len(stats)
    total_len = 22585
    pct = (completed_len / total_len * 100)
    
    n_src = sum(x.get("n_sources", 0) for x in stats.values())
    n_csv = sum(x.get("n_valid", 0) for x in stats.values())
    
    print(f" GLOBAL PROGRESS:  {pct:5.1f}%  [{completed_len:,} / {total_len:,} Tiles]")
    print(f" FILES WRITTEN:    {n_csv:,} light curves ({n_src:,} scanned)")
    if len(failed) > 0:
        print(f" FAILURES:         {len(failed)} tiles (Check logs!)")
    print("-" * 80)

    # --- 2. Live Shard Table ---
    print(f" {'ID':<4} | {'STATUS':<10} | {'BATCH':<10} | {'TILE ID':<15} | {'PROGRESS':<14} | {'UPDATED'}")
    print("-" * 80)
    
    logs = get_latest_logs(Path("."))
    active_shards = sorted(logs.keys())
    
    for shard_idx in active_shards:
        if shard_idx > 3: continue # Hide cancelled shards
        
        info = parse_shard_status(logs[shard_idx])
        
        state = "ACTIVE"
        if "Starting" in info['tile']: state = "INIT"
        
        print(f" {shard_idx:<4} | {state:<10} | {info['batch']:<10} | {info['tile']:<15} | {info['progress']:<14} | {info['updated']}")

    print("-" * 80)

    # --- 3. Coverage ---
    primvs = get_primvs_coverage(output_dir)
    if primvs["found"]:
        print(f" TARGETS: {primvs['pct']:5.2f}% found in PRIMVS_ID.csv {primvs['note']}")
    else:
        print(" TARGETS: PRIMVS_ID.csv not found.")

    # --- 4. Deep QC ---
    sample_file = get_sample_file(output_dir)
    if sample_file:
        qc = inspect_file_health(sample_file)
        status = "PASS" if qc['valid'] else f"FAIL [{qc['error']}]"
        
        print("-" * 80)
        print(f" QC SAMPLE: {qc['name']} | Size: {qc['size']} bytes | Integrity: {status}")
        if qc['head']:
            print(f"   Header: {qc['head'][0][:70]}...")
            if len(qc['head']) > 1:
                print(f"   Row 1:  {qc['head'][1][:70]}...")
    else:
        print("-" * 80)
        print(" QC SAMPLE: Waiting for files...")

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
