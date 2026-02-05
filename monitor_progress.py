#!/usr/bin/env python3
"""
VIRAC Extraction Cockpit (System Monitor Edition)
=================================================
- Live Job Forensics (Memory & CPU usage via sstat)
- Cluster Health Summary
- Global Progress & Speed
- Deep Data Integrity & Science Checks
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
from datetime import datetime, timedelta
import time

DEFAULT_OUTPUT_DIR = "/beegfs/car/njm/virac_lightcurves/"
PRIMVS_FILENAME = "PRIMVS_ID.csv"
USER = os.environ.get('USER', 'njm')

# =============================================================================
# Core Utilities
# =============================================================================

def load_json_safe(filepath: Path) -> dict:
    if not filepath.exists(): return {}
    try:
        with open(filepath, 'r') as f: return json.load(f)
    except: return {}

def get_latest_logs(directory: Path) -> dict:
    """Map Shard ID -> (Job ID, Log Path)"""
    shard_logs = {}
    pattern = re.compile(r"virac_shard_(\d+)_(\d+)\.err")
    for log_file in glob.glob("virac_shard_*_*.err"):
        match = pattern.search(log_file)
        if match:
            job_id, shard_idx = int(match.group(1)), int(match.group(2))
            if shard_idx not in shard_logs or job_id > shard_logs[shard_idx][0]:
                shard_logs[shard_idx] = (job_id, log_file)
    return shard_logs

def get_slurm_stats(active_job_ids: list) -> dict:
    """
    Query 'sstat' for all active jobs to get Memory/CPU usage.
    Returns: { '233105_0': {'MaxRSS': '4.2G', 'AveCPU': '98.2%'}, ... }
    """
    stats = {}
    if not active_job_ids: return stats
    
    # Clean job IDs for sstat (e.g. 233105_0 -> 233105_0.batch usually, but sstat accepts 233105_0)
    job_str = ",".join(str(jid) for jid in active_job_ids)
    
    try:
        # Request specific fields. Note: We use -a to see steps if needed
        cmd = ['sstat', '-j', job_str, '--format=JobID,MaxRSS,AveCPU', '-n', '-P']
        res = subprocess.run(cmd, capture_output=True, text=True)
        
        for line in res.stdout.splitlines():
            # format: 233105_0.batch|429496K|95.2%
            parts = line.strip().split('|')
            if len(parts) >= 3:
                full_id = parts[0]
                # Map back to simple job ID (233105_0.batch -> 233105_0)
                simple_id = full_id.split('.')[0]
                
                # We typically want the 'batch' step or the main step
                if 'batch' in full_id or full_id == simple_id:
                    stats[simple_id] = {
                        'rss': parts[1],
                        'cpu': parts[2]
                    }
    except: pass
    return stats

def parse_shard_status(log_file: str) -> dict:
    status = {"tile": "Starting...", "progress": "-", "batch": "-", "updated": "N/A"}
    try:
        fpath = Path(log_file)
        if not fpath.exists(): return status
        status["updated"] = datetime.fromtimestamp(fpath.stat().st_mtime).strftime('%H:%M:%S')
        
        # Check staleness
        mtime = datetime.fromtimestamp(fpath.stat().st_mtime)
        if datetime.now() - mtime > timedelta(minutes=30):
            status["stale"] = True
        else:
            status["stale"] = False

        file_size = fpath.stat().st_size
        read_size = min(65536, file_size)
        with open(fpath, 'rb') as f:
            if file_size > read_size: f.seek(-read_size, 2)
            lines = f.read().decode('utf-8', errors='ignore').splitlines()
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

def get_queue_status():
    pending = []
    try:
        res = subprocess.run(['squeue', '-u', USER, '-h', '-o', '%i %n %t %r'], 
                             capture_output=True, text=True)
        for line in res.stdout.splitlines():
            if 'PD' in line:
                parts = line.split()
                pending.append(f"Shard {parts[0].split('_')[-1]} ({parts[3]})")
    except: pass
    return pending

# =============================================================================
# Science & QC
# =============================================================================

def get_primvs_coverage(output_dir: Path) -> dict:
    script_dir = Path(__file__).parent.resolve()
    primvs_path = script_dir / PRIMVS_FILENAME
    result = {"found": False, "pct": 0.0, "note": ""}
    if not primvs_path.exists(): return result
    result["found"] = True

    try:
        with open(primvs_path, 'r') as f:
            header_line = f.readline().strip().lower()
            f.seek(0)
            if "sourceid" in header_line:
                reader = csv.DictReader(f)
                id_col = next((h for h in reader.fieldnames if "sourceid" in h.lower()), None)
                if id_col: ids = [row[id_col].strip() for row in reader if row[id_col]]
                else: 
                    f.seek(0); next(f)
                    ids = [line.split(',')[0].strip() for line in f if line.strip()]
            else:
                ids = [line.split(',')[0].strip() for line in f if line.strip()]
        
        if len(ids) > 5000:
            check_ids = random.sample(ids, 2000)
            result["note"] = "(Sampled 2k)"
        else:
            check_ids = ids
            result["note"] = "(Exact)"
            
        hits = sum(1 for sid in check_ids if (output_dir / f"{sid}.csv").exists())
        if len(check_ids) > 0: result["pct"] = (hits / len(check_ids)) * 100
    except Exception as e: result["note"] = f"(Error: {str(e)})"
    return result

def get_newest_sample(directory: Path, scan_limit: int = 2000) -> Path:
    newest_file, newest_time, count = None, 0, 0
    try:
        with os.scandir(directory) as entries:
            for entry in entries:
                if entry.name.endswith('.csv'):
                    mtime = entry.stat().st_mtime
                    if mtime > newest_time:
                        newest_time = mtime
                        newest_file = Path(entry.path)
                    count += 1
                    if count >= scan_limit: break
    except: pass
    return newest_file

def inspect_file_health(filepath: Path) -> dict:
    result = {"valid": False, "head": [], "error": None, "size": 0, "name": filepath.name}
    try:
        result["size"] = filepath.stat().st_size
        result["mtime"] = datetime.fromtimestamp(filepath.stat().st_mtime).strftime('%H:%M:%S')
        if result["size"] == 0:
            result["error"] = "Empty file (0B)"
            return result
        with open(filepath, 'r') as f:
            lines = [f.readline() for _ in range(3)]
            result["head"] = [L.strip() for L in lines if L]
        if not result["head"] or "mjd" not in result["head"][0].lower(): result["error"] = "Invalid Header"
        elif len(result["head"]) < 2: result["error"] = "No Data Rows"
        else: result["valid"] = True
    except Exception as e: result["error"] = str(e)
    return result

# =============================================================================
# Main Display
# =============================================================================

def display_progress(output_dir: str, clear: bool = True):
    output_dir = Path(output_dir)
    if clear: os.system('clear' if os.name == 'posix' else 'cls')
    
    print("=" * 95)
    print(f"VIRAC EXTRACTOR COCKPIT  |  {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 95)
    
    # --- 1. Global Stats ---
    stats = load_json_safe(output_dir / "checkpoints/completed_tiles.json").get("stats", {})
    completed = len(stats)
    total = 22585
    pct = (completed / total * 100)
    files_written = sum(x.get('n_valid', 0) for x in stats.values())
    sources_scanned = sum(x.get('n_sources', 0) for x in stats.values())
    
    print(f" PROGRESS: {pct:5.1f}%  [{completed:,} / {total:,} Tiles]")
    print(f" DATA:     {files_written:,} light curves saved ({sources_scanned:,} scanned)")
    print("-" * 95)

    # --- 2. Live Resource Usage ---
    logs = get_latest_logs(Path("."))
    
    # Construct real Job IDs (e.g. 233105_0)
    job_map = {} # Shard -> JobID string
    active_job_ids = []
    
    for shard_idx, (jid_num, _) in logs.items():
        full_id = f"{jid_num}_{shard_idx}"
        job_map[shard_idx] = full_id
        active_job_ids.append(full_id)
        
    slurm_stats = get_slurm_stats(active_job_ids)

    # Header
    print(f" {'ID':<3} | {'STATUS':<9} | {'MEM (RSS)':<10} | {'CPU (Avg)':<10} | {'BATCH':<9} | {'TILE ID':<14} | {'PROGRESS':<12} | {'UPDATED'}")
    print("-" * 95)

    total_rss = 0.0 # in GB
    
    for shard_idx in sorted(logs.keys()):
        info = parse_shard_status(logs[shard_idx][1])
        job_id_str = job_map[shard_idx]
        
        # Get usage
        usage = slurm_stats.get(job_id_str, {'rss': '-', 'cpu': '-'})
        rss_str = usage['rss']
        cpu_str = usage['cpu']
        
        # Calculate Health
        state = "ACTIVE"
        if "Starting" in info['tile']: state = "INIT"
        if info.get("stale"): state = "STALLED"
        
        # Format RSS for summary (convert K/M to G)
        try:
            val = float(re.sub(r'[a-zA-Z]', '', rss_str))
            if 'K' in rss_str: total_rss += val / 1024 / 1024
            elif 'M' in rss_str: total_rss += val / 1024
            elif 'G' in rss_str: total_rss += val
        except: pass

        print(f" {shard_idx:<3} | {state:<9} | {rss_str:<10} | {cpu_str:<10} | {info['batch']:<9} | {info['tile']:<14} | {info['progress']:<12} | {info['updated']}")

    # --- 3. Queue & System Health ---
    print("-" * 95)
    pending = get_queue_status()
    
    health_msg = f"CLUSTER LOAD: {len(active_job_ids)} active nodes | ~{total_rss:.1f} GB RAM Total Usage"
    print(f" {health_msg}")
    
    if pending:
        print(f" QUEUE:        {len(pending)} shards pending (Reason: {pending[0].split('(')[-1] if pending else '?'})")
    
    # --- 4. Science Checks ---
    print("=" * 95)
    primvs = get_primvs_coverage(output_dir)
    print(f" TARGETS:  {primvs['pct']:5.2f}% coverage in output ({primvs['note']})")
    
    sample_file = get_newest_sample(output_dir, scan_limit=1000)
    if sample_file:
        qc = inspect_file_health(sample_file)
        status = "PASS" if qc['valid'] else f"FAIL [{qc['error']}]"
        print(f" LATEST:   {qc['name']} ({qc['size']} bytes) -> Integrity: {status}")
    else:
        print(" LATEST:   Waiting for files...")

    print("=" * 95)

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
