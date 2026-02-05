#!/usr/bin/env python3
"""
VIRAC Extraction Cockpit (Robust Squeue Edition)
================================================
- Robust squeue parsing (Handling [0-9] ranges)
- Accurate Resource Stats (Memory/CPU)
- Real-time File Activity
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
# Get user safely, default to njm if not found
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

def parse_slurm_range(range_str: str) -> list:
    """Expands Slurm ranges like '6-19' or '1,3,5-7' into a list of integers."""
    ids = []
    try:
        parts = range_str.split(',')
        for p in parts:
            if '-' in p:
                start, end = map(int, p.split('-'))
                ids.extend(range(start, end + 1))
            else:
                ids.append(int(p))
    except: pass
    return ids

def get_active_jobs_info():
    """
    Get mapping of Shard ID -> Slurm Job Info by parsing Job ID strings.
    Handles '233251_0' (Active) and '233251_[6-19]' (Pending).
    """
    mapping = {}
    try:
        # Get JobID, State, Time, Reason
        cmd = ['squeue', '-u', USER, '-h', '-o', '%i %t %M %r']
        res = subprocess.run(cmd, capture_output=True, text=True)
        
        # Regex to parse '12345_1' or '12345_[1-5]'
        # Capture groups: 1=BaseID, 2=IndexOrRange
        id_pattern = re.compile(r"(\d+)_(\[.*?\]|\d+)")

        for line in res.stdout.splitlines():
            parts = line.split()
            if len(parts) < 3: continue
            
            job_str = parts[0]
            state = parts[1]
            time_str = parts[2]
            
            match = id_pattern.search(job_str)
            if match:
                base_id = match.group(1)
                suffix = match.group(2)
                
                # Check for brackets [6-19]
                if '[' in suffix:
                    # Remove brackets and parse range
                    inner = suffix.replace('[', '').replace(']', '')
                    indices = parse_slurm_range(inner)
                    for idx in indices:
                        mapping[idx] = {
                            'job_id': f"{base_id}_{idx}", # Synthesize full ID
                            'state': state,
                            'time': time_str,
                            'is_range': True
                        }
                else:
                    # Single ID
                    idx = int(suffix)
                    mapping[idx] = {
                        'job_id': job_str,
                        'state': state,
                        'time': time_str,
                        'is_range': False
                    }
    except Exception as e: 
        # Fail silently but could print e for debug
        pass
    return mapping

def get_slurm_resources(active_shard_map):
    """
    Query 'sstat' for running jobs to get usage.
    """
    stats = {}
    # Only query running jobs (R) that are single IDs (not ranges)
    job_ids = [info['job_id'] for info in active_shard_map.values() 
               if info['state'] == 'R' and not info.get('is_range')]
    
    if not job_ids: return stats
    
    # Batch query sstat
    job_str = ",".join(job_ids)
    try:
        cmd = ['sstat', '-j', job_str, '--format=JobID,MaxRSS,AveCPU', '-n', '-P']
        res = subprocess.run(cmd, capture_output=True, text=True)
        
        for line in res.stdout.splitlines():
            # Output: 233251_0.batch|429496K|95.2%
            parts = line.strip().split('|')
            if len(parts) >= 3:
                full_id = parts[0]
                base_id = full_id.split('.')[0] # 233251_0
                
                # We want the main batch step stats
                if 'batch' in full_id or full_id == base_id:
                    stats[base_id] = {'rss': parts[1], 'cpu': parts[2]}
    except: pass
    return stats

def parse_shard_status(log_file: str) -> dict:
    status = {"tile": "Starting...", "progress": "-", "batch": "-", "updated": "N/A"}
    try:
        fpath = Path(log_file)
        if not fpath.exists(): return status
        
        mtime = datetime.fromtimestamp(fpath.stat().st_mtime)
        status["updated"] = mtime.strftime('%H:%M:%S')
        status["age_seconds"] = (datetime.now() - mtime).total_seconds()

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

# =============================================================================
# Science & QC
# =============================================================================

def get_recent_files(directory: Path, scan_limit: int = 3000):
    files_found = []
    try:
        count = 0
        with os.scandir(directory) as entries:
            for entry in entries:
                if entry.name.endswith('.csv'):
                    files_found.append((entry.stat().st_mtime, entry.path, entry.name, entry.stat().st_size))
                    count += 1
                    if count >= scan_limit: break
    except: pass
    files_found.sort(key=lambda x: x[0], reverse=True)
    return files_found[:5]

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

# =============================================================================
# Main Display
# =============================================================================

def display_progress(output_dir: str, clear: bool = True):
    output_dir = Path(output_dir)
    if clear: os.system('clear' if os.name == 'posix' else 'cls')
    
    print("=" * 95)
    print(f"VIRAC EXTRACTOR COCKPIT  |  {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 95)
    
    # 1. Stats
    stats = load_json_safe(output_dir / "checkpoints/completed_tiles.json").get("stats", {})
    completed = len(stats)
    total = 22585
    pct = (completed / total * 100)
    files_written = sum(x.get('n_valid', 0) for x in stats.values())
    sources_scanned = sum(x.get('n_sources', 0) for x in stats.values())
    
    print(f" PROGRESS: {pct:5.1f}%  [{completed:,} / {total:,} Tiles]")
    print(f" DATA:     {files_written:,} light curves saved ({sources_scanned:,} scanned)")
    print("-" * 95)

    # 2. Live Resource Table
    logs = get_latest_logs(Path("."))
    active_shards = get_active_jobs_info() # Squeue data (Truth)
    resources = get_slurm_resources(active_shards) # Sstat data

    print(f" {'ID':<3} | {'STATUS':<9} | {'MEM (RSS)':<10} | {'CPU (Avg)':<10} | {'BATCH':<9} | {'TILE ID':<14} | {'PROGRESS':<12} | {'UPDATED'}")
    print("-" * 95)

    total_rss = 0.0
    
    # Merge log IDs and Queue IDs to show everything
    all_ids = sorted(list(set(list(logs.keys()) + list(active_shards.keys()))))
    
    for shard_idx in all_ids:
        # Get Log Info
        log_info = parse_shard_status(logs[shard_idx][1]) if shard_idx in logs else {}
        
        # Get Queue Info (The Truth)
        q_info = active_shards.get(shard_idx, {})
        job_id = q_info.get('job_id')
        slurm_state = q_info.get('state', 'MISSING')
        
        # Determine Display State
        display_state = slurm_state
        if slurm_state == 'R': display_state = "ACTIVE"
        elif slurm_state == 'PD': display_state = "PENDING"
        elif slurm_state == 'MISSING': 
             if log_info.get('age_seconds', 9999) < 600: display_state = "FINISHING"
             else: display_state = "DEAD"

        # If Pending, log info is invalid
        if display_state == "PENDING":
            log_info = {"batch": "-", "tile": "(Queued)", "progress": "-", "updated": "-"}
            
        # Get Resources
        usage = {'rss': '-', 'cpu': '-'}
        if job_id in resources:
            usage = resources[job_id]

        # Sum Memory
        try:
            val = float(re.sub(r'[a-zA-Z]', '', usage['rss']))
            if 'K' in usage['rss']: total_rss += val/1024/1024
            elif 'M' in usage['rss']: total_rss += val/1024
            elif 'G' in usage['rss']: total_rss += val
        except: pass

        print(f" {shard_idx:<3} | {display_state:<9} | {usage['rss']:<10} | {usage['cpu']:<10} | {log_info.get('batch','-'):<9} | {log_info.get('tile','-'):<14} | {log_info.get('progress','-'):<12} | {log_info.get('updated','-')}")

    print("-" * 95)
    print(f" CLUSTER LOAD: {len(resources)} active jobs reporting stats | Total RAM: {total_rss:.2f} GB")
    print("=" * 95)

    # 3. File Activity & QC
    recents = get_recent_files(output_dir)
    print(f" RECENT ACTIVITY (Last {len(recents)} files found in scan):")
    if recents:
        for t, path, name, size in recents:
            ts = datetime.fromtimestamp(t).strftime('%H:%M:%S')
            print(f"   [{ts}] {name} ({size} bytes)")
        
        # QC newest
        latest_file = Path(recents[0][1])
        qc = inspect_file_health(latest_file)
        print("-" * 95)
        print(f" INTEGRITY CHECK: {qc['name']}")
        print(f"   Status: {('PASS' if qc['valid'] else 'FAIL')} [{qc.get('error','')}]")
        if qc['head']:
            print(f"   Header: {qc['head'][0][:80]}...")
            if len(qc['head']) > 1:
                print(f"   Row 1:  {qc['head'][1][:80]}...")
    else:
        print("   No files found yet.")

    print("=" * 95)
    
    # Coverage
    primvs = get_primvs_coverage(output_dir)
    if primvs["found"]:
         print(f" TARGETS:  {primvs['pct']:5.2f}% coverage in output {primvs['note']}")

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
