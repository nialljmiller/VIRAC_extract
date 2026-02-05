#!/usr/bin/env python3
"""
VIRAC Extraction Cockpit (Bulletproof Edition)
==============================================
- Guaranteed Resource Stats (sstat -a)
- Startup Hang Detection
- Lockfile & Stale Data Warnings
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

def parse_slurm_range(range_str: str) -> list:
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
    """Get Shard ID -> Job Info from squeue."""
    mapping = {}
    try:
        cmd = ['squeue', '-u', USER, '-h', '-o', '%i %t %M %r']
        res = subprocess.run(cmd, capture_output=True, text=True)
        id_pattern = re.compile(r"(\d+)_(\[.*?\]|\d+)")

        for line in res.stdout.splitlines():
            parts = line.split()
            if len(parts) < 3: continue
            
            job_str, state, time_str = parts[0], parts[1], parts[2]
            match = id_pattern.search(job_str)
            
            if match:
                base_id, suffix = match.group(1), match.group(2)
                if '[' in suffix:
                    inner = suffix.replace('[', '').replace(']', '')
                    for idx in parse_slurm_range(inner):
                        mapping[idx] = {'job_id': f"{base_id}_{idx}", 'state': state, 'time': time_str, 'is_range': True}
                else:
                    idx = int(suffix)
                    mapping[idx] = {'job_id': job_str, 'state': state, 'time': time_str, 'is_range': False}
    except: pass
    return mapping

def get_slurm_resources(active_shard_map):
    """Query sstat with --allsteps to ensure we catch the batch process."""
    stats = {}
    job_ids = [info['job_id'] for info in active_shard_map.values() if info['state'] == 'R' and not info.get('is_range')]
    if not job_ids: return stats
    
    job_str = ",".join(job_ids)
    try:
        # Added -a (allsteps) to catch the batch step
        cmd = ['sstat', '-a', '-j', job_str, '--format=JobID,MaxRSS,AveCPU', '-n', '-P']
        res = subprocess.run(cmd, capture_output=True, text=True)
        
        for line in res.stdout.splitlines():
            # 233251_0.batch|429496K|95.2%
            parts = line.strip().split('|')
            if len(parts) >= 3:
                full_id = parts[0]
                base_id = full_id.split('.')[0]
                # Prioritize the 'batch' step which has the real stats
                if 'batch' in full_id:
                    stats[base_id] = {'rss': parts[1], 'cpu': parts[2]}
                elif base_id not in stats:
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
        with open(filepath, 'r') as f:
            lines = [f.readline() for _ in range(3)]
            result["head"] = [L.strip() for L in lines if L]
        if not result["head"] or "mjd" not in result["head"][0].lower(): result["error"] = "Invalid Header"
        elif len(result["head"]) < 2: result["error"] = "No Data Rows"
        else: result["valid"] = True
    except Exception as e: result["error"] = str(e)
    return result

def check_lock_file(output_dir: Path):
    """Check if the JSON checkpoint is locked."""
    lock = output_dir / "checkpoints/completed_tiles.json.lock"
    if lock.exists():
        age = (datetime.now() - datetime.fromtimestamp(lock.stat().st_mtime)).total_seconds()
        return True, age
    return False, 0

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
    
    print(f" PROGRESS: {pct:5.1f}%  [{completed:,} / {total:,} Tiles]")
    print(f" DATA:     {files_written:,} light curves saved")
    
    # Check Lock
    is_locked, lock_age = check_lock_file(output_dir)
    if is_locked:
        print(f" WARNING:  Lockfile found! (Age: {int(lock_age)}s). If >300s, jobs may be hung.")
    print("-" * 95)

    # 2. Live Resource Table
    logs = get_latest_logs(Path("."))
    active_shards = get_active_jobs_info()
    resources = get_slurm_resources(active_shards)

    print(f" {'ID':<3} | {'STATUS':<9} | {'MEM (RSS)':<10} | {'CPU (Avg)':<10} | {'BATCH':<9} | {'TILE ID':<14} | {'PROGRESS':<12} | {'UPDATED'}")
    print("-" * 95)

    all_ids = sorted(list(set(list(logs.keys()) + list(active_shards.keys()))))
    
    for shard_idx in all_ids:
        log_info = parse_shard_status(logs[shard_idx][1]) if shard_idx in logs else {}
        q_info = active_shards.get(shard_idx, {})
        
        display_state = q_info.get('state', 'MISSING')
        if display_state == 'R': display_state = "ACTIVE"
        elif display_state == 'PD': display_state = "PENDING"
        elif display_state == 'MISSING': 
             if log_info.get('age_seconds', 9999) < 600: display_state = "FINISHING"
             else: display_state = "DEAD"

        if display_state == "PENDING":
            log_info = {"batch": "-", "tile": "(Queued)", "progress": "-", "updated": "-"}
            
        # Resources
        usage = resources.get(q_info.get('job_id'), {'rss': '-', 'cpu': '-'})

        # Startup Hang Detection
        if display_state == "ACTIVE" and "Starting" in log_info.get('tile', ''):
             if log_info.get('age_seconds', 0) > 300:
                 display_state = "HUNG?"

        print(f" {shard_idx:<3} | {display_state:<9} | {usage['rss']:<10} | {usage['cpu']:<10} | {log_info.get('batch','-'):<9} | {log_info.get('tile','-'):<14} | {log_info.get('progress','-'):<12} | {log_info.get('updated','-')}")

    print("-" * 95)

    # 3. File Activity
    recents = get_recent_files(output_dir)
    if recents:
        last_file_age = (datetime.now().timestamp() - recents[0][0])
        age_str = str(timedelta(seconds=int(last_file_age)))
        print(f" RECENT ACTIVITY: Last file written {age_str} ago.")
        
        if last_file_age > 3600:
            print(f" !!! WARNING !!! No new data produced in over 1 hour.")

        latest_file = Path(recents[0][1])
        qc = inspect_file_health(latest_file)
        print("-" * 95)
        print(f" LATEST FILE: {qc['name']}")
        print(f"   Status: {('PASS' if qc['valid'] else 'FAIL')} [{qc.get('error','')}]")
        if qc['head']:
            print(f"   Row 1:  {qc['head'][1][:80]}...")
    else:
        print(" RECENT ACTIVITY: No files found yet.")

    print("=" * 95)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--output-dir", "-o", default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--watch", "-w", action="store_true")
    parser.add_argument("--interval", "-i", type=int, default=10)
    args = parser.parse_args()
    if args.watch: 
        try:
             while True: display_progress(args.output_dir); time.sleep(args.interval)
        except: pass
    else: display_progress(args.output_dir, clear=False)
