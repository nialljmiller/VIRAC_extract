#!/usr/bin/env python3
"""
VIRAC Extraction Cockpit (Complete Edition)
===========================================
- Real-time Resource Stats (sstat -a)
- Full File Activity Log (Last 5 files)
- Target Coverage (PRIMVS)
- Deep QC (Header + Row Preview)
- Lockfile Detection
- Size Estimation from Ledger Data
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
    """Get Shard ID -> Job Info from squeue (Handles ranges [6-19])."""
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
    """Query sstat with -a to catch batch steps."""
    stats = {}
    job_ids = [info['job_id'] for info in active_shard_map.values() if info['state'] == 'R' and not info.get('is_range')]
    if not job_ids: return stats
    
    job_str = ",".join(job_ids)
    try:
        cmd = ['sstat', '-a', '-j', job_str, '--format=JobID,MaxRSS,AveCPU', '-n', '-P']
        res = subprocess.run(cmd, capture_output=True, text=True)
        
        for line in res.stdout.splitlines():
            parts = line.strip().split('|')
            if len(parts) >= 3:
                full_id = parts[0]
                base_id = full_id.split('.')[0]
                # Prioritize 'batch' step
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

def check_lock_file(output_dir: Path):
    lock = output_dir / "checkpoints/completed_tiles.json.lock"
    if lock.exists():
        age = (datetime.now() - datetime.fromtimestamp(lock.stat().st_mtime)).total_seconds()
        return True, age
    return False, 0

# =============================================================================
# File Size Estimation
# =============================================================================

def sample_file_sizes(directory: Path, sample_size: int = 1000):
    """
    Quickly sample file sizes using find + head (fast, doesn't traverse everything).
    Returns average size in bytes.
    """
    try:
        cmd = f"find {directory} -type f -name '*.csv' | head -{sample_size} | xargs stat -c%s"
        result = subprocess.run(cmd, shell=True, capture_output=True, text=True, timeout=30)
        
        if result.returncode == 0 and result.stdout.strip():
            sizes = [int(s) for s in result.stdout.strip().split('\n') if s]
            if sizes:
                return sum(sizes) / len(sizes)
    except:
        pass
    
    # Fallback: manual sampling if command fails
    try:
        count = 0
        total_size = 0
        for root, dirs, files in os.walk(directory):
            if 'checkpoints' in root or 'logs' in root:
                continue
            for filename in files:
                if filename.endswith('.csv'):
                    try:
                        filepath = os.path.join(root, filename)
                        total_size += os.path.getsize(filepath)
                        count += 1
                        if count >= sample_size:
                            return total_size / count
                    except:
                        continue
            if count >= sample_size:
                break
        if count > 0:
            return total_size / count
    except:
        pass
    
    return 0

# =============================================================================
# Science & QC
# =============================================================================

def get_recent_files(directory: Path, scan_limit: int = 3000):
    """Scan for newest 5 files in hierarchical directory structure."""
    files_found = []
    try:
        count = 0
        # Walk through hierarchical subdirectories
        for root, dirs, files in os.walk(directory):
            # Skip checkpoint and logs directories
            if 'checkpoints' in root or 'logs' in root:
                continue
            for filename in files:
                if filename.endswith('.csv'):
                    filepath = os.path.join(root, filename)
                    stat_info = os.stat(filepath)
                    files_found.append((stat_info.st_mtime, filepath, filename, stat_info.st_size))
                    count += 1
                    if count >= scan_limit:
                        break
            if count >= scan_limit:
                break
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

def get_hierarchical_path(source_id, base_dir):
    """Get hierarchical path for a source ID"""
    source_str = str(source_id)
    subdir1 = source_str[:3]
    subdir2 = source_str[3:6]
    return base_dir / subdir1 / subdir2 / f"{source_id}.csv"

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
            
        hits = sum(1 for sid in check_ids if get_hierarchical_path(sid, output_dir).exists())
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
    
    # 1. Stats & Warnings
    stats = load_json_safe(output_dir / "checkpoints/completed_tiles.json").get("stats", {})
    completed = len(stats)
    total = 22585
    pct = (completed / total * 100)
    files_written = sum(x.get('n_valid', 0) for x in stats.values())
    sources_scanned = sum(x.get('n_sources', 0) for x in stats.values())
    
    print(f" PROGRESS: {pct:5.1f}%  [{completed:,} / {total:,} Tiles]")
    print(f" DATA:     {files_written:,} light curves saved ({sources_scanned:,} scanned)")
    
    # Sample file sizes and estimate total
    avg_size = sample_file_sizes(output_dir, sample_size=1000)
    if avg_size > 0:
        current_size_gb = (files_written * avg_size) / (1024**3)
        projected_files = (files_written / pct * 100) if pct > 0 else 0
        projected_size_tb = (projected_files * avg_size) / (1024**4)
        print(f" SIZE:     Current: {current_size_gb:.2f} GB  |  Avg: {avg_size/1024:.1f} KB/file  |  Projected @ 100%: {projected_size_tb:.2f} TB")
    
    is_locked, lock_age = check_lock_file(output_dir)
    if is_locked:
        print(f" WARNING:  Lockfile active for {int(lock_age)}s")
    print("-" * 95)

    # 2. Live Resource Table
    logs = get_latest_logs(Path("."))
    active_shards = get_active_jobs_info()
    resources = get_slurm_resources(active_shards)

    print(f" {'ID':<3} | {'STATUS':<9} | {'MEM (RSS)':<10} | {'CPU (Avg)':<10} | {'BATCH':<9} | {'TILE ID':<14} | {'PROGRESS':<12} | {'UPDATED'}")
    print("-" * 95)

    all_ids = sorted(list(set(list(logs.keys()) + list(active_shards.keys()))))
    total_rss = 0.0

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

        usage = resources.get(q_info.get('job_id'), {'rss': '-', 'cpu': '-'})
        
        # Calculate Total RAM
        try:
            val = float(re.sub(r'[a-zA-Z]', '', usage['rss']))
            if 'K' in usage['rss']: total_rss += val/1024/1024
            elif 'M' in usage['rss']: total_rss += val/1024
            elif 'G' in usage['rss']: total_rss += val
        except: pass

        # Hang Warning
        if display_state == "ACTIVE" and "Starting" in log_info.get('tile', ''):
             if log_info.get('age_seconds', 0) > 300: display_state = "HUNG?"

        print(f" {shard_idx:<3} | {display_state:<9} | {usage['rss']:<10} | {usage['cpu']:<10} | {log_info.get('batch','-'):<9} | {log_info.get('tile','-'):<14} | {log_info.get('progress','-'):<12} | {log_info.get('updated','-')}")

    print("-" * 95)
    print(f" CLUSTER LOAD: {len(resources)} active jobs | Total RAM: {total_rss:.2f} GB")
    print("=" * 95)

    # 3. File Activity
    recents = get_recent_files(output_dir)
    print(f" RECENT ACTIVITY (Last {len(recents)} files):")
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
        print("   No files found.")

    print("=" * 95)
    
    # 4. Targets
    primvs = get_primvs_coverage(output_dir)
    if primvs["found"]:
         print(f" TARGETS:  {primvs['pct']:5.2f}% coverage in output {primvs['note']}")

def watch_progress(output_dir: str, interval: int = 10):
    try:
        while True: display_progress(output_dir); time.sleep(interval)
    except: pass

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--output-dir", "-o", default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--watch", "-w", action="store_true")
    parser.add_argument("--interval", "-i", type=int, default=10)
    args = parser.parse_args()
    if args.watch: watch_progress(args.output_dir, args.interval)
    else: display_progress(args.output_dir, clear=False)
