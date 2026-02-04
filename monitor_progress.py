#!/usr/bin/env python3
"""
VIRAC Extraction Progress Monitor (With Target List Support)
============================================================
Includes specific coverage check for PRIMVS_ID.csv targets.
Auto-switches to statistical sampling if target list is huge.
"""

import os
import sys
import json
import argparse
import subprocess
import random
import csv
from pathlib import Path
from datetime import datetime
import time

DEFAULT_OUTPUT_DIR = "/beegfs/car/njm/virac_lightcurves/"
PRIMVS_FILENAME = "PRIMVS_ID.csv"

def load_json_safe(filepath: Path) -> dict:
    if not filepath.exists():
        return {}
    try:
        with open(filepath, 'r') as f:
            return json.load(f)
    except:
        return {}

def get_primvs_coverage(output_dir: Path) -> dict:
    """
    Calculate coverage of IDs in PRIMVS_ID.csv.
    Uses sampling if list is large to preserve performance.
    """
    # Look for CSV in the same directory as this script
    script_dir = Path(__file__).parent.resolve()
    primvs_path = script_dir / PRIMVS_FILENAME
    
    result = {
        "found": False,
        "total_targets": 0,
        "checked_count": 0,
        "hits": 0,
        "percentage": 0.0,
        "method": "exact"
    }

    if not primvs_path.exists():
        return result

    result["found"] = True
    ids = []

    try:
        with open(primvs_path, 'r') as f:
            reader = csv.DictReader(f)
            # Handle potential whitespace in headers
            headers = [h.strip() for h in reader.fieldnames]
            
            # Identify the ID column (case insensitive)
            id_col = next((h for h in headers if "sourceid" in h.lower()), None)
            
            if not id_col:
                # Fallback: try first column if no header match
                f.seek(0)
                next(f) # Skip header
                ids = [line.split(',')[0].strip() for line in f if line.strip()]
            else:
                # Re-read with correct column
                f.seek(0)
                reader = csv.DictReader(f)
                ids = [row[id_col].strip() for row in reader if row[id_col]]
                
        result["total_targets"] = len(ids)
        
        # SMART SAMPLING:
        # If list is huge, check a random sample to save filesystem I/O
        check_ids = ids

        if len(ids) > 5000:
            check_ids = random.sample(ids, 5000)
            result["method"] = "sampled (2k)"
        else:
            check_ids = ids
            result["method"] = "exact"
       
        # Check existence
        hits = 0
        for source_id in check_ids:
            if (output_dir / f"{source_id}.csv").exists():
                hits += 1
        
        result["hits"] = hits
        if result["checked_count"] > 0:
            result["percentage"] = (hits / result["checked_count"]) * 100
            
    except Exception as e:
        print(f"Error reading {PRIMVS_FILENAME}: {e}")
        
    return result

def get_directory_size_fast(path: Path, timeout: int = 2) -> str:
    try:
        result = subprocess.run(
            ['du', '-sh', str(path)], 
            capture_output=True, 
            text=True, 
            timeout=timeout
        )
        if result.returncode == 0:
            return result.stdout.split()[0]
    except subprocess.TimeoutExpired:
        return "Calculating..."
    except Exception:
        pass
    return "N/A"

def get_sample_files(directory: Path, n: int = 5) -> list:
    samples = []
    try:
        with os.scandir(directory) as entries:
            for entry in entries:
                if entry.is_file() and entry.name.endswith('.csv'):
                    samples.append(Path(entry.path))
                    if len(samples) >= n:
                        break
    except Exception:
        return []
    return samples

def inspect_file_health(filepath: Path) -> dict:
    result = {"valid": False, "head": [], "error": None, "size": 0}
    try:
        result["size"] = filepath.stat().st_size
        if result["size"] == 0:
            result["error"] = "Empty file (0B)"
            return result
        
        with open(filepath, 'r') as f:
            lines = [f.readline() for _ in range(4)]
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

def display_progress(output_dir: str, clear: bool = True):
    output_dir = Path(output_dir)
    checkpoint_dir = output_dir / "checkpoints"
    
    if clear:
        os.system('clear' if os.name == 'posix' else 'cls')
    
    print("=" * 70)
    print("VIRAC Light Curve Extraction Progress")
    print("=" * 70)
    print(f"Output: {output_dir}")
    print(f"Time:   {datetime.now().strftime('%H:%M:%S')}")
    print("-" * 70)
    
    # Stats
    completed_data = load_json_safe(checkpoint_dir / "completed_tiles.json")
    failed_data = load_json_safe(checkpoint_dir / "failed_tiles.json")
    progress_data = load_json_safe(checkpoint_dir / "progress.json")
    
    stats = completed_data.get("stats", {})
    n_completed = len(completed_data.get("completed", []))
    n_failed = len(failed_data.get("failed", {}))
    
    real_csv_count = sum(item.get("n_valid", 0) for item in stats.values())
    real_source_count = sum(item.get("n_sources", 0) for item in stats.values())
    total_tiles = progress_data.get("total_tiles", 22585)
    
    percent = (n_completed / total_tiles * 100) if total_tiles > 0 else 0.0

    print(f"Tiles:   {n_completed:,} / {total_tiles:,} ({percent:.1f}%)")
    print(f"Sources: {real_source_count:,} scanned -> {real_csv_count:,} saved")
    
    # --- PRIMVS COVERAGE CHECK ---
    print("-" * 70)
    primvs = get_primvs_coverage(output_dir)
    if primvs["found"]:
        p_str = f"{primvs['percentage']:.2f}%"
        print(f"TARGET LIST ({PRIMVS_FILENAME})")
        print(f"Coverage: {p_str} ({primvs['hits']}/{primvs['checked_count']} found)")
        print(f"Total IDs: {primvs['total_targets']:,} | Method: {primvs['method']}")
    else:
        print(f"Target list '{PRIMVS_FILENAME}' not found (skipping coverage check)")

    # --- QC ---
    print("-" * 70)
    samples = get_sample_files(output_dir, n=3)
    if samples:
        print(f"QC Sample ({samples[0].name}):")
        health = inspect_file_health(samples[0])
        status = "OK" if health["valid"] else f"FAIL: {health['error']}"
        print(f"  Integrity: {status} | Size: {health['size']} bytes")
        if health["head"]:
            print(f"  Header: {health['head'][0][:60]}...")
            print(f"  Row 1:  {health['head'][1][:60]}...")
    else:
        print("Waiting for files...")

    print("=" * 70)

def watch_progress(output_dir: str, interval: int = 10):
    print(f"Watching... (Ctrl+C to stop)")
    try:
        while True:
            display_progress(output_dir, clear=True)
            time.sleep(interval)
    except KeyboardInterrupt:
        print("\nDone.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--output-dir", "-o", default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--watch", "-w", action="store_true")
    parser.add_argument("--interval", "-i", type=int, default=30)
    args = parser.parse_args()
    
    if args.watch:
        watch_progress(args.output_dir, args.interval)
    else:
        display_progress(args.output_dir, clear=False)
