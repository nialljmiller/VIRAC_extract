#!/usr/bin/env python3
"""
VIRAC Extraction Progress Monitor (Quality Control Edition)
===========================================================
Displays real-time progress and performs deep integrity checks on random files.
Safe to run on directories with millions of files.
"""

import os
import sys
import json
import argparse
import subprocess
from pathlib import Path
from datetime import datetime
import time

DEFAULT_OUTPUT_DIR = "/beegfs/car/njm/virac_lightcurves/"


def load_json_safe(filepath: Path) -> dict:
    """Load JSON file safely."""
    if not filepath.exists():
        return {}
    try:
        with open(filepath, 'r') as f:
            return json.load(f)
    except:
        return {}


def get_directory_size_fast(path: Path, timeout: int = 2) -> str:
    """Get directory size using 'du' with timeout."""
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
    """
    Efficiently grab 'n' arbitrary CSV files using scandir.
    Does NOT attempt to list the full directory.
    """
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
    """Read file head and verify integrity."""
    result = {
        "valid": False,
        "head": [],
        "error": None,
        "size": 0
    }
    try:
        result["size"] = filepath.stat().st_size
        if result["size"] == 0:
            result["error"] = "File is empty (0 bytes)"
            return result
        
        with open(filepath, 'r') as f:
            # Read first 4 lines
            lines = [f.readline() for _ in range(4)]
            result["head"] = [L.strip() for L in lines if L]
            
        # Check Header
        expected_start = "mjd,ks_mag,ks_err"
        if not result["head"] or not result["head"][0].startswith(expected_start):
            result["error"] = "Invalid CSV Header"
        elif len(result["head"]) < 2:
            result["error"] = "File contains header only (no data)"
        else:
            result["valid"] = True
            
    except Exception as e:
        result["error"] = str(e)
        
    return result


def display_progress(output_dir: str, clear: bool = True):
    """Display progress and quality checks."""
    output_dir = Path(output_dir)
    checkpoint_dir = output_dir / "checkpoints"
    
    if clear:
        os.system('clear' if os.name == 'posix' else 'cls')
    
    print("=" * 70)
    print("VIRAC Light Curve Extraction Progress")
    print("=" * 70)
    print(f"Output directory: {output_dir}")
    print(f"Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("-" * 70)
    
    # --- Load Stats ---
    completed_data = load_json_safe(checkpoint_dir / "completed_tiles.json")
    failed_data = load_json_safe(checkpoint_dir / "failed_tiles.json")
    progress_data = load_json_safe(checkpoint_dir / "progress.json")
    
    stats = completed_data.get("stats", {})
    completed_list = completed_data.get("completed", [])
    
    n_completed = len(completed_list)
    n_failed = len(failed_data.get("failed", {}))
    
    real_csv_count = sum(item.get("n_valid", 0) for item in stats.values())
    real_source_count = sum(item.get("n_sources", 0) for item in stats.values())
    
    total_tiles = progress_data.get("total_tiles", 22585)
    percent = (n_completed / total_tiles * 100) if total_tiles > 0 else 0.0

    print(f"Tiles Completed:   {n_completed:,} / {total_tiles:,} ({percent:.1f}%)")
    print(f"Tiles Failed:      {n_failed:,}")
    print(f"Sources Scanned:   {real_source_count:,}")
    print(f"Valid Sources:     {real_csv_count:,} (Files Written)")
    
    # --- Quality Control Section ---
    print("-" * 70)
    print("QUALITY CONTROL & SANITY CHECKS")
    print("-" * 70)
    
    samples = get_sample_files(output_dir, n=5)
    
    if not samples:
        print("Waiting for files to be created...")
    else:
        print(f"Sample filenames on disk: {[f.name for f in samples]}")
        print("-" * 70)
        
        # Inspect 3 files for deep check
        files_to_inspect = samples[:3]
        all_passed = True
        
        for fpath in files_to_inspect:
            health = inspect_file_health(fpath)
            status = "PASS" if health["valid"] else f"FAIL [{health['error']}]"
            if not health["valid"]: all_passed = False
            
            print(f"File: {fpath.name}")
            print(f"Size: {health['size']} bytes | Integrity: {status}")
            print("Preview:")
            for i, line in enumerate(health["head"]):
                print(f"  {i+1}: {line}")
            print("")
            
        print("-" * 70)
        if all_passed:
            print("System Health: OK (Files appear valid and populated)")
        else:
            print("System Health: WARNING (Detected malformed or empty files)")

    print("=" * 70)
    
    if n_failed > 0:
        print("\nRecent Tile Failures:")
        recent_fails = list(failed_data.get("failed", {}).items())[-5:]
        for tile_id, info in recent_fails:
            print(f"  {tile_id}: {info.get('error', 'Unknown')[:80]}...")


def watch_progress(output_dir: str, interval: int = 10):
    """Continuously watch progress."""
    print(f"Watching progress (refresh every {interval}s). Press Ctrl+C to stop.")
    try:
        while True:
            display_progress(output_dir, clear=True)
            time.sleep(interval)
    except KeyboardInterrupt:
        print("\nStopped watching.")


def main():
    parser = argparse.ArgumentParser(description="Monitor VIRAC Extraction (QC Mode)")
    parser.add_argument("--output-dir", "-o", default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--watch", "-w", action="store_true")
    parser.add_argument("--interval", "-i", type=int, default=30)
    
    args = parser.parse_args()
    
    if args.watch:
        watch_progress(args.output_dir, args.interval)
    else:
        display_progress(args.output_dir, clear=False)


if __name__ == "__main__":
    main()
