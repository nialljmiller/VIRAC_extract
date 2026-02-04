#!/usr/bin/env python3
"""
VIRAC Extraction Progress Monitor (Optimized)
==============================================
Displays real-time progress without crashing on millions of files.
Calculates stats from checkpoint JSONs instead of filesystem scanning.
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
    """
    Get directory size using 'du' command with a timeout.
    Avoids Python's slow recursive glob.
    """
    try:
        # Run 'du -sh' (summarize, human-readable)
        # using a short timeout so we don't hang if the filesystem is slow
        result = subprocess.run(
            ['du', '-sh', str(path)], 
            capture_output=True, 
            text=True, 
            timeout=timeout
        )
        if result.returncode == 0:
            return result.stdout.split()[0]  # Returns e.g., "150G"
    except subprocess.TimeoutExpired:
        return "Calculating..."
    except Exception:
        pass
    return "N/A"


def display_progress(output_dir: str, clear: bool = True):
    """Display current progress statistics."""
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
    
    # Load checkpoint files
    completed_data = load_json_safe(checkpoint_dir / "completed_tiles.json")
    failed_data = load_json_safe(checkpoint_dir / "failed_tiles.json")
    progress_data = load_json_safe(checkpoint_dir / "progress.json")
    
    # 1. Calculate stats from "Completed Tiles" (Source of Truth)
    # The 'stats' dict contains the exact number of valid sources per tile
    stats = completed_data.get("stats", {})
    completed_list = completed_data.get("completed", [])
    
    n_completed = len(completed_list)
    n_failed = len(failed_data.get("failed", {}))
    
    # Sum up valid sources (which equals number of CSV files)
    # We iterate over the stats dictionary which is much faster than ls/glob
    real_csv_count = sum(item.get("n_valid", 0) for item in stats.values())
    real_source_count = sum(item.get("n_sources", 0) for item in stats.values())
    
    # 2. Get Global Progress if available
    total_tiles = progress_data.get("total_tiles", 22585) # Default to known total if missing
    
    if total_tiles > 0:
        percent = (n_completed / total_tiles) * 100
    else:
        percent = 0.0

    print(f"Tiles Completed:   {n_completed:,} / {total_tiles:,} ({percent:.1f}%)")
    print(f"Tiles Failed:      {n_failed:,}")
    print("-" * 70)
    
    print(f"Sources Scanned:   {real_source_count:,}")
    print(f"Valid Sources:     {real_csv_count:,} (Files Written)")
    
    # 3. Disk Usage (Safe Method)
    # Only try to calculate size if we are in watch mode or specifically requested,
    # because even 'du' can be slow on huge directories.
    dir_size = get_directory_size_fast(output_dir)
    print(f"Disk Usage:        {dir_size}")
    
    print("=" * 70)
    
    # Show recent failures if any
    if n_failed > 0:
        print("\nRecent failures:")
        # Show last 5 failures
        recent_fails = list(failed_data.get("failed", {}).items())[-5:]
        for tile_id, info in recent_fails:
            err = info.get('error', 'Unknown')
            # Truncate error message to fit on screen
            print(f"  {tile_id}: {err[:80]}...")


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
    parser = argparse.ArgumentParser(
        description="Monitor VIRAC light curve extraction progress (Safe Mode)"
    )
    parser.add_argument(
        "--output-dir", "-o",
        default=DEFAULT_OUTPUT_DIR,
        help="Output directory to monitor"
    )
    parser.add_argument(
        "--watch", "-w",
        action="store_true",
        help="Continuously watch progress"
    )
    parser.add_argument(
        "--interval", "-i",
        type=int,
        default=30,
        help="Refresh interval in seconds"
    )
    
    args = parser.parse_args()
    
    if args.watch:
        watch_progress(args.output_dir, args.interval)
    else:
        display_progress(args.output_dir, clear=False)


if __name__ == "__main__":
    main()
