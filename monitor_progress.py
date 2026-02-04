#!/usr/bin/env python3
"""
VIRAC Extraction Progress Monitor
==================================
Displays real-time progress and statistics for the extraction process.

Usage:
    python monitor_progress.py [--output-dir PATH] [--watch]
"""

import os
import sys
import json
import argparse
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


def count_csv_files(output_dir: Path) -> int:
    """Count CSV files in output directory."""
    try:
        return len(list(output_dir.glob("*.csv")))
    except:
        return 0


def format_bytes(size: int) -> str:
    """Format bytes to human-readable string."""
    for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
        if abs(size) < 1024.0:
            return f"{size:.1f} {unit}"
        size /= 1024.0
    return f"{size:.1f} PB"


def get_dir_size(path: Path) -> int:
    """Get total size of directory in bytes."""
    try:
        total = 0
        for f in path.glob("*.csv"):
            total += f.stat().st_size
        return total
    except:
        return 0


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
    
    # Load progress file
    progress = load_json_safe(checkpoint_dir / "progress.json")
    
    if progress:
        total_tiles = progress.get("total_tiles", 0)
        processed_tiles = progress.get("processed_tiles", 0)
        total_sources = progress.get("total_sources", 0)
        valid_sources = progress.get("valid_sources", 0)
        percent = progress.get("percent_complete", 0)
        last_update = progress.get("last_update", "N/A")
        
        print(f"Tiles:     {processed_tiles:,} / {total_tiles:,} ({percent:.1f}%)")
        print(f"Sources processed: {total_sources:,}")
        print(f"Valid sources:     {valid_sources:,}")
        print(f"Last update:       {last_update}")
    else:
        print("No progress data available yet.")
    
    print("-" * 70)
    
    # Load completed tiles
    completed = load_json_safe(checkpoint_dir / "completed_tiles.json")
    n_completed = len(completed.get("completed", []))
    print(f"Completed tiles: {n_completed:,}")
    
    # Load failed tiles
    failed = load_json_safe(checkpoint_dir / "failed_tiles.json")
    n_failed = len(failed.get("failed", {}))
    print(f"Failed tiles:    {n_failed:,}")
    
    print("-" * 70)
    
    # Count actual CSV files
    n_csv = count_csv_files(output_dir)
    dir_size = get_dir_size(output_dir)
    
    print(f"CSV files on disk: {n_csv:,}")
    print(f"Total size:        {format_bytes(dir_size)}")
    
    if n_csv > 0:
        avg_size = dir_size / n_csv
        print(f"Avg file size:     {format_bytes(avg_size)}")
    
    print("=" * 70)
    
    # Show recent failures if any
    if n_failed > 0 and n_failed <= 10:
        print("\nRecent failures:")
        for tile_id, info in list(failed.get("failed", {}).items())[-5:]:
            print(f"  {tile_id}: {info.get('error', 'Unknown')[:60]}")


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
        description="Monitor VIRAC light curve extraction progress"
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
        default=10,
        help="Refresh interval in seconds (for watch mode)"
    )
    
    args = parser.parse_args()
    
    if args.watch:
        watch_progress(args.output_dir, args.interval)
    else:
        display_progress(args.output_dir, clear=False)


if __name__ == "__main__":
    main()
