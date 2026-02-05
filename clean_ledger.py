import json
from pathlib import Path

p = Path("/beegfs/car/njm/virac_lightcurves/checkpoints/completed_tiles.json")
if p.exists():
    with open(p, 'r') as f:
        data = json.load(f)
    
    initial_count = len(data['stats'])
    # Remove entries where n_sources > 1000 but n_valid == 0
    # (This keeps genuinely empty sky tiles, but resets the failed dense ones)
    new_stats = {
        k: v for k, v in data['stats'].items() 
        if not (v['n_sources'] > 1000 and v['n_valid'] == 0)
    }
    
    data['stats'] = new_stats
    data['completed'] = list(new_stats.keys())
    
    with open(p, 'w') as f:
        json.dump(data, f, indent=2)
        
    print(f"cleaned ledger: Removed {initial_count - len(new_stats)} corrupted tiles.")
