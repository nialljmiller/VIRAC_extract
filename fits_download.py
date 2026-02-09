import subprocess
from primvs_api import PrimvsCatalog

# Configuration
REMOTE_HOST = "njm@uhhpc.herts.ac.uk"
REMOTE_BASE = "/beegfs/car/njm/virac_lightcurves/"
LOCAL_BASE = "/media/bigdata/PRIMVS/light_curves/"

def download_missing_file(source_id):
    """Download a single missing file via rsync."""
    source_str = str(int(source_id))
    subdir1 = source_str[:3]
    subdir2 = source_str[3:6]
    relative_path = f"{subdir1}/{subdir2}/{source_str}.csv"
    
    remote_path = f"{REMOTE_HOST}:{REMOTE_BASE}{relative_path}"
    local_path = f"{LOCAL_BASE}{subdir1}/{subdir2}/"
    
    # Create local directory if needed
    subprocess.run(["mkdir", "-p", local_path], check=True)
    
    # Download the file
    cmd = ["rsync", "-avhP", remote_path, local_path]
    print(f"Downloading {source_id}...")
    result = subprocess.run(cmd, capture_output=True, text=True)
    
    if result.returncode == 0:
        print(f"✓ Downloaded {source_id}")
        return True
    else:
        print(f"✗ Failed to download {source_id}: {result.stderr}")
        return False

cat = PrimvsCatalog(LOCAL_BASE)

# Read source IDs from FITS
from astropy.io import fits as afits
from astropy.table import Table
import sys

if len(sys.argv) < 2:
    print("Usage: python combine_lightcurves.py <input_fits> [output_fits]")
    print("Example: python combine_lightcurves.py reclass.fits reclass_lightcurves.fits")
    sys.exit(1)

input_fits = sys.argv[1]

tbl = Table.read(input_fits, hdu=1)
source_ids = tbl["sourceid"].data
print(f"Found {len(source_ids)} source IDs in reclass.fits\n")

# Check and download
downloaded = 0
already_exist = 0

for sid in source_ids:
    if cat.source_exists(sid):
        already_exist += 1
    else:
        if download_missing_file(sid):
            downloaded += 1

print(f"\nSummary:")
print(f"  Already existed: {already_exist}")
print(f"  Downloaded: {downloaded}")
print(f"  Total: {len(source_ids)}")

# Now retrieve the lightcurves
print("\nRetrieving lightcurves...")
results = cat.get_lightcurves(source_ids)
for sid, lc in results.items():
    print(f"{sid}: {len(lc)} detections")
