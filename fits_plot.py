import os
from pathlib import Path
from primvs_api import PrimvsCatalog

# Configuration
LOCAL_BASE = "/media/bigdata/PRIMVS/light_curves/"
FITS_FILE = "reclass.fits"

# Create figures directory
figures_dir = Path("figures")
figures_dir.mkdir(exist_ok=True)

# Initialize catalog
cat = PrimvsCatalog(LOCAL_BASE)

# Get source IDs from FITS file
results = cat.get_lightcurves_from_fits(FITS_FILE, id_column="sourceid")

print(f"\nGenerating plots for {len(results)} lightcurves...")

# Plot each lightcurve
for source_id, df in results.items():
    output_path = figures_dir / f"{source_id}.png"
    cat.plot_lightcurve(source_id, save_path=str(output_path))
    print(f"✓ Saved plot for {source_id}")

print(f"\nAll plots saved to {figures_dir.absolute()}")
