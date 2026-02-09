#!/usr/bin/env python3
"""
Combine lightcurves from a FITS catalog into a single FITS table.
"""
import pandas as pd
from astropy.table import Table
from astropy.io import fits
from pathlib import Path
from primvs_api import PrimvsCatalog

def combine_lightcurves_to_fits(input_fits, output_fits, id_column="sourceid", data_dir="/media/bigdata/PRIMVS/light_curves/"):
    """
    Read source IDs from input FITS, retrieve their lightcurves, and combine into a single FITS table.
    
    Args:
        input_fits: Input FITS file with source IDs
        output_fits: Output FITS file to write combined lightcurves
        id_column: Column name for source IDs in input FITS
        data_dir: Path to lightcurve data directory
    """
    cat = PrimvsCatalog(data_dir)
    
    # Read source IDs from input FITS
    input_table = Table.read(input_fits, hdu=1)
    source_ids = input_table[id_column].data
    print(f"Found {len(source_ids)} source IDs in {input_fits}")
    
    # Retrieve all lightcurves
    results = cat.get_lightcurves(source_ids)
    
    if not results:
        print("No lightcurves found!")
        return
    
    print(f"Retrieved {len(results)} lightcurves")
    
    # Combine all lightcurves into single DataFrame
    all_data = []
    for source_id, lc_df in results.items():
        # Add source_id column to each lightcurve
        lc_df['source_id'] = source_id
        all_data.append(lc_df)
    
    combined_df = pd.concat(all_data, ignore_index=True)
    
    # Reorder columns: source_id first, then the rest
    cols = ['source_id'] + [col for col in combined_df.columns if col != 'source_id']
    combined_df = combined_df[cols]
    
    print(f"Combined table has {len(combined_df)} observations from {len(results)} sources")
    
    # Convert to astropy Table and write to FITS
    output_table = Table.from_pandas(combined_df)
    output_table.write(output_fits, format='fits', overwrite=True)
    
    print(f"Wrote combined lightcurves to {output_fits}")
    
    # Print summary statistics
    print("\nSummary:")
    print(f"  Total observations: {len(combined_df)}")
    print(f"  Unique sources: {combined_df['source_id'].nunique()}")
    print(f"  Filters: {sorted(combined_df['filter'].unique())}")
    print(f"  MJD range: {combined_df['mjd'].min():.2f} - {combined_df['mjd'].max():.2f}")

if __name__ == "__main__":
    import sys
    
    if len(sys.argv) < 2:
        print("Usage: python combine_lightcurves.py <input_fits> [output_fits]")
        print("Example: python combine_lightcurves.py reclass.fits reclass_lightcurves.fits")
        sys.exit(1)
    
    input_fits = sys.argv[1]
    output_fits = sys.argv[2] if len(sys.argv) > 2 else input_fits.replace('.fits', '_lightcurves.fits')
    
    combine_lightcurves_to_fits(input_fits, output_fits)
