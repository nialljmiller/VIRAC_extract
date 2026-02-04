import os
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from pathlib import Path

# Configuration
DEFAULT_DATA_DIR = "/beegfs/car/njm/virac_lightcurves/"

class ViracCatalog:
    def __init__(self, data_dir=DEFAULT_DATA_DIR):
        self.data_dir = Path(data_dir)
        if not self.data_dir.exists():
            raise FileNotFoundError(f"Catalog directory not found: {self.data_dir}")

    def get_lightcurve(self, source_id):
        """
        Retrieve the lightcurve for a given VIRAC Source ID.
        
        Args:
            source_id (int or str): The unique source identifier.
            
        Returns:
            pd.DataFrame: A pandas DataFrame containing the photometry.
            Returns None if the source ID is not found.
        """
        filepath = self.data_dir / f"{source_id}.csv"
        
        if not filepath.exists():
            print(f"Error: Source ID {source_id} not found in catalog.")
            return None
            
        try:
            # fast_read=True optimized for standard CSVs
            df = pd.read_csv(filepath)
            
            # Convert MJD to numeric, coercing errors just in case
            df['mjd'] = pd.to_numeric(df['mjd'], errors='coerce')
            
            # Create a 'mag' and 'err' column that coalesces the multi-band columns
            # This makes plotting easier (we rely on the 'filter' column for hue)
            df['mag'] = df['ks_mag'].fillna(df['z_mag']).fillna(df['y_mag']).fillna(df['j_mag']).fillna(df['h_mag'])
            df['err'] = df['ks_err'].fillna(df['z_err']).fillna(df['y_err']).fillna(df['j_err']).fillna(df['h_err'])
            
            return df
        
        except Exception as e:
            print(f"Error reading lightcurve for {source_id}: {e}")
            return None

    def plot_lightcurve(self, source_id, save_path=None):
        """
        Quickly visualize the lightcurve for a student.
        """
        df = self.get_lightcurve(source_id)
        if df is None:
            return

        plt.figure(figsize=(10, 6))
        sns.set_style("whitegrid")
        
        # Define VIRAC colors
        colors = {'Ks': 'r', 'Z': 'b', 'Y': 'g', 'J': 'orange', 'H': 'brown'}
        
        # Plot each filter
        for filt in df['filter'].unique():
            mask = df['filter'] == filt
            subset = df[mask]
            
            # Only plot valid detections
            valid = subset.dropna(subset=['mag'])
            
            if len(valid) > 0:
                plt.errorbar(
                    valid['mjd'], 
                    valid['mag'], 
                    yerr=valid['err'], 
                    fmt='o', 
                    label=filt,
                    color=colors.get(filt, 'gray'),
                    markersize=4,
                    alpha=0.7
                )

        plt.gca().invert_yaxis() # Magnitudes go backwards
        plt.title(f"VIRAC Lightcurve: {source_id}")
        plt.xlabel("MJD (Modified Julian Date)")
        plt.ylabel("Magnitude")
        plt.legend()
        
        if save_path:
            plt.savefig(save_path)
            print(f"Plot saved to {save_path}")
        else:
            plt.show()

# ==========================================
# CLI Usage (for quick testing)
# ==========================================
if __name__ == "__main__":
    import sys
    if len(sys.argv) < 2:
        print("Usage: python virac_api.py <source_id>")
        sys.exit(1)
        
    source_id = sys.argv[1]
    catalog = ViracCatalog()
    
    print(f"Fetching data for {source_id}...")
    lc = catalog.get_lightcurve(source_id)
    
    if lc is not None:
        print("\n--- First 5 rows ---")
        print(lc[['mjd', 'filter', 'mag', 'err', 'chi']].head())
        print(f"\nTotal detections: {len(lc)}")
        print(f"Filters found: {lc['filter'].unique()}")
        print("\nAttempting to plot...")
        try:
            catalog.plot_lightcurve(source_id, save_path=f"lc_{source_id}.png")
        except Exception as e:
            print(f"Could not plot (no display?): {e}")
