"""Data processor module for TEMPO Analyzer."""

import xarray as xr
import numpy as np
from pathlib import Path
import logging
import pandas as pd

logger = logging.getLogger(__name__)

class DataProcessor:
    """Handles processing of TEMPO data (averaging, FNR calculation)."""
    
    @staticmethod
    def process_dataset(file_paths: list[Path]) -> xr.Dataset:
        """
        Load multiple NetCDF files and combine them preserving date-hour.
        
        Args:
            file_paths: List of paths to .nc files
            
        Returns:
            Processed xarray Dataset with each date-hour as separate timestep
        """
        if not file_paths:
            return None
            
        datasets = []
        try:
            # Load all datasets, extracting date and hour from filename
            for p in file_paths:
                try:
                    ds = xr.open_dataset(p)
                    
                    # Drop existing TSTEP variable if it exists (conflicts with dimension)
                    if 'TSTEP' in ds.variables:
                        ds = ds.drop_vars('TSTEP')
                    
                    # Extract date and hour from filename pattern: tempo_YYYY-MM-DD_HH.nc
                    fname = p.stem  # e.g., tempo_2024-12-01_17
                    parts = fname.split('_')
                    if len(parts) >= 3:
                        date_str = parts[1]  # 2024-12-01
                        hour = int(parts[-1])  # 17
                        
                        # Create full datetime and use TIME as dimension name
                        timestamp = pd.Timestamp(f"{date_str} {hour:02d}:00:00")
                        ds = ds.assign_coords(TIME=timestamp)
                        ds = ds.expand_dims('TIME')
                        
                        logger.info(f"Loaded {p.name} -> {timestamp}")
                    
                    datasets.append(ds)
                except Exception as e:
                    logger.warning(f"Failed to open {p}: {e}")
            
            if not datasets:
                return None
                
            # Combine along TIME dimension (each file is one timestep)
            combined = xr.concat(datasets, dim='TIME')
            
            # Sort by timestamp
            combined = combined.sortby('TIME')
            
            # Calculate FNR (HCHO / NO2)
            # Filter low NO2 to avoid division by zero or noise
            combined['FNR'] = xr.where(
                (combined['NO2_TropVCD'] > 1e-12) & (combined['HCHO_TotVCD'] > -9e30),
                combined['HCHO_TotVCD'] / combined['NO2_TropVCD'],
                np.nan
            )
            
            # Load data into memory so we can close source files
            combined.load()
            
            logger.info(f"Combined dataset: {len(combined.TIME)} timesteps from {combined.TIME.values[0]} to {combined.TIME.values[-1]}")
            
            return combined
            
        except Exception as e:
            logger.error(f"Processing failed: {e}")
            raise
        finally:
            # Close all opened datasets
            for ds in datasets:
                try:
                    ds.close()
                except Exception:
                    pass

    @staticmethod
    def save_processed(dataset: xr.Dataset, output_path: Path):
        """Save processed dataset to NetCDF."""
        dataset.to_netcdf(output_path)

