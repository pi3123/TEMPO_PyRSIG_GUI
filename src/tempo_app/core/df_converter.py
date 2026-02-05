"""
Converts TEMPO xarray Datasets to pandas DataFrames for AI analysis.

Flattens multi-dimensional data (TIME, LAT, LON) into a tabular format.
"""

import pandas as pd
import xarray as xr
from pathlib import Path
from typing import Optional


class DataFrameConverter:
    """
    Converts TEMPO NetCDF datasets to pandas DataFrames.

    The conversion flattens the 3D structure (TIME, LAT, LON) into rows,
    making it suitable for pandas/matplotlib operations.
    """

    @staticmethod
    def dataset_to_dataframe(
        dataset_path: Path,
        include_coords: bool = True,
        downsample: Optional[int] = None
    ) -> pd.DataFrame:
        """
        Convert a TEMPO dataset to a pandas DataFrame.

        Args:
            dataset_path: Path to the NetCDF file
            include_coords: Whether to include LAT/LON columns
            downsample: If set, take every Nth point to reduce size

        Returns:
            DataFrame with columns:
                - TIME (datetime)
                - NO2_TropVCD (float)
                - HCHO_TotVCD (float)
                - FNR (float, if available)
                - LAT (float, if include_coords=True)
                - LON (float, if include_coords=True)

        Example:
            df = DataFrameConverter.dataset_to_dataframe(
                Path("dataset.nc"),
                downsample=10  # Take every 10th point
            )
        """

        # Load xarray dataset
        ds = xr.open_dataset(dataset_path)

        # Convert to DataFrame
        df = ds.to_dataframe().reset_index()
        
        # Close the dataset
        ds.close()

        # Apply downsampling if requested
        if downsample and downsample > 1:
            df = df.iloc[::downsample].reset_index(drop=True)

        # Remove coordinate columns if not needed (saves memory)
        if not include_coords:
            df = df.drop(columns=['LAT', 'LON'], errors='ignore')

        # Ensure TIME is datetime
        if 'TIME' in df.columns:
            df['TIME'] = pd.to_datetime(df['TIME'])

        # Sort by time for better plotting
        if 'TIME' in df.columns:
            df = df.sort_values('TIME').reset_index(drop=True)

        return df

    @staticmethod
    def get_schema_fast(dataset_path: Path) -> dict:
        """
        Extract schema from NetCDF without loading full data into memory.
        
        This is much faster than loading the entire DataFrame for schema extraction.
        Uses xarray's lazy loading to only read metadata and a small sample.
        
        Args:
            dataset_path: Path to the NetCDF file
            
        Returns:
            Schema dictionary compatible with ChartGenerator.generate_code():
            {
                "columns": [...],
                "dtypes": {...},
                "shape": (rows, cols),
                "sample_values": {...}
            }
        """
        import json
        
        # Check for cached schema file first
        schema_path = dataset_path.with_suffix('.schema.json')
        if schema_path.exists():
            try:
                with open(schema_path, 'r') as f:
                    return json.load(f)
            except Exception:
                pass  # Fall through to live extraction
        
        # Open dataset lazily (doesn't load data into memory)
        ds = xr.open_dataset(dataset_path)
        
        try:
            # Get variable names (these become columns)
            data_vars = list(ds.data_vars.keys())
            coord_vars = list(ds.coords.keys())
            all_columns = coord_vars + data_vars
            
            # Get dtypes
            dtypes = {}
            for var in data_vars:
                dtypes[var] = str(ds[var].dtype)
            for coord in coord_vars:
                dtypes[coord] = str(ds.coords[coord].dtype)
            
            # Estimate shape (total data points)
            # For gridded data: product of dimension sizes
            total_points = 1
            for dim in ds.dims:
                total_points *= ds.dims[dim]
            
            # Get sample values for categorical-like variables (if any string types)
            sample_values = {}
            
            # Add temporal features that will be added by add_temporal_features()
            if 'TIME' in all_columns or 'time' in [c.lower() for c in all_columns]:
                all_columns.extend(['hour', 'day_of_week', 'is_weekend', 'date'])
                dtypes['hour'] = 'int64'
                dtypes['day_of_week'] = 'object'
                dtypes['is_weekend'] = 'bool'
                dtypes['date'] = 'object'
            
            schema = {
                "columns": all_columns,
                "dtypes": dtypes,
                "shape": (total_points, len(all_columns)),
                "sample_values": sample_values
            }
            
            return schema
            
        finally:
            ds.close()
    
    @staticmethod
    def save_schema(dataset_path: Path, schema: dict = None) -> Path:
        """
        Save schema to a JSON file alongside the dataset.
        
        Args:
            dataset_path: Path to the NetCDF file
            schema: Optional pre-computed schema. If None, will extract.
            
        Returns:
            Path to the saved schema file
        """
        import json
        
        if schema is None:
            schema = DataFrameConverter.get_schema_fast(dataset_path)
        
        schema_path = dataset_path.with_suffix('.schema.json')
        with open(schema_path, 'w') as f:
            json.dump(schema, f, indent=2)
        
        return schema_path

    @staticmethod
    def add_site_data(
        df: pd.DataFrame,
        sites: list[tuple[str, str, float, float]],
        tolerance: float = 0.05
    ) -> pd.DataFrame:
        """
        Add site_code and site_name columns by matching LAT/LON.

        Args:
            df: DataFrame with LAT/LON columns
            sites: List of (code, name, lat, lon) tuples
            tolerance: Lat/lon matching tolerance in degrees (default 0.05 ~ 5km)

        Returns:
            DataFrame with added 'site_code' and 'site_name' columns

        Example:
            sites = [
                ("BV", "Bakersfield - Planz", 35.3528, -119.0369),
                ("LA", "Los Angeles", 34.0522, -118.2437),
            ]
            df = DataFrameConverter.add_site_data(df, sites)
        """

        if 'LAT' not in df.columns or 'LON' not in df.columns:
            # Can't match without coordinates, return as-is
            return df

        # Initialize columns
        df['site_code'] = None
        df['site_name'] = None

        # Match each row to nearest site
        for code, name, site_lat, site_lon in sites:
            # Find points within tolerance
            mask = (
                (abs(df['LAT'] - site_lat) < tolerance) &
                (abs(df['LON'] - site_lon) < tolerance)
            )

            df.loc[mask, 'site_code'] = code
            df.loc[mask, 'site_name'] = name

        return df

    @staticmethod
    def add_temporal_features(df: pd.DataFrame) -> pd.DataFrame:
        """
        Add useful time-based columns for analysis.

        Args:
            df: DataFrame with TIME column

        Returns:
            DataFrame with added columns:
                - hour: Hour of day (0-23)
                - day_of_week: Day name (Monday, Tuesday, ...)
                - is_weekend: Boolean
                - date: Date only (no time)

        Example:
            df = DataFrameConverter.add_temporal_features(df)
            # Now can query: "Plot NO2 on weekdays"
        """

        if 'TIME' not in df.columns:
            # Can't add features without TIME, return as-is
            return df

        # Ensure TIME is datetime
        if not pd.api.types.is_datetime64_any_dtype(df['TIME']):
            df['TIME'] = pd.to_datetime(df['TIME'])

        df['hour'] = df['TIME'].dt.hour
        df['day_of_week'] = df['TIME'].dt.day_name()
        df['is_weekend'] = df['TIME'].dt.dayofweek >= 5
        df['date'] = df['TIME'].dt.date

        return df
