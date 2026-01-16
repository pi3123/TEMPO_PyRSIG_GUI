"""Data exporter module for TEMPO Analyzer."""

import pandas as pd
import xarray as xr
import numpy as np
from pathlib import Path
import logging
from typing import Optional
from math import radians, sin, cos, sqrt, atan2

from .constants import SITES

logger = logging.getLogger(__name__)

MISSING_VALUE = -999


def haversine(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Calculate great-circle distance between two points in kilometers."""
    R = 6371.0  # Earth radius in kilometers
    lat1_rad, lon1_rad = radians(lat1), radians(lon1)
    lat2_rad, lon2_rad = radians(lat2), radians(lon2)
    dlat = lat2_rad - lat1_rad
    dlon = lon2_rad - lon1_rad
    a = sin(dlat / 2)**2 + cos(lat1_rad) * cos(lat2_rad) * sin(dlon / 2)**2
    c = 2 * atan2(sqrt(a), sqrt(1 - a))
    return R * c


def find_n_nearest_cells(lat: float, lon: float, lats_2d: np.ndarray,
                         lons_2d: np.ndarray, n: int) -> list[tuple]:
    """Find the N nearest grid cells to a target location."""
    distances = np.zeros_like(lats_2d)
    rows, cols = lats_2d.shape
    for i in range(rows):
        for j in range(cols):
            distances[i, j] = haversine(lat, lon, lats_2d[i, j], lons_2d[i, j])
    flat_idx = np.argsort(distances, axis=None)[:n]
    row_indices, col_indices = np.unravel_index(flat_idx, distances.shape)
    result = []
    for r, c in zip(row_indices, col_indices):
        result.append((int(r), int(c), float(distances[r, c])))
    return result


def find_cells_within_distance(lat: float, lon: float, lats_2d: np.ndarray,
                               lons_2d: np.ndarray, max_distance_km: float) -> list[tuple]:
    """Find all grid cells within a specified distance from a target location.
    
    Args:
        lat, lon: Target coordinates
        lats_2d, lons_2d: Grid coordinates arrays
        max_distance_km: Maximum distance in kilometers
        
    Returns:
        List of (row, col, distance) tuples sorted by distance
    """
    distances = np.zeros_like(lats_2d)
    rows, cols = lats_2d.shape
    for i in range(rows):
        for j in range(cols):
            distances[i, j] = haversine(lat, lon, lats_2d[i, j], lons_2d[i, j])
    
    # Find all indices where distance <= max_distance_km
    row_indices, col_indices = np.where(distances <= max_distance_km)
    
    result = []
    for r, c in zip(row_indices, col_indices):
        result.append((int(r), int(c), float(distances[r, c])))
        
    # Sort by distance
    result.sort(key=lambda x: x[2])
    
    return result





def filter_sites_in_bbox(sites: dict, dataset: xr.Dataset) -> dict:
    """Filter sites to only those within dataset's bounding box."""
    if 'LAT' not in dataset.coords:
        return {}
    lats = dataset['LAT'].values
    lons = dataset['LON'].values
    min_lat, max_lat = lats.min(), lats.max()
    min_lon, max_lon = lons.min(), lons.max()
    return {
        site: coords for site, coords in sites.items()
        if (min_lat <= coords[0] <= max_lat) and (min_lon <= coords[1] <= max_lon)
    }


def apply_monthly_hourly_fill(df: pd.DataFrame, value_columns: list) -> pd.DataFrame:
    """Fill NaN values using monthly-hourly climatological means."""
    df_filled = df.copy()
    group_cols = []
    if 'Site' in df.columns:
        group_cols.append('Site')
    if 'Month' in df.columns:
        group_cols.append('Month')
    if 'Hour' in df.columns:
        group_cols.append('Hour')
    if not group_cols:
        return df_filled
    means = df_filled.groupby(group_cols)[value_columns].transform('mean')
    df_filled[value_columns] = df_filled[value_columns].fillna(means)
    return df_filled


class DataExporter:
    """Handles exporting processed data to Excel formats."""

    def __init__(self, output_dir: Path):
        """Initialize exporter with output directory."""
        self.output_dir = output_dir / "exports"
        self.output_dir.mkdir(parents=True, exist_ok=True)

    def export_dataset(self,
                      dataset: xr.Dataset,
                      dataset_name: str,
                      export_format: str,
                      num_points: Optional[int] = None,
                      distance_km: Optional[float] = None,
                      utc_offset: float = -6.0,
                      metadata: Optional[dict] = None,
                      sites: Optional[dict] = None) -> list[str]:
        """Export dataset in specified format.

        Args:
            dataset: xarray.Dataset with LAT, LON coords
            dataset_name: Base name for output files
            export_format: One of "hourly_multicell", "daily_aggregated", "spatial_average"
            num_points: Number of nearest cells to include. Defaults depend on format.
            distance_km: Radius in km to include cells (overrides num_points if set)
            utc_offset: Hours offset from UTC for local time (default -6.0)
            metadata: Optional dictionary of dataset metadata (settings, stats)
            sites: Dict mapping site code to (lat, lon) tuple. Uses database sites if None.

        Returns:
            List of generated file paths
        """
        # Use provided sites or fall back to hardcoded SITES constant
        sites_to_use = sites if sites is not None else SITES

        if export_format == "hourly" or export_format == "hourly_multicell":
            np = num_points if num_points is not None else 9
            return self._export_hourly(dataset, dataset_name, utc_offset, np, distance_km, metadata, sites_to_use)
        elif export_format == "daily" or export_format == "daily_aggregated":
            np = num_points if num_points is not None else 9
            return self._export_daily(dataset, dataset_name, utc_offset, np, distance_km, metadata, sites_to_use)
        elif export_format == "spatial_average":
            np = num_points if num_points is not None else 4 # Default for spatial average
            return self._export_spatial_average(dataset, dataset_name, utc_offset, np, distance_km, metadata, sites_to_use)
        else:
            raise ValueError(f"Unknown export format: {export_format}")

    def _create_metadata_df(self, metadata: dict) -> pd.DataFrame:
        """Create a DataFrame from metadata dictionary for export."""
        if not metadata:
            return pd.DataFrame({'Parameter': ['No metadata available'], 'Value': ['']})
        
        rows = []
        # Add basic settings first
        settings_keys = ['max_cloud', 'max_sza', 'date_start', 'date_end', 'day_filter', 'hour_filter']
        for k in settings_keys:
            if k in metadata:
                rows.append({'Parameter': k, 'Value': str(metadata[k])})
        
        # Add any other keys
        for k, v in metadata.items():
            if k not in settings_keys:
                rows.append({'Parameter': k, 'Value': str(v)})
                
        return pd.DataFrame(rows)

    def _get_time_info(self, dataset: xr.Dataset):
        """Extract time dimension and values from dataset."""
        # Check for datetime dimensions: TIME (new), TSTEP (old), then HOUR
        if 'TIME' in dataset.dims:
            return 'TIME', pd.to_datetime(dataset['TIME'].values)
        elif 'TSTEP' in dataset.dims:
            return 'TSTEP', pd.to_datetime(dataset['TSTEP'].values)
        elif 'HOUR' in dataset.dims:
            # HOUR dimension - check if datetime or just integers
            hour_values = dataset['HOUR'].values
            if np.issubdtype(hour_values.dtype, np.datetime64):
                return 'HOUR', pd.to_datetime(hour_values)
            else:
                # Create datetime index from hour integers (use placeholder date)
                hours = [int(h) for h in hour_values]
                return 'HOUR', hours  # Return raw hours, handle downstream
        else:
            return None, None

    def _export_hourly(self, dataset: xr.Dataset, dataset_name: str,
                       utc_offset: float, num_points: int = 9,
                       distance_km: Optional[float] = None,
                       metadata: Optional[dict] = None,
                       sites: Optional[dict] = None) -> list[str]:
        """Export hourly format - separate file per site with N cells.

        Columns: UTC_Time, Local_Time (UTC-X.0), Cell1_NO2...CellN_NO2, Cell1_HCHO...CellN_HCHO
        """
        time_dim, time_values = self._get_time_info(dataset)
        if time_dim is None or 'LAT' not in dataset.coords:
            logger.warning("Missing required coords/dims")
            return []

        lats = dataset['LAT'].values
        lons = dataset['LON'].values

        sites_to_use = sites if sites is not None else SITES
        valid_sites = filter_sites_in_bbox(sites_to_use, dataset)
        if not valid_sites:
            logger.warning("No sites found within dataset bounds")
            return []

        # Handle time values
        if isinstance(time_values, pd.DatetimeIndex):
            utc_times = time_values
            local_times = utc_times + pd.Timedelta(hours=utc_offset)
            utc_col = utc_times
            local_col = local_times
        else:
            # Hours only - create time columns as strings
            hours = time_values
            utc_col = [f"{h:02d}:00 UTC" for h in hours]
            local_hours = [(h + int(utc_offset)) % 24 for h in hours]
            local_col = [f"{h:02d}:00 Local" for h in local_hours]

        generated_files = []

        for site, (t_lat, t_lon) in valid_sites.items():
            # Find cells
            if distance_km is not None:
                cells = find_cells_within_distance(t_lat, t_lon, lats, lons, distance_km)
            else:
                cells = find_n_nearest_cells(t_lat, t_lon, lats, lons, num_points)
            
            # Build data dictionary
            data = {
                'UTC_Time': utc_col,
                f'Local_Time (UTC{utc_offset:+.1f})': local_col,
            }

            # Extract NO2 and HCHO interleaved for each cell
            for i, (r, c, dist) in enumerate(cells):
                cell_num = i + 1
                if 'NO2_TropVCD' in dataset:
                    values = dataset['NO2_TropVCD'].isel(ROW=r, COL=c).values.flatten()
                    # Keep NaN as empty, not -999
                    data[f'Cell{cell_num}_NO2'] = values
                if 'HCHO_TotVCD' in dataset:
                    values = dataset['HCHO_TotVCD'].isel(ROW=r, COL=c).values.flatten()
                    data[f'Cell{cell_num}_HCHO'] = values

            # Create hourly data DataFrame
            df = pd.DataFrame(data)
            
            # Add Month and Hour columns for fill calculation
            if isinstance(time_values, pd.DatetimeIndex):
                df['Month'] = time_values.month
                df['Hour'] = time_values.hour
            else:
                # If only hours, we need dates from somewhere - assume single day
                df['Month'] = 1  # Placeholder
                df['Hour'] = [int(h) for h in time_values]
            
            # Get value columns (NO2 and HCHO)
            value_cols = [c for c in df.columns if '_NO2' in c or '_HCHO' in c]
            
            # Create NoFill version (replace NaN with -999)
            df_nofill = df.copy()
            for col in value_cols:
                df_nofill[col] = df_nofill[col].fillna(MISSING_VALUE)
            
            # Create Fill version (apply monthly-hourly mean fill)
            df_fill = apply_monthly_hourly_fill(df.copy(), value_cols)
            for col in value_cols:
                df_fill[col] = df_fill[col].fillna(MISSING_VALUE)  # Any remaining NaN -> -999
            
            # Drop Month/Hour from output (they were just for grouping)
            cols_to_drop = ['Month', 'Hour']
            df_nofill_out = df_nofill.drop(columns=cols_to_drop, errors='ignore')
            df_fill_out = df_fill.drop(columns=cols_to_drop, errors='ignore')
            
            # Create Grid_Info sheet with cell metadata
            grid_info = []
            for i, (r, c, dist) in enumerate(cells):
                cell_num = i + 1
                cell_lat = float(lats[r, c])
                cell_lon = float(lons[r, c])
                grid_info.append({
                    'Cell_ID': f'Cell{cell_num}',
                    'Lat': cell_lat,
                    'Lon': cell_lon,
                    'Dist_km': dist,
                    'Grid_Row': int(r),
                    'Grid_Col': int(c),
                })
            df_grid = pd.DataFrame(grid_info)
            
            # Save to Excel with multiple sheets
            fname = self.output_dir / f"{site}_{dataset_name}_hourly_multicell.xlsx"
            with pd.ExcelWriter(fname, engine='openpyxl') as writer:
                df_nofill_out.to_excel(writer, sheet_name='Hourly_NoFill', index=False)
                df_fill_out.to_excel(writer, sheet_name='Hourly_Fill', index=False)
                df_grid.to_excel(writer, sheet_name='Grid_Info', index=False)
                if metadata:
                    # Add site-specific stats to metadata
                    meta_df = self._create_metadata_df(metadata)
                    # Calculate missing data stats for this site
                    total_pts = len(df)
                    no2_missing = df.filter(like='_NO2').isna().sum().sum()
                    no2_total = df.filter(like='_NO2').size
                    hcho_missing = df.filter(like='_HCHO').isna().sum().sum()
                    hcho_total = df.filter(like='_HCHO').size
                    
                    # Count fill values applied
                    nofill_missing = (df_nofill_out.filter(like='_NO2') == MISSING_VALUE).sum().sum()
                    fill_missing = (df_fill_out.filter(like='_NO2') == MISSING_VALUE).sum().sum()
                    filled_count = nofill_missing - fill_missing
                    
                    stats_rows = pd.DataFrame([
                        {'Parameter': 'Site', 'Value': site},
                        {'Parameter': 'Total_Time_Steps', 'Value': total_pts},
                        {'Parameter': 'NO2_Missing_Pct', 'Value': f"{(no2_missing/no2_total)*100:.1f}%" if no2_total else "0%"},
                        {'Parameter': 'HCHO_Missing_Pct', 'Value': f"{(hcho_missing/hcho_total)*100:.1f}%" if hcho_total else "0%"},
                        {'Parameter': 'Fill_Applied_Count', 'Value': filled_count}
                    ])
                    meta_final = pd.concat([meta_df, stats_rows], ignore_index=True)
                    meta_final.to_excel(writer, sheet_name='Metadata', index=False)
            
            generated_files.append(str(fname))

        return generated_files

    def _export_daily(self, dataset: xr.Dataset, dataset_name: str,
                      utc_offset: float, num_points: int = 8,
                      distance_km: Optional[float] = None,
                      metadata: Optional[dict] = None,
                      sites: Optional[dict] = None) -> list[str]:
        """Export daily format - single file with all sites.

        Columns: Date, Site, TMP_NO2_NoFill_Ngridcells, TMP_NO2_NoFill_Ncnt, ...
        Uses hours 08-14 (inclusive) local time.
        Uses configurable num_points (default 8) for cell selection.
        """
        time_dim, time_values = self._get_time_info(dataset)
        if time_dim is None or 'LAT' not in dataset.coords:
            logger.warning("Missing required coords/dims")
            return []

        lats = dataset['LAT'].values
        lons = dataset['LON'].values

        sites_to_use = sites if sites is not None else SITES
        valid_sites = filter_sites_in_bbox(sites_to_use, dataset)
        if not valid_sites:
            logger.warning("No sites found within dataset bounds")
            return []

        # Handle time values - require full timestamps
        if isinstance(time_values, pd.DatetimeIndex):
            utc_times = time_values
            local_times = utc_times + pd.Timedelta(hours=utc_offset)
        else:
            # Dataset doesn't have proper timestamps - needs reprocessing
            logger.warning("Daily export requires full timestamps. Please reprocess the dataset.")
            return []

        all_rows = []

        for site, (t_lat, t_lon) in valid_sites.items():
            # Find cells
            if distance_km is not None:
                cells = find_cells_within_distance(t_lat, t_lon, lats, lons, distance_km)
                # For radius mode, use TEMPO_NoFill_NO2_Xkm style columns
                radius_str = f"{int(distance_km)}km" if distance_km == int(distance_km) else f"{distance_km}km"
                use_radius_naming = True
            else:
                cells = find_n_nearest_cells(t_lat, t_lon, lats, lons, num_points)
                use_radius_naming = False
            
            n_cells = len(cells)
            
            # Extract raw data for all cells
            raw_data = {
                'Local_Time': local_times,
                'Date': local_times.date,
                'Hour': local_times.hour,
                'Month': local_times.month,
                'Site': site,
            }
            
            # Extract NO2 and HCHO for each cell
            for i, (r, c, dist) in enumerate(cells):
                if 'NO2_TropVCD' in dataset:
                    raw_data[f'Cell{i}_NO2'] = dataset['NO2_TropVCD'].isel(ROW=r, COL=c).values.flatten()
                if 'HCHO_TotVCD' in dataset:
                    raw_data[f'Cell{i}_HCHO'] = dataset['HCHO_TotVCD'].isel(ROW=r, COL=c).values.flatten()
            
            df_site = pd.DataFrame(raw_data)
            
            # Filter to hours 8-14 local time
            df_filtered = df_site[(df_site['Hour'] >= 8) & (df_site['Hour'] <= 14)].copy()
            
            if df_filtered.empty:
                continue
            
            # Create filled version
            no2_cols = [f'Cell{i}_NO2' for i in range(n_cells) if f'Cell{i}_NO2' in df_filtered.columns]
            hcho_cols = [f'Cell{i}_HCHO' for i in range(n_cells) if f'Cell{i}_HCHO' in df_filtered.columns]
            value_cols = no2_cols + hcho_cols
            
            df_filled = apply_monthly_hourly_fill(df_filtered, value_cols)
            
            # Aggregate by date
            for date, grp in df_filtered.groupby('Date'):
                row = {'Date': date, 'Site': site}
                grp_filled = df_filled[df_filled['Date'] == date]
                
                # Column naming based on mode
                if use_radius_naming:
                    no2_nofill_col = f'TEMPO_NoFill_NO2_{radius_str}'
                    no2_nofill_cnt_col = 'TEMPO_NoFill_NO2_Cnt'
                    hcho_nofill_col = f'TEMPO_NoFill_HCHO_{radius_str}'
                    hcho_nofill_cnt_col = 'TEMPO_NoFill_HCHO_Cnt'
                    no2_fill_col = f'TEMPO_Fill_NO2_{radius_str}'
                    no2_fill_cnt_col = 'TEMPO_Fill_NO2_Cnt'
                    hcho_fill_col = f'TEMPO_Fill_HCHO_{radius_str}'
                    hcho_fill_cnt_col = 'TEMPO_Fill_HCHO_Cnt'
                else:
                    label = str(n_cells)
                    no2_nofill_col = f'NO2_NoFill_{label}_Avg'
                    no2_nofill_cnt_col = f'NO2_NoFill_{label}_Cnt'
                    hcho_nofill_col = f'HCHO_NoFill_{label}_Avg'
                    hcho_nofill_cnt_col = f'HCHO_NoFill_{label}_Cnt'
                    no2_fill_col = f'NO2_Fill_{label}_Avg'
                    no2_fill_cnt_col = f'NO2_Fill_{label}_Cnt'
                    hcho_fill_col = f'HCHO_Fill_{label}_Avg'
                    hcho_fill_cnt_col = f'HCHO_Fill_{label}_Cnt'
                
                # NoFill
                if no2_cols:
                    no2_vals = grp[no2_cols].values.flatten()
                    no2_valid = no2_vals[~np.isnan(no2_vals)]
                    # Also filter out fill values
                    no2_valid = no2_valid[(no2_valid > -900) & (no2_valid < 1e20)]
                    row[no2_nofill_col] = np.mean(no2_valid) if len(no2_valid) > 0 else MISSING_VALUE
                    row[no2_nofill_cnt_col] = len(no2_valid)
                
                if hcho_cols:
                    hcho_vals = grp[hcho_cols].values.flatten()
                    hcho_valid = hcho_vals[~np.isnan(hcho_vals)]
                    hcho_valid = hcho_valid[(hcho_valid > -900) & (hcho_valid < 1e20)]
                    row[hcho_nofill_col] = np.mean(hcho_valid) if len(hcho_valid) > 0 else MISSING_VALUE
                    row[hcho_nofill_cnt_col] = len(hcho_valid)
                
                # Fill
                if no2_cols:
                    no2_vals_f = grp_filled[no2_cols].values.flatten()
                    no2_valid_f = no2_vals_f[~np.isnan(no2_vals_f)]
                    no2_valid_f = no2_valid_f[(no2_valid_f > -900) & (no2_valid_f < 1e20)]
                    row[no2_fill_col] = np.mean(no2_valid_f) if len(no2_valid_f) > 0 else MISSING_VALUE
                    row[no2_fill_cnt_col] = len(no2_valid_f)
                
                if hcho_cols:
                    hcho_vals_f = grp_filled[hcho_cols].values.flatten()
                    hcho_valid_f = hcho_vals_f[~np.isnan(hcho_vals_f)]
                    hcho_valid_f = hcho_valid_f[(hcho_valid_f > -900) & (hcho_valid_f < 1e20)]
                    row[hcho_fill_col] = np.mean(hcho_valid_f) if len(hcho_valid_f) > 0 else MISSING_VALUE
                    row[hcho_fill_cnt_col] = len(hcho_valid_f)
                
                all_rows.append(row)
        
        if not all_rows:
            return []
        
        # Create final DataFrame and order columns properly
        df_final = pd.DataFrame(all_rows)
        
        # Order: Date, Site, then NoFill columns (smaller cell count first), then Fill columns
        base_cols = ['Date', 'Site']
        other_cols = [c for c in df_final.columns if c not in base_cols]
        
        # Sort columns: NoFill before Fill, smaller cell count first, NO2 before HCHO
        def col_sort_key(col):
            fill_order = 0 if 'NoFill' in col else 1
            no2_order = 0 if 'NO2' in col else 1
            cnt_order = 1 if 'Cnt' in col else 0
            # Extract cell count from column name
            import re
            match = re.search(r'_(\d+)_', col)
            cell_count = int(match.group(1)) if match else 0
            return (fill_order, cell_count, no2_order, cnt_order)
        
        other_cols.sort(key=col_sort_key)
        df_final = df_final[base_cols + other_cols]
        df_final = df_final.sort_values(['Date', 'Site']).reset_index(drop=True)
        
        # Save
        fname = self.output_dir / f"{dataset_name}_daily_aggregated.xlsx"
        with pd.ExcelWriter(fname, engine='openpyxl') as writer:
            df_final.to_excel(writer, sheet_name='Daily_Data', index=False)
            if metadata:
                meta_df = self._create_metadata_df(metadata)
                
                # Calculate stats for daily data
                stats_data = []
                total_rows = len(df_final)
                stats_data.append({'Parameter': 'Total_Site_Days', 'Value': total_rows})
                
                # Check missing data in NoFill columns
                nofill_cols = [c for c in df_final.columns if 'NoFill' in c and 'Avg' in c]
                for col in nofill_cols:
                    # MISSING_VALUE is -999
                    missing_cnt = (df_final[col] == MISSING_VALUE).sum()
                    stats_data.append({
                        'Parameter': f'{col}_Missing_Pct', 
                        'Value': f"{(missing_cnt/total_rows)*100:.1f}%" if total_rows else "0%"
                    })
                
                stats_df = pd.DataFrame(stats_data)
                meta_final = pd.concat([meta_df, stats_df], ignore_index=True)
                meta_final.to_excel(writer, sheet_name='Metadata', index=False)
        
        return [str(fname)]

    def _export_spatial_average(self, dataset: xr.Dataset, dataset_name: str,
                              utc_offset: float, num_points: int = 9,
                              distance_km: Optional[float] = None,
                              metadata: Optional[dict] = None,
                              sites: Optional[dict] = None) -> list[str]:
        """Export spatial average format - single file with spatial means per site."""
        time_dim, time_values = self._get_time_info(dataset)
        if time_dim is None or 'LAT' not in dataset.coords:
            logger.warning("Missing required coords/dims")
            return []

        lats = dataset['LAT'].values
        lons = dataset['LON'].values

        sites_to_use = sites if sites is not None else SITES
        valid_sites = filter_sites_in_bbox(sites_to_use, dataset)
        if not valid_sites:
            logger.warning("No sites found within dataset bounds")
            return []

        # Time handling
        if isinstance(time_values, pd.DatetimeIndex):
            utc_times = time_values
            local_times = utc_times + pd.Timedelta(hours=utc_offset)
        else:
            hours = time_values
            # Create dummy dates for hours
            base_date = pd.Timestamp.now().normalize()
            utc_times = [base_date + pd.Timedelta(hours=int(h)) for h in hours]
            local_times = [t + pd.Timedelta(hours=utc_offset) for t in utc_times]
            utc_times = pd.DatetimeIndex(utc_times)
            local_times = pd.DatetimeIndex(local_times)

        # Prepare DataFrames
        df_raw = pd.DataFrame({
            'UTC': utc_times,
            'Local': local_times,
            'Date': local_times.date,
            'Hour': local_times.hour
        })
        
        grid_cells_info = []
        site_stats = []

        for site, (t_lat, t_lon) in valid_sites.items():
             # Find cells
            if distance_km is not None:
                cells = find_cells_within_distance(t_lat, t_lon, lats, lons, distance_km)
            else:
                cells = find_n_nearest_cells(t_lat, t_lon, lats, lons, num_points)
            
            site_stats.append({'Site': site, 'Points': len(cells)})
            
            # Record grid cells info
            for r, c, dist in cells:
                grid_cells_info.append({
                    'Site': site,
                    'Grid_Lat': float(lats[r, c]),
                    'Grid_Lon': float(lons[r, c]),
                    'Dist (km)': dist
                })

            # Calculate spatial means
            no2_means = []
            hcho_means = []
            
            # Extract data for all timesteps
            for t_idx in range(len(utc_times)):
                # Get values for all cells at this timestep
                no2_vals = []
                hcho_vals = []
                
                for r, c, _ in cells:
                    if 'NO2_TropVCD' in dataset:
                        # Handle TIME, TSTEP, or HOUR dimension
                        val = dataset['NO2_TropVCD'].isel(**{time_dim: t_idx}, ROW=r, COL=c).item()
                        no2_vals.append(val)
                    
                    if 'HCHO_TotVCD' in dataset:
                        val = dataset['HCHO_TotVCD'].isel(**{time_dim: t_idx}, ROW=r, COL=c).item()
                        hcho_vals.append(val)
                
                # Average (ignoring NaNs)
                # Suppress RuntimeWarning for mean of empty slice
                with np.errstate(all='ignore'):
                    no2_mean = np.nanmean(no2_vals) if no2_vals else np.nan
                    hcho_mean = np.nanmean(hcho_vals) if hcho_vals else np.nan
                
                no2_means.append(no2_mean)
                hcho_means.append(hcho_mean)
            
            # Add to DataFrame
            df_raw[f'{site}_NO2'] = no2_means
            df_raw[f'{site}_HCHO'] = hcho_means
            
            # Calculate FNR
            # Avoid division by zero/small numbers
            no2_arr = np.array(no2_means)
            hcho_arr = np.array(hcho_means)
            fnr = np.where(no2_arr > 1e-12, hcho_arr / no2_arr, np.nan)
            df_raw[f'{site}_FNR'] = fnr

        # Create Filled_Data (gap filling)
        df_filled = df_raw.copy()
        value_cols = [c for c in df_filled.columns if '_NO2' in c or '_HCHO' in c or '_FNR' in c]
        
        # Helper to fill by hour
        for col in value_cols:
            means = df_filled.groupby('Hour')[col].transform('mean')
            df_filled[col] = df_filled[col].fillna(means)

        # Save
        fname = self.output_dir / f"FNR_{dataset_name}_spatial_average.xlsx"
        with pd.ExcelWriter(fname, engine='openpyxl') as writer:
            df_raw.to_excel(writer, sheet_name='Raw_Data', index=False)
            df_filled.to_excel(writer, sheet_name='Filled_Data', index=False)
            pd.DataFrame(site_stats).to_excel(writer, sheet_name='Summary', index=False)
            pd.DataFrame(grid_cells_info).to_excel(writer, sheet_name='Grid_Cells', index=False)
            
            if metadata:
                self._create_metadata_df(metadata).to_excel(writer, sheet_name='Metadata', index=False)
                
        return [str(fname)]
