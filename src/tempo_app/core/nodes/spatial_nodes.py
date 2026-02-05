"""Spatial aggregation nodes."""

import pandas as pd
import numpy as np
from typing import List

from .base import Node, register_node
from .pipeline import ExportContext


@register_node
class NearestPixelNode(Node):
    """Uses the single nearest pixel value (no aggregation)."""
    
    node_type = "nearest_pixel"
    display_name = "Nearest Pixel"
    category = "spatial"
    
    def execute(self, data: pd.DataFrame, context: ExportContext) -> pd.DataFrame:
        """Pass through - SelectVariableNode already extracts nearest pixel."""
        # The source node already extracts nearest pixel, so this is a no-op
        return data


@register_node
class NPixelAvgNode(Node):
    """Averages the N nearest pixels."""
    
    node_type = "n_pixel_avg"
    display_name = "N-Pixel Average"
    category = "spatial"
    
    def __init__(self, n_pixels: int = 4, **kwargs):
        super().__init__(n_pixels=n_pixels, **kwargs)
        self.n_pixels = n_pixels
    
    def execute(self, data: pd.DataFrame, context: ExportContext) -> pd.DataFrame:
        """Re-extract values using N nearest pixels and average."""
        ds = context.dataset
        sites = context.sites
        
        # Get variable name from data (we need to re-extract)
        # This node expects data from SelectVariableNode with '_row', '_col' columns
        if data.empty:
            return data
        
        lats = ds['LAT'].values
        lons = ds['LON'].values
        
        # Get the variable from the first row's metadata or infer
        # Actually, we need to know which variable to extract
        # For now, assume 'Value' column exists from source node
        
        result_rows = []
        
        for site_code, (t_lat, t_lon) in sites.items():
            # Find N nearest pixels
            dist = (lats - t_lat)**2 + (lons - t_lon)**2
            flat_indices = np.argsort(dist, axis=None)[:self.n_pixels]
            pixel_coords = [np.unravel_index(idx, dist.shape) for idx in flat_indices]
            
            # Get site rows from input data
            site_data = data[data['Site'] == site_code].copy()
            
            if site_data.empty:
                continue
            
            # For each timestep, average across N pixels
            # We need to re-extract from dataset - get the variable name
            # The Value column has single-pixel values, we need to recalculate
            
            # Get variable from context or infer from first source node
            var_names = [v for v in ds.data_vars if 'VCD' in v or 'FNR' in v]
            if not var_names:
                # Just pass through
                result_rows.append(site_data)
                continue
            
            var_name = var_names[0]  # Use first available
            
            # Extract all N pixel values
            pixel_values = []
            for r, c in pixel_coords:
                vals = ds[var_name].isel(ROW=r, COL=c).values
                pixel_values.append(vals)
            
            pixel_array = np.array(pixel_values)  # Shape: (N_pixels, Time)
            
            # Average across pixels (axis 0)
            with np.errstate(invalid='ignore'):
                avg_values = np.nanmean(pixel_array, axis=0)
            
            site_data['Value'] = avg_values
            site_data['_n_pixels'] = self.n_pixels
            result_rows.append(site_data)
        
        if result_rows:
            return pd.concat(result_rows, ignore_index=True)
        return data


@register_node
class RadiusAvgNode(Node):
    """Averages all pixels within a radius."""
    
    node_type = "radius_avg"
    display_name = "Radius Average"
    category = "spatial"
    
    def __init__(self, radius_km: float = 10.0, min_coverage: float = 0.5, **kwargs):
        super().__init__(radius_km=radius_km, min_coverage=min_coverage, **kwargs)
        self.radius_km = radius_km
        self.min_coverage = min_coverage
    
    def execute(self, data: pd.DataFrame, context: ExportContext) -> pd.DataFrame:
        """Average all pixels within radius."""
        ds = context.dataset
        sites = context.sites
        
        if data.empty:
            return data
        
        lats = ds['LAT'].values
        lons = ds['LON'].values
        
        result_rows = []
        
        for site_code, (t_lat, t_lon) in sites.items():
            # Calculate distances in km
            cos_lat = np.cos(np.deg2rad(t_lat))
            dy_km = (lats - t_lat) * 111.0
            dx_km = (lons - t_lon) * 111.0 * cos_lat
            dist_km = np.sqrt(dx_km**2 + dy_km**2)
            
            # Find pixels within radius
            mask = dist_km <= self.radius_km
            rows_in_radius, cols_in_radius = np.where(mask)
            
            if len(rows_in_radius) == 0:
                continue
            
            site_data = data[data['Site'] == site_code].copy()
            if site_data.empty:
                continue
            
            # Get variable
            var_names = [v for v in ds.data_vars if 'VCD' in v or 'FNR' in v]
            if not var_names:
                result_rows.append(site_data)
                continue
            
            var_name = var_names[0]
            
            # Extract all pixel values within radius
            pixel_values = []
            for r, c in zip(rows_in_radius, cols_in_radius):
                vals = ds[var_name].isel(ROW=r, COL=c).values
                pixel_values.append(vals)
            
            pixel_array = np.array(pixel_values)  # Shape: (N_pixels, Time)
            
            # Calculate averages and coverage
            with np.errstate(invalid='ignore'):
                avg_values = np.nanmean(pixel_array, axis=0)
                valid_counts = np.sum(np.isfinite(pixel_array), axis=0)
                coverage = valid_counts / len(rows_in_radius)
            
            # Apply min coverage threshold
            avg_values = np.where(coverage >= self.min_coverage, avg_values, np.nan)
            
            site_data['Value'] = avg_values
            site_data['_radius_km'] = self.radius_km
            site_data['_pixel_count'] = len(rows_in_radius)
            result_rows.append(site_data)
        
        if result_rows:
            return pd.concat(result_rows, ignore_index=True)
        return data
