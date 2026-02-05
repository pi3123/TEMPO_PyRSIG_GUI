"""Source nodes for data loading and variable selection."""

import pandas as pd
import numpy as np
from typing import List, Optional

from .base import Node, register_node
from .pipeline import ExportContext


@register_node
class SelectVariableNode(Node):
    """Selects a variable from the dataset and extracts values for all sites."""
    
    node_type = "select_variable"
    display_name = "Select Variable"
    category = "source"
    
    def __init__(self, variable: str = "NO2_TropVCD", **kwargs):
        super().__init__(variable=variable, **kwargs)
        self.variable = variable
    
    def execute(self, data: pd.DataFrame, context: ExportContext) -> pd.DataFrame:
        """Extract variable data for all sites from the dataset."""
        ds = context.dataset
        sites = context.sites

        if self.variable not in ds:
            available = list(ds.data_vars.keys())
            raise ValueError(
                f"Variable '{self.variable}' not found.\n"
                f"Available: {', '.join(available)}\n"
                f"This dataset may not have included this variable."
            )
        
        # Get coordinates
        lats = ds['LAT'].values
        lons = ds['LON'].values
        
        # Get time info
        if 'TSTEP' in ds.dims:
            utc_times = pd.to_datetime(ds['TSTEP'].values)
        elif 'hour' in ds.dims:
            hours = ds['hour'].values
            utc_times = pd.to_datetime([f"2000-01-01 {h:02d}:00:00" for h in hours])
        else:
            raise ValueError("No time dimension found in dataset")
        
        local_times = utc_times + pd.Timedelta(hours=context.utc_offset)
        
        # Build rows for all sites
        rows = []
        for site_code, (t_lat, t_lon) in sites.items():
            # Find nearest pixel
            dist = (lats - t_lat)**2 + (lons - t_lon)**2
            flat_idx = np.argmin(dist)
            row_idx, col_idx = np.unravel_index(flat_idx, dist.shape)
            
            # Extract values at this pixel
            values = ds[self.variable].isel(ROW=row_idx, COL=col_idx).values
            
            for i, (utc, local, val) in enumerate(zip(utc_times, local_times, values)):
                rows.append({
                    'Site': site_code,
                    'UTC_Time': utc,
                    'Local_Time': local,
                    'Date': local.date(),
                    'Month': local.month,
                    'Hour': local.hour,
                    'Value': val,
                    '_lat': t_lat,
                    '_lon': t_lon,
                    '_row': row_idx,
                    '_col': col_idx,
                })
        
        return pd.DataFrame(rows)
    
    def validate(self) -> List[str]:
        errors = []
        if not self.variable:
            errors.append("Variable name is required")
        return errors
