"""Temporal aggregation nodes."""

import pandas as pd
import numpy as np
from typing import List

from .base import Node, register_node
from .pipeline import ExportContext


@register_node
class HourlyNode(Node):
    """Pass-through node - keeps data at hourly resolution."""
    
    node_type = "hourly"
    display_name = "Hourly (Raw)"
    category = "temporal"
    
    def execute(self, data: pd.DataFrame, context: ExportContext) -> pd.DataFrame:
        """No aggregation, return as-is."""
        return data


@register_node  
class DailyMeanNode(Node):
    """Aggregates to daily mean values."""
    
    node_type = "daily_mean"
    display_name = "Daily Mean"
    category = "temporal"
    
    def __init__(self, min_hours: int = 1, **kwargs):
        super().__init__(min_hours=min_hours, **kwargs)
        self.min_hours = min_hours
    
    def execute(self, data: pd.DataFrame, context: ExportContext) -> pd.DataFrame:
        """Aggregate to daily mean per site."""
        if data.empty or 'Value' not in data.columns:
            return data
        
        # Group by Site and Date
        grouped = data.groupby(['Site', 'Date']).agg({
            'Value': 'mean',
            'UTC_Time': 'first',
            'Local_Time': 'first',
            'Month': 'first',
        })
        
        # Count hours per day
        hour_counts = data.groupby(['Site', 'Date']).size()
        grouped['_hour_count'] = hour_counts
        
        # Apply min hours filter
        if self.min_hours > 0:
            valid_mask = grouped['_hour_count'] >= self.min_hours
            grouped = grouped[valid_mask]
        
        return grouped.reset_index()


@register_node
class DiurnalCycleNode(Node):
    """Aggregates to monthly-hourly averages (diurnal cycle)."""
    
    node_type = "diurnal_cycle"
    display_name = "Diurnal Cycle"
    category = "temporal"
    
    def execute(self, data: pd.DataFrame, context: ExportContext) -> pd.DataFrame:
        """Aggregate to monthly-hourly average per site."""
        if data.empty or 'Value' not in data.columns:
            return data
        
        # Group by Site, Month, Hour
        grouped = data.groupby(['Site', 'Month', 'Hour']).agg({
            'Value': 'mean',
        })
        
        # Count samples
        counts = data.groupby(['Site', 'Month', 'Hour']).size()
        grouped['_sample_count'] = counts
        
        return grouped.reset_index()
