"""Transform nodes for filtering and post-processing."""

import pandas as pd
import numpy as np
from typing import List, Optional

from .base import Node, register_node
from .pipeline import ExportContext


@register_node
class GapFillNode(Node):
    """Fills gaps using monthly-hourly mean values."""
    
    node_type = "gap_fill"
    display_name = "Gap Fill (Monthly Mean)"
    category = "transform"
    
    def execute(self, data: pd.DataFrame, context: ExportContext) -> pd.DataFrame:
        """Fill NaN values using monthly-hourly mean."""
        if data.empty or 'Value' not in data.columns:
            return data
        
        # Need Month and Hour columns
        if 'Month' not in data.columns or 'Hour' not in data.columns:
            return data
        
        result = data.copy()
        
        # Calculate monthly-hourly means per site
        means = result.groupby(['Site', 'Month', 'Hour'])['Value'].transform('mean')
        
        # Fill NaNs
        result['Value'] = result['Value'].fillna(means)
        result['_gap_filled'] = data['Value'].isna()
        
        return result


@register_node
class FilterNode(Node):
    """Filters rows based on a condition."""
    
    node_type = "filter"
    display_name = "Filter"
    category = "transform"
    
    def __init__(self, column: str = "Value", operator: str = ">", threshold: float = 0, **kwargs):
        super().__init__(column=column, operator=operator, threshold=threshold, **kwargs)
        self.column = column
        self.operator = operator
        self.threshold = threshold
    
    def execute(self, data: pd.DataFrame, context: ExportContext) -> pd.DataFrame:
        """Filter rows based on condition."""
        if data.empty or self.column not in data.columns:
            return data
        
        col = data[self.column]
        
        if self.operator == ">":
            mask = col > self.threshold
        elif self.operator == ">=":
            mask = col >= self.threshold
        elif self.operator == "<":
            mask = col < self.threshold
        elif self.operator == "<=":
            mask = col <= self.threshold
        elif self.operator == "==":
            mask = col == self.threshold
        elif self.operator == "!=":
            mask = col != self.threshold
        else:
            return data
        
        return data[mask].copy()


@register_node
class RenameValueNode(Node):
    """Renames the Value column to a custom name."""
    
    node_type = "rename_value"
    display_name = "Rename Output"
    category = "transform"
    
    def __init__(self, new_name: str = "Value", **kwargs):
        super().__init__(new_name=new_name, **kwargs)
        self.new_name = new_name
    
    def execute(self, data: pd.DataFrame, context: ExportContext) -> pd.DataFrame:
        """Rename the Value column."""
        if 'Value' in data.columns:
            return data.rename(columns={'Value': self.new_name})
        return data


@register_node
class StatisticsNode(Node):
    """Computes additional statistics (std, min, max, count)."""
    
    node_type = "statistics"
    display_name = "Add Statistics"
    category = "transform"
    
    def __init__(self, include_std: bool = True, include_min: bool = False, 
                 include_max: bool = False, include_count: bool = True, **kwargs):
        super().__init__(include_std=include_std, include_min=include_min,
                        include_max=include_max, include_count=include_count, **kwargs)
        self.include_std = include_std
        self.include_min = include_min
        self.include_max = include_max
        self.include_count = include_count
    
    def execute(self, data: pd.DataFrame, context: ExportContext) -> pd.DataFrame:
        """Add statistics columns based on grouping."""
        # This node is a placeholder - statistics are typically computed
        # during spatial aggregation. Here we just add metadata flags.
        result = data.copy()
        result['_stats_std'] = self.include_std
        result['_stats_min'] = self.include_min
        result['_stats_max'] = self.include_max
        result['_stats_count'] = self.include_count
        return result
