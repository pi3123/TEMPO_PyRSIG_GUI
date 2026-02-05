"""
Chart Executor - Executes ChartIntent using xarray for memory-efficient aggregation.

Supports:
- Multiple Y columns (comparison charts)
- Computed expressions (e.g., NO2 / HCHO)
- Group by site for multi-site comparison
- Temporal aggregation (hour, date, month, etc.)
"""

import logging
from pathlib import Path
from typing import Optional
import re

import matplotlib
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import xarray as xr

from .chart_intent import ChartIntent, ChartType, Aggregation

# Use non-interactive backend
matplotlib.use('Agg')

# Color palette for multiple series
COLORS = ['#2E86C1', '#E74C3C', '#27AE60', '#9B59B6', '#F39C12', '#1ABC9C', '#34495E', '#E91E63']


class ChartExecutionError(Exception):
    """Raised when chart execution fails."""
    pass


class ChartExecutor:
    """
    Executes ChartIntent using xarray for lazy aggregation.
    
    Supports multiple y columns, expressions, and group_by.
    """
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
    
    def execute(
        self, 
        intent: ChartIntent, 
        dataset_path: Path, 
        output_path: Path
    ) -> Path:
        """Execute a chart intent and save the result."""
        self.logger.info(f"[CHART_EXEC] Starting: {intent}")
        
        try:
            # Open dataset lazily
            self.logger.info("[CHART_EXEC] Opening dataset...")
            ds = xr.open_dataset(dataset_path)
            
            # Handle group_by (e.g., by site_code)
            if intent.group_by:
                all_series = self._aggregate_with_groups(ds, intent)
            else:
                # No grouping - aggregate each y column normally
                all_series = {}
                for y_spec in intent.y_columns:
                    self.logger.info(f"[CHART_EXEC] Aggregating: {y_spec}")
                    df = self._aggregate_series(ds, y_spec, intent)
                    all_series[y_spec] = df
            
            ds.close()
            
            # Generate chart
            self.logger.info(f"[CHART_EXEC] Generating chart with {len(all_series)} series...")
            self._plot_multi(all_series, intent, output_path)
            
            self.logger.info(f"[CHART_EXEC] Complete! Saved to {output_path}")
            return output_path
            
        except ChartExecutionError:
            raise
        except Exception as e:
            self.logger.error(f"[CHART_EXEC] Error: {e}")
            raise ChartExecutionError(f"Chart execution failed: {e}")
    
    def _aggregate_with_groups(
        self, 
        ds: xr.Dataset, 
        intent: ChartIntent
    ) -> dict[str, pd.DataFrame]:
        """
        Aggregate data with grouping (e.g., separate series per site).
        
        Returns dict mapping group_value -> DataFrame
        """
        group_col = intent.group_by
        self.logger.info(f"[CHART_EXEC] Grouping by: {group_col}")
        
        # For each y_column, aggregate per group
        # For simplicity, use first y_column for group_by
        y_spec = intent.y_columns[0]
        x_col = intent.x_column
        
        # Get the y data
        if intent.is_expression(y_spec):
            y_data = self._evaluate_expression(ds, y_spec)
        else:
            y_var = self._find_variable(ds, y_spec)
            y_data = ds[y_var]
        
        # Filter out fill values
        y_data = y_data.where(np.abs(y_data) < 1e30)
        
        # Get group column
        if group_col not in ds.data_vars and group_col not in ds.coords:
            raise ChartExecutionError(f"Group column '{group_col}' not found")
        
        group_data = ds[group_col] if group_col in ds.data_vars else ds.coords[group_col]
        
        # Find unique group values
        unique_groups = np.unique(group_data.values.flatten())
        unique_groups = [g for g in unique_groups if pd.notna(g) and g != '']
        
        self.logger.info(f"[CHART_EXEC] Found {len(unique_groups)} groups: {unique_groups[:5]}...")
        
        # Aggregate each group separately
        all_series = {}
        for group_val in unique_groups:
            self.logger.info(f"[CHART_EXEC] Processing group: {group_val}")
            
            # Create mask for this group
            mask = group_data == group_val
            filtered_data = y_data.where(mask)
            
            # Aggregate by x_column
            df = self._aggregate_by_x(filtered_data, x_col, str(group_val))
            
            if len(df) > 0:
                all_series[str(group_val)] = df
        
        return all_series
    
    def _aggregate_series(
        self, 
        ds: xr.Dataset, 
        y_spec: str, 
        intent: ChartIntent
    ) -> pd.DataFrame:
        """Aggregate a single Y column or expression."""
        # Check if this is an expression
        if intent.is_expression(y_spec):
            data = self._evaluate_expression(ds, y_spec)
            y_label = y_spec
        else:
            y_var = self._find_variable(ds, y_spec)
            data = ds[y_var]
            y_label = y_var
        
        # Filter out fill values
        data = data.where(np.abs(data) < 1e30)
        
        # Aggregate by x_column
        df = self._aggregate_by_x(data, intent.x_column, y_label)
        
        return df
    
    def _find_variable(self, ds: xr.Dataset, name: str) -> str:
        """Find variable in dataset, case-insensitive."""
        if name in ds.data_vars:
            return name
        for var in ds.data_vars:
            if var.lower() == name.lower():
                return var
        raise ChartExecutionError(f"Variable '{name}' not found in dataset")
    
    def _evaluate_expression(self, ds: xr.Dataset, expr: str) -> xr.DataArray:
        """Safely evaluate an expression like 'NO2_TropVCD / HCHO_TropVCD'."""
        self.logger.info(f"[CHART_EXEC] Evaluating expression: {expr}")
        
        col_pattern = r'[A-Za-z_][A-Za-z0-9_]*'
        columns = set(re.findall(col_pattern, expr))
        
        namespace = {}
        for col in columns:
            try:
                var_name = self._find_variable(ds, col)
                namespace[col] = ds[var_name]
            except ChartExecutionError:
                pass
        
        if not namespace:
            raise ChartExecutionError(f"No valid columns found in expression: {expr}")
        
        namespace['abs'] = np.abs
        namespace['log'] = np.log
        namespace['log10'] = np.log10
        namespace['sqrt'] = np.sqrt
        
        try:
            result = eval(expr, {"__builtins__": {}}, namespace)
            return result
        except Exception as e:
            raise ChartExecutionError(f"Failed to evaluate expression '{expr}': {e}")
    
    def _aggregate_by_x(
        self, 
        data: xr.DataArray, 
        x_col: str, 
        y_label: str
    ) -> pd.DataFrame:
        """Aggregate data by x column (hour, date, etc.)."""
        
        if x_col == "hour":
            result = data.groupby("TIME.hour").mean(skipna=True)
            for dim in list(result.dims):
                if dim != 'hour':
                    result = result.mean(dim=dim, skipna=True)
            df = pd.DataFrame({"hour": result.hour.values, y_label: result.values})
            
        elif x_col == "date":
            result = data.groupby("TIME.date").mean(skipna=True)
            for dim in list(result.dims):
                if dim != 'date':
                    result = result.mean(dim=dim, skipna=True)
            df = pd.DataFrame({"date": result.date.values, y_label: result.values})
            
        elif x_col == "month":
            result = data.groupby("TIME.month").mean(skipna=True)
            for dim in list(result.dims):
                if dim != 'month':
                    result = result.mean(dim=dim, skipna=True)
            df = pd.DataFrame({"month": result.month.values, y_label: result.values})
            
        elif x_col == "day_of_week":
            result = data.groupby("TIME.dayofweek").mean(skipna=True)
            for dim in list(result.dims):
                if dim != 'dayofweek':
                    result = result.mean(dim=dim, skipna=True)
            df = pd.DataFrame({"day_of_week": result.dayofweek.values, y_label: result.values})
            
        elif x_col == "year":
            result = data.groupby("TIME.year").mean(skipna=True)
            for dim in list(result.dims):
                if dim != 'year':
                    result = result.mean(dim=dim, skipna=True)
            df = pd.DataFrame({"year": result.year.values, y_label: result.values})
            
        else:
            raise ChartExecutionError(f"Unknown x_column: {x_col}")
        
        df = df.dropna()
        df = df.sort_values(df.columns[0]).reset_index(drop=True)
        
        self.logger.info(f"[CHART_EXEC] Series '{y_label}': {len(df)} points")
        return df
    
    def _plot_multi(
        self, 
        all_series: dict[str, pd.DataFrame], 
        intent: ChartIntent, 
        output_path: Path
    ):
        """Generate chart with multiple series."""
        plt.figure(figsize=(10, 6))
        
        x_col = intent.x_column
        
        for i, (series_label, df) in enumerate(all_series.items()):
            x = df.iloc[:, 0]  # First column is x
            y = df.iloc[:, 1]  # Second column is y
            color = COLORS[i % len(COLORS)]
            
            if intent.chart_type == ChartType.LINE:
                plt.plot(x, y, linewidth=2, marker='o', markersize=4, 
                        color=color, label=self._format_label(series_label))
            elif intent.chart_type == ChartType.BAR:
                width = 0.8 / len(all_series)
                offset = (i - len(all_series)/2 + 0.5) * width
                plt.bar([xi + offset for xi in range(len(x))], y, 
                       width=width, color=color, label=self._format_label(series_label))
                plt.xticks(range(len(x)), x)
            elif intent.chart_type == ChartType.SCATTER:
                plt.scatter(x, y, alpha=0.6, color=color, s=50, 
                           label=self._format_label(series_label))
            elif intent.chart_type == ChartType.HISTOGRAM:
                plt.hist(y.dropna(), bins=30, color=color, alpha=0.7,
                        label=self._format_label(series_label), edgecolor='white')
        
        # Labels
        plt.xlabel(self._format_label(x_col), fontsize=11)
        
        if len(all_series) == 1 and not intent.group_by:
            y_label = list(all_series.keys())[0]
            plt.ylabel(self._format_label(y_label), fontsize=11)
        else:
            # For grouped data, use y_column name
            plt.ylabel(self._format_label(intent.y_columns[0]), fontsize=11)
        
        # Title
        if intent.title:
            title = intent.title
        elif intent.group_by:
            title = f"{self._format_label(intent.y_columns[0])} by {self._format_label(x_col)} (per {intent.group_by})"
        else:
            y_names = ", ".join(self._format_label(y) for y in all_series.keys())
            title = f"{y_names} by {self._format_label(x_col)}"
        plt.title(title, fontsize=13, fontweight='bold', pad=10)
        
        # Legend for multiple series
        if len(all_series) > 1:
            plt.legend(loc='best', framealpha=0.9)
        
        # Styling
        plt.grid(True, alpha=0.3, linestyle='--')
        
        if len(list(all_series.values())[0]) > 10 or x_col in ['date', 'hour']:
            plt.xticks(rotation=45, ha='right')
        
        plt.tight_layout()
        
        # Save
        output_path.parent.mkdir(parents=True, exist_ok=True)
        plt.savefig(output_path, dpi=150, bbox_inches='tight', facecolor='white')
        plt.close()
    
    def _format_label(self, name: str) -> str:
        """Format column/expression name for display."""
        if '/' in name:
            parts = name.split('/')
            return f"{self._format_single(parts[0].strip())} / {self._format_single(parts[1].strip())}"
        return self._format_single(name)
    
    def _format_single(self, name: str) -> str:
        """Format a single column name."""
        label = name.replace('_', ' ')
        special = {
            'no2 tropvcd': 'NO₂',
            'hcho tropvcd': 'HCHO',
            'fnr': 'FNR',
            'site code': 'Site',
        }
        return special.get(label.lower(), label.title())
