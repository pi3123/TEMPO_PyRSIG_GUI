"""Column-Centric Export Job Orchestrator.

This module provides the ExportJob class that runs all column pipelines
and merges the results into a single output.
"""

import pandas as pd
import numpy as np
import xarray as xr
from pathlib import Path
from typing import List, Dict, Any, Optional
from dataclasses import dataclass, field
import logging

from .nodes import (
    ColumnDefinition,
    ExportContext,
    ColumnRunner,
    NodeConfig,
)

logger = logging.getLogger(__name__)


@dataclass
class ExportJobConfig:
    """Configuration for an export job."""
    
    dataset_id: str
    dataset_path: Path
    sites: Dict[str, tuple]  # {code: (lat, lon)}
    columns: List[ColumnDefinition]
    
    # Optional settings
    output_path: Optional[Path] = None
    output_name: str = "export"
    utc_offset: float = -6.0
    
    def to_dict(self) -> dict:
        return {
            "dataset_id": self.dataset_id,
            "dataset_path": str(self.dataset_path),
            "sites": self.sites,
            "columns": [c.to_dict() for c in self.columns],
            "output_name": self.output_name,
            "utc_offset": self.utc_offset,
        }


class ExportJob:
    """Orchestrator that runs all column pipelines and produces final output."""
    
    def __init__(self, config: ExportJobConfig):
        self.config = config
        self._context: Optional[ExportContext] = None
    
    def execute(self) -> pd.DataFrame:
        """Execute all column pipelines and merge results."""
        logger.info(f"Starting export job with {len(self.config.columns)} columns")
        
        # Load dataset
        ds = xr.open_dataset(self.config.dataset_path)
        
        # Create context
        self._context = ExportContext(
            dataset=ds,
            sites=self.config.sites,
            utc_offset=self.config.utc_offset,
        )
        
        # Run each column
        column_results: Dict[str, pd.Series] = {}
        index_df = None
        
        for col_def in self.config.columns:
            logger.info(f"Processing column: {col_def.name}")
            
            try:
                runner = ColumnRunner(col_def)
                result_df = runner.pipeline.run(pd.DataFrame(), self._context)
                
                # Store the result
                if not result_df.empty:
                    # Determine index columns based on what's present
                    index_cols = []
                    for col in ['Site', 'Date', 'Month', 'Hour', 'UTC_Time', 'Local_Time']:
                        if col in result_df.columns:
                            index_cols.append(col)
                    
                    # Extract Value column
                    if 'Value' in result_df.columns:
                        column_results[col_def.name] = result_df.set_index(index_cols)['Value']
                        
                        # Store index for alignment
                        if index_df is None:
                            index_df = result_df[index_cols].copy()
                    
            except Exception as e:
                logger.error(f"Column '{col_def.name}' failed: {e}")
                import traceback
                traceback.print_exc()
        
        ds.close()
        
        # Merge all columns
        if column_results:
            # Create DataFrame from all series
            merged = pd.DataFrame(column_results)
            merged = merged.reset_index()
            return merged
        else:
            logger.warning("No columns produced results")
            return pd.DataFrame()
    
    def export_to_excel(self, output_path: Path = None) -> Path:
        """Execute and save to Excel."""
        result_df = self.execute()
        
        if output_path is None:
            output_path = self.config.output_path or Path.cwd()
        
        output_path.mkdir(parents=True, exist_ok=True)
        file_path = output_path / f"{self.config.output_name}.xlsx"
        
        with pd.ExcelWriter(file_path, engine='xlsxwriter') as writer:
            result_df.to_excel(writer, sheet_name='Data', index=False)
            
            # Metadata sheet
            meta = pd.DataFrame({
                'Parameter': ['Dataset', 'Columns', 'Sites'],
                'Value': [
                    self.config.dataset_id,
                    ', '.join(c.name for c in self.config.columns),
                    ', '.join(self.config.sites.keys()),
                ]
            })
            meta.to_excel(writer, sheet_name='Info', index=False)
        
        logger.info(f"Exported to: {file_path}")
        return file_path


def create_default_column(name: str, variable: str = "NO2_TropVCD") -> ColumnDefinition:
    """Create a column with default pipeline: Source -> Nearest -> Hourly."""
    return ColumnDefinition(
        name=name,
        node_configs=[
            NodeConfig("select_variable", {"variable": variable}),
            NodeConfig("nearest_pixel", {}),
            NodeConfig("hourly", {}),
        ]
    )


def create_daily_mean_column(name: str, variable: str = "NO2_TropVCD", 
                             radius_km: float = 10.0) -> ColumnDefinition:
    """Create a column with daily mean aggregation."""
    return ColumnDefinition(
        name=name,
        node_configs=[
            NodeConfig("select_variable", {"variable": variable}),
            NodeConfig("radius_avg", {"radius_km": radius_km}),
            NodeConfig("daily_mean", {"min_hours": 1}),
        ]
    )


def create_filled_column(name: str, variable: str = "NO2_TropVCD") -> ColumnDefinition:
    """Create a column with gap filling."""
    return ColumnDefinition(
        name=name,
        node_configs=[
            NodeConfig("select_variable", {"variable": variable}),
            NodeConfig("nearest_pixel", {}),
            NodeConfig("hourly", {}),
            NodeConfig("gap_fill", {}),
        ]
    )
