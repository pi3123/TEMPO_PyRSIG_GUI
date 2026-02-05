"""Pipeline and Column Definition classes."""

from dataclasses import dataclass, field
from typing import List, Dict, Any, Optional
import pandas as pd
import numpy as np
import logging

from .base import Node, NodeConfig, create_node

logger = logging.getLogger(__name__)


@dataclass
class ColumnDefinition:
    """Definition of a single output column with its processing stack."""
    
    name: str
    node_configs: List[NodeConfig] = field(default_factory=list)
    
    def to_dict(self) -> dict:
        return {
            "name": self.name,
            "nodes": [n.to_dict() for n in self.node_configs]
        }
    
    @classmethod
    def from_dict(cls, data: dict) -> "ColumnDefinition":
        return cls(
            name=data["name"],
            node_configs=[NodeConfig.from_dict(n) for n in data.get("nodes", [])]
        )


@dataclass 
class ExportContext:
    """Shared context for pipeline execution."""
    
    dataset: Any  # xr.Dataset
    sites: Dict[str, tuple]  # {code: (lat, lon)}
    date_range: Optional[tuple] = None  # (start, end)
    utc_offset: float = -6.0
    
    # Cached computed values
    _site_pixel_cache: Dict[str, Any] = field(default_factory=dict, repr=False)


class Pipeline:
    """Runs a sequence of nodes on input data."""
    
    def __init__(self, nodes: List[Node] = None):
        self.nodes = nodes or []
    
    def add_node(self, node: Node):
        """Add a node to the pipeline."""
        self.nodes.append(node)
        return self
    
    def run(self, input_data: pd.DataFrame, context: ExportContext) -> pd.DataFrame:
        """Execute all nodes in sequence."""
        data = input_data
        
        for i, node in enumerate(self.nodes):
            try:
                data = node.execute(data, context)
                logger.debug(f"Node {i} ({node.__class__.__name__}) output shape: {data.shape if hasattr(data, 'shape') else 'N/A'}")
            except Exception as e:
                logger.error(f"Node {i} ({node.__class__.__name__}) failed: {e}")
                raise
        
        return data
    
    @classmethod
    def from_configs(cls, configs: List[NodeConfig]) -> "Pipeline":
        """Create pipeline from list of node configs."""
        nodes = [create_node(c) for c in configs]
        return cls(nodes=nodes)


class ColumnRunner:
    """Runs a single column's pipeline and produces a Series."""
    
    def __init__(self, column_def: ColumnDefinition):
        self.column_def = column_def
        self.pipeline = Pipeline.from_configs(column_def.node_configs)
    
    def run(self, context: ExportContext) -> pd.Series:
        """Execute the column pipeline."""
        # Start with an empty DataFrame that will be populated by source nodes
        initial_data = pd.DataFrame()
        
        result_df = self.pipeline.run(initial_data, context)
        
        # The pipeline should produce a DataFrame with a single value column
        # We take the first non-index column as the result
        value_cols = [c for c in result_df.columns if c not in ['Site', 'Date', 'Hour', 'Month', 'UTC_Time', 'Local_Time']]
        
        if value_cols:
            return result_df[value_cols[0]].rename(self.column_def.name)
        else:
            logger.warning(f"Column '{self.column_def.name}' produced no value column")
            return pd.Series(name=self.column_def.name)
