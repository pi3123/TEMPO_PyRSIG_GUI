"""Node-Based Pipeline Engine for TEMPO Analyzer.

This package provides a composable, column-centric data processing system.
"""

from .base import (
    Node,
    NodeConfig,
    register_node,
    get_node_class,
    create_node,
    list_node_types,
)

from .pipeline import (
    ColumnDefinition,
    ExportContext,
    Pipeline,
    ColumnRunner,
)

# Import all node types to register them
from . import source_nodes
from . import spatial_nodes
from . import temporal_nodes
from . import transform_nodes

__all__ = [
    'Node',
    'NodeConfig',
    'register_node',
    'get_node_class',
    'create_node',
    'list_node_types',
    'ColumnDefinition',
    'ExportContext',
    'Pipeline',
    'ColumnRunner',
]
