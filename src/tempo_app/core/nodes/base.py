"""Base classes for the Node-Based Pipeline Engine."""

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional
import pandas as pd
import numpy as np


@dataclass
class NodeConfig:
    """Configuration for a node instance."""
    node_type: str
    params: Dict[str, Any] = field(default_factory=dict)
    
    def to_dict(self) -> dict:
        return {"type": self.node_type, "params": self.params}
    
    @classmethod
    def from_dict(cls, data: dict) -> "NodeConfig":
        return cls(node_type=data["type"], params=data.get("params", {}))


class Node(ABC):
    """Abstract base class for all pipeline nodes.
    
    A Node is an atomic unit of data transformation.
    It receives a DataFrame (or Series), applies its logic, and returns transformed data.
    """
    
    # Class-level metadata
    node_type: str = "base"
    display_name: str = "Base Node"
    category: str = "base"
    
    def __init__(self, **params):
        """Initialize node with parameters."""
        self.params = params
    
    @abstractmethod
    def execute(self, data: pd.DataFrame, context: dict) -> pd.DataFrame:
        """Execute the node's transformation.
        
        Args:
            data: Input DataFrame from previous node (or source).
            context: Shared context with dataset, sites, etc.
            
        Returns:
            Transformed DataFrame.
        """
        pass
    
    def validate(self) -> List[str]:
        """Validate node configuration. Returns list of error messages."""
        return []
    
    def get_config(self) -> NodeConfig:
        """Get node configuration for serialization."""
        return NodeConfig(node_type=self.node_type, params=self.params)
    
    @classmethod
    def from_config(cls, config: NodeConfig) -> "Node":
        """Create node instance from config."""
        return cls(**config.params)
    
    def __repr__(self):
        return f"{self.__class__.__name__}({self.params})"


# Registry for node types
_NODE_REGISTRY: Dict[str, type] = {}


def register_node(node_class: type) -> type:
    """Decorator to register a node class."""
    _NODE_REGISTRY[node_class.node_type] = node_class
    return node_class


def get_node_class(node_type: str) -> Optional[type]:
    """Get node class by type name."""
    return _NODE_REGISTRY.get(node_type)


def create_node(config: NodeConfig) -> Node:
    """Factory function to create a node from config."""
    node_class = get_node_class(config.node_type)
    if not node_class:
        raise ValueError(f"Unknown node type: {config.node_type}")
    return node_class.from_config(config)


def list_node_types() -> List[str]:
    """List all registered node types."""
    return list(_NODE_REGISTRY.keys())
