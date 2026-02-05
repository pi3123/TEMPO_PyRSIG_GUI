"""Stack Editor - Vertical list of nodes for a single column."""

import flet as ft
from typing import Callable, List, Dict, Any

from ..theme import Colors
from .node_widgets import NodeCard, AddNodeButton, NodeParamEditor
from ...core.nodes import NodeConfig, get_node_class


# Node metadata lookup
NODE_INFO = {
    "select_variable": ("Select Variable", "source"),
    "nearest_pixel": ("Nearest Pixel", "spatial"),
    "n_pixel_avg": ("N-Pixel Average", "spatial"),
    "radius_avg": ("Radius Average", "spatial"),
    "hourly": ("Hourly (Raw)", "temporal"),
    "daily_mean": ("Daily Mean", "temporal"),
    "diurnal_cycle": ("Diurnal Cycle", "temporal"),
    "gap_fill": ("Gap Fill", "transform"),
    "filter": ("Filter", "transform"),
    "statistics": ("Add Statistics", "transform"),
}


class StackEditor(ft.Container):
    """Vertical stack editor for a column's pipeline."""
    
    def __init__(
        self,
        node_configs: List[NodeConfig] = None,
        on_change: Callable = None,
    ):
        self._configs = node_configs or []
        self.on_change = on_change
        self._node_cards = []
        
        super().__init__(
            content=self._build_content(),
            bgcolor=Colors.BACKGROUND,
            border_radius=8,
            padding=8,
        )
    
    def _build_content(self):
        """Build the stack UI."""
        self._node_cards = []
        controls = []
        
        for i, config in enumerate(self._configs):
            info = NODE_INFO.get(config.node_type, (config.node_type, "base"))
            card = NodeCard(
                node_type=config.node_type,
                display_name=info[0],
                category=info[1],
                params=config.params,
                on_edit=self._edit_node,
                on_delete=self._delete_node,
                index=i,
            )
            self._node_cards.append(card)
            controls.append(card)
            
            # Add connector line between nodes
            if i < len(self._configs) - 1:
                controls.append(
                    ft.Container(
                        content=ft.Icon(ft.Icons.ARROW_DOWNWARD, size=16, color=Colors.BORDER),
                        alignment=ft.Alignment(0, 0),
                        height=24,
                    )
                )
        
        # Add button at the end
        controls.append(ft.Container(height=8))
        controls.append(AddNodeButton(on_add=self._add_node))
        
        return ft.Column(controls, spacing=4, horizontal_alignment=ft.CrossAxisAlignment.CENTER)
    
    def _add_node(self, node_type: str):
        """Add a new node to the stack."""
        # Default params
        default_params = {}
        if node_type == "select_variable":
            default_params = {"variable": "NO2_TropVCD"}
        elif node_type == "n_pixel_avg":
            default_params = {"n_pixels": 4}
        elif node_type == "radius_avg":
            default_params = {"radius_km": 10.0, "min_coverage": 0.5}
        elif node_type == "daily_mean":
            default_params = {"min_hours": 1}
        
        self._configs.append(NodeConfig(node_type, default_params))
        self._refresh()
        
        if self.on_change:
            self.on_change(self._configs)
    
    def _edit_node(self, index: int):
        """Open editor for a node."""
        if not self.page:
            return
            
        if 0 <= index < len(self._configs):
            config = self._configs[index]
            
            def save_params(params):
                self._configs[index] = NodeConfig(config.node_type, params)
                self._refresh()
                if self.on_change:
                    self.on_change(self._configs)
            
            dlg = NodeParamEditor(
                node_type=config.node_type,
                params=config.params,
                on_save=save_params,
            )
            self.page.overlay.append(dlg)
            dlg.open = True
            self.page.update()
    
    def _delete_node(self, index: int):
        """Remove a node from the stack."""
        if 0 <= index < len(self._configs):
            self._configs.pop(index)
            self._refresh()
            
            if self.on_change:
                self.on_change(self._configs)
    
    def _refresh(self):
        """Rebuild the UI."""
        self.content = self._build_content()
        self.update()
    
    @property
    def configs(self) -> List[NodeConfig]:
        """Get current node configurations."""
        return self._configs.copy()
    
    @configs.setter
    def configs(self, value: List[NodeConfig]):
        """Set node configurations."""
        self._configs = list(value)
        self._refresh()
