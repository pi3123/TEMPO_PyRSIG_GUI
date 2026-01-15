"""UI widgets for the Node-Based Pipeline Editor."""

import flet as ft
from typing import Callable, Optional, Dict, Any, List

from ..theme import Colors, Spacing


class NodeCard(ft.Container):
    """Visual card representing a single node in the pipeline stack."""
    
    # Icon mapping for node types
    NODE_ICONS = {
        "select_variable": ft.Icons.DATA_OBJECT,
        "nearest_pixel": ft.Icons.LOCATION_ON,
        "n_pixel_avg": ft.Icons.GRID_ON,
        "radius_avg": ft.Icons.RADIO_BUTTON_CHECKED,
        "hourly": ft.Icons.ACCESS_TIME,
        "daily_mean": ft.Icons.TODAY,
        "diurnal_cycle": ft.Icons.LOOP,
        "gap_fill": ft.Icons.AUTO_FIX_HIGH,
        "filter": ft.Icons.FILTER_ALT,
        "statistics": ft.Icons.ANALYTICS,
    }
    
    # Color mapping for categories
    CATEGORY_COLORS = {
        "source": Colors.INFO,
        "spatial": Colors.SUCCESS,
        "temporal": Colors.WARNING,
        "transform": Colors.PRIMARY,
    }
    
    def __init__(
        self,
        node_type: str,
        display_name: str,
        category: str = "base",
        params: Dict[str, Any] = None,
        on_edit: Callable = None,
        on_delete: Callable = None,
        index: int = 0,
    ):
        self.node_type = node_type
        self.display_name = display_name
        self.category = category
        self.params = params or {}
        self.on_edit = on_edit
        self.on_delete = on_delete
        self.index = index
        
        super().__init__(
            content=self._build_content(),
            bgcolor=Colors.SURFACE,
            border_radius=8,
            border=ft.border.all(1, self.CATEGORY_COLORS.get(category, Colors.BORDER)),
            padding=12,
        )
    
    def _build_content(self):
        """Build the card content."""
        icon = self.NODE_ICONS.get(self.node_type, ft.Icons.SETTINGS)
        color = self.CATEGORY_COLORS.get(self.category, Colors.PRIMARY)
        
        # Parameter summary
        param_text = ""
        if self.params:
            param_parts = [f"{k}={v}" for k, v in self.params.items() if not k.startswith('_')]
            param_text = ", ".join(param_parts[:3])  # Limit to 3 params
        
        return ft.Row([
            ft.Container(
                content=ft.Icon(icon, size=20, color=color),
                bgcolor=f"{color}20",  # 20% opacity
                padding=8,
                border_radius=6,
            ),
            ft.Column([
                ft.Text(self.display_name, size=14, weight=ft.FontWeight.W_600, color=Colors.ON_SURFACE),
                ft.Text(param_text or "Default settings", size=11, color=Colors.ON_SURFACE_VARIANT),
            ], spacing=2, expand=True),
            ft.Row([
                ft.IconButton(
                    icon=ft.Icons.EDIT,
                    icon_size=16,
                    icon_color=Colors.ON_SURFACE_VARIANT,
                    tooltip="Edit",
                    on_click=lambda e: self.on_edit(self.index) if self.on_edit else None,
                ),
                ft.IconButton(
                    icon=ft.Icons.DELETE_OUTLINE,
                    icon_size=16,
                    icon_color=Colors.ERROR,
                    tooltip="Remove",
                    on_click=lambda e: self.on_delete(self.index) if self.on_delete else None,
                ),
            ], spacing=0),
        ], spacing=12)


class AddNodeButton(ft.Container):
    """Button to add a new node to the stack."""
    
    def __init__(self, on_add: Callable = None):
        self.on_add = on_add
        
        super().__init__(
            content=ft.Row([
                ft.Icon(ft.Icons.ADD, size=16, color=Colors.PRIMARY),
                ft.Text("Add Step", size=12, color=Colors.PRIMARY),
            ], spacing=6, alignment=ft.MainAxisAlignment.CENTER),
            bgcolor=f"{Colors.PRIMARY}10",
            border_radius=8,
            border=ft.border.all(1, f"{Colors.PRIMARY}50"),
            padding=ft.padding.symmetric(horizontal=16, vertical=8),
            on_click=self._show_menu,
        )
    
    def _show_menu(self, e):
        """Show node type selection menu."""
        if not self.on_add:
            return
        
        # Create menu items for each node category
        menu_items = [
            ("Source", [
                ("select_variable", "Select Variable"),
            ]),
            ("Spatial", [
                ("nearest_pixel", "Nearest Pixel"),
                ("n_pixel_avg", "N-Pixel Average"),
                ("radius_avg", "Radius Average"),
            ]),
            ("Temporal", [
                ("hourly", "Hourly (Raw)"),
                ("daily_mean", "Daily Mean"),
                ("diurnal_cycle", "Diurnal Cycle"),
            ]),
            ("Transform", [
                ("gap_fill", "Gap Fill"),
                ("filter", "Filter"),
            ]),
        ]
        
        # Build menu
        menu_controls = []
        for category, items in menu_items:
            menu_controls.append(
                ft.Text(category, size=11, weight=ft.FontWeight.BOLD, color=Colors.ON_SURFACE_VARIANT)
            )
            for node_type, name in items:
                menu_controls.append(
                    ft.TextButton(
                        content=ft.Text(name),
                        on_click=lambda e, nt=node_type: self._add_node(nt),
                    )
                )
            menu_controls.append(ft.Divider(height=8))
        
        # Show as bottom sheet
        bs = ft.BottomSheet(
            content=ft.Container(
                content=ft.Column(menu_controls, spacing=4, scroll=ft.ScrollMode.AUTO),
                padding=16,
            ),
            open=True,
        )
        self.page.overlay.append(bs)
        self.page.update()
    
    def _add_node(self, node_type: str):
        """Add the selected node type."""
        # Close bottom sheet
        if self.page.overlay:
            self.page.overlay.pop()
            self.page.update()
        
        if self.on_add:
            self.on_add(node_type)


class NodeParamEditor(ft.AlertDialog):
    """Dialog for editing node parameters."""
    
    def __init__(self, node_type: str, params: Dict[str, Any], on_save: Callable):
        self.node_type = node_type
        self.params = params.copy()
        self.on_save = on_save
        self._fields = {}
        
        super().__init__(
            title=ft.Text(f"Edit: {node_type}"),
            content=self._build_form(),
            actions=[
                ft.TextButton("Cancel", on_click=lambda e: self._close()),
                ft.FilledButton("Save", on_click=lambda e: self._save()),
            ],
        )
    
    def _build_form(self):
        """Build parameter edit form based on node type."""
        controls = []
        
        # Common parameters by node type
        param_defs = {
            "select_variable": [
                ("variable", "Variable", ["NO2_TropVCD", "HCHO_TotVCD", "FNR"]),
            ],
            "n_pixel_avg": [
                ("n_pixels", "Number of Pixels", [1, 4, 9, 16]),
            ],
            "radius_avg": [
                ("radius_km", "Radius (km)", None),
                ("min_coverage", "Min Coverage (%)", None),
            ],
            "daily_mean": [
                ("min_hours", "Min Hours Required", None),
            ],
            "filter": [
                ("column", "Column", ["Value"]),
                ("operator", "Operator", [">", ">=", "<", "<=", "==", "!="]),
                ("threshold", "Threshold", None),
            ],
        }
        
        if self.node_type in param_defs:
            for param_name, label, options in param_defs[self.node_type]:
                current_value = self.params.get(param_name, "")
                
                if options:
                    # Dropdown
                    field = ft.Dropdown(
                        label=label,
                        options=[ft.DropdownOption(str(o)) for o in options],
                        value=str(current_value) if current_value else str(options[0]),
                        text_style=ft.TextStyle(color=Colors.ON_SURFACE),
                    )
                else:
                    # Text field
                    field = ft.TextField(
                        label=label,
                        value=str(current_value),
                        text_style=ft.TextStyle(color=Colors.ON_SURFACE),
                    )
                
                self._fields[param_name] = field
                controls.append(field)
        else:
            controls.append(ft.Text("No parameters to edit", color=Colors.ON_SURFACE_VARIANT))
        
        return ft.Container(
            content=ft.Column(controls, spacing=16),
            width=300,
            padding=16,
        )
    
    def _save(self):
        """Save parameters and close."""
        for name, field in self._fields.items():
            value = field.value
            # Try to convert to appropriate type
            try:
                if '.' in str(value):
                    value = float(value)
                else:
                    value = int(value)
            except ValueError:
                pass
            self.params[name] = value
        
        self.on_save(self.params)
        self._close()
    
    def _close(self):
        """Close the dialog."""
        self.open = False
        if self.page and self in self.page.overlay:
            self.page.overlay.remove(self)
        if self.page:
            self.page.update()
