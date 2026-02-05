"""Column Lane - Container for a single column definition."""

import flet as ft
from typing import Callable, List

from ..theme import Colors
from .stack_editor import StackEditor
from ...core.nodes import NodeConfig, ColumnDefinition


class ColumnLane(ft.Container):
    """A vertical lane representing one output column."""
    
    def __init__(
        self,
        column_def: ColumnDefinition = None,
        on_change: Callable = None,
        on_delete: Callable = None,
        index: int = 0,
    ):
        self.column_def = column_def or ColumnDefinition(name="New Column")
        self.on_change = on_change
        self.on_delete = on_delete
        self.index = index
        
        self._name_field = ft.TextField(
            value=self.column_def.name,
            text_size=14,
            text_style=ft.TextStyle(color=Colors.ON_SURFACE, weight=ft.FontWeight.BOLD),
            border_color="transparent",
            focused_border_color=Colors.PRIMARY,
            content_padding=ft.padding.symmetric(horizontal=8, vertical=4),
            on_blur=self._on_name_change,
            expand=True,
        )
        
        self._stack_editor = StackEditor(
            node_configs=self.column_def.node_configs,
            on_change=self._on_stack_change,
        )
        
        super().__init__(
            content=self._build_content(),
            width=280,
            bgcolor=Colors.SURFACE,
            border_radius=12,
            border=ft.border.all(1, Colors.BORDER),
            padding=0,
        )
    
    def _build_content(self):
        """Build the lane content."""
        header = ft.Container(
            content=ft.Row([
                self._name_field,
                ft.IconButton(
                    icon=ft.Icons.FULLSCREEN,
                    icon_size=16,
                    icon_color=Colors.ON_SURFACE_VARIANT,
                    tooltip="Expand Column",
                    on_click=self._open_fullscreen,
                ),
                ft.IconButton(
                    icon=ft.Icons.CONTENT_COPY,
                    icon_size=16,
                    icon_color=Colors.ON_SURFACE_VARIANT,
                    tooltip="Duplicate Column",
                    on_click=self._duplicate,
                ),
                ft.IconButton(
                    icon=ft.Icons.DELETE_OUTLINE,
                    icon_size=16,
                    icon_color=Colors.ERROR,
                    tooltip="Delete Column",
                    on_click=lambda e: self.on_delete(self.index) if self.on_delete else None,
                ),
            ], spacing=0),
            bgcolor=Colors.SURFACE_VARIANT,
            padding=ft.padding.symmetric(horizontal=8, vertical=4),
            border_radius=ft.border_radius.only(top_left=12, top_right=12),
        )
        
        return ft.Column([
            header,
            ft.Container(
                content=self._stack_editor,
                padding=8,
                expand=True,
            ),
        ], spacing=0)
    
    def _on_name_change(self, e):
        """Handle column name change."""
        self.column_def.name = self._name_field.value
        if self.on_change:
            self.on_change(self.index, self.column_def)
    
    def _on_stack_change(self, configs: List[NodeConfig]):
        """Handle stack change."""
        self.column_def.node_configs = configs
        if self.on_change:
            self.on_change(self.index, self.column_def)
    
    def _duplicate(self, e):
        """Request duplication of this column."""
        # Emit a change event with a special flag
        if self.on_change:
            new_def = ColumnDefinition(
                name=f"{self.column_def.name}_copy",
                node_configs=[NodeConfig(c.node_type, c.params.copy()) for c in self.column_def.node_configs],
            )
            self.on_change(self.index, new_def, duplicate=True)
    
    def _open_fullscreen(self, e):
        """Open this column's stack in a fullscreen dialog."""
        # Get page from the event or self
        page = e.page if hasattr(e, 'page') and e.page else self.page
        if not page:
            print("ERROR: No page reference available for fullscreen dialog")
            return
        
        # Create a larger stack editor for fullscreen
        self._fullscreen_stack = StackEditor(
            node_configs=[NodeConfig(c.node_type, c.params.copy()) for c in self.column_def.node_configs],
            on_change=self._on_fullscreen_change,
        )
        
        self._fullscreen_dlg = ft.AlertDialog(
            modal=True,
            title=ft.Text(f"Edit: {self.column_def.name}", size=18, weight=ft.FontWeight.BOLD, color=Colors.ON_SURFACE),
            content=ft.Container(
                content=ft.Column([
                    self._fullscreen_stack,
                ], scroll=ft.ScrollMode.AUTO),
                width=400,
                height=500,
                padding=8,
            ),
            actions=[
                ft.FilledButton("Done", on_click=self._close_fullscreen),
            ],
        )
        
        page.dialog = self._fullscreen_dlg
        self._fullscreen_dlg.open = True
        page.update()
    
    def _close_fullscreen(self, e):
        """Close fullscreen dialog and sync changes."""
        # Sync changes back to main stack
        if hasattr(self, '_fullscreen_stack'):
            self._stack_editor.configs = self._fullscreen_stack.configs
            self.column_def.node_configs = self._fullscreen_stack.configs
            if self.on_change:
                self.on_change(self.index, self.column_def)
        
        # Close dialog
        if hasattr(self, '_fullscreen_dlg'):
            self._fullscreen_dlg.open = False
        self.page.update()
    
    def _on_fullscreen_change(self, configs: List[NodeConfig]):
        """Handle changes from fullscreen editor (just update the temp config)."""
        # Fullscreen stack manages its own state, we sync on close
        pass
    
    def get_definition(self) -> ColumnDefinition:
        """Get the current column definition."""
        self.column_def.name = self._name_field.value
        self.column_def.node_configs = self._stack_editor.configs
        return self.column_def

