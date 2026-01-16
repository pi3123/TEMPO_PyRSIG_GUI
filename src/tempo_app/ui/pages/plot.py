"""Plot Page - Visualize TEMPO data maps.

This page allows users to:
1. Select a dataset to visualize
2. Choose variable (NO2, HCHO, FNR)
3. Select hour and view/animate maps
"""

import flet as ft
from pathlib import Path
from typing import Optional
import asyncio

from ..theme import Colors, Spacing
from ..components.widgets import LabeledField, SectionCard, StatusLogPanel
from ...storage.database import Database
from ...core.plotter import MapPlotter


class PlotPage(ft.Container):
    """Page for visualizing TEMPO data as maps."""
    
    def __init__(self, db: Database, data_dir: Path):
        super().__init__()
        self.db = db
        self.data_dir = data_dir
        self.plotter = MapPlotter(data_dir)
        
        self._current_dataset = None
        self._current_hour = 12
        
        self._build()

    def did_mount(self):
        """Called when control is added to page - load data async."""
        self.page.run_task(self._load_datasets_async)

    async def _load_datasets_async(self):
        """Load datasets without blocking UI."""
        datasets = await asyncio.to_thread(self.db.get_all_datasets)
        self._apply_datasets(datasets)
        self.update()

    def _build(self):
        """Build the page layout."""
        # Dataset selector
        self._dataset_dropdown = ft.Dropdown(
            label="Select Dataset",
            border_color=Colors.BORDER,
            focused_border_color=Colors.PRIMARY,
            bgcolor=Colors.SURFACE_VARIANT,
            width=300,
            text_style=ft.TextStyle(color=Colors.ON_SURFACE),
        )
        self._dataset_dropdown.on_change = self._on_dataset_change
        
        # Variable selector
        self._variable_dropdown = ft.Dropdown(
            label="Variable",
            options=[
                ft.DropdownOption(key="FNR", text="FNR (HCHO/NO₂ Ratio)"),
                ft.DropdownOption(key="NO2", text="NO₂ Tropospheric VCD"),
                ft.DropdownOption(key="HCHO", text="HCHO Total VCD"),
            ],
            value="FNR",
            border_color=Colors.BORDER,
            focused_border_color=Colors.PRIMARY,
            bgcolor=Colors.SURFACE_VARIANT,
            width=200,
            text_style=ft.TextStyle(color=Colors.ON_SURFACE),
        )
        
        # Road detail selector
        self._road_dropdown = ft.Dropdown(
            label="Road Detail",
            options=[
                ft.DropdownOption(key="primary", text="Interstates Only"),
                ft.DropdownOption(key="major", text="Major Roads"),
                ft.DropdownOption(key="all", text="All Roads"),
            ],
            value="primary",
            border_color=Colors.BORDER,
            focused_border_color=Colors.PRIMARY,
            bgcolor=Colors.SURFACE_VARIANT,
            width=150,
            text_style=ft.TextStyle(color=Colors.ON_SURFACE),
        )
        
        # Hour slider
        self._hour_slider = ft.Slider(
            min=0,
            max=23,
            value=12,
            divisions=23,
            label="{value}:00 UTC",
            expand=True,
        )
        self._hour_slider.on_change = self._on_hour_change
        
        self._hour_text = ft.Text(
            "12:00 UTC",
            size=16,
            weight=ft.FontWeight.W_500,
            color=Colors.ON_SURFACE,
        )
        
        # Generate button
        self._generate_btn = ft.FilledButton(
            content=ft.Row([
                ft.Icon(ft.Icons.MAP, size=20),
                ft.Text("Generate Map"),
            ], spacing=8, tight=True),
            on_click=self._on_generate_click,
        )
        
        # Customization controls
        self._font_slider = ft.Slider(min=8, max=24, divisions=16, value=10, label="{value}")
        self._border_slider = ft.Slider(min=0.5, max=5.0, divisions=45, value=1.5, label="{value}")
        self._road_slider = ft.Slider(min=0.5, max=3.0, divisions=25, value=1.0, label="{value}")
        self._cmap_dropdown = ft.Dropdown(
            options=[
                ft.DropdownOption("Default"),
                ft.DropdownOption("viridis"),
                ft.DropdownOption("plasma"),
                ft.DropdownOption("inferno"),
                ft.DropdownOption("magma"),
                ft.DropdownOption("cividis"),
                ft.DropdownOption("coolwarm"),
                ft.DropdownOption("bwr"),
                ft.DropdownOption("seismic"),
                ft.DropdownOption("jet"),
            ],
            value="Default",
            label="Colormap",
            width=200,
            text_size=14,
            text_style=ft.TextStyle(color=Colors.ON_SURFACE),
        )
        
        self._vmin_input = ft.TextField(
            label="Min Value",
            width=90,
            text_size=14,
            text_style=ft.TextStyle(color=Colors.ON_SURFACE),
            keyboard_type=ft.KeyboardType.NUMBER,
            border_color=Colors.BORDER,
        )
        
        self._vmax_input = ft.TextField(
            label="Max Value",
            width=90,
            text_size=14,
            text_style=ft.TextStyle(color=Colors.ON_SURFACE),
            keyboard_type=ft.KeyboardType.NUMBER,
            border_color=Colors.BORDER,
        )

        # Image display
        self._map_image = ft.Image(
            src="",
            width=700,
            height=600,
            fit="contain",
            visible=False,
        )
        
        self._placeholder = ft.Container(
            content=ft.Column([
                ft.Icon(ft.Icons.MAP_OUTLINED, size=64, color=Colors.ON_SURFACE_VARIANT),
                ft.Text(
                    "Select a dataset and click 'Generate Map'",
                    size=16,
                    color=Colors.ON_SURFACE_VARIANT,
                ),
            ], horizontal_alignment=ft.CrossAxisAlignment.CENTER, spacing=16),
            alignment=ft.Alignment(0, 0),
            width=700,
            height=600,
            bgcolor=Colors.SURFACE_VARIANT,
            border_radius=12,
        )
        
        # Status Message Area
        self._status_icon = ft.Icon(ft.Icons.INFO_OUTLINE, color=Colors.PRIMARY)
        self._status_text = ft.Text(
            "Ready to generate map",
            size=16,
            color=Colors.ON_SURFACE,
            weight=ft.FontWeight.W_500,
            expand=True,
        )
        
        self._status_container = ft.Container(
            content=ft.Column([
                ft.Row([
                    self._status_icon,
                    self._status_text,
                ], spacing=12),
                ft.ProgressBar(
                    visible=False,
                    color=Colors.PRIMARY,
                    bgcolor=Colors.BACKGROUND,
                    height=4,
                ),
            ], spacing=0),  # Zero spacing so bar is tight against bottom or separate? Let's use internal padding or spacing.
            padding=16,
            border_radius=8,
            bgcolor=Colors.SURFACE_VARIANT,
            visible=True,
        )
        # Store ref to progress bar for easy access
        self._progress_bar = self._status_container.content.controls[1]
        
        # Layout
        controls_row = ft.Row([
            self._dataset_dropdown,
            self._variable_dropdown,
            self._road_dropdown,
            self._generate_btn,
        ], spacing=16, wrap=True)
        
        
        hour_row = ft.Row([
            ft.Text("Hour:", color=Colors.ON_SURFACE),
            self._hour_slider,
            self._hour_text,
        ], spacing=12, vertical_alignment=ft.CrossAxisAlignment.CENTER)
        
        left_col = ft.Column([
            # Header
            ft.Text("🗺️ Map Visualization", size=28, weight=ft.FontWeight.BOLD, color=Colors.ON_SURFACE),
            ft.Divider(height=20, color=Colors.DIVIDER),
            
            # Controls
            controls_row,
            ft.Container(height=16),
            hour_row,
            ft.Container(height=16),
            
            # Status Message (Top)
            self._status_container,
            ft.Container(height=16),

            # Map display
            ft.Stack([
                self._placeholder,
                ft.GestureDetector(
                    mouse_cursor=ft.MouseCursor.ZOOM_IN,
                    on_tap=self._on_map_click,
                    content=ft.Container(
                        content=self._map_image,
                        bgcolor="transparent",
                    ),
                ),
            ]),
            
        ], scroll=ft.ScrollMode.AUTO, expand=True)
        


        # Right Column - Customization
        right_col = ft.Column([
            ft.Text("Map Style", size=16, weight=ft.FontWeight.BOLD, color=Colors.ON_SURFACE),
            
            SectionCard("Appearance", ft.Column([
                ft.Text("Font Size", color=Colors.ON_SURFACE),
                self._font_slider,
                
                ft.Text("Border Boldness", color=Colors.ON_SURFACE),
                self._border_slider,
                
                ft.Text("Road Width Scale", color=Colors.ON_SURFACE),
                self._road_slider,
                
                self._cmap_dropdown,
                
                ft.Text("Data Scale (Empty = Auto)", color=Colors.ON_SURFACE),
                ft.Row([
                    self._vmin_input,
                    self._vmax_input,
                ], spacing=10),
            ], spacing=20)),
            
        ], width=300, scroll=ft.ScrollMode.AUTO)
        
        # Main layout
        self.content = ft.Row([
            left_col,
            ft.VerticalDivider(width=1),
            right_col
        ], expand=True)
        self.expand = True
        self.padding = Spacing.PAGE_HORIZONTAL

    def _show_message(self, message: str, is_error: bool = False, is_success: bool = False):
        """Show a status message with appropriate styling."""
        self._status_text.value = message
        
        if is_error:
            self._status_container.bgcolor = Colors.ERROR_CONTAINER
            self._status_text.color = Colors.ON_ERROR_CONTAINER
            self._status_icon.name = ft.Icons.ERROR_OUTLINE
            self._status_icon.color = Colors.ERROR
        elif is_success:
            self._status_container.bgcolor = Colors.PRIMARY_CONTAINER
            self._status_text.color = Colors.ON_PRIMARY_CONTAINER
            self._status_icon.name = ft.Icons.CHECK_CIRCLE_OUTLINE
            self._status_icon.color = Colors.PRIMARY
        else:
            self._status_container.bgcolor = Colors.SURFACE_VARIANT
            self._status_text.color = Colors.ON_SURFACE_VARIANT
            self._status_icon.name = ft.Icons.INFO_OUTLINE
            self._status_icon.color = Colors.PRIMARY
            
        self.update()
    
    def _apply_datasets(self, datasets: list):
        """Apply datasets to dropdown (no DB call)."""
        options = []
        for ds in datasets:
            options.append(ft.DropdownOption(key=ds.id, text=ds.name))
        self._dataset_dropdown.options = options
        if options:
            self._dataset_dropdown.value = options[0].key

    def _on_dataset_change(self, e):
        """Handle dataset selection change."""
        self._current_dataset = None
        self._map_image.visible = False
        self._placeholder.visible = True
        self.update()
    
    def _on_hour_change(self, e):
        """Handle hour slider change."""
        self._current_hour = int(e.control.value)
        self._hour_text.value = f"{self._current_hour:02d}:00 UTC"
        self.update()
    
    def _on_generate_click(self, e):
        """Generate the map."""
        self.page.run_task(self._generate_map)
    
    async def _generate_map(self):
        """Generate map asynchronously."""
        import xarray as xr
        
        dataset_id = self._dataset_dropdown.value
        if not dataset_id:
            self._show_message("⚠️ Please select a dataset", is_error=True)
            return
        
        dataset = self.db.get_dataset(dataset_id)
        if not dataset:
            self._show_message("⚠️ Dataset not found", is_error=True)
            return
        
        # Find processed file - use file_path from database if available (handles batch imports)
        if dataset.file_path and Path(dataset.file_path).exists():
            processed_path = Path(dataset.file_path)
        else:
            # Fallback to constructed path for backwards compatibility
            safe_name = "".join(c if c.isalnum() or c in "._- " else "_" for c in dataset.name)
            processed_path = self.data_dir / "datasets" / safe_name / f"{safe_name}_processed.nc"

        if not processed_path.exists():
            self._show_message("⚠️ Processed data not found. Download or process the dataset first.", is_error=True)
            return
        
        self._show_message("🎨 Generating map...")
        self._progress_bar.visible = True
        self.update()
        
        try:
            # Load dataset
            ds = xr.open_dataset(processed_path)
            
            # Capture available hours for error reporting
            available_hours = []
            if 'HOUR' in ds.coords:
                available_hours = sorted(ds.HOUR.values.tolist())
            elif 'hour' in ds.coords:
                available_hours = sorted(ds.hour.values.tolist())
            
            variable = self._variable_dropdown.value
            hour = self._current_hour
            road_detail = self._road_dropdown.value
            bbox = [dataset.bbox.west, dataset.bbox.south, dataset.bbox.east, dataset.bbox.north]
            
            # Style args
            style_args = {
                'font_size': int(self._font_slider.value),
                'border_width': float(self._border_slider.value),
                'road_scale': float(self._road_slider.value),
            }
            if self._cmap_dropdown.value != "Default":
                style_args['colormap'] = self._cmap_dropdown.value
            
            # Parse Min/Max
            if self._vmin_input.value:
                try:
                    style_args['vmin'] = float(self._vmin_input.value)
                except ValueError:
                    self._show_message("⚠️ Invalid Min Value", is_error=True)
                    self._progress_bar.visible = False
                    return
            
            if self._vmax_input.value:
                try:
                    style_args['vmax'] = float(self._vmax_input.value)
                except ValueError:
                    self._show_message("⚠️ Invalid Max Value", is_error=True)
                    self._progress_bar.visible = False
                    return

            # Get sites from DB
            sites = self.db.get_sites_as_dict(dataset.bbox)

            # Generate map
            result = await asyncio.to_thread(
                self.plotter.generate_map,
                ds,
                hour,
                variable,
                dataset.name,
                bbox=bbox,
                road_detail=road_detail,
                sites=sites,
                **style_args
            )
            
            ds.close()
            self._progress_bar.visible = False
            
            if result:
                self._map_image.src = result
                self._map_image.visible = True
                self._placeholder.visible = False
                self._map_image.update()
                self._placeholder.update()
                self._show_message(f"✅ Generated {variable} map for {hour:02d}:00 UTC", is_success=True)
            else:
                hours_str = ", ".join(f"{h:02d}:00" for h in available_hours) if available_hours else "None found"
                self._show_message(
                    f"⚠️ No data for hour {hour:02d}:00.\nAvailable hours: {hours_str}", 
                    is_error=True
                )
            
        except Exception as e:
            self._progress_bar.visible = False
            self._show_message(f"❌ Error: {e}", is_error=True)
    
    def _on_map_click(self, e):
        """Open map in full-screen gallery."""
        if not self._map_image.src:
            return
            
        # Create a dialog with the image in an InteractiveViewer
        img = ft.Image(
            src=self._map_image.src,
            fit="contain",
        )
        
        viewer = ft.InteractiveViewer(
            min_scale=1,
            max_scale=5,
            content=img,
        )
        
        dlg = ft.AlertDialog(
            content=ft.Container(
                content=viewer,
                width=1000,
                height=800,
            ),
            inset_padding=10,
        )
        
        self.page.dialog = dlg
        dlg.open = True
        self.page.update()
