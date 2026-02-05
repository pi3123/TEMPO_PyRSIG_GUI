"""Workspace Page - Unified view for a dataset with Map, Export, and Sites.

This page provides a single-page layout combining:
- Left: Map preview with controls
- Right Sidebar: Sites list + Export panel
"""

import flet as ft
from pathlib import Path
from typing import Optional
import asyncio
import xarray as xr

from ..theme import Colors, Spacing
from ..components.widgets import SectionCard
from ...storage.database import Database
from ...storage.models import Dataset, Site
from ...core.plotter import MapPlotter
from ...core.exporter import DataExporter


class WorkspacePage(ft.Container):
    """Unified workspace for a dataset - single page layout."""

    def __init__(self, db: Database, data_dir: Path, dataset_id: str = None):
        super().__init__()
        self.db = db
        self.data_dir = data_dir
        self.dataset_id = dataset_id
        self.plotter = MapPlotter(data_dir)
        self.exporter = DataExporter(data_dir)

        self._dataset: Optional[Dataset] = None
        self._sites: list[Site] = []
        self._current_hour = 12

        self._build()

    def did_mount(self):
        """Called when control is added to page - load data async."""
        import logging
        logging.info(f"WorkspacePage.did_mount called, dataset_id={self.dataset_id}")
        self.page.run_task(self._load_datasets_async)

    async def _load_datasets_async(self):
        """Load all datasets into dropdown."""
        import logging
        logging.info("Loading all datasets...")
        
        # Load all datasets for dropdown
        datasets = await asyncio.to_thread(self.db.get_all_datasets)
        logging.info(f"Found {len(datasets)} datasets")
        
        # Populate dropdown
        options = []
        for ds in datasets:
            options.append(ft.DropdownOption(key=ds.id, text=ds.name))
        self._dataset_dropdown.options = options
        
        # Select initial dataset
        if self.dataset_id:
            self._dataset_dropdown.value = self.dataset_id
        elif options:
            self._dataset_dropdown.value = options[0].key
        
        # Load selected dataset data
        await self._load_selected_dataset()
        self.update()

    async def _load_selected_dataset(self):
        """Load the currently selected dataset's data."""
        import logging
        dataset_id = self._dataset_dropdown.value if hasattr(self, '_dataset_dropdown') and self._dataset_dropdown.value else self.dataset_id
        
        if not dataset_id:
            self._dataset_title.value = "No datasets available"
            self._dataset_title.value = "No datasets available"
            logging.warning("No dataset selected")
            return
            
        self._dataset = await asyncio.to_thread(self.db.get_dataset, dataset_id)
        logging.info(f"Loaded dataset: {self._dataset}")
        
        if self._dataset:
            self._dataset_title.value = f"📊 {self._dataset.name}"
            self._sites = await asyncio.to_thread(
                self.db.get_sites_in_bbox, self._dataset.bbox
            )
            logging.info(f"Found {len(self._sites)} sites in bbox")
            self._update_sites_list()
            logging.info(f"Loaded: {self._dataset.name}")
            logging.info(f"Found {len(self._sites)} sites in bounds")
            
            # Load available hours from dataset file
            await self._load_available_hours()
        else:
            self._dataset_title.value = "Dataset not found"
            logging.error(f"Dataset not found: {dataset_id}")

    async def _load_available_hours(self):
        """Load available hours and variables from the dataset file and update UI."""
        import logging
        import pandas as pd
        try:
            if not self._dataset or not self._dataset.file_path:
                return

            processed_path = Path(self._dataset.file_path)
            if not processed_path.exists():
                return

            ds = await asyncio.to_thread(xr.open_dataset, processed_path)

            # Populate variable dropdown with available variables from dataset
            self._populate_variable_dropdown(ds)
            
            available_hours = []
            num_timesteps = 0
            
            # Check for TIME (new format) or TSTEP (old format) datetime dimensions
            if 'TIME' in ds.dims:
                timestamps = pd.to_datetime(ds.TIME.values)
                available_hours = sorted(set(timestamps.hour.tolist()))
                num_timesteps = len(timestamps)
                logging.info(f"Dataset has {num_timesteps} timesteps ({timestamps[0].date()} to {timestamps[-1].date()})")
            elif 'TSTEP' in ds.dims:
                timestamps = pd.to_datetime(ds.TSTEP.values)
                available_hours = sorted(set(timestamps.hour.tolist()))
                num_timesteps = len(timestamps)
                logging.info(f"Dataset has {num_timesteps} timesteps ({timestamps[0].date()} to {timestamps[-1].date()})")
            # Fallback to HOUR dimension (old aggregated format)
            elif 'HOUR' in ds.coords:
                available_hours = sorted(ds.HOUR.values.tolist())
            elif 'hour' in ds.coords:
                available_hours = sorted(ds.hour.values.tolist())
            
            ds.close()
            
            if available_hours:
                self._available_hours = available_hours
                min_hour = int(min(available_hours))
                max_hour = int(max(available_hours))
                
                # Update slider range and value
                self._hour_slider.min = min_hour
                self._hour_slider.max = max_hour
                self._hour_slider.divisions = max(1, max_hour - min_hour)
                self._hour_slider.value = min_hour
                self._current_hour = min_hour
                self._hour_text.value = f"Hour: {min_hour} UTC"
                
                hours_str = ", ".join(f"{h}" for h in available_hours)
                logging.info(f"Available hours: {hours_str}")
                logging.info(f"Set hour slider: min={min_hour}, max={max_hour}")
        except Exception as e:
            import logging
            logging.error(f"Failed to load available hours: {e}")

    def _populate_variable_dropdown(self, ds: xr.Dataset):
        """Populate variable dropdown with available variables from dataset."""
        import logging
        from ...core.variable_registry import VariableRegistry

        # Get all data variables from the dataset (excluding coordinates)
        available_vars = [var for var in ds.data_vars.keys()]

        logging.info(f"Available variables in dataset: {available_vars}")

        # Get metadata from registry for better display names
        registry_vars = {v.output_var: v for v in VariableRegistry.discover_variables()}

        # Build dropdown options
        options = []
        default_value = None

        for var_name in sorted(available_vars):
            # Get display name from registry if available
            if var_name in registry_vars:
                var_meta = registry_vars[var_name]
                display_name = var_meta.display_name
                if var_meta.unit:
                    display_name += f" ({var_meta.unit})"
            else:
                # Fallback for variables not in registry (like FNR)
                if var_name == 'FNR':
                    display_name = "FNR (HCHO/NO₂ Ratio)"
                else:
                    display_name = var_name

            options.append(ft.DropdownOption(key=var_name, text=display_name))

            # Set default to FNR if available, otherwise first variable
            if var_name == 'FNR' or default_value is None:
                default_value = var_name

        # Update dropdown
        self._variable_dropdown.options = options
        if default_value:
            self._variable_dropdown.value = default_value
        elif options:
            self._variable_dropdown.value = options[0].key

        logging.info(f"Populated variable dropdown with {len(options)} variables, default: {self._variable_dropdown.value}")

    def _on_dataset_change(self, e):
        """Handle dataset selection change."""
        self.page.run_task(self._on_dataset_change_async)

    async def _on_dataset_change_async(self):
        """Load newly selected dataset."""
        await self._load_selected_dataset()
        self.update()


    def _build(self):
        """Build the unified workspace layout."""
        # === HEADER with Dataset Selector ===
        self._dataset_dropdown = ft.Dropdown(
            label="Dataset",
            border_color=Colors.BORDER,
            focused_border_color=Colors.PRIMARY,
            bgcolor=Colors.SURFACE_VARIANT,
            width=300,
            text_style=ft.TextStyle(color=Colors.ON_SURFACE),
            label_style=ft.TextStyle(color=Colors.ON_SURFACE_VARIANT),
        )
        self._dataset_dropdown.on_change = self._on_dataset_change

        self._dataset_title = ft.Text(
            "Select a dataset",
            size=16,
            color=Colors.ON_SURFACE_VARIANT,
        )

        header = ft.Container(
            content=ft.Row([
                ft.IconButton(
                    icon=ft.Icons.ARROW_BACK,
                    icon_color=Colors.ON_SURFACE,
                    tooltip="Back to Library",
                    on_click=self._on_back_click,
                ),
                self._dataset_dropdown,
                ft.Container(width=16),
                self._dataset_title,
            ], spacing=8, vertical_alignment=ft.CrossAxisAlignment.CENTER),
            padding=ft.padding.only(bottom=Spacing.SM),
        )


        # === LEFT COLUMN: Map Generation ===
        left_column = self._build_map_section()

        # === RIGHT SIDEBAR: Sites + Export ===
        right_sidebar = self._build_sidebar()

        # === MAIN LAYOUT ===
        main_content = ft.Row([
            ft.Container(content=left_column, expand=True),
            ft.Container(
                content=right_sidebar,
                width=300,
                padding=ft.padding.only(left=Spacing.MD),
            ),
        ], expand=True, spacing=0)

        self.content = ft.Column([
            header,
            main_content,
        ], expand=True)
        self.expand = True
        self.padding = Spacing.PAGE_HORIZONTAL

    def _build_map_section(self):
        """Build the map preview and controls section (left column)."""
        # Variable selector - dynamically populated when dataset loads
        self._variable_dropdown = ft.Dropdown(
            label="Variable",
            value="",
            options=[
                # Will be populated from dataset when loaded
            ],
            width=200,
            border_color=Colors.BORDER,
            bgcolor=Colors.SURFACE_VARIANT,
            dense=True,
            text_style=ft.TextStyle(color=Colors.ON_SURFACE),
            label_style=ft.TextStyle(color=Colors.ON_SURFACE_VARIANT),
        )

        # Colormap selector
        self._colormap_dropdown = ft.Dropdown(
            label="Colormap",
            value="auto",
            options=[
                ft.DropdownOption(key="auto", text="Auto (Default)"),
                ft.DropdownOption(key="viridis", text="Viridis"),
                ft.DropdownOption(key="plasma", text="Plasma"),
                ft.DropdownOption(key="RdBu_r", text="Red-Blue"),
                ft.DropdownOption(key="YlOrRd", text="Yellow-Orange-Red"),
                ft.DropdownOption(key="coolwarm", text="Cool-Warm"),
            ],
            width=160,
            border_color=Colors.BORDER,
            bgcolor=Colors.SURFACE_VARIANT,
            dense=True,
            text_style=ft.TextStyle(color=Colors.ON_SURFACE),
            label_style=ft.TextStyle(color=Colors.ON_SURFACE_VARIANT),
        )

        # Color scale range controls
        self._vmin_field = ft.TextField(
            label="Min",
            hint_text="Auto",
            width=80,
            dense=True,
            disabled=True,  # Start disabled since auto is checked by default
            border_color=Colors.BORDER,
            bgcolor=Colors.SURFACE_VARIANT,
            text_style=ft.TextStyle(color=Colors.ON_SURFACE),
            label_style=ft.TextStyle(color=Colors.ON_SURFACE_VARIANT, size=11),
        )

        self._vmax_field = ft.TextField(
            label="Max",
            hint_text="Auto",
            width=80,
            dense=True,
            disabled=True,  # Start disabled since auto is checked by default
            border_color=Colors.BORDER,
            bgcolor=Colors.SURFACE_VARIANT,
            text_style=ft.TextStyle(color=Colors.ON_SURFACE),
            label_style=ft.TextStyle(color=Colors.ON_SURFACE_VARIANT, size=11),
        )

        self._auto_scale_checkbox = ft.Checkbox(
            label="Auto",
            value=True,
            label_style=ft.TextStyle(color=Colors.ON_SURFACE, size=12),
            on_change=self._on_auto_scale_change,
        )

        # Road options
        self._road_dropdown = ft.Dropdown(
            label="Roads",
            value="primary",
            options=[
                ft.DropdownOption(key="primary", text="Interstates Only"),
                ft.DropdownOption(key="major", text="Major Roads"),
                ft.DropdownOption(key="all", text="All Roads"),
            ],
            width=140,
            border_color=Colors.BORDER,
            bgcolor=Colors.SURFACE_VARIANT,
            dense=True,
            text_style=ft.TextStyle(color=Colors.ON_SURFACE),
            label_style=ft.TextStyle(color=Colors.ON_SURFACE_VARIANT),
        )

        self._show_sites_checkbox = ft.Checkbox(
            label="Show Sites",
            value=True,
            label_style=ft.TextStyle(color=Colors.ON_SURFACE),
            on_change=self._on_show_sites_change,
        )

        # Hour slider
        self._hour_slider = ft.Slider(
            min=0, max=23, divisions=23, value=12,
            label="{value}",
            on_change=self._on_hour_change,
            expand=True,
        )
        self._hour_text = ft.Text("Hour: 12 UTC", size=13, width=90, color=Colors.ON_SURFACE)

        # Generate button
        self._generate_btn = ft.FilledButton(
            content=ft.Row([
                ft.Icon(ft.Icons.MAP, size=18),
                ft.Text("Generate Map", color=Colors.ON_PRIMARY),
            ], spacing=6, tight=True),
            on_click=self._on_generate_click,
        )

        # Map image display
        self._map_image = ft.Image(
            src="",
            visible=False,
        )

        self._map_placeholder = ft.Container(
            content=ft.Column([
                ft.Icon(ft.Icons.MAP, size=64, color=Colors.ON_SURFACE_VARIANT),
                ft.Text("Select options and click Generate Map", 
                        color=Colors.ON_SURFACE_VARIANT),
            ], horizontal_alignment=ft.CrossAxisAlignment.CENTER, spacing=16),
            alignment=ft.Alignment(0, 0),
            expand=True,
            bgcolor=Colors.SURFACE_VARIANT,
            border_radius=8,
        )

        self._progress_bar = ft.ProgressBar(visible=False, color=Colors.PRIMARY)

        # Status message display (for errors/warnings)
        self._status_icon = ft.Icon(ft.Icons.INFO_OUTLINE, color=Colors.PRIMARY, size=20)
        self._status_text = ft.Text(
            "",
            size=13,
            color=Colors.ON_SURFACE,
            expand=True,
        )
        self._status_container = ft.Container(
            content=ft.Row([
                self._status_icon,
                self._status_text,
            ], spacing=8),
            padding=8,
            border_radius=6,
            bgcolor=Colors.SURFACE_VARIANT,
            visible=False,  # Hidden by default
        )

        # Controls row - split into two rows for better layout
        controls_row_1 = ft.Row([
            self._variable_dropdown,
            self._colormap_dropdown,
            ft.Text("Range:", size=12, color=Colors.ON_SURFACE_VARIANT),
            self._vmin_field,
            ft.Text("-", size=12, color=Colors.ON_SURFACE_VARIANT),
            self._vmax_field,
            self._auto_scale_checkbox,
        ], vertical_alignment=ft.CrossAxisAlignment.CENTER, spacing=8)

        controls_row_2 = ft.Row([
            self._road_dropdown,
            self._show_sites_checkbox,
            ft.Container(expand=True),
            self._generate_btn,
        ], vertical_alignment=ft.CrossAxisAlignment.CENTER, spacing=8)

        # Hour row
        hour_row = ft.Row([
            self._hour_text,
            self._hour_slider,
        ], vertical_alignment=ft.CrossAxisAlignment.CENTER)

        # Map container
        map_container = ft.Container(
            content=ft.Stack(
                [
                    self._map_placeholder,
                    self._map_image,
                ],
                fit=ft.StackFit.EXPAND,
            ),
            expand=True,
            border=ft.border.all(1, Colors.BORDER),
            border_radius=8,
            clip_behavior=ft.ClipBehavior.HARD_EDGE,
        )

        return ft.Column([
            controls_row_1,
            ft.Container(height=4),
            controls_row_2,
            ft.Container(height=4),
            hour_row,
            self._progress_bar,
            ft.Container(height=4),
            self._status_container,  # Status messages display
            map_container,
        ], expand=True, spacing=0)

    def _build_sidebar(self):
        """Build the right sidebar with Sites and Export panels."""
        # === SITES SECTION ===
        self._sites_list = ft.ListView(spacing=4, expand=True)
        self._sites_count = ft.Text(
            "0 sites",
            size=12,
            color=Colors.ON_SURFACE_VARIANT,
        )

        sites_header = ft.Row([
            ft.Icon(ft.Icons.LOCATION_ON, size=18, color=Colors.PRIMARY),
            ft.Text("Sites", size=14, weight=ft.FontWeight.W_600, color=Colors.ON_SURFACE),
            ft.Container(expand=True),
            self._sites_count,
        ])

        manage_sites_btn = ft.TextButton(
            content=ft.Row([
                ft.Icon(ft.Icons.SETTINGS, size=16, color=Colors.PRIMARY),
                ft.Text("Manage Sites...", color=Colors.PRIMARY),
            ], spacing=4, tight=True),
            on_click=self._on_manage_sites,
        )

        sites_section = ft.Container(
            content=ft.Column([
                sites_header,
                ft.Container(
                    content=self._sites_list,
                    expand=True,
                    border=ft.border.all(1, Colors.BORDER),
                    border_radius=4,
                    padding=4,
                ),
                manage_sites_btn,
            ], spacing=6),
            expand=True,
            padding=Spacing.SM,
            bgcolor=Colors.SURFACE,
            border_radius=8,
            border=ft.border.all(1, Colors.BORDER),
        )

        # === EXPORT BUTTON ===
        self._export_nav_btn = ft.FilledButton(
            content=ft.Row([
                ft.Icon(ft.Icons.FILE_DOWNLOAD, size=18),
                ft.Text("Export Data...", color=Colors.ON_PRIMARY),
            ], spacing=6, tight=True),
            on_click=self._on_export_nav_click,
            style=ft.ButtonStyle(
                padding=ft.padding.all(20),
            )
        )

        return ft.Column([
            sites_section,
            ft.Container(height=16),
            ft.Container(
                content=self._export_nav_btn,
                alignment=ft.Alignment(0, 0)
            )
        ], expand=True)

    def _update_sites_list(self):
        """Update the sites list display."""
        self._sites_list.controls.clear()
        self._sites_count.value = f"{len(self._sites)} sites"

        for i, site in enumerate(self._sites):
            # Fallback name if code is missing/empty
            site_name = site.code if site.code else f"Site #{i+1}"
            
            self._sites_list.controls.append(
                ft.Container(
                    content=ft.Row([
                        ft.Container(
                            content=ft.Icon(ft.Icons.PLACE, size=16, color=Colors.PRIMARY),
                            padding=8,
                            bgcolor=Colors.SURFACE,
                            border_radius=8,
                        ),
                        ft.Column([
                            ft.Text(site_name, weight=ft.FontWeight.W_600, size=14, color=Colors.ON_SURFACE_VARIANT),
                            ft.Text(
                                f"{site.latitude:.4f}, {site.longitude:.4f}",
                                size=11,
                                color=Colors.ON_SURFACE_VARIANT,
                                opacity=0.7,
                            ),
                        ], spacing=2, expand=True),
                    ], spacing=12),
                    padding=ft.padding.all(8),
                    bgcolor=Colors.SURFACE_VARIANT,
                    border_radius=8,
                    border=ft.border.all(1, Colors.BORDER),
                )
            )

    # === EVENT HANDLERS ===

    def _on_back_click(self, e):
        """Navigate back to library."""
        if self.page:
            shell = self.page.controls[0] if self.page.controls else None
            if shell and hasattr(shell, 'navigate_to'):
                shell.navigate_to("/library")

    def _on_manage_sites(self, e):
        """Navigate to sites management page."""
        if self.page:
            shell = self.page.controls[0] if self.page.controls else None
            if shell and hasattr(shell, 'navigate_to'):
                shell.navigate_to("/sites")

    def _show_status_message(self, message: str, is_error: bool = False, is_warning: bool = False):
        """Show a status message with appropriate styling."""
        self._status_text.value = message
        self._status_container.visible = True

        if is_error:
            self._status_container.bgcolor = Colors.ERROR_CONTAINER
            self._status_text.color = Colors.ON_ERROR_CONTAINER
            self._status_icon.name = ft.Icons.ERROR_OUTLINE
            self._status_icon.color = Colors.ERROR
        elif is_warning:
            self._status_container.bgcolor = "#FFF3E0"  # Light orange
            self._status_text.color = "#E65100"  # Dark orange
            self._status_icon.name = ft.Icons.WARNING_OUTLINE
            self._status_icon.color = "#FF9800"  # Orange
        else:
            self._status_container.bgcolor = Colors.PRIMARY_CONTAINER
            self._status_text.color = Colors.ON_PRIMARY_CONTAINER
            self._status_icon.name = ft.Icons.INFO_OUTLINE
            self._status_icon.color = Colors.PRIMARY

        self.update()

    def _hide_status_message(self):
        """Hide the status message."""
        self._status_container.visible = False
        self.update()

    def _on_hour_change(self, e):
        """Handle hour slider change."""
        hour = int(e.control.value)
        self._current_hour = hour
        self._hour_text.value = f"Hour: {hour} UTC"
        self.update()

    def _on_auto_scale_change(self, e):
        """Handle auto scale checkbox change - enable/disable min/max fields."""
        auto_enabled = self._auto_scale_checkbox.value
        self._vmin_field.disabled = auto_enabled
        self._vmax_field.disabled = auto_enabled
        if auto_enabled:
            self._vmin_field.value = ""
            self._vmax_field.value = ""
        self.update()

    def _on_show_sites_change(self, e):
        """Handle show sites checkbox change - regenerate map."""
        if self._dataset and self._map_image.visible:
            # Only regenerate if we have a map already displayed
            self.page.run_task(self._generate_map_async)

    def _on_generate_click(self, e):
        """Generate the map."""
        if not self._dataset:
            logging.error("No dataset loaded")
            return
        self.page.run_task(self._generate_map_async)

    async def _generate_map_async(self):
        """Generate map asynchronously."""
        import logging
        self._progress_bar.visible = True
        logging.info(f"Generating map for hour {self._current_hour}...")
        self.update()

        try:
            # Find processed file
            if self._dataset.file_path and Path(self._dataset.file_path).exists():
                processed_path = Path(self._dataset.file_path)
            else:
                safe_name = "".join(c if c.isalnum() or c in "._- " else "_" for c in self._dataset.name)
                processed_path = self.data_dir / "datasets" / safe_name / f"{safe_name}_processed.nc"

            logging.info(f"Looking for processed file: {processed_path}")
            
            if not processed_path.exists():
                logging.error("Processed data not found")
                self._progress_bar.visible = False
                self.update()
                return

            ds = await asyncio.to_thread(xr.open_dataset, processed_path)
            logging.info(f"Opened dataset with dims: {list(ds.dims)}")

            # Get sites if checkbox checked, otherwise pass empty dict to hide sites
            if self._show_sites_checkbox.value:
                sites = {s.code: s.to_tuple() for s in self._sites}
            else:
                sites = {}  # Empty dict = no sites shown

            variable = self._variable_dropdown.value
            road_detail = self._road_dropdown.value
            hour = self._current_hour

            # Get colormap settings
            colormap_value = self._colormap_dropdown.value
            colormap = None if colormap_value == "auto" else colormap_value

            # Get vmin/vmax settings
            vmin = None
            vmax = None
            if not self._auto_scale_checkbox.value:
                try:
                    if self._vmin_field.value:
                        vmin = float(self._vmin_field.value)
                    if self._vmax_field.value:
                        vmax = float(self._vmax_field.value)
                except ValueError:
                    logging.warning("Invalid min/max values, using auto scale")

            logging.info(f"Calling plotter.generate_map(variable={variable}, hour={hour}, colormap={colormap}, vmin={vmin}, vmax={vmax})")

            # Generate map - now returns (result, messages)
            plot_path, messages = await asyncio.to_thread(
                self.plotter.generate_map,
                ds,
                hour,
                variable,
                self._dataset.name,
                self._dataset.bbox.to_list(),
                road_detail,
                sites,
                colormap=colormap,
                vmin=vmin,
                vmax=vmax,
            )

            ds.close()

            # Log any messages from the plotter
            for msg in messages:
                if "❌" in msg or "ERROR" in msg:
                    logging.error(msg)
                elif "⚠️" in msg or "WARNING" in msg:
                    logging.warning(msg)
                else:
                    logging.info(msg)

            logging.info(f"Plotter returned: {plot_path}")

            if plot_path:
                if Path(plot_path).exists():
                    self._map_image.src = plot_path
                    self._map_image.visible = True
                    self._map_placeholder.visible = False

                    # Display messages in UI and logs
                    if messages:
                        warnings_text = "\n".join(messages)
                        self._show_status_message(
                            f"✅ Map generated for {hour:02d}:00 UTC\n{warnings_text}",
                            is_warning=True
                        )
                        logging.info(f"Map generated: {variable} at {hour}:00 UTC (with warnings: {'; '.join(messages)})")
                    else:
                        self._hide_status_message()
                        logging.info(f"Map generated: {variable} at {hour}:00 UTC")
                    logging.info(f"Map image set to: {plot_path}")
                else:
                    logging.error(f"Plot path doesn't exist: {plot_path}")
                    self._show_status_message(f"❌ Error: Plot file not found", is_error=True)
            else:
                error_msg = f"No map returned for hour {hour}"
                if messages:
                    error_msg = "\n".join(messages)
                    self._show_status_message(error_msg, is_error=True)
                else:
                    self._show_status_message(f"❌ No map returned for hour {hour}", is_error=True)
                logging.error(error_msg)

        except Exception as ex:
            import traceback
            logging.error(f"Error: {ex}")
            logging.error(f"Map generation error: {ex}")
            traceback.print_exc()
        finally:
            self._progress_bar.visible = False
            self.update()

    async def _on_export_nav_click(self, e):
        """Navigate to export page with current dataset."""
        if not self._dataset:
            logging.warning("Select a dataset first to export.")
            return

        if self.page:
            shell = self.page.controls[0]
            if shell and hasattr(shell, 'navigate_to'):
                 # Pass dataset_id in route
                shell.navigate_to(f"/export/{self._dataset.id}")

