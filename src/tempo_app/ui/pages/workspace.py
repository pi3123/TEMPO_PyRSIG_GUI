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
from ..components.widgets import SectionCard, StatusLogPanel
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
            self._status_log.add_warning("No dataset selected")
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
            self._status_log.add_info(f"Loaded: {self._dataset.name}")
            self._status_log.add_info(f"Found {len(self._sites)} sites in bounds")
            
            # Load available hours from dataset file
            await self._load_available_hours()
        else:
            self._dataset_title.value = "Dataset not found"
            self._status_log.add_error(f"Dataset not found: {dataset_id}")

    async def _load_available_hours(self):
        """Load available hours from the dataset file and update slider."""
        import logging
        import pandas as pd
        try:
            if not self._dataset or not self._dataset.file_path:
                return
            
            processed_path = Path(self._dataset.file_path)
            if not processed_path.exists():
                return
                
            ds = await asyncio.to_thread(xr.open_dataset, processed_path)
            
            available_hours = []
            num_timesteps = 0
            
            # Check for TIME (new format) or TSTEP (old format) datetime dimensions
            if 'TIME' in ds.dims:
                timestamps = pd.to_datetime(ds.TIME.values)
                available_hours = sorted(set(timestamps.hour.tolist()))
                num_timesteps = len(timestamps)
                self._status_log.add_info(f"Dataset has {num_timesteps} timesteps ({timestamps[0].date()} to {timestamps[-1].date()})")
            elif 'TSTEP' in ds.dims:
                timestamps = pd.to_datetime(ds.TSTEP.values)
                available_hours = sorted(set(timestamps.hour.tolist()))
                num_timesteps = len(timestamps)
                self._status_log.add_info(f"Dataset has {num_timesteps} timesteps ({timestamps[0].date()} to {timestamps[-1].date()})")
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
                self._status_log.add_info(f"Available hours: {hours_str}")
                logging.info(f"Set hour slider: min={min_hour}, max={max_hour}")
        except Exception as e:
            import logging
            logging.error(f"Failed to load available hours: {e}")

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
        # Variable selector - keys must match plotter expectations: 'NO2', 'HCHO', 'FNR'
        self._variable_dropdown = ft.Dropdown(
            label="Variable",
            value="FNR",
            options=[
                ft.DropdownOption(key="FNR", text="FNR (HCHO/NO2)"),
                ft.DropdownOption(key="NO2", text="NO2 Tropospheric VCD"),
                ft.DropdownOption(key="HCHO", text="HCHO Total VCD"),
            ],
            width=200,
            border_color=Colors.BORDER,
            bgcolor=Colors.SURFACE_VARIANT,
            dense=True,
            text_style=ft.TextStyle(color=Colors.ON_SURFACE),
            label_style=ft.TextStyle(color=Colors.ON_SURFACE_VARIANT),
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

        # Status log (shared for map and export)
        self._status_log = StatusLogPanel()

        # Controls row
        controls_row = ft.Row([
            self._variable_dropdown,
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
            content=ft.Stack([
                self._map_placeholder,
                self._map_image,
            ]),
            expand=True,
            border=ft.border.all(1, Colors.BORDER),
            border_radius=8,
            clip_behavior=ft.ClipBehavior.HARD_EDGE,
        )

        return ft.Column([
            controls_row,
            ft.Container(height=4),
            hour_row,
            self._progress_bar,
            ft.Container(height=4),
            map_container,
            ft.Container(height=8),
            ft.Container(
                content=self._status_log,
                height=100,
                border=ft.border.all(1, Colors.BORDER),
                border_radius=8,
            ),
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

    def _on_hour_change(self, e):
        """Handle hour slider change."""
        hour = int(e.control.value)
        self._current_hour = hour
        self._hour_text.value = f"Hour: {hour} UTC"
        self.update()

    def _on_generate_click(self, e):
        """Generate the map."""
        if not self._dataset:
            self._status_log.add_error("No dataset loaded")
            return
        self.page.run_task(self._generate_map_async)

    async def _generate_map_async(self):
        """Generate map asynchronously."""
        import logging
        self._progress_bar.visible = True
        self._status_log.add_info(f"Generating map for hour {self._current_hour}...")
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
                self._status_log.add_error("Processed data not found")
                self._progress_bar.visible = False
                self.update()
                return

            ds = await asyncio.to_thread(xr.open_dataset, processed_path)
            logging.info(f"Opened dataset with dims: {list(ds.dims)}")

            # Get sites if checkbox checked
            sites = None
            if self._show_sites_checkbox.value:
                sites = {s.code: s.to_tuple() for s in self._sites}

            variable = self._variable_dropdown.value
            road_detail = self._road_dropdown.value
            hour = self._current_hour
            
            logging.info(f"Calling plotter.generate_map(variable={variable}, hour={hour})")

            plot_path = await asyncio.to_thread(
                self.plotter.generate_map,
                ds,
                hour,
                variable,
                self._dataset.name,
                self._dataset.bbox.to_list(),
                road_detail,
                sites,
            )

            ds.close()
            
            logging.info(f"Plotter returned: {plot_path}")
            self._status_log.add_info(f"Plotter returned: {plot_path}")

            if plot_path:
                if Path(plot_path).exists():
                    self._map_image.src = plot_path
                    self._map_image.visible = True
                    self._map_placeholder.visible = False
                    self._status_log.add_success(f"Map generated: {variable} at {hour}:00 UTC")
                    logging.info(f"Map image set to: {plot_path}")
                else:
                    self._status_log.add_error(f"Plot path doesn't exist: {plot_path}")
            else:
                self._status_log.add_error(f"No map returned for hour {hour}")

        except Exception as ex:
            import traceback
            self._status_log.add_error(f"Error: {ex}")
            logging.error(f"Map generation error: {ex}")
            traceback.print_exc()
        finally:
            self._progress_bar.visible = False
            self.update()

    async def _on_export_nav_click(self, e):
        """Navigate to export page with current dataset."""
        if not self._dataset:
            self._status_log.add_warning("Select a dataset first to export.")
            return
            
        if self.page:
            shell = self.page.controls[0]
            if shell and hasattr(shell, 'navigate_to'):
                 # Pass dataset_id in route
                shell.navigate_to(f"/export/{self._dataset.id}")

