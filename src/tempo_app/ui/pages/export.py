"""Export page - Export datasets to various formats."""

import flet as ft
import pandas as pd
import xarray as xr
from pathlib import Path
from typing import Optional

from ..theme import Colors, Spacing
from ..components.widgets import SectionCard, StatusLogPanel
from ...storage.database import Database
from ...storage.models import Dataset, DatasetStatus
from ...core.exporter import DataExporter


class ExportPage(ft.Container):
    """Page for exporting datasets to various formats."""

    def __init__(self, db: Database, data_dir: Path):
        super().__init__()
        self.db = db
        self.data_dir = data_dir
        self.exporter = DataExporter(data_dir)
        self._selected_dataset: Optional[Dataset] = None
        self._build()

    def did_mount(self):
        """Called when control is added to page - load data async."""
        self.page.run_task(self._load_datasets_async)

    async def _load_datasets_async(self):
        """Load datasets without blocking UI."""
        import asyncio
        datasets = await asyncio.to_thread(self.db.get_all_datasets)
        self._apply_datasets(datasets)
        self.update()

    def _apply_datasets(self, datasets: list):
        """Apply datasets to dropdown (no DB call)."""
        try:
            self._status_log.add_info(f"Found {len(datasets)} total datasets")

            for ds in datasets:
                self._status_log.add_info(f"  - {ds.name}: status={ds.status}")

            completed = [ds for ds in datasets if ds.status == DatasetStatus.COMPLETE]

            options = [
                ft.DropdownOption(
                    key=str(ds.id),
                    text=f"{ds.name} ({ds.date_start} to {ds.date_end})"
                )
                for ds in completed
            ]
            self._dataset_dropdown.options = options
            self._status_log.add_info(f"Added {len(options)} options to dropdown")

            if not completed:
                self._status_log.add_warning(
                    "No completed datasets available. Create and download a dataset first."
                )
                self._export_button.disabled = True
            else:
                self._status_log.add_success(f"Found {len(completed)} datasets ready for export")
                if options and completed:
                    self._dataset_dropdown.value = options[0].key
                    self._selected_dataset = completed[0]
                    self._export_button.disabled = False
                    self._status_log.add_info(f"Auto-selected first dataset: {completed[0].name}")
                    self._update_preview()
        except Exception as e:
            self._status_log.add_error(f"Error refreshing datasets: {str(e)}")

    def _build(self):
        """Build the export page."""
        # Header
        header = ft.Row([
            ft.Icon(ft.Icons.FILE_DOWNLOAD, size=28, color=Colors.PRIMARY),
            ft.Text("Export Data", size=24, weight=ft.FontWeight.BOLD, color=Colors.ON_SURFACE),
        ], spacing=12)

        # Dataset selection dropdown
        self._dataset_dropdown = ft.Dropdown(
            label="Select Dataset",
            hint_text="Choose a dataset to export",
            border_color=Colors.BORDER,
            color=Colors.ON_SURFACE,
            expand=True,
            options=[],
        )
        self._dataset_dropdown.on_change = self._on_dataset_selected

        # Export format radio group
        self._export_format = ft.RadioGroup(
            content=ft.Column([
                ft.Radio(
                    value="hourly",
                    label="Hourly",
                    label_style=ft.TextStyle(color="#000000"),
                ),
                ft.Text(
                    "Separate file per site. Interleaved NO2/HCHO columns. Metadata sheet included.",
                    size=12,
                    color="#404040",
                    italic=True,
                ),
                ft.Container(height=8),
                ft.Radio(
                    value="daily",
                    label="Daily",
                    label_style=ft.TextStyle(color="#000000"),
                ),
                ft.Text(
                    "Single file for all sites. Dynamic columns based on settings. Metadata with stats.",
                    size=12,
                    color="#404040",
                    italic=True,
                ),
            ], spacing=4),
            value="daily",
        )

        # Export options
        self._num_points_field = ft.TextField(
            label="Number of Points",
            hint_text="4",
            value="4",
            width=150,
            border_color=Colors.BORDER,
            color="#000000",
            keyboard_type=ft.KeyboardType.NUMBER,
            label_style=ft.TextStyle(color="#000000"),
        )

        self._distance_field = ft.TextField(
            label="Distance (km)",
            hint_text="Leave empty to use point count",
            width=150,
            border_color=Colors.BORDER,
            color="#000000",
            keyboard_type=ft.KeyboardType.NUMBER,
            label_style=ft.TextStyle(color="#000000"),
        )

        self._utc_offset_field = ft.TextField(
            label="UTC Offset (hours)",
            hint_text="-6.0",
            value="-6.0",
            width=150,
            border_color=Colors.BORDER,
            color="#000000",
            keyboard_type=ft.KeyboardType.NUMBER,
            label_style=ft.TextStyle(color="#000000"),
        )

        # Export button
        self._export_button = ft.FilledButton(
            content=ft.Row([
                ft.Icon(ft.Icons.DOWNLOAD, size=18),
                ft.Text("Export"),
            ], spacing=8, tight=True),
            on_click=self._on_export,
            disabled=True,
        )

        # Status log
        self._status_log = StatusLogPanel(max_entries=50)
        self._status_log.height = 200

        # Preview controls
        self._preview_table = ft.DataTable(
            columns=[ft.DataColumn(ft.Text("Info", color="#000000"))],
            rows=[ft.DataRow([ft.DataCell(ft.Text("Select a dataset to preview", color="#404040"))])],
            border=ft.border.all(1, Colors.BORDER),
            vertical_lines=ft.border.BorderSide(1, Colors.BORDER),
            horizontal_lines=ft.border.BorderSide(1, Colors.BORDER),
        )
        self._preview_info = ft.Column(spacing=4)
        self._preview_container = ft.Column([
            ft.Text("Dataset Preview", weight=ft.FontWeight.BOLD, size=16, color="#000000"),
            self._preview_info,
            ft.Container(height=10),
            ft.Row([self._preview_table], scroll=ft.ScrollMode.AUTO),
        ], scroll=ft.ScrollMode.AUTO, expand=True)

        # Build sections
        dataset_card = SectionCard(
            title="Dataset Selection",
            icon=ft.Icons.DATASET,
            content=ft.Column([
                self._dataset_dropdown,
                ft.Container(height=8),
                ft.Row([
                    ft.FilledButton(
                        content=ft.Row([ft.Icon(ft.Icons.REFRESH, size=16), ft.Text("Refresh")], spacing=4, tight=True),
                        on_click=self._on_refresh,
                    ),
                ]),
            ], spacing=8),
        )

        format_card = SectionCard(
            title="Export Format",
            icon=ft.Icons.SETTINGS,
            content=self._export_format,
        )

        options_card = SectionCard(
            title="Export Options",
            icon=ft.Icons.TUNE,
            content=ft.Column([
                ft.Text("Spatial Averaging:", weight=ft.FontWeight.BOLD, color="#000000"),
                self._num_points_field,
                ft.Container(height=8),
                ft.Text("Time Zone:", weight=ft.FontWeight.BOLD, color="#000000"),
                self._utc_offset_field,
                ft.Container(height=16),
                self._export_button,
            ], spacing=8),
        )

        # Prepare content views for manual tabs
        self._settings_content = ft.Container(
            content=ft.Column([
                ft.Container(height=10),
                format_card,
                ft.Container(height=16),
                options_card,
                ft.Container(height=16),
                SectionCard(
                    title="Export Status",
                    icon=ft.Icons.INFO_OUTLINE,
                    content=self._status_log,
                ),
            ], scroll=ft.ScrollMode.AUTO),
            padding=10,
            visible=True,
        )
        
        self._preview_container.visible = False
        self._preview_container.padding = 10

        # Manual Tab Bar
        self._tab_1_btn = ft.TextButton(
            content=ft.Row([ft.Icon(ft.Icons.SETTINGS, size=16), ft.Text("Export Settings")], spacing=8),
            style=ft.ButtonStyle(color=Colors.PRIMARY),
            on_click=lambda e: self._switch_tab(0)
        )
        self._tab_2_btn = ft.TextButton(
            content=ft.Row([ft.Icon(ft.Icons.PREVIEW, size=16), ft.Text("Data Preview")], spacing=8),
            style=ft.ButtonStyle(color=Colors.ON_SURFACE_VARIANT),
            on_click=lambda e: self._switch_tab(1)
        )
        
        self._tab_bar = ft.Container(
            content=ft.Row([
                self._tab_1_btn,
                self._tab_2_btn,
            ], spacing=10),
            border=ft.border.only(bottom=ft.border.BorderSide(1, Colors.BORDER)),
            padding=ft.padding.only(bottom=5)
        )

        self.content = ft.Column([
            header,
            ft.Container(height=16),
            dataset_card,
            ft.Container(height=10),
            self._tab_bar,
            ft.Container(
                content=ft.Stack([
                    self._settings_content,
                    self._preview_container
                ]),
                expand=True
            )
        ], expand=True)

        self.expand = True
        self.padding = Spacing.PAGE_HORIZONTAL

    def _switch_tab(self, index):
        """Switch between tabs manually."""
        if index == 0:
            self._settings_content.visible = True
            self._preview_container.visible = False
            self._tab_1_btn.style = ft.ButtonStyle(color=Colors.PRIMARY)
            self._tab_2_btn.style = ft.ButtonStyle(color=Colors.ON_SURFACE_VARIANT)
        else:
            self._settings_content.visible = False
            self._preview_container.visible = True
            self._tab_1_btn.style = ft.ButtonStyle(color=Colors.ON_SURFACE_VARIANT)
            self._tab_2_btn.style = ft.ButtonStyle(color=Colors.PRIMARY)
            self._update_preview()
        self.update()

    def _update_preview(self):
        """Update data preview tab with selected dataset info."""
        if not self._selected_dataset or not self._selected_dataset.file_path:
            self._preview_info.controls = [ft.Text("No dataset selected or file not found")]
            self._preview_table.columns = []
            self._preview_table.rows = []
            return

        try:
            fpath = Path(self._selected_dataset.file_path)
            if not fpath.exists():
                self._preview_info.controls = [ft.Text(f"File not found: {fpath}", color=Colors.ERROR)]
                return

            # Open with xarray
            with xr.open_dataset(fpath) as ds:
                dims_str = ", ".join([f"{k}: {v}" for k, v in ds.dims.items()])
                coords_str = ", ".join(list(ds.coords))
                vars_str = ", ".join(list(ds.data_vars))
                
                info_lines = [
                    ft.Text(f"Dimensions: {dims_str}", size=12, color="#000000"),
                    ft.Text(f"Coordinates: {coords_str}", size=12, color="#000000"),
                    ft.Text(f"Variables: {vars_str}", size=12, color="#000000"),
                ]
                
                # If TSTEP exists (raw), show range
                if 'TSTEP' in ds.dims:
                    try:
                        t_start = pd.to_datetime(ds['TSTEP'].values[0])
                        t_end = pd.to_datetime(ds['TSTEP'].values[-1])
                        info_lines.append(ft.Text(f"Time Range: {t_start} to {t_end}", size=12, weight=ft.FontWeight.BOLD, color="#000000"))
                    except:
                        pass
                
                self._preview_info.controls = info_lines
                
                # Create a simple table preview (variables vs attributes or first few values)
                # Just show data vars list more formally
                cols = [
                    ft.DataColumn(ft.Text("Variable", color="#000000")), 
                    ft.DataColumn(ft.Text("Dims", color="#000000")), 
                    ft.DataColumn(ft.Text("Dtype", color="#000000"))
                ]
                rows = []
                for vname, var in ds.data_vars.items():
                    rows.append(ft.DataRow([
                        ft.DataCell(ft.Text(vname, color="#000000")),
                        ft.DataCell(ft.Text(str(var.dims), color="#000000")),
                        ft.DataCell(ft.Text(str(var.dtype), color="#000000")),
                    ]))
                
                self._preview_table.columns = cols
                self._preview_table.rows = rows
                
            self.update()
            
        except Exception as e:
            self._preview_info.controls = [ft.Text(f"Error generating preview: {e}", color=Colors.ERROR)]
            self.update()

    def _on_dataset_selected(self, e):
        """Handle dataset selection."""
        value = self._dataset_dropdown.value
        self._status_log.add_info(f"Dropdown changed, value: {value}")
        
        if not value:
            self._status_log.add_warning("No dataset value received")
            self._export_button.disabled = True
            self.update()
            return
            
        try:
            dataset_id = value  # Keep as string since db.get_dataset may accept string
            self._status_log.add_info(f"Looking up dataset ID: {dataset_id}")
            self._selected_dataset = self.db.get_dataset(dataset_id)
    
            if self._selected_dataset:
                self._export_button.disabled = False
                self._status_log.add_success(f"Selected dataset: {self._selected_dataset.name}")
                self._update_preview()
            else:
                self._status_log.add_error(f"Dataset not found for ID: {dataset_id}")
                self._export_button.disabled = True
        except Exception as ex:
            self._status_log.add_error(f"Error selecting dataset: {ex}")
            self._export_button.disabled = True

        self.update()

    def _on_refresh(self, e):
        """Handle refresh button click."""
        self._status_log.add_info("Refreshing dataset list...")
        self.page.run_task(self._load_datasets_async)

    def _on_export(self, e):
        """Handle export button click."""
        if not self._selected_dataset:
            self._status_log.add_error("No dataset selected")
            return

        try:
            # Parse export options
            export_format = self._export_format.value
            num_points = int(self._num_points_field.value or "4")
            utc_offset = float(self._utc_offset_field.value or "-6.0")

            # Load the dataset
            import xarray as xr
            dataset_path = Path(self._selected_dataset.file_path)

            if not dataset_path.exists():
                self._status_log.add_error(f"Dataset file not found: {dataset_path}")
                return

            self._status_log.add_info(f"Loading dataset from {dataset_path}...")
            ds = xr.open_dataset(dataset_path)
            
            # Log dataset structure
            self._status_log.add_info(f"Dataset dims: {list(ds.dims)}")
            self._status_log.add_info(f"Dataset coords: {list(ds.coords)}")
            
            # If processed dataset only has HOUR (no timestamps), load raw granules instead
            if 'HOUR' in ds.dims and 'TSTEP' not in ds.dims:
                self._status_log.add_info("Processed dataset detected - loading raw granules for timestamps...")
                ds.close()
                
                # Find raw granule files in dataset folder
                dataset_folder = dataset_path.parent
                raw_files = sorted(dataset_folder.glob("tempo_*.nc"))
                
                if raw_files:
                    self._status_log.add_info(f"Found {len(raw_files)} raw granule files")
                    raw_datasets = []
                    timestamps = []
                    
                    for rf in raw_files:
                        try:
                            raw_ds = xr.open_dataset(rf)
                            
                            # Extract datetime from filename: tempo_YYYY-MM-DD_HH.nc
                            fname = rf.stem  # e.g., "tempo_2024-05-01_15"
                            parts = fname.split('_')
                            if len(parts) >= 3:
                                date_str = parts[1]  # "2024-05-01"
                                hour_str = parts[2]  # "15"
                                dt = pd.to_datetime(f"{date_str} {hour_str}:00:00")
                                timestamps.append(dt)
                                raw_datasets.append(raw_ds)
                            else:
                                self._status_log.add_warning(f"Couldn't parse datetime from {rf.name}")
                        except Exception as e:
                            self._status_log.add_warning(f"Failed to load {rf.name}: {e}")
                    
                    if raw_datasets:
                        # Add TSTEP coordinate to each dataset and concatenate
                        for i, (raw_ds, ts) in enumerate(zip(raw_datasets, timestamps)):
                            if 'TSTEP' in raw_ds.dims:
                                raw_datasets[i] = raw_ds.assign_coords(TSTEP=[ts])
                            else:
                                raw_datasets[i] = raw_ds.expand_dims(TSTEP=[ts])
                        
                        ds = xr.concat(raw_datasets, dim='TSTEP')
                        
                        # Mask fill values (-1E+37) with NaN
                        fill_threshold = -1e30
                        for var in ds.data_vars:
                            ds[var] = ds[var].where(ds[var] > fill_threshold)
                        
                        self._status_log.add_info(f"Combined dataset dims: {list(ds.dims)}")
                        self._status_log.add_info(f"Date range: {timestamps[0]} to {timestamps[-1]}")
                    else:
                        self._status_log.add_error("No raw granules could be loaded")
                        return
                else:
                    self._status_log.add_warning("No raw granules found - using processed data")
                    ds = xr.open_dataset(dataset_path)

            # Disable button during export
            self._export_button.disabled = True
            self.update()

            # Perform export
            self._status_log.add_info(f"Exporting in format: {export_format}")
            self._status_log.add_info(f"  num_points={num_points}, utc_offset={utc_offset}")
            
            # Prepare metadata for export
            dataset = self._selected_dataset
            metadata = {
                'dataset_name': dataset.name,
                'created_at': str(dataset.created_at),
                'max_cloud': dataset.max_cloud,
                'max_sza': dataset.max_sza,
                'date_start': str(dataset.date_start),
                'date_end': str(dataset.date_end),
                'day_filter': dataset.day_filter_str(),
                'hour_filter': dataset.hour_filter_str(),
                'export_params_num_points': num_points,
                'export_params_utc_offset': utc_offset,
            }

            generated_files = self.exporter.export_dataset(
                dataset=ds,
                dataset_name=self._selected_dataset.name,
                export_format=export_format,
                num_points=num_points,
                utc_offset=utc_offset,
                metadata=metadata,
            )

            # Close dataset
            ds.close()

            # Report results
            if generated_files:
                self._status_log.add_success(f"Export complete! Generated {len(generated_files)} file(s):")
                for fpath in generated_files:
                    self._status_log.add_success(f"  📁 {fpath}")
                # Show export location
                export_dir = self.exporter.output_dir
                self._status_log.add_info(f"Files saved to: {export_dir}")
            else:
                self._status_log.add_warning("Export completed but no files were generated")
                self._status_log.add_warning("Possible causes:")
                self._status_log.add_warning("  - No sites found within dataset bounds")

        except ValueError as ve:
            self._status_log.add_error(f"Invalid input: {str(ve)}")
        except Exception as ex:
            self._status_log.add_error(f"Export failed: {str(ex)}")
            import traceback
            traceback.print_exc()
        finally:
            self._export_button.disabled = False
            self.update()
