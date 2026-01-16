"""Export Page - Dedicated data export interface."""

import flet as ft
from pathlib import Path
import asyncio
import pandas as pd
import xarray as xr
import numpy as np
from typing import Optional
import logging

from ..theme import Colors, Spacing, Typography
from ...storage.database import Database
from ...storage.models import Dataset, Site
from ...core.exporter import DataExporter
from ..components.widgets import StatusLogPanel

class ExportPage(ft.Container):
    """Page for exporting dataset data to Excel files."""

    def __init__(self, db: Database, data_dir: Path, dataset_id: str = None):
        super().__init__()
        self.db = db
        self.data_dir = data_dir
        self.dataset_id = dataset_id # Can be passed from navigation
        self.exporter = DataExporter(data_dir)
        
        # State
        self._dataset: Optional[Dataset] = None
        self._sites: list[Site] = []
        
        self.expand = True
        self.padding = 0
        self.bgcolor = Colors.BACKGROUND
        
        self._build()

    def did_mount(self):
        """Load data when mounted."""
        self.page.run_task(self._load_initial_data)

    async def _load_initial_data(self):
        """Load datasets and initial state."""
        # Load all datasets for dropdown
        datasets = await asyncio.to_thread(self.db.get_all_datasets)
        
        options = []
        for ds in datasets:
            # Only show completed datasets? Or all? User req: "Complete"
            # Assuming all for now, maybe filter by status if we had it.
            options.append(ft.DropdownOption(key=ds.id, text=ds.name))
        self._dataset_dropdown.options = options

        # Pre-select if ID provided, or defaults
        if self.dataset_id:
            # Validate it exists
            if any(d.id == self.dataset_id for d in datasets):
                self._dataset_dropdown.value = self.dataset_id
                await self._load_selected_dataset()
            else:
                self.dataset_id = None
        
        if not self.dataset_id and options:
            # Optional: Select first by default or leave empty
            # self._dataset_dropdown.value = options[0].key
            # await self._load_selected_dataset()
            pass
            
        self.update()

    async def _load_selected_dataset(self):
        """Load the full dataset object and update UI."""
        ds_id = self._dataset_dropdown.value
        logging.info(f"ExportPage: Loading dataset ID {ds_id}")
        self._status_log.add_info(f"Loading dataset info for ID: {ds_id}")
        self.update()
        
        if not ds_id:
            logging.warning("ExportPage: No dataset ID selected")
            return

        self._dataset = await asyncio.to_thread(self.db.get_dataset, ds_id)
        if self._dataset:
            # Update info text
            self._dataset_info.value = f"Selected: {self._dataset.name}\n" \
                                       f"Range: {self._dataset.date_start} to {self._dataset.date_end}"
            
            # Load sites
            logging.info(f"ExportPage: Loading sites in bbox {self._dataset.bbox}")
            self._sites = await asyncio.to_thread(
                self.db.get_sites_in_bbox, self._dataset.bbox
            )
            logging.info(f"ExportPage: Found {len(self._sites)} sites")
            
            # Update Preview
            await self._update_preview()
            
        else:
            logging.error(f"ExportPage: Dataset {ds_id} returned None from DB")
            self._status_log.add_error(f"Failed to load dataset {ds_id}")
            
        self.update()

    def _is_valid_value(self, val) -> bool:
        """Check if a value is valid (not NaN and not a fill value)."""
        if pd.isna(val):
            return False
        # Filter out common fill values (large negative numbers)
        # Many datasets use values like -9.99e36 or -999 as fill values
        if val < -1e30:  # Filter out extremely large negative values
            return False
        if val < -900:  # Filter out common fill values like -999
            return False
        # Also filter extremely large positive values (likely fill values)
        if val > 1e30:
            return False
        # NO2/HCHO values should reasonably be in range 1e13 to 1e18 molecules/cm2
        # Values outside this range are suspect
        if abs(val) > 1e20:
            return False
        return True

    async def _update_preview(self):
        """Update the data preview table based on current config."""
        if not self._dataset:
            self._preview_table.rows = []
            return

        logging.info("ExportPage: Updating preview...")
        self._status_log.add_info("Updating preview...")
        self._progress.visible = True
        self.update()
        
        try:
            # Find processed file
            if self._dataset.file_path and Path(self._dataset.file_path).exists():
                processed_path = Path(self._dataset.file_path)
            else:
                # Fallback path logic
                safe_name = "".join(c if c.isalnum() or c in "._- " else "_" for c in self._dataset.name)
                processed_path = self.data_dir / "datasets" / safe_name / f"{safe_name}_processed.nc"

            logging.info(f"ExportPage: Looking for processed file at {processed_path}")

            if not processed_path.exists():
                logging.error(f"ExportPage: Processed file not found at {processed_path}")
                self._status_log.add_error(f"Data file not found at {processed_path}")
                self._progress.visible = False
                self.update()
                return

            # Open with xarray (lazy)
            ds = await asyncio.to_thread(xr.open_dataset, processed_path)
            
            # Depending on format, show different columns
            fmt = self._format_radio.value
            utc_offset = float(self._utc_offset.value)
            
            # Get spatial aggregation settings
            spatial_mode = self._spatial_mode.value  # "points" or "radius"
            try:
                spatial_value = float(self._spatial_value.value)
            except ValueError:
                spatial_value = 9.0
            
            preview_rows = []
            columns = []
            
            # Get first site info for cell selection
            site_lat = self._sites[0].latitude if self._sites else None
            site_lon = self._sites[0].longitude if self._sites else None
            site_code = self._sites[0].code if self._sites else "N/A"
            
            # Determine time dimension
            time_dim = None
            time_values = None
            if 'TIME' in ds.dims:
                time_dim = 'TIME'
                time_values = pd.to_datetime(ds.TIME.values)
            elif 'TSTEP' in ds.dims:
                time_dim = 'TSTEP'
                time_values = pd.to_datetime(ds.TSTEP.values)
            elif 'HOUR' in ds.dims:
                time_dim = 'HOUR'
                time_values = ds.HOUR.values
            
            # Get coordinate arrays
            lats = ds['LAT'].values if 'LAT' in ds.coords else None
            lons = ds['LON'].values if 'LON' in ds.coords else None
            
            # Find cells based on spatial settings
            selected_cells = []  # List of (row, col) tuples
            
            if lats is not None and lons is not None and site_lat is not None and site_lon is not None:
                # Calculate distances from site to all grid cells
                from math import radians, sin, cos, sqrt, atan2
                
                def haversine(lat1, lon1, lat2, lon2):
                    R = 6371.0  # Earth radius in km
                    lat1_rad, lon1_rad = radians(lat1), radians(lon1)
                    lat2_rad, lon2_rad = radians(lat2), radians(lon2)
                    dlat = lat2_rad - lat1_rad
                    dlon = lon2_rad - lon1_rad
                    a = sin(dlat / 2)**2 + cos(lat1_rad) * cos(lat2_rad) * sin(dlon / 2)**2
                    c = 2 * atan2(sqrt(a), sqrt(1 - a))
                    return R * c
                
                # Calculate distances for all cells
                cell_distances = []
                for i in range(lats.shape[0]):
                    for j in range(lats.shape[1]):
                        dist = haversine(site_lat, site_lon, lats[i, j], lons[i, j])
                        cell_distances.append((i, j, dist))
                
                # Sort by distance
                cell_distances.sort(key=lambda x: x[2])
                
                if spatial_mode == "points":
                    # Take N nearest cells
                    n_points = int(spatial_value)
                    selected_cells = [(r, c) for r, c, _ in cell_distances[:n_points]]
                else:  # radius
                    # Take all cells within distance
                    selected_cells = [(r, c) for r, c, d in cell_distances if d <= spatial_value]
                    if not selected_cells:
                        # If no cells within radius, take at least 1
                        selected_cells = [(cell_distances[0][0], cell_distances[0][1])]
            else:
                # Fallback to center cell
                if lats is not None:
                    row_idx = lats.shape[0] // 2
                    col_idx = lats.shape[1] // 2
                else:
                    row_idx, col_idx = 0, 0
                selected_cells = [(row_idx, col_idx)]
            
            logging.info(f"ExportPage: Using {len(selected_cells)} cells for preview (mode={spatial_mode}, value={spatial_value})")
            
            # Limit to first 10 timesteps
            num_preview = min(10, len(time_values) if time_values is not None else 0)

            
            if fmt == "hourly":
                n_cells = len(selected_cells)
                use_fill = self._use_fill.value
                
                # Build columns to match export format: UTC_Time, Local_Time, Cell1_NO2, Cell1_HCHO, ...
                columns = ["UTC_Time", f"Local_Time (UTC{utc_offset:+.1f})"]
                for i in range(min(n_cells, 3)):  # Limit preview to first 3 cells
                    columns.extend([f"Cell{i+1}_NO2", f"Cell{i+1}_HCHO"])
                if n_cells > 3:
                    columns.append(f"... +{n_cells - 3} more")
                
                for t_idx in range(num_preview):
                    # Get time
                    if time_values is not None:
                        t = time_values[t_idx]
                        if isinstance(t, (pd.Timestamp, np.datetime64)):
                            t = pd.Timestamp(t)
                            local_t = t + pd.Timedelta(hours=utc_offset)
                            utc_str = t.strftime("%Y-%m-%d %H:%M")
                            local_str = local_t.strftime("%Y-%m-%d %H:%M")
                        else:
                            utc_str = f"{int(t):02d}:00 UTC"
                            local_h = (int(t) + int(utc_offset)) % 24
                            local_str = f"{local_h:02d}:00 Local"
                    else:
                        utc_str = f"T{t_idx}"
                        local_str = f"T{t_idx}"
                    
                    cells = [
                        ft.DataCell(ft.Text(utc_str, color=Colors.ON_SURFACE)),
                        ft.DataCell(ft.Text(local_str, color=Colors.ON_SURFACE)),
                    ]
                    
                    # Add per-cell values (first 3 cells)
                    for cell_idx, (row_idx, col_idx) in enumerate(selected_cells[:3]):
                        no2_str = "-999"
                        hcho_str = "-999"
                        
                        if 'NO2_TropVCD' in ds:
                            try:
                                val = ds['NO2_TropVCD'].isel(**{time_dim: t_idx}, ROW=row_idx, COL=col_idx).values.item()
                                if self._is_valid_value(val):
                                    no2_str = f"{val:.2e}"
                            except Exception:
                                pass
                        
                        if 'HCHO_TotVCD' in ds:
                            try:
                                val = ds['HCHO_TotVCD'].isel(**{time_dim: t_idx}, ROW=row_idx, COL=col_idx).values.item()
                                if self._is_valid_value(val):
                                    hcho_str = f"{val:.2e}"
                            except Exception:
                                pass
                        
                        cells.extend([
                            ft.DataCell(ft.Text(no2_str, color=Colors.ON_SURFACE)),
                            ft.DataCell(ft.Text(hcho_str, color=Colors.ON_SURFACE)),
                        ])
                    
                    if n_cells > 3:
                        cells.append(ft.DataCell(ft.Text("...", color=Colors.ON_SURFACE_VARIANT)))
                    
                    preview_rows.append(ft.DataRow(cells=cells))
            
            elif fmt == "daily":
                min_hours = int(self._min_hours_slider.value)
                use_fill = self._use_fill.value
                n_cells = len(selected_cells)
                spatial_mode = self._spatial_mode.value
                
                # Simpler column names for radius mode (include km in name)
                if spatial_mode == "radius":
                    try:
                        radius_km = float(self._spatial_value.value)
                    except ValueError:
                        radius_km = 10.0
                    radius_str = f"{int(radius_km)}km" if radius_km == int(radius_km) else f"{radius_km}km"
                    
                    columns = [
                        "Date", "Site",
                        f"TEMPO_NoFill_NO2_{radius_str}", f"TEMPO_NoFill_NO2_Cnt",
                        f"TEMPO_NoFill_HCHO_{radius_str}", f"TEMPO_NoFill_HCHO_Cnt"
                    ]
                    if use_fill:
                        columns.extend([
                            f"TEMPO_Fill_NO2_{radius_str}", f"TEMPO_Fill_NO2_Cnt",
                            f"TEMPO_Fill_HCHO_{radius_str}", f"TEMPO_Fill_HCHO_Cnt"
                        ])
                else:
                    # N-points mode: include cell count in column name
                    columns = [
                        "Date", "Site",
                        f"NO2_NoFill_{n_cells}_Avg", f"NO2_NoFill_{n_cells}_Cnt",
                        f"HCHO_NoFill_{n_cells}_Avg", f"HCHO_NoFill_{n_cells}_Cnt"
                    ]
                    if use_fill:
                        columns.extend([
                            f"NO2_Fill_{n_cells}_Avg", f"NO2_Fill_{n_cells}_Cnt",
                            f"HCHO_Fill_{n_cells}_Avg", f"HCHO_Fill_{n_cells}_Cnt"
                        ])
                
                # Group by date and compute daily averages
                if time_values is not None and len(time_values) > 0:
                    # Collect all values by date
                    daily_data = {}  # date -> {'no2_vals': [], 'hcho_vals': []}
                    
                    for t_idx in range(len(time_values)):
                        t = time_values[t_idx]
                        if isinstance(t, (pd.Timestamp, np.datetime64)):
                            t = pd.Timestamp(t)
                            local_t = t + pd.Timedelta(hours=utc_offset)
                            date_str = local_t.strftime("%Y-%m-%d")
                        else:
                            date_str = "Unknown"
                        
                        if date_str not in daily_data:
                            daily_data[date_str] = {'no2_vals': [], 'hcho_vals': [], 'hours': 0}
                        
                        daily_data[date_str]['hours'] += 1
                        
                        # Collect values from all selected cells for this timestep
                        for row_idx, col_idx in selected_cells:
                            if 'NO2_TropVCD' in ds:
                                try:
                                    val = ds['NO2_TropVCD'].isel(**{time_dim: t_idx}, ROW=row_idx, COL=col_idx).values.item()
                                    if self._is_valid_value(val):
                                        daily_data[date_str]['no2_vals'].append(val)
                                except Exception:
                                    pass
                            if 'HCHO_TotVCD' in ds:
                                try:
                                    val = ds['HCHO_TotVCD'].isel(**{time_dim: t_idx}, ROW=row_idx, COL=col_idx).values.item()
                                    if self._is_valid_value(val):
                                        daily_data[date_str]['hcho_vals'].append(val)
                                except Exception:
                                    pass
                    
                    # Build rows matching export format
                    sorted_dates = sorted(daily_data.keys())[:10]  # First 10 days
                    
                    for date_str in sorted_dates:
                        data = daily_data[date_str]
                        hours = data['hours']
                        
                        # NoFill: only if hours >= min_hours
                        if hours >= min_hours:
                            no2_avg = np.nanmean(data['no2_vals']) if data['no2_vals'] else -999
                            hcho_avg = np.nanmean(data['hcho_vals']) if data['hcho_vals'] else -999
                            no2_cnt = len(data['no2_vals'])
                            hcho_cnt = len(data['hcho_vals'])
                        else:
                            no2_avg = -999  # MISSING_VALUE indicator
                            hcho_avg = -999
                            no2_cnt = 0
                            hcho_cnt = 0
                        
                        # Format values
                        no2_avg_str = f"{no2_avg:.2e}" if no2_avg != -999 else "-999"
                        hcho_avg_str = f"{hcho_avg:.2e}" if hcho_avg != -999 else "-999"
                        
                        cells = [
                            ft.DataCell(ft.Text(date_str, color=Colors.ON_SURFACE)),
                            ft.DataCell(ft.Text(site_code, color=Colors.ON_SURFACE)),
                            ft.DataCell(ft.Text(no2_avg_str, color=Colors.ON_SURFACE)),
                            ft.DataCell(ft.Text(str(no2_cnt), color=Colors.ON_SURFACE)),
                            ft.DataCell(ft.Text(hcho_avg_str, color=Colors.ON_SURFACE)),
                            ft.DataCell(ft.Text(str(hcho_cnt), color=Colors.ON_SURFACE)),
                        ]
                        
                        if use_fill:
                            # Fill: always compute (even if < min_hours)
                            # For preview, show that Fill will have >= NoFill counts
                            # Real export uses monthly-hourly means which will fill more gaps
                            no2_fill_avg = np.nanmean(data['no2_vals']) if data['no2_vals'] else -999
                            hcho_fill_avg = np.nanmean(data['hcho_vals']) if data['hcho_vals'] else -999
                            
                            # In actual export, fill counts will be >= nofill counts
                            # Show with + indicator to indicate fill will add values
                            no2_fill_cnt = len(data['no2_vals'])
                            hcho_fill_cnt = len(data['hcho_vals'])
                            
                            no2_fill_str = f"{no2_fill_avg:.2e}" if no2_fill_avg != -999 else "-999"
                            hcho_fill_str = f"{hcho_fill_avg:.2e}" if hcho_fill_avg != -999 else "-999"
                            
                            # Show counts with + to indicate fill will have more
                            no2_cnt_str = f"{no2_fill_cnt}+" if no2_fill_cnt > 0 else "0"
                            hcho_cnt_str = f"{hcho_fill_cnt}+" if hcho_fill_cnt > 0 else "0"
                            
                            cells.extend([
                                ft.DataCell(ft.Text(no2_fill_str, color=Colors.ON_SURFACE)),
                                ft.DataCell(ft.Text(no2_cnt_str, color=Colors.ON_SURFACE)),
                                ft.DataCell(ft.Text(hcho_fill_str, color=Colors.ON_SURFACE)),
                                ft.DataCell(ft.Text(hcho_cnt_str, color=Colors.ON_SURFACE)),
                            ])
                        
                        preview_rows.append(ft.DataRow(cells=cells))

            # Update DataTable columns and rows
            logging.info(f"ExportPage: Setting {len(columns)} columns and {len(preview_rows)} rows for format '{fmt}'")
            self._preview_table.columns = [ft.DataColumn(ft.Text(c, color=Colors.ON_SURFACE)) for c in columns]
            self._preview_table.rows = preview_rows
            self._preview_table.update()  # Explicitly update the table
            
            ds.close()
            self._status_log.add_info(f"Preview updated for {fmt} - showing {len(preview_rows)} rows")
            logging.info(f"ExportPage: Preview updated successfully for {fmt}")

        except Exception as e:
            import traceback
            logging.error(f"ExportPage: Preview error: {e}")
            logging.error(traceback.format_exc())
            self._status_log.add_error(f"Preview error: {e}")
        
        self._progress.visible = False
        self.update()

    def _on_dataset_change(self, e):
        """Handle dataset dropdown change."""
        print(f"DEBUG: _on_dataset_change triggered. Value: {self._dataset_dropdown.value}")
        self._status_log.add_info("Selection changed...")
        self.update()
        self.page.run_task(self._load_selected_dataset)

    def _on_param_change(self, e):
        """Handle parameter changes to update preview."""
        logging.info(f"ExportPage: _on_param_change triggered, format={self._format_radio.value}")
        print(f"DEBUG: _on_param_change triggered, format={self._format_radio.value}")
        self.page.run_task(self._update_preview)

    def _on_min_hours_change(self, e):
        """Handle min hours slider change."""
        val = int(self._min_hours_slider.value)
        self._min_hours_label.value = f"Min Hours for Daily (NOFILL): {val}"
        self._min_hours_label.update()
        self.page.run_task(self._update_preview)

    def _on_export_click(self, e):
        """Run the export process."""
        self.page.run_task(self._export_async)

    async def _export_async(self):
        """Async implementation of export process."""
        if not self._dataset:
            self._status_log.add_error("Please select a dataset first.")
            return

        self._export_btn.disabled = True
        self._progress.visible = True
        self.update()
        
        try:
            # Prepare args
            ds_name = self._dataset.name
            fmt = self._format_radio.value
            utc_off = float(self._utc_offset.value)
            
            # Map radio selection to internal format strings
            format_map = {
                "hourly": "hourly",
                "daily": "daily",
            }
            export_fmt = format_map.get(fmt, "hourly")
            
            # Spatial mode
            mode = self._spatial_mode.value
            val = float(self._spatial_value.value)
            
            num_points = int(val) if mode == "points" else None
            distance = val if mode == "radius" else None
            
            self._status_log.add_info(f"Starting export: {ds_name} ({fmt})...")
            
            # Find file
            if self._dataset.file_path and Path(self._dataset.file_path).exists():
                processed_path = Path(self._dataset.file_path)
            else:
                 # Fallback path logic
                safe_name = "".join(c if c.isalnum() or c in "._- " else "_" for c in self._dataset.name)
                processed_path = self.data_dir / "datasets" / safe_name / f"{safe_name}_processed.nc"
                
            if not processed_path.exists():
                raise FileNotFoundError(f"Processed file not found: {processed_path}")

            # Run in thread
            ds = await asyncio.to_thread(xr.open_dataset, processed_path)
            
            # Get sites dict
            sites_dict = {s.code: s.to_tuple() for s in self._sites} if self._sites else None

            metadata = {
                "dataset_id": self._dataset.id,
                "exported_by": "Tempo Analyzer",
                "spatial_mode": mode,
                "spatial_value": val
            }

            files = await asyncio.to_thread(
                self.exporter.export_dataset,
                dataset=ds,
                dataset_name=ds_name,
                export_format=export_fmt,
                utc_offset=utc_off,
                num_points=num_points,
                distance_km=distance,
                sites=sites_dict,
                metadata=metadata
            )
            
            ds.close()
            
            if files:
                self._status_log.add_success(f"Export successful! Created {len(files)} files.")
                self._status_log.add_success(f"Location: {self.exporter.output_dir}")
                # Ideally show a button to open folder
            else:
                self._status_log.add_warning("No files created.")

        except Exception as ex:
             import traceback
             traceback.print_exc()
             self._status_log.add_error(f"Export failed: {ex}")
        
        self._export_btn.disabled = False
        self._progress.visible = False
        self.update()


    def _build(self):
        """Build the UI layout."""
        
        # 1. Configuration Panel (Left)
        # -----------------------------
        
        # Dataset Selection
        self._dataset_dropdown = ft.Dropdown(
            label="Select Dataset",
            text_size=14,
            width=300,
            text_style=ft.TextStyle(color=Colors.ON_SURFACE),
            label_style=ft.TextStyle(color=Colors.ON_SURFACE_VARIANT),
            on_select=self._on_dataset_change,
        )
        self._dataset_info = ft.Text(
            "No dataset selected", 
            size=12, 
            color=Colors.ON_SURFACE_VARIANT
        )
        
        # Format Selection
        self._format_radio = ft.RadioGroup(
            content=ft.Column([
                ft.Radio(value="hourly", label="Hourly (Per Site)", label_style=ft.TextStyle(color=Colors.ON_SURFACE)),
                ft.Radio(value="daily", label="Daily (Aggregated)", label_style=ft.TextStyle(color=Colors.ON_SURFACE)),
            ]),
            value="hourly",
            on_change=self._on_param_change
        )
        
        # Parameters
        self._utc_offset = ft.TextField(
            label="Time Zone Offset (UTC)",
            value="-6.0", 
            width=100, 
            keyboard_type=ft.KeyboardType.NUMBER,
            text_style=ft.TextStyle(color=Colors.ON_SURFACE),
            label_style=ft.TextStyle(color=Colors.ON_SURFACE_VARIANT),
            on_change=self._on_param_change # Update preview on change?
        )
        
        self._spatial_mode = ft.RadioGroup(
             content=ft.Row([
                ft.Radio(value="points", label="N-Points", label_style=ft.TextStyle(color=Colors.ON_SURFACE)),
                ft.Radio(value="radius", label="Radius (km)", label_style=ft.TextStyle(color=Colors.ON_SURFACE)),
            ]),
            value="points",
            on_change=self._on_param_change
        )
        
        self._spatial_value = ft.TextField(
            label="Count / Dist",
            value="9",
            width=100,
            keyboard_type=ft.KeyboardType.NUMBER,
            text_style=ft.TextStyle(color=Colors.ON_SURFACE),
            label_style=ft.TextStyle(color=Colors.ON_SURFACE_VARIANT),
            on_change=self._on_param_change
        )
        
        # Min hours slider for daily NOFILL
        self._min_hours_label = ft.Text(
            "Min Hours for Daily (NOFILL): 4",
            size=12,
            color=Colors.ON_SURFACE
        )
        self._min_hours_slider = ft.Slider(
            min=1,
            max=12,
            value=4,
            divisions=11,
            label="{value}",
            on_change=self._on_min_hours_change,
            width=200,
        )
        
        # Gap filling checkbox
        self._use_fill = ft.Checkbox(
            label="Apply Monthly Mean Fill",
            value=False,
            label_style=ft.TextStyle(color=Colors.ON_SURFACE),
            on_change=self._on_param_change
        )

        self._export_btn = ft.FilledButton(
            "Export Excel File",
            icon=ft.Icons.DOWNLOAD,
            width=300,
            on_click=self._on_export_click
        )

        config_panel = ft.Container(
            content=ft.Column([
                ft.Text("1. Source", weight=ft.FontWeight.BOLD, size=16, color=Colors.ON_SURFACE),
                self._dataset_dropdown,
                self._dataset_info,
                ft.Divider(),
                
                ft.Text("2. Format", weight=ft.FontWeight.BOLD, size=16, color=Colors.ON_SURFACE),
                self._format_radio,
                ft.Divider(),
                
                ft.Text("3. Configuration", weight=ft.FontWeight.BOLD, size=16, color=Colors.ON_SURFACE),
                self._utc_offset,
                ft.Text("Spatial Aggregation:", size=14, weight=ft.FontWeight.W_500, color=Colors.ON_SURFACE),
                self._spatial_mode,
                self._spatial_value,
                ft.Container(height=5),
                ft.Text("Daily Settings:", size=14, weight=ft.FontWeight.W_500, color=Colors.ON_SURFACE),
                self._min_hours_label,
                self._min_hours_slider,
                self._use_fill,
                
                ft.Divider(),
                ft.Container(height=10),
                self._export_btn,
                
            ], spacing=10, scroll=ft.ScrollMode.AUTO),
            width=350,
            padding=20,
            border=ft.border.only(right=ft.BorderSide(1, Colors.DIVIDER)),
            bgcolor=Colors.SURFACE
        )


        # 2. Preview Panel (Right)
        # ------------------------
        
        self._preview_table = ft.DataTable(
            columns=[ft.DataColumn(ft.Text("Select a dataset", color=Colors.ON_SURFACE))],
            rows=[],
            border=ft.border.all(1, Colors.BORDER),
            vertical_lines=ft.border.BorderSide(1, Colors.BORDER),
            horizontal_lines=ft.border.BorderSide(1, Colors.BORDER),
            heading_row_color=Colors.SURFACE_VARIANT,
        )
        
        self._status_log = StatusLogPanel()
        self._progress = ft.ProgressBar(visible=False, color=Colors.PRIMARY)

        preview_panel = ft.Container(
            content=ft.Column([
                ft.Row([
                    ft.Text("Data Preview", weight=ft.FontWeight.BOLD, size=18, color=Colors.ON_SURFACE),
                    ft.Container(expand=True),
                    ft.TextButton("Refresh Preview", icon=ft.Icons.REFRESH, on_click=self._on_param_change)
                ]),
                ft.Container(
                    content=ft.Row(
                        [self._preview_table],
                        scroll=ft.ScrollMode.AUTO,  # Horizontal scroll
                    ),
                    expand=True,
                    border=ft.border.all(1, Colors.BORDER),
                    border_radius=8,
                    bgcolor=Colors.SURFACE,
                ),
                ft.Container(height=10),
                ft.Text("Export Log", weight=ft.FontWeight.BOLD, color=Colors.ON_SURFACE),
                self._progress,
                ft.Container(
                    content=self._status_log,
                    height=150,
                    border=ft.border.all(1, Colors.BORDER),
                    border_radius=8,
                )
            ], expand=True, spacing=10),
            padding=20,
            expand=True
        )

        # Header with Back button
        header = ft.Container(
            content=ft.Row([
                ft.IconButton(ft.Icons.ARROW_BACK, on_click=lambda e: self.page.go("/workspace")),
                ft.Text("EXPORT DATASET", size=20, weight=ft.FontWeight.BOLD)
            ]),
            padding=10,
            bgcolor=Colors.SURFACE,
            border=ft.border.only(bottom=ft.BorderSide(1, Colors.DIVIDER))
        )

        self.content = ft.Column([
            # header, # Nav bar handles nav? Or should I keep a back button to workspace? 
            # Requirements said "[< Back to Workspace]". I'll include it.
            # But "page.go" might not work if shell uses _on_route_change directly. 
            # I'll rely on shell navigation if possible, but the button is visual.
            # I'll enable the button to navigate to /workspace
            
            ft.Row([
                config_panel,
                preview_panel
            ], expand=True, spacing=0)
        ], expand=True)

