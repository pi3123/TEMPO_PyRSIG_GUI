"""Data Inspector page - detailed view of datasets and their data."""

import flet as ft
from datetime import datetime
from pathlib import Path
from typing import Optional
import asyncio

try:
    import xarray as xr
    import np
    import matplotlib
    matplotlib.use('Agg')
    import matplotlib.pyplot as plt
    import io
    import base64
except ImportError:
    xr = None
    np = None
    plt = None

from ..theme import Colors, Spacing
from ..components.widgets import SectionCard
from ...storage.database import Database
from ...storage.models import Dataset, DatasetStatus


class InspectPage(ft.Container):
    """Page for inspecting dataset details and statistics."""
    
    def __init__(self, db: Database):
        super().__init__()
        self.db = db
        self.selected_dataset: Optional[Dataset] = None
        self._build()
    
    def did_mount(self):
        """Called when control is added to page - load data async."""
        self.page.run_task(self._load_initial_data_async)

    async def _load_initial_data_async(self):
        """Load initial page data without blocking UI."""
        datasets = await asyncio.to_thread(self.db.get_all_datasets)
        self._apply_datasets(datasets)
        # Load stats for initially selected dataset
        if self.selected_dataset:
            await self._load_data_stats()
        self.update()

    def _apply_datasets(self, datasets: list):
        """Apply datasets to dropdown (no DB call)."""
        self._dataset_dropdown.options = [
            ft.DropdownOption(dataset.id, dataset.name) for dataset in datasets
        ]
        if datasets:
            self._dataset_dropdown.value = datasets[0].id
            self.selected_dataset = datasets[0]
            self._update_info_display()
    
    def _build(self):
        """Build the inspect page."""
        # Header
        header = ft.Row([
            ft.Icon(ft.Icons.ANALYTICS, size=28, color=Colors.PRIMARY),
            ft.Text("Data Inspector", size=24, weight=ft.FontWeight.BOLD, color=Colors.ON_SURFACE),
        ], spacing=12)
        
        # Dataset Selector
        self._dataset_dropdown = ft.Dropdown(
            label="Select Dataset",
            hint_text="Choose a dataset to inspect",
            width=400,
            options=[],
            border_color=Colors.BORDER,
            bgcolor=Colors.SURFACE_VARIANT,
            text_style=ft.TextStyle(color=Colors.ON_SURFACE),
        )
        self._dataset_dropdown.on_change = self._on_dataset_change
        
        self._refresh_btn = ft.IconButton(
            icon=ft.Icons.REFRESH,
            tooltip="Refresh datasets",
            on_click=self._on_refresh,
        )
        
        selector_row = ft.Row([
            self._dataset_dropdown,
            self._refresh_btn,
        ], spacing=8)
        
        # Dataset Info Card
        self._info_content = ft.Column([
            ft.Text("Select a dataset to view details", color=Colors.ON_SURFACE_VARIANT, italic=True)
        ], spacing=8)
        
        info_card = SectionCard(
            title="Dataset Configuration",
            icon=ft.Icons.INFO,
            content=self._info_content,
        )
        
        # Data Statistics Card
        self._stats_content = ft.Column([
            ft.Text("No data loaded", color=Colors.ON_SURFACE_VARIANT, italic=True)
        ], spacing=8)
        
        stats_card = SectionCard(
            title="Data Statistics",
            icon=ft.Icons.BAR_CHART,
            content=self._stats_content,
        )
        
        # Loading indicator
        self._loading = ft.ProgressRing(visible=False)
        
        self.content = ft.Column([
            header,
            ft.Container(height=16),
            selector_row,
            ft.Container(height=16),
            self._loading,
            info_card,
            ft.Container(height=16),
            stats_card,
        ], scroll=ft.ScrollMode.AUTO)
        
        self.expand = True
        self.padding = Spacing.PAGE_HORIZONTAL

    def _on_refresh(self, e):
        """Refresh the dataset list."""
        self.page.run_task(self._refresh_async)

    async def _refresh_async(self):
        """Async refresh handler."""
        datasets = await asyncio.to_thread(self.db.get_all_datasets)
        self._apply_datasets(datasets)
        if self.selected_dataset:
            await self._load_data_stats()
        self.update()

    def _on_dataset_change(self, e):
        """Handle dataset selection change."""
        dataset_id = e.control.value
        if dataset_id:
            self.page.run_task(self._change_dataset_async, dataset_id)

    async def _change_dataset_async(self, dataset_id: str):
        """Async dataset change handler."""
        self.selected_dataset = await asyncio.to_thread(self.db.get_dataset, dataset_id)
        self._update_info_display()
        await self._load_data_stats()
        self.update()
    
    def _update_info_display(self):
        """Update the dataset info display."""
        if not self.selected_dataset:
            return
        
        ds = self.selected_dataset
        
        # Status indicator
        if ds.status == DatasetStatus.COMPLETE:
            status_text = "✓ Complete"
            status_color = Colors.SUCCESS
        elif ds.status == DatasetStatus.PARTIAL:
            status_text = "⚠️ Partial"
            status_color = Colors.WARNING
        elif ds.status == DatasetStatus.ERROR:
            status_text = "✗ Error"
            status_color = Colors.ERROR
        else:
            status_text = "Pending"
            status_color = Colors.ON_SURFACE_VARIANT
        
        self._info_content.controls = [
            self._info_row("Name", ds.name),
            self._info_row("Status", status_text, status_color),
            ft.Divider(height=1, color=Colors.BORDER),
            self._info_row("Date Range", f"{ds.date_start} to {ds.date_end}"),
            self._info_row("Day Filter", ds.day_filter_str()),
            self._info_row("Hour Filter", ds.hour_filter_str()),
            ft.Divider(height=1, color=Colors.BORDER),
            self._info_row("Region", f"{ds.bbox.west:.2f}° to {ds.bbox.east:.2f}° W, {ds.bbox.south:.2f}° to {ds.bbox.north:.2f}° N"),
            self._info_row("Cloud Filter", f"≤ {ds.max_cloud:.0%}"),
            self._info_row("SZA Filter", f"≤ {ds.max_sza:.0f}°"),
            ft.Divider(height=1, color=Colors.BORDER),
            self._info_row("File Size", f"{ds.file_size_mb:.1f} MB"),
            self._info_row("Granules", f"{ds.granules_downloaded} / {ds.granule_count}"),
        ]
        
        if ds.file_path:
            self._info_content.controls.append(
                self._info_row("File Path", ds.file_path, size=10)
            )
    
    def _info_row(self, label: str, value: str, color=None, size: int = 13) -> ft.Row:
        """Create an info row."""
        return ft.Row([
            ft.Text(label + ":", size=size, color=Colors.ON_SURFACE_VARIANT, width=100),
            ft.Text(value, size=size, color=color or Colors.ON_SURFACE, expand=True),
        ], spacing=8)
    
    async def _load_data_stats(self):
        """Load and display statistics from the actual data file."""
        if not self.selected_dataset or not self.selected_dataset.file_path:
            self._stats_content.controls = [
                ft.Text("No processed data file available", color=Colors.ON_SURFACE_VARIANT, italic=True)
            ]
            self.update()
            return
        
        if xr is None:
            self._stats_content.controls = [
                ft.Text("xarray not available", color=Colors.ERROR)
            ]
            self.update()
            return
        
        self._loading.visible = True
        self.update()
        
        try:
            # Load stats in thread
            stats = await asyncio.to_thread(self._compute_stats, self.selected_dataset.file_path)
            
            if stats:
                self._build_stats_display(stats)
            else:
                self._stats_content.controls = [
                    ft.Text("Could not load data statistics", color=Colors.WARNING)
                ]
        except Exception as e:
            self._stats_content.controls = [
                ft.Text(f"Error loading stats: {e}", color=Colors.ERROR)
            ]
        
        self._loading.visible = False
        self.update()
    
    def _build_stats_display(self, stats: dict):
        """Build the statistics display with charts."""
        controls = []
        
        # Basic info section
        controls.extend([
            self._info_row("Grid Size", stats.get("grid_size", "N/A")),
            self._info_row("Available Hours", stats.get("hours", "N/A")),
            ft.Container(height=8),
        ])
        
        # Hourly Averages Chart
        hourly_data = stats.get("hourly_data", {})
        if hourly_data:
            controls.extend([
                ft.Divider(height=1, color=Colors.BORDER),
                ft.Container(height=8),
                ft.Text("📊 Hourly Averages & Trends", size=16, weight=ft.FontWeight.W_600, color=Colors.PRIMARY),
                ft.Container(height=8),
                self._build_hourly_trend_chart(hourly_data),
                ft.Container(height=16),
                self._build_hourly_chart(hourly_data),
                ft.Container(height=16),
            ])
        
        # Variable Statistics Cards
        controls.append(ft.Divider(height=1, color=Colors.BORDER))
        controls.append(ft.Container(height=8))
        controls.append(ft.Text("📈 Variable Statistics", size=16, weight=ft.FontWeight.W_600, color=Colors.PRIMARY))
        controls.append(ft.Container(height=8))
        
        # Create stats cards for each variable
        var_cards = []
        
        # NO2 card
        if stats.get("no2_mean") is not None:
            var_cards.append(self._build_variable_card(
                "NO₂ Tropospheric VCD",
                stats.get("no2_min"),
                stats.get("no2_max"),
                stats.get("no2_mean"),
                stats.get("no2_std"),
                stats.get("no2_valid_pct"),
                ft.Colors.BLUE_800,
            ))

        # HCHO card
        if stats.get("hcho_mean") is not None:
            var_cards.append(self._build_variable_card(
                "HCHO Total VCD",
                stats.get("hcho_min"),
                stats.get("hcho_max"),
                stats.get("hcho_mean"),
                stats.get("hcho_std"),
                stats.get("hcho_valid_pct"),
                ft.Colors.GREEN_800,
            ))

        # FNR card
        if stats.get("fnr_mean") is not None:
            var_cards.append(self._build_variable_card(
                "FNR (HCHO/NO₂)",
                stats.get("fnr_min"),
                stats.get("fnr_max"),
                stats.get("fnr_mean"),
                stats.get("fnr_std"),
                stats.get("fnr_valid_pct"),
                ft.Colors.ORANGE_800,
            ))
        
        if var_cards:
            controls.append(ft.Row(var_cards, wrap=True, spacing=16, run_spacing=16))
        

        
        self._stats_content.controls = controls
    
    def _build_variable_card(self, title: str, min_val, max_val, mean_val, std_val, valid_pct, color) -> ft.Container:
        """Build a statistics card for a variable."""
        def fmt(v):
            if v is None:
                return "N/A"
            if abs(v) < 0.001 or abs(v) > 1000:
                return f"{v:.2e}"
            return f"{v:.4f}"
        
        return ft.Container(
            content=ft.Column([
                ft.Text(title, size=16, weight=ft.FontWeight.BOLD, color=color),
                ft.Container(height=8),
                ft.Row([
                    ft.Column([
                        ft.Text("Min", size=12, color=Colors.ON_SURFACE_VARIANT),
                        ft.Text(fmt(min_val), size=14, font_family="monospace", color=Colors.ON_SURFACE),
                    ], spacing=4, horizontal_alignment=ft.CrossAxisAlignment.CENTER),
                    ft.Column([
                        ft.Text("Mean", size=12, color=Colors.ON_SURFACE_VARIANT),
                        ft.Text(fmt(mean_val), size=15, font_family="monospace", weight=ft.FontWeight.BOLD, color=Colors.ON_SURFACE),
                    ], spacing=4, horizontal_alignment=ft.CrossAxisAlignment.CENTER),
                    ft.Column([
                        ft.Text("Max", size=12, color=Colors.ON_SURFACE_VARIANT),
                        ft.Text(fmt(max_val), size=14, font_family="monospace", color=Colors.ON_SURFACE),
                    ], spacing=4, horizontal_alignment=ft.CrossAxisAlignment.CENTER),
                ], spacing=20, alignment=ft.MainAxisAlignment.SPACE_AROUND),
                ft.Container(height=8),
                ft.Row([
                    ft.Text(f"σ = {fmt(std_val)}", size=12, color=Colors.ON_SURFACE_VARIANT),
                    ft.Text(f"{valid_pct:.1f}% valid" if valid_pct else "", size=12, color=Colors.ON_SURFACE_VARIANT),
                ], spacing=16, alignment=ft.MainAxisAlignment.CENTER),
            ], spacing=0, horizontal_alignment=ft.CrossAxisAlignment.CENTER),
            bgcolor=Colors.SURFACE_VARIANT,
            padding=16,
            border_radius=8,
            width=300,
        )
    
    def _build_hourly_chart(self, hourly_data: dict) -> ft.Container:
        """Build a table showing hourly averages for each variable."""
        hours = sorted(hourly_data.keys())
        if not hours:
            return ft.Text("No hourly data available", color=Colors.ON_SURFACE_VARIANT, italic=True)

        # Build table rows
        rows = [
            # Header row
            ft.DataRow(cells=[
                ft.DataCell(ft.Text("Hour", size=12, weight=ft.FontWeight.BOLD, color=Colors.ON_SURFACE)),
                ft.DataCell(ft.Text("NO₂ (molec/cm²)", size=12, weight=ft.FontWeight.BOLD, color=Colors.ON_SURFACE)),
                ft.DataCell(ft.Text("HCHO (molec/cm²)", size=12, weight=ft.FontWeight.BOLD, color=Colors.ON_SURFACE)),
            ])
        ]

        # Data rows
        for hour in hours:
            data = hourly_data[hour]
            no2_str = f"{data['no2']:.2e}" if data.get("no2") is not None else "N/A"
            hcho_str = f"{data['hcho']:.2e}" if data.get("hcho") is not None else "N/A"

            rows.append(ft.DataRow(cells=[
                ft.DataCell(ft.Text(f"{hour:02d}:00 UTC", size=11, color=Colors.ON_SURFACE)),
                ft.DataCell(ft.Text(no2_str, size=11, font_family="monospace", color=ft.Colors.BLUE_800)),
                ft.DataCell(ft.Text(hcho_str, size=11, font_family="monospace", color=ft.Colors.GREEN_800)),
            ]))

        table = ft.DataTable(
            columns=[
                ft.DataColumn(ft.Text("")),
                ft.DataColumn(ft.Text("")),
                ft.DataColumn(ft.Text("")),
            ],
            rows=rows,
            border=ft.border.all(1, Colors.BORDER),
            border_radius=8,
            horizontal_lines=ft.BorderSide(1, Colors.BORDER),
        )

        return ft.Container(
            content=table,
            bgcolor=Colors.SURFACE,
            padding=12,
            border_radius=8,
            border=ft.border.all(1, Colors.BORDER),
        )
    
    def _build_hourly_trend_chart(self, hourly_data: dict) -> ft.Container:
        """Build a line chart showing hourly trends for NO2 and HCHO using Matplotlib."""
        if not hourly_data or plt is None:
            return ft.Container(content=ft.Text("No data or matplotlib missing"))

        hours = sorted(hourly_data.keys())
        if not hours:
            return ft.Container(content=ft.Text("No hourly data"))

        no2_vals = []
        hcho_vals = []
        valid_hours = []

        for h in hours:
            data = hourly_data[h]
            if data.get("no2") is not None and data.get("hcho") is not None:
                valid_hours.append(h)
                no2_vals.append(data["no2"])
                hcho_vals.append(data["hcho"])

        if not valid_hours:
            return ft.Container(content=ft.Text("No valid data for chart"))

        # Create plot
        fig, ax1 = plt.subplots(figsize=(10, 5))
        
        # Determine colors from theme if possible, or use standard
        color_no2 = '#1565C0'  # Blue 800
        color_hcho = '#2E7D32' # Green 800
        
        # Plot NO2
        ax1.set_xlabel('Hour (UTC)')
        ax1.set_ylabel('NO₂ (molec/cm²)', color=color_no2, fontweight='bold')
        line1, = ax1.plot(valid_hours, no2_vals, color=color_no2, marker='o', linewidth=2, label='NO₂')
        ax1.tick_params(axis='y', labelcolor=color_no2)
        ax1.grid(True, linestyle='--', alpha=0.3)
        
        # Plot HCHO on same axis for now (or dual axis if ranges differ substantially)
        # Using dual axis for better visibility since magnitudes might differ
        ax2 = ax1.twinx()
        ax2.set_ylabel('HCHO (molec/cm²)', color=color_hcho, fontweight='bold')
        line2, = ax2.plot(valid_hours, hcho_vals, color=color_hcho, marker='s', linewidth=2, label='HCHO')
        ax2.tick_params(axis='y', labelcolor=color_hcho)

        # Title and Layout
        plt.title('Hourly Average Trends', fontweight='bold')
        
        # Legend
        lines = [line1, line2]
        labels = [l.get_label() for l in lines]
        ax1.legend(lines, labels, loc='upper center', bbox_to_anchor=(0.5, -0.15), ncol=2)
        
        plt.tight_layout()

        # Save to buffer
        buf = io.BytesIO()
        plt.savefig(buf, format='png', dpi=100)
        plt.close(fig)
        buf.seek(0)
        img_b64 = base64.b64encode(buf.read()).decode()

        return ft.Container(
            content=ft.Image(
                src_base64=img_b64,
                width=800,
                height=400,
                fit=ft.ImageFit.CONTAIN,
            ),
            bgcolor=Colors.SURFACE,
            padding=10,
            border_radius=12,
            border=ft.border.all(1, Colors.BORDER),
            alignment=ft.Alignment(0, 0),
        )
    
    def _stats_row(self, label: str, value) -> ft.Row:
        """Create a stats row with formatted value."""
        if value is None:
            formatted = "N/A"
        elif isinstance(value, float):
            if abs(value) < 0.001 or abs(value) > 1000:
                formatted = f"{value:.2e}"
            else:
                formatted = f"{value:.4f}"
        else:
            formatted = str(value)
        
        return ft.Row([
            ft.Text(f"  {label}:", size=12, color=Colors.ON_SURFACE_VARIANT, width=60),
            ft.Text(formatted, size=12, color=Colors.ON_SURFACE, font_family="monospace"),
        ], spacing=8)
    
    def _compute_stats(self, file_path: str) -> dict:
        """Compute statistics from a NetCDF file."""
        try:
            with xr.open_dataset(file_path) as ds:
                stats = {}
                
                # Metadata
                stats["dims"] = dict(ds.sizes)
                stats["coords"] = list(ds.coords)
                stats["data_vars"] = list(ds.data_vars)
                
                # Grid dimensions
                if 'ROW' in ds.sizes and 'COL' in ds.sizes:
                    stats["grid_size"] = f"{ds.sizes['ROW']} × {ds.sizes['COL']}"
                else:
                    stats["grid_size"] = "N/A"
                
                # Available hours (handle case-insensitive)
                hours = []
                hour_dim = None
                for dim_name in ['hour', 'HOUR', 'Hour']:
                    if dim_name in ds.dims:
                        hour_dim = dim_name
                        break

                if hour_dim:
                    hours = sorted(ds[hour_dim].values.tolist())
                    stats["hours"] = ", ".join(f"{h:02d}:00" for h in hours)
                elif 'TSTEP' in ds.dims:
                    stats["hours"] = f"{len(ds['TSTEP'])} time steps"
                else:
                    stats["hours"] = "N/A"
                
                # Compute hourly data for charts
                hourly_data = {}
                coverage_by_hour = {}
                
                for var_name, stat_prefix in [('NO2_TropVCD', 'no2'), ('HCHO_TotVCD', 'hcho'), ('FNR', 'fnr')]:
                    if var_name in ds:
                        data = ds[var_name].values
                        # Filter valid values
                        valid_mask = np.isfinite(data) & (data > -1e30) & (data < 1e30)
                        valid = data[valid_mask]
                        
                        if len(valid) > 0:
                            stats[f"{stat_prefix}_min"] = float(np.min(valid))
                            stats[f"{stat_prefix}_max"] = float(np.max(valid))
                            stats[f"{stat_prefix}_mean"] = float(np.mean(valid))
                            stats[f"{stat_prefix}_std"] = float(np.std(valid))
                            stats[f"{stat_prefix}_valid_pct"] = 100.0 * len(valid) / data.size
                        
                        # Hourly averages
                        if hour_dim and hours:
                            for h in hours:
                                if h not in hourly_data:
                                    hourly_data[h] = {}

                                # Get data for this hour
                                try:
                                    hour_slice = ds[var_name].sel({hour_dim: h}).values
                                    hour_valid = hour_slice[np.isfinite(hour_slice) & (hour_slice > -1e30) & (hour_slice < 1e30)]
                                    if len(hour_valid) > 0:
                                        hourly_data[h][stat_prefix] = float(np.mean(hour_valid))


                                except Exception:
                                    pass
                
                stats["hourly_data"] = hourly_data
                stats["coverage_by_hour"] = coverage_by_hour
                
                return stats
        except Exception as e:
            print(f"Error computing stats: {e}")
            return None
