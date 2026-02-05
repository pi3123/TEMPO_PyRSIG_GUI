"""Create Dataset Page - Configure and download TEMPO data.

This page allows users to:
1. Select a geographic region (preset or custom)
2. Choose date range and time filters
3. Set quality filters
4. Download data with real-time progress updates
"""

import flet as ft
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Optional
import asyncio
import tempfile
import matplotlib
matplotlib.use('Agg')  # Non-interactive backend
import matplotlib.pyplot as plt

from ..theme import Colors, Spacing
from ..components.widgets import (
    HelpTooltip,
    LabeledField,
    SectionCard,
    StatusLogPanel,
    ProgressPanel,
    WorkerProgressPanel,
    DaySelector,
)
from ...storage.models import REGION_PRESETS, BoundingBox, Dataset, DatasetStatus, Granule
from ...storage.database import Database
from ...core.status import get_status_manager
from ...core.downloader import RSIGDownloader
from ...core.processor import DataProcessor
from ...core.config import ConfigManager

class UIStatusAdapter:
    """Adapts UI StatusLogPanel to core StatusManager interface."""
    def __init__(self, log_panel, progress_panel, worker_panel=None, total_granules: int = 0, num_workers: int = 4):
        self.log = log_panel
        self.progress = progress_panel
        self.worker_panel = worker_panel
        self.total = total_granules
        self.completed = 0
        self._worker_slot = 0  # Cycle through worker slots for display
        self._num_workers = num_workers
        
    def emit(self, event: str, message: str, value: Optional[float] = None):
        if event == "download":
            if value is not None:
                self.progress.update_progress(value, message)
                # Update worker panel overall progress  
                if self.worker_panel and self.total > 0:
                    self.completed = int(value * self.total)
                    self.worker_panel.update_overall(self.completed, self.total, f"Downloading...")
            
            # Update a worker slot with the current download message
            if self.worker_panel and message:
                # Extract meaningful part from message (remove emoji prefixes)
                clean_msg = message.lstrip("⬇️✅⚠️❌ ").strip()
                if clean_msg:
                    self.worker_panel.update_worker(self._worker_slot, clean_msg[:40], active=True)
                    self._worker_slot = (self._worker_slot + 1) % self._num_workers
            
            # Log progress events
            self.log.add_progress(message)
        elif event == "error":
            self.log.add_error(message)
        elif event == "info":
            self.log.add_info(message)
            if self.worker_panel:
                self.worker_panel.update_overall(self.completed, self.total, message)
        elif event == "ok":
            self.log.add_success(message)
            # Mark as complete
            if self.worker_panel:
                self.worker_panel.complete(self.completed, self.total)
        elif event == "warning":
            self.log.add_warning(message)


# =============================================================================
# Help Text Constants - Detailed explanations for every field
# =============================================================================

HELP_TEXTS = {
    "dataset_name": (
        "A unique, descriptive name for this dataset. This helps you identify it later.\n\n"
        "Examples:\n"
        "• 'July_Weekdays_SoCal' - July weekday data for Southern California\n"
        "• 'Houston_Summer_2024' - Summer 2024 data for Houston area\n\n"
        "Tip: Include the time period and region in the name for easy reference."
    ),
    
    "region_preset": (
        "Pre-defined geographic regions with optimized bounding boxes.\n\n"
        "Each preset includes:\n"
        "• Bounding box coordinates (West, South, East, North)\n"
        "• Region-appropriate road data for map overlays\n\n"
        "Select 'Custom Region' to enter your own coordinates."
    ),
    
    "bbox_west": (
        "Western boundary longitude in decimal degrees.\n\n"
        "• Negative values = West of Prime Meridian (Americas)\n"
        "• Positive values = East of Prime Meridian (Europe, Asia)\n\n"
        "Example: Los Angeles is at approximately -118.2°\n"
        "Valid range: -180 to 180"
    ),
    
    "bbox_south": (
        "Southern boundary latitude in decimal degrees.\n\n"
        "• Positive values = Northern Hemisphere\n"
        "• Negative values = Southern Hemisphere\n\n"
        "Example: San Diego is at approximately 32.7°\n"
        "Valid range: -90 to 90"
    ),
    
    "bbox_east": (
        "Eastern boundary longitude in decimal degrees.\n\n"
        "Must be greater than the Western boundary.\n\n"
        "Example: For Southern California, a typical value is -116.4°"
    ),
    
    "bbox_north": (
        "Northern boundary latitude in decimal degrees.\n\n"
        "Must be greater than the Southern boundary.\n\n"
        "Example: For Southern California, a typical value is 35.7°"
    ),
    
    "date_start": (
        "First date to include in the download (inclusive).\n\n"
        "TEMPO data availability:\n"
        "• Data begins from August 2023\n"
        "• Recent data may have a 1-2 day delay\n\n"
        "Format: Year-Month-Day (YYYY-MM-DD)"
    ),
    
    "date_end": (
        "Last date to include in the download (inclusive).\n\n"
        "Tip: Keep the date range reasonable (1-2 months) for faster downloads.\n"
        "You can always create additional datasets for other periods."
    ),
    
    "day_filter": (
        "Filter by day of week to analyze patterns.\n\n"
        "Common choices:\n"
        "• Weekdays Only (Mon-Fri): Compare to weekends for traffic analysis\n"
        "• Weekends Only (Sat-Sun): Lower traffic baseline\n"
        "• All Days: Complete dataset\n\n"
        "This is useful for studying how pollution varies by human activity patterns."
    ),
    
    "hour_start": (
        "Starting hour for daily data (UTC timezone).\n\n"
        "⚠️ IMPORTANT: All hours are in UTC (Coordinated Universal Time), not local time!\n\n"
        "UTC to Local Time Conversions:\n"
        "• UTC 16:00 = PST 8:00 AM (Pacific, winter)\n"
        "• UTC 16:00 = PDT 9:00 AM (Pacific, summer)\n"
        "• UTC 16:00 = EST 11:00 AM (Eastern, winter)\n"
        "• UTC 16:00 = EDT 12:00 PM (Eastern, summer)\n\n"
        "TEMPO observes North America roughly 8 AM - 6 PM local time.\n"
        "Typical UTC range: 13:00 - 23:00 (covers continental US daylight hours)"
    ),
    
    "hour_end": (
        "Ending hour for daily data (UTC timezone).\n\n"
        "Each hour between start and end (inclusive) will be downloaded separately.\n\n"
        "Example: Hours 16-20 UTC = 5 hourly observations per day\n\n"
        "More hours = larger download but better temporal resolution."
    ),
    
    "max_cloud": (
        "Maximum cloud fraction allowed (0.0 to 1.0).\n\n"
        "• 0.0 = Only completely clear pixels (very strict)\n"
        "• 0.3 = Up to 30% cloud cover (recommended)\n"
        "• 0.5 = Up to 50% cloud cover (more data, less quality)\n"
        "• 1.0 = No filtering (includes fully cloudy pixels)\n\n"
        "Cloud-covered pixels have unreliable NO₂/HCHO measurements.\n"
        "Recommended: 0.2 - 0.4 for good balance of quality and coverage."
    ),
    
    "max_sza": (
        "Maximum Solar Zenith Angle in degrees.\n\n"
        "Solar Zenith Angle (SZA) is the angle between the sun and vertical:\n"
        "• 0° = Sun directly overhead (noon at equator)\n"
        "• 90° = Sun at horizon (sunrise/sunset)\n\n"
        "High SZA means longer light path through atmosphere = less accurate retrieval.\n\n"
        "Recommended values:\n"
        "• 70° - Standard filter (excludes very early/late observations)\n"
        "• 60° - Stricter filter (higher quality)\n"
        "• 80° - Lenient filter (more data, lower quality)"
    ),
}


class CreatePage(ft.Container):
    """Create Dataset page with form inputs and download functionality."""
    
    def __init__(self, db: Database, config: ConfigManager = None):
        super().__init__()
        self.db = db
        self.config = config
        self.status = get_status_manager()
        self._is_downloading = False
        self._current_download_id: Optional[str] = None  # For global DownloadManager
        
        # Form state
        self._selected_preset: Optional[str] = None
        self._extend_mode = False  # True = extending existing dataset
        self._extend_dataset_id: Optional[str] = None  # ID of dataset being extended

        # Variable selection state
        self._available_variables = []  # Populated in _build
        self._selected_var_ids = set()  # Set of product IDs
        self._var_checkboxes = {}  # {product_id: ft.Checkbox}
        self._var_search_field = None  # Search text field

        # Build the page
        self._build()

    def did_mount(self):
        """Called when the control is added to the page."""
        # Add date pickers to overlay safely
        if hasattr(self, "_start_picker") and self._start_picker not in self.page.overlay:
            self.page.overlay.append(self._start_picker)
        if hasattr(self, "_end_picker") and self._end_picker not in self.page.overlay:
            self.page.overlay.append(self._end_picker)
        # Load datasets async
        self.page.run_task(self._load_datasets_async)
        # Generate initial map preview
        self.page.run_task(self._update_map_preview_async)

    async def _load_datasets_async(self):
        """Load datasets without blocking UI."""
        import asyncio
        datasets = await asyncio.to_thread(self.db.get_all_datasets)
        self._apply_datasets(datasets)
        self.update()

    def _apply_datasets(self, datasets: list):
        """Apply datasets to selector (no DB call)."""
        options = []
        for ds in datasets:
            label = f"{ds.name} ({ds.date_start} to {ds.date_end})"
            options.append(ft.DropdownOption(key=ds.id, text=label))
        self._dataset_selector.options = options
        if options:
            self._dataset_selector.value = options[0].key
    
    def _build(self):
        """Build the page layout."""
        # =====================================================================
        # Mode Toggle - New vs Extend Existing
        # =====================================================================
        self._mode_radio = ft.RadioGroup(
            value="new",
            on_change=self._on_mode_change,
            content=ft.Row([
                ft.Radio(value="new", label="New Dataset", label_style=ft.TextStyle(color=Colors.ON_SURFACE)),
                ft.Radio(value="extend", label="Extend Existing", label_style=ft.TextStyle(color=Colors.ON_SURFACE)),
            ], spacing=16),
        )
        
        # Dataset selector for extend mode
        self._dataset_selector = ft.Dropdown(
            label="Select Dataset",
            border_color=Colors.BORDER,
            focused_border_color=Colors.PRIMARY,
            bgcolor=Colors.SURFACE_VARIANT,
            text_style=ft.TextStyle(color=Colors.ON_SURFACE),
            width=300,
        )
        
        self._load_btn = ft.FilledTonalButton(
            content=ft.Row([
                ft.Icon(ft.Icons.DOWNLOAD, size=18, color=Colors.ON_SURFACE),
                ft.Text("Load", color=Colors.ON_SURFACE),
            ], spacing=6, tight=True),
            on_click=self._on_load_dataset_click,
        )
        
        self._extend_container = ft.Container(
            content=ft.Row([
                self._dataset_selector,
                self._load_btn,
            ], spacing=12),
            visible=False,  # Hidden by default
            padding=ft.padding.only(top=8),
        )
        
        mode_section = ft.Container(
            content=ft.Column([
                ft.Row([
                    ft.Text("Mode:", size=14, weight=ft.FontWeight.W_500, color=Colors.ON_SURFACE),
                    self._mode_radio,
                ], spacing=16, vertical_alignment=ft.CrossAxisAlignment.CENTER),
                self._extend_container,
            ], spacing=4),
            padding=ft.padding.only(bottom=8),
        )
        
        # =====================================================================
        # Dataset Name Section
        # =====================================================================
        self._name_field = ft.TextField(
            hint_text="e.g., July_Weekdays_SoCal",
            border_color=Colors.BORDER,
            focused_border_color=Colors.PRIMARY,
            bgcolor=Colors.SURFACE_VARIANT,
            text_style=ft.TextStyle(color=Colors.ON_SURFACE),
            hint_style=ft.TextStyle(color=Colors.ON_SURFACE_VARIANT),
        )
        
        name_section = LabeledField(
            label="Dataset Name",
            field=self._name_field,
            help_text=HELP_TEXTS["dataset_name"],
            required=True,
        )
        
        # =====================================================================
        # Geographic Region Section - Simple Range Inputs
        # =====================================================================
        preset_options = [
            ft.DropdownOption(key="custom", text="Custom Region"),
        ]
        for name, (bbox, _) in REGION_PRESETS.items():
            preset_options.append(ft.DropdownOption(key=name, text=name))
        
        self._region_dropdown = ft.Dropdown(
            options=preset_options,
            value="Southern California",
            border_color=Colors.BORDER,
            focused_border_color=Colors.PRIMARY,
            bgcolor=Colors.SURFACE_VARIANT,
            text_style=ft.TextStyle(color=Colors.ON_SURFACE),
            on_select=self._on_region_change,
            width=220,
        )
        
        # Store bbox values
        self._bbox = [-119.68, 32.23, -116.38, 35.73]  # west, south, east, north
        
        # Custom coordinate inputs (shown when "Custom" selected)
        self._west_field = ft.TextField(value="-119.68", width=90, label="West", dense=True,
            border_color=Colors.BORDER, bgcolor=Colors.SURFACE_VARIANT,
            text_style=ft.TextStyle(color=Colors.ON_SURFACE, size=13))
        self._east_field = ft.TextField(value="-116.38", width=90, label="East", dense=True,
            border_color=Colors.BORDER, bgcolor=Colors.SURFACE_VARIANT,
            text_style=ft.TextStyle(color=Colors.ON_SURFACE, size=13))
        self._south_field = ft.TextField(value="32.23", width=90, label="South", dense=True,
            border_color=Colors.BORDER, bgcolor=Colors.SURFACE_VARIANT,
            text_style=ft.TextStyle(color=Colors.ON_SURFACE, size=13))
        self._north_field = ft.TextField(value="35.73", width=90, label="North", dense=True,
            border_color=Colors.BORDER, bgcolor=Colors.SURFACE_VARIANT,
            text_style=ft.TextStyle(color=Colors.ON_SURFACE, size=13))
        
        self._custom_fields = ft.Container(
            content=ft.Column([
                ft.Row([
                    ft.Text("Lon:", size=12, color=Colors.ON_SURFACE_VARIANT, width=30),
                    self._west_field, ft.Text("to", size=12, color=Colors.ON_SURFACE_VARIANT), self._east_field,
                ], spacing=8),
                ft.Row([
                    ft.Text("Lat:", size=12, color=Colors.ON_SURFACE_VARIANT, width=30),
                    self._south_field, ft.Text("to", size=12, color=Colors.ON_SURFACE_VARIANT), self._north_field,
                ], spacing=8),
            ], spacing=8),
            visible=False,
            padding=ft.padding.only(top=8),
        )
        
        # Coordinate display (shown when preset selected)
        self._coord_display = ft.Container(
            content=ft.Column([
                ft.Text(f"Longitude: {self._bbox[0]:.2f}° to {self._bbox[2]:.2f}°", size=13, color=Colors.ON_SURFACE),
                ft.Text(f"Latitude: {self._bbox[1]:.2f}° to {self._bbox[3]:.2f}°", size=13, color=Colors.ON_SURFACE),
            ], spacing=4),
            visible=True,
        )
        
        region_section = SectionCard(
            title="Geographic Region",
            icon=ft.Icons.MAP,
            help_text="Select a preset region or enter custom coordinates.",
            content=ft.Column([
                ft.Row([
                    ft.Text("Region:", size=14, color=Colors.ON_SURFACE),
                    self._region_dropdown,
                ], spacing=12),
                self._custom_fields,
                self._coord_display,
            ], spacing=6),
        )


        
        # =====================================================================
        # Temporal Selection Section
        # =====================================================================
        today = date.today()
        default_start = today - timedelta(days=30)
        
        self._start_date = default_start
        self._end_date = today
        
        # Date picker buttons (click to open calendar)
        self._date_start_label = ft.Text(default_start.strftime("%b %d, %Y"), color=Colors.ON_SURFACE)
        self._date_start_btn = ft.OutlinedButton(
            content=ft.Row(
                [
                    ft.Icon(ft.Icons.CALENDAR_MONTH, color=Colors.ON_SURFACE),
                    self._date_start_label,
                ],
                alignment=ft.MainAxisAlignment.CENTER,
                spacing=8,
            ),
            on_click=self._open_start_picker,
            style=ft.ButtonStyle(
                shape=ft.RoundedRectangleBorder(radius=8),
            ),
        )

        self._date_end_label = ft.Text(today.strftime("%b %d, %Y"), color=Colors.ON_SURFACE)
        self._date_end_btn = ft.OutlinedButton(
             content=ft.Row(
                [
                    ft.Icon(ft.Icons.CALENDAR_MONTH, color=Colors.ON_SURFACE),
                    self._date_end_label,
                ],
                alignment=ft.MainAxisAlignment.CENTER,
                spacing=8,
            ),
            on_click=self._open_end_picker,
            style=ft.ButtonStyle(
                shape=ft.RoundedRectangleBorder(radius=8),
            ),
        )
        
        # Date pickers
        self._start_picker = ft.DatePicker(
            first_date=date(2023, 8, 1),  # TEMPO data start
            last_date=today,
            on_change=self._on_start_date_change,
        )
        
        self._end_picker = ft.DatePicker(
            first_date=date(2023, 8, 1),
            last_date=today,
            on_change=self._on_end_date_change,
        )
        
        # Day selector - toggle chips
        self._day_selector = DaySelector(value=[0, 1, 2, 3, 4], on_change=self._on_day_change)
        
        # Quick preset buttons for day selection
        day_presets = ft.Row([
            ft.TextButton(
                content=ft.Text("Weekdays", color=Colors.ON_SURFACE),
                on_click=lambda e: self._day_selector.select_weekdays()
            ),
            ft.TextButton(
                content=ft.Text("Weekends", color=Colors.ON_SURFACE),
                on_click=lambda e: self._day_selector.select_weekends()
            ),
            ft.TextButton(
                content=ft.Text("All", color=Colors.ON_SURFACE),
                on_click=lambda e: self._day_selector.select_all()
            ),
        ], spacing=4)
        
        self._hour_start = ft.Dropdown(
            options=[ft.DropdownOption(str(h), f"{h:02d}:00 UTC") for h in range(24)],
            value="16",
            width=130,
            border_color=Colors.BORDER,
            focused_border_color=Colors.PRIMARY,
            bgcolor=Colors.SURFACE_VARIANT,
            text_style=ft.TextStyle(color=Colors.ON_SURFACE),
        )
        self._hour_start.on_change = self._on_hour_change
        
        self._hour_end = ft.Dropdown(
            options=[ft.DropdownOption(str(h), f"{h:02d}:00 UTC") for h in range(24)],
            value="20",
            width=130,
            border_color=Colors.BORDER,
            focused_border_color=Colors.PRIMARY,
            bgcolor=Colors.SURFACE_VARIANT,
            text_style=ft.TextStyle(color=Colors.ON_SURFACE),
        )
        self._hour_end.on_change = self._on_hour_change
        
        # UTC timezone warning
        utc_warning = ft.Container(
            content=ft.Row([
                ft.Icon(ft.Icons.INFO, size=16, color=Colors.WARNING),
                ft.Text(
                    "⏰ Hours are in UTC (Coordinated Universal Time), not local time! "
                    "UTC 16:00 = 8 AM PST / 11 AM EST",
                    size=12,
                    color=Colors.WARNING,
                    italic=True,
                ),
            ], spacing=8),
            bgcolor=Colors.WARNING_CONTAINER,
            border_radius=8,
            padding=10,
            margin=ft.margin.only(top=8),
        )
        
        temporal_section = SectionCard(
            title="Time Selection",
            icon=ft.Icons.CALENDAR_MONTH,
            help_text=(
                "Select the date range and hours to download.\n\n"
                "TEMPO satellite scans North America hourly during daylight hours, "
                "typically from ~13:00 to ~23:00 UTC (covering sunrise to sunset across the continent).\n\n"
                "Each unique combination of date + hour is called a 'granule'."
            ),
            content=ft.Column([
                # Date range row
                ft.Row([
                    ft.Column([
                        ft.Row([
                            ft.Text("Start Date", size=14, weight=ft.FontWeight.W_500, color=Colors.ON_SURFACE),
                            HelpTooltip(HELP_TEXTS["date_start"]),
                        ]),
                        self._date_start_btn,
                    ], spacing=6),
                    ft.Text("to", color=Colors.ON_SURFACE_VARIANT),
                    ft.Column([
                        ft.Row([
                            ft.Text("End Date", size=14, weight=ft.FontWeight.W_500, color=Colors.ON_SURFACE),
                            HelpTooltip(HELP_TEXTS["date_end"]),
                        ]),
                        self._date_end_btn,
                    ], spacing=6),
                ], spacing=16, vertical_alignment=ft.CrossAxisAlignment.END),
                
                # Day filter with toggle chips
                ft.Column([
                    ft.Row([
                        ft.Text("Days of Week", size=14, weight=ft.FontWeight.W_500, color=Colors.ON_SURFACE),
                        HelpTooltip(HELP_TEXTS["day_filter"]),
                    ]),
                    ft.Row([
                        self._day_selector,
                        ft.Container(width=16),
                        day_presets,
                    ]),
                ], spacing=6),
                
                ft.Row([
                    LabeledField("Hour Start", self._hour_start, HELP_TEXTS["hour_start"]),
                    ft.Text("to", color=Colors.ON_SURFACE_VARIANT),
                    LabeledField("Hour End", self._hour_end, HELP_TEXTS["hour_end"]),
                ], spacing=16, vertical_alignment=ft.CrossAxisAlignment.END),
                
                utc_warning,
            ], spacing=12),
        )
        
        # =====================================================================
        # Quality Filters Section
        # =====================================================================
        self._max_cloud = ft.Slider(
            min=0,
            max=1,
            value=0.3,
            divisions=20,
            label="{value}",
            active_color=Colors.PRIMARY,
            inactive_color=Colors.SURFACE_VARIANT,
            on_change=self._on_cloud_change,
        )
        
        self._cloud_label = ft.Text(
            "0.30 (30% cloud cover)",
            size=13,
            color=Colors.ON_SURFACE_VARIANT,
        )
        
        self._max_sza = ft.Slider(
            min=30,
            max=90,
            value=70,
            divisions=12,
            label="{value}°",
            active_color=Colors.PRIMARY,
            inactive_color=Colors.SURFACE_VARIANT,
            on_change=self._on_sza_change,
        )
        
        self._sza_label = ft.Text(
            "70° (excludes very early/late observations)",
            size=13,
            color=Colors.ON_SURFACE_VARIANT,
        )
        
        filter_section = SectionCard(
            title="Quality Filters",
            icon=ft.Icons.FILTER_ALT,
            help_text=(
                "Quality filters remove unreliable data points.\n\n"
                "Stricter filters (lower values) = higher quality but less data.\n"
                "Lenient filters (higher values) = more data but lower quality.\n\n"
                "The defaults are good starting points for most analyses."
            ),
            content=ft.Column([
                ft.Column([
                    ft.Row([
                        ft.Text("Max Cloud Fraction", size=14, color=Colors.ON_SURFACE),
                        HelpTooltip(HELP_TEXTS["max_cloud"]),
                    ]),
                    self._max_cloud,
                    self._cloud_label,
                ], spacing=4),
                
                ft.Container(height=8),
                
                ft.Column([
                    ft.Row([
                        ft.Text("Max Solar Zenith Angle", size=14, color=Colors.ON_SURFACE),
                        HelpTooltip(HELP_TEXTS["max_sza"]),
                    ]),
                    self._max_sza,
                    self._sza_label,
                ], spacing=4),
            ], spacing=8),
        )

        # =====================================================================
        # Variable Selection Section (NEW - DYNAMIC TEMPO VARIABLES)
        # =====================================================================
        from ...core.variable_registry import VariableRegistry

        # Discover available variables
        self._available_variables = VariableRegistry.discover_variables()

        # Default to legacy 3 variables
        self._selected_var_ids = set(VariableRegistry.get_default_variables())

        # Search box
        self._var_search_field = ft.TextField(
            label="Search variables",
            prefix_icon=ft.Icons.SEARCH,
            border_color=Colors.BORDER,
            focused_border_color=Colors.PRIMARY,
            bgcolor=Colors.SURFACE_VARIANT,
            text_style=ft.TextStyle(color=Colors.ON_SURFACE, size=13),
            hint_style=ft.TextStyle(color=Colors.ON_SURFACE_VARIANT),
            on_change=self._on_var_search_change,
            dense=True,
        )

        # Preset buttons
        preset_buttons = ft.Row([
            ft.TextButton(
                content=ft.Row([
                    ft.Icon(ft.Icons.CHECK_CIRCLE_OUTLINE, size=16, color=Colors.ON_SURFACE),
                    ft.Text("Default (NO₂, HCHO, O₃)", size=12, color=Colors.ON_SURFACE),
                ], spacing=4, tight=True),
                on_click=lambda _: self._on_var_preset_click("default"),
                style=ft.ButtonStyle(padding=ft.padding.symmetric(horizontal=8, vertical=4)),
            ),
            ft.TextButton(
                content=ft.Row([
                    ft.Icon(ft.Icons.SCIENCE, size=16, color=Colors.ON_SURFACE),
                    ft.Text("All Trace Gases", size=12, color=Colors.ON_SURFACE),
                ], spacing=4, tight=True),
                on_click=lambda _: self._on_var_preset_click("trace_gases"),
                style=ft.ButtonStyle(padding=ft.padding.symmetric(horizontal=8, vertical=4)),
            ),
            ft.TextButton(
                content=ft.Row([
                    ft.Icon(ft.Icons.CLEAR_ALL, size=16, color=Colors.ON_SURFACE),
                    ft.Text("Clear All", size=12, color=Colors.ON_SURFACE),
                ], spacing=4, tight=True),
                on_click=lambda _: self._on_var_preset_click("clear"),
                style=ft.ButtonStyle(padding=ft.padding.symmetric(horizontal=8, vertical=4)),
            ),
        ], spacing=8, wrap=True)

        # Group variables by category
        grouped_vars = VariableRegistry.group_by_category(self._available_variables)

        # Create checkboxes grouped by category
        var_checkbox_groups = []
        for category, variables in grouped_vars.items():
            # Category header
            category_header = ft.Text(
                category,
                size=13,
                weight=ft.FontWeight.W_600,
                color=Colors.PRIMARY,
            )

            # Variable checkboxes
            var_checkboxes = []
            for var in variables:
                is_selected = var.product_id in self._selected_var_ids

                checkbox = ft.Checkbox(
                    label=var.display_name,
                    value=is_selected,
                    on_change=lambda e, pid=var.product_id: self._on_var_toggle(pid, e.control.value),
                    label_style=ft.TextStyle(color=Colors.ON_SURFACE),
                )

                # Store checkbox reference
                self._var_checkboxes[var.product_id] = checkbox

                # Create row with checkbox and tooltip
                var_row = ft.Row([
                    checkbox,
                    HelpTooltip(f"{var.description}\nUnit: {var.unit}" if var.unit else var.description),
                ], spacing=4, tight=True)

                var_checkboxes.append(var_row)

            var_checkbox_groups.append(ft.Column([
                category_header,
                *var_checkboxes,
                ft.Container(height=8),
            ], spacing=4))

        # Scrollable variable list
        var_list_scrollable = ft.Container(
            content=ft.Column(var_checkbox_groups, spacing=0, scroll=ft.ScrollMode.AUTO),
            height=300,
            border=ft.Border(
                left=ft.BorderSide(1, Colors.BORDER),
                top=ft.BorderSide(1, Colors.BORDER),
                right=ft.BorderSide(1, Colors.BORDER),
                bottom=ft.BorderSide(1, Colors.BORDER),
            ),
            border_radius=8,
            padding=12,
            bgcolor=Colors.SURFACE,
        )

        # Validation error message
        self._var_validation_error = ft.Text(
            "⚠️ At least 1 variable must be selected",
            color=Colors.ERROR,
            size=12,
            visible=False,
        )

        variable_section = SectionCard(
            title="Variables to Download",
            icon=ft.Icons.SCIENCE,
            help_text=(
                "Select which TEMPO data variables to download.\n\n"
                "TEMPO provides measurements of trace gases, aerosols, and clouds. "
                "The default selection (NO₂, HCHO, O₃) is recommended for air quality analysis.\n\n"
                "Tips:\n"
                "• More variables = longer download times\n"
                "• Variables can be filtered per analysis after download\n"
                "• Some derived metrics (like FNR) require specific variables"
            ),
            content=ft.Column([
                self._var_search_field,
                ft.Container(height=4),
                preset_buttons,
                ft.Container(height=8),
                var_list_scrollable,
                ft.Container(height=4),
                self._var_validation_error,
            ], spacing=0),
        )

        # =====================================================================
        # Download Button and Progress
        # =====================================================================
        self._download_btn = ft.FilledButton(
            content=ft.Row([
                ft.Icon(ft.Icons.CLOUD_DOWNLOAD, size=20, color=Colors.ON_PRIMARY),
                ft.Text("Download & Create Dataset", color=Colors.ON_PRIMARY),
            ], spacing=8, tight=True),
            style=ft.ButtonStyle(
                padding=ft.padding.symmetric(horizontal=24, vertical=12),
            ),
            on_click=self._on_download_click,
        )

        self._cancel_btn = ft.OutlinedButton(
            content=ft.Row([
                ft.Icon(ft.Icons.CANCEL, size=20, color=Colors.ON_SURFACE),
                ft.Text("Cancel", color=Colors.ON_SURFACE),
            ], spacing=8, tight=True),
            visible=False,
            on_click=self._on_cancel_click,
        )
        
        self._progress_panel = ProgressPanel()
        # Use configured number of workers (default 4 if config not available)
        num_workers = self.config.download_workers if self.config else 4
        self._worker_progress = WorkerProgressPanel(num_workers=num_workers)  # For right column
        self._status_log = StatusLogPanel()
        
        # =====================================================================
        # Dataset Preview / Summary
        # =====================================================================
        self._preview_text = ft.Text(
            "",
            size=13,
            color=Colors.ON_SURFACE_VARIANT,
        )
        
        preview_section = ft.Container(
            content=ft.Column([
                ft.Row([
                    ft.Icon(ft.Icons.PREVIEW, size=18, color=Colors.PRIMARY),
                    ft.Text("Dataset Preview", size=14, weight=ft.FontWeight.W_600, color=Colors.ON_SURFACE),
                    HelpTooltip(
                        "A summary of what will be downloaded based on your current settings.\n\n"
                        "Check this before clicking Download to ensure the dataset is reasonable in size."
                    ),
                ], spacing=8),
                self._preview_text,
            ], spacing=8),
            bgcolor=Colors.SURFACE,
            border_radius=12,
            border=ft.Border(
                left=ft.BorderSide(1, Colors.BORDER),
                top=ft.BorderSide(1, Colors.BORDER),
                right=ft.BorderSide(1, Colors.BORDER),
                bottom=ft.BorderSide(1, Colors.BORDER),
            ),
            padding=16,
        )
        

        
        # =====================================================================
        # Map Preview Section (Right Panel)
        # =====================================================================
        # Initialize map controls
        # Use 1x1 transparent pixel to avoid "valid src must be specified" error
        self._map_image_control = ft.Image(
            src="data:image/gif;base64,R0lGODlhAQABAIAAAAAAAP///yH5BAEAAAAALAAAAAABAAEAAAIBRAA7",
            fit="contain", 
            gapless_playback=True
        )
        
        self._map_progress = ft.ProgressBar(width=None, visible=False, color=Colors.PRIMARY, height=2)
        
        self._map_preview = ft.Container(
            content=ft.Column([
                ft.Icon(ft.Icons.MAP_OUTLINED, size=64, color=Colors.ON_SURFACE_VARIANT),
                ft.Text("Region Preview", size=16, weight=ft.FontWeight.W_500, color=Colors.ON_SURFACE),
                ft.Text("Select a region to see it on the map", size=12, color=Colors.ON_SURFACE_VARIANT),
            ], horizontal_alignment=ft.CrossAxisAlignment.CENTER, alignment=ft.MainAxisAlignment.CENTER),
            bgcolor=Colors.SURFACE_VARIANT,
            border_radius=12,
            alignment=ft.Alignment(0, 0),
            expand=True,
        )
        
        # Coordinate display for map preview
        self._map_coord_label = ft.Text(
            "W: -119.68°  S: 32.23°  E: -116.38°  N: 35.73°",
            size=12,
            color=Colors.ON_SURFACE_VARIANT,
            text_align=ft.TextAlign.CENTER,
        )
        
        map_preview_panel = ft.Container(
            content=ft.Column([
                ft.Row([
                    ft.Text("Region Preview", size=16, weight=ft.FontWeight.W_600, color=Colors.ON_SURFACE),
                    ft.Container(expand=True),
                    ft.IconButton(
                        icon=ft.Icons.SAVE_ALT,
                        icon_color=Colors.ON_SURFACE,
                        tooltip="Save Image",
                        on_click=self._on_save_map_click,
                    ),
                    ft.IconButton(
                        icon=ft.Icons.REFRESH,
                        icon_color=Colors.ON_SURFACE,
                        tooltip="Refresh map preview",
                        on_click=self._on_refresh_map_click,
                    ),
                ], spacing=8),
                ft.Container(height=8),

                self._map_progress,
                # Map Preview (Static)
                ft.Container(
                    content=self._map_image_control,
                    bgcolor=Colors.SURFACE_VARIANT,
                    border_radius=12,
                    alignment=ft.Alignment(0, 0),
                    clip_behavior=ft.ClipBehavior.HARD_EDGE,
                    expand=True,
                ),
                
                ft.Container(height=8),
                self._map_coord_label,
            ], expand=True),
            padding=16,
            expand=True,
        )
        
        # =====================================================================
        # Page Layout - Left (Form 40%) / Right (Map 60%)
        # =====================================================================
        
        # Back button for header
        back_btn = ft.TextButton(
            content=ft.Row([
                ft.Icon(ft.Icons.ARROW_BACK, size=18, color=Colors.ON_SURFACE),
                ft.Text("Back to Library", color=Colors.ON_SURFACE),
            ], spacing=6, tight=True),
            on_click=self._on_back_click,
        )
        
        left_column = ft.Column(
            controls=[
                # Header with back button
                ft.Row([
                    back_btn,
                    ft.Container(width=16),
                    ft.Text("📊 New Dataset", size=24, weight=ft.FontWeight.BOLD, color=Colors.ON_SURFACE),
                    HelpTooltip(
                        "Create a new dataset by downloading TEMPO satellite data.\n\n"
                        "TEMPO (Tropospheric Emissions: Monitoring of Pollution) is a NASA "
                        "satellite instrument that measures air quality over North America "
                        "every hour during daylight.\n\n"
                        "It provides:\n"
                        "• NO₂ (Nitrogen Dioxide) - from vehicles, power plants\n"
                        "• HCHO (Formaldehyde) - from vegetation, fires, industry\n"
                        "• FNR (HCHO/NO₂ ratio) - indicates VOC vs NOx sensitivity"
                    ),
                ], spacing=8, vertical_alignment=ft.CrossAxisAlignment.CENTER),
                
                ft.Divider(height=20, color=Colors.DIVIDER),
                
                mode_section,
                name_section,
                ft.Container(height=8),
                region_section,
                ft.Container(height=8),
                temporal_section,
                ft.Container(height=8),
                filter_section,
                ft.Container(height=8),
                variable_section,
                ft.Container(height=16),

                # Download controls
                ft.Row([
                    self._download_btn,
                    self._cancel_btn,
                ], spacing=12),
                
                self._progress_panel,
            ],
            scroll=ft.ScrollMode.AUTO,
            spacing=0,
            expand=True,
        )
        
        right_column = ft.Column(
            controls=[
                map_preview_panel,
                ft.Container(height=16),
                self._worker_progress,  # Download progress
                self._status_log,
            ],
            expand=True,
        )
        
        # Main content - 40/60 split
        self.content = ft.Row(
            controls=[
                ft.Container(content=left_column, expand=2, padding=ft.padding.only(right=16)),
                ft.VerticalDivider(width=1, color=Colors.DIVIDER),
                ft.Container(content=right_column, expand=3),
            ],
            expand=True,
            spacing=0,
        )
        
        self.expand = True
        self.padding = Spacing.PAGE_HORIZONTAL
        
        # Initial preview/estimation update
        self._update_preview()
        self._update_map_coord_label()
    
    def _get_shell(self):
        """Get the AppShell from the page."""
        if self.page and self.page.controls:
            return self.page.controls[0]
        return None
    
    def _get_download_manager(self):
        """Get the global DownloadManager from the shell."""
        shell = self._get_shell()
        if shell and hasattr(shell, 'download_manager'):
            return shell.download_manager
        return None
    
    def _on_back_click(self, e):
        """Navigate back to Library."""
        shell = self._get_shell()
        if shell and hasattr(shell, 'navigate_to'):
            shell.navigate_to("/library")
    
    def _on_refresh_map_click(self, e):
        """Manually refresh the map preview."""
        self._update_map_coord_label()
        if self.page:
            self.page.run_task(self._update_map_preview_async)
    
    def _update_map_coord_label(self):
        """Update the map coordinate label based on current bbox."""
        try:
            if hasattr(self, '_map_coord_label'):
                self._map_coord_label.value = f"W: {self._bbox[0]:.2f}°  S: {self._bbox[1]:.2f}°  E: {self._bbox[2]:.2f}°  N: {self._bbox[3]:.2f}°"
        except Exception:
            pass

    
    def _generate_map_preview(self, bbox=None, detailed=False):
        """Generate a lightweight map preview showing the bbox region using cartopy."""
        try:
            import cartopy.crs as ccrs
            import cartopy.feature as cfeature
            import cartopy.io.shapereader as shapereader
            import time
            
            # Use passed bbox or current state
            west, south, east, north = bbox if bbox else self._bbox
            
            # Create figure with cartopy projection - higher res for dynamic sizing
            fig = plt.figure(figsize=(10, 8), dpi=150)
            ax = fig.add_subplot(1, 1, 1, projection=ccrs.PlateCarree())
            
            # Set extent with some padding
            pad_lon = (east - west) * 0.15
            pad_lat = (north - south) * 0.15
            ax.set_extent([
                west - pad_lon, east + pad_lon,
                south - pad_lat, north + pad_lat
            ], crs=ccrs.PlateCarree())
            
            # Add geographic features
            ax.add_feature(cfeature.LAND, facecolor='#f8f9fa')
            ax.add_feature(cfeature.OCEAN, facecolor='#e3f2fd')
            ax.add_feature(cfeature.COASTLINE, linewidth=0.8, edgecolor='#37474f')
            ax.add_feature(cfeature.BORDERS, linewidth=1.0, edgecolor='#263238', linestyle='-')
            ax.add_feature(cfeature.STATES, linewidth=0.8, edgecolor='#546e7a', linestyle='-')
            
            # Add detailed highways (Natural Earth roads) if requested
            if detailed:
                try:
                    roads_10m = shapereader.natural_earth(
                        resolution='10m',
                        category='cultural',
                        name='roads'
                    )
                    reader = shapereader.Reader(roads_10m)
                    
                    # Filter and plot roads
                    for record in reader.records():
                        road_type = record.attributes.get('type', '')
                        
                        # Filter for map detail
                        # Interstate: Black, thick, top priority
                        # Major/Beltway: Dark Grey, medium
                        # Others: Light Grey, thin
                        
                        linewidth = 0.5
                        color = '#9e9e9e'
                        zorder = 3
                        
                        if road_type == 'Interstate':
                            linewidth = 1.2
                            color = '#000000' # Dark Black
                            zorder = 5
                        elif road_type in ['Major Highway', 'Beltway']:
                            linewidth = 1.0
                            color = '#424242' # Dark Grey
                            zorder = 4
                        else:
                            # Keep little roads but grey
                            linewidth = 0.6
                            color = '#757575' # Grey
                            zorder = 3
                            
                        # Check intersection with view bounds (optimization)
                        geom = record.geometry
                        bounds = geom.bounds
                        # Simple bounds check with padding
                        if (bounds[2] < west - pad_lon or bounds[0] > east + pad_lon or 
                            bounds[3] < south - pad_lat or bounds[1] > north + pad_lat):
                            continue
                            
                        ax.add_geometries(
                            [geom],
                            ccrs.PlateCarree(),
                            facecolor='none',
                            edgecolor=color,
                            linewidth=linewidth,
                            alpha=0.7,
                            zorder=zorder
                        )
                except Exception as e:
                    print(f"Road overlay warning: {e}")
                    
                except Exception as e:
                    print(f"Road overlay warning: {e}")
                    
                # Add top 5 major cities
                try:
                    cities_shp = shapereader.natural_earth(
                        resolution='10m',
                        category='cultural',
                        name='populated_places'
                    )
                    reader = shapereader.Reader(cities_shp)
                    
                    valid_cities = []
                    
                    for record in reader.records():
                        # Get attributes
                        name = record.attributes.get('NAME', '')
                        pop_max = record.attributes.get('POP_MAX', 0)
                        
                        # Use lower threshold to gather candidates, then sort
                        if pop_max < 50000:
                            continue
                            
                        geom = record.geometry
                        x = geom.x
                        y = geom.y
                        
                        # Check bounds (strict to view)
                        if (x < west - pad_lon or x > east + pad_lon or 
                            y < south - pad_lat or y > north + pad_lat):
                            continue
                            
                        valid_cities.append((pop_max, name, x, y))
                    
                    # Sort by population descending and take top 5
                    valid_cities.sort(key=lambda x: x[0], reverse=True)
                    top_cities = valid_cities[:5]
                    
                    for pop, name, x, y in top_cities:
                        # Plot marker
                        ax.plot(x, y, marker='o', color='black', markersize=4, 
                               transform=ccrs.PlateCarree(), zorder=10)
                        
                        # Use simple right alignment for all labels as requested
                        txt_x = x + 0.05
                        txt_y = y + 0.02
                        ha = 'left'
                        
                        # Plot label with halo
                        text = ax.text(txt_x, txt_y, name,
                                     fontsize=9, fontweight='bold', color='#212121',
                                     ha=ha, va='bottom',
                                     transform=ccrs.PlateCarree(), zorder=11)
                                     
                        import matplotlib.patheffects as path_effects
                        text.set_path_effects([
                            path_effects.withStroke(linewidth=2, foreground='white', alpha=0.8)
                        ])
                        
                except Exception as e:
                    print(f"City overlay warning: {e}")
            
            # Draw the bbox rectangle
            from matplotlib.patches import Rectangle
            rect = Rectangle(
                (west, south), east - west, north - south,
                linewidth=2, edgecolor='#6200EE', facecolor='#6200EE',
                alpha=0.2, transform=ccrs.PlateCarree(),
                zorder=6
            )
            ax.add_patch(rect)
            
            # Draw bbox outline
            ax.plot([west, east, east, west, west],
                    [south, south, north, north, south],
                    color='#6200EE', linewidth=2, transform=ccrs.PlateCarree(), zorder=6)
            
            # Add gridlines
            gl = ax.gridlines(draw_labels=True, linewidth=0.3, color='gray', alpha=0.5, zorder=7)
            gl.top_labels = False
            gl.right_labels = False
            gl.xlabel_style = {'size': 8}
            gl.ylabel_style = {'size': 8}
            
            plt.tight_layout()
            
            # Save to temp file with unique name to bust cache
            temp_dir = Path(tempfile.gettempdir()) / "tempo_app"
            temp_dir.mkdir(exist_ok=True)
            timestamp = int(time.time() * 1000)
            preview_path = temp_dir / f"region_preview_{timestamp}.png"
            fig.savefig(preview_path, dpi=100, bbox_inches='tight', 
                       facecolor='white', edgecolor='none')
            plt.close(fig)
            
            return str(preview_path)
            
        except Exception as e:
            # Return error message as string
            import traceback
            traceback.print_exc()
            return f"Error: {e}"
    
    def _on_mode_change(self, e):
        """Handle mode toggle change."""
        selected = e.control.value if e.control.value else "new"
        self._extend_mode = (selected == "extend")
        
        # Show/hide extend container
        self._extend_container.visible = self._extend_mode
        
        # Update name field state
        if self._extend_mode:
            self._name_field.read_only = True
            self._name_field.hint_text = "Select a dataset to load"
            # Refresh dataset options in case new ones were created
            self.page.run_task(self._load_datasets_async)
        else:
            self._name_field.read_only = False
            self._name_field.hint_text = "e.g., July_Weekdays_SoCal"
            self._name_field.value = ""
            self._extend_dataset_id = None
        
        self.update()
    
    def _on_load_dataset_click(self, e):
        """Load selected dataset configuration into the form."""
        selected_id = self._dataset_selector.value
        if not selected_id:
            self._status_log.add_error("Please select a dataset to load")
            return
        
        dataset = self.db.get_dataset(selected_id)
        if not dataset:
            self._status_log.add_error("Dataset not found")
            return
        
        self._extend_dataset_id = dataset.id
        
        # Populate name field
        self._name_field.value = dataset.name
        
        # Populate bbox - find matching preset or set custom
        preset_found = False
        for preset_name, (preset_bbox, _) in REGION_PRESETS.items():
            if (abs(preset_bbox.west - dataset.bbox.west) < 0.01 and
                abs(preset_bbox.south - dataset.bbox.south) < 0.01 and
                abs(preset_bbox.east - dataset.bbox.east) < 0.01 and
                abs(preset_bbox.north - dataset.bbox.north) < 0.01):
                self._region_dropdown.value = preset_name
                preset_found = True
                break
        
        if not preset_found:
            self._region_dropdown.value = "custom"
            self._west_field.value = str(dataset.bbox.west)
            self._south_field.value = str(dataset.bbox.south)
            self._east_field.value = str(dataset.bbox.east)
            self._north_field.value = str(dataset.bbox.north)
            self._custom_fields.visible = True
            self._coord_display.visible = False
        else:
            self._bbox = [dataset.bbox.west, dataset.bbox.south, dataset.bbox.east, dataset.bbox.north]
            self._coord_display.content.controls[0].value = f"Longitude: {dataset.bbox.west:.2f}° to {dataset.bbox.east:.2f}°"
            self._coord_display.content.controls[1].value = f"Latitude: {dataset.bbox.south:.2f}° to {dataset.bbox.north:.2f}°"
            self._custom_fields.visible = False
            self._coord_display.visible = True
        
        # Populate dates
        self._start_date = dataset.date_start
        self._end_date = dataset.date_end
        self._date_start_label.value = dataset.date_start.strftime("%b %d, %Y")
        self._date_end_label.value = dataset.date_end.strftime("%b %d, %Y")
        
        # Populate day filter
        self._day_selector.value = dataset.day_filter
        
        # Populate hour range
        if dataset.hour_filter:
            self._hour_start.value = str(min(dataset.hour_filter))
            self._hour_end.value = str(max(dataset.hour_filter))
        
        # Populate quality filters
        self._max_cloud.value = dataset.max_cloud
        self._max_sza.value = dataset.max_sza
        # Update labels
        self._on_cloud_change(type('obj', (object,), {'control': self._max_cloud})())
        self._on_sza_change(type('obj', (object,), {'control': self._max_sza})())
        
        self._status_log.add_info(f"Loaded dataset: {dataset.name}")
        self._status_log.add_info(f"Tip: Change the date range to add new data")
        
        self._update_preview()
        self.update()
    
    def _on_region_change(self, e):
        """Handle region dropdown change."""
        value = e.control.value
        if value == "custom":
            # Show custom input fields
            self._custom_fields.visible = True
            self._coord_display.visible = False
        elif value in REGION_PRESETS:
            bbox, _ = REGION_PRESETS[value]
            self._bbox = [bbox.west, bbox.south, bbox.east, bbox.north]
            # Update display
            self._coord_display.content.controls[0].value = f"Longitude: {bbox.west:.2f}° to {bbox.east:.2f}°"
            self._coord_display.content.controls[1].value = f"Latitude: {bbox.south:.2f}° to {bbox.north:.2f}°"
            # Hide custom fields, show display
            self._custom_fields.visible = False
            self._coord_display.visible = True
        
        self._update_preview()
        self._update_map_coord_label()
        # Generate map preview asynchronously to avoid blocking UI
        if self.page:
            self.page.run_task(self._update_map_preview_async)
        self.update()
    
    
    def _on_save_map_click(self, e):
        """Save the current map preview to Downloads."""
        if not self._map_image_control.src:
             self._status_log.add_warning("No map to save")
             return
             
        try:
            import shutil
            import time
            
            src_path = Path(self._map_image_control.src)
            if not src_path.exists():
                self._status_log.add_error("Map file source not found")
                return
                
            # Get Downloads folder safely
            downloads_path = Path.home() / "Downloads"
            if not downloads_path.exists():
                 # Fallback to home
                 downloads_path = Path.home()
            
            timestamp = int(time.time())
            filename = f"TEMPO_Map_Preview_{timestamp}.png"
            dest_path = downloads_path / filename
            
            shutil.copy2(src_path, dest_path)
            
            self._status_log.add_success(f"Map saved to: {dest_path}")
            
        except Exception as e:
            self._status_log.add_error(f"Failed to save map: {e}")

    async def _update_map_preview_async(self):

        """Generate map preview asynchronously with two-stage loading."""
        # Capture current bbox to avoid race conditions
        current_bbox = list(self._bbox)
        
        # Show progress
        if hasattr(self, '_map_progress'):
            self._map_progress.visible = True
            self.update()
        
        # Stage 1: Generate base map immediately (fast)
        path_base = await asyncio.to_thread(
            self._generate_map_preview, 
            bbox=current_bbox, 
            detailed=False
        )
        
        # Update UI with base map - Use image control directly
        if path_base and not path_base.startswith("Error"):
             self._map_image_control.src = path_base
             self._map_image_control.update()
        
        # Stage 2: Generate detailed map with roads (slower)
        path_detailed = await asyncio.to_thread(
            self._generate_map_preview, 
            bbox=current_bbox, 
            detailed=True
        )
        
        # Update UI with detailed map
        if path_detailed and not path_detailed.startswith("Error"):
             self._map_image_control.src = path_detailed
             self._map_image_control.update()
        elif path_detailed:
             # Show error log
             print(f"Map Error: {path_detailed}")
             self._status_log.add_error(f"Map Error: {path_detailed}")
            
        # Hide progress
        if hasattr(self, '_map_progress'):
            self._map_progress.visible = False
            self.update()
    
    def _on_cloud_change(self, e):
        """Update cloud fraction label."""
        val = e.control.value
        percent = int(val * 100)
        if val <= 0.2:
            desc = "(strict - high quality)"
        elif val <= 0.4:
            desc = "(recommended balance)"
        elif val <= 0.6:
            desc = "(lenient - more data)"
        else:
            desc = "(very lenient - may include cloudy pixels)"
        self._cloud_label.value = f"{val:.2f} ({percent}% cloud cover) {desc}"
        self._update_preview()
        self.update()
    
    def _on_sza_change(self, e):
        """Update SZA label."""
        val = int(e.control.value)
        if val <= 60:
            desc = "(strict - midday observations only)"
        elif val <= 70:
            desc = "(excludes very early/late observations)"
        elif val <= 80:
            desc = "(includes early morning/late afternoon)"
        else:
            desc = "(includes near-horizon observations)"
        self._sza_label.value = f"{val}° {desc}"
        self._update_preview()
        self.update()
    
    def _open_start_picker(self, e):
        """Open start date picker."""
        self._start_picker.value = self._start_date
        self._start_picker.open = True
        self._start_picker.update()
    
    def _open_end_picker(self, e):
        """Open end date picker."""
        self._end_picker.value = self._end_date
        self._end_picker.open = True
        self._end_picker.update()
    
    def _on_start_date_change(self, e):
        """Handle start date selection."""
        if e.control.value:
            self._start_date = e.control.value
            self._date_start_label.value = self._start_date.strftime("%b %d, %Y")
            self._update_preview()

            self.update()
    
    def _on_end_date_change(self, e):
        """Handle end date selection."""
        if e.control.value:
            self._end_date = e.control.value
            self._date_end_label.value = self._end_date.strftime("%b %d, %Y")
            self._update_preview()

            self.update()
    
    def _on_day_change(self, days: list[int]):
        """Handle day selection change."""
        self._update_preview()

        if hasattr(self, 'update'):
            self.update()
    
    def _on_hour_change(self, e):
        """Handle hour dropdown change."""
        self._update_preview()

        self.update()

    def _update_preview(self):
        """Update the dataset preview text."""
        try:
            # Use stored dates
            start = self._start_date
            end = self._end_date
            
            # Get selected days from DaySelector
            days = self._day_selector.value
            day_names = ["M", "T", "W", "T", "F", "S", "S"]
            day_str = "".join(day_names[d] for d in days) if len(days) < 7 else "all days"
            if days == [0, 1, 2, 3, 4]:
                day_str = "weekdays"
            elif days == [5, 6]:
                day_str = "weekends"
            
            total_days = 0
            current = start
            while current <= end:
                if current.weekday() in days:
                    total_days += 1
                current += timedelta(days=1)
            
            # Count hours per day
            hour_start = int(self._hour_start.value)
            hour_end = int(self._hour_end.value)
            hours_per_day = max(0, hour_end - hour_start + 1)
            
            # Total granules
            total_granules = total_days * hours_per_day
            
            # Format preview - clean and organized
            self._preview_text.value = (
                f"Date Range: {start} to {end} ({(end - start).days + 1} days)\n"
                f"Day Filter: {day_str} ({total_days} matching days)\n"
                f"Hours: {hour_start:02d}:00 - {hour_end:02d}:00 UTC ({hours_per_day}/day)\n"
                f"Total Granules: {total_granules:,}"
            )
        except Exception as e:
            self._preview_text.value = f"⚠️ Error calculating preview: {e}"
    
    def _on_download_click(self, e):
        """Start the download process."""
        if self._is_downloading:
            return
        
        # Validate inputs
        name = self._name_field.value.strip()
        if not name:
            self._status_log.add_error("Dataset name is required!")
            return
        
        # Check for duplicate name (only in new mode)
        if not self._extend_mode:
            if self.db.get_dataset_by_name(name):
                self._status_log.add_error(f"A dataset named '{name}' already exists!")
                return
        else:
            # Extend mode: verify we have a dataset loaded
            if not self._extend_dataset_id:
                self._status_log.add_error("Please load a dataset first!")
                return

        # Validate variable selection
        if not self._validate_variable_selection():
            self._status_log.add_error("⚠️ At least 1 variable must be selected!")
            self._var_validation_error.visible = True
            self.update()
            return
        else:
            self._var_validation_error.visible = False

        # Start download
        self._is_downloading = True
        self._download_btn.disabled = True
        self._cancel_btn.visible = True
        self._progress_panel.show()
        
        # Register with global download manager
        import uuid
        self._current_download_id = str(uuid.uuid4())
        download_manager = self._get_download_manager()
        if download_manager:
            download_manager.add_download(self._current_download_id, name)
        
        if self._extend_mode:
            self._status_log.add_info(f"Extending dataset: {name}")
        else:
            self._status_log.add_info(f"Starting dataset creation: {name}")
        
        # Run download in background
        self.page.run_task(self._run_download)
        self.update()
    
    async def _run_download(self):
        """Run the download process asynchronously."""
        try:
            # Parse form values
            name = self._name_field.value.strip()
            
            # Get bbox - from custom fields if custom mode, else from stored values
            if self._region_dropdown.value == "custom":
                bbox = BoundingBox(
                    west=float(self._west_field.value),
                    south=float(self._south_field.value),
                    east=float(self._east_field.value),
                    north=float(self._north_field.value),
                )
            else:
                bbox = BoundingBox(
                    west=self._bbox[0],
                    south=self._bbox[1],
                    east=self._bbox[2],
                    north=self._bbox[3],
                )
            
            # Ensure dates are date objects (not datetime) for consistent comparison
            start_date = self._start_date if isinstance(self._start_date, date) and not isinstance(self._start_date, datetime) else self._start_date.date() if hasattr(self._start_date, 'date') else self._start_date
            end_date = self._end_date if isinstance(self._end_date, date) and not isinstance(self._end_date, datetime) else self._end_date.date() if hasattr(self._end_date, 'date') else self._end_date
            
            day_filter = self._day_selector.value
            
            hour_start = int(self._hour_start.value)
            hour_end = int(self._hour_end.value)
            hour_filter = list(range(hour_start, hour_end + 1))
            
            max_cloud = float(self._max_cloud.value)
            max_sza = float(self._max_sza.value)
            
            # Create or get existing dataset record
            if self._extend_mode and self._extend_dataset_id:
                # Extend mode: use existing dataset
                self._status_log.add_info(f"Loading existing dataset...")
                dataset = self.db.get_dataset(self._extend_dataset_id)
                if not dataset:
                    raise ValueError(f"Dataset {self._extend_dataset_id} not found")
                
                # Update date range to include new dates
                if start_date < dataset.date_start:
                    dataset.date_start = start_date
                if end_date > dataset.date_end:
                    dataset.date_end = end_date
                dataset.status = DatasetStatus.DOWNLOADING
                self.db.update_dataset(dataset)
                self._status_log.add_success(f"Extending dataset: {dataset.name}")
            else:
                # New mode: create new dataset
                dataset = Dataset(
                    id="",  # Will be generated
                    name=name,
                    created_at=datetime.now(),
                    bbox=bbox,
                    date_start=start_date,
                    date_end=end_date,
                    day_filter=day_filter,
                    hour_filter=hour_filter,
                    max_cloud=max_cloud,
                    max_sza=max_sza,
                    selected_variables=list(self._selected_var_ids),  # NEW
                    status=DatasetStatus.DOWNLOADING,
                )
                
                self._status_log.add_info(f"Creating dataset record...")
                dataset = self.db.create_dataset(dataset)
                self._status_log.add_success(f"Dataset ID: {dataset.id[:8]}...")
            
            # Generate granule list
            self._status_log.add_info("Calculating required granules...")
            granules = []
            current_date = start_date
            while current_date <= end_date:
                if current_date.weekday() in day_filter:
                    for hour in hour_filter:
                        granule = Granule(
                            dataset_id=dataset.id,
                            date=current_date,
                            hour=hour,
                            bbox_west=bbox.west,
                            bbox_south=bbox.south,
                            bbox_east=bbox.east,
                            bbox_north=bbox.north,
                            max_cloud=max_cloud,
                            max_sza=max_sza,
                        )
                        granules.append(granule)
                current_date += timedelta(days=1)
            
            self._status_log.add_success(f"Found {len(granules)} granules to download")
            
            # Save granule records
            self.db.create_granules_batch(granules)
            # Accumulate granule count if extending, otherwise set it
            current_count = dataset.granule_count or 0
            dataset.granule_count = current_count + len(granules)
            self.db.update_dataset(dataset)
            
            # Download all granules
            self._status_log.add_info(f"📡 Downloading {len(granules)} granules...")
            self._status_log.add_info(f"🔬 Selected variables: {', '.join(dataset.selected_variables)}")

            # Show worker progress panel
            # Update workers count based on current config
            num_workers = self.config.download_workers if self.config else 4
            self._worker_progress.set_workers(num_workers)
            self._worker_progress.show(total=len(granules))
            self.update()
            
            # Build date-hour list for downloader
            date_set = set()
            hour_set = set()
            for g in granules:
                date_set.add(g.date.isoformat())
                hour_set.add(g.hour)
            
            dates_list = sorted(date_set)
            hours_list = sorted(hour_set)
            
            # Create datasets folder with dataset name (sanitize for filesystem)
            safe_name = "".join(c if c.isalnum() or c in "._- " else "_" for c in name)
            datasets_dir = self.db.db_path.parent / "datasets" / safe_name
            datasets_dir.mkdir(parents=True, exist_ok=True)
            
            # Initialize downloader with the named dataset directory and configured workers
            num_workers = self.config.download_workers if self.config else 4
            api_key = self.config.rsig_api_key if self.config else ""
            downloader = RSIGDownloader(datasets_dir, max_concurrent=num_workers, api_key=api_key)
            adapter = UIStatusAdapter(
                self._status_log, 
                self._progress_panel,
                worker_panel=self._worker_progress,
                total_granules=len(granules),
                num_workers=num_workers
            )
            
            # Single call to download all granules (parallel execution)
            new_files = await downloader.download_granules(
                dates=dates_list,
                hours=hours_list,
                bbox=[bbox.west, bbox.south, bbox.east, bbox.north],
                dataset_name=safe_name,
                max_cloud=max_cloud,
                max_sza=max_sza,
                selected_variables=dataset.selected_variables,  # NEW - Pass selected variables
                status=adapter
            )
            
            # Update progress panel to complete
            self._worker_progress.complete(len(new_files), len(granules))
            
            if not self._is_downloading:
                dataset.status = DatasetStatus.PARTIAL
                self.db.update_dataset(dataset)
                return

            # Update dataset count - use total existing files
            # Gather ALL granule files (existing + new) for processing
            all_files = list(datasets_dir.glob("tempo_*.nc"))
            
            dataset.granules_downloaded = len(all_files)
            self.db.update_dataset(dataset)

            # Processing Step - FNR Calculation
            self._progress_panel.update_progress(0.9, "Processing data...")
            self._status_log.add_info("⚙️ Processing hourly averages and FNR...")
            
            # Use ALL files for processing
            # all_files is already set above
            
            if not all_files:
                 self._status_log.add_error("No files found to process.")
                 dataset.status = DatasetStatus.ERROR
                 self.db.update_dataset(dataset)
                 return

            try:
                # Process Data
                ds_avg = await asyncio.to_thread(DataProcessor.process_dataset, all_files)
                
                if ds_avg is None:
                    self._status_log.add_error("Processing failed (no valid data returned).")
                    dataset.status = DatasetStatus.ERROR
                else:
                    # Save processed NetCDF file in the named datasets folder
                    try:
                        output_path = datasets_dir / f"{safe_name}_processed.nc"
                        await asyncio.to_thread(DataProcessor.save_processed, ds_avg, output_path)
                        
                        dataset.file_path = str(output_path)
                        # Calculate file size in MB
                        if output_path.exists():
                            dataset.file_size_mb = output_path.stat().st_size / (1024 * 1024)
                        dataset.granules_downloaded = len(granules)
                        dataset.status = DatasetStatus.COMPLETE
                        self._status_log.add_success("✓ Processing complete!")
                        self._progress_panel.update_progress(1.0, "Complete!")
                    except Exception as e:
                         self._status_log.add_error(f"Save processed file failed: {e}")
                         dataset.status = DatasetStatus.ERROR
                    
            except Exception as e:
                self._status_log.add_error(f"Processing error: {e}")
                dataset.status = DatasetStatus.ERROR
                
            self.db.update_dataset(dataset)
            
            if dataset.status == DatasetStatus.COMPLETE:
                # Mark all granules as downloaded
                self.db.mark_granules_downloaded(dataset.id)
                self._status_log.add_success(f"🎉 Dataset '{name}' created successfully!")
                self._status_log.add_info(f"   └─ {len(granules)} granules processed")
                
                # Update global download manager
                download_manager = self._get_download_manager()
                if download_manager and self._current_download_id:
                    download_manager.complete(self._current_download_id)
                
                # Auto-navigate to Workspace after a short delay
                await asyncio.sleep(1.5)
                shell = self._get_shell()
                if shell and hasattr(shell, 'navigate_to'):
                    shell.navigate_to(f"/workspace/{dataset.id}")
            else:
                # Mark as error in download manager
                download_manager = self._get_download_manager()
                if download_manager and self._current_download_id:
                    download_manager.error(self._current_download_id)
            
        except Exception as e:
            self._status_log.add_error(f"Download failed: {e}")
            import traceback
            traceback.print_exc()
            # Mark as error in download manager
            download_manager = self._get_download_manager()
            if download_manager and self._current_download_id:
                download_manager.error(self._current_download_id)
        
        finally:
            self._is_downloading = False
            self._download_btn.disabled = False
            self._cancel_btn.visible = False
            self._current_download_id = None
            self.update()
    
    def _on_cancel_click(self, e):
        """Cancel the download."""
        self._is_downloading = False
        self._status_log.add_warning("Cancelling download...")

        # Cancel in download manager
        download_manager = self._get_download_manager()
        if download_manager and self._current_download_id:
            download_manager.cancel(self._current_download_id)

        self.update()

    # =========================================================================
    # Variable Selection Handlers
    # =========================================================================

    def _on_var_search_change(self, e):
        """Filter variables by search text."""
        search_text = e.control.value.lower()

        for product_id, checkbox in self._var_checkboxes.items():
            # Search in both product_id and display name
            var_meta = next((v for v in self._available_variables if v.product_id == product_id), None)
            if var_meta:
                visible = (
                    search_text in product_id.lower()
                    or search_text in var_meta.display_name.lower()
                    or search_text in var_meta.description.lower()
                )
                checkbox.visible = visible

        self.update()

    def _on_var_toggle(self, product_id: str, selected: bool):
        """Update selection set when checkbox is toggled."""
        if selected:
            self._selected_var_ids.add(product_id)
        else:
            self._selected_var_ids.discard(product_id)

    def _on_var_preset_click(self, preset: str):
        """Apply variable preset."""
        from ...core.variable_registry import VariableRegistry

        if preset == "default":
            # Default 3 variables
            selected = set(VariableRegistry.get_default_variables())
        elif preset == "trace_gases":
            # All trace gas variables
            selected = {v.product_id for v in self._available_variables if v.category == "Trace Gases"}
        elif preset == "clear":
            selected = set()
        else:
            return

        # Update checkboxes
        for product_id, checkbox in self._var_checkboxes.items():
            checkbox.value = product_id in selected

        self._selected_var_ids = selected
        self.update()

    def _validate_variable_selection(self) -> bool:
        """Validate that at least one variable is selected."""
        return len(self._selected_var_ids) > 0
