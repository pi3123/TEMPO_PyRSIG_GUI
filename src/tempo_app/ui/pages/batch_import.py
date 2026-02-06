"""Batch Import Page - Import and process multiple sites from Excel/CSV.

This page allows users to:
1. Import sites from an Excel or CSV file
2. Configure default settings for all sites
3. Start batch processing with parallel downloads
4. Monitor progress and resume interrupted jobs
"""

import flet as ft
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Optional
import asyncio
import uuid
import logging

from ..theme import Colors, Spacing
from ..components.widgets import (
    HelpTooltip,
    LabeledField,
    SectionCard,
    StatusLogPanel,
    ProgressPanel,
    DaySelector,
)
from ...storage.models import BatchJob, BatchSite, BatchJobStatus, BatchSiteStatus
from ...storage.database import Database
from ...core.config import ConfigManager
from ...core.batch_parser import parse_import_file, ParseResult, ParsedSite
from ...core.batch_scheduler import BatchScheduler
from ...core.geo_utils import bbox_from_center

logger = logging.getLogger(__name__)

HELP_TEXTS = {
    "file_import": (
        "Upload an Excel (.xlsx) or CSV file containing site information.\n\n"
        "Required columns:\n"
        "- name (or site_name): Unique identifier for each site\n"
        "- latitude (or lat): Site latitude in decimal degrees\n"
        "- longitude (or lon): Site longitude in decimal degrees\n\n"
        "Optional columns:\n"
        "- date_start, date_end: Custom date range for this site\n"
        "- max_cloud, max_sza: Custom quality filters"
    ),
    "default_radius": (
        "Default radius in kilometers for calculating bounding boxes.\n\n"
        "A bounding box will be created around each site's coordinates\n"
        "using this radius.\n\n"
        "Typical values: 5-20 km depending on area of interest."
    ),
    "batch_size": (
        "Number of sites to process in parallel.\n\n"
        "Higher values = faster processing but more memory/network usage.\n"
        "Lower values = slower but more stable.\n\n"
        "Recommended: 3-5 for most systems."
    ),
}


class BatchImportPage(ft.Container):
    """Batch import page for processing multiple sites from Excel/CSV."""

    def __init__(self, db: Database, config: ConfigManager = None, data_dir: Path = None):
        super().__init__()
        self.db = db
        self.config = config
        self.data_dir = data_dir or Path("data")

        self._scheduler: Optional[BatchScheduler] = None
        self._current_job: Optional[BatchJob] = None
        self._parse_result: Optional[ParseResult] = None
        self._is_processing = False

        self._build()

    def did_mount(self):
        """Called when the control is added to the page."""
        # Add date pickers to overlay
        if hasattr(self, "_start_picker") and self._start_picker not in self.page.overlay:
            self.page.overlay.append(self._start_picker)
        if hasattr(self, "_end_picker") and self._end_picker not in self.page.overlay:
            self.page.overlay.append(self._end_picker)
        self.page.update()

        # Check for resumable jobs
        self._check_resumable_jobs()

    def _build(self):
        """Build the page layout."""
        # =================================================================
        # File Import Section
        # =================================================================
        self._file_picker = ft.FilePicker()
        self._file_path_text = ft.Text(
            "No file selected",
            size=13,
            color=Colors.ON_SURFACE_VARIANT,
            italic=True,
        )

        self._browse_btn = ft.ElevatedButton(
            content=ft.Row([
                ft.Icon(ft.Icons.FOLDER_OPEN, size=18),
                ft.Text("Browse..."),
            ], spacing=8, tight=True),
            on_click=lambda e: asyncio.create_task(self._open_file_picker()),
        )

        # Sites preview table
        self._sites_table = ft.DataTable(
            columns=[
                ft.DataColumn(ft.Text("Row", size=12, color=Colors.ON_SURFACE)),
                ft.DataColumn(ft.Text("Name", size=12, color=Colors.ON_SURFACE)),
                ft.DataColumn(ft.Text("Latitude", size=12, color=Colors.ON_SURFACE)),
                ft.DataColumn(ft.Text("Longitude", size=12, color=Colors.ON_SURFACE)),

                ft.DataColumn(ft.Text("Status", size=12, color=Colors.ON_SURFACE)),
            ],
            rows=[],
            border=ft.border.all(1, Colors.BORDER),
            border_radius=8,
            heading_row_color=Colors.SURFACE_VARIANT,
            heading_row_height=40,
            data_row_min_height=36,
            data_row_max_height=36,
        )

        self._sites_table_container = ft.Container(
            content=ft.Column([
                self._sites_table,
            ], scroll=ft.ScrollMode.AUTO),
            height=200,
            visible=False,
        )

        self._parse_status = ft.Text(
            "",
            size=13,
            color=Colors.ON_SURFACE_VARIANT,
        )

        file_section = SectionCard(
            title="Import File",
            icon=ft.Icons.UPLOAD_FILE,
            help_text=HELP_TEXTS["file_import"],
            content=ft.Column([
                ft.Row([
                    self._browse_btn,
                    self._file_path_text,
                ], spacing=12),
                self._parse_status,
                self._sites_table_container,
            ], spacing=12),
        )

        # =================================================================
        # Job Name Section
        # =================================================================
        self._job_name_field = ft.TextField(
            hint_text="e.g., California_Sites_Jan2024",
            border_color=Colors.BORDER,
            focused_border_color=Colors.PRIMARY,
            bgcolor=Colors.SURFACE_VARIANT,
            text_style=ft.TextStyle(color=Colors.ON_SURFACE),
            hint_style=ft.TextStyle(color=Colors.ON_SURFACE_VARIANT),
            width=300,
        )

        name_section = LabeledField(
            label="Batch Job Name",
            field=self._job_name_field,
            help_text="A unique name for this batch import job. Helps identify it in the job list.",
            required=True,
        )

        # =================================================================
        # Default Settings Section
        # =================================================================
        self._default_radius = ft.Slider(
            min=1,
            max=50,
            value=10,
            divisions=49,
            label="{value} km",
            active_color=Colors.PRIMARY,
            inactive_color=Colors.SURFACE_VARIANT,
            on_change=self._on_radius_change,
        )

        self._radius_label = ft.Text(
            "10 km",
            size=13,
            color=Colors.ON_SURFACE_VARIANT,
        )

        # Date pickers
        today = date.today()
        default_start = today - timedelta(days=30)
        self._start_date = default_start
        self._end_date = today

        self._date_start_label = ft.Text(default_start.strftime("%b %d, %Y"))
        self._date_start_btn = ft.OutlinedButton(
            content=ft.Row([
                ft.Icon(ft.Icons.CALENDAR_MONTH, size=18),
                self._date_start_label,
            ], spacing=8, tight=True),
            on_click=self._open_start_picker,
        )

        self._date_end_label = ft.Text(today.strftime("%b %d, %Y"))
        self._date_end_btn = ft.OutlinedButton(
            content=ft.Row([
                ft.Icon(ft.Icons.CALENDAR_MONTH, size=18),
                self._date_end_label,
            ], spacing=8, tight=True),
            on_click=self._open_end_picker,
        )

        self._start_picker = ft.DatePicker(
            first_date=date(2023, 8, 1),
            last_date=today,
            on_change=self._on_start_date_change,
        )

        self._end_picker = ft.DatePicker(
            first_date=date(2023, 8, 1),
            last_date=today,
            on_change=self._on_end_date_change,
        )

        # Day selector
        self._day_selector = DaySelector(value=[0, 1, 2, 3, 4], on_change=self._on_day_change)

        day_presets = ft.Row([
            ft.TextButton("Weekdays", on_click=lambda e: self._day_selector.select_weekdays()),
            ft.TextButton("Weekends", on_click=lambda e: self._day_selector.select_weekends()),
            ft.TextButton("All", on_click=lambda e: self._day_selector.select_all()),
        ], spacing=4)

        # Hour selection
        self._hour_start = ft.Dropdown(
            options=[ft.DropdownOption(str(h), f"{h:02d}:00 UTC") for h in range(24)],
            value="16",
            width=130,
            border_color=Colors.BORDER,
            focused_border_color=Colors.PRIMARY,
            bgcolor=Colors.SURFACE_VARIANT,
            text_style=ft.TextStyle(color=Colors.ON_SURFACE),
        )

        self._hour_end = ft.Dropdown(
            options=[ft.DropdownOption(str(h), f"{h:02d}:00 UTC") for h in range(24)],
            value="20",
            width=130,
            border_color=Colors.BORDER,
            focused_border_color=Colors.PRIMARY,
            bgcolor=Colors.SURFACE_VARIANT,
            text_style=ft.TextStyle(color=Colors.ON_SURFACE),
        )

        # Quality filters
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
            label="{value}",
            active_color=Colors.PRIMARY,
            inactive_color=Colors.SURFACE_VARIANT,
            on_change=self._on_sza_change,
        )

        self._sza_label = ft.Text(
            "70 degrees",
            size=13,
            color=Colors.ON_SURFACE_VARIANT,
        )

        # Batch size
        self._batch_size = ft.Dropdown(
            options=[
                ft.DropdownOption("1", "1 site at a time"),
                ft.DropdownOption("3", "3 sites in parallel"),
                ft.DropdownOption("5", "5 sites in parallel (Recommended)"),
                ft.DropdownOption("10", "10 sites in parallel"),
            ],
            value="5",
            width=220,
            border_color=Colors.BORDER,
            focused_border_color=Colors.PRIMARY,
            bgcolor=Colors.SURFACE_VARIANT,
            text_style=ft.TextStyle(color=Colors.ON_SURFACE),
        )

        settings_section = SectionCard(
            title="Default Settings",
            icon=ft.Icons.SETTINGS,
            help_text="These settings apply to all sites unless overridden in the Excel file.",
            content=ft.Column([
                # Radius
                ft.Column([
                    ft.Row([
                        ft.Text("Default Radius", size=14, color=Colors.ON_SURFACE),
                        HelpTooltip(HELP_TEXTS["default_radius"]),
                    ]),
                    self._default_radius,
                    self._radius_label,
                ], spacing=4),

                ft.Divider(height=16, color=Colors.DIVIDER),

                # Date range
                ft.Row([
                    ft.Column([
                        ft.Text("Start Date", size=14, weight=ft.FontWeight.W_500, color=Colors.ON_SURFACE),
                        self._date_start_btn,
                    ], spacing=6),
                    ft.Text("to", color=Colors.ON_SURFACE_VARIANT),
                    ft.Column([
                        ft.Text("End Date", size=14, weight=ft.FontWeight.W_500, color=Colors.ON_SURFACE),
                        self._date_end_btn,
                    ], spacing=6),
                ], spacing=16, vertical_alignment=ft.CrossAxisAlignment.END),

                # Day filter
                ft.Column([
                    ft.Text("Days of Week", size=14, weight=ft.FontWeight.W_500, color=Colors.ON_SURFACE),
                    ft.Row([
                        self._day_selector,
                        ft.Container(width=16),
                        day_presets,
                    ]),
                ], spacing=6),

                # Hour range
                ft.Row([
                    LabeledField("Hour Start", self._hour_start),
                    ft.Text("to", color=Colors.ON_SURFACE_VARIANT),
                    LabeledField("Hour End", self._hour_end),
                ], spacing=16, vertical_alignment=ft.CrossAxisAlignment.END),

                ft.Divider(height=16, color=Colors.DIVIDER),

                # Quality filters
                ft.Column([
                    ft.Text("Max Cloud Fraction", size=14, color=Colors.ON_SURFACE),
                    self._max_cloud,
                    self._cloud_label,
                ], spacing=4),

                ft.Column([
                    ft.Text("Max Solar Zenith Angle", size=14, color=Colors.ON_SURFACE),
                    self._max_sza,
                    self._sza_label,
                ], spacing=4),

                ft.Divider(height=16, color=Colors.DIVIDER),

                # Batch size
                LabeledField(
                    label="Parallel Processing",
                    field=self._batch_size,
                    help_text=HELP_TEXTS["batch_size"],
                ),
            ], spacing=12),
        )

        # =================================================================
        # Action Buttons
        # =================================================================
        self._start_btn = ft.FilledButton(
            content=ft.Row([
                ft.Icon(ft.Icons.PLAY_ARROW, size=20),
                ft.Text("Start Batch Import"),
            ], spacing=8, tight=True),
            on_click=self._on_start_click,
        )

        self._pause_btn = ft.OutlinedButton(
            content=ft.Row([
                ft.Icon(ft.Icons.PAUSE, size=20),
                ft.Text("Pause"),
            ], spacing=8, tight=True),
            visible=False,
            on_click=self._on_pause_click,
        )

        self._cancel_btn = ft.OutlinedButton(
            content=ft.Row([
                ft.Icon(ft.Icons.CANCEL, size=20),
                ft.Text("Cancel"),
            ], spacing=8, tight=True),
            visible=False,
            on_click=self._on_cancel_click,
        )

        # =================================================================
        # Progress Section
        # =================================================================
        self._progress_bar = ft.ProgressBar(
            value=0,
            color=Colors.PRIMARY,
            bgcolor=Colors.SURFACE_VARIANT,
        )

        self._progress_text = ft.Text(
            "Ready",
            size=14,
            color=Colors.ON_SURFACE,
        )

        self._progress_detail = ft.Text(
            "",
            size=13,
            color=Colors.ON_SURFACE_VARIANT,
        )

        progress_section = ft.Container(
            content=ft.Column([
                ft.Row([
                    ft.Icon(ft.Icons.TRENDING_UP, size=18, color=Colors.PRIMARY),
                    ft.Text("Progress", size=14, weight=ft.FontWeight.W_600, color=Colors.ON_SURFACE),
                ], spacing=8),
                self._progress_bar,
                self._progress_text,
                self._progress_detail,
            ], spacing=8),
            bgcolor=Colors.SURFACE,
            border_radius=12,
            border=ft.border.all(1, Colors.BORDER),
            padding=16,
        )

        # Activity log
        self._status_log = StatusLogPanel()

        # =================================================================
        # Resumable Jobs Section
        # =================================================================
        self._resumable_jobs_container = ft.Container(
            content=ft.Column([
                ft.Row([
                    ft.Icon(ft.Icons.HISTORY, size=18, color=Colors.WARNING),
                    ft.Text("Resumable Jobs", size=14, weight=ft.FontWeight.W_600, color=Colors.ON_SURFACE),
                ], spacing=8),
                ft.Text(
                    "These jobs were interrupted and can be resumed:",
                    size=13,
                    color=Colors.ON_SURFACE_VARIANT,
                ),
            ], spacing=8),
            bgcolor=Colors.WARNING_CONTAINER,
            border_radius=12,
            padding=16,
            visible=False,
        )

        # =================================================================
        # Page Layout
        # =================================================================
        left_column = ft.Column(
            controls=[
                # Header
                ft.Row([
                    ft.Icon(ft.Icons.UPLOAD_FILE, size=28, color=Colors.PRIMARY),
                    ft.Text("Batch Import", size=28, weight=ft.FontWeight.BOLD, color=Colors.ON_SURFACE),
                    HelpTooltip(
                        "Import multiple sites from an Excel or CSV file.\n\n"
                        "Each site will get its own dataset with a bounding box\n"
                        "calculated from the site coordinates and radius.\n\n"
                        "Downloads run in parallel for fast processing of\n"
                        "hundreds or thousands of sites."
                    ),
                ], spacing=8),

                ft.Divider(height=20, color=Colors.DIVIDER),

                self._resumable_jobs_container,

                file_section,
                ft.Container(height=8),
                name_section,
                ft.Container(height=8),
                settings_section,
                ft.Container(height=16),

                # Action buttons
                ft.Row([
                    self._start_btn,
                    self._pause_btn,
                    self._cancel_btn,
                ], spacing=12),
            ],
            scroll=ft.ScrollMode.AUTO,
            spacing=0,
            expand=True,
        )

        right_column = ft.Column(
            controls=[
                progress_section,
                ft.Container(height=8),
                self._status_log,
            ],
            expand=True,
        )

        self.content = ft.Row(
            controls=[
                ft.Container(content=left_column, expand=2, padding=ft.padding.only(right=16)),
                ft.Container(content=right_column, expand=1),
            ],
            expand=True,
            spacing=0,
        )

        self.expand = True
        self.padding = Spacing.PAGE_HORIZONTAL

    # =====================================================================
    # File Handling
    # =====================================================================

    async def _open_file_picker(self):
        """Open the file picker dialog and handle result."""
        files = await self._file_picker.pick_files(
            allowed_extensions=["xlsx", "xls", "csv"],
            dialog_title="Select sites file",
        )

        if not files:
            return

        file_path = Path(files[0].path)
        self._file_path_text.value = file_path.name
        self._file_path_text.italic = False

        # Parse the file
        self._parse_result = parse_import_file(file_path)

        # Update UI with results
        self._update_parse_results()
        self.update()

    def _update_parse_results(self):
        """Update the UI with parse results."""
        if not self._parse_result:
            return

        result = self._parse_result

        # Update status text
        if result.is_valid:
            status_color = Colors.SUCCESS
            status_text = f"Parsed {result.valid_count} valid sites"
            if result.invalid_sites:
                status_text += f" ({len(result.invalid_sites)} with errors)"
            if result.warnings:
                status_text += f", {len(result.warnings)} warnings"
        else:
            status_color = Colors.ERROR
            status_text = f"Parse failed: {', '.join(result.errors)}"

        self._parse_status.value = status_text
        self._parse_status.color = status_color

        # Update table
        rows = []
        for site in result.sites[:50]:  # Limit to 50 for performance


            if site.error:
                status_icon = ft.Icon(ft.Icons.ERROR, size=16, color=Colors.ERROR)
                status_text = site.error[:30]
            else:
                status_icon = ft.Icon(ft.Icons.CHECK_CIRCLE, size=16, color=Colors.SUCCESS)
                status_text = "OK"

            rows.append(ft.DataRow(
                cells=[
                    ft.DataCell(ft.Text(str(site.row_number), size=12, color=Colors.ON_SURFACE)),
                    ft.DataCell(ft.Text(site.site_name[:20], size=12, color=Colors.ON_SURFACE)),
                    ft.DataCell(ft.Text(f"{site.latitude:.4f}", size=12, color=Colors.ON_SURFACE)),
                    ft.DataCell(ft.Text(f"{site.longitude:.4f}", size=12, color=Colors.ON_SURFACE)),

                    ft.DataCell(ft.Row([status_icon, ft.Text(status_text, size=11, color=Colors.ON_SURFACE)], spacing=4)),
                ]
            ))

        self._sites_table.rows = rows
        self._sites_table_container.visible = len(rows) > 0

        # Auto-generate job name from filename
        if result.file_path and not self._job_name_field.value:
            base_name = Path(result.file_path).stem
            self._job_name_field.value = f"{base_name}_{date.today().strftime('%Y%m%d')}"

    # =====================================================================
    # Date Pickers
    # =====================================================================

    def _open_start_picker(self, e):
        self._start_picker.open = True
        self.page.update()

    def _open_end_picker(self, e):
        self._end_picker.open = True
        self.page.update()

    def _on_start_date_change(self, e):
        if self._start_picker.value:
            self._start_date = self._start_picker.value
            self._date_start_label.value = self._start_date.strftime("%b %d, %Y")
            self.update()

    def _on_end_date_change(self, e):
        if self._end_picker.value:
            self._end_date = self._end_picker.value
            self._date_end_label.value = self._end_date.strftime("%b %d, %Y")
            self.update()

    # =====================================================================
    # Slider/Filter Handlers
    # =====================================================================

    def _on_day_change(self, days: list[int]):
        pass  # Day selector handles its own state

    def _on_radius_change(self, e):
        self._radius_label.value = f"{int(e.control.value)} km"
        self.update()

    def _on_cloud_change(self, e):
        val = e.control.value
        self._cloud_label.value = f"{val:.2f} ({int(val*100)}% cloud cover)"
        self.update()

    def _on_sza_change(self, e):
        self._sza_label.value = f"{int(e.control.value)} degrees"
        self.update()

    # =====================================================================
    # Job Actions
    # =====================================================================

    def _on_start_click(self, e):
        """Start the batch import job."""
        asyncio.create_task(self._start_batch_job())

    def _on_pause_click(self, e):
        """Pause the current job."""
        if self._scheduler:
            asyncio.create_task(self._scheduler.pause_job())
            self._status_log.add_info("Pausing job...")

    def _on_cancel_click(self, e):
        """Cancel the current job."""
        if self._scheduler:
            asyncio.create_task(self._scheduler.cancel_job())
            self._status_log.add_warning("Cancelling job...")

    async def _start_batch_job(self):
        """Create and start the batch import job."""
        # Validate inputs
        if not self._parse_result or not self._parse_result.valid_sites:
            self._status_log.add_error("No valid sites to import. Please select a file first.")
            return

        job_name = self._job_name_field.value.strip()
        if not job_name:
            self._status_log.add_error("Please enter a job name.")
            return

        # Get settings
        default_radius = self._default_radius.value
        hour_start = int(self._hour_start.value)
        hour_end = int(self._hour_end.value)
        hour_filter = list(range(hour_start, hour_end + 1))
        day_filter = self._day_selector.value
        max_cloud = self._max_cloud.value
        max_sza = self._max_sza.value
        batch_size = int(self._batch_size.value)

        # Create batch job
        job = BatchJob(
            id=str(uuid.uuid4()),
            name=job_name,
            created_at=datetime.now(),
            status=BatchJobStatus.PENDING,
            source_file=self._parse_result.file_path,
            total_sites=len(self._parse_result.valid_sites),
            default_radius_km=default_radius,
            date_start=self._start_date,
            date_end=self._end_date,
            day_filter=day_filter,
            hour_filter=hour_filter,
            max_cloud=max_cloud,
            max_sza=max_sza,
            batch_size=batch_size,
        )

        job = self.db.create_batch_job(job)
        self._current_job = job

        # Create batch sites
        batch_sites = []
        for i, parsed_site in enumerate(self._parse_result.valid_sites):
            radius = parsed_site.custom_radius_km if parsed_site.custom_radius_km else default_radius
            bbox = bbox_from_center(parsed_site.latitude, parsed_site.longitude, radius)

            batch_site = BatchSite(
                batch_job_id=job.id,
                site_name=parsed_site.site_name,
                latitude=parsed_site.latitude,
                longitude=parsed_site.longitude,
                radius_km=radius,
                bbox_west=bbox.west,
                bbox_south=bbox.south,
                bbox_east=bbox.east,
                bbox_north=bbox.north,
                custom_date_start=date.fromisoformat(parsed_site.custom_date_start) if parsed_site.custom_date_start else None,
                custom_date_end=date.fromisoformat(parsed_site.custom_date_end) if parsed_site.custom_date_end else None,
                custom_hour_start=parsed_site.custom_hour_start,
                custom_hour_end=parsed_site.custom_hour_end,
                custom_max_cloud=parsed_site.custom_max_cloud,
                custom_max_sza=parsed_site.custom_max_sza,
                sequence_number=i,
                status=BatchSiteStatus.PENDING,
            )
            batch_sites.append(batch_site)

        self.db.create_batch_sites(batch_sites)
        
        # Debug logging
        logger.info(f"Created batch job {job.id} with {len(batch_sites)} sites")
        for site in batch_sites:
            logger.debug(f"  Site: {site.site_name}, status={site.status}, batch_job_id={site.batch_job_id}")

        # Update UI
        self._is_processing = True
        self._start_btn.visible = False
        self._pause_btn.visible = True
        self._cancel_btn.visible = True
        self.update()

        self._status_log.add_info(f"Starting batch job: {job_name}")
        self._status_log.add_info(f"Processing {job.total_sites} sites with batch size {batch_size}")

        # Create scheduler and start
        api_key = self.config.rsig_api_key if self.config else ""
        self._scheduler = BatchScheduler(
            db=self.db,
            data_dir=self.data_dir,
            max_concurrent_sites=batch_size,
            api_key=api_key,
            on_progress=self._on_progress,
            on_site_complete=self._on_site_complete,
            on_job_complete=self._on_job_complete,
        )

        try:
            await self._scheduler.start_job(job.id)
        except Exception as e:
            self._status_log.add_error(f"Job failed: {e}")
        finally:
            self._is_processing = False
            self._start_btn.visible = True
            self._pause_btn.visible = False
            self._cancel_btn.visible = False
            self.update()

    def _on_progress(self, job: BatchJob, site: BatchSite, message: str):
        """Handle progress updates from scheduler."""
        self._progress_text.value = message

        progress = job.progress
        self._progress_bar.value = progress
        self._progress_detail.value = f"{job.completed_sites + job.failed_sites}/{job.total_sites} sites processed"

        self._status_log.add_progress(f"{site.site_name}: {message}")
        self.update()

    def _on_site_complete(self, site: BatchSite):
        """Handle site completion."""
        if site.status == BatchSiteStatus.COMPLETED:
            self._status_log.add_success(f"Completed: {site.site_name}")
        else:
            self._status_log.add_error(f"Failed: {site.site_name} - {site.error_message}")
        self.update()

    def _on_job_complete(self, job: BatchJob):
        """Handle job completion."""
        if job.status == BatchJobStatus.COMPLETED:
            self._status_log.add_success(f"Job completed! {job.completed_sites} sites processed, {job.failed_sites} failed.")
            self._progress_text.value = "Completed!"
        elif job.status == BatchJobStatus.PAUSED:
            self._status_log.add_warning("Job paused. You can resume it later.")
            self._progress_text.value = "Paused"
        else:
            self._status_log.add_error(f"Job ended with status: {job.status.value}")
            self._progress_text.value = f"Ended: {job.status.value}"

        self._progress_bar.value = job.progress
        self.update()

    def _check_resumable_jobs(self):
        """Check for jobs that can be resumed."""
        resumable = self.db.get_resumable_batch_jobs()

        if not resumable:
            self._resumable_jobs_container.visible = False
            return

        # Build resumable jobs UI
        job_controls = []
        for job in resumable[:5]:  # Show up to 5
            progress_pct = int(job.progress * 100)
            resume_btn = ft.OutlinedButton(
                content=ft.Row([
                    ft.Icon(ft.Icons.PLAY_ARROW, size=16),
                    ft.Text(f"{job.name} ({progress_pct}%)", size=13),
                ], spacing=4, tight=True),
                on_click=lambda e, j=job: asyncio.create_task(self._resume_job(j.id)),
            )
            delete_btn = ft.IconButton(
                icon=ft.Icons.DELETE_OUTLINE,
                icon_size=18,
                tooltip="Delete this job",
                on_click=lambda e, j=job: asyncio.create_task(self._delete_resumable_job(j.id)),
            )
            job_controls.append(ft.Row([resume_btn, delete_btn], spacing=4))

        self._resumable_jobs_container.content.controls = [
            ft.Row([
                ft.Icon(ft.Icons.HISTORY, size=18, color=Colors.WARNING),
                ft.Text("Resumable Jobs", size=14, weight=ft.FontWeight.W_600, color=Colors.ON_SURFACE),
            ], spacing=8),
            ft.Text(
                "These jobs were interrupted and can be resumed or deleted:",
                size=13,
                color=Colors.ON_SURFACE_VARIANT,
            ),
            ft.Column(job_controls, spacing=8),
        ]

        self._resumable_jobs_container.visible = True
        self.update()

    async def _resume_job(self, job_id: str):
        """Resume a paused/interrupted job."""
        job = self.db.get_batch_job(job_id)
        if not job:
            self._status_log.add_error("Job not found")
            return

        self._current_job = job
        self._job_name_field.value = job.name

        # Update UI
        self._is_processing = True
        self._start_btn.visible = False
        self._pause_btn.visible = True
        self._cancel_btn.visible = True
        self._resumable_jobs_container.visible = False
        self.update()

        self._status_log.add_info(f"Resuming job: {job.name}")

        # Create scheduler and start
        api_key = self.config.rsig_api_key if self.config else ""
        self._scheduler = BatchScheduler(
            db=self.db,
            data_dir=self.data_dir,
            max_concurrent_sites=job.batch_size,
            api_key=api_key,
            on_progress=self._on_progress,
            on_site_complete=self._on_site_complete,
            on_job_complete=self._on_job_complete,
        )

        try:
            await self._scheduler.start_job(job_id)
        except Exception as e:
            self._status_log.add_error(f"Resume failed: {e}")
        finally:
            self._is_processing = False
            self._start_btn.visible = True
            self._pause_btn.visible = False
            self._cancel_btn.visible = False
            self._check_resumable_jobs()
            self.update()
    async def _delete_resumable_job(self, job_id: str):
        """Delete a resumable job and its sites."""
        job = self.db.get_batch_job(job_id)
        if not job:
            self._status_log.add_error("Job not found")
            return

        try:
            # Delete job and all associated data
            self.db.delete_batch_job_full(job_id)
            self._status_log.add_info(f"Deleted job: {job.name}")
            # Refresh the resumable jobs list
            self._check_resumable_jobs()
        except Exception as e:
            self._status_log.add_error(f"Failed to delete job: {e}")
            logger.error(f"Error deleting job {job_id}: {e}")