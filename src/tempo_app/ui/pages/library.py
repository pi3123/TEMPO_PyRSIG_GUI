"""Data Library page - View and manage downloaded datasets.

The new home page of the app. Shows datasets in a grid view with search,
filter, and sort capabilities. Includes FAB for quick access to create new datasets.
"""

import flet as ft
import asyncio
from datetime import datetime
from typing import Optional, Callable
from enum import Enum

from ..theme import Colors, Spacing, Sizing
from ..components.widgets import SectionCard, StatusLogPanel
from ...storage.database import Database
from ...storage.models import Dataset, DatasetStatus, BatchJob


class FilterOption(Enum):
    ALL = "All"
    COMPLETE = "Complete"
    PARTIAL = "Partial"
    DOWNLOADING = "Downloading"


class SortOption(Enum):
    RECENT = "Recent"
    NAME_AZ = "Name A-Z"
    NAME_ZA = "Name Z-A"
    SIZE = "Size"


class DatasetCard(ft.Container):
    """Grid card for displaying a dataset with hover actions."""
    
    def __init__(
        self, 
        dataset: Dataset,
        on_click: Callable[[Dataset], None] = None,
        on_delete: Callable[[Dataset], None] = None,
        on_duplicate: Callable[[Dataset], None] = None,
    ):
        super().__init__()
        self.dataset = dataset
        self._on_click = on_click
        self._on_delete = on_delete
        self._on_duplicate = on_duplicate
        self._build()
    
    def _build(self):
        """Build the card UI."""
        ds = self.dataset
        
        # Status indicator
        if ds.status == DatasetStatus.COMPLETE:
            status_icon = ft.Icons.CHECK_CIRCLE
            status_color = Colors.SUCCESS
            status_text = "Complete"
        elif ds.status == DatasetStatus.PARTIAL:
            status_icon = ft.Icons.WARNING
            status_color = Colors.WARNING
            status_text = "Partial"
        else:
            status_icon = ft.Icons.DOWNLOADING
            status_color = Colors.INFO
            status_text = "Downloading"
        
        # Thumbnail placeholder (could be last map image)
        thumbnail = ft.Container(
            content=ft.Icon(ft.Icons.MAP, size=48, color=Colors.PRIMARY),
            height=100,
            bgcolor=Colors.PRIMARY_CONTAINER,
            border_radius=ft.border_radius.only(top_left=8, top_right=8),
            alignment=ft.Alignment(0, 0),
        )
        
        # Quick action buttons (visible on hover via opacity)
        self._actions_row = ft.Row([
            ft.IconButton(
                icon=ft.Icons.COPY,
                icon_size=18,
                icon_color=Colors.ON_SURFACE_VARIANT,
                tooltip="Duplicate config",
                on_click=lambda e: self._on_duplicate(ds) if self._on_duplicate else None,
            ),
            ft.IconButton(
                icon=ft.Icons.DELETE_OUTLINE,
                icon_size=18,
                icon_color=Colors.ERROR,
                tooltip="Delete",
                on_click=lambda e: self._on_delete(ds) if self._on_delete else None,
            ),
        ], spacing=0, alignment=ft.MainAxisAlignment.END)
        
        # Card content
        card_content = ft.Column([
            thumbnail,
            ft.Container(
                content=ft.Column([
                    # Title row
                    ft.Row([
                        ft.Text(
                            ds.name,
                            size=14,
                            weight=ft.FontWeight.W_600,
                            color=Colors.ON_SURFACE,
                            overflow=ft.TextOverflow.ELLIPSIS,
                            max_lines=1,
                            expand=True,
                        ),
                        ft.Icon(status_icon, size=16, color=status_color),
                    ]),
                    # Date range
                    ft.Text(
                        f"{ds.date_start} → {ds.date_end}",
                        size=11,
                        color=Colors.ON_SURFACE_VARIANT,
                    ),
                    # Variables
                    ft.Text(
                        f"Variables: {ds.variables_str()}",
                        size=11,
                        color=Colors.ON_SURFACE_VARIANT,
                    ),
                    # Size and status
                    ft.Row([
                        ft.Text(
                            f"{ds.file_size_mb:.1f} MB",
                            size=11,
                            color=Colors.ON_SURFACE_VARIANT,
                        ),
                        ft.Container(expand=True),
                        self._actions_row,
                    ], spacing=0),
                ], spacing=4),
                padding=ft.padding.only(left=12, right=8, top=8, bottom=8),
            ),
        ], spacing=0)
        
        self.content = card_content
        self.width = 220
        self.bgcolor = Colors.SURFACE
        self.border_radius = 8
        self.border = ft.border.all(1, Colors.BORDER)
        self.on_click = lambda e: self._on_click(ds) if self._on_click else None
        self.on_hover = self._handle_hover
        
        # Shadow on hover
        self.shadow = None
        self.animate = ft.Animation(150, ft.AnimationCurve.EASE_OUT)
    
    def _handle_hover(self, e):
        """Handle hover state."""
        if e.data == "true":
            self.shadow = ft.BoxShadow(
                spread_radius=0,
                blur_radius=8,
                color=Colors.CARD_SHADOW,
                offset=ft.Offset(0, 2),
            )
            self.border = ft.border.all(1, Colors.PRIMARY)
        else:
            self.shadow = None
            self.border = ft.border.all(1, Colors.BORDER)
        self.update()


class BatchFolderCard(ft.Container):
    """Card representing a batch of datasets (folder)."""

    def __init__(self, batch_name: str, batch_id: str, count: int, on_click, on_delete=None):
        super().__init__()
        self.batch_name = batch_name
        self.batch_id = batch_id
        self.count = count
        self._on_click = on_click
        self._on_delete = on_delete
        self._build()

    def _build(self):
        # Delete button
        delete_btn = ft.IconButton(
            icon=ft.Icons.DELETE_OUTLINE,
            icon_size=18,
            icon_color=Colors.ERROR,
            tooltip="Delete batch",
            on_click=lambda e: self._on_delete(self.batch_id, self.batch_name) if self._on_delete else None,
        )

        self.content = ft.Column([
            ft.Container(
                content=ft.Icon(ft.Icons.FOLDER, size=48, color=Colors.PRIMARY),
                height=100,
                bgcolor=Colors.PRIMARY_CONTAINER,
                border_radius=ft.border_radius.only(top_left=8, top_right=8),
                alignment=ft.Alignment(0, 0),
            ),
            ft.Container(
                content=ft.Column([
                    ft.Row([
                        ft.Text(
                            self.batch_name,
                            size=14,
                            weight=ft.FontWeight.W_600,
                            color=Colors.ON_SURFACE,
                            overflow=ft.TextOverflow.ELLIPSIS,
                            expand=True,
                        ),
                        delete_btn,
                    ]),
                    ft.Text(
                        f"{self.count} datasets",
                        size=12,
                        color=Colors.ON_SURFACE_VARIANT,
                    ),
                ], spacing=4),
                padding=ft.padding.only(left=12, right=4, top=8, bottom=8),
            ),
        ], spacing=0)

        self.width = 220
        self.bgcolor = Colors.SURFACE
        self.border_radius = 8
        self.border = ft.border.all(1, Colors.BORDER)
        self.on_click = lambda e: self._on_click(self.batch_id)
        self.on_hover = self._handle_hover

    def _handle_hover(self, e):
        if e.data == "true":
            self.border = ft.border.all(1, Colors.PRIMARY)
            self.shadow = ft.BoxShadow(blur_radius=8, color=Colors.CARD_SHADOW, offset=ft.Offset(0, 2))
        else:
            self.border = ft.border.all(1, Colors.BORDER)
            self.shadow = None
        self.update()


class LibraryPage(ft.Container):
    """Page for browsing and managing stored datasets.
    
    Features:
    - Grid view of dataset cards
    - Search by name
    - Filter by status
    - Sort by date/name/size
    - Active downloads section
    - FAB for creating new datasets
    """
    
    def __init__(self, db: Database, on_navigate: Callable[[str], None] = None):
        super().__init__()
        self.db = db
        self._on_navigate = on_navigate
        self._all_datasets: list[Dataset] = []
        self._filtered_datasets: list[Dataset] = []
        self._batch_jobs: dict[str, BatchJob] = {}  # Map batch_id -> BatchJob
        self._current_filter = FilterOption.ALL
        self._current_sort = SortOption.RECENT
        self._current_folder: Optional[str] = None  # batch_job_id or None
        self._folder_name: str = ""
        self._search_query = ""
        self._build()
    
    def did_mount(self):
        """Called when control is added to page - load data async."""
        # Set the FAB on the page for proper positioning
        if hasattr(self, '_fab'):
            self.page.floating_action_button = self._fab
            self.page.update()
        self.page.run_task(self._load_data_async)
    
    def will_unmount(self):
        """Called when control is removed from page."""
        # Remove the FAB when leaving this page
        if self.page and self.page.floating_action_button == self._fab:
            self.page.floating_action_button = None

    async def _load_data_async(self):
        """Load all page data without blocking UI."""
        stats = await asyncio.to_thread(self.db.get_storage_stats)
        datasets = await asyncio.to_thread(self.db.get_all_datasets)
        batch_jobs = await asyncio.to_thread(self.db.get_all_batch_jobs)

        self._all_datasets = datasets
        self._batch_jobs = {job.id: job for job in batch_jobs}
        self._apply_filters()
        self._apply_storage_stats(stats)
        self._render_datasets()
        self.update()
    
    def _build(self):
        """Build the library page."""
        # Header with storage info
        self._storage_text = ft.Text("0 datasets • 0 MB", size=13, color=Colors.ON_SURFACE_VARIANT)
        
        header = ft.Container(
            content=ft.Row([
                ft.Row([
                    ft.Icon(ft.Icons.FOLDER_OPEN, size=28, color=Colors.PRIMARY),
                    ft.Text("Library", size=24, weight=ft.FontWeight.BOLD, color=Colors.ON_SURFACE),
                ], spacing=12),
                ft.Container(expand=True),
                self._storage_text,
                ft.IconButton(
                    icon=ft.Icons.REFRESH,
                    icon_color=Colors.ON_SURFACE_VARIANT,
                    tooltip="Refresh",
                    on_click=self._on_refresh,
                ),
            ]),
            padding=ft.padding.only(bottom=Spacing.MD),
        )
        
        # Search and filter bar
        self._search_field = ft.TextField(
            hint_text="Search datasets...",
            prefix_icon=ft.Icons.SEARCH,
            border_radius=8,
            height=40,
            text_size=14,
            content_padding=ft.padding.only(left=8, right=8, bottom=8),
            on_change=self._on_search_change,
            expand=True,
        )
        
        self._filter_dropdown = ft.Dropdown(
            value=FilterOption.ALL.value,
            options=[ft.DropdownOption(opt.value) for opt in FilterOption],
            width=130,
            height=40,
            text_size=13,
            content_padding=ft.padding.symmetric(horizontal=12),
        )
        self._filter_dropdown.on_change = self._on_filter_change
        
        self._sort_dropdown = ft.Dropdown(
            value=SortOption.RECENT.value,
            options=[ft.DropdownOption(opt.value) for opt in SortOption],
            width=120,
            height=40,
            text_size=13,
            content_padding=ft.padding.symmetric(horizontal=12),
        )
        self._sort_dropdown.on_change = self._on_sort_change
        
        filter_row = ft.Row([
            self._search_field,
            ft.Container(width=8),
            ft.Text("Filter:", size=13, color=Colors.ON_SURFACE_VARIANT),
            self._filter_dropdown,
            ft.Container(width=8),
            ft.Text("Sort:", size=13, color=Colors.ON_SURFACE_VARIANT),
            self._sort_dropdown,
        ], vertical_alignment=ft.CrossAxisAlignment.CENTER)
        
        # Active downloads section (hidden when empty)
        self._downloads_section = ft.Container(
            content=ft.Column([
                ft.Row([
                    ft.Icon(ft.Icons.DOWNLOADING, size=20, color=Colors.INFO),
                    ft.Text("Active Downloads", size=14, weight=ft.FontWeight.W_600, color=Colors.ON_SURFACE),
                ], spacing=8),
                ft.Container(height=8),
                # Downloads will be added here
                ft.Column([], spacing=8),
            ]),
            bgcolor=Colors.INFO + "10",  # Very light blue tint
            border_radius=8,
            padding=Spacing.MD,
            visible=False,  # Hidden by default
        )
        
        # Dataset grid
        self._dataset_grid = ft.Row(
            wrap=True,
            spacing=16,
            run_spacing=16,
        )
        
        # Empty state
        self._empty_state = ft.Container(
            content=ft.Column([
                ft.Icon(ft.Icons.FOLDER_OPEN, size=64, color=Colors.ON_SURFACE_VARIANT),
                ft.Container(height=16),
                ft.Text(
                    "No datasets yet",
                    size=18,
                    weight=ft.FontWeight.W_500,
                    color=Colors.ON_SURFACE,
                ),
                ft.Text(
                    "Hit that + button to get started!",
                    size=14,
                    color=Colors.ON_SURFACE_VARIANT,
                ),
            ], horizontal_alignment=ft.CrossAxisAlignment.CENTER),
            alignment=ft.Alignment(0, 0),
            expand=True,
            visible=True,
        )
        
        # Grid container
        self._grid_container = ft.Container(
            content=ft.Column([
                self._dataset_grid,
            ], scroll=ft.ScrollMode.AUTO, expand=True),
            expand=True,
            visible=False,  # Start hidden, will be shown if datasets exist
        )
        
        # FAB button - will be added to page in did_mount
        self._fab = ft.FloatingActionButton(
            icon=ft.Icons.ADD,
            bgcolor=Colors.PRIMARY,
            foreground_color=Colors.ON_PRIMARY,
            tooltip="Create new dataset",
            on_click=self._on_fab_click,
        )
        
        # Content area with either empty state or grid
        self._content_stack = ft.Stack([
            self._empty_state,
            self._grid_container,
        ], expand=True)
        
        # Main layout
        main_content = ft.Column([
            header,
            filter_row,
            ft.Container(height=Spacing.MD),
            self._downloads_section,
            ft.Container(
                content=self._content_stack,
                expand=True,
            ),
        ], expand=True)
        
        self.content = main_content
        self.expand = True
        self.padding = Spacing.PAGE_HORIZONTAL
    
    def _apply_storage_stats(self, stats: dict):
        """Apply storage stats to UI."""
        try:
            total_mb = stats.get("total_size_mb", 0)
            dataset_count = stats.get("dataset_count", 0)
            self._storage_text.value = f"{dataset_count} dataset{'s' if dataset_count != 1 else ''} • {total_mb:.1f} MB"
        except Exception as e:
            self._storage_text.value = f"Error: {e}"
    
    def _apply_filters(self):
        """Apply current filter and sort to datasets."""
        # First, filter by current folder (batch)
        if self._current_folder:
            # Inside a folder: show only datasets in this batch
            current_level = [d for d in self._all_datasets if d.batch_job_id == self._current_folder]
        else:
            # Root level: show independent datasets (batch_job_id is None)
            # We will handle batch folders separately in _render_datasets
            current_level = [d for d in self._all_datasets if d.batch_job_id is None]

        filtered = current_level
        
        if self._current_filter == FilterOption.COMPLETE:
            filtered = [d for d in filtered if d.status == DatasetStatus.COMPLETE]
        elif self._current_filter == FilterOption.PARTIAL:
            filtered = [d for d in filtered if d.status == DatasetStatus.PARTIAL]
        elif self._current_filter == FilterOption.DOWNLOADING:
            filtered = [d for d in filtered if d.status == DatasetStatus.DOWNLOADING]
        
        # Search
        if self._search_query:
            query = self._search_query.lower()
            filtered = [d for d in filtered if query in d.name.lower()]
        
        # Sort
        if self._current_sort == SortOption.RECENT:
            filtered = sorted(filtered, key=lambda d: d.created_at or datetime.min, reverse=True)
        elif self._current_sort == SortOption.NAME_AZ:
            filtered = sorted(filtered, key=lambda d: d.name.lower())
        elif self._current_sort == SortOption.NAME_ZA:
            filtered = sorted(filtered, key=lambda d: d.name.lower(), reverse=True)
        elif self._current_sort == SortOption.SIZE:
            filtered = sorted(filtered, key=lambda d: d.file_size_mb, reverse=True)
        
        self._filtered_datasets = filtered
    
    def _render_datasets(self):
        """Render the dataset grid."""
        self._dataset_grid.controls.clear()
        items_to_show = []

        # If in a folder, show a "Back" button
        if self._current_folder:
            back_btn = ft.Container(
                content=ft.Row([
                    ft.Icon(ft.Icons.ARROW_BACK, size=16, color=Colors.PRIMARY),
                    ft.Text(f"Back to Library", color=Colors.PRIMARY),
                ], spacing=8),
                on_click=self._exit_folder,
                padding=10,
                border_radius=8,
                ink=True,
            )
            self._dataset_grid.controls.append(ft.Container(content=back_btn, width=220))

        # Handle search mode - search across all datasets ignoring hierarchy
        if self._search_query:
            query = self._search_query.lower()
            filtered = [d for d in self._all_datasets if query in d.name.lower()]

            # Apply status filter if not ALL
            if self._current_filter == FilterOption.COMPLETE:
                filtered = [d for d in filtered if d.status == DatasetStatus.COMPLETE]
            elif self._current_filter == FilterOption.PARTIAL:
                filtered = [d for d in filtered if d.status == DatasetStatus.PARTIAL]
            elif self._current_filter == FilterOption.DOWNLOADING:
                filtered = [d for d in filtered if d.status == DatasetStatus.DOWNLOADING]

            # Apply sort
            filtered = self._sort_datasets(filtered)

            for ds in filtered:
                items_to_show.append(
                    DatasetCard(
                        dataset=ds,
                        on_click=self._on_dataset_click,
                        on_delete=self._on_delete_click,
                        on_duplicate=self._on_duplicate_click,
                    )
                )
        elif self._current_folder:
            # Inside a batch folder - show datasets from this batch
            for ds in self._filtered_datasets:
                items_to_show.append(
                    DatasetCard(
                        dataset=ds,
                        on_click=self._on_dataset_click,
                        on_delete=self._on_delete_click,
                        on_duplicate=self._on_duplicate_click,
                    )
                )
        else:
            # Root level - show folders and independent datasets
            # First, collect batch folders
            if self._current_filter == FilterOption.ALL:
                batch_groups = {}
                for d in self._all_datasets:
                    if d.batch_job_id:
                        if d.batch_job_id not in batch_groups:
                            batch_groups[d.batch_job_id] = []
                        batch_groups[d.batch_job_id].append(d)

                # Create folder cards
                for bid, dlist in batch_groups.items():
                    # Get batch job name if available
                    batch_job = self._batch_jobs.get(bid)
                    folder_name = batch_job.name if batch_job else "Batch Import"
                    items_to_show.append(BatchFolderCard(folder_name, bid, len(dlist), self._enter_folder, self._on_batch_delete_click))

            # Add independent datasets (not in any batch)
            for ds in self._filtered_datasets:
                items_to_show.append(
                    DatasetCard(
                        dataset=ds,
                        on_click=self._on_dataset_click,
                        on_delete=self._on_delete_click,
                        on_duplicate=self._on_duplicate_click,
                    )
                )

        # Show empty state or grid
        if not items_to_show and not self._current_folder:
            self._empty_state.visible = True
            self._grid_container.visible = False
        else:
            self._empty_state.visible = False
            self._grid_container.visible = True
            for item in items_to_show:
                self._dataset_grid.controls.append(item)

    def _sort_datasets(self, datasets: list[Dataset]) -> list[Dataset]:
        """Sort datasets according to current sort option."""
        if self._current_sort == SortOption.RECENT:
            return sorted(datasets, key=lambda d: d.created_at or datetime.min, reverse=True)
        elif self._current_sort == SortOption.NAME_AZ:
            return sorted(datasets, key=lambda d: d.name.lower())
        elif self._current_sort == SortOption.NAME_ZA:
            return sorted(datasets, key=lambda d: d.name.lower(), reverse=True)
        elif self._current_sort == SortOption.SIZE:
            return sorted(datasets, key=lambda d: d.file_size_mb, reverse=True)
        return datasets

    def _enter_folder(self, batch_id: str):
        self._current_folder = batch_id
        # Get the batch name
        batch_job = self._batch_jobs.get(batch_id)
        self._folder_name = batch_job.name if batch_job else "Batch Import"
        self._apply_filters()
        self._render_datasets()
        self.update()

    def _exit_folder(self, e):
        self._current_folder = None
        self._apply_filters()
        self._render_datasets()
        self.update()

    
    def _on_search_change(self, e):
        """Handle search input change."""
        self._search_query = e.control.value or ""
        self._apply_filters()
        self._render_datasets()
        self.update()
    
    def _on_filter_change(self, e):
        """Handle filter dropdown change."""
        value = e.control.value
        self._current_filter = next((f for f in FilterOption if f.value == value), FilterOption.ALL)
        self._apply_filters()
        self._render_datasets()
        self.update()
    
    def _on_sort_change(self, e):
        """Handle sort dropdown change."""
        value = e.control.value
        self._current_sort = next((s for s in SortOption if s.value == value), SortOption.RECENT)
        self._apply_filters()
        self._render_datasets()
        self.update()
    
    def _on_refresh(self, e):
        """Refresh the dataset list."""
        self.page.run_task(self._load_data_async)
    
    def _on_fab_click(self, e):
        """Handle FAB click - navigate to new dataset page."""
        if self.page:
            # Navigate using shell's navigate function
            shell = self.page.controls[0] if self.page.controls else None
            if shell and hasattr(shell, 'navigate_to'):
                shell.navigate_to("/new")
    
    def _on_dataset_click(self, dataset: Dataset):
        """Handle dataset card click - navigate to workspace."""
        if self.page:
            shell = self.page.controls[0] if self.page.controls else None
            if shell and hasattr(shell, 'navigate_to'):
                shell.navigate_to(f"/workspace/{dataset.id}")
    
    def _on_delete_click(self, dataset: Dataset):
        """Handle delete button click."""
        # Show confirmation dialog
        def close_dialog(e):
            dlg.open = False
            self.page.update()
        
        def confirm_delete(e):
            dlg.open = False
            self.page.run_task(self._delete_dataset_async, dataset)
        
        dlg = ft.AlertDialog(
            modal=True,
            title=ft.Text("Delete Dataset?", color=Colors.ON_SURFACE),
            content=ft.Text(f"Are you sure you want to delete '{dataset.name}'?\nThis cannot be undone.", color=Colors.ON_SURFACE),
            actions=[
                ft.TextButton("Cancel", on_click=close_dialog),
                ft.TextButton("Delete", on_click=confirm_delete, style=ft.ButtonStyle(color=Colors.ERROR)),
            ],
            actions_alignment=ft.MainAxisAlignment.END,
        )
        
        self.page.overlay.append(dlg)
        dlg.open = True
        self.page.update()
    
    async def _delete_dataset_async(self, dataset: Dataset):
        """Delete a dataset asynchronously."""
        try:
            await asyncio.to_thread(self.db.delete_dataset, dataset.id)
            # Refresh
            await self._load_data_async()
        except Exception as e:
            print(f"Error deleting dataset: {e}")

    def _on_batch_delete_click(self, batch_id: str, batch_name: str):
        """Handle batch folder delete button click."""
        def close_dialog(e):
            dlg.open = False
            self.page.update()

        def confirm_delete(e):
            dlg.open = False
            self.page.run_task(self._delete_batch_async, batch_id)

        dlg = ft.AlertDialog(
            modal=True,
            title=ft.Text("Delete Entire Batch?", color=Colors.ON_SURFACE),
            content=ft.Text(
                f"Are you sure you want to delete the batch '{batch_name}' and ALL its datasets?\n\nThis will delete all files and cannot be undone.",
                color=Colors.ON_SURFACE
            ),
            actions=[
                ft.TextButton("Cancel", on_click=close_dialog),
                ft.TextButton("Delete All", on_click=confirm_delete, style=ft.ButtonStyle(color=Colors.ERROR)),
            ],
            actions_alignment=ft.MainAxisAlignment.END,
        )

        self.page.overlay.append(dlg)
        dlg.open = True
        self.page.update()

    async def _delete_batch_async(self, batch_id: str):
        """Delete an entire batch job and all associated data."""
        try:
            await asyncio.to_thread(self.db.delete_batch_job_full, batch_id)
            # Refresh
            await self._load_data_async()
        except Exception as e:
            print(f"Error deleting batch: {e}")

    def _on_duplicate_click(self, dataset: Dataset):
        """Handle duplicate button click - navigate to new dataset with prefilled config."""
        if self.page:
            shell = self.page.controls[0] if self.page.controls else None
            if shell and hasattr(shell, 'navigate_to'):
                # TODO: Pass dataset config as query params
                shell.navigate_to("/new")
