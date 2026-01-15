"""Data Library page - View and manage downloaded datasets."""

import flet as ft
import asyncio
from datetime import datetime
from typing import Optional

from ..theme import Colors, Spacing
from ..components.widgets import SectionCard, StatusLogPanel
from ...storage.database import Database
from ...storage.models import Dataset, DatasetStatus


class LibraryPage(ft.Container):
    """Page for browsing and managing stored datasets."""
    
    def __init__(self, db: Database):
        super().__init__()
        self.db = db
        self._build()
    
    def did_mount(self):
        """Called when control is added to page - load data async."""
        self.page.run_task(self._load_data_async)

    async def _load_data_async(self):
        """Load all page data without blocking UI."""
        stats = await asyncio.to_thread(self.db.get_storage_stats)
        datasets = await asyncio.to_thread(self.db.get_all_datasets)
        self._apply_storage_stats(stats)
        self._apply_datasets(datasets)
        self.update()
    
    def _build(self):
        """Build the library page."""
        # Header
        header = ft.Row([
            ft.Icon(ft.Icons.FOLDER_OPEN, size=28, color=Colors.PRIMARY),
            ft.Text("Data Library", size=24, weight=ft.FontWeight.BOLD, color=Colors.ON_SURFACE),
        ], spacing=12)
        
        # Storage overview card - Dynamic elements
        self._storage_used_text = ft.Text("0 MB", color=Colors.ON_SURFACE_VARIANT)
        self._storage_progress = ft.ProgressBar(value=0, bgcolor=Colors.SURFACE_VARIANT, color=Colors.PRIMARY)
        self._storage_info_text = ft.Text(
            "No datasets yet. Create one from the Create tab.", 
            size=12, color=Colors.ON_SURFACE_VARIANT, italic=True
        )
        
        storage_card = SectionCard(
            title="Storage Overview",
            icon=ft.Icons.STORAGE,
            content=ft.Column([
                ft.Row([
                    ft.Text("Total Used:", color=Colors.ON_SURFACE),
                    self._storage_used_text,
                ], alignment=ft.MainAxisAlignment.SPACE_BETWEEN),
                self._storage_progress,
                self._storage_info_text,
            ], spacing=8),
        )
        
        # Dataset list
        self._dataset_list = ft.Column(spacing=8)
        
        # Log panel
        self._status_log = StatusLogPanel(max_entries=50)
        self._status_log.height = 150

        datasets_card = SectionCard(
            title="Datasets",
            icon=ft.Icons.DATASET,
            content=ft.Column([
                ft.Row([
                    ft.FilledButton(
                        content=ft.Row([ft.Icon(ft.Icons.REFRESH, size=16), ft.Text("Refresh")], spacing=4, tight=True),
                        on_click=self._on_refresh,
                    ),
                ]),
                ft.Container(height=8),
                self._dataset_list,
            ], spacing=8),
        )
        
        self.content = ft.Column([
            header,
            ft.Container(height=16),
            storage_card,
            ft.Container(height=16),
            datasets_card,
            ft.Container(height=16),
            self._status_log,
        ], scroll=ft.ScrollMode.AUTO)
        
        self.expand = True
        self.padding = Spacing.PAGE_HORIZONTAL
    
    def _apply_storage_stats(self, stats: dict):
        """Apply storage stats to UI (no DB call)."""
        try:
            total_mb = stats.get("total_size_mb", 0)
            dataset_count = stats.get("dataset_count", 0)

            self._storage_used_text.value = f"{total_mb:.1f} MB"

            max_storage_mb = 10 * 1024
            self._storage_progress.value = min(1.0, total_mb / max_storage_mb)

            if dataset_count == 0:
                self._storage_info_text.value = "No datasets yet. Create one from the Create tab."
            else:
                self._storage_info_text.value = f"{dataset_count} dataset{'s' if dataset_count != 1 else ''}"
        except Exception as e:
            self._storage_info_text.value = f"Error loading storage stats: {e}"

    def _apply_datasets(self, datasets: list):
        """Apply datasets list to UI (no DB call)."""
        self._dataset_list.controls.clear()
        if not datasets:
            self._dataset_list.controls.append(
                ft.Text("No datasets found.", color=Colors.ON_SURFACE_VARIANT, italic=True)
            )
        else:
            for ds in datasets:
                self._dataset_list.controls.append(self._create_dataset_card(ds))
    
    def _create_dataset_card(self, ds: Dataset) -> ft.Control:
        """Create a card for a dataset."""
        status_icon = "✓" if ds.status == DatasetStatus.COMPLETE else "⚠️" if ds.status == DatasetStatus.PARTIAL else "..."
        status_color = Colors.SUCCESS if ds.status == DatasetStatus.COMPLETE else Colors.WARNING
        
        return ft.Container(
            content=ft.Row([
                ft.Column([
                    ft.Text(ds.name, size=16, weight=ft.FontWeight.W_500, color=Colors.ON_SURFACE),
                    ft.Text(
                        f"{ds.date_start} to {ds.date_end} • {ds.file_size_mb:.1f} MB",
                        size=12, color=Colors.ON_SURFACE_VARIANT
                    ),
                ], spacing=4, expand=True),
                ft.Text(status_icon, color=status_color),
                ft.IconButton(
                    icon=ft.Icons.DELETE_OUTLINE,
                    icon_color=Colors.ERROR,
                    tooltip="Delete dataset",
                    on_click=lambda e, d=ds: self._delete_dataset(d),
                ),
            ], alignment=ft.MainAxisAlignment.SPACE_BETWEEN),
            padding=12,
            bgcolor=Colors.SURFACE_VARIANT,
            border_radius=8,
        )
    
    def _on_refresh(self, e):
        """Refresh the dataset list."""
        self._status_log.add_info("Refreshing...")
        self.page.run_task(self._refresh_async)

    async def _refresh_async(self):
        """Async refresh handler."""
        stats = await asyncio.to_thread(self.db.get_storage_stats)
        datasets = await asyncio.to_thread(self.db.get_all_datasets)
        self._apply_storage_stats(stats)
        self._apply_datasets(datasets)
        self._status_log.add_info("Refreshed dataset list")
        self.update()
    
    def _delete_dataset(self, dataset: Dataset):
        """Delete a dataset - sync wrapper that runs async delete."""
        self.page.run_task(self._on_delete_async, dataset)
    
    async def _on_delete_async(self, dataset: Dataset):
        """Delete a dataset asynchronously."""
        try:
            await asyncio.to_thread(self.db.delete_dataset, dataset.id)
            self._status_log.add_success(f"Deleted dataset '{dataset.name}'")
            # Refresh data after delete
            stats = await asyncio.to_thread(self.db.get_storage_stats)
            datasets = await asyncio.to_thread(self.db.get_all_datasets)
            self._apply_storage_stats(stats)
            self._apply_datasets(datasets)
            self.update()
        except Exception as e:
            self._status_log.add_error(f"Error deleting dataset: {e}")
            print(f"Error deleting dataset: {e}")
