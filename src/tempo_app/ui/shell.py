"""Application shell with top navigation bar and download manager."""

import flet as ft
from typing import Callable, Optional
from dataclasses import dataclass, field
from datetime import datetime

from .theme import Colors, Spacing, Sizing


@dataclass
class DownloadItem:
    """Represents an active download."""
    id: str
    name: str
    progress: float = 0.0  # 0.0 to 1.0
    status: str = "downloading"  # downloading, complete, error, cancelled
    started_at: datetime = field(default_factory=datetime.now)


class DownloadManager:
    """Global download state manager."""
    
    def __init__(self, on_change: Callable = None):
        self._downloads: dict[str, DownloadItem] = {}
        self._on_change = on_change
    
    def add_download(self, id: str, name: str) -> DownloadItem:
        """Add a new download."""
        item = DownloadItem(id=id, name=name)
        self._downloads[id] = item
        self._notify()
        return item
    
    def update_progress(self, id: str, progress: float):
        """Update download progress (0.0 to 1.0)."""
        if id in self._downloads:
            self._downloads[id].progress = min(1.0, max(0.0, progress))
            self._notify()
    
    def complete(self, id: str):
        """Mark download as complete."""
        if id in self._downloads:
            self._downloads[id].status = "complete"
            self._downloads[id].progress = 1.0
            self._notify()
    
    def error(self, id: str):
        """Mark download as errored."""
        if id in self._downloads:
            self._downloads[id].status = "error"
            self._notify()
    
    def cancel(self, id: str):
        """Cancel a download."""
        if id in self._downloads:
            self._downloads[id].status = "cancelled"
            self._notify()
    
    def remove(self, id: str):
        """Remove a download from the list."""
        if id in self._downloads:
            del self._downloads[id]
            self._notify()
    
    def get_active(self) -> list[DownloadItem]:
        """Get list of active (non-complete) downloads."""
        return [d for d in self._downloads.values() if d.status == "downloading"]
    
    def get_all(self) -> list[DownloadItem]:
        """Get all downloads."""
        return list(self._downloads.values())
    
    @property
    def active_count(self) -> int:
        """Number of active downloads."""
        return len(self.get_active())
    
    def _notify(self):
        """Notify listeners of state change."""
        if self._on_change:
            self._on_change()


class NavigationItem:
    """Navigation tab item configuration."""
    
    def __init__(
        self,
        icon: str,
        selected_icon: str,
        label: str,
        route: str,
    ):
        self.icon = icon
        self.selected_icon = selected_icon
        self.label = label
        self.route = route


# Navigation items - organized by user journey frequency
NAV_ITEMS = [
    NavigationItem(
        icon=ft.Icons.FOLDER_OUTLINED,
        selected_icon=ft.Icons.FOLDER,
        label="Library",
        route="/library",
    ),
    NavigationItem(
        icon=ft.Icons.ADD_CHART_OUTLINED,
        selected_icon=ft.Icons.ADD_CHART,
        label="New Dataset",
        route="/new",
    ),
    NavigationItem(
        icon=ft.Icons.UPLOAD_FILE_OUTLINED,
        selected_icon=ft.Icons.UPLOAD_FILE,
        label="Batch Import",
        route="/batch",
    ),
    NavigationItem(
        icon=ft.Icons.MAP_OUTLINED,
        selected_icon=ft.Icons.MAP,
        label="Workspace",
        route="/workspace",
    ),
    NavigationItem(
        icon=ft.Icons.PLACE_OUTLINED,
        selected_icon=ft.Icons.PLACE,
        label="Sites",
        route="/sites",
    ),
]


class DownloadDropdown(ft.Container):
    """Dropdown panel showing active downloads."""
    
    def __init__(self, download_manager: DownloadManager, on_view: Callable[[str], None] = None, on_cancel: Callable[[str], None] = None):
        super().__init__()
        self.download_manager = download_manager
        self._on_view = on_view
        self._on_cancel = on_cancel
        self._build()
    
    def _build(self):
        """Build the dropdown content."""
        downloads = self.download_manager.get_all()
        
        if not downloads:
            content = ft.Container(
                content=ft.Text("No active downloads", color=Colors.ON_SURFACE_VARIANT, size=13),
                padding=Spacing.MD,
            )
        else:
            items = []
            for dl in downloads:
                status_icon = ft.Icons.DOWNLOADING if dl.status == "downloading" else \
                              ft.Icons.CHECK_CIRCLE if dl.status == "complete" else \
                              ft.Icons.ERROR if dl.status == "error" else ft.Icons.CANCEL
                status_color = Colors.INFO if dl.status == "downloading" else \
                               Colors.SUCCESS if dl.status == "complete" else \
                               Colors.ERROR
                
                item = ft.Container(
                    content=ft.Column([
                        ft.Row([
                            ft.Icon(status_icon, color=status_color, size=16),
                            ft.Text(dl.name, color=Colors.ON_SURFACE, size=13, expand=True),
                            # View button
                            ft.IconButton(
                                icon=ft.Icons.OPEN_IN_NEW,
                                icon_size=16,
                                icon_color=Colors.PRIMARY,
                                tooltip="View in Workspace",
                                on_click=lambda e, id=dl.id: self._on_view(id) if self._on_view else None,
                            ) if dl.status in ("complete", "downloading") else ft.Container(),
                            # Cancel button
                            ft.IconButton(
                                icon=ft.Icons.CLOSE,
                                icon_size=16,
                                icon_color=Colors.ERROR,
                                tooltip="Cancel",
                                on_click=lambda e, id=dl.id: self._on_cancel(id) if self._on_cancel else None,
                            ) if dl.status == "downloading" else ft.Container(),
                        ], spacing=4),
                        # Progress bar for active downloads
                        ft.ProgressBar(
                            value=dl.progress,
                            bgcolor=Colors.SURFACE_VARIANT,
                            color=Colors.PRIMARY,
                            height=4,
                        ) if dl.status == "downloading" else ft.Container(),
                    ], spacing=4),
                    padding=ft.padding.symmetric(horizontal=Spacing.SM, vertical=Spacing.XS),
                )
                items.append(item)
            
            content = ft.Column(items, spacing=0, scroll=ft.ScrollMode.AUTO)
        
        self.content = ft.Container(
            content=ft.Column([
                ft.Container(
                    content=ft.Text("Downloads", weight=ft.FontWeight.W_600, color=Colors.ON_SURFACE, size=14),
                    padding=ft.padding.only(left=Spacing.MD, right=Spacing.MD, top=Spacing.SM, bottom=Spacing.XS),
                ),
                ft.Divider(height=1, color=Colors.DIVIDER),
                content,
            ], spacing=0),
            width=280,
            bgcolor=Colors.SURFACE,
            border_radius=8,
            border=ft.border.all(1, Colors.BORDER),
            shadow=ft.BoxShadow(
                spread_radius=0,
                blur_radius=8,
                color=Colors.CARD_SHADOW,
                offset=ft.Offset(0, 4),
            ),
        )
    
    def refresh(self):
        """Refresh the dropdown content."""
        self._build()


class AppShell(ft.Container):
    """Main application shell with top navigation and content area."""
    
    def __init__(
        self,
        page: ft.Page,
        on_route_change: Callable[[str], None] = None,
    ):
        super().__init__()
        self._page = page
        self._on_route_change_callback = on_route_change
        self._selected_index = 0
        self._download_dropdown_visible = False
        
        # Download manager
        self.download_manager = DownloadManager(on_change=self._on_downloads_changed)
        
        # Content placeholder
        self._content_area = ft.Container(
            expand=True,
            padding=Spacing.PAGE_HORIZONTAL,
        )
        
        # Build the shell
        self._build()
    
    def _build(self):
        """Build the shell layout."""
        # Create navigation tabs
        self._nav_tabs = []
        for i, item in enumerate(NAV_ITEMS):
            is_selected = i == self._selected_index
            tab = ft.Container(
                content=ft.Row([
                    ft.Icon(
                        item.selected_icon if is_selected else item.icon,
                        color=Colors.PRIMARY if is_selected else Colors.ON_SURFACE_VARIANT,
                        size=20,
                    ),
                    ft.Text(
                        item.label,
                        color=Colors.PRIMARY if is_selected else Colors.ON_SURFACE,
                        weight=ft.FontWeight.W_600 if is_selected else ft.FontWeight.NORMAL,
                        size=14,
                    ),
                ], spacing=6),
                padding=ft.padding.symmetric(horizontal=Spacing.MD, vertical=Spacing.SM),
                border_radius=6,
                bgcolor=Colors.PRIMARY_CONTAINER if is_selected else None,
                on_click=lambda e, idx=i: self._on_tab_click(idx),
            )
            self._nav_tabs.append(tab)
        
        # Download badge
        self._download_badge = ft.Container(
            content=ft.Text(
                str(self.download_manager.active_count),
                color=Colors.ON_PRIMARY,
                size=10,
                weight=ft.FontWeight.BOLD,
            ),
            width=16,
            height=16,
            border_radius=8,
            bgcolor=Colors.ERROR,
            alignment=ft.Alignment(0, 0),
            visible=self.download_manager.active_count > 0,
        )
        
        # Download button with badge
        self._download_button = ft.Stack([
            ft.IconButton(
                icon=ft.Icons.DOWNLOAD,
                icon_color=Colors.ON_SURFACE_VARIANT,
                tooltip="Downloads",
                on_click=self._toggle_download_dropdown,
            ),
            ft.Container(
                content=self._download_badge,
                right=4,
                top=4,
            ),
        ])
        
        # Download dropdown
        self._download_dropdown = DownloadDropdown(
            self.download_manager,
            on_view=self._on_download_view,
            on_cancel=self._on_download_cancel,
        )
        self._download_dropdown.visible = False
        
        # Settings button
        settings_button = ft.IconButton(
            icon=ft.Icons.SETTINGS_OUTLINED,
            icon_color=Colors.ON_SURFACE_VARIANT,
            tooltip="Settings",
            on_click=lambda _: self._navigate_to("/settings"),
        )
        
        # App bar with navigation tabs
        self._app_bar = ft.Container(
            content=ft.Row(
                controls=[
                    # Logo and title
                    ft.Row(
                        controls=[
                            ft.Icon(ft.Icons.SATELLITE_ALT, color=Colors.PRIMARY, size=28),
                            ft.Text(
                                "TEMPO Analyzer",
                                size=20,
                                weight=ft.FontWeight.W_600,
                                color=Colors.ON_SURFACE,
                            ),
                        ],
                        spacing=Spacing.SM,
                    ),
                    ft.Container(width=Spacing.XL),
                    # Navigation tabs
                    ft.Row(
                        controls=self._nav_tabs,
                        spacing=Spacing.XS,
                    ),
                    # Spacer
                    ft.Container(expand=True),
                    # Download button with dropdown
                    ft.Stack([
                        self._download_button,
                        ft.Container(
                            content=self._download_dropdown,
                            right=0,
                            top=44,
                        ),
                    ]),
                    # Settings button
                    settings_button,
                ],
            ),
            padding=ft.padding.symmetric(horizontal=Spacing.LG, vertical=Spacing.SM),
            bgcolor=Colors.SURFACE,
            border=ft.border.only(bottom=ft.BorderSide(1, Colors.BORDER)),
        )
        
        # Main layout
        self.content = ft.Column(
            controls=[
                self._app_bar,
                self._content_area,
            ],
            spacing=0,
            expand=True,
        )
        
        self.bgcolor = Colors.BACKGROUND
        self.expand = True
    
    def _on_tab_click(self, index: int):
        """Handle tab click."""
        self._selected_index = index
        route = NAV_ITEMS[index].route
        self._update_tab_styles(defer_update=True)
        self._navigate_to(route, skip_tab_update=True)
    
    def _update_tab_styles(self, defer_update: bool = False):
        """Update tab visual states.

        Args:
            defer_update: If True, skip page.update() to allow batching.
        """
        for i, tab in enumerate(self._nav_tabs):
            is_selected = i == self._selected_index
            item = NAV_ITEMS[i]

            # Update icon
            tab.content.controls[0].name = item.selected_icon if is_selected else item.icon
            tab.content.controls[0].color = Colors.PRIMARY if is_selected else Colors.ON_SURFACE_VARIANT

            # Update text
            tab.content.controls[1].color = Colors.PRIMARY if is_selected else Colors.ON_SURFACE
            tab.content.controls[1].weight = ft.FontWeight.W_600 if is_selected else ft.FontWeight.NORMAL

            # Update background
            tab.bgcolor = Colors.PRIMARY_CONTAINER if is_selected else None

        if not defer_update and self._page:
            self._page.update()
    
    def _toggle_download_dropdown(self, e):
        """Toggle download dropdown visibility."""
        self._download_dropdown_visible = not self._download_dropdown_visible
        self._download_dropdown.visible = self._download_dropdown_visible
        if self._download_dropdown_visible:
            self._download_dropdown.refresh()
        self._page.update()
    
    def _on_downloads_changed(self):
        """Handle download state changes."""
        count = self.download_manager.active_count
        self._download_badge.content.value = str(count)
        self._download_badge.visible = count > 0
        if self._download_dropdown_visible:
            self._download_dropdown.refresh()
        if self._page:
            self._page.update()
    
    def _on_download_view(self, download_id: str):
        """Handle view download click."""
        self._download_dropdown_visible = False
        self._download_dropdown.visible = False
        # Navigate to workspace with this dataset
        self._navigate_to(f"/workspace/{download_id}")
    
    def _on_download_cancel(self, download_id: str):
        """Handle cancel download click."""
        self.download_manager.cancel(download_id)
    
    def _navigate_to(self, route: str, skip_tab_update: bool = False):
        """Navigate to a route.

        Args:
            route: The route to navigate to.
            skip_tab_update: If True, skip tab style update (already done by caller).
        """
        # Update tab selection if it's a main nav route (unless already done)
        if not skip_tab_update:
            for i, item in enumerate(NAV_ITEMS):
                if route.startswith(item.route):
                    self._selected_index = i
                    self._update_tab_styles(defer_update=True)
                    break

        # Close download dropdown
        self._download_dropdown_visible = False
        self._download_dropdown.visible = False

        # Notify callback (this sets content and triggers update)
        if self._on_route_change_callback:
            self._on_route_change_callback(route)
            # Callback's set_content handles the page.update()
        elif self._page:
            # Only update directly if no callback
            self._page.update()
    
    def set_content(self, content: ft.Control, defer_update: bool = False):
        """Set the main content area.

        Args:
            content: The content control to display.
            defer_update: If True, skip page.update() (caller handles it).
        """
        self._content_area.content = content
        if not defer_update and self._page:
            self._page.update()
    
    def navigate_to(self, route: str):
        """Public method to navigate to a route."""
        self._navigate_to(route)
    
    @property
    def selected_route(self) -> str:
        """Get the currently selected route."""
        return NAV_ITEMS[self._selected_index].route
