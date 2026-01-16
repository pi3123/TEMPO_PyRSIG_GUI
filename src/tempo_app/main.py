"""TEMPO Analyzer - Main Application Entry Point.

A modern desktop application for analyzing NASA TEMPO satellite data.
"""

import flet as ft
from pathlib import Path
import sys

from .ui.theme import create_dark_theme, create_light_theme, Colors, LightColors, Sizing
from .ui.shell import AppShell
from .ui.pages.create import CreatePage
from .ui.pages.library import LibraryPage
from .ui.pages.inspect import InspectPage
from .ui.pages.sites import SitesPage
from .ui.pages.plot import PlotPage
from .ui.pages.export import ExportPage
from .ui.pages.workspace import WorkspacePage
from .ui.pages.settings import SettingsPage
from .ui.pages.batch_import import BatchImportPage
from .storage.database import Database
from .core.status import get_status_manager
from .core.config import ConfigManager
from .core.batch_scheduler import recover_interrupted_jobs


class App:
    """Main application class."""
    
    def __init__(self, page: ft.Page):
        self.page = page
        self.config = ConfigManager()
        self.data_dir = self._init_data_dir()
        self.db = Database(self.data_dir / "tempo.db")
        self.status = get_status_manager()

        # Recover interrupted batch jobs on startup
        recovered = recover_interrupted_jobs(self.db)
        if recovered > 0:
            print(f"Recovered {recovered} interrupted batch job(s)")

        # Page content cache
        self._pages: dict[str, ft.Control] = {}
        
        self._setup_page()
        self._build_ui()
    
    def _init_data_dir(self) -> Path:
        """Initialize and return data directory."""
        # Check config first
        if self.config.data_dir:
            return Path(self.config.data_dir)
            
        # Default logic
        if getattr(sys, 'frozen', False):
            base_path = Path(sys.executable).parent
        else:
            base_path = Path(__file__).parent.parent.parent
        
        data_dir = base_path / "data"
        self._ensure_dirs(data_dir)
        return data_dir

    def _ensure_dirs(self, data_dir: Path):
        """Ensure data directories exist."""
        data_dir.mkdir(parents=True, exist_ok=True)
        (data_dir / "cache").mkdir(exist_ok=True)
        (data_dir / "exports").mkdir(exist_ok=True)
        (data_dir / "plots").mkdir(exist_ok=True)
        (data_dir / "assets").mkdir(exist_ok=True)
        

    
    def _setup_page(self):
        """Configure the page settings."""
        self.page.title = "TEMPO Analyzer"
        self.page.theme = create_light_theme(font_scale=self.config.font_scale)
        self.page.dark_theme = create_dark_theme()
        self.page.theme_mode = ft.ThemeMode.LIGHT
        self.page.bgcolor = LightColors.BACKGROUND
        
        # Window sizing
        self.page.window.width = Sizing.WINDOW_DEFAULT_WIDTH
        self.page.window.height = Sizing.WINDOW_DEFAULT_HEIGHT
        self.page.window.min_width = Sizing.WINDOW_MIN_WIDTH
        self.page.window.min_height = Sizing.WINDOW_MIN_HEIGHT
        # Note: window.center() is async in newer Flet, skip for now
        
        # Padding is handled by shell
        self.page.padding = 0
        self.page.spacing = 0
    
    def _build_ui(self):
        """Build the main UI."""
        # Create shell with navigation
        self.shell = AppShell(
            page=self.page,
            on_route_change=self._on_route_change,
        )
        
        # Add shell to page
        self.page.add(self.shell)
        
        # Navigate to default route - Library is now home
        self._on_route_change("/library")
    
    def _on_route_change(self, route: str):
        """Handle route changes."""
        # Get or create page content
        content = self._get_page_content(route)
        self.shell.set_content(content)
    
    def _get_page_content(self, route: str) -> ft.Control:
        """Get content for a route (lazy loading)."""
        if route in self._pages:
            return self._pages[route]

        # Create page based on route
        # Extract base route and any parameters (e.g., /workspace/123)
        route_parts = route.split("/")
        base_route = "/" + route_parts[1] if len(route_parts) > 1 else route
        route_param = route_parts[2] if len(route_parts) > 2 else None
        
        # Use base route for caching
        cache_key = route
        
        if base_route == "/library":
            content = LibraryPage(db=self.db)
        elif base_route == "/new":
            # New Dataset page (formerly Create)
            content = CreatePage(db=self.db, config=self.config)
        elif base_route == "/batch":
            content = BatchImportPage(db=self.db, config=self.config, data_dir=self.data_dir)
        elif base_route == "/workspace":
            # Workspace page - unified Map/Export/Sites view
            content = WorkspacePage(db=self.db, data_dir=self.data_dir, dataset_id=route_param)
        elif base_route == "/sites":
            content = SitesPage(db=self.db)
        elif base_route == "/settings":
            content = SettingsPage(
                config=self.config,
                on_restart_request=self._show_restart_dialog
            )
        # Legacy routes (keep for backward compatibility during transition)
        elif route == "/create":
            content = CreatePage(db=self.db, config=self.config)
        elif route == "/plot":
            content = PlotPage(db=self.db, data_dir=self.data_dir)
        elif route == "/inspect":
            content = InspectPage(db=self.db)
        elif route == "/export":
            content = ExportPage(db=self.db, data_dir=self.data_dir)
        else:
            content = self._create_page_placeholder("Unknown Page", "❓")

        self._pages[route] = content
        return content
    
    def _create_page_placeholder(self, title: str, icon: str) -> ft.Control:
        """Create a placeholder page (to be replaced with real implementations)."""
        return ft.Container(
            content=ft.Column(
                controls=[
                    ft.Text(
                        f"{icon} {title}",
                        size=32,
                        weight=ft.FontWeight.BOLD,
                        color=Colors.ON_SURFACE,
                    ),
                    ft.Container(height=20),
                    ft.Text(
                        "This page is under construction.",
                        size=16,
                        color=Colors.ON_SURFACE_VARIANT,
                    ),
                    ft.Container(height=40),
                    ft.Button(
                        "Coming Soon",
                        icon=ft.Icons.CONSTRUCTION,
                        disabled=True,
                    ),
                ],
                horizontal_alignment=ft.CrossAxisAlignment.CENTER,
                alignment=ft.MainAxisAlignment.CENTER,
            ),
            expand=True,
            alignment=ft.Alignment(0, 0),  # Center
        )
    
    def _show_restart_dialog(self, message: str):
        """Show a dialog requesting restart."""
        dlg = ft.AlertDialog(
            title=ft.Text("Restart Required"),
            content=ft.Text(message),
            actions=[
                ft.TextButton("OK", on_click=lambda e: self._close_dialog(dlg))
            ],
            actions_alignment=ft.MainAxisAlignment.END,
        )
        self.page.dialog = dlg
        dlg.open = True
        self.page.update()
        
    def _close_dialog(self, dlg):
        dlg.open = False
        self.page.update()


def main(page: ft.Page):
    """Flet application entry point."""
    App(page)


if __name__ == "__main__":
    import logging
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)

    try:
        logger.info("Starting TEMPO Analyzer...")
        # WARNING: ft.app is deprecated, using ft.run as recommended
        ft.run(main)
    except Exception as e:
        logger.error(f"Failed to start app: {e}", exc_info=True)
        input("Press Enter to exit...")  # Keep window open if it crashes immediately
