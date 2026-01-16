import flet as ft
from pathlib import Path
from typing import Callable

from ..theme import Colors, Spacing
from ..components.widgets import SectionCard
from ...core.config import ConfigManager

class SettingsPage(ft.Container):
    """Application settings page."""
    
    def _on_save_dir(self, e):
        """Save the manually entered directory."""
        path_str = self._dir_input.value.strip()
        if not path_str:
            self.page.snack_bar = ft.SnackBar(ft.Text("Path cannot be empty"))
            self.page.snack_bar.open = True
            self.page.update()
            return

        # Basic cleanup
        if path_str.startswith('"') and path_str.endswith('"'):
            path_str = path_str[1:-1]
            
        try:
            p = Path(path_str)
            # Create if doesn't exist? Or just warn?
            # Let's try to verify if it's a valid path format
            if not p.exists():
                # Ask user if they want to create it? Or just assume yes?
                # For simplicity, just save it. The app will try to create it on restart.
                pass
                
            self.config.set("data_dir", str(p))
            
            if self.on_restart_request:
                self.on_restart_request("Data directory changed. Please restart the app.")
            else:
                self.page.snack_bar = ft.SnackBar(ft.Text("Path saved. Restart required."))
                self.page.snack_bar.open = True
                self.page.update()
                
        except Exception as e:
            self.page.snack_bar = ft.SnackBar(ft.Text(f"Invalid path: {e}"))
            self.page.snack_bar.open = True
            self.page.update()

    def __init__(self, config: ConfigManager, on_restart_request: Callable = None):
        super().__init__()
        self.config = config
        self.on_restart_request = on_restart_request
        self._build()
        
    async def _open_picker(self, e):
        """Open directory picker asynchronously."""
        # Note: In Flet async mode, get_directory_path is a coroutine
        # FilePicker removed by user request
        pass
        
    def _build(self):
        # Header
        header = ft.Column([
            ft.Text("⚙️ Settings", size=28, weight=ft.FontWeight.BOLD, color=Colors.ON_SURFACE),
            ft.Divider(height=20, color=Colors.DIVIDER),
        ])
        
        # Data Directory Section
        # Data Directory Section
        self._dir_input = ft.TextField(
            value=self.config.data_dir or str(Path.cwd() / "data"),
            label="Path",
            text_style=ft.TextStyle(color=Colors.ON_SURFACE),
            border_color=Colors.BORDER,
            expand=True,
            text_size=14,
        )
        
        data_section = SectionCard("Data Storage", ft.Column([
            ft.Text("Location where datasets and plots are stored.", color=Colors.ON_SURFACE_VARIANT),
            ft.Row([
                ft.Icon(ft.Icons.FOLDER, color=Colors.PRIMARY),
                self._dir_input,
                ft.IconButton(
                    icon=ft.Icons.SAVE, 
                    icon_color=Colors.PRIMARY,
                    tooltip="Save Path",
                    on_click=self._on_save_dir
                )
            ], alignment=ft.MainAxisAlignment.SPACE_BETWEEN),
            ft.Text("⚠️ Changing this requires an app restart.", size=12, color=Colors.ERROR),
        ], spacing=10))
        
        # Appearance Section
        self._font_slider = ft.Slider(
            min=0.8, max=1.5, divisions=7, value=self.config.font_scale,
            label="{value}x",
            on_change_end=self._on_font_scale_change
        )
        
        appearance_section = SectionCard("Appearance", ft.Column([
            ft.Text("Application Font Size (Scale)", color=Colors.ON_SURFACE),
            ft.Row([
                ft.Text("Small", size=12, color=Colors.ON_SURFACE),
                ft.Container(content=self._font_slider, expand=True),
                ft.Text("Large", size=12, color=Colors.ON_SURFACE),
            ]),
            ft.Text("Adjusts the text size across the entire application (accessibility).", size=12, color=Colors.ON_SURFACE_VARIANT),
        ], spacing=10))
        
        # Download Workers Section
        self._workers_slider = ft.Slider(
            min=1, max=8, divisions=7, value=self.config.download_workers,
            label="{value}",
            on_change_end=self._on_workers_change
        )
        
        download_section = SectionCard("Downloads", ft.Column([
            ft.Text("Parallel Download Workers", color=Colors.ON_SURFACE),
            ft.Row([
                ft.Text("1", size=12, color=Colors.ON_SURFACE),
                ft.Container(content=self._workers_slider, expand=True),
                ft.Text("8", size=12, color=Colors.ON_SURFACE),
            ]),
            ft.Text(
                "Number of simultaneous downloads. Higher = faster but uses more bandwidth. "
                "Reduce if you experience network issues.", 
                size=12, color=Colors.ON_SURFACE_VARIANT
            ),
        ], spacing=10))
        
        # API Key Section
        self._api_key_input = ft.TextField(
            value=self.config.rsig_api_key,
            label="RSIG API Key",
            password=True,
            can_reveal_password=True,
            text_style=ft.TextStyle(color=Colors.ON_SURFACE),
            border_color=Colors.BORDER,
            expand=True,
            text_size=14,
            hint_text="Enter your NASA RSIG API key (optional)",
        )
        
        api_section = SectionCard("API Configuration", ft.Column([
            ft.Text("NASA RSIG API Key", color=Colors.ON_SURFACE),
            ft.Row([
                ft.Icon(ft.Icons.KEY, color=Colors.PRIMARY),
                self._api_key_input,
                ft.IconButton(
                    icon=ft.Icons.SAVE, 
                    icon_color=Colors.PRIMARY,
                    tooltip="Save API Key",
                    on_click=self._on_save_api_key
                )
            ], alignment=ft.MainAxisAlignment.SPACE_BETWEEN),
            ft.Text(
                "An API key is recommended for reliable data downloads. "
                "Leave empty for anonymous access (may have limits).", 
                size=12, color=Colors.ON_SURFACE_VARIANT
            ),
        ], spacing=10))
        
        # Content
        self.content = ft.Column([
            header,
            data_section,
            ft.Container(height=10),
            appearance_section,
            ft.Container(height=10),
            download_section,
            ft.Container(height=10),
            api_section,
            ft.Container(height=20),
        ], scroll=ft.ScrollMode.AUTO)
        
        self.expand = True
        self.padding = Spacing.PAGE_HORIZONTAL

    def _on_font_scale_change(self, e):
        new_scale = float(e.control.value)
        self.config.set("font_scale", new_scale)
        
        if self.on_restart_request:
            self.on_restart_request("Font scale changed. Please restart to apply fully.")
    
    def _on_workers_change(self, e):
        """Handle download workers slider change."""
        new_workers = int(e.control.value)
        self.config.set("download_workers", new_workers)
        
        # Show confirmation (no restart needed for this setting)
        if self.page:
            self.page.snack_bar = ft.SnackBar(
                ft.Text(f"Download workers set to {new_workers}"),
                duration=2000
            )
            self.page.snack_bar.open = True
            self.page.update()

    def _on_save_api_key(self, e):
        """Save the API key."""
        api_key = self._api_key_input.value.strip()
        self.config.set("rsig_api_key", api_key)
        
        if self.page:
            msg = "API key saved!" if api_key else "API key cleared (using anonymous access)"
            self.page.snack_bar = ft.SnackBar(
                ft.Text(msg),
                duration=2000
            )
            self.page.snack_bar.open = True
            self.page.update()
