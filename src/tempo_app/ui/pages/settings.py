import flet as ft
import logging
import asyncio
from pathlib import Path
from typing import Callable

from ..theme import Colors, Spacing
from ..components.widgets import SectionCard
from ...core.config import ConfigManager
from ...core.chart_generator import ChartGenerator

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

    def did_mount(self):
        """Called when page is mounted - auto-refresh models if API key exists."""
        api_key = self.config.get("gemini_api_key", "")
        if api_key:
            self.page.run_task(self._refresh_models_async)
        
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
        
        # Gemini AI Section
        self._gemini_key_input = ft.TextField(
            value=self.config.get("gemini_api_key", ""),
            label="Gemini API Key",
            password=True,
            can_reveal_password=True,
            text_style=ft.TextStyle(color=Colors.ON_SURFACE),
            border_color=Colors.BORDER,
            expand=True,
            text_size=14,
            hint_text="Enter your Google Gemini API key",
        )
        
        # Model dropdown
        self._model_dropdown = ft.Dropdown(
            label="Gemini Model",
            border_color=Colors.BORDER,
            focused_border_color=Colors.PRIMARY,
            bgcolor=Colors.SURFACE_VARIANT,
            width=350,
            text_style=ft.TextStyle(color=Colors.ON_SURFACE),
            label_style=ft.TextStyle(color=Colors.ON_SURFACE_VARIANT),
            value=self.config.get("gemini_model", "gemini-2.0-flash-lite"),
            options=[
                ft.DropdownOption(key="gemini-2.0-flash-lite", text="Gemini 2.0 Flash Lite (default)"),
            ],
        )
        
        # Model buttons
        self._refresh_models_btn = ft.IconButton(
            icon=ft.Icons.REFRESH,
            icon_color=Colors.PRIMARY,
            tooltip="Refresh Models List",
            on_click=self._on_refresh_models,
        )
        
        self._save_model_btn = ft.IconButton(
            icon=ft.Icons.SAVE,
            icon_color=Colors.PRIMARY,
            tooltip="Save Selected Model",
            on_click=self._on_save_model,
        )
        
        self._models_status = ft.Text("", size=11, color=Colors.ON_SURFACE_VARIANT)
        
        ai_section = SectionCard("AI Chart Analysis", ft.Column([
            ft.Text("Google Gemini API Key", color=Colors.ON_SURFACE),
            ft.Row([
                ft.Icon(ft.Icons.AUTO_AWESOME, color=Colors.PRIMARY),
                self._gemini_key_input,
                ft.IconButton(
                    icon=ft.Icons.SAVE, 
                    icon_color=Colors.PRIMARY,
                    tooltip="Save Gemini Key",
                    on_click=self._on_save_gemini_key
                )
            ], alignment=ft.MainAxisAlignment.SPACE_BETWEEN),
            ft.Text(
                "Required for AI-powered natural language chart generation. "
                "Get a free key at: aistudio.google.com/apikey", 
                size=12, color=Colors.ON_SURFACE_VARIANT
            ),
            ft.Divider(height=10, color=Colors.DIVIDER),
            ft.Text("Model Selection", color=Colors.ON_SURFACE),
            ft.Row([
                self._model_dropdown,
                self._save_model_btn,
                self._refresh_models_btn,
            ], spacing=8),
            self._models_status,
            ft.Text(
                "⚠️ API key is stored locally and used only for generating chart code.",
                size=12, color=Colors.WARNING
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
            ft.Container(height=10),
            ai_section,
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

    def _on_save_gemini_key(self, e):
        """Save the Gemini API key."""
        logging.info("[SETTINGS] _on_save_gemini_key called")
        api_key = self._gemini_key_input.value.strip()
        logging.info(f"[SETTINGS] API key length: {len(api_key)}")
        logging.info(f"[SETTINGS] Calling config.set('gemini_api_key', ...)")
        self.config.set("gemini_api_key", api_key)
        logging.info(f"[SETTINGS] config.set returned. Verifying...")
        saved_key = self.config.get("gemini_api_key", "")
        logging.info(f"[SETTINGS] Verification - saved key length: {len(saved_key)}")
        logging.info(f"[SETTINGS] Config file path: {self.config.config_file}")
        
        if self.page:
            msg = "Gemini API key saved!" if api_key else "Gemini API key cleared"
            logging.info(f"[SETTINGS] Showing snackbar: {msg}")
            self.page.snack_bar = ft.SnackBar(
                ft.Text(msg),
                duration=2000
            )
            self.page.snack_bar.open = True
            self.page.update()
            
            # Auto-refresh models after saving key
            if api_key:
                self.page.run_task(self._refresh_models_async)
        else:
            logging.warning("[SETTINGS] self.page is None, cannot show snackbar")

    def _on_save_model(self, e):
        """Handle save model button click."""
        logging.info(f"[SETTINGS] _on_save_model CALLED! e={e}")
        model_name = self._model_dropdown.value
        logging.info(f"[SETTINGS] dropdown.value={model_name}")
        if model_name:
            logging.info(f"[SETTINGS] Saving model: {model_name}")
            self.config.set("gemini_model", model_name)
            saved_model = self.config.get("gemini_model")
            logging.info(f"[SETTINGS] Saved model verification: {saved_model}")
            if self.page:
                self.page.snack_bar = ft.SnackBar(
                    ft.Text(f"Model set to: {model_name}"),
                    duration=2000
                )
                self.page.snack_bar.open = True
                self.page.update()

    def _on_refresh_models(self, e):
        """Handle refresh models button click."""
        if self.page:
            self.page.run_task(self._refresh_models_async)

    async def _refresh_models_async(self):
        """Fetch available models from Gemini API."""
        logging.info("[SETTINGS] Refreshing models list...")
        self._models_status.value = "Loading models..."
        self._models_status.color = Colors.INFO
        self.update()
        
        api_key = self.config.get("gemini_api_key", "")
        if not api_key:
            self._models_status.value = "⚠️ Enter API key first, then refresh"
            self._models_status.color = Colors.WARNING
            self.update()
            return
        
        try:
            models = await asyncio.to_thread(
                ChartGenerator.list_available_models,
                api_key
            )
            
            if not models:
                self._models_status.value = "⚠️ No models found or API error"
                self._models_status.color = Colors.WARNING
                self.update()
                return
            
            logging.info(f"[SETTINGS] Found {len(models)} models")
            
            # Update dropdown options
            current_model = self.config.get("gemini_model", "gemini-2.0-flash-lite")
            self._model_dropdown.options = [
                ft.DropdownOption(key=m['name'], text=m['display_name'])
                for m in models
            ]
            
            # Ensure current model is in the list, otherwise use first available
            model_names = [m['name'] for m in models]
            if current_model in model_names:
                self._model_dropdown.value = current_model
            elif model_names:
                self._model_dropdown.value = model_names[0]
                self.config.set("gemini_model", model_names[0])
            
            self._models_status.value = f"✓ {len(models)} models available"
            self._models_status.color = Colors.SUCCESS
            self.update()
            
        except Exception as ex:
            logging.error(f"[SETTINGS] Error refreshing models: {ex}")
            self._models_status.value = f"Error: {str(ex)[:50]}"
            self._models_status.color = Colors.ERROR
            self.update()
