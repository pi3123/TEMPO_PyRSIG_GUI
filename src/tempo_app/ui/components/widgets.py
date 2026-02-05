"""Reusable UI components for TEMPO Analyzer."""

import flet as ft
from ..theme import Colors, Spacing


class HelpTooltip(ft.IconButton):
    """A help icon (?) that shows help on hover or click."""
    
    def __init__(self, help_text: str, icon_size: int = 18):
        self._help_text = help_text
        super().__init__(
            icon=ft.Icons.HELP_OUTLINE,
            icon_size=icon_size,
            icon_color=Colors.ON_SURFACE_VARIANT,
            tooltip=help_text,
            style=ft.ButtonStyle(padding=0),
            on_click=self._show_help,
        )
    
    def _show_help(self, e):
        """Show help dialog on click."""
        def close_dlg(e):
            dlg.open = False
            self.page.update()
        
        dlg = ft.AlertDialog(
            modal=True,
            title=ft.Text("Help"),
            content=ft.Text(self._help_text, size=14),
            actions=[ft.TextButton("OK", on_click=close_dlg)],
        )
        self.page.overlay.append(dlg)
        dlg.open = True
        self.page.update()


class LabeledField(ft.Column):
    """A form field with a label and optional help tooltip.
    
    Usage:
        LabeledField(
            label="Hour Range",
            help_text="Select the UTC hours to download...",
            field=ft.Dropdown(...)
        )
    """
    
    def __init__(
        self,
        label: str,
        field: ft.Control,
        help_text: str = None,
        required: bool = False,
    ):
        label_row = ft.Row(
            controls=[
                ft.Text(
                    label + ("*" if required else ""),
                    size=14,
                    weight=ft.FontWeight.W_500,
                    color=Colors.ON_SURFACE,
                ),
            ],
            spacing=6,
        )
        
        if help_text:
            label_row.controls.append(HelpTooltip(help_text))
        
        super().__init__(
            controls=[label_row, field],
            spacing=6,
        )


class SectionCard(ft.Container):
    """A card container for grouping related form fields.
    
    Usage:
        SectionCard(
            title="Geographic Region",
            icon=ft.Icons.MAP,
            help_text="Define the area to download data for...",
            content=ft.Column([...])
        )
    """
    
    def __init__(
        self,
        title: str,
        content: ft.Control,
        icon: str = None,
        help_text: str = None,
        collapsed: bool = False,
    ):
        self._collapsed = collapsed
        self._content = content
        
        # Header row
        header_controls = []
        if icon:
            header_controls.append(ft.Icon(icon, size=20, color=Colors.PRIMARY))
        
        header_controls.append(
            ft.Text(
                title,
                size=16,
                weight=ft.FontWeight.W_600,
                color=Colors.ON_SURFACE,
            )
        )
        
        if help_text:
            header_controls.append(HelpTooltip(help_text))
        
        header_controls.append(ft.Container(expand=True))  # Spacer
        
        # Collapse toggle
        self._collapse_btn = ft.IconButton(
            icon=ft.Icons.EXPAND_LESS if not collapsed else ft.Icons.EXPAND_MORE,
            icon_size=20,
            icon_color=Colors.ON_SURFACE_VARIANT,
            on_click=self._toggle_collapse,
            tooltip="Collapse/Expand",
        )
        header_controls.append(self._collapse_btn)
        
        header = ft.Row(controls=header_controls, spacing=8)
        
        # Content wrapper
        self._content_wrapper = ft.Container(
            content=content,
            visible=not collapsed,
            padding=ft.padding.only(top=12),
        )
        
        super().__init__(
            content=ft.Column(
                controls=[header, self._content_wrapper],
                spacing=0,
            ),
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
    
    def _toggle_collapse(self, e):
        self._collapsed = not self._collapsed
        self._content_wrapper.visible = not self._collapsed
        self._collapse_btn.icon = (
            ft.Icons.EXPAND_LESS if not self._collapsed else ft.Icons.EXPAND_MORE
        )
        self.update()


class StatusLogPanel(ft.Container):
    """A panel showing real-time activity log with status updates.
    
    Usage:
        log = StatusLogPanel()
        log.add_info("Starting download...")
        log.add_success("Download complete!")
    """
    
    def __init__(self, max_entries: int = 50):
        self._entries: list[ft.Control] = []
        self._max_entries = max_entries
        
        self._log_column = ft.Column(
            controls=[],
            spacing=4,
            scroll=ft.ScrollMode.AUTO,
            auto_scroll=True,
        )
        
        super().__init__(
            content=ft.Column(
                controls=[
                    ft.Row(
                        controls=[
                            ft.Icon(ft.Icons.TERMINAL, size=18, color=Colors.ON_SURFACE_VARIANT),
                            ft.Text(
                                "Activity Log",
                                size=14,
                                weight=ft.FontWeight.W_500,
                                color=Colors.ON_SURFACE,
                            ),
                            HelpTooltip(
                                "Real-time updates showing what the application is doing. "
                                "Each line is timestamped so you can track progress. "
                                "Check mark = completed, Hourglass = waiting, X = error."
                            ),
                            ft.Container(expand=True),
                            ft.IconButton(
                                icon=ft.Icons.DELETE_SWEEP,
                                icon_size=18,
                                icon_color=Colors.ON_SURFACE_VARIANT,
                                tooltip="Clear log",
                                on_click=self._clear_log,
                            ),
                        ],
                        spacing=6,
                    ),
                    ft.Container(
                        content=self._log_column,
                        bgcolor=Colors.BACKGROUND,
                        border_radius=8,
                        padding=12,
                        expand=True,
                        height=200,
                    ),
                ],
                spacing=8,
            ),
            bgcolor=Colors.SURFACE,
            border_radius=12,
            border=ft.Border(
                left=ft.BorderSide(1, Colors.BORDER),
                top=ft.BorderSide(1, Colors.BORDER),
                right=ft.BorderSide(1, Colors.BORDER),
                bottom=ft.BorderSide(1, Colors.BORDER),
            ),
            padding=16,
            expand=True,
        )
    
    def _add_entry(self, icon: str, message: str, color: str):
        """Add an entry to the log."""
        from datetime import datetime
        timestamp = datetime.now().strftime("%H:%M:%S")
        
        entry = ft.Row(
            controls=[
                ft.Text(timestamp, size=11, color=Colors.ON_SURFACE_VARIANT, font_family="monospace"),
                ft.Text(icon, size=12),
                ft.Text(message, size=12, color=color, expand=True),
            ],
            spacing=8,
        )
        
        self._entries.append(entry)
        if len(self._entries) > self._max_entries:
            self._entries = self._entries[-self._max_entries:]
        
        self._log_column.controls = self._entries.copy()
        self.update()
    
    def add_info(self, message: str):
        """Add an info message."""
        self._add_entry("[>]", message, Colors.ON_SURFACE_VARIANT)
    
    def add_success(self, message: str):
        """Add a success message."""
        self._add_entry("[OK]", message, Colors.SUCCESS)
    
    def add_warning(self, message: str):
        """Add a warning message."""
        self._add_entry("[!]", message, Colors.WARNING)
    
    def add_error(self, message: str):
        """Add an error message."""
        self._add_entry("[X]", message, Colors.ERROR)
    
    def add_progress(self, message: str):
        """Add a progress/waiting message."""
        self._add_entry("[...]", message, Colors.INFO)
    
    def _clear_log(self, e):
        """Clear all log entries."""
        self._entries.clear()
        self._log_column.controls.clear()
        self.update()

    def get_handler(self):
        """Get a logging handler that outputs to this panel."""
        return StatusLogHandler(self)


class StatusLogHandler:
    """A logging handler that outputs to a StatusLogPanel."""
    # This is a dummy class definition that will be replaced by the actual inheritance
    # But since we can't import logging at the top level without potential circular imports or
    # messing up the file, we'll do it inside.
    # update: Actually standard library imports are fine.
    pass

import logging

class StatusLogHandler(logging.Handler):
    """A logging handler that outputs to a StatusLogPanel."""
    
    def __init__(self, panel: StatusLogPanel):
        super().__init__()
        self.panel = panel
        self.setFormatter(logging.Formatter('%(message)s'))
        
    def emit(self, record):
        try:
            msg = self.format(record)
            # Map log levels to panel methods
            if record.levelno >= logging.ERROR:
                self.panel.add_error(msg)
            elif record.levelno >= logging.WARNING:
                self.panel.add_warning(msg)
            elif record.levelno >= logging.INFO:
                self.panel.add_info(msg)
            else:
                self.panel.add_info(msg) # DEBUG etc
        except Exception:
            self.handleError(record)


class ProgressPanel(ft.Container):
    """A progress panel showing overall download progress.
    
    Shows:
    - Progress bar
    - Current step description
    - Estimated time remaining
    """
    
    def __init__(self):
        self._progress_bar = ft.ProgressBar(
            value=0,
            color=Colors.PRIMARY,
            bgcolor=Colors.SURFACE_VARIANT,
        )
        
        self._status_text = ft.Text(
            "Ready to start",
            size=14,
            color=Colors.ON_SURFACE,
        )
        
        self._detail_text = ft.Text(
            "",
            size=12,
            color=Colors.ON_SURFACE_VARIANT,
        )
        
        self._eta_text = ft.Text(
            "",
            size=12,
            color=Colors.ON_SURFACE_VARIANT,
        )
        
        super().__init__(
            content=ft.Column(
                controls=[
                    self._status_text,
                    self._progress_bar,
                    ft.Row(
                        controls=[
                            self._detail_text,
                            ft.Container(expand=True),
                            self._eta_text,
                        ],
                    ),
                ],
                spacing=8,
            ),
            visible=False,
            padding=ft.padding.only(top=16),
        )
    
    def show(self):
        """Show the progress panel."""
        self.visible = True
        self.update()
    
    def hide(self):
        """Hide the progress panel."""
        self.visible = False
        self.update()
    
    def update_progress(
        self,
        progress: float,
        status: str,
        detail: str = "",
        eta_seconds: float = None,
    ):
        """Update the progress display."""
        self._progress_bar.value = progress
        self._status_text.value = status
        self._detail_text.value = detail
        
        if eta_seconds is not None:
            if eta_seconds < 60:
                self._eta_text.value = f"~{int(eta_seconds)}s remaining"
            else:
                self._eta_text.value = f"~{int(eta_seconds / 60)}m remaining"
        else:
            self._eta_text.value = ""
        
        self.update()


class WorkerProgressPanel(ft.Container):
    """Multi-worker progress panel showing individual bars per download worker.
    
    Shows:
    - Overall progress bar
    - Individual worker status rows
    - Completed/Total count
    """
    
    def __init__(self, num_workers: int = 4):
        self._num_workers = num_workers
        self._worker_rows: list[ft.Container] = []
        
        # Overall progress
        self._overall_bar = ft.ProgressBar(
            value=0,
            color=Colors.PRIMARY,
            bgcolor=Colors.SURFACE_VARIANT,
        )
        
        self._overall_text = ft.Text(
            "Ready to download",
            size=14,
            weight=ft.FontWeight.W_500,
            color=Colors.ON_SURFACE,
        )
        
        self._count_text = ft.Text(
            "0/0",
            size=13,
            color=Colors.ON_SURFACE_VARIANT,
        )
        
        # Create worker rows
        for i in range(num_workers):
            worker_bar = ft.ProgressBar(
                value=None,  # Indeterminate
                color=Colors.INFO,
                bgcolor=Colors.SURFACE_VARIANT,
                width=200,
            )
            worker_label = ft.Text(
                f"Worker {i+1}: Idle",
                size=12,
                color=Colors.ON_SURFACE_VARIANT,
                width=250,
            )
            row = ft.Container(
                content=ft.Row([
                    worker_label,
                    worker_bar,
                ], spacing=12),
                visible=False,
            )
            self._worker_rows.append({
                "container": row,
                "label": worker_label,
                "bar": worker_bar,
            })
        
        self._worker_column = ft.Column(
            controls=[r["container"] for r in self._worker_rows],
            spacing=6,
        )
        
        super().__init__(
            content=ft.Column(
                controls=[
                    ft.Row([
                        ft.Icon(ft.Icons.CLOUD_DOWNLOAD, size=18, color=Colors.PRIMARY),
                        ft.Text("Download Progress", size=14, weight=ft.FontWeight.W_500, color=Colors.ON_SURFACE),
                        ft.Container(expand=True),
                        self._count_text,
                    ], spacing=8),
                    self._overall_text,
                    self._overall_bar,
                    ft.Container(height=8),
                    self._worker_column,
                ],
                spacing=6,
            ),
            visible=False,
            bgcolor=Colors.SURFACE,
            border_radius=12,
            border=ft.Border.all(1, Colors.BORDER),
            padding=16,
        )

    def set_workers(self, count: int):
        """Update the number of worker rows."""
        if count == self._num_workers:
            return
            
        self._num_workers = count
        self._worker_rows.clear()
        
        for i in range(count):
            worker_bar = ft.ProgressBar(
                value=None,
                color=Colors.INFO,
                bgcolor=Colors.SURFACE_VARIANT,
                width=200,
            )
            worker_label = ft.Text(
                f"Worker {i+1}: Idle",
                size=12,
                color=Colors.ON_SURFACE_VARIANT,
                width=250,
            )
            row = ft.Container(
                content=ft.Row([
                    worker_label,
                    worker_bar,
                ], spacing=12),
                visible=False, # Hidden until show() is called
            )
            self._worker_rows.append({
                "container": row,
                "label": worker_label,
                "bar": worker_bar,
            })
            
        self._worker_column.controls = [r["container"] for r in self._worker_rows]
        self.update()
    
    def show(self, total: int = 0):
        """Show the panel and reset state."""
        self.visible = True
        self._overall_bar.value = 0
        self._overall_text.value = "Starting download..."
        self._count_text.value = f"0/{total}"
        # Show worker rows
        for r in self._worker_rows:
            r["container"].visible = True
            r["label"].value = "Idle"
            r["bar"].value = None  # Indeterminate
        self.update()
    
    def hide(self):
        """Hide the panel."""
        self.visible = False
        for r in self._worker_rows:
            r["container"].visible = False
        self.update()
    
    def update_overall(self, completed: int, total: int, status: str = ""):
        """Update overall progress."""
        progress = completed / total if total > 0 else 0
        self._overall_bar.value = progress
        self._count_text.value = f"{completed}/{total}"
        if status:
            self._overall_text.value = status
        self.update()
    
    def update_worker(self, worker_id: int, status: str, active: bool = True):
        """Update a specific worker's status."""
        if 0 <= worker_id < len(self._worker_rows):
            row = self._worker_rows[worker_id]
            row["label"].value = f"W{worker_id+1}: {status}"
            if active:
                row["bar"].value = None  # Indeterminate = active
                row["label"].color = Colors.ON_SURFACE
            else:
                row["bar"].value = 1.0  # Full = done
                row["label"].color = Colors.ON_SURFACE_VARIANT
            self.update()
    
    def complete(self, success_count: int, total: int):
        """Mark download as complete."""
        self._overall_bar.value = 1.0
        self._overall_text.value = f"✅ Downloaded {success_count}/{total} granules"
        self._overall_text.color = Colors.SUCCESS
        for r in self._worker_rows:
            r["container"].visible = False
        self.update()


class DaySelector(ft.Row):
    """Toggle chips for selecting days of the week.
    
    Usage:
        days = DaySelector(value=[0,1,2,3,4])  # Mon-Fri selected
        selected = days.value  # Returns list of indices
    """
    
    DAY_LABELS = ["M", "T", "W", "T", "F", "S", "S"]
    DAY_NAMES = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]
    
    def __init__(self, value: list[int] = None, on_change=None):
        self._selected = set(value) if value else {0, 1, 2, 3, 4}  # Default weekdays
        self._on_change = on_change
        self._chips = []
        
        for i, label in enumerate(self.DAY_LABELS):
            chip = ft.Container(
                content=ft.Text(
                    label,
                    size=13,
                    weight=ft.FontWeight.W_600,
                    text_align=ft.TextAlign.CENTER,
                ),
                width=32,
                height=32,
                border_radius=16,
                alignment=ft.Alignment(0, 0),
                on_click=lambda e, idx=i: self._toggle(idx),
                tooltip=self.DAY_NAMES[i],
            )
            self._chips.append(chip)
        
        self._update_chip_styles()
        
        super().__init__(
            controls=self._chips,
            spacing=4,
        )
    
    def _toggle(self, idx: int):
        """Toggle a day on/off."""
        if idx in self._selected:
            self._selected.discard(idx)
        else:
            self._selected.add(idx)
        self._update_chip_styles()
        if self._on_change:
            self._on_change(self.value)
        self.update()
    
    def _update_chip_styles(self):
        """Update visual styles based on selection."""
        for i, chip in enumerate(self._chips):
            if i in self._selected:
                chip.bgcolor = Colors.PRIMARY
                chip.content.color = Colors.ON_PRIMARY
            else:
                chip.bgcolor = Colors.SURFACE_VARIANT
                chip.content.color = Colors.ON_SURFACE_VARIANT
    
    @property
    def value(self) -> list[int]:
        """Get selected days as sorted list of indices."""
        return sorted(self._selected)
    
    @value.setter
    def value(self, days: list[int]):
        """Set selected days."""
        self._selected = set(days)
        self._update_chip_styles()
        self.update()
    
    def select_weekdays(self):
        """Select Mon-Fri."""
        self.value = [0, 1, 2, 3, 4]
    
    def select_weekends(self):
        """Select Sat-Sun."""
        self.value = [5, 6]
    
    def select_all(self):
        """Select all days."""
        self.value = [0, 1, 2, 3, 4, 5, 6]


class MultiSelectChipGroup(ft.Container):
    """A row of toggleable chips for multi-selection.
    
    Usage:
        chips = MultiSelectChipGroup(["Mean", "Std", "Count"])
        selected = chips.value
    """
    
    def __init__(self, options: list[str], initial_value: list[str] = None):
        self._options = options
        self._selected = set(initial_value) if initial_value else set(options[:1])
        self._chips = []
        
        for opt in options:
            chip = ft.Container(
                content=ft.Text(opt, size=12, weight=ft.FontWeight.W_500),
                padding=ft.padding.symmetric(horizontal=12, vertical=6),
                border_radius=16,
                on_click=lambda e, o=opt: self._toggle(o),
                data=opt,
            )
            self._chips.append(chip)
            
        self._update_styles()
        
        super().__init__(
            content=ft.Row(self._chips, spacing=8, wrap=True)
        )
        
    def _toggle(self, option: str):
        if option in self._selected:
            if len(self._selected) > 1: # Prevent empty selection
                self._selected.discard(option)
        else:
            self._selected.add(option)
        self._update_styles()
        self.update()
        
    def _update_styles(self):
        for chip in self._chips:
            opt = chip.data
            if opt in self._selected:
                chip.bgcolor = Colors.PRIMARY_CONTAINER
                chip.content.color = Colors.ON_PRIMARY_CONTAINER
                chip.border = ft.border.all(1, Colors.PRIMARY)
            else:
                chip.bgcolor = Colors.SURFACE_VARIANT
                chip.content.color = Colors.ON_SURFACE_VARIANT
                chip.border = ft.border.all(1, Colors.BORDER)
                
    @property
    def value(self) -> list[str]:
        return [opt for opt in self._options if opt in self._selected]

