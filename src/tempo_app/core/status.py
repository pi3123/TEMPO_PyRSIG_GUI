"""Status manager for verbose progress updates.

Provides real-time feedback to the UI so the app never looks stuck.
"""

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Callable, Optional
import time


class StatusCategory(Enum):
    """Categories for status messages."""
    DOWNLOAD = "download"
    PROCESS = "process"
    PLOT = "plot"
    EXPORT = "export"
    ASSETS = "assets"
    SYSTEM = "system"


class StatusLevel(Enum):
    """Severity level of status messages."""
    INFO = "info"          # Normal operation
    SUCCESS = "success"    # Completed successfully
    WARNING = "warning"    # Non-fatal issue
    ERROR = "error"        # Failed operation
    PROGRESS = "progress"  # Progress update with bar


@dataclass
class StatusEvent:
    """A single status update event."""
    category: StatusCategory
    message: str
    level: StatusLevel = StatusLevel.INFO
    progress: Optional[float] = None  # 0.0-1.0 for progress bar
    timestamp: datetime = field(default_factory=datetime.now)
    details: Optional[str] = None  # Additional details
    
    @property
    def icon(self) -> str:
        """Get emoji icon for this status level."""
        icons = {
            StatusLevel.INFO: "🔄",
            StatusLevel.SUCCESS: "✓",
            StatusLevel.WARNING: "⚠️",
            StatusLevel.ERROR: "❌",
            StatusLevel.PROGRESS: "📥",
        }
        return icons.get(self.level, "•")
    
    @property
    def time_str(self) -> str:
        """Formatted timestamp."""
        return self.timestamp.strftime("%H:%M:%S")
    
    def format_message(self) -> str:
        """Format message with icon and timestamp."""
        return f"⏱️ {self.time_str}  {self.icon} {self.message}"


class StatusManager:
    """Central manager for status updates throughout the application.
    
    Usage:
        status = StatusManager()
        status.on_update = lambda event: print(event.format_message())
        
        status.info("download", "Starting download...")
        status.progress("download", "Downloading file...", 0.5)
        status.success("download", "Download complete!")
    """
    
    def __init__(self):
        self._listeners: list[Callable[[StatusEvent], None]] = []
        self._history: list[StatusEvent] = []
        self._max_history = 100
        
        # Current operation tracking
        self._current_operation: Optional[str] = None
        self._operation_start: Optional[float] = None
        self._total_steps: int = 0
        self._completed_steps: int = 0
    
    def add_listener(self, callback: Callable[[StatusEvent], None]) -> None:
        """Add a listener for status updates."""
        self._listeners.append(callback)
    
    def remove_listener(self, callback: Callable[[StatusEvent], None]) -> None:
        """Remove a status listener."""
        if callback in self._listeners:
            self._listeners.remove(callback)
    
    def _emit(self, event: StatusEvent) -> None:
        """Emit an event to all listeners."""
        self._history.append(event)
        if len(self._history) > self._max_history:
            self._history = self._history[-self._max_history:]
        
        for listener in self._listeners:
            try:
                listener(event)
            except Exception:
                pass  # Don't let listener errors break the app
    
    # ==========================================================================
    # Convenience methods for different status levels
    # ==========================================================================
    
    def info(self, category: str, message: str, details: str = None) -> None:
        """Emit an info status."""
        self._emit(StatusEvent(
            category=StatusCategory(category),
            message=message,
            level=StatusLevel.INFO,
            details=details,
        ))
    
    def success(self, category: str, message: str, details: str = None) -> None:
        """Emit a success status."""
        self._emit(StatusEvent(
            category=StatusCategory(category),
            message=message,
            level=StatusLevel.SUCCESS,
            details=details,
        ))
    
    def warning(self, category: str, message: str, details: str = None) -> None:
        """Emit a warning status."""
        self._emit(StatusEvent(
            category=StatusCategory(category),
            message=message,
            level=StatusLevel.WARNING,
            details=details,
        ))
    
    def error(self, category: str, message: str, details: str = None) -> None:
        """Emit an error status."""
        self._emit(StatusEvent(
            category=StatusCategory(category),
            message=message,
            level=StatusLevel.ERROR,
            details=details,
        ))
    
    def progress(self, category: str, message: str, progress: float, details: str = None) -> None:
        """Emit a progress update (0.0-1.0)."""
        self._emit(StatusEvent(
            category=StatusCategory(category),
            message=message,
            level=StatusLevel.PROGRESS,
            progress=min(1.0, max(0.0, progress)),
            details=details,
        ))
    
    # ==========================================================================
    # Operation tracking
    # ==========================================================================
    
    def start_operation(self, name: str, total_steps: int) -> None:
        """Start tracking a multi-step operation."""
        self._current_operation = name
        self._operation_start = time.time()
        self._total_steps = total_steps
        self._completed_steps = 0
        self.info("system", f"Starting: {name}")
    
    def step_completed(self, message: str = None) -> None:
        """Mark a step as completed."""
        self._completed_steps += 1
        if message:
            progress = self._completed_steps / max(1, self._total_steps)
            self.progress("system", message, progress)
    
    def end_operation(self, success: bool = True) -> None:
        """End the current operation."""
        if self._current_operation and self._operation_start:
            elapsed = time.time() - self._operation_start
            if success:
                self.success("system", f"Completed: {self._current_operation} ({elapsed:.1f}s)")
            else:
                self.error("system", f"Failed: {self._current_operation} ({elapsed:.1f}s)")
        
        self._current_operation = None
        self._operation_start = None
        self._total_steps = 0
        self._completed_steps = 0
    
    @property
    def current_progress(self) -> float:
        """Get current operation progress (0.0-1.0)."""
        if self._total_steps == 0:
            return 0.0
        return self._completed_steps / self._total_steps
    
    @property
    def estimated_remaining(self) -> Optional[float]:
        """Estimate remaining time in seconds."""
        if not self._operation_start or self._completed_steps == 0:
            return None
        
        elapsed = time.time() - self._operation_start
        rate = self._completed_steps / elapsed
        remaining_steps = self._total_steps - self._completed_steps
        
        if rate > 0:
            return remaining_steps / rate
        return None
    
    def get_history(self, limit: int = 50) -> list[StatusEvent]:
        """Get recent status history."""
        return self._history[-limit:]
    
    def clear_history(self) -> None:
        """Clear status history."""
        self._history.clear()


# Global status manager instance
_global_status: Optional[StatusManager] = None


def get_status_manager() -> StatusManager:
    """Get the global status manager instance."""
    global _global_status
    if _global_status is None:
        _global_status = StatusManager()
    return _global_status
