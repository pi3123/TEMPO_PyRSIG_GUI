"""Theme configuration for TEMPO Analyzer.

Material Design 3 color scheme optimized for data visualization.
"""

import flet as ft


# =============================================================================
# Color Palette (Dark Theme First)
# =============================================================================

class Colors:
    """Application color constants (Light Mode)."""
    
    # Primary palette
    PRIMARY = "#5E35B1"           # Deep Purple 600
    PRIMARY_CONTAINER = "#EDE7F6"
    ON_PRIMARY = "#FFFFFF"
    
    # Surface colors (Light Mode)
    BACKGROUND = "#FAFAFA"        # Light gray
    SURFACE = "#FFFFFF"           # White card backgrounds
    SURFACE_VARIANT = "#F5F5F5"   # Slightly darker surfaces
    ON_SURFACE = "#212121"        # Primary text (dark)
    ON_SURFACE_VARIANT = "#757575"  # Secondary text (gray)
    
    # Status colors
    SUCCESS = "#43A047"           # Green
    SUCCESS_CONTAINER = "#E8F5E9"
    WARNING = "#FB8C00"           # Orange
    WARNING_CONTAINER = "#FFF3E0"
    ERROR = "#E53935"             # Red
    ERROR_CONTAINER = "#FFEBEE"
    INFO = "#1E88E5"              # Blue
    
    # "On" colors for containers
    ON_PRIMARY_CONTAINER = "#311B92"  # Deep Purple 900
    ON_SUCCESS_CONTAINER = "#1B5E20"  # Green 900
    ON_WARNING_CONTAINER = "#E65100"  # Orange 900
    ON_ERROR_CONTAINER = "#B71C1C"    # Red 900
    
    # Map-specific colors
    NO2_LOW = "#1A237E"           # Deep blue
    NO2_HIGH = "#FFD600"          # Yellow
    HCHO_LOW = "#311B92"          # Deep purple
    HCHO_HIGH = "#FF6D00"         # Deep orange
    FNR_VOC = "#3F51B5"           # Blue (VOC-limited)
    FNR_TRANSITION = "#9E9E9E"    # Gray (transition)
    FNR_NOX = "#F44336"           # Red (NOx-limited)
    
    # UI elements
    BORDER = "#E0E0E0"
    DIVIDER = "#EEEEEE"
    CARD_SHADOW = "#00000020"


class LightColors:
    """Light theme colors (for future use)."""
    
    PRIMARY = "#5E35B1"           # Deep Purple 600
    PRIMARY_CONTAINER = "#EDE7F6"
    ON_PRIMARY = "#FFFFFF"
    
    BACKGROUND = "#FAFAFA"
    SURFACE = "#FFFFFF"
    SURFACE_VARIANT = "#F5F5F5"
    ON_SURFACE = "#212121"
    ON_SURFACE_VARIANT = "#757575"
    
    SUCCESS = "#43A047"
    WARNING = "#FB8C00"
    ERROR = "#E53935"
    INFO = "#1E88E5"
    
    BORDER = "#E0E0E0"
    DIVIDER = "#EEEEEE"


# =============================================================================
# Typography
# =============================================================================

class Typography:
    """Font configurations."""
    
    # Font families
    FAMILY_PRIMARY = "Inter, Roboto, sans-serif"
    FAMILY_MONO = "JetBrains Mono, Consolas, monospace"
    
    # Font sizes
    DISPLAY_LARGE = 57
    DISPLAY_MEDIUM = 45
    DISPLAY_SMALL = 36
    HEADLINE_LARGE = 32
    HEADLINE_MEDIUM = 28
    HEADLINE_SMALL = 24
    TITLE_LARGE = 22
    TITLE_MEDIUM = 16
    TITLE_SMALL = 14
    BODY_LARGE = 16
    BODY_MEDIUM = 14
    BODY_SMALL = 12
    LABEL_LARGE = 14
    LABEL_MEDIUM = 12
    LABEL_SMALL = 11


# =============================================================================
# Spacing & Sizing
# =============================================================================

class Spacing:
    """Spacing constants."""
    
    XS = 4
    SM = 8
    MD = 16
    LG = 24
    XL = 32
    XXL = 48
    
    # Page padding
    PAGE_HORIZONTAL = 24
    PAGE_VERTICAL = 16
    
    # Card padding
    CARD_PADDING = 16
    
    # Navigation rail width
    NAV_RAIL_WIDTH = 80
    NAV_RAIL_EXPANDED = 200


class Sizing:
    """Component size constants."""
    
    # Window
    WINDOW_MIN_WIDTH = 1200
    WINDOW_MIN_HEIGHT = 700
    WINDOW_DEFAULT_WIDTH = 1600
    WINDOW_DEFAULT_HEIGHT = 900
    
    # Cards
    CARD_BORDER_RADIUS = 12
    
    # Buttons
    BUTTON_HEIGHT = 40
    BUTTON_BORDER_RADIUS = 8
    
    # Inputs
    INPUT_HEIGHT = 48
    INPUT_BORDER_RADIUS = 8
    
    # Icons
    ICON_SM = 16
    ICON_MD = 24
    ICON_LG = 32


# =============================================================================
# Theme Builder
# =============================================================================

def create_dark_theme() -> ft.Theme:
    """Create the dark theme for the application."""
    return ft.Theme(
        color_scheme_seed=Colors.PRIMARY,
        visual_density=ft.VisualDensity.COMFORTABLE,
    )


def create_light_theme(font_scale: float = 1.0) -> ft.Theme:
    """Create the light theme for the application."""
    t = ft.Theme(
        color_scheme_seed=LightColors.PRIMARY,
        visual_density=ft.VisualDensity.COMFORTABLE,
    )
    
    # Scale typography
    t.text_theme = ft.TextTheme(
        display_large=ft.TextStyle(size=Typography.DISPLAY_LARGE * font_scale),
        display_medium=ft.TextStyle(size=Typography.DISPLAY_MEDIUM * font_scale),
        display_small=ft.TextStyle(size=Typography.DISPLAY_SMALL * font_scale),
        headline_large=ft.TextStyle(size=Typography.HEADLINE_LARGE * font_scale),
        headline_medium=ft.TextStyle(size=Typography.HEADLINE_MEDIUM * font_scale),
        headline_small=ft.TextStyle(size=Typography.HEADLINE_SMALL * font_scale),
        title_large=ft.TextStyle(size=Typography.TITLE_LARGE * font_scale),
        title_medium=ft.TextStyle(size=Typography.TITLE_MEDIUM * font_scale),
        title_small=ft.TextStyle(size=Typography.TITLE_SMALL * font_scale),
        body_large=ft.TextStyle(size=Typography.BODY_LARGE * font_scale),
        body_medium=ft.TextStyle(size=Typography.BODY_MEDIUM * font_scale),
        body_small=ft.TextStyle(size=Typography.BODY_SMALL * font_scale),
        label_large=ft.TextStyle(size=Typography.LABEL_LARGE * font_scale),
        label_medium=ft.TextStyle(size=Typography.LABEL_MEDIUM * font_scale),
        label_small=ft.TextStyle(size=Typography.LABEL_SMALL * font_scale),
    )
    return t


# =============================================================================
# Reusable Styles
# =============================================================================

def card_style(
    padding: int = Spacing.CARD_PADDING,
    bgcolor: str = Colors.SURFACE,
) -> dict:
    """Get common card styling."""
    return {
        "bgcolor": bgcolor,
        "border_radius": Sizing.CARD_BORDER_RADIUS,
        "padding": padding,
        "border": ft.border.all(1, Colors.BORDER),
    }


def section_header_style() -> dict:
    """Get section header text style."""
    return {
        "size": Typography.TITLE_MEDIUM,
        "weight": ft.FontWeight.W_600,
        "color": Colors.ON_SURFACE,
    }


def body_text_style() -> dict:
    """Get body text style."""
    return {
        "size": Typography.BODY_MEDIUM,
        "color": Colors.ON_SURFACE_VARIANT,
    }


def primary_button_style() -> ft.ButtonStyle:
    """Get primary button style."""
    return ft.ButtonStyle(
        color=Colors.ON_PRIMARY,
        bgcolor=Colors.PRIMARY,
        shape=ft.RoundedRectangleBorder(radius=Sizing.BUTTON_BORDER_RADIUS),
        padding=ft.padding.symmetric(horizontal=Spacing.LG, vertical=Spacing.SM),
    )


def secondary_button_style() -> ft.ButtonStyle:
    """Get secondary button style."""
    return ft.ButtonStyle(
        color=Colors.PRIMARY,
        bgcolor=Colors.SURFACE_VARIANT,
        shape=ft.RoundedRectangleBorder(radius=Sizing.BUTTON_BORDER_RADIUS),
        padding=ft.padding.symmetric(horizontal=Spacing.MD, vertical=Spacing.SM),
    )
