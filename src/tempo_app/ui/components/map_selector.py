"""Interactive map component for bounding box selection."""

import flet as ft
import flet.canvas as cv
from typing import Callable, Optional
from dataclasses import dataclass

from ..theme import Colors, Spacing


@dataclass
class MapBounds:
    """Geographic bounds for the map display."""
    west: float = -130.0   # Western edge of visible map
    east: float = -60.0    # Eastern edge
    south: float = 20.0    # Southern edge
    north: float = 55.0    # Northern edge
    
    @property
    def width(self) -> float:
        return self.east - self.west
    
    @property
    def height(self) -> float:
        return self.north - self.south


# North America base map bounds
NA_BOUNDS = MapBounds(west=-130, east=-60, south=20, north=55)


class MapBBoxSelector(ft.Container):
    """Interactive map for selecting a geographic bounding box.
    
    Shows a simplified map of North America with a draggable rectangle
    that users can resize to select their area of interest.
    """
    
    def __init__(
        self,
        initial_bbox: tuple[float, float, float, float] = (-119.68, 32.23, -116.38, 35.73),
        on_change: Optional[Callable[[float, float, float, float], None]] = None,
        width: int = 400,
        height: int = 280,
    ):
        super().__init__()
        
        self._map_bounds = NA_BOUNDS
        self._bbox_west, self._bbox_south, self._bbox_east, self._bbox_north = initial_bbox
        self._on_change = on_change
        self._map_width = width
        self._map_height = height
        
        # Drag state
        self._dragging = False
        self._drag_handle: Optional[str] = None  # 'nw', 'ne', 'sw', 'se', 'move'
        self._drag_start_x = 0
        self._drag_start_y = 0
        self._drag_start_bbox = (0, 0, 0, 0)
        
        self._build()
    
    def _build(self):
        """Build the map component."""
        # Canvas for drawing
        self._canvas = cv.Canvas(
            shapes=self._get_shapes(),
            width=self._map_width,
            height=self._map_height,
        )
        
        # Gesture detector for mouse interactions
        self._gesture = ft.GestureDetector(
            content=self._canvas,
            on_pan_start=self._on_pan_start,
            on_pan_update=self._on_pan_update,
            on_pan_end=self._on_pan_end,
        )
        
        # Coordinate display
        self._coord_text = ft.Text(
            self._format_coords(),
            size=12,
            color=Colors.ON_SURFACE_VARIANT,
            text_align=ft.TextAlign.CENTER,
        )
        
        self.content = ft.Column(
            controls=[
                ft.Container(
                    content=self._gesture,
                    bgcolor="#1a2744",  # Ocean blue
                    border_radius=8,
                    clip_behavior=ft.ClipBehavior.HARD_EDGE,
                ),
                ft.Container(height=8),
                self._coord_text,
                ft.Text(
                    "Drag corners to resize | Drag center to move",
                    size=11,
                    color=Colors.ON_SURFACE_VARIANT,
                    italic=True,
                    text_align=ft.TextAlign.CENTER,
                ),
            ],
            horizontal_alignment=ft.CrossAxisAlignment.CENTER,
            spacing=4,
        )
    
    def _get_shapes(self) -> list:
        """Generate canvas shapes for the map and bbox."""
        shapes = []
        
        # Background fill (ocean) - already done via container bgcolor
        
        # Draw simplified land masses as rectangles (for CONUS approximation)
        shapes.extend(self._draw_land())
        
        # Draw the selection bbox
        shapes.extend(self._draw_bbox())
        
        return shapes
    
    def _draw_land(self) -> list:
        """Draw simplified land representation."""
        shapes = []
        
        # Draw a simplified representation of continental US
        # Using rectangles to approximate major regions
        land_paint = ft.Paint(color="#2d4a3e", style=ft.PaintingStyle.FILL)
        border_paint = ft.Paint(color="#4a6b5a", style=ft.PaintingStyle.STROKE, stroke_width=1)
        
        # Main CONUS landmass (simplified rectangle)
        x1, y1 = self._geo_to_pixel(-125, 49)
        x2, y2 = self._geo_to_pixel(-67, 25)
        shapes.append(cv.Rect(x=x1, y=y1, width=x2-x1, height=y2-y1, paint=land_paint))
        shapes.append(cv.Rect(x=x1, y=y1, width=x2-x1, height=y2-y1, paint=border_paint))
        
        # Florida peninsula
        fx1, fy1 = self._geo_to_pixel(-88, 31)
        fx2, fy2 = self._geo_to_pixel(-80, 24)
        shapes.append(cv.Rect(x=fx1, y=fy1, width=fx2-fx1, height=fy2-fy1, paint=land_paint))
        
        # Texas extension
        tx1, ty1 = self._geo_to_pixel(-106, 32)
        tx2, ty2 = self._geo_to_pixel(-93, 26)
        shapes.append(cv.Rect(x=tx1, y=ty1, width=tx2-tx1, height=ty2-ty1, paint=land_paint))
        
        # Canada (simplified)
        cx1, cy1 = self._geo_to_pixel(-130, 55)
        cx2, cy2 = self._geo_to_pixel(-60, 49)
        shapes.append(cv.Rect(x=cx1, y=cy1, width=cx2-cx1, height=cy2-cy1, paint=land_paint))
        shapes.append(cv.Rect(x=cx1, y=cy1, width=cx2-cx1, height=cy2-cy1, paint=border_paint))
        
        # Mexico (partial)
        mx1, my1 = self._geo_to_pixel(-118, 25)
        mx2, my2 = self._geo_to_pixel(-86, 20)
        shapes.append(cv.Rect(x=mx1, y=my1, width=mx2-mx1, height=my2-my1, paint=land_paint))
        
        # Add grid lines
        grid_paint = ft.Paint(color="#ffffff15", style=ft.PaintingStyle.STROKE, stroke_width=0.5)
        
        # Longitude lines every 10 degrees
        for lon in range(-130, -59, 10):
            x1, y1 = self._geo_to_pixel(lon, self._map_bounds.south)
            x2, y2 = self._geo_to_pixel(lon, self._map_bounds.north)
            shapes.append(cv.Line(x1, y1, x2, y2, paint=grid_paint))
        
        # Latitude lines every 10 degrees
        for lat in range(20, 56, 10):
            x1, y1 = self._geo_to_pixel(self._map_bounds.west, lat)
            x2, y2 = self._geo_to_pixel(self._map_bounds.east, lat)
            shapes.append(cv.Line(x1, y1, x2, y2, paint=grid_paint))
        
        # Add some city markers for reference
        cities = [
            (-118.24, 34.05, "LA"),
            (-122.42, 37.77, "SF"),
            (-73.94, 40.67, "NY"),
            (-87.63, 41.88, "CHI"),
            (-95.37, 29.76, "HOU"),
        ]
        
        marker_paint = ft.Paint(color="#ffffff80")
        for lon, lat, name in cities:
            x, y = self._geo_to_pixel(lon, lat)
            shapes.append(cv.Circle(x, y, 3, paint=marker_paint))
        
        return shapes
    
    def _draw_bbox(self) -> list:
        """Draw the selection bounding box with handles."""
        shapes = []
        
        # Get pixel coordinates
        x1, y1 = self._geo_to_pixel(self._bbox_west, self._bbox_north)
        x2, y2 = self._geo_to_pixel(self._bbox_east, self._bbox_south)
        
        # Selection rectangle fill
        shapes.append(
            cv.Rect(
                x=x1, y=y1,
                width=x2 - x1,
                height=y2 - y1,
                paint=ft.Paint(color="#7C4DFF40"),  # Semi-transparent purple
            )
        )
        
        # Selection rectangle border
        shapes.append(
            cv.Rect(
                x=x1, y=y1,
                width=x2 - x1,
                height=y2 - y1,
                paint=ft.Paint(
                    color=Colors.PRIMARY,
                    style=ft.PaintingStyle.STROKE,
                    stroke_width=2,
                ),
            )
        )
        
        # Corner handles
        handle_size = 8
        handle_paint = ft.Paint(color=Colors.PRIMARY, style=ft.PaintingStyle.FILL)
        handle_border = ft.Paint(color="#ffffff", style=ft.PaintingStyle.STROKE, stroke_width=1)
        
        corners = [
            (x1, y1),  # NW
            (x2, y1),  # NE
            (x1, y2),  # SW
            (x2, y2),  # SE
        ]
        
        for cx, cy in corners:
            shapes.append(
                cv.Rect(
                    x=cx - handle_size/2,
                    y=cy - handle_size/2,
                    width=handle_size,
                    height=handle_size,
                    paint=handle_paint,
                )
            )
            shapes.append(
                cv.Rect(
                    x=cx - handle_size/2,
                    y=cy - handle_size/2,
                    width=handle_size,
                    height=handle_size,
                    paint=handle_border,
                )
            )
        
        return shapes
    
    def _geo_to_pixel(self, lon: float, lat: float) -> tuple[float, float]:
        """Convert geographic coordinates to pixel coordinates."""
        x = ((lon - self._map_bounds.west) / self._map_bounds.width) * self._map_width
        y = ((self._map_bounds.north - lat) / self._map_bounds.height) * self._map_height
        return x, y
    
    def _pixel_to_geo(self, x: float, y: float) -> tuple[float, float]:
        """Convert pixel coordinates to geographic coordinates."""
        lon = (x / self._map_width) * self._map_bounds.width + self._map_bounds.west
        lat = self._map_bounds.north - (y / self._map_height) * self._map_bounds.height
        return lon, lat
    
    def _get_handle_at(self, x: float, y: float) -> Optional[str]:
        """Determine which handle (if any) is at the given pixel position."""
        handle_radius = 12  # Click tolerance
        
        x1, y1 = self._geo_to_pixel(self._bbox_west, self._bbox_north)
        x2, y2 = self._geo_to_pixel(self._bbox_east, self._bbox_south)
        
        # Check corners
        if abs(x - x1) < handle_radius and abs(y - y1) < handle_radius:
            return 'nw'
        if abs(x - x2) < handle_radius and abs(y - y1) < handle_radius:
            return 'ne'
        if abs(x - x1) < handle_radius and abs(y - y2) < handle_radius:
            return 'sw'
        if abs(x - x2) < handle_radius and abs(y - y2) < handle_radius:
            return 'se'
        
        # Check if inside bbox (for move)
        if x1 < x < x2 and y1 < y < y2:
            return 'move'
        
        return None
    
    def _on_pan_start(self, e: ft.DragStartEvent):
        """Handle drag start."""
        # In Flet 0.80+, use e.x and e.y for position
        x = getattr(e, 'local_x', None) or getattr(e, 'x', 0)
        y = getattr(e, 'local_y', None) or getattr(e, 'y', 0)
        
        handle = self._get_handle_at(x, y)
        if handle:
            self._dragging = True
            self._drag_handle = handle
            self._drag_start_x = x
            self._drag_start_y = y
            self._drag_start_bbox = (
                self._bbox_west, self._bbox_south,
                self._bbox_east, self._bbox_north
            )
    
    def _on_pan_update(self, e: ft.DragUpdateEvent):
        """Handle drag movement."""
        if not self._dragging:
            return
        
        # In Flet 0.80+, use e.x and e.y for position
        x = getattr(e, 'local_x', None) or getattr(e, 'x', 0)
        y = getattr(e, 'local_y', None) or getattr(e, 'y', 0)
        
        # Calculate delta in geographic coordinates
        start_lon, start_lat = self._pixel_to_geo(self._drag_start_x, self._drag_start_y)
        current_lon, current_lat = self._pixel_to_geo(x, y)
        delta_lon = current_lon - start_lon
        delta_lat = current_lat - start_lat
        
        w, s, e_coord, n = self._drag_start_bbox
        
        if self._drag_handle == 'move':
            self._bbox_west = w + delta_lon
            self._bbox_east = e_coord + delta_lon
            self._bbox_south = s + delta_lat
            self._bbox_north = n + delta_lat
        elif self._drag_handle == 'nw':
            self._bbox_west = w + delta_lon
            self._bbox_north = n + delta_lat
        elif self._drag_handle == 'ne':
            self._bbox_east = e_coord + delta_lon
            self._bbox_north = n + delta_lat
        elif self._drag_handle == 'sw':
            self._bbox_west = w + delta_lon
            self._bbox_south = s + delta_lat
        elif self._drag_handle == 'se':
            self._bbox_east = e_coord + delta_lon
            self._bbox_south = s + delta_lat
        
        # Ensure valid bbox
        self._clamp_bbox()
        
        # Update display
        self._canvas.shapes = self._get_shapes()
        self._coord_text.value = self._format_coords()
        self.update()
        
        # Notify callback
        if self._on_change:
            self._on_change(
                self._bbox_west, self._bbox_south,
                self._bbox_east, self._bbox_north
            )
    
    def _on_pan_end(self, e: ft.DragEndEvent):
        """Handle drag end."""
        self._dragging = False
        self._drag_handle = None
    
    def _clamp_bbox(self):
        """Ensure bbox is valid and within map bounds."""
        # Ensure west < east and south < north
        if self._bbox_west > self._bbox_east:
            self._bbox_west, self._bbox_east = self._bbox_east, self._bbox_west
        if self._bbox_south > self._bbox_north:
            self._bbox_south, self._bbox_north = self._bbox_north, self._bbox_south
        
        # Clamp to map bounds
        self._bbox_west = max(self._map_bounds.west, min(self._bbox_west, self._map_bounds.east - 1))
        self._bbox_east = max(self._map_bounds.west + 1, min(self._bbox_east, self._map_bounds.east))
        self._bbox_south = max(self._map_bounds.south, min(self._bbox_south, self._map_bounds.north - 1))
        self._bbox_north = max(self._map_bounds.south + 1, min(self._bbox_north, self._map_bounds.north))
    
    def _format_coords(self) -> str:
        """Format coordinates for display."""
        return (
            f"W: {self._bbox_west:.2f}  |  E: {self._bbox_east:.2f}  |  "
            f"S: {self._bbox_south:.2f}  |  N: {self._bbox_north:.2f}"
        )
    
    def get_bbox(self) -> tuple[float, float, float, float]:
        """Get the current bounding box as (west, south, east, north)."""
        return (self._bbox_west, self._bbox_south, self._bbox_east, self._bbox_north)
    
    def set_bbox(self, west: float, south: float, east: float, north: float):
        """Set the bounding box programmatically."""
        self._bbox_west = west
        self._bbox_south = south
        self._bbox_east = east
        self._bbox_north = north
        self._clamp_bbox()
        self._canvas.shapes = self._get_shapes()
        self._coord_text.value = self._format_coords()
        self.update()
