"""Geographic utility functions for TEMPO Analyzer.

Provides functions for calculating bounding boxes from center points and radii,
coordinate validation, and distance calculations.
"""

import math
from ..storage.models import BoundingBox


# Approximate km per degree of latitude (constant everywhere on Earth)
KM_PER_DEG_LAT = 111.0


def km_to_degrees_lat(km: float) -> float:
    """Convert kilometers to degrees of latitude.

    Args:
        km: Distance in kilometers

    Returns:
        Equivalent distance in degrees of latitude
    """
    return km / KM_PER_DEG_LAT


def km_to_degrees_lon(km: float, latitude: float) -> float:
    """Convert kilometers to degrees of longitude at a given latitude.

    Longitude degrees shrink toward poles: 1 degree lon ≈ cos(lat) * 111 km

    Args:
        km: Distance in kilometers
        latitude: Reference latitude in degrees

    Returns:
        Equivalent distance in degrees of longitude
    """
    cos_lat = math.cos(math.radians(latitude))
    if cos_lat < 0.001:  # Near poles, longitude becomes meaningless
        return 180.0
    return km / (KM_PER_DEG_LAT * cos_lat)


def bbox_from_center(lat: float, lon: float, radius_km: float) -> BoundingBox:
    """Calculate bounding box from center point and radius.

    Creates a square bounding box centered on the given coordinates
    with sides equal to 2 * radius_km.

    Args:
        lat: Center latitude in degrees (-90 to 90)
        lon: Center longitude in degrees (-180 to 180)
        radius_km: Radius in kilometers

    Returns:
        BoundingBox with west, south, east, north coordinates

    Example:
        >>> bbox = bbox_from_center(40.0, -111.0, 10.0)
        >>> print(f"W: {bbox.west:.4f}, E: {bbox.east:.4f}")
    """
    delta_lat = km_to_degrees_lat(radius_km)
    delta_lon = km_to_degrees_lon(radius_km, lat)

    return BoundingBox(
        west=lon - delta_lon,
        south=lat - delta_lat,
        east=lon + delta_lon,
        north=lat + delta_lat
    )


def validate_coordinates(lat: float, lon: float) -> tuple[bool, str]:
    """Validate latitude and longitude coordinates.

    Args:
        lat: Latitude in degrees
        lon: Longitude in degrees

    Returns:
        Tuple of (is_valid, error_message)
    """
    if not (-90 <= lat <= 90):
        return False, f"Latitude {lat} must be between -90 and 90"
    if not (-180 <= lon <= 180):
        return False, f"Longitude {lon} must be between -180 and 180"
    return True, ""


def validate_bbox(bbox: BoundingBox) -> tuple[bool, str]:
    """Validate a bounding box.

    Args:
        bbox: BoundingBox to validate

    Returns:
        Tuple of (is_valid, error_message)
    """
    if bbox.west >= bbox.east:
        return False, "West must be less than East"
    if bbox.south >= bbox.north:
        return False, "South must be less than North"
    if not (-180 <= bbox.west <= 180):
        return False, f"West ({bbox.west}) must be between -180 and 180"
    if not (-180 <= bbox.east <= 180):
        return False, f"East ({bbox.east}) must be between -180 and 180"
    if not (-90 <= bbox.south <= 90):
        return False, f"South ({bbox.south}) must be between -90 and 90"
    if not (-90 <= bbox.north <= 90):
        return False, f"North ({bbox.north}) must be between -90 and 90"
    return True, ""


def haversine_distance(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Calculate the great-circle distance between two points in kilometers.

    Uses the Haversine formula for accurate distance on a sphere.

    Args:
        lat1, lon1: First point coordinates in degrees
        lat2, lon2: Second point coordinates in degrees

    Returns:
        Distance in kilometers
    """
    R = 6371.0  # Earth's radius in km

    lat1_rad = math.radians(lat1)
    lat2_rad = math.radians(lat2)
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)

    a = math.sin(dlat / 2) ** 2 + math.cos(lat1_rad) * math.cos(lat2_rad) * math.sin(dlon / 2) ** 2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))

    return R * c
