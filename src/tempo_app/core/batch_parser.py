"""Parser for batch site import files (Excel/CSV).

Parses Excel (.xlsx, .xls) or CSV files containing site information
for batch TEMPO data downloads.
"""

from dataclasses import dataclass, field
from datetime import date
from pathlib import Path
from typing import Optional
import pandas as pd

from .geo_utils import bbox_from_center, validate_coordinates


@dataclass
class ParsedSite:
    """A site parsed from an import file."""
    row_number: int              # Excel row number (for error reporting)
    site_name: str
    latitude: float
    longitude: float

    custom_radius_km: Optional[float] = None  # Radius in km (overrides job default)
    custom_date_start: Optional[str] = None  # ISO format date string
    custom_date_end: Optional[str] = None  # ISO format date string
    custom_hour_start: Optional[int] = None  # Start hour (0-23, overrides job default)
    custom_hour_end: Optional[int] = None  # End hour (0-23, overrides job default)
    custom_max_cloud: Optional[float] = None  # Max cloud fraction
    custom_max_sza: Optional[float] = None  # Max solar zenith angle
    error: Optional[str] = None  # Validation error for this row


@dataclass
class ParseResult:
    """Result of parsing an import file."""
    sites: list[ParsedSite] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)  # File-level errors
    warnings: list[str] = field(default_factory=list)  # Non-fatal issues
    file_path: Optional[str] = None

    @property
    def is_valid(self) -> bool:
        """Check if parsing succeeded with no fatal errors."""
        return len(self.errors) == 0

    @property
    def valid_sites(self) -> list[ParsedSite]:
        """Get only sites without errors."""
        return [s for s in self.sites if s.error is None]

    @property
    def invalid_sites(self) -> list[ParsedSite]:
        """Get sites with errors."""
        return [s for s in self.sites if s.error is not None]

    @property
    def site_count(self) -> int:
        """Total number of parsed sites (including invalid)."""
        return len(self.sites)

    @property
    def valid_count(self) -> int:
        """Number of valid sites."""
        return len(self.valid_sites)


# Column name aliases for flexible Excel formats
COLUMN_ALIASES = {
    "name": ["name", "site_name", "site", "location", "id", "site_id"],
    "latitude": ["latitude", "lat", "y", "lat_dd"],
    "longitude": ["longitude", "lon", "long", "x", "lng", "lon_dd"],
}


def _find_column(df: pd.DataFrame, aliases: list[str]) -> Optional[str]:
    """Find a column by checking multiple possible names."""
    for alias in aliases:
        if alias in df.columns:
            return alias
    return None


def _parse_date_value(value) -> Optional[str]:
    """Parse a date value from Excel into ISO format string."""
    if pd.isna(value):
        return None
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, str):
        # Try to parse common date formats
        try:
            parsed = pd.to_datetime(value)
            return parsed.date().isoformat()
        except Exception:
            return value  # Return as-is, validation happens later
    return str(value)


def parse_import_file(file_path: Path) -> ParseResult:
    """Parse an Excel or CSV file for batch site import.

    Expected columns:
        Required: name (or site_name), latitude (or lat), longitude (or lon)
        Optional: radius_km, date_start, date_end, max_cloud, max_sza

    Args:
        file_path: Path to .xlsx, .xls, or .csv file

    Returns:
        ParseResult with parsed sites and any errors/warnings
    """
    result = ParseResult(file_path=str(file_path))

    # Check file exists
    if not file_path.exists():
        result.errors.append(f"File not found: {file_path}")
        return result

    # Load file based on extension
    try:
        suffix = file_path.suffix.lower()
        if suffix in (".xlsx", ".xls"):
            df = pd.read_excel(file_path, engine="openpyxl" if suffix == ".xlsx" else None)
        elif suffix == ".csv":
            df = pd.read_csv(file_path)
        else:
            result.errors.append(f"Unsupported file format: {suffix}. Use .xlsx, .xls, or .csv")
            return result
    except Exception as e:
        result.errors.append(f"Failed to read file: {e}")
        return result

    # Check for empty file
    if df.empty:
        result.errors.append("File is empty or has no data rows")
        return result

    # Normalize column names (lowercase, strip whitespace)
    df.columns = [str(c).lower().strip() for c in df.columns]

    # Find required columns
    name_col = _find_column(df, COLUMN_ALIASES["name"])
    lat_col = _find_column(df, COLUMN_ALIASES["latitude"])
    lon_col = _find_column(df, COLUMN_ALIASES["longitude"])

    missing_cols = []
    if not name_col:
        missing_cols.append("name (or site_name, site, location)")
    if not lat_col:
        missing_cols.append("latitude (or lat, y)")
    if not lon_col:
        missing_cols.append("longitude (or lon, long, x)")

    if missing_cols:
        result.errors.append(f"Missing required columns: {', '.join(missing_cols)}")
        result.errors.append(f"Found columns: {', '.join(df.columns.tolist())}")
        return result

    # Find optional columns
    # Radius column removed as per user request
    radius_col = _find_column(df, ["radius_km", "radius", "radius (km)"])
    date_start_col = _find_column(df, ["date_start", "start_date"])
    date_end_col = _find_column(df, ["date_end", "end_date"])
    hour_start_col = _find_column(df, ["hour_start", "time_start", "start_hour"])
    hour_end_col = _find_column(df, ["hour_end", "time_end", "end_hour"])
    max_cloud_col = _find_column(df, ["max_cloud", "cloud_fraction", "cloud"])
    max_sza_col = _find_column(df, ["max_sza", "sza", "solar_zenith"])

    # Parse each row
    for idx, row in df.iterrows():
        row_num = idx + 2  # Excel is 1-indexed, plus header row

        site = ParsedSite(
            row_number=row_num,
            site_name="",
            latitude=0.0,
            longitude=0.0,
        )

        # Parse site name
        if pd.notna(row[name_col]):
            site.site_name = str(row[name_col]).strip()
        if not site.site_name:
            site.error = "Missing site name"
            result.sites.append(site)
            continue

        # Parse latitude
        try:
            site.latitude = float(row[lat_col])
        except (ValueError, TypeError):
            site.error = f"Invalid latitude: {row[lat_col]}"
            result.sites.append(site)
            continue

        # Parse longitude
        try:
            site.longitude = float(row[lon_col])
        except (ValueError, TypeError):
            site.error = f"Invalid longitude: {row[lon_col]}"
            result.sites.append(site)
            continue

        # Validate coordinates
        valid, err_msg = validate_coordinates(site.latitude, site.longitude)
        if not valid:
            site.error = err_msg
            result.sites.append(site)
            continue

        # Parse optional fields
        if radius_col and pd.notna(row[radius_col]):
            try:
                site.custom_radius_km = float(row[radius_col])
            except (ValueError, TypeError):
                result.warnings.append(f"Row {row_num}: Invalid radius_km, using default")

        if date_start_col and pd.notna(row[date_start_col]):
            site.custom_date_start = _parse_date_value(row[date_start_col])

        if date_end_col and pd.notna(row[date_end_col]):
            site.custom_date_end = _parse_date_value(row[date_end_col])

        if hour_start_col and pd.notna(row[hour_start_col]):
            try:
                site.custom_hour_start = int(row[hour_start_col])
                if not (0 <= site.custom_hour_start <= 23):
                    result.warnings.append(f"Row {row_num}: Hour start must be 0-23, using default")
                    site.custom_hour_start = None
            except (ValueError, TypeError):
                result.warnings.append(f"Row {row_num}: Invalid hour_start, using default")
                site.custom_hour_start = None

        if hour_end_col and pd.notna(row[hour_end_col]):
            try:
                site.custom_hour_end = int(row[hour_end_col])
                if not (0 <= site.custom_hour_end <= 23):
                    result.warnings.append(f"Row {row_num}: Hour end must be 0-23, using default")
                    site.custom_hour_end = None
            except (ValueError, TypeError):
                result.warnings.append(f"Row {row_num}: Invalid hour_end, using default")
                site.custom_hour_end = None

        if max_cloud_col and pd.notna(row[max_cloud_col]):
            try:
                site.custom_max_cloud = float(row[max_cloud_col])
            except (ValueError, TypeError):
                result.warnings.append(f"Row {row_num}: Invalid max_cloud, using default")

        if max_sza_col and pd.notna(row[max_sza_col]):
            try:
                site.custom_max_sza = float(row[max_sza_col])
            except (ValueError, TypeError):
                result.warnings.append(f"Row {row_num}: Invalid max_sza, using default")

        result.sites.append(site)

    # Final validation
    if not result.sites:
        result.errors.append("No sites found in file")

    return result


def create_sample_excel(file_path: Path, num_sites: int = 5) -> None:
    """Create a sample Excel file showing the expected format.

    Useful for users to understand the required structure.

    Args:
        file_path: Where to save the sample file
        num_sites: Number of sample sites to include
    """
    sample_data = {
        "name": [f"Site_{i+1}" for i in range(num_sites)],
        "latitude": [40.0 + i * 0.5 for i in range(num_sites)],
        "longitude": [-111.0 - i * 0.5 for i in range(num_sites)],
        "radius_km": [10.0] * num_sites,
        "date_start": ["2024-01-01"] * num_sites,
        "date_end": ["2024-01-31"] * num_sites,
        "hour_start": [16] * num_sites,
        "hour_end": [20] * num_sites,
        "max_cloud": [0.3] * num_sites,
        "max_sza": [70.0] * num_sites,
    }

    df = pd.DataFrame(sample_data)
    df.to_excel(file_path, index=False, engine="openpyxl")
