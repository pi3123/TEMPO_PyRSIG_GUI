"""Data models for TEMPO Analyzer storage system.

Defines dataclasses for Dataset, Granule, and ExportRecord that map to SQLite tables.
"""

from dataclasses import dataclass, field
from datetime import datetime, date
from enum import Enum
from typing import Optional
from uuid import uuid4
import json
import hashlib


class DatasetStatus(Enum):
    """Status of a dataset download."""
    PENDING = "pending"
    DOWNLOADING = "downloading"
    PARTIAL = "partial"
    COMPLETE = "complete"
    ERROR = "error"


class BatchJobStatus(Enum):
    """Status of a batch import job."""
    PENDING = "pending"
    RUNNING = "running"
    PAUSED = "paused"
    COMPLETED = "completed"
    ERROR = "error"


class BatchSiteStatus(Enum):
    """Status of a single site within a batch job."""
    PENDING = "pending"
    QUEUED = "queued"
    DOWNLOADING = "downloading"
    PROCESSING = "processing"
    COMPLETED = "completed"
    ERROR = "error"
    SKIPPED = "skipped"


@dataclass
class BoundingBox:
    """Geographic bounding box."""
    west: float
    south: float
    east: float
    north: float
    
    def to_list(self) -> list[float]:
        """Return as [west, south, east, north] list."""
        return [self.west, self.south, self.east, self.north]
    
    @classmethod
    def from_list(cls, coords: list[float]) -> "BoundingBox":
        """Create from [west, south, east, north] list."""
        return cls(west=coords[0], south=coords[1], east=coords[2], north=coords[3])
    
    def contains_point(self, lat: float, lon: float) -> bool:
        """Check if a point is inside this bounding box."""
        return (self.west <= lon <= self.east) and (self.south <= lat <= self.north)


@dataclass
class Dataset:
    """A user-created dataset configuration and its download status."""
    id: str                          # UUID
    name: str                        # User-friendly name
    created_at: datetime

    # Geographic region
    bbox: BoundingBox

    # Temporal filters
    date_start: date
    date_end: date
    day_filter: list[int]            # 0=Mon, 1=Tue, ..., 6=Sun
    hour_filter: list[int]           # UTC hours (0-23)

    # Quality filters
    max_cloud: float                 # 0.0-1.0
    max_sza: float                   # Solar zenith angle in degrees

    # Variable selection (NEW - dynamic TEMPO variables)
    selected_variables: Optional[list[str]] = None  # Product IDs (e.g., ["tempo.l2.no2.vertical_column_troposphere"])

    # Download status
    status: DatasetStatus = DatasetStatus.PENDING
    batch_job_id: Optional[str] = None # ID of batch job if part of one
    file_path: Optional[str] = None  # Path to combined .nc file
    file_hash: Optional[str] = None  # SHA256 of file content
    file_size_mb: float = 0.0

    # Metadata
    last_accessed: Optional[datetime] = None
    granule_count: int = 0
    granules_downloaded: int = 0

    def __post_init__(self):
        """Backward compatibility: default to legacy 3 variables if not set."""
        if self.selected_variables is None:
            from ..core.variable_registry import VariableRegistry
            self.selected_variables = VariableRegistry.get_default_variables()
    
    @property
    def progress(self) -> float:
        """Download progress as 0.0-1.0."""
        if self.granule_count == 0:
            return 0.0
        return self.granules_downloaded / self.granule_count
    
    @property
    def is_complete(self) -> bool:
        """Check if all granules are downloaded."""
        return self.status == DatasetStatus.COMPLETE
    
    def day_filter_str(self) -> str:
        """Human-readable day filter string."""
        day_names = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]
        if self.day_filter == [0, 1, 2, 3, 4]:
            return "Weekdays"
        elif self.day_filter == [5, 6]:
            return "Weekends"
        elif self.day_filter == [0, 1, 2, 3, 4, 5, 6]:
            return "All Days"
        else:
            return ", ".join(day_names[d] for d in sorted(self.day_filter))
    
    def hour_filter_str(self) -> str:
        """Human-readable hour filter string."""
        if not self.hour_filter:
            return "None"
        hours = sorted(self.hour_filter)
        return f"{hours[0]:02d}:00-{hours[-1]:02d}:00 UTC"

    def variables_str(self) -> str:
        """Human-readable summary of selected variables for UI display."""
        if not self.selected_variables:
            return "None"

        from ..core.variable_registry import VariableRegistry

        # If it's the legacy 3 variables, show names
        if self.selected_variables == VariableRegistry.get_default_variables():
            return "NO₂, HCHO, O₃ (default)"

        # If <= 3 variables, show short names
        if len(self.selected_variables) <= 3:
            try:
                names = []
                for product_id in self.selected_variables:
                    var = VariableRegistry.get_variable_by_id(product_id)
                    if var:
                        # Extract short name (e.g., "NO₂" from "NO₂ Tropospheric VCD")
                        short_name = var.display_name.split()[0]
                        names.append(short_name)
                    else:
                        # Fallback: extract from product_id
                        names.append(product_id.split('.')[-1].upper()[:4])
                return ", ".join(names)
            except Exception:
                pass

        # If > 3 variables, just show count
        return f"{len(self.selected_variables)} variables"


@dataclass
class Granule:
    """A single hourly data granule (one hour of data for a region)."""
    id: Optional[int] = None         # Auto-increment ID
    dataset_id: str = ""             # Parent dataset UUID
    
    # Temporal
    date: date = field(default_factory=date.today)
    hour: int = 0                    # UTC hour (0-23)
    
    # Spatial (copied from dataset for inspection)
    bbox_west: float = 0.0
    bbox_south: float = 0.0
    bbox_east: float = 0.0
    bbox_north: float = 0.0
    
    # Filters (copied from dataset for inspection)
    max_cloud: float = 0.5
    max_sza: float = 70.0
    
    # Status
    downloaded: bool = False
    downloaded_at: Optional[datetime] = None
    
    # Content info
    content_hash: str = ""           # SHA256 of request params
    no2_valid_pixels: int = 0
    hcho_valid_pixels: int = 0
    o3_valid_pixels: int = 0
    no2_mean: Optional[float] = None
    hcho_mean: Optional[float] = None
    o3_mean: Optional[float] = None
    
    # File reference
    file_path: Optional[str] = None
    file_size_bytes: int = 0
    
    def compute_content_hash(self) -> str:
        """Compute SHA256 hash of request parameters for deduplication."""
        hash_input = {
            "bbox": [self.bbox_west, self.bbox_south, self.bbox_east, self.bbox_north],
            "date": self.date.isoformat(),
            "hour": self.hour,
            "max_cloud_fraction": round(self.max_cloud, 4),
            "max_solar_zenith_angle": round(self.max_sza, 2),
            "grid_kw": "1US1"
        }
        json_str = json.dumps(hash_input, sort_keys=True)
        return hashlib.sha256(json_str.encode()).hexdigest()
    
    @property
    def datetime_str(self) -> str:
        """Formatted datetime string."""
        return f"{self.date.isoformat()} @ {self.hour:02d}:00 UTC"


@dataclass
class ExportRecord:
    """Record of an export operation."""
    id: Optional[int] = None
    dataset_id: str = ""
    format: str = "Legacy"           # 'Legacy', 'V1_Hourly', 'V2_Daily_Wide'
    file_path: str = ""
    created_at: datetime = field(default_factory=datetime.now)
    file_size_bytes: int = 0


@dataclass
class Analysis:
    """
    Stores a saved chart analysis with its generated code.
    
    Attributes:
        id: Unique identifier
        dataset_id: Which dataset this analysis uses
        name: User-friendly name (e.g., "NO₂ Hourly Trends")
        query: Original natural language query
        code: Generated matplotlib code (editable)
        plot_path: Path to saved PNG file
        created_at: When first generated
        updated_at: When code was last edited/re-run
        error_message: If execution failed, store error here
    """
    id: str
    dataset_id: str
    name: str
    query: str
    code: str
    plot_path: str
    created_at: datetime
    updated_at: datetime
    error_message: Optional[str] = None

    @staticmethod
    def new(dataset_id: str, query: str, code: str, plot_path: str, name: str = "") -> "Analysis":
        """Create a new analysis record."""
        now = datetime.now()
        return Analysis(
            id=str(uuid4()),
            dataset_id=dataset_id,
            name=name or f"Analysis {now.strftime('%Y-%m-%d %H:%M')}",
            query=query,
            code=code,
            plot_path=plot_path,
            created_at=now,
            updated_at=now,
            error_message=None
        )


@dataclass
class Site:
    """A monitoring site to mark on maps."""
    id: Optional[int] = None
    code: str = ""                   # Short code (e.g., "BV", "LC")
    name: str = ""                   # Full name (e.g., "Bountiful, UT")
    latitude: float = 0.0
    longitude: float = 0.0
    created_at: datetime = field(default_factory=datetime.now)

    def to_tuple(self) -> tuple[float, float]:
        """Return as (latitude, longitude) tuple for plotter compatibility."""
        return (self.latitude, self.longitude)


@dataclass
class BatchJob:
    """A batch import job for processing multiple sites from Excel/CSV."""
    id: str                          # UUID
    name: str                        # Job name
    created_at: datetime
    status: BatchJobStatus = BatchJobStatus.PENDING
    source_file: Optional[str] = None  # Path to source Excel/CSV file

    # Progress counts
    total_sites: int = 0
    completed_sites: int = 0
    failed_sites: int = 0

    # Default settings (can be overridden per site)
    default_radius_km: float = 10.0
    date_start: date = field(default_factory=date.today)
    date_end: date = field(default_factory=date.today)
    day_filter: list[int] = field(default_factory=lambda: [0, 1, 2, 3, 4])  # Weekdays
    hour_filter: list[int] = field(default_factory=lambda: [16, 17, 18, 19, 20])  # UTC hours
    max_cloud: float = 0.3
    max_sza: float = 70.0

    # Processing config
    batch_size: int = 5              # Sites to process in parallel
    last_processed_at: Optional[datetime] = None
    error_message: Optional[str] = None

    @property
    def progress(self) -> float:
        """Batch progress as 0.0-1.0."""
        if self.total_sites == 0:
            return 0.0
        return (self.completed_sites + self.failed_sites) / self.total_sites

    @property
    def is_resumable(self) -> bool:
        """Check if this job can be resumed."""
        return self.status in (BatchJobStatus.PAUSED, BatchJobStatus.ERROR)

    @property
    def is_complete(self) -> bool:
        """Check if all sites have been processed."""
        return self.status == BatchJobStatus.COMPLETED


@dataclass
class BatchSite:
    """A single site within a batch import job."""
    id: Optional[int] = None         # Auto-increment ID
    batch_job_id: str = ""           # Parent batch job UUID
    site_name: str = ""              # Site identifier from Excel
    latitude: float = 0.0
    longitude: float = 0.0
    radius_km: float = 10.0          # Radius used (from job default)

    # Computed bounding box
    bbox_west: float = 0.0
    bbox_south: float = 0.0
    bbox_east: float = 0.0
    bbox_north: float = 0.0

    # Processing status
    status: BatchSiteStatus = BatchSiteStatus.PENDING
    dataset_id: Optional[str] = None  # Link to created dataset
    error_message: Optional[str] = None
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None

    # Ordering for resume
    sequence_number: int = 0

    # Per-site overrides (optional)
    custom_date_start: Optional[date] = None
    custom_date_end: Optional[date] = None
    custom_hour_start: Optional[int] = None  # Start hour (0-23)
    custom_hour_end: Optional[int] = None  # End hour (0-23)
    custom_max_cloud: Optional[float] = None
    custom_max_sza: Optional[float] = None

    @property
    def bbox(self) -> "BoundingBox":
        """Return computed bbox as BoundingBox object."""
        return BoundingBox(self.bbox_west, self.bbox_south, self.bbox_east, self.bbox_north)


# Region presets with FIPS codes for auto-downloading road data
REGION_PRESETS: dict[str, tuple[BoundingBox, str]] = {
    "Southern California": (BoundingBox(-119.68, 32.23, -116.38, 35.73), "06"),
    "Utah (Salt Lake)": (BoundingBox(-112.8, 40.0, -111.5, 41.5), "49"),
    "Texas (Houston)": (BoundingBox(-96.5, 29.0, -94.5, 30.5), "48"),
    "Arizona (Phoenix)": (BoundingBox(-113.3, 32.8, -111.0, 34.2), "04"),
    "Colorado (Denver)": (BoundingBox(-105.5, 39.3, -104.3, 40.2), "08"),
    "New York City": (BoundingBox(-74.3, 40.4, -73.7, 41.0), "36"),
    "Florida (Miami)": (BoundingBox(-80.5, 25.5, -80.0, 26.0), "12"),
}

# Site locations for marking on maps
SITES: dict[str, tuple[float, float]] = {
    # Utah
    "BV": (40.903, -111.884),
    "HW": (40.736, -111.872),
    "RB": (40.767, -111.828),
    "ER": (40.601, -112.356),
    # Colorado
    "LC": (39.779, -105.005),
    # Arizona
    "PX": (33.504, -112.096),
    # Texas
    "HA": (29.9, -95.33),
    "HB": (29.67, -95.5),
    # California
    "PR": (34.01, -118.069),
    "BN": (33.921, -116.858),
    "PS": (33.853, -116.541),
    "SB": (34.107, -117.274),
}

# FIPS codes for US states
STATE_FIPS: dict[str, str] = {
    "06": "California",
    "48": "Texas",
    "49": "Utah",
    "04": "Arizona",
    "08": "Colorado",
    "36": "New York",
    "12": "Florida",
    "32": "Nevada",
    "35": "New Mexico",
    "41": "Oregon",
    "53": "Washington",
}
