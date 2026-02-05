"""
Pytest configuration and fixtures for TEMPO Analyzer tests.
"""

import pytest
import tempfile
import shutil
import numpy as np
import xarray as xr
from pathlib import Path
from datetime import datetime, date
from typing import Generator
import sys

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from tempo_app.storage.database import Database
from tempo_app.storage.models import Dataset, Granule, BoundingBox, DatasetStatus


@pytest.fixture
def tmp_dir() -> Generator[Path, None, None]:
    """Create a temporary directory for test files."""
    tmp = Path(tempfile.mkdtemp(prefix="tempo_test_"))
    yield tmp
    # Cleanup
    shutil.rmtree(tmp, ignore_errors=True)


@pytest.fixture
def tmp_db(tmp_dir: Path) -> Generator[Database, None, None]:
    """Create a temporary database for testing."""
    db_path = tmp_dir / "test.db"
    db = Database(db_path)
    yield db
    # No explicit cleanup needed - tmp_dir handles it


@pytest.fixture
def sample_bbox() -> BoundingBox:
    """Sample bounding box (Utah/Salt Lake)."""
    return BoundingBox(west=-112.8, south=40.0, east=-111.5, north=41.5)


@pytest.fixture
def sample_dataset(sample_bbox: BoundingBox) -> Dataset:
    """Sample dataset for testing."""
    return Dataset(
        id="test-dataset-001",
        name="Test Dataset",
        created_at=datetime.now(),
        bbox=sample_bbox,
        date_start=date(2024, 6, 1),
        date_end=date(2024, 6, 14),
        day_filter=[0, 1, 2, 3, 4],  # Weekdays
        hour_filter=[16, 17, 18, 19, 20],
        max_cloud=0.5,
        max_sza=70.0,
        status=DatasetStatus.PENDING,
    )


@pytest.fixture
def sample_granules(sample_dataset: Dataset) -> list[Granule]:
    """Sample granules for testing."""
    granules = []
    for day in range(1, 8):  # 7 days
        for hour in [16, 17, 18]:  # 3 hours
            g = Granule(
                dataset_id=sample_dataset.id,
                date=date(2024, 6, day),
                hour=hour,
                bbox_west=sample_dataset.bbox.west,
                bbox_south=sample_dataset.bbox.south,
                bbox_east=sample_dataset.bbox.east,
                bbox_north=sample_dataset.bbox.north,
                max_cloud=sample_dataset.max_cloud,
                max_sza=sample_dataset.max_sza,
            )
            granules.append(g)
    return granules


@pytest.fixture
def sample_netcdf_file(tmp_dir: Path) -> Path:
    """Create a sample NetCDF file with NO2 and HCHO data."""
    filepath = tmp_dir / "sample_tempo.nc"
    
    # Create sample data
    rows, cols = 10, 10
    tsteps = 3
    
    # Create coordinates
    lats = np.linspace(40.0, 41.5, rows)
    lons = np.linspace(-112.8, -111.5, cols)
    lat_2d, lon_2d = np.meshgrid(lats, lons, indexing='ij')
    
    # Create time steps
    times = [
        np.datetime64('2024-06-01T16:00:00'),
        np.datetime64('2024-06-01T17:00:00'),
        np.datetime64('2024-06-01T18:00:00'),
    ]
    
    # Create sample NO2 and HCHO data (values above FNR threshold of 1e-12)
    np.random.seed(42)
    no2_data = np.random.rand(tsteps, rows, cols) * 1e-6 + 5e-7  # ~5e-7 to 1.5e-6
    hcho_data = np.random.rand(tsteps, rows, cols) * 5e-6 + 1e-6  # ~1e-6 to 6e-6
    
    # Create dataset
    ds = xr.Dataset(
        data_vars={
            "NO2_TropVCD": (["TSTEP", "ROW", "COL"], no2_data),
            "HCHO_TotVCD": (["TSTEP", "ROW", "COL"], hcho_data),
        },
        coords={
            "TSTEP": times,
            "LAT": (["ROW", "COL"], lat_2d),
            "LON": (["ROW", "COL"], lon_2d),
        },
    )
    
    ds.to_netcdf(filepath)
    ds.close()
    
    return filepath


@pytest.fixture
def sample_netcdf_files(tmp_dir: Path) -> list[Path]:
    """Create multiple sample NetCDF files for processing tests."""
    files = []
    
    for day in range(1, 4):  # 3 days
        for hour in [16, 17, 18]:
            filepath = tmp_dir / f"tempo_2024-06-{day:02d}_{hour:02d}.nc"
            
            rows, cols = 10, 10
            lats = np.linspace(40.0, 41.5, rows)
            lons = np.linspace(-112.8, -111.5, cols)
            lat_2d, lon_2d = np.meshgrid(lats, lons, indexing='ij')
            
            time = np.datetime64(f'2024-06-{day:02d}T{hour:02d}:00:00')
            
            np.random.seed(42 + day * 100 + hour)
            no2_data = np.random.rand(1, rows, cols) * 1e-6 + 5e-7  # Above FNR threshold
            hcho_data = np.random.rand(1, rows, cols) * 5e-6 + 1e-6
            
            ds = xr.Dataset(
                data_vars={
                    "NO2_TropVCD": (["TSTEP", "ROW", "COL"], no2_data),
                    "HCHO_TotVCD": (["TSTEP", "ROW", "COL"], hcho_data),
                },
                coords={
                    "TSTEP": [time],
                    "LAT": (["ROW", "COL"], lat_2d),
                    "LON": (["ROW", "COL"], lon_2d),
                },
            )
            
            ds.to_netcdf(filepath)
            ds.close()
            files.append(filepath)
    
    return files


class MockStatusManager:
    """Mock status manager that records events for testing."""
    
    def __init__(self):
        self.events = []
    
    def emit(self, event: str, message: str, value: float = None):
        self.events.append({"event": event, "message": message, "value": value})


@pytest.fixture
def mock_status() -> MockStatusManager:
    """Mock status manager for testing."""
    return MockStatusManager()
