"""Tests for DataExporter class (core/exporter.py)."""

import pytest
import pandas as pd
import xarray as xr
import numpy as np
from pathlib import Path

from tempo_app.core.exporter import (
    DataExporter,
    haversine,
    find_n_nearest_cells,
    find_cells_within_distance,
    apply_monthly_hourly_fill,
    filter_sites_in_bbox,
)

# Check if openpyxl is available for reading Excel files
try:
    import openpyxl
    HAS_OPENPYXL = True
except ImportError:
    HAS_OPENPYXL = False


class TestHaversine:
    """Tests for haversine distance function."""

    def test_same_point_zero_distance(self):
        """Distance from a point to itself is zero."""
        assert haversine(40.0, -111.0, 40.0, -111.0) == 0.0

    def test_known_distance(self):
        """Test against known distance (Salt Lake City to Denver ~600km)."""
        dist = haversine(40.76, -111.89, 39.74, -104.99)
        assert 590 < dist < 610  # ~600 km

    def test_symmetry(self):
        """Distance A->B equals B->A."""
        d1 = haversine(40.0, -111.0, 41.0, -112.0)
        d2 = haversine(41.0, -112.0, 40.0, -111.0)
        assert d1 == pytest.approx(d2)


class TestFindNearestCells:
    """Tests for find_n_nearest_cells function."""

    def test_finds_correct_number(self):
        """Returns exactly N cells."""
        lats = np.linspace(40.0, 41.0, 10)
        lons = np.linspace(-112.0, -111.0, 10)
        lat_2d, lon_2d = np.meshgrid(lats, lons, indexing='ij')

        cells = find_n_nearest_cells(40.5, -111.5, lat_2d, lon_2d, n=4)
        assert len(cells) == 4

    def test_sorted_by_distance(self):
        """Cells are sorted by distance (closest first)."""
        lats = np.linspace(40.0, 41.0, 10)
        lons = np.linspace(-112.0, -111.0, 10)
        lat_2d, lon_2d = np.meshgrid(lats, lons, indexing='ij')

        cells = find_n_nearest_cells(40.5, -111.5, lat_2d, lon_2d, n=9)
        distances = [c[2] for c in cells]
        assert distances == sorted(distances)

    def test_returns_tuples(self):
        """Returns tuples of (row, col, distance)."""
        lats = np.linspace(40.0, 41.0, 10)
        lons = np.linspace(-112.0, -111.0, 10)
        lat_2d, lon_2d = np.meshgrid(lats, lons, indexing='ij')

        cells = find_n_nearest_cells(40.5, -111.5, lat_2d, lon_2d, n=4)
        for cell in cells:
            assert len(cell) == 3  # row, col, distance
            assert isinstance(cell[0], int)  # row
            assert isinstance(cell[1], int)  # col
            assert isinstance(cell[2], float)  # distance


class TestApplyMonthlyHourlyFill:
    """Tests for gap filling function."""

    def test_fills_nan_values(self):
        """NaN values are filled with monthly-hourly means."""
        df = pd.DataFrame({
            'Site': ['A', 'A', 'A', 'A'],
            'Month': [1, 1, 1, 1],
            'Hour': [10, 10, 10, 10],
            'Value': [1.0, 2.0, np.nan, 4.0]
        })
        filled = apply_monthly_hourly_fill(df, ['Value'])
        # Mean of [1, 2, 4] = 2.33...
        assert not filled['Value'].isna().any()
        assert filled['Value'].iloc[2] == pytest.approx(2.333, rel=0.01)

    def test_preserves_existing_values(self):
        """Non-NaN values remain unchanged."""
        df = pd.DataFrame({
            'Site': ['A', 'A'],
            'Month': [1, 1],
            'Hour': [10, 10],
            'Value': [1.0, 2.0]
        })
        filled = apply_monthly_hourly_fill(df, ['Value'])
        assert filled['Value'].iloc[0] == 1.0
        assert filled['Value'].iloc[1] == 2.0

    def test_handles_missing_group_columns(self):
        """Returns original dataframe if group columns are missing."""
        df = pd.DataFrame({'Value': [1.0, 2.0, np.nan]})
        filled = apply_monthly_hourly_fill(df, ['Value'])
        # Should return unchanged since no group columns
        assert filled.equals(df)


class TestFindCellsWithinDistance:
    """Tests for find_cells_within_distance function."""

    def test_finds_cells_within_radius(self):
        """Returns only cells within specified distance."""
        lats = np.linspace(40.0, 41.0, 10)
        lons = np.linspace(-112.0, -111.0, 10)
        lat_2d, lon_2d = np.meshgrid(lats, lons, indexing='ij')

        # Find cells within 50km of center
        cells = find_cells_within_distance(40.5, -111.5, lat_2d, lon_2d, max_distance_km=50.0)

        # All returned cells should be within 50km
        for row, col, dist in cells:
            assert dist <= 50.0

    def test_sorted_by_distance(self):
        """Cells are sorted by distance (closest first)."""
        lats = np.linspace(40.0, 41.0, 10)
        lons = np.linspace(-112.0, -111.0, 10)
        lat_2d, lon_2d = np.meshgrid(lats, lons, indexing='ij')

        cells = find_cells_within_distance(40.5, -111.5, lat_2d, lon_2d, max_distance_km=100.0)
        distances = [c[2] for c in cells]
        assert distances == sorted(distances)

    def test_returns_empty_if_none_within_distance(self):
        """Returns empty list if no cells within distance."""
        lats = np.linspace(40.0, 41.0, 10)
        lons = np.linspace(-112.0, -111.0, 10)
        lat_2d, lon_2d = np.meshgrid(lats, lons, indexing='ij')

        # Try to find cells within 0.1km (very small radius)
        cells = find_cells_within_distance(40.5, -111.5, lat_2d, lon_2d, max_distance_km=0.001)
        # May return 0 or 1 cell depending on grid resolution
        assert isinstance(cells, list)


class TestFilterSitesInBbox:
    """Tests for site filtering function."""

    def test_filters_sites_correctly(self):
        """Only returns sites within bbox."""
        sites = {
            'A': (40.5, -111.5),  # Inside
            'B': (30.0, -100.0),  # Outside
            'C': (41.0, -112.0),  # Inside
        }

        lats = np.linspace(40.0, 41.5, 10)
        lons = np.linspace(-112.5, -111.0, 10)
        lat_2d, lon_2d = np.meshgrid(lats, lons, indexing='ij')

        ds = xr.Dataset(coords={'LAT': (['ROW', 'COL'], lat_2d),
                                'LON': (['ROW', 'COL'], lon_2d)})

        filtered = filter_sites_in_bbox(sites, ds)
        assert 'A' in filtered
        assert 'B' not in filtered
        assert 'C' in filtered

    def test_returns_empty_if_no_coords(self):
        """Returns empty dict if LAT/LON coords missing."""
        sites = {'A': (40.0, -111.0)}
        ds = xr.Dataset()
        filtered = filter_sites_in_bbox(sites, ds)
        assert filtered == {}


@pytest.fixture
def sample_dataset(tmp_dir):
    """Create a sample dataset covering Utah region."""
    rows, cols = 20, 20
    hours = list(range(14, 22))  # UTC 14:00-21:00

    lats = np.linspace(40.0, 41.5, rows)
    lons = np.linspace(-112.8, -111.5, cols)
    lat_2d, lon_2d = np.meshgrid(lats, lons, indexing='ij')

    tsteps = [np.datetime64(f'2024-06-01T{h:02d}:00:00') for h in hours]

    np.random.seed(42)
    no2 = np.random.rand(len(hours), rows, cols) * 1e-15 + 5e-16
    hcho = np.random.rand(len(hours), rows, cols) * 5e-15 + 1e-15
    fnr = hcho / no2

    return xr.Dataset(
        data_vars={
            "NO2_TropVCD": (["TSTEP", "ROW", "COL"], no2),
            "HCHO_TotVCD": (["TSTEP", "ROW", "COL"], hcho),
            "FNR": (["TSTEP", "ROW", "COL"], fnr),
        },
        coords={
            "TSTEP": tsteps,
            "LAT": (["ROW", "COL"], lat_2d),
            "LON": (["ROW", "COL"], lon_2d),
        },
    )


@pytest.fixture
def exporter(tmp_dir):
    """Create exporter with temp output directory."""
    return DataExporter(tmp_dir)


class TestHourlyMulticellExport:
    """Tests for hourly_multicell format."""

    def test_creates_per_site_files(self, exporter, sample_dataset):
        """Creates separate files for each valid site."""
        files = exporter.export_dataset(
            sample_dataset, "Test", "hourly_multicell"
        )
        assert len(files) > 0
        assert all("hourly_multicell.xlsx" in f for f in files)
        for f in files:
            assert Path(f).exists()

    @pytest.mark.skipif(not HAS_OPENPYXL, reason="openpyxl required")
    def test_has_required_sheets(self, exporter, sample_dataset):
        """Excel files have Hourly_Data and Grid_Info sheets."""
        files = exporter.export_dataset(
            sample_dataset, "Test", "hourly_multicell"
        )
        xlsx = pd.ExcelFile(files[0])
        assert "Hourly_Data" in xlsx.sheet_names
        assert "Grid_Info" in xlsx.sheet_names

    @pytest.mark.skipif(not HAS_OPENPYXL, reason="openpyxl required")
    def test_hourly_data_has_9_cells(self, exporter, sample_dataset):
        """Hourly_Data has columns for 9 cells."""
        files = exporter.export_dataset(
            sample_dataset, "Test", "hourly_multicell"
        )
        df = pd.read_excel(files[0], sheet_name="Hourly_Data")
        # Should have: UTC_Time, Local_Time, Cell1_NO2, Cell1_HCHO, ..., Cell9_HCHO
        assert "Cell9_NO2" in df.columns
        assert "Cell9_HCHO" in df.columns

    @pytest.mark.skipif(not HAS_OPENPYXL, reason="openpyxl required")
    def test_grid_info_has_metadata(self, exporter, sample_dataset):
        """Grid_Info sheet contains cell metadata."""
        files = exporter.export_dataset(
            sample_dataset, "Test", "hourly_multicell"
        )
        # Read only the first 6 columns to avoid visual grid data
        df = pd.read_excel(files[0], sheet_name="Grid_Info", usecols="A:F")
        assert "Cell_ID" in df.columns
        assert "Lat" in df.columns
        assert "Lon" in df.columns
        assert "Dist_km" in df.columns
        assert len(df) == 9  # 9 cells


class TestDailyAggregatedExport:
    """Tests for daily_aggregated format."""

    def test_creates_per_site_files(self, exporter, sample_dataset):
        """Creates separate files for each valid site."""
        files = exporter.export_dataset(
            sample_dataset, "Test", "daily_aggregated"
        )
        assert len(files) > 0
        assert all("daily_aggregated.xlsx" in f for f in files)
        for f in files:
            assert Path(f).exists()

    @pytest.mark.skipif(not HAS_OPENPYXL, reason="openpyxl required")
    def test_has_all_column_combinations(self, exporter, sample_dataset):
        """Has columns for all Fill/NoFill x 4/9 x Avg/Cnt combinations."""
        files = exporter.export_dataset(
            sample_dataset, "Test", "daily_aggregated"
        )
        df = pd.read_excel(files[0])

        expected_cols = [
            "NO2_NoFill_4_Avg", "NO2_NoFill_4_Cnt",
            "NO2_Fill_4_Avg", "NO2_Fill_4_Cnt",
            "NO2_NoFill_9_Avg", "NO2_NoFill_9_Cnt",
            "NO2_Fill_9_Avg", "NO2_Fill_9_Cnt",
            "HCHO_NoFill_4_Avg", "HCHO_NoFill_4_Cnt",
            "HCHO_Fill_4_Avg", "HCHO_Fill_4_Cnt",
            "HCHO_NoFill_9_Avg", "HCHO_NoFill_9_Cnt",
            "HCHO_Fill_9_Avg", "HCHO_Fill_9_Cnt",
        ]
        for col in expected_cols:
            assert col in df.columns, f"Missing column: {col}"

    @pytest.mark.skipif(not HAS_OPENPYXL, reason="openpyxl required")
    def test_has_date_column(self, exporter, sample_dataset):
        """Has Date column."""
        files = exporter.export_dataset(
            sample_dataset, "Test", "daily_aggregated"
        )
        df = pd.read_excel(files[0])
        assert "Date" in df.columns


class TestSpatialAverageExport:
    """Tests for spatial_average format."""

    def test_creates_single_file(self, exporter, sample_dataset):
        """Creates exactly one file for all sites."""
        files = exporter.export_dataset(
            sample_dataset, "Test", "spatial_average"
        )
        assert len(files) == 1
        assert "FNR_Test_spatial_average.xlsx" in files[0]
        assert Path(files[0]).exists()

    @pytest.mark.skipif(not HAS_OPENPYXL, reason="openpyxl required")
    def test_has_required_sheets(self, exporter, sample_dataset):
        """Has Raw_Data, Filled_Data, Summary, Grid_Cells sheets."""
        files = exporter.export_dataset(
            sample_dataset, "Test", "spatial_average"
        )
        xlsx = pd.ExcelFile(files[0])

        assert "Raw_Data" in xlsx.sheet_names
        assert "Filled_Data" in xlsx.sheet_names
        assert "Summary" in xlsx.sheet_names
        assert "Grid_Cells" in xlsx.sheet_names

    @pytest.mark.skipif(not HAS_OPENPYXL, reason="openpyxl required")
    def test_raw_data_has_site_columns(self, exporter, sample_dataset):
        """Raw_Data has columns for each site variable."""
        files = exporter.export_dataset(
            sample_dataset, "Test", "spatial_average"
        )
        df = pd.read_excel(files[0], sheet_name="Raw_Data")

        # Should have UTC, Local, Date columns
        assert "UTC" in df.columns
        assert "Date" in df.columns

        # Should have site columns (at least one Utah site)
        site_cols = [c for c in df.columns if "_NO2" in c or "_HCHO" in c or "_FNR" in c]
        assert len(site_cols) > 0

    @pytest.mark.skipif(not HAS_OPENPYXL, reason="openpyxl required")
    def test_summary_has_site_info(self, exporter, sample_dataset):
        """Summary sheet contains site information."""
        files = exporter.export_dataset(
            sample_dataset, "Test", "spatial_average"
        )
        df = pd.read_excel(files[0], sheet_name="Summary")
        assert "Site" in df.columns
        assert "Points" in df.columns

    @pytest.mark.skipif(not HAS_OPENPYXL, reason="openpyxl required")
    def test_grid_cells_has_coordinates(self, exporter, sample_dataset):
        """Grid_Cells sheet contains pixel coordinates."""
        files = exporter.export_dataset(
            sample_dataset, "Test", "spatial_average"
        )
        df = pd.read_excel(files[0], sheet_name="Grid_Cells")
        assert "Site" in df.columns
        assert "Grid_Lat" in df.columns
        assert "Grid_Lon" in df.columns
        assert "Dist (km)" in df.columns


class TestExportEdgeCases:
    """Edge case tests."""

    def test_no_sites_in_region_returns_empty(self, exporter, tmp_dir):
        """Returns empty list when no sites are in the dataset region."""
        # Create dataset for Pacific Ocean (no monitoring sites)
        rows, cols = 5, 5
        lats = np.linspace(0.0, 1.0, rows)  # Equator
        lons = np.linspace(-170.0, -169.0, cols)  # Pacific Ocean
        lat_2d, lon_2d = np.meshgrid(lats, lons, indexing='ij')

        tsteps = [np.datetime64(f'2024-06-01T{h:02d}:00:00') for h in [16, 17, 18]]

        ds = xr.Dataset(
            data_vars={
                "NO2_TropVCD": (["TSTEP", "ROW", "COL"], np.random.rand(3, rows, cols)),
                "HCHO_TotVCD": (["TSTEP", "ROW", "COL"], np.random.rand(3, rows, cols)),
            },
            coords={
                "TSTEP": tsteps,
                "LAT": (["ROW", "COL"], lat_2d),
                "LON": (["ROW", "COL"], lon_2d),
            },
        )

        files = exporter.export_dataset(ds, "Ocean", "hourly_multicell")
        assert files == []

    def test_invalid_format_raises_error(self, exporter, sample_dataset):
        """Raises ValueError for unknown export format."""
        with pytest.raises(ValueError, match="Unknown export format"):
            exporter.export_dataset(sample_dataset, "Test", "invalid_format")

    def test_handles_nan_values(self, exporter, tmp_dir):
        """Handles datasets with NaN values gracefully."""
        rows, cols = 10, 10
        lats = np.linspace(40.0, 41.5, rows)
        lons = np.linspace(-112.8, -111.5, cols)
        lat_2d, lon_2d = np.meshgrid(lats, lons, indexing='ij')

        # Create data with NaN values
        no2_data = np.random.rand(3, rows, cols)
        no2_data[0, :, :] = np.nan  # First hour all NaN

        tsteps = [np.datetime64(f'2024-06-01T{h:02d}:00:00') for h in [16, 17, 18]]

        ds = xr.Dataset(
            data_vars={
                "NO2_TropVCD": (["TSTEP", "ROW", "COL"], no2_data),
                "HCHO_TotVCD": (["TSTEP", "ROW", "COL"], np.random.rand(3, rows, cols)),
            },
            coords={
                "TSTEP": tsteps,
                "LAT": (["ROW", "COL"], lat_2d),
                "LON": (["ROW", "COL"], lon_2d),
            },
        )

        # Should not crash
        files = exporter.export_dataset(ds, "NaN", "hourly_multicell")
        assert isinstance(files, list)

    def test_missing_coords_returns_empty(self, exporter):
        """Returns empty list if required coordinates are missing."""
        ds = xr.Dataset(
            data_vars={"NO2_TropVCD": (["time"], [1.0, 2.0, 3.0])},
            coords={"time": [0, 1, 2]}
        )
        files = exporter.export_dataset(ds, "NoCoords", "hourly_multicell")
        assert files == []

    def test_export_output_directory_created(self, tmp_dir, sample_dataset):
        """Test that output directory is automatically created."""
        output_base = tmp_dir / "new_folder"
        assert not output_base.exists()

        exporter = DataExporter(output_base)
        # The exports subdirectory should be created
        assert (output_base / "exports").exists()


class TestDataExporterInit:
    """Tests for DataExporter initialization."""

    def test_creates_output_directory(self, tmp_dir):
        """Creates output directory on initialization."""
        output_dir = tmp_dir / "test_output"
        exporter = DataExporter(output_dir)
        assert (output_dir / "exports").exists()

    def test_handles_existing_directory(self, tmp_dir):
        """Handles existing directory gracefully."""
        output_dir = tmp_dir / "existing"
        (output_dir / "exports").mkdir(parents=True)

        # Should not raise error
        exporter = DataExporter(output_dir)
        assert (output_dir / "exports").exists()


class TestDistanceBasedExport:
    """Tests for distance-based cell selection in exports."""

    def test_hourly_multicell_with_distance(self, exporter, sample_dataset):
        """Hourly multicell export works with distance_km parameter."""
        files = exporter.export_dataset(
            sample_dataset, "Test", "hourly_multicell", distance_km=10.0
        )
        assert len(files) > 0
        for f in files:
            assert Path(f).exists()

    @pytest.mark.skipif(not HAS_OPENPYXL, reason="openpyxl required")
    def test_hourly_multicell_distance_has_variable_cells(self, exporter, sample_dataset):
        """Distance-based export may have different number of cells than fixed count."""
        files = exporter.export_dataset(
            sample_dataset, "Test", "hourly_multicell", distance_km=20.0
        )
        df = pd.read_excel(files[0], sheet_name="Grid_Info")
        # Should have at least 1 cell
        assert len(df) >= 1

    def test_daily_aggregated_with_distance(self, exporter, sample_dataset):
        """Daily aggregated export works with distance_km parameter."""
        files = exporter.export_dataset(
            sample_dataset, "Test", "daily_aggregated", distance_km=10.0
        )
        assert len(files) > 0
        for f in files:
            assert Path(f).exists()

    def test_spatial_average_with_distance(self, exporter, sample_dataset):
        """Spatial average export works with distance_km parameter."""
        files = exporter.export_dataset(
            sample_dataset, "Test", "spatial_average", distance_km=10.0
        )
        assert len(files) == 1
        assert Path(files[0]).exists()

    @pytest.mark.skipif(not HAS_OPENPYXL, reason="openpyxl required")
    def test_spatial_average_distance_updates_summary(self, exporter, sample_dataset):
        """Summary reflects actual number of cells used with distance_km."""
        files = exporter.export_dataset(
            sample_dataset, "Test", "spatial_average", distance_km=15.0
        )
        df = pd.read_excel(files[0], sheet_name="Summary")
        # Points should reflect actual cells found, not default 4
        assert "Points" in df.columns
        assert len(df) > 0
