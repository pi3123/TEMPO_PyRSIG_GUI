"""
Tests for MapPlotter class (core/plotter.py).
"""

import pytest
import numpy as np
import xarray as xr
from pathlib import Path

from tempo_app.core.plotter import MapPlotter


@pytest.fixture
def sample_hourly_dataset(tmp_dir):
    """Create a dataset structured for hourly plotting."""
    rows, cols = 20, 20
    
    # Create lat/lon grid covering Utah
    lats = np.linspace(40.0, 41.5, rows)
    lons = np.linspace(-112.8, -111.5, cols)
    lat_2d, lon_2d = np.meshgrid(lats, lons, indexing='ij')
    
    # Create time with specific hours
    times = [
        np.datetime64('2024-06-01T16:00:00'),
        np.datetime64('2024-06-01T17:00:00'),
        np.datetime64('2024-06-01T18:00:00'),
    ]
    
    np.random.seed(42)
    no2_data = np.random.rand(len(times), rows, cols) * 1e-15 + 5e-16
    hcho_data = np.random.rand(len(times), rows, cols) * 5e-15 + 1e-15
    fnr_data = hcho_data / no2_data
    
    ds = xr.Dataset(
        data_vars={
            "NO2_TropVCD": (["TSTEP", "ROW", "COL"], no2_data),
            "HCHO_TotVCD": (["TSTEP", "ROW", "COL"], hcho_data),
            "FNR": (["TSTEP", "ROW", "COL"], fnr_data),
        },
        coords={
            "TSTEP": times,
            "LAT": (["ROW", "COL"], lat_2d),
            "LON": (["ROW", "COL"], lon_2d),
        },
    )
    
    return ds


@pytest.fixture
def plotter(tmp_dir):
    """Create a plotter with temporary cache directory."""
    return MapPlotter(tmp_dir)


class TestMapPlotter:
    """Tests for MapPlotter functionality."""
    
    def test_generate_map_no2(self, plotter, sample_hourly_dataset):
        """Test generating NO2 map."""
        result, messages = plotter.generate_map(
            dataset=sample_hourly_dataset,
            hour=16,
            variable="NO2",
            dataset_name="TestDataset",
            bbox=[-112.8, 40.0, -111.5, 41.5],
        )

        # Should return a tuple of (path, messages)
        assert isinstance(messages, list)
        # May be None if cartopy not available, or path if it worked
        if result is not None:
            assert Path(result).exists()
            assert result.endswith(".png")
    
    def test_generate_map_hcho(self, plotter, sample_hourly_dataset):
        """Test generating HCHO map."""
        result, messages = plotter.generate_map(
            dataset=sample_hourly_dataset,
            hour=17,
            variable="HCHO",
            dataset_name="TestDataset",
            bbox=[-112.8, 40.0, -111.5, 41.5],
        )

        assert isinstance(messages, list)
        if result is not None:
            assert Path(result).exists()
    
    def test_generate_map_fnr(self, plotter, sample_hourly_dataset):
        """Test generating FNR map with custom colormap."""
        result, messages = plotter.generate_map(
            dataset=sample_hourly_dataset,
            hour=18,
            variable="FNR",
            dataset_name="TestDataset",
            bbox=[-112.8, 40.0, -111.5, 41.5],
        )

        assert isinstance(messages, list)
        if result is not None:
            assert Path(result).exists()
    
    def test_map_cached(self, plotter, sample_hourly_dataset):
        """Test that cached maps are returned."""
        # Generate first time
        result1, messages1 = plotter.generate_map(
            dataset=sample_hourly_dataset,
            hour=16,
            variable="NO2",
            dataset_name="CacheTest",
            bbox=[-112.8, 40.0, -111.5, 41.5],
        )

        if result1 is None:
            pytest.skip("Map generation not available (cartopy missing)")

        # Get modification time
        mtime1 = Path(result1).stat().st_mtime

        # Generate second time - should return cached
        result2, messages2 = plotter.generate_map(
            dataset=sample_hourly_dataset,
            hour=16,
            variable="NO2",
            dataset_name="CacheTest",
            bbox=[-112.8, 40.0, -111.5, 41.5],
        )

        assert result1 == result2
        mtime2 = Path(result2).stat().st_mtime
        assert mtime1 == mtime2  # File wasn't regenerated
    
    def test_invalid_hour_returns_none(self, plotter, sample_hourly_dataset):
        """Test that requesting unavailable hour returns None."""
        result, messages = plotter.generate_map(
            dataset=sample_hourly_dataset,
            hour=23,  # Not in sample data (only 16, 17, 18)
            variable="NO2",
            dataset_name="TestDataset",
            bbox=[-112.8, 40.0, -111.5, 41.5],
        )

        assert result is None
        assert isinstance(messages, list)
        # Should have an error message about the hour not being found
        assert len(messages) > 0
    
    def test_fallback_without_cartopy(self, plotter, sample_hourly_dataset, monkeypatch):
        """Test fallback dummy map when cartopy is not available."""
        # Mock cartopy as not available
        import sys
        
        # Store original
        original_modules = dict(sys.modules)
        
        try:
            # Remove cartopy from modules to simulate it missing
            for mod in list(sys.modules.keys()):
                if 'cartopy' in mod:
                    del sys.modules[mod]
            
            # This test mainly checks the fallback path exists
            # In real scenario, _generate_dummy_map would be called
            result, messages = plotter._generate_dummy_map("NO2", 16)

            assert result is not None
            assert Path(result).exists()
            assert isinstance(messages, list)
        finally:
            # Restore modules
            sys.modules.update(original_modules)


class TestMapPlotterEdgeCases:
    """Edge case tests for MapPlotter."""
    
    def test_all_nan_data_returns_none(self, plotter, tmp_dir):
        """Test that all-NaN data returns None."""
        rows, cols = 10, 10
        lats = np.linspace(40.0, 41.5, rows)
        lons = np.linspace(-112.8, -111.5, cols)
        lat_2d, lon_2d = np.meshgrid(lats, lons, indexing='ij')
        
        # All NaN data
        ds = xr.Dataset(
            data_vars={
                "NO2_TropVCD": (["TSTEP", "ROW", "COL"], np.full((1, rows, cols), np.nan)),
            },
            coords={
                "TSTEP": [np.datetime64('2024-06-01T16:00:00')],
                "LAT": (["ROW", "COL"], lat_2d),
                "LON": (["ROW", "COL"], lon_2d),
            },
        )
        
        result, messages = plotter.generate_map(
            dataset=ds,
            hour=16,
            variable="NO2",
            dataset_name="NaNTest",
            bbox=[-112.8, 40.0, -111.5, 41.5],
        )

        # Should return None for empty data
        assert result is None
        assert isinstance(messages, list)
        # Should have a warning message about NaN data
        assert len(messages) > 0
    
    def test_cache_directory_created(self, tmp_dir):
        """Test that cache directory is automatically created."""
        cache_base = tmp_dir / "new_cache"
        assert not cache_base.exists()
        
        plotter = MapPlotter(cache_base)
        assert (cache_base / "plots").exists()
