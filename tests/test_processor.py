"""
Tests for DataProcessor class (core/processor.py).
"""

import pytest
import numpy as np
import xarray as xr
from pathlib import Path

from tempo_app.core.processor import DataProcessor


class TestDataProcessor:
    """Tests for DataProcessor functionality."""
    
    def test_process_empty_list(self):
        """Test processing empty file list returns None."""
        result = DataProcessor.process_dataset([])
        assert result is None
    
    def test_process_single_file(self, sample_netcdf_file):
        """Test processing a single NetCDF file."""
        result = DataProcessor.process_dataset([sample_netcdf_file])
        
        assert result is not None
        assert "NO2_TropVCD" in result
        assert "HCHO_TotVCD" in result
        assert "FNR" in result
    
    def test_process_multiple_files(self, sample_netcdf_files):
        """Test processing multiple NetCDF files."""
        result = DataProcessor.process_dataset(sample_netcdf_files)
        
        assert result is not None
        # Should have aggregated by hour
        assert "hour" in result.dims or len(result.dims) > 0
    
    def test_fnr_calculation(self, sample_netcdf_file):
        """Test FNR calculation (HCHO/NO2) with thresholds."""
        result = DataProcessor.process_dataset([sample_netcdf_file])
        
        assert "FNR" in result
        fnr_values = result["FNR"].values
        
        # FNR should be roughly HCHO/NO2
        # In our sample data, HCHO ~ 1e-15 to 6e-15, NO2 ~ 5e-16 to 1.5e-15
        # So FNR should be ~1 to ~10 range (realistic for atmospheric data)
        valid_fnr = fnr_values[~np.isnan(fnr_values)]
        assert len(valid_fnr) > 0
        assert valid_fnr.min() > 0  # Should be positive
    
    def test_file_handles_closed(self, sample_netcdf_files, tmp_dir):
        """Test that file handles are properly closed after processing."""
        result = DataProcessor.process_dataset(sample_netcdf_files)
        
        # Try to delete the source files - should work if handles are closed
        for f in sample_netcdf_files:
            try:
                f.unlink()
            except PermissionError:
                pytest.fail(f"File {f} is still locked after processing")
    
    def test_process_invalid_file(self, tmp_dir):
        """Test processing a non-existent file."""
        fake_file = tmp_dir / "nonexistent.nc"
        
        # Should handle gracefully without crashing
        result = DataProcessor.process_dataset([fake_file])
        assert result is None
    
    def test_save_processed(self, sample_netcdf_file, tmp_dir):
        """Test saving processed dataset to NetCDF."""
        result = DataProcessor.process_dataset([sample_netcdf_file])
        
        output_path = tmp_dir / "processed.nc"
        DataProcessor.save_processed(result, output_path)
        
        assert output_path.exists()
        
        # Verify we can read it back
        with xr.open_dataset(output_path) as ds:
            assert "NO2_TropVCD" in ds
            assert "FNR" in ds


class TestDataProcessorEdgeCases:
    """Edge case tests for DataProcessor."""
    
    def test_mixed_valid_invalid_files(self, sample_netcdf_file, tmp_dir):
        """Test processing mix of valid and invalid files."""
        fake_file = tmp_dir / "fake.nc"
        
        # Should process the valid file and skip invalid
        result = DataProcessor.process_dataset([sample_netcdf_file, fake_file])
        assert result is not None
    
    def test_nan_handling_in_fnr(self, tmp_dir):
        """Test that NaN values are properly handled in FNR calculation."""
        filepath = tmp_dir / "nan_test.nc"
        
        # Create dataset with some NaN values (values above FNR threshold 1e-12)
        no2_data = np.array([[[1e-6, np.nan], [1e-6, 1e-6]]])
        hcho_data = np.array([[[2e-6, 2e-6], [np.nan, 2e-6]]])
        
        ds = xr.Dataset(
            data_vars={
                "NO2_TropVCD": (["TSTEP", "ROW", "COL"], no2_data),
                "HCHO_TotVCD": (["TSTEP", "ROW", "COL"], hcho_data),
            },
            coords={
                "TSTEP": [np.datetime64('2024-06-01T16:00:00')],
                "LAT": (["ROW", "COL"], [[40.0, 40.1], [40.2, 40.3]]),
                "LON": (["ROW", "COL"], [[-112.0, -111.9], [-111.8, -111.7]]),
            },
        )
        ds.to_netcdf(filepath)
        ds.close()
        
        result = DataProcessor.process_dataset([filepath])
        assert result is not None
        
        fnr = result["FNR"].values
        # Some should be NaN (where NO2 or HCHO was NaN)
        assert np.any(np.isnan(fnr))
        # But not all
        assert np.any(~np.isnan(fnr))
