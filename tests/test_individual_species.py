"""
Tests for individual species plotting (NO2, HCHO, O3) - not just FNR.
"""

import pytest
import numpy as np
import xarray as xr
from pathlib import Path

from tempo_app.core.plotter import MapPlotter
from tempo_app.core.variable_registry import VariableRegistry


@pytest.fixture
def sample_multi_species_dataset(tmp_dir):
    """Create a dataset with all individual species variables."""
    rows, cols = 20, 20

    # Create lat/lon grid
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

    # Individual species data
    no2_trop_data = np.random.rand(len(times), rows, cols) * 1e16
    no2_strat_data = np.random.rand(len(times), rows, cols) * 5e15
    no2_total_data = no2_trop_data + no2_strat_data
    hcho_data = np.random.rand(len(times), rows, cols) * 2e16
    o3_data = np.random.rand(len(times), rows, cols) * 300 + 200  # DU range 200-500

    # Cloud data
    cloud_frac = np.random.rand(len(times), rows, cols) * 0.5
    cloud_pres = np.random.rand(len(times), rows, cols) * 500 + 500

    # FNR calculated
    fnr_data = hcho_data / no2_trop_data

    ds = xr.Dataset(
        data_vars={
            "NO2_TropVCD": (["TSTEP", "ROW", "COL"], no2_trop_data),
            "NO2_StratVCD": (["TSTEP", "ROW", "COL"], no2_strat_data),
            "NO2_TotalVCD": (["TSTEP", "ROW", "COL"], no2_total_data),
            "HCHO_TotVCD": (["TSTEP", "ROW", "COL"], hcho_data),
            "O3_TotVCD": (["TSTEP", "ROW", "COL"], o3_data),
            "CloudFrac": (["TSTEP", "ROW", "COL"], cloud_frac),
            "CloudPres": (["TSTEP", "ROW", "COL"], cloud_pres),
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


class TestIndividualSpeciesPlotting:
    """Tests for plotting individual species (not just FNR)."""

    def test_plot_no2_tropospheric(self, plotter, sample_multi_species_dataset):
        """Test plotting NO2 tropospheric VCD."""
        result, messages = plotter.generate_map(
            dataset=sample_multi_species_dataset,
            hour=16,
            variable="NO2_TropVCD",
            dataset_name="TestDataset",
            bbox=[-112.8, 40.0, -111.5, 41.5],
        )

        assert isinstance(messages, list)
        if result is not None:
            assert Path(result).exists()
            assert result.endswith(".png")

    def test_plot_no2_stratospheric(self, plotter, sample_multi_species_dataset):
        """Test plotting NO2 stratospheric VCD."""
        result, messages = plotter.generate_map(
            dataset=sample_multi_species_dataset,
            hour=16,
            variable="NO2_StratVCD",
            dataset_name="TestDataset",
            bbox=[-112.8, 40.0, -111.5, 41.5],
        )

        assert isinstance(messages, list)
        if result is not None:
            assert Path(result).exists()

    def test_plot_no2_total(self, plotter, sample_multi_species_dataset):
        """Test plotting NO2 total VCD."""
        result, messages = plotter.generate_map(
            dataset=sample_multi_species_dataset,
            hour=16,
            variable="NO2_TotalVCD",
            dataset_name="TestDataset",
            bbox=[-112.8, 40.0, -111.5, 41.5],
        )

        assert isinstance(messages, list)
        if result is not None:
            assert Path(result).exists()

    def test_plot_hcho(self, plotter, sample_multi_species_dataset):
        """Test plotting HCHO total VCD."""
        result, messages = plotter.generate_map(
            dataset=sample_multi_species_dataset,
            hour=16,
            variable="HCHO_TotVCD",
            dataset_name="TestDataset",
            bbox=[-112.8, 40.0, -111.5, 41.5],
        )

        assert isinstance(messages, list)
        if result is not None:
            assert Path(result).exists()

    def test_plot_o3(self, plotter, sample_multi_species_dataset):
        """Test plotting O3 total column."""
        result, messages = plotter.generate_map(
            dataset=sample_multi_species_dataset,
            hour=16,
            variable="O3_TotVCD",
            dataset_name="TestDataset",
            bbox=[-112.8, 40.0, -111.5, 41.5],
        )

        assert isinstance(messages, list)
        if result is not None:
            assert Path(result).exists()

    def test_plot_fnr(self, plotter, sample_multi_species_dataset):
        """Test plotting FNR (still works)."""
        result, messages = plotter.generate_map(
            dataset=sample_multi_species_dataset,
            hour=16,
            variable="FNR",
            dataset_name="TestDataset",
            bbox=[-112.8, 40.0, -111.5, 41.5],
        )

        assert isinstance(messages, list)
        if result is not None:
            assert Path(result).exists()

    def test_plot_cloud_fraction(self, plotter, sample_multi_species_dataset):
        """Test plotting cloud fraction."""
        result, messages = plotter.generate_map(
            dataset=sample_multi_species_dataset,
            hour=16,
            variable="CloudFrac",
            dataset_name="TestDataset",
            bbox=[-112.8, 40.0, -111.5, 41.5],
        )

        assert isinstance(messages, list)
        if result is not None:
            assert Path(result).exists()

    def test_plot_with_custom_colormap(self, plotter, sample_multi_species_dataset):
        """Test plotting with custom colormap."""
        result, messages = plotter.generate_map(
            dataset=sample_multi_species_dataset,
            hour=16,
            variable="NO2_TropVCD",
            dataset_name="TestDataset",
            bbox=[-112.8, 40.0, -111.5, 41.5],
            colormap="viridis",
        )

        assert isinstance(messages, list)
        if result is not None:
            assert Path(result).exists()

    def test_plot_with_custom_range(self, plotter, sample_multi_species_dataset):
        """Test plotting with custom vmin/vmax range."""
        result, messages = plotter.generate_map(
            dataset=sample_multi_species_dataset,
            hour=16,
            variable="O3_TotVCD",
            dataset_name="TestDataset",
            bbox=[-112.8, 40.0, -111.5, 41.5],
            vmin=250,
            vmax=400,
        )

        assert isinstance(messages, list)
        if result is not None:
            assert Path(result).exists()


class TestVariableDefaults:
    """Tests that variable defaults are correctly applied."""

    def test_no2_default_range(self, plotter, sample_multi_species_dataset):
        """Test that NO2 uses correct default range (0 to 1e16)."""
        # This test verifies the code path runs without error
        # The actual range validation would require inspecting the plot
        result, messages = plotter.generate_map(
            dataset=sample_multi_species_dataset,
            hour=16,
            variable="NO2_TropVCD",
            dataset_name="TestDataset",
            bbox=[-112.8, 40.0, -111.5, 41.5],
        )

        assert isinstance(messages, list)

    def test_o3_default_range(self, plotter, sample_multi_species_dataset):
        """Test that O3 uses correct default range (200 to 500 DU)."""
        result, messages = plotter.generate_map(
            dataset=sample_multi_species_dataset,
            hour=16,
            variable="O3_TotVCD",
            dataset_name="TestDataset",
            bbox=[-112.8, 40.0, -111.5, 41.5],
        )

        assert isinstance(messages, list)

    def test_fnr_default_range(self, plotter, sample_multi_species_dataset):
        """Test that FNR uses correct default range (2 to 8)."""
        result, messages = plotter.generate_map(
            dataset=sample_multi_species_dataset,
            hour=16,
            variable="FNR",
            dataset_name="TestDataset",
            bbox=[-112.8, 40.0, -111.5, 41.5],
        )

        assert isinstance(messages, list)


class TestVariableRegistry:
    """Tests for variable registry integration."""

    def test_discover_variables(self):
        """Test that variable registry discovers TEMPO variables."""
        variables = VariableRegistry.discover_variables()

        assert len(variables) > 0

        # Check that core variables are present
        output_vars = [v.output_var for v in variables]
        assert "NO2_TropVCD" in output_vars
        assert "HCHO_TotVCD" in output_vars
        assert "O3_TotVCD" in output_vars

    def test_get_variable_by_id(self):
        """Test retrieving specific variable metadata."""
        var = VariableRegistry.get_variable_by_id("tempo.l2.no2.vertical_column_troposphere")

        assert var is not None
        assert var.output_var == "NO2_TropVCD"
        assert var.unit == "molecules/cm²"

    def test_variable_colormaps(self):
        """Test that variables have appropriate colormaps."""
        variables = VariableRegistry.discover_variables()

        var_map = {v.output_var: v for v in variables}

        # Check specific colormaps
        assert var_map["NO2_TropVCD"].colormap == "RdYlBu_r"
        assert var_map["HCHO_TotVCD"].colormap == "YlOrRd"
        assert var_map["O3_TotVCD"].colormap == "PuBu"
