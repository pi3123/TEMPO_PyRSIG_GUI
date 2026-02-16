"""
Tests for batch_parser module - CSV/Excel import with per-site customization.
"""

import pytest
import tempfile
from pathlib import Path
from datetime import date

from tempo_app.core.batch_parser import (
    parse_import_file,
    create_sample_excel,
    ParsedSite,
    ParseResult,
)


class TestBatchParserBasic:
    """Tests for basic CSV parsing functionality."""

    def test_parse_csv_basic(self, tmp_dir):
        """Test parsing a basic CSV with required columns only."""
        csv_path = tmp_dir / "sites_basic.csv"
        csv_content = """name,latitude,longitude
Site A,40.7128,-74.0060
Site B,34.0522,-118.2437
"""
        csv_path.write_text(csv_content)

        result = parse_import_file(csv_path)

        assert result.is_valid
        assert len(result.valid_sites) == 2
        assert result.valid_sites[0].site_name == "Site A"
        assert result.valid_sites[0].latitude == 40.7128
        assert result.valid_sites[0].longitude == -74.0060

    def test_parse_csv_with_radius_km(self, tmp_dir):
        """Test parsing CSV with radius_km column."""
        csv_path = tmp_dir / "sites_radius.csv"
        csv_content = """name,latitude,longitude,radius_km
Site A,40.7128,-74.0060,15.0
Site B,34.0522,-118.2437,25.5
Site C,41.8781,-87.6298,
"""
        csv_path.write_text(csv_content)

        result = parse_import_file(csv_path)

        assert result.is_valid
        assert len(result.valid_sites) == 3

        # Site A with custom radius
        site_a = result.valid_sites[0]
        assert site_a.custom_radius_km == 15.0

        # Site B with custom radius
        site_b = result.valid_sites[1]
        assert site_b.custom_radius_km == 25.5

        # Site C with no radius (blank)
        site_c = result.valid_sites[2]
        assert site_c.custom_radius_km is None

    def test_parse_csv_with_time_range(self, tmp_dir):
        """Test parsing CSV with hour_start and hour_end columns."""
        csv_path = tmp_dir / "sites_hours.csv"
        csv_content = """name,latitude,longitude,hour_start,hour_end
Site A,40.7128,-74.0060,8,16
Site B,34.0522,-118.2437,,
Site C,41.8781,-87.6298,10,14
"""
        csv_path.write_text(csv_content)

        result = parse_import_file(csv_path)

        assert result.is_valid
        assert len(result.valid_sites) == 3

        # Site A with custom hours
        site_a = result.valid_sites[0]
        assert site_a.custom_hour_start == 8
        assert site_a.custom_hour_end == 16

        # Site B with no hours (blank - uses defaults)
        site_b = result.valid_sites[1]
        assert site_b.custom_hour_start is None
        assert site_b.custom_hour_end is None

        # Site C with different hours
        site_c = result.valid_sites[2]
        assert site_c.custom_hour_start == 10
        assert site_c.custom_hour_end == 14

    def test_parse_csv_with_quality_filters(self, tmp_dir):
        """Test parsing CSV with max_cloud and max_sza columns."""
        csv_path = tmp_dir / "sites_quality.csv"
        csv_content = """name,latitude,longitude,max_cloud,max_sza
Site A,40.7128,-74.0060,0.3,70.0
Site B,34.0522,-118.2437,0.5,80.0
Site C,41.8781,-87.6298,,
"""
        csv_path.write_text(csv_content)

        result = parse_import_file(csv_path)

        assert result.is_valid
        assert len(result.valid_sites) == 3

        # Site A with custom quality filters
        site_a = result.valid_sites[0]
        assert site_a.custom_max_cloud == 0.3
        assert site_a.custom_max_sza == 70.0

        # Site B with different filters
        site_b = result.valid_sites[1]
        assert site_b.custom_max_cloud == 0.5
        assert site_b.custom_max_sza == 80.0

        # Site C with no filters (blank)
        site_c = result.valid_sites[2]
        assert site_c.custom_max_cloud is None
        assert site_c.custom_max_sza is None

    def test_parse_csv_with_date_range(self, tmp_dir):
        """Test parsing CSV with date_start and date_end columns."""
        csv_path = tmp_dir / "sites_dates.csv"
        csv_content = """name,latitude,longitude,date_start,date_end
Site A,40.7128,-74.0060,2024-01-01,2024-01-31
Site B,34.0522,-118.2437,,
"""
        csv_path.write_text(csv_content)

        result = parse_import_file(csv_path)

        assert result.is_valid
        assert len(result.valid_sites) == 2

        # Site A with custom dates
        site_a = result.valid_sites[0]
        assert site_a.custom_date_start == "2024-01-01"
        assert site_a.custom_date_end == "2024-01-31"

        # Site B with no dates
        site_b = result.valid_sites[1]
        assert site_b.custom_date_start is None
        assert site_b.custom_date_end is None

    def test_parse_csv_full_customization(self, tmp_dir):
        """Test parsing CSV with all optional columns."""
        csv_path = tmp_dir / "sites_full.csv"
        csv_content = """name,latitude,longitude,radius_km,date_start,date_end,hour_start,hour_end,max_cloud,max_sza
Site A,40.7128,-74.0060,12.5,2024-06-01,2024-06-30,14,20,0.25,65.0
Site B,34.0522,-118.2437,8.0,2024-07-01,2024-07-15,10,16,0.4,75.0
"""
        csv_path.write_text(csv_content)

        result = parse_import_file(csv_path)

        assert result.is_valid
        assert len(result.valid_sites) == 2

        # Verify Site A has all custom values
        site_a = result.valid_sites[0]
        assert site_a.custom_radius_km == 12.5
        assert site_a.custom_date_start == "2024-06-01"
        assert site_a.custom_date_end == "2024-06-30"
        assert site_a.custom_hour_start == 14
        assert site_a.custom_hour_end == 20
        assert site_a.custom_max_cloud == 0.25
        assert site_a.custom_max_sza == 65.0


class TestBatchParserEdgeCases:
    """Tests for edge cases and error handling."""

    def test_invalid_hour_range(self, tmp_dir):
        """Test that invalid hours generate warnings but don't fail."""
        csv_path = tmp_dir / "sites_bad_hours.csv"
        csv_content = """name,latitude,longitude,hour_start,hour_end
Site A,40.7128,-74.0060,25,30
"""
        csv_path.write_text(csv_content)

        result = parse_import_file(csv_path)

        # Should still be valid but with warnings
        assert result.is_valid
        assert len(result.valid_sites) == 1
        # Hours outside 0-23 should be set to None with warning
        site = result.valid_sites[0]
        assert site.custom_hour_start is None
        assert site.custom_hour_end is None
        assert len(result.warnings) >= 2  # Should have warnings for invalid hours

    def test_invalid_coordinates(self, tmp_dir):
        """Test that invalid coordinates are caught."""
        csv_path = tmp_dir / "sites_bad_coords.csv"
        csv_content = """name,latitude,longitude
Site A,invalid,-74.0060
Site B,40.7128,invalid
Site C,999,-999
"""
        csv_path.write_text(csv_content)

        result = parse_import_file(csv_path)

        # Should have 3 sites but all with errors
        assert len(result.sites) == 3
        assert len(result.invalid_sites) == 3
        assert len(result.valid_sites) == 0

    def test_missing_required_columns(self, tmp_dir):
        """Test that missing required columns are reported."""
        csv_path = tmp_dir / "sites_missing_cols.csv"
        csv_content = """name,latitude
Site A,40.7128
"""
        csv_path.write_text(csv_content)

        result = parse_import_file(csv_path)

        assert not result.is_valid
        assert "longitude" in str(result.errors).lower()

    def test_column_name_aliases(self, tmp_dir):
        """Test that alternative column names work."""
        csv_path = tmp_dir / "sites_aliases.csv"
        csv_content = """site_name,lat,lon,radius
Site A,40.7128,-74.0060,10.0
"""
        csv_path.write_text(csv_content)

        result = parse_import_file(csv_path)

        assert result.is_valid
        assert len(result.valid_sites) == 1
        assert result.valid_sites[0].site_name == "Site A"

    def test_empty_file(self, tmp_dir):
        """Test handling of empty CSV."""
        csv_path = tmp_dir / "sites_empty.csv"
        csv_path.write_text("name,latitude,longitude\n")

        result = parse_import_file(csv_path)

        assert not result.is_valid
        assert "empty" in str(result.errors).lower()

    def test_nonexistent_file(self, tmp_dir):
        """Test handling of non-existent file."""
        csv_path = tmp_dir / "does_not_exist.csv"

        result = parse_import_file(csv_path)

        assert not result.is_valid
        assert "not found" in str(result.errors).lower()


class TestBatchParserExcel:
    """Tests for Excel file parsing."""

    def test_create_sample_excel(self, tmp_dir):
        """Test creating and parsing a sample Excel file."""
        excel_path = tmp_dir / "sample_sites.xlsx"

        create_sample_excel(excel_path, num_sites=3)

        assert excel_path.exists()

        result = parse_import_file(excel_path)

        assert result.is_valid
        assert len(result.valid_sites) == 3

        # Verify all columns are parsed
        site = result.valid_sites[0]
        assert site.custom_radius_km == 10.0
        assert site.custom_date_start == "2024-01-01"
        assert site.custom_date_end == "2024-01-31"
        assert site.custom_hour_start == 16
        assert site.custom_hour_end == 20
        assert site.custom_max_cloud == 0.3
        assert site.custom_max_sza == 70.0


class TestBatchParserWarnings:
    """Tests for warning generation."""

    def test_invalid_radius_warning(self, tmp_dir):
        """Test warning for invalid radius value."""
        csv_path = tmp_dir / "sites_bad_radius.csv"
        csv_content = """name,latitude,longitude,radius_km
Site A,40.7128,-74.0060,invalid
"""
        csv_path.write_text(csv_content)

        result = parse_import_file(csv_path)

        assert result.is_valid
        assert len(result.warnings) >= 1
        assert "radius" in str(result.warnings[0]).lower()

    def test_invalid_max_cloud_warning(self, tmp_dir):
        """Test warning for invalid max_cloud value."""
        csv_path = tmp_dir / "sites_bad_cloud.csv"
        csv_content = """name,latitude,longitude,max_cloud
Site A,40.7128,-74.0060,invalid
"""
        csv_path.write_text(csv_content)

        result = parse_import_file(csv_path)

        assert result.is_valid
        assert len(result.warnings) >= 1
        assert "cloud" in str(result.warnings[0]).lower()
