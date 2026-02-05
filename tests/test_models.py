"""
Tests for data models (BoundingBox, Dataset, Granule, etc.)
"""

import pytest
from datetime import datetime, date
from tempo_app.storage.models import (
    BoundingBox, Dataset, Granule, ExportRecord, 
    DatasetStatus, REGION_PRESETS
)


class TestBoundingBox:
    """Tests for BoundingBox dataclass."""
    
    def test_to_list(self, sample_bbox):
        """Test BoundingBox serialization to list."""
        result = sample_bbox.to_list()
        assert result == [-112.8, 40.0, -111.5, 41.5]
        assert len(result) == 4
    
    def test_from_list(self):
        """Test BoundingBox deserialization from list."""
        coords = [-119.68, 32.23, -116.38, 35.73]
        bbox = BoundingBox.from_list(coords)
        assert bbox.west == -119.68
        assert bbox.south == 32.23
        assert bbox.east == -116.38
        assert bbox.north == 35.73
    
    def test_contains_point_inside(self, sample_bbox):
        """Test point inside bounding box."""
        # Salt Lake City is at approximately 40.76, -111.89
        assert sample_bbox.contains_point(40.76, -111.89) is True
    
    def test_contains_point_outside(self, sample_bbox):
        """Test point outside bounding box."""
        # Los Angeles is way outside Utah
        assert sample_bbox.contains_point(34.05, -118.24) is False
    
    def test_contains_point_edge(self, sample_bbox):
        """Test point on edge of bounding box."""
        # Exactly on the west edge
        assert sample_bbox.contains_point(40.5, -112.8) is True


class TestDataset:
    """Tests for Dataset dataclass."""
    
    def test_progress_zero(self, sample_dataset):
        """Test progress calculation with no downloads."""
        sample_dataset.granule_count = 0
        assert sample_dataset.progress == 0.0
    
    def test_progress_partial(self, sample_dataset):
        """Test progress calculation with partial downloads."""
        sample_dataset.granule_count = 100
        sample_dataset.granules_downloaded = 50
        assert sample_dataset.progress == 0.5
    
    def test_progress_complete(self, sample_dataset):
        """Test progress calculation when complete."""
        sample_dataset.granule_count = 100
        sample_dataset.granules_downloaded = 100
        assert sample_dataset.progress == 1.0
    
    def test_is_complete(self, sample_dataset):
        """Test is_complete property."""
        sample_dataset.status = DatasetStatus.COMPLETE
        assert sample_dataset.is_complete is True
        
        sample_dataset.status = DatasetStatus.PARTIAL
        assert sample_dataset.is_complete is False
    
    def test_day_filter_weekdays(self, sample_dataset):
        """Test day filter string for weekdays."""
        sample_dataset.day_filter = [0, 1, 2, 3, 4]
        assert sample_dataset.day_filter_str() == "Weekdays"
    
    def test_day_filter_weekends(self, sample_dataset):
        """Test day filter string for weekends."""
        sample_dataset.day_filter = [5, 6]
        assert sample_dataset.day_filter_str() == "Weekends"
    
    def test_hour_filter_str(self, sample_dataset):
        """Test hour filter string formatting."""
        sample_dataset.hour_filter = [14, 15, 16, 17, 18]
        assert "14:00" in sample_dataset.hour_filter_str()
        assert "18:00" in sample_dataset.hour_filter_str()


class TestGranule:
    """Tests for Granule dataclass."""
    
    def test_compute_content_hash_deterministic(self):
        """Test that content hash is deterministic."""
        g1 = Granule(
            dataset_id="test",
            date=date(2024, 6, 1),
            hour=16,
            bbox_west=-112.8, bbox_south=40.0,
            bbox_east=-111.5, bbox_north=41.5,
            max_cloud=0.5, max_sza=70.0
        )
        g2 = Granule(
            dataset_id="test",
            date=date(2024, 6, 1),
            hour=16,
            bbox_west=-112.8, bbox_south=40.0,
            bbox_east=-111.5, bbox_north=41.5,
            max_cloud=0.5, max_sza=70.0
        )
        
        assert g1.compute_content_hash() == g2.compute_content_hash()
    
    def test_compute_content_hash_different_params(self):
        """Test that different params produce different hashes."""
        g1 = Granule(
            dataset_id="test",
            date=date(2024, 6, 1),
            hour=16,
            bbox_west=-112.8, bbox_south=40.0,
            bbox_east=-111.5, bbox_north=41.5,
            max_cloud=0.5, max_sza=70.0
        )
        g2 = Granule(
            dataset_id="test",
            date=date(2024, 6, 1),
            hour=17,  # Different hour
            bbox_west=-112.8, bbox_south=40.0,
            bbox_east=-111.5, bbox_north=41.5,
            max_cloud=0.5, max_sza=70.0
        )
        
        assert g1.compute_content_hash() != g2.compute_content_hash()
    
    def test_datetime_str(self):
        """Test datetime string formatting."""
        g = Granule(date=date(2024, 6, 15), hour=18)
        assert g.datetime_str == "2024-06-15 @ 18:00 UTC"


class TestDatasetStatus:
    """Tests for DatasetStatus enum."""
    
    def test_status_values(self):
        """Test all status values exist."""
        assert DatasetStatus.PENDING.value == "pending"
        assert DatasetStatus.DOWNLOADING.value == "downloading"
        assert DatasetStatus.PARTIAL.value == "partial"
        assert DatasetStatus.COMPLETE.value == "complete"
        assert DatasetStatus.ERROR.value == "error"


class TestRegionPresets:
    """Tests for region presets."""
    
    def test_presets_exist(self):
        """Test that region presets are defined."""
        assert "Southern California" in REGION_PRESETS
        assert "Utah (Salt Lake)" in REGION_PRESETS
    
    def test_preset_structure(self):
        """Test preset structure (bbox, fips)."""
        bbox, fips = REGION_PRESETS["Southern California"]
        assert isinstance(bbox, BoundingBox)
        assert fips == "06"  # California FIPS code
