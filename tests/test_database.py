"""
Tests for Database class (storage/database.py).
"""

import pytest
from datetime import datetime, date
from pathlib import Path

from tempo_app.storage.database import Database
from tempo_app.storage.models import (
    Dataset, Granule, ExportRecord, BoundingBox, DatasetStatus
)


class TestDatabaseDataset:
    """Tests for Dataset CRUD operations."""
    
    def test_create_dataset(self, tmp_db, sample_dataset):
        """Test creating a dataset."""
        tmp_db.create_dataset(sample_dataset)
        
        # Retrieve it
        retrieved = tmp_db.get_dataset(sample_dataset.id)
        assert retrieved is not None
        assert retrieved.id == sample_dataset.id
        assert retrieved.name == sample_dataset.name
    
    def test_get_dataset_not_found(self, tmp_db):
        """Test getting a non-existent dataset."""
        result = tmp_db.get_dataset("nonexistent-id")
        assert result is None
    
    def test_get_dataset_by_name(self, tmp_db, sample_dataset):
        """Test getting dataset by name."""
        tmp_db.create_dataset(sample_dataset)
        
        retrieved = tmp_db.get_dataset_by_name(sample_dataset.name)
        assert retrieved is not None
        assert retrieved.id == sample_dataset.id
    
    def test_get_all_datasets(self, tmp_db, sample_bbox):
        """Test getting all datasets."""
        # Create multiple datasets
        for i in range(3):
            ds = Dataset(
                id=f"test-{i}",
                name=f"Dataset {i}",
                created_at=datetime.now(),
                bbox=sample_bbox,
                date_start=date(2024, 6, 1),
                date_end=date(2024, 6, 14),
                day_filter=[],
                hour_filter=[],
                max_cloud=0.5,
                max_sza=70.0,
            )
            tmp_db.create_dataset(ds)
        
        datasets = tmp_db.get_all_datasets()
        assert len(datasets) == 3
    
    def test_update_dataset(self, tmp_db, sample_dataset):
        """Test updating a dataset."""
        tmp_db.create_dataset(sample_dataset)
        
        # Update it
        sample_dataset.status = DatasetStatus.COMPLETE
        sample_dataset.granules_downloaded = 50
        tmp_db.update_dataset(sample_dataset)
        
        # Verify update
        retrieved = tmp_db.get_dataset(sample_dataset.id)
        assert retrieved.status == DatasetStatus.COMPLETE
        assert retrieved.granules_downloaded == 50
    
    def test_delete_dataset_cascades_granules(self, tmp_db, sample_dataset, sample_granules):
        """Test that deleting dataset removes associated granules."""
        tmp_db.create_dataset(sample_dataset)
        tmp_db.create_granules_batch(sample_granules)
        
        # Verify granules exist
        granules = tmp_db.get_granules_for_dataset(sample_dataset.id)
        assert len(granules) == len(sample_granules)
        
        # Delete dataset
        tmp_db.delete_dataset(sample_dataset.id)
        
        # Verify cascaded deletion
        granules_after = tmp_db.get_granules_for_dataset(sample_dataset.id)
        assert len(granules_after) == 0
    
    def test_delete_dataset_removes_file(self, tmp_db, sample_dataset, tmp_dir):
        """Test that deleting dataset removes associated file."""
        # Create a dummy file
        dummy_file = tmp_dir / "test_output.nc"
        dummy_file.touch()
        sample_dataset.file_path = str(dummy_file)
        
        tmp_db.create_dataset(sample_dataset)
        assert dummy_file.exists()
        
        # Delete dataset
        tmp_db.delete_dataset(sample_dataset.id)
        
        # Verify file removed
        assert not dummy_file.exists()
    
    def test_touch_dataset(self, tmp_db, sample_dataset):
        """Test updating last_accessed timestamp."""
        tmp_db.create_dataset(sample_dataset)
        
        # Touch it
        tmp_db.touch_dataset(sample_dataset.id)
        
        # Verify timestamp updated
        retrieved = tmp_db.get_dataset(sample_dataset.id)
        assert retrieved.last_accessed is not None


class TestDatabaseGranule:
    """Tests for Granule CRUD operations."""
    
    def test_create_granule(self, tmp_db, sample_dataset):
        """Test creating a single granule."""
        tmp_db.create_dataset(sample_dataset)
        
        granule = Granule(
            dataset_id=sample_dataset.id,
            date=date(2024, 6, 1),
            hour=16,
            bbox_west=-112.8, bbox_south=40.0,
            bbox_east=-111.5, bbox_north=41.5,
            max_cloud=0.5, max_sza=70.0,
        )
        tmp_db.create_granule(granule)
        
        granules = tmp_db.get_granules_for_dataset(sample_dataset.id)
        assert len(granules) == 1
    
    def test_create_granules_batch(self, tmp_db, sample_dataset, sample_granules):
        """Test batch granule insertion."""
        tmp_db.create_dataset(sample_dataset)
        tmp_db.create_granules_batch(sample_granules)
        
        granules = tmp_db.get_granules_for_dataset(sample_dataset.id)
        assert len(granules) == len(sample_granules)
    
    def test_get_pending_granules(self, tmp_db, sample_dataset, sample_granules):
        """Test filtering for non-downloaded granules."""
        tmp_db.create_dataset(sample_dataset)
        
        # Mark some as downloaded
        for i, g in enumerate(sample_granules):
            g.downloaded = (i % 2 == 0)  # Every other one downloaded
        
        tmp_db.create_granules_batch(sample_granules)
        
        pending = tmp_db.get_pending_granules(sample_dataset.id)
        expected_pending = sum(1 for g in sample_granules if not g.downloaded)
        assert len(pending) == expected_pending
    
    def test_update_granule(self, tmp_db, sample_dataset):
        """Test updating granule after download."""
        tmp_db.create_dataset(sample_dataset)
        
        granule = Granule(
            dataset_id=sample_dataset.id,
            date=date(2024, 6, 1),
            hour=16,
            bbox_west=-112.8, bbox_south=40.0,
            bbox_east=-111.5, bbox_north=41.5,
            max_cloud=0.5, max_sza=70.0,
        )
        tmp_db.create_granule(granule)
        
        # Retrieve to get ID
        granules = tmp_db.get_granules_for_dataset(sample_dataset.id)
        granule = granules[0]
        
        # Update
        granule.downloaded = True
        granule.no2_valid_pixels = 5000
        granule.file_size_bytes = 1024 * 1024
        tmp_db.update_granule(granule)
        
        # Verify
        granules = tmp_db.get_granules_for_dataset(sample_dataset.id)
        assert granules[0].downloaded is True
        assert granules[0].no2_valid_pixels == 5000


class TestDatabaseExport:
    """Tests for ExportRecord operations."""
    
    def test_create_export(self, tmp_db, sample_dataset):
        """Test creating an export record."""
        tmp_db.create_dataset(sample_dataset)
        
        export = ExportRecord(
            dataset_id=sample_dataset.id,
            format="legacy",
            file_path="/path/to/export.xlsx",
            file_size_bytes=50000,
        )
        tmp_db.create_export(export)
        
        exports = tmp_db.get_exports_for_dataset(sample_dataset.id)
        assert len(exports) == 1
        assert exports[0].format == "legacy"
    
    def test_get_exports_for_dataset(self, tmp_db, sample_dataset):
        """Test getting all exports for a dataset."""
        tmp_db.create_dataset(sample_dataset)
        
        # Create multiple exports
        for fmt in ["legacy", "v1_hourly", "v2_daily"]:
            export = ExportRecord(
                dataset_id=sample_dataset.id,
                format=fmt,
                file_path=f"/path/to/{fmt}.xlsx",
            )
            tmp_db.create_export(export)
        
        exports = tmp_db.get_exports_for_dataset(sample_dataset.id)
        assert len(exports) == 3


class TestDatabaseStorage:
    """Tests for storage statistics."""
    
    def test_get_storage_stats(self, tmp_db, sample_dataset, sample_granules):
        """Test storage statistics calculation."""
        tmp_db.create_dataset(sample_dataset)
        
        # Add some file sizes
        for i, g in enumerate(sample_granules):
            g.file_size_bytes = 1024 * 1024  # 1 MB each
        
        tmp_db.create_granules_batch(sample_granules)
        
        stats = tmp_db.get_storage_stats()
        assert "dataset_count" in stats
        assert "granule_count" in stats
        assert stats["dataset_count"] == 1
        assert stats["granule_count"] == len(sample_granules)
