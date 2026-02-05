"""
Integration tests for TEMPO Analyzer - End-to-end workflow with real data.

These tests download actual data from NASA RSIG and verify the full pipeline.
Run with: pytest tests/test_integration.py -v --timeout=1800 -s

WARNING: These tests require network access and may take 10-15 minutes.
"""

import pytest
import asyncio
import shutil
from pathlib import Path
from datetime import datetime, date
import sys

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from tempo_app.storage.database import Database
from tempo_app.storage.models import Dataset, Granule, BoundingBox, DatasetStatus
from tempo_app.core.downloader import RSIGDownloader
from tempo_app.core.processor import DataProcessor
from tempo_app.core.exporter import DataExporter
from tempo_app.core.plotter import MapPlotter


# Test configuration
INTEGRATION_TEST_CONFIG = {
    "region": "Utah (Salt Lake)",
    "bbox": [-112.8, 40.0, -111.5, 41.5],
    "date_start": "2024-06-01",
    "date_end": "2024-06-14",  # 2 weeks
    "hours": [16, 17, 18, 19, 20],  # 5 peak hours
    "max_cloud": 0.5,
    "max_sza": 70.0,
}


@pytest.fixture(scope="module")
def integration_workdir(tmp_path_factory):
    """Create a persistent work directory for integration tests."""
    workdir = tmp_path_factory.mktemp("tempo_integration")
    yield workdir
    # Cleanup after all tests
    shutil.rmtree(workdir, ignore_errors=True)


@pytest.fixture(scope="module")
def integration_db(integration_workdir):
    """Create a database for integration tests."""
    db_path = integration_workdir / "integration_test.db"
    return Database(db_path)


@pytest.fixture(scope="module")
def integration_dataset(integration_db):
    """Create and store the test dataset in the database."""
    config = INTEGRATION_TEST_CONFIG
    
    dataset = Dataset(
        id="integration-test-001",
        name="Integration Test Dataset",
        created_at=datetime.now(),
        bbox=BoundingBox.from_list(config["bbox"]),
        date_start=date.fromisoformat(config["date_start"]),
        date_end=date.fromisoformat(config["date_end"]),
        day_filter=[0, 1, 2, 3, 4, 5, 6],  # All days
        hour_filter=config["hours"],
        max_cloud=config["max_cloud"],
        max_sza=config["max_sza"],
        status=DatasetStatus.PENDING,
    )
    
    integration_db.create_dataset(dataset)
    return dataset


class MockIntegrationStatus:
    """Status manager for integration tests that prints progress."""
    
    def emit(self, event: str, message: str, value: float = None):
        progress = f" [{value*100:.1f}%]" if value is not None else ""
        print(f"  [{event.upper()}]{progress} {message}")


@pytest.fixture
def integration_status():
    """Status manager for logging progress."""
    return MockIntegrationStatus()


class TestIntegrationDownload:
    """Integration tests for data download."""
    
    @pytest.mark.integration
    @pytest.mark.timeout(1800)  # 30 minute timeout
    @pytest.mark.asyncio
    async def test_full_workflow_download(
        self, integration_workdir, integration_dataset, integration_status
    ):
        """Test downloading real data from NASA RSIG."""
        config = INTEGRATION_TEST_CONFIG
        
        print("\n" + "="*60)
        print("INTEGRATION TEST: Full Download Workflow")
        print(f"Region: {config['region']}")
        print(f"Dates: {config['date_start']} to {config['date_end']}")
        print(f"Hours: {config['hours']}")
        print("="*60)
        
        downloader = RSIGDownloader(integration_workdir)
        
        # Generate date list
        from datetime import timedelta
        start = date.fromisoformat(config["date_start"])
        end = date.fromisoformat(config["date_end"])
        dates = []
        current = start
        while current <= end:
            dates.append(current.isoformat())
            current += timedelta(days=1)
        
        print(f"\nDownloading {len(dates)} days × {len(config['hours'])} hours = {len(dates) * len(config['hours'])} granules...")
        
        files = await downloader.download_granules(
            dates=dates,
            hours=config["hours"],
            bbox=config["bbox"],
            dataset_name=integration_dataset.name,
            max_cloud=config["max_cloud"],
            max_sza=config["max_sza"],
            status=integration_status,
        )
        
        print(f"\nDownload complete. {len(files)} files created.")
        
        # Store file list for other tests
        integration_workdir._downloaded_files = files
        
        # Assertions
        assert len(files) > 0, "No files were downloaded"
        for f in files:
            assert f.exists(), f"File does not exist: {f}"
            assert f.stat().st_size > 1000, f"File too small: {f}"
        
        print("✓ Download test PASSED")


class TestIntegrationProcess:
    """Integration tests for data processing."""
    
    @pytest.mark.integration
    @pytest.mark.timeout(300)  # 5 minute timeout
    def test_full_workflow_process(self, integration_workdir, integration_dataset):
        """Test processing downloaded data."""
        print("\n" + "="*60)
        print("INTEGRATION TEST: Processing Workflow")
        print("="*60)
        
        # Get downloaded files
        files = getattr(integration_workdir, '_downloaded_files', None)
        
        if not files:
            # Try to find files in the dataset directory
            dataset_dir = integration_workdir / integration_dataset.id
            files = list(dataset_dir.glob("*.nc")) if dataset_dir.exists() else []
        
        if not files:
            pytest.skip("No downloaded files found - run download test first")
        
        print(f"\nProcessing {len(files)} files...")
        
        result = DataProcessor.process_dataset(files)
        
        assert result is not None, "Processing returned None"
        assert "NO2_TropVCD" in result, "NO2 data missing"
        assert "HCHO_TotVCD" in result, "HCHO data missing"
        assert "FNR" in result, "FNR not calculated"
        
        # Save processed data
        output_path = integration_workdir / "processed" / f"{integration_dataset.id}.nc"
        output_path.parent.mkdir(exist_ok=True)
        DataProcessor.save_processed(result, output_path)
        
        assert output_path.exists()
        print(f"Saved processed data: {output_path}")
        
        # Store for other tests
        integration_workdir._processed_path = output_path
        integration_workdir._processed_dataset = result
        
        print("✓ Processing test PASSED")


class TestIntegrationExport:
    """Integration tests for data export."""
    
    @pytest.mark.integration
    @pytest.mark.timeout(120)  # 2 minute timeout
    def test_full_workflow_export(self, integration_workdir, integration_dataset):
        """Test exporting processed data to Excel."""
        print("\n" + "="*60)
        print("INTEGRATION TEST: Export Workflow")
        print("="*60)
        
        processed_ds = getattr(integration_workdir, '_processed_dataset', None)
        
        if processed_ds is None:
            pytest.skip("No processed dataset found - run process test first")
        
        exporter = DataExporter(integration_workdir)
        
        print("\nExporting to Legacy format...")
        files = exporter.export_dataset(
            dataset=processed_ds,
            dataset_name=integration_dataset.name,
            export_format="legacy",
        )
        
        if files:
            print(f"Created {len(files)} export files:")
            for f in files:
                print(f"  - {Path(f).name}")
                assert Path(f).exists()
        else:
            print("No exports created (no sites in region or empty data)")
        
        print("✓ Export test PASSED")


class TestIntegrationPlot:
    """Integration tests for map generation."""
    
    @pytest.mark.integration
    @pytest.mark.timeout(120)  # 2 minute timeout
    def test_full_workflow_plot(self, integration_workdir, integration_dataset):
        """Test generating map visualization."""
        print("\n" + "="*60)
        print("INTEGRATION TEST: Plot Workflow")
        print("="*60)
        
        processed_ds = getattr(integration_workdir, '_processed_dataset', None)
        
        if processed_ds is None:
            pytest.skip("No processed dataset found - run process test first")
        
        plotter = MapPlotter(integration_workdir)
        config = INTEGRATION_TEST_CONFIG
        
        print("\nGenerating NO2 map for hour 18...")
        result, messages = plotter.generate_map(
            dataset=processed_ds,
            hour=18,
            variable="NO2",
            dataset_name=integration_dataset.name,
            bbox=config["bbox"],
        )

        if messages:
            print(f"Messages from plotter: {messages}")

        if result:
            print(f"Created map: {Path(result).name}")
            assert Path(result).exists()
            assert Path(result).stat().st_size > 1000  # Should be a real image
        else:
            print("Map generation returned None (cartopy may not be installed)")
        
        print("✓ Plot test PASSED")


# Convenience test class for running all integration tests together
class TestFullIntegrationPipeline:
    """Run all integration tests in sequence."""
    
    @pytest.mark.integration
    @pytest.mark.timeout(2400)  # 40 minute timeout for full pipeline
    @pytest.mark.asyncio
    async def test_complete_pipeline(self, tmp_path):
        """Run complete pipeline: download → process → export → plot."""
        print("\n" + "="*70)
        print("FULL INTEGRATION PIPELINE TEST")
        print("="*70)
        
        config = INTEGRATION_TEST_CONFIG
        workdir = tmp_path / "full_pipeline"
        workdir.mkdir()
        
        # Setup
        db = Database(workdir / "test.db")
        status = MockIntegrationStatus()
        
        dataset = Dataset(
            id="pipeline-test",
            name="Full Pipeline Test",
            created_at=datetime.now(),
            bbox=BoundingBox.from_list(config["bbox"]),
            date_start=date.fromisoformat(config["date_start"]),
            date_end=date.fromisoformat(config["date_end"]),
            day_filter=[0, 1, 2, 3, 4, 5, 6],
            hour_filter=config["hours"],
            max_cloud=config["max_cloud"],
            max_sza=config["max_sza"],
        )
        db.create_dataset(dataset)
        
        # Step 1: Download
        print("\n[1/4] DOWNLOADING...")
        downloader = RSIGDownloader(workdir)
        
        from datetime import timedelta
        start = date.fromisoformat(config["date_start"])
        end = date.fromisoformat(config["date_end"])
        dates = []
        current = start
        while current <= end:
            dates.append(current.isoformat())
            current += timedelta(days=1)
        
        files = await downloader.download_granules(
            dates=dates,
            hours=config["hours"],
            bbox=config["bbox"],
            dataset_name=dataset.name,
            max_cloud=config["max_cloud"],
            max_sza=config["max_sza"],
            status=status,
        )
        print(f"Downloaded {len(files)} files")
        assert len(files) > 0
        
        # Step 2: Process
        print("\n[2/4] PROCESSING...")
        processed = DataProcessor.process_dataset(files)
        assert processed is not None
        assert "FNR" in processed
        print(f"Processed dataset with {len(processed.data_vars)} variables")
        
        # Step 3: Export
        print("\n[3/4] EXPORTING...")
        exporter = DataExporter(workdir)
        export_files = exporter.export_dataset(
            dataset=processed,
            dataset_name=dataset.name,
            export_format="legacy",
        )
        print(f"Created {len(export_files)} export files")
        
        # Step 4: Plot
        print("\n[4/4] PLOTTING...")
        plotter = MapPlotter(workdir)
        map_path, messages = plotter.generate_map(
            dataset=processed,
            hour=18,
            variable="NO2",
            dataset_name=dataset.name,
            bbox=config["bbox"],
        )
        if messages:
            print(f"Messages from plotter: {messages}")
        if map_path:
            print(f"Created map: {Path(map_path).name}")
        
        print("\n" + "="*70)
        print("✓✓✓ FULL PIPELINE TEST PASSED ✓✓✓")
        print("="*70)


# Configuration for running integration tests
def pytest_configure(config):
    """Add integration marker."""
    config.addinivalue_line(
        "markers", "integration: marks tests as integration tests (deselect with '-m \"not integration\"')"
    )
