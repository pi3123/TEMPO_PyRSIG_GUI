"""
Tests for RSIGDownloader class (core/downloader.py).
"""

import pytest
import asyncio
from pathlib import Path

from tempo_app.core.downloader import RSIGDownloader


@pytest.fixture
def downloader(tmp_dir):
    """Create a downloader with temporary work directory."""
    return RSIGDownloader(tmp_dir)


class TestRSIGDownloader:
    """Tests for RSIGDownloader functionality."""
    
    @pytest.mark.asyncio
    async def test_workdir_created(self, tmp_dir):
        """Test that workdir is created on initialization."""
        workdir = tmp_dir / "downloads"
        assert not workdir.exists()
        
        downloader = RSIGDownloader(workdir)
        assert workdir.exists()
    
    @pytest.mark.asyncio
    async def test_simulate_download(self, downloader, mock_status):
        """Test fallback simulation when pyrsig is missing."""
        dates = ["2024-06-01", "2024-06-02"]
        hours = [16, 17]
        dataset_name = "test-sim"
        
        # Call simulate directly
        files = await downloader._simulate_download(
            dates=dates,
            hours=hours,
            dataset_name=dataset_name,
            status=mock_status,
        )
        
        # Should create files
        assert len(files) == len(dates) * len(hours)
        for f in files:
            assert f.exists()
    
    @pytest.mark.asyncio
    async def test_simulate_download_creates_dataset_dir(self, downloader, mock_status):
        """Test that dataset subdirectory is created."""
        dataset_name = "new-dataset-123"
        
        await downloader._simulate_download(
            dates=["2024-06-01"],
            hours=[16],
            dataset_name=dataset_name,
            status=mock_status,
        )
        
        dataset_dir = downloader.workdir / dataset_name
        assert dataset_dir.exists()
    
    @pytest.mark.asyncio
    async def test_progress_events_fired(self, downloader, mock_status):
        """Test that status events are emitted during download."""
        await downloader._simulate_download(
            dates=["2024-06-01"],
            hours=[16, 17],
            dataset_name="test-events",
            status=mock_status,
        )
        
        # Should have progress events
        assert len(mock_status.events) > 0
        
        # Check for download events
        download_events = [e for e in mock_status.events if e["event"] == "download"]
        assert len(download_events) == 2  # Two hours


class TestRSIGDownloaderAsync:
    """Async-specific tests for RSIGDownloader."""
    
    @pytest.mark.asyncio
    async def test_download_granules_no_pyrsig(self, downloader, mock_status):
        """Test download_granules fallback when pyrsig is not installed."""
        # This will fallback to simulation if pyrsig is not installed
        files = await downloader.download_granules(
            dates=["2024-06-01"],
            hours=[16],
            bbox=[-112.8, 40.0, -111.5, 41.5],
            dataset_name="test-fallback",
            status=mock_status,
        )
        
        # Should return files (either real or simulated)
        assert isinstance(files, list)
    
    @pytest.mark.asyncio
    async def test_concurrent_downloads_simulated(self, tmp_dir, mock_status):
        """Test running multiple simulated downloads concurrently."""
        downloader1 = RSIGDownloader(tmp_dir / "dl1")
        downloader2 = RSIGDownloader(tmp_dir / "dl2")
        
        # Run two downloads concurrently
        task1 = downloader1._simulate_download(
            dates=["2024-06-01"],
            hours=[16],
            dataset_name="ds1",
            status=mock_status,
        )
        task2 = downloader2._simulate_download(
            dates=["2024-06-02"],
            hours=[17],
            dataset_name="ds2",
            status=mock_status,
        )
        
        files1, files2 = await asyncio.gather(task1, task2)
        
        assert len(files1) == 1
        assert len(files2) == 1


class TestRSIGDownloaderValidation:
    """Validation tests for downloader parameters."""
    
    @pytest.mark.asyncio
    async def test_empty_dates(self, downloader, mock_status):
        """Test handling empty dates list."""
        files = await downloader._simulate_download(
            dates=[],
            hours=[16],
            dataset_name="empty-dates",
            status=mock_status,
        )
        
        assert files == []
    
    @pytest.mark.asyncio
    async def test_empty_hours(self, downloader, mock_status):
        """Test handling empty hours list."""
        files = await downloader._simulate_download(
            dates=["2024-06-01"],
            hours=[],
            dataset_name="empty-hours",
            status=mock_status,
        )
        
        assert files == []
