"""Batch job scheduler for processing multiple sites in parallel.

Manages the execution of batch import jobs, handling parallel site processing,
resume support, and progress tracking.
"""

import asyncio
from datetime import datetime, timedelta
from pathlib import Path
from typing import Callable, Optional
import logging
import uuid

from ..storage.database import Database
from ..storage.models import (
    BatchJob, BatchSite, BatchJobStatus, BatchSiteStatus,
    Dataset, DatasetStatus, Granule, BoundingBox
)
from .downloader import RSIGDownloader
from .processor import DataProcessor
from .geo_utils import bbox_from_center

logger = logging.getLogger(__name__)


def _sanitize_filename(name: str) -> str:
    """Sanitize a string for use as a filename."""
    return "".join(c if c.isalnum() or c in "._- " else "_" for c in name).strip()


class BatchScheduler:
    """Manages batch site processing with parallel execution and resume support.

    Example:
        scheduler = BatchScheduler(db, data_dir)
        scheduler.on_progress = lambda job, site, msg: print(msg)
        await scheduler.start_job(job_id)
    """

    def __init__(
        self,
        db: Database,
        data_dir: Path,
        max_concurrent_sites: int = 5,
        api_key: str = "",
        on_progress: Optional[Callable[[BatchJob, BatchSite, str], None]] = None,
        on_site_complete: Optional[Callable[[BatchSite], None]] = None,
        on_job_complete: Optional[Callable[[BatchJob], None]] = None,
    ):
        """Initialize the batch scheduler.

        Args:
            db: Database instance for persistence
            data_dir: Base directory for datasets
            max_concurrent_sites: Maximum sites to process in parallel
            api_key: RSIG API key for downloads (empty = anonymous)
            on_progress: Callback for progress updates
            on_site_complete: Callback when a site finishes
            on_job_complete: Callback when the entire job finishes
        """
        self.db = db
        self.data_dir = data_dir
        self.max_concurrent_sites = max_concurrent_sites
        self.api_key = api_key
        self.on_progress = on_progress
        self.on_site_complete = on_site_complete
        self.on_job_complete = on_job_complete

        self._running = False
        self._paused = False
        self._cancel_requested = False
        self._current_job: Optional[BatchJob] = None

    @property
    def is_running(self) -> bool:
        """Check if a job is currently running."""
        return self._running

    @property
    def current_job(self) -> Optional[BatchJob]:
        """Get the currently running job."""
        return self._current_job

    async def start_job(self, job_id: str) -> None:
        """Start or resume a batch job.

        Args:
            job_id: UUID of the batch job to start

        Raises:
            ValueError: If job not found or already running
        """
        job = self.db.get_batch_job(job_id)
        if not job:
            raise ValueError(f"Batch job {job_id} not found")

        if job.status == BatchJobStatus.RUNNING:
            raise ValueError("Job is already running")

        self._current_job = job
        self._running = True
        self._paused = False
        self._cancel_requested = False

        # Update job status
        job.status = BatchJobStatus.RUNNING
        self.db.update_batch_job(job)

        try:
            await self._process_job(job)
        except Exception as e:
            logger.error(f"Batch job failed: {e}")
            job.status = BatchJobStatus.ERROR
            job.error_message = str(e)
            self.db.update_batch_job(job)
        finally:
            self._running = False
            self._current_job = None

    async def pause_job(self) -> None:
        """Pause the currently running job."""
        self._paused = True

    async def cancel_job(self) -> None:
        """Cancel the currently running job."""
        self._cancel_requested = True

    async def _process_job(self, job: BatchJob) -> None:
        """Process all pending sites in a batch job."""
        # Reset any interrupted sites from previous runs
        reset_count = self.db.reset_interrupted_batch_sites(job.id)
        if reset_count > 0:
            logger.info(f"Reset {reset_count} interrupted sites")

        # Get pending sites
        pending_sites = self.db.get_pending_batch_sites(job.id)

        if not pending_sites:
            job.status = BatchJobStatus.COMPLETED
            self.db.update_batch_job(job)
            if self.on_job_complete:
                self.on_job_complete(job)
            return

        logger.info(f"Processing {len(pending_sites)} pending sites for job {job.name}")

        # Create semaphore for concurrency control
        semaphore = asyncio.Semaphore(job.batch_size or self.max_concurrent_sites)

        async def process_site_with_semaphore(site: BatchSite) -> None:
            async with semaphore:
                if self._cancel_requested or self._paused:
                    return
                await self._process_site(job, site)

        # Process sites in parallel batches
        tasks = [process_site_with_semaphore(site) for site in pending_sites]
        await asyncio.gather(*tasks, return_exceptions=True)

        # Update final job status
        job = self.db.get_batch_job(job.id)  # Refresh from DB

        if self._cancel_requested:
            job.status = BatchJobStatus.ERROR
            job.error_message = "Cancelled by user"
        elif self._paused:
            job.status = BatchJobStatus.PAUSED
        elif job.completed_sites + job.failed_sites >= job.total_sites:
            job.status = BatchJobStatus.COMPLETED

        job.last_processed_at = datetime.now()
        self.db.update_batch_job(job)

        if self.on_job_complete:
            self.on_job_complete(job)

    async def _process_site(self, job: BatchJob, site: BatchSite) -> None:
        """Process a single site within the batch job."""
        logger.info(f"Processing site: {site.site_name}")

        # Update status to downloading
        site.status = BatchSiteStatus.DOWNLOADING
        site.started_at = datetime.now()
        self.db.update_batch_site(site)

        if self.on_progress:
            self.on_progress(job, site, f"Starting {site.site_name}")

        try:
            # Determine settings (use per-site overrides or job defaults)
            date_start = site.custom_date_start or job.date_start
            date_end = site.custom_date_end or job.date_end
            max_cloud = site.custom_max_cloud if site.custom_max_cloud is not None else job.max_cloud
            max_sza = site.custom_max_sza if site.custom_max_sza is not None else job.max_sza

            # Create dataset for this site
            safe_name = _sanitize_filename(f"{job.name}_{site.site_name}")
            dataset = Dataset(
                id=str(uuid.uuid4()),
                name=safe_name,
                created_at=datetime.now(),
                bbox=site.bbox,
                date_start=date_start,
                date_end=date_end,
                day_filter=job.day_filter,
                hour_filter=job.hour_filter,
                max_cloud=max_cloud,
                max_sza=max_sza,
                status=DatasetStatus.DOWNLOADING,
            )
            dataset = self.db.create_dataset(dataset)
            site.dataset_id = dataset.id
            self.db.update_batch_site(site)

            # Create dataset directory
            dataset_dir = self.data_dir / "datasets" / safe_name
            dataset_dir.mkdir(parents=True, exist_ok=True)

            # Generate granule list
            granules = self._generate_granules(
                dataset, date_start, date_end, job.day_filter, job.hour_filter
            )
            self.db.create_granules_batch(granules)
            dataset.granule_count = len(granules)
            self.db.update_dataset(dataset)

            # Generate date and hour lists for downloader
            dates_list = sorted(set(g.date.isoformat() for g in granules))
            hours_list = sorted(set(g.hour for g in granules))

            if self.on_progress:
                self.on_progress(job, site, f"Downloading {site.site_name} ({len(granules)} granules)")

            # Download
            downloader = RSIGDownloader(dataset_dir, max_concurrent=4, api_key=self.api_key)
            files = await downloader.download_granules(
                dates=dates_list,
                hours=hours_list,
                bbox=site.bbox.to_list(),
                dataset_name=safe_name,
                max_cloud=max_cloud,
                max_sza=max_sza,
                status=None  # Could add status callback here
            )

            # Update to processing status
            site.status = BatchSiteStatus.PROCESSING
            self.db.update_batch_site(site)

            if self.on_progress:
                self.on_progress(job, site, f"Processing {site.site_name}")

            # Get all downloaded files
            all_files = list(dataset_dir.glob("tempo_*.nc"))

            if all_files:
                # Process the data
                ds_avg = await asyncio.to_thread(DataProcessor.process_dataset, all_files)
                if ds_avg:
                    output_path = dataset_dir / f"{safe_name}_processed.nc"
                    await asyncio.to_thread(DataProcessor.save_processed, ds_avg, output_path)
                    dataset.file_path = str(output_path)
                    dataset.file_size_mb = output_path.stat().st_size / (1024 * 1024)
                    dataset.status = DatasetStatus.COMPLETE
                else:
                    dataset.status = DatasetStatus.ERROR
            else:
                dataset.status = DatasetStatus.ERROR
                logger.warning(f"No files downloaded for site {site.site_name}")

            dataset.granules_downloaded = len(all_files)
            self.db.update_dataset(dataset)

            # Mark site complete
            site.status = BatchSiteStatus.COMPLETED
            site.completed_at = datetime.now()
            self.db.update_batch_site(site)

            # Update job counts
            job = self.db.get_batch_job(job.id)  # Refresh
            job.completed_sites += 1
            self.db.update_batch_job(job)

            if self.on_site_complete:
                self.on_site_complete(site)

            logger.info(f"Site {site.site_name} completed successfully")

        except Exception as e:
            logger.error(f"Site {site.site_name} failed: {e}")
            site.status = BatchSiteStatus.ERROR
            site.error_message = str(e)
            site.completed_at = datetime.now()
            self.db.update_batch_site(site)

            # Update job counts
            job = self.db.get_batch_job(job.id)  # Refresh
            job.failed_sites += 1
            self.db.update_batch_job(job)

    def _generate_granules(
        self,
        dataset: Dataset,
        date_start,
        date_end,
        day_filter: list[int],
        hour_filter: list[int]
    ) -> list[Granule]:
        """Generate list of granules for a dataset."""
        granules = []
        current = date_start

        while current <= date_end:
            if current.weekday() in day_filter:
                for hour in hour_filter:
                    granules.append(Granule(
                        dataset_id=dataset.id,
                        date=current,
                        hour=hour,
                        bbox_west=dataset.bbox.west,
                        bbox_south=dataset.bbox.south,
                        bbox_east=dataset.bbox.east,
                        bbox_north=dataset.bbox.north,
                        max_cloud=dataset.max_cloud,
                        max_sza=dataset.max_sza,
                    ))
            current += timedelta(days=1)

        return granules


def recover_interrupted_jobs(db: Database) -> int:
    """Recover jobs that were interrupted by app restart.

    Marks RUNNING jobs as PAUSED and resets in-progress sites.

    Args:
        db: Database instance

    Returns:
        Number of jobs recovered
    """
    recovered = 0
    jobs = db.get_all_batch_jobs()

    for job in jobs:
        if job.status == BatchJobStatus.RUNNING:
            job.status = BatchJobStatus.PAUSED
            job.error_message = "Interrupted by app restart"
            db.update_batch_job(job)

            # Reset in-progress sites
            db.reset_interrupted_batch_sites(job.id)

            recovered += 1
            logger.info(f"Recovered interrupted job: {job.name}")

    return recovered
