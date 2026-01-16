"""RSIG Downloader module for TEMPO data with parallel downloading."""

import asyncio
import pandas as pd
import xarray as xr
import numpy as np
from pathlib import Path
from datetime import datetime
import json
import logging
import time
import gc
import traceback

try:
    from pyrsig import RsigApi
except ImportError:
    RsigApi = None

from .status import StatusManager
from .constants import DEFAULT_BBOX

logger = logging.getLogger(__name__)

# Configure parallel download settings
MAX_CONCURRENT_DOWNLOADS = 4  # Number of parallel downloads
DOWNLOAD_TIMEOUT = 180.0      # Timeout per granule in seconds


def format_duration(seconds: float) -> str:
    """Format duration in human-readable format.
    
    Returns:
        < 60s: "45s"
        1m - 60m: "2m 30s"
        > 1h: "1h 15m"
    """
    if seconds < 0:
        return "calculating..."
    
    seconds = int(seconds)
    
    if seconds < 60:
        return f"{seconds}s"
    elif seconds < 3600:
        m, s = divmod(seconds, 60)
        return f"{m}m {s}s" if s else f"{m}m"
    else:
        h, remainder = divmod(seconds, 3600)
        m = remainder // 60
        return f"{h}h {m}m" if m else f"{h}h"





class RSIGDownloader:
    """Handles downloading TEMPO data from EPA RSIG API with parallel execution."""
    
    def __init__(self, workdir: Path, max_concurrent: int = MAX_CONCURRENT_DOWNLOADS, api_key: str = ""):
        self.workdir = workdir
        self.workdir.mkdir(parents=True, exist_ok=True)
        self.max_concurrent = max_concurrent
        self.api_key = api_key  # Configured API key (empty = anonymous)
        
        # Shared state for progress tracking
        self._completed = 0
        self._total = 0
        self._lock = asyncio.Lock()
        
    async def download_granules(self, 
                              dates: list[str], 
                              hours: list[int], 
                              bbox: list[float], 
                              dataset_name: str,
                              max_cloud: float = 0.5, 
                              max_sza: float = 70.0,
                              status: StatusManager = None) -> list[Path]:
        """
        Download TEMPO granules for the specified criteria using parallel execution.
        
        Args:
            dates: List of date strings (YYYY-MM-DD)
            hours: List of UTC hours (0-23)
            bbox: [west, south, east, north]
            dataset_name: Name of the dataset (for file naming)
            max_cloud: Maximum cloud fraction (0-1)
            max_sza: Maximum solar zenith angle (deg)
            status: StatusManager for UI updates
        
        Returns:
            List of paths to downloaded .nc files
        """
        if RsigApi is None:
            if status: status.emit("error", "pyrsig library not installed!")
            return await self._simulate_download(dates, hours, dataset_name, status)
        
        dataset_dir = self.workdir
        dataset_dir.mkdir(parents=True, exist_ok=True)
        
        # Build list of all (date, hour) combinations to download
        download_tasks = []
        for d_str in dates:
            for h in hours:
                download_tasks.append((d_str, h))
        
        self._total = len(download_tasks)
        self._completed = 0
        
        if status:
            status.emit("info", f"🚀 Starting parallel download: {self._total} granules with {self.max_concurrent} workers")
        
        logger.info(f"[PARALLEL] Starting {self._total} downloads with {self.max_concurrent} concurrent workers")
        
        # Create semaphore to limit concurrent downloads
        semaphore = asyncio.Semaphore(self.max_concurrent)
        
        # Create download worker coroutine
        async def download_worker(d_str: str, hour: int) -> Path | None:
            async with semaphore:
                return await self._download_single_granule(
                    d_str, hour, bbox, max_cloud, max_sza, dataset_dir, status
                )
        
        # Launch all downloads concurrently (semaphore limits actual parallelism)
        start_time = time.time()
        tasks = [download_worker(d_str, h) for d_str, h in download_tasks]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        elapsed = time.time() - start_time
        
        # Collect successful downloads
        saved_files = []
        errors = 0
        for result in results:
            if isinstance(result, Exception):
                errors += 1
                logger.error(f"[PARALLEL] Task exception: {result}")
            elif result is not None:
                saved_files.append(result)
            else:
                # None means no data for that hour (not an error)
                pass
        
        logger.info(f"[PARALLEL] Complete: {len(saved_files)} saved, {errors} errors in {elapsed:.1f}s")
        if status:
            status.emit("ok", f"✅ Downloaded {len(saved_files)}/{self._total} granules in {format_duration(elapsed)}")
        
        return saved_files
    
    async def _download_single_granule(
        self, 
        d_str: str, 
        hour: int, 
        bbox: list[float],
        max_cloud: float,
        max_sza: float,
        dataset_dir: Path,
        status: StatusManager
    ) -> Path | None:
        """Download a single granule."""
        
        # Use configured API key if available, otherwise use "anonymous"
        api_key = self.api_key if self.api_key else "anonymous"
        
        filename = f"tempo_{d_str}_{hour:02d}.nc"
        filepath = dataset_dir / filename
        
        logger.info(f"[WORKER] Starting: {d_str} {hour:02d}:00 (key={api_key})")
        
        if status:
            async with self._lock:
                progress = self._completed / self._total if self._total > 0 else 0
            status.emit("download", f"⬇️ {d_str} @ {hour:02d}:00 UTC", progress)
        
        def _fetch_hour():
            """Synchronous fetch function to run in thread."""
            import tempfile
            import shutil
            
            temp_dir = tempfile.mkdtemp(prefix="rsig_")
            
            try:
                # Setup API with configured or anonymous key
                api = RsigApi(bbox=bbox, workdir=temp_dir, grid_kw='1US1', gridfit=True)
                api.tempo_kw.update({
                    'minimum_quality': 'normal',
                    'maximum_cloud_fraction': max_cloud,
                    'maximum_solar_zenith_angle': max_sza,
                    'api_key': api_key
                })
                
                d_obj = pd.to_datetime(d_str)
                bdate = d_obj + pd.to_timedelta(hour, unit='h')
                edate = bdate + pd.to_timedelta('59m')
                
                logger.info(f"[WORKER] Fetching NO2 for {bdate}")
                no2ds = api.to_ioapi('tempo.l2.no2.vertical_column_troposphere', bdate=bdate, edate=edate)
                
                logger.info(f"[WORKER] Fetching HCHO for {bdate}")
                hchods = api.to_ioapi('tempo.l2.hcho.vertical_column', bdate=bdate, edate=edate)
                
                # Merge into output dataset
                outds = xr.Dataset(attrs=dict(no2ds.attrs))
                
                if 'LATITUDE' in no2ds:
                    outds.coords['LAT'] = (('ROW', 'COL'), no2ds['LATITUDE'].isel(TSTEP=0, LAY=0).values.copy())
                if 'LONGITUDE' in no2ds:
                    outds.coords['LON'] = (('ROW', 'COL'), no2ds['LONGITUDE'].isel(TSTEP=0, LAY=0).values.copy())
                
                n_var = no2ds.get('NO2_VERTICAL_CO', xr.DataArray(np.nan, coords=no2ds.coords, dims=no2ds.dims))
                h_var = hchods.get('VERTICAL_COLUMN', xr.DataArray(np.nan, coords=hchods.coords, dims=hchods.dims))
                
                if 'LAY' in n_var.dims: n_var = n_var.isel(LAY=0)
                if 'LAY' in h_var.dims: h_var = h_var.isel(LAY=0)
                
                outds['NO2_TropVCD'] = xr.DataArray(n_var.values.copy(), dims=n_var.dims, attrs=dict(n_var.attrs))
                outds['HCHO_TotVCD'] = xr.DataArray(h_var.values.copy(), dims=h_var.dims, attrs=dict(h_var.attrs))
                
                # Check validity
                if outds['NO2_TropVCD'].isnull().all() and outds['HCHO_TotVCD'].isnull().all():
                    logger.info(f"[WORKER] No valid data for {bdate}")
                    no2ds.close()
                    hchods.close()
                    return None
                
                no2ds.close()
                hchods.close()
                
                return outds
                
            except Exception as e:
                # Check for "Unknown file format" or empty/corrupt file which means no data
                error_str = str(e)
                if "Unknown file format" in error_str or "NetCDF: Unknown file format" in error_str:
                    logger.info(f"[WORKER] No data available for {d_str} {hour:02d}:00 (server returned empty/invalid file)")
                    return None  # Return None to indicate no data (not an error)
                    
                logger.error(f"[WORKER] Fetch failed for {d_str} {hour:02d}:00: {e}")
                logger.error(f"[WORKER] Traceback:\n{traceback.format_exc()}")
                return e  # Return exception for handling
            finally:
                try:
                    shutil.rmtree(temp_dir, ignore_errors=True)
                except:
                    pass
        
        # Run fetch in thread with timeout
        try:
            ds = await asyncio.wait_for(
                asyncio.to_thread(_fetch_hour),
                timeout=DOWNLOAD_TIMEOUT
            )
            
            # Check if fetch returned an exception
            if isinstance(ds, Exception):
                if status:
                    status.emit("error", f"❌ Fetch failed: {d_str} {hour:02d}:00 - {ds}")
                async with self._lock:
                    self._completed += 1
                return None
            
            # Update progress
            async with self._lock:
                self._completed += 1
                progress = self._completed / self._total
            
            if ds is None:
                if status:
                    status.emit("warning", f"⚠️ No data: {d_str} {hour:02d}:00")
                return None
            
            # Save to disk
            return await self._save_granule(ds, filepath, filename, progress, status)
            
        except asyncio.TimeoutError:
            logger.error(f"[WORKER] Timeout: {d_str} {hour:02d}:00")
            if status:
                status.emit("error", f"⏱️ Timeout: {d_str} {hour:02d}:00")
            async with self._lock:
                self._completed += 1
            return None
        except Exception as e:
            logger.error(f"[WORKER] Exception: {d_str} {hour:02d}:00 - {e}")
            logger.error(f"[WORKER] Traceback:\n{traceback.format_exc()}")
            if status:
                status.emit("error", f"❌ Error: {d_str} {hour:02d}:00 - {e}")
            async with self._lock:
                self._completed += 1
            return None
    
    async def _save_granule(
        self, 
        ds: xr.Dataset, 
        filepath: Path, 
        filename: str, 
        progress: float,
        status: StatusManager
    ) -> Path | None:
        """Save a downloaded dataset to disk with robust error handling."""
        
        logger.info(f"[SAVE] Saving: {filepath}")
        
        # Delete existing file if present
        if filepath.exists():
            logger.info(f"[SAVE] Deleting existing file: {filepath}")
            gc.collect()
            try:
                filepath.unlink()
            except PermissionError:
                await asyncio.sleep(0.5)
                gc.collect()
                try:
                    filepath.unlink()
                except PermissionError as e:
                    logger.error(f"[SAVE] Cannot delete locked file: {e}")
                    if status:
                        status.emit("error", f"🔒 File locked: {filename}")
                    ds.close()
                    return None
        
        # Save to netCDF
        try:
            await asyncio.to_thread(
                lambda: ds.to_netcdf(filepath, engine='netcdf4', compute=True)
            )
            ds.close()
        except PermissionError as e:
            logger.error(f"[SAVE] Permission denied: {e}")
            if status:
                status.emit("error", f"🔒 Permission denied: {filename}")
            ds.close()
            return None
        except Exception as e:
            logger.error(f"[SAVE] Save error: {e}")
            logger.error(f"[SAVE] Traceback:\n{traceback.format_exc()}")
            if status:
                status.emit("error", f"❌ Save failed: {filename} - {e}")
            ds.close()
            return None
        
        # Validate saved file
        if not filepath.exists():
            logger.error(f"[SAVE] File missing after save!")
            if status:
                status.emit("error", f"❌ File disappeared: {filename}")
            return None
        
        fsize = filepath.stat().st_size
        if fsize < 1000:
            logger.warning(f"[SAVE] File too small: {fsize} bytes")
            if status:
                status.emit("error", f"⚠️ File too small: {filename}")
            filepath.unlink()
            return None
        
        logger.info(f"[SAVE] Success: {filename} ({fsize/1024:.1f} KB)")
        if status:
            status.emit("download", f"✅ {filename} ({fsize/1024:.1f} KB)", progress)
        
        return filepath
    
    async def _simulate_download(self, dates, hours, dataset_name, status):
        """Fallback simulation for testing without credentials/deps."""
        dataset_dir = self.workdir
        dataset_dir.mkdir(parents=True, exist_ok=True)
        saved = []
        total = len(dates) * len(hours)
        curr = 0
        
        for d in dates:
            for h in hours:
                curr += 1
                if status: status.emit("download", f"Simulating: {d} @ {h:02d}:00", curr/total)
                await asyncio.sleep(0.3)  # Faster simulation
                # Create dummy file
                p = dataset_dir / f"tempo_{d}_{h:02d}.nc"
                with open(p, 'w') as f: f.write("dummy netcdf")
                saved.append(p)
                
        return saved
