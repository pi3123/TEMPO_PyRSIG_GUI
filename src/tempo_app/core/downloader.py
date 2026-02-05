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


def _get_variable_name(ds: xr.Dataset, var_meta, product_id: str) -> str:
    """Get variable name with strict validation and 3-tier fallback.

    Order of precedence:
    1. Registered netcdf_var (validated, instant) - FAST PATH
    2. Cached discovery (from database) - MEDIUM PATH
    3. Safe auto-discovery (with validation) - SLOW PATH
    4. ERROR if ambiguous or not found

    Args:
        ds: xarray Dataset
        var_meta: TempoVariable metadata
        product_id: TEMPO product ID

    Returns:
        Actual variable name in dataset

    Raises:
        ValueError: If variable cannot be determined safely
    """
    # FAST PATH: Use registered name
    if var_meta.netcdf_var and var_meta.netcdf_var in ds.data_vars:
        logger.debug(f"[VAR] Using registered: {var_meta.netcdf_var}")
        return var_meta.netcdf_var

    # MEDIUM PATH: Check cache
    from ..storage.database import Database
    db = Database(Path("data/tempo.db"))
    cached = db.get_cached_variable(product_id)
    if cached and cached in ds.data_vars:
        logger.info(f"[VAR] Using cached: {cached}")
        return cached

    # SLOW PATH: Safe discovery with validation
    logger.warning(f"[VAR] Attempting discovery for {product_id}")
    candidates = _discover_variable_candidates(ds, product_id)

    if len(candidates) == 0:
        raise ValueError(
            f"No data variables found for {product_id}. "
            f"Available: {list(ds.data_vars)}"
        )

    if len(candidates) == 1:
        discovered = candidates[0]
        logger.warning(f"[VAR] AUTO-DISCOVERED: {product_id} → {discovered}")
        logger.warning(f"[VAR] Action Required: Update variable_registry.py:")
        logger.warning(f"[VAR]   netcdf_var='{discovered}'")

        # Cache for future use
        db.cache_discovered_variable(product_id, discovered, verified=False)
        return discovered

    # AMBIGUOUS: Multiple candidates - require manual intervention
    logger.error(f"[VAR] AMBIGUOUS: {product_id}")
    logger.error(f"[VAR] Multiple candidates found: {candidates}")
    logger.error(f"[VAR] Action Required: Update variable_registry.py with correct variable:")
    for i, candidate in enumerate(candidates, 1):
        logger.error(f"[VAR]   Option {i}: netcdf_var='{candidate}'")

    raise ValueError(
        f"Ambiguous variable discovery for {product_id}. "
        f"Found {len(candidates)} candidates: {candidates}. "
        f"Manual update to variable_registry.py required."
    )


def _discover_variable_candidates(ds: xr.Dataset, product_id: str) -> list[str]:
    """Find ALL potential data variables (no guessing).

    Returns:
        List of candidate variable names (may be empty or have multiple)
    """
    # Exclude known metadata variables
    metadata_vars = {
        'TFLAG', 'LONGITUDE', 'LATITUDE', 'COUNT',
        'ROW', 'COL', 'LAY', 'TSTEP', 'VAR', 'DATE-TIME'
    }

    # Exclude quality/uncertainty variables (usually not the main data)
    exclude_patterns = ['FLAG', 'UNCERTAINTY', 'ERROR', 'PRECISION', 'QUALITY']

    candidates = []
    for var in ds.data_vars:
        # Skip metadata
        if var in metadata_vars:
            continue

        # Skip quality variables
        if any(pattern in var.upper() for pattern in exclude_patterns):
            continue

        candidates.append(var)

    return candidates





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
                              selected_variables: list[str] = None,
                              status: StatusManager = None) -> list[Path]:
        """
        Download TEMPO granules using daily batch requests with controlled parallelism.

        This approach uses daily batches (instead of per-hour) to reduce server load,
        while still allowing parallel processing of multiple days for speed.

        Args:
            dates: List of date strings (YYYY-MM-DD)
            hours: List of UTC hours (0-23)
            bbox: [west, south, east, north]
            dataset_name: Name of the dataset (for file naming)
            max_cloud: Maximum cloud fraction (0-1)
            max_sza: Maximum solar zenith angle (deg)
            selected_variables: List of TEMPO product IDs to download (NEW)
            status: StatusManager for UI updates

        Returns:
            List of paths to downloaded .nc files
        """
        # Default to legacy 3 variables if not specified
        if selected_variables is None:
            from .variable_registry import VariableRegistry
            selected_variables = VariableRegistry.get_default_variables()
        if RsigApi is None:
            if status: status.emit("error", "pyrsig library not installed!")
            return await self._simulate_download(dates, hours, dataset_name, status)
        
        dataset_dir = self.workdir
        dataset_dir.mkdir(parents=True, exist_ok=True)
        
        # Determine hour range for daily requests
        min_hour = min(hours)
        max_hour = max(hours)
        
        self._total = len(dates)  # Counting days
        self._completed = 0
        
        if status:
            status.emit("info", f"🚀 Daily batch download: {self._total} days × {len(hours)} hours, {self.max_concurrent} workers")
        
        logger.info(f"[BATCH] Downloading {self._total} days with {self.max_concurrent} parallel workers")
        
        
        # Use configured API key or anonymous
        api_key = self.api_key if self.api_key else "anonymous"
        
        start_time = time.time()
        
        # Create semaphore for controlled parallelism
        semaphore = asyncio.Semaphore(self.max_concurrent)
        
        async def download_day_worker(d_str: str, worker_id: int) -> list[Path]:
            """Worker that downloads one day with its own API session."""
            async with semaphore:
                import tempfile
                import shutil
                
                # Each worker gets its own temp directory and API session
                temp_dir = tempfile.mkdtemp(prefix=f"rsig_w{worker_id}_")
                
                try:
                    # Create API session for this worker
                    api = RsigApi(bbox=bbox, workdir=temp_dir, grid_kw='1US1', gridfit=True)
                    api.tempo_kw.update({
                        'minimum_quality': 'normal',
                        'maximum_cloud_fraction': max_cloud,
                        'maximum_solar_zenith_angle': max_sza,
                        'api_key': api_key
                    })
                    
                    if status:
                        async with self._lock:
                            progress = self._completed / self._total
                        status.emit("download", f"⬇️ W{worker_id}: {d_str}", progress)
                    
                    # Download entire day in one request
                    result = await self._download_daily_batch(
                        api, d_str, min_hour, max_hour, hours, dataset_dir, selected_variables, status
                    )
                    
                    async with self._lock:
                        self._completed += 1
                    
                    return result if result else []
                    
                except Exception as e:
                    logger.error(f"[BATCH] Worker {worker_id} failed for {d_str}: {e}")
                    if status:
                        status.emit("error", f"❌ W{worker_id}: {d_str} - {e}")
                    async with self._lock:
                        self._completed += 1
                    return []
                    
                finally:
                    try:
                        shutil.rmtree(temp_dir, ignore_errors=True)
                    except:
                        pass
        
        # Launch all day downloads in parallel (semaphore controls actual concurrency)
        tasks = [download_day_worker(d_str, i % self.max_concurrent + 1) for i, d_str in enumerate(dates)]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Collect results
        saved_files = []
        errors = 0
        for result in results:
            if isinstance(result, Exception):
                errors += 1
                logger.error(f"[BATCH] Task exception: {result}")
            elif result:
                saved_files.extend(result)
        
        elapsed = time.time() - start_time
        
        logger.info(f"[BATCH] Complete: {len(saved_files)} files, {errors} errors in {elapsed:.1f}s")
        if status:
            status.emit("ok", f"✅ Downloaded {len(saved_files)} granules from {self._total} days in {format_duration(elapsed)}")
        
        return saved_files
    
    async def _download_daily_batch(
        self,
        api: 'RsigApi',
        d_str: str,
        min_hour: int,
        max_hour: int,
        hours: list[int],
        dataset_dir: Path,
        selected_variables: list[str],
        status: StatusManager
    ) -> list[Path]:
        """Download an entire day's worth of data in one request.

        Uses a persistent API session to maintain connection and reduce overhead.
        Splits the returned data into per-hour files for consistent processing.
        """
        logger.info(f"[BATCH] Fetching day {d_str} (hours {min_hour:02d}-{max_hour:02d})")

        def _fetch_day():
            """Synchronous fetch for entire day range - DYNAMIC VARIABLES."""
            from .variable_registry import VariableRegistry

            d_obj = pd.to_datetime(d_str)
            bdate = d_obj + pd.to_timedelta(min_hour, unit='h')
            edate = d_obj + pd.to_timedelta(max_hour, unit='h') + pd.to_timedelta('59m')

            # Get variable metadata
            registry = VariableRegistry.discover_variables()
            var_map = {v.product_id: v for v in registry}

            datasets_dict = {}  # {product_id: xarray.Dataset}

            # Download each selected variable
            for product_id in selected_variables:
                var_meta = var_map.get(product_id)
                if not var_meta:
                    logger.warning(f"[BATCH] Unknown variable: {product_id}, skipping")
                    continue

                logger.info(f"[BATCH] Requesting {var_meta.display_name}: {bdate} to {edate}")
                if status:
                    status.emit("info", f"Requesting {var_meta.display_name}: {d_str}")

                try:
                    ds = api.to_ioapi(product_id, bdate=bdate, edate=edate)
                    datasets_dict[product_id] = ds
                    logger.info(f"[BATCH] Successfully fetched {var_meta.display_name}")
                    # Debug: Log available variables in dataset
                    logger.debug(f"[BATCH] Available variables in {product_id}: {list(ds.data_vars)}")
                except ValueError as e:
                    # Variable discovery/validation error
                    logger.error(f"[BATCH] Variable error for {product_id}: {e}")
                    if status:
                        status.emit("warning", f"⚠️ Variable error: {var_meta.display_name}")
                    continue
                except Exception as e:
                    # Other errors (API, network, etc.)
                    logger.error(f"[BATCH] Failed to fetch {product_id}: {e}")
                    if status:
                        status.emit("warning", f"⚠️ Failed: {var_meta.display_name}")

            return datasets_dict
        
        try:
            datasets_dict = await asyncio.wait_for(
                asyncio.to_thread(_fetch_day),
                timeout=DOWNLOAD_TIMEOUT * len(selected_variables)  # Scale timeout with variable count
            )
        except asyncio.TimeoutError:
            logger.error(f"[BATCH] Timeout fetching day {d_str}")
            if status:
                status.emit("error", f"⏱️ Timeout: {d_str}")
            return []
        except Exception as e:
            error_str = str(e)
            if "Unknown file format" in error_str or "NetCDF: Unknown file format" in error_str:
                logger.info(f"[BATCH] No data available for {d_str}")
                if status: status.emit("warning", f"No data for {d_str}")
                return []
            raise

        # Check if we got any data
        if not datasets_dict:
            logger.warning(f"[BATCH] No datasets fetched for {d_str}")
            if status:
                status.emit("warning", f"⚠️ No data: {d_str}")
            return []
        
        # Split into per-hour files for consistent downstream processing
        saved = []

        try:
            from .variable_registry import VariableRegistry

            # Get variable metadata
            registry = VariableRegistry.discover_variables()
            var_map = {v.product_id: v for v in registry}

            # Get timestamps from the first dataset
            first_ds = next(iter(datasets_dict.values()))
            if 'TSTEP' in first_ds.dims:
                timestamps = pd.to_datetime(first_ds.TSTEP.values)
            else:
                # Single timestep, just use requested hours
                timestamps = [pd.to_datetime(d_str) + pd.to_timedelta(h, unit='h') for h in hours]

            # Group data by hour
            for hour in hours:
                # Check if this hour has data
                hour_data_mask = [t.hour == hour for t in timestamps]
                if not any(hour_data_mask):
                    continue

                filename = f"tempo_{d_str}_{hour:02d}.nc"
                filepath = dataset_dir / filename

                # Create output dataset with attributes from first dataset
                outds = xr.Dataset(attrs=dict(first_ds.attrs))

                # Extract LAT/LON coordinates from first dataset
                if 'LATITUDE' in first_ds:
                    lat_data = first_ds['LATITUDE']
                    if 'TSTEP' in lat_data.dims:
                        lat_data = lat_data.isel(TSTEP=0)
                    if 'LAY' in lat_data.dims:
                        lat_data = lat_data.isel(LAY=0)
                    outds.coords['LAT'] = (('ROW', 'COL'), lat_data.values.copy())

                if 'LONGITUDE' in first_ds:
                    lon_data = first_ds['LONGITUDE']
                    if 'TSTEP' in lon_data.dims:
                        lon_data = lon_data.isel(TSTEP=0)
                    if 'LAY' in lon_data.dims:
                        lon_data = lon_data.isel(LAY=0)
                    outds.coords['LON'] = (('ROW', 'COL'), lon_data.values.copy())

                # Extract each selected variable dynamically
                all_nan = True

                for product_id, ds in datasets_dict.items():
                    var_meta = var_map.get(product_id)
                    if not var_meta:
                        logger.warning(f"[BATCH] No metadata for {product_id}, skipping")
                        continue

                    # Extract data for this hour
                    if 'TSTEP' in ds.dims:
                        hour_indices = [i for i, m in enumerate(hour_data_mask) if m]
                        if not hour_indices:
                            continue
                        ds_hour = ds.isel(TSTEP=hour_indices)
                    else:
                        ds_hour = ds

                    # Get validated variable name (uses 3-tier discovery)
                    try:
                        var_name = _get_variable_name(ds_hour, var_meta, product_id)
                        var_data = ds_hour[var_name]
                        logger.debug(f"[BATCH] Extracting {var_name} -> {var_meta.output_var} for hour {hour}")
                    except ValueError as e:
                        # Discovery failed or ambiguous
                        logger.error(f"[BATCH] Failed to extract {product_id}: {e}")
                        if status:
                            status.emit("warning", f"⚠️ Variable error: {var_meta.display_name}")
                        continue

                    # Remove extra dimensions (LAY, TSTEP)
                    if 'LAY' in var_data.dims:
                        var_data = var_data.isel(LAY=0)
                    if 'TSTEP' in var_data.dims:
                        var_data = var_data.mean(dim='TSTEP')

                    # Mask fill values (typically -9.999e36) as NaN
                    var_data = var_data.where(var_data > -1e30, np.nan)

                    # Store with standardized output name
                    outds[var_meta.output_var] = xr.DataArray(
                        var_data.values.copy(),
                        dims=var_data.dims,
                        attrs=dict(var_data.attrs) if hasattr(var_data, 'attrs') else {}
                    )

                    # Track if we have any valid data
                    if not var_data.isnull().all():
                        all_nan = False
                    else:
                        logger.debug(f"[BATCH] {var_meta.output_var} is all NaN for hour {hour}")

                # Skip file if all variables are NaN
                if all_nan:
                    logger.info(f"[BATCH] Skipping {filename} - all variables are NaN")
                    continue

                # Save file
                await asyncio.to_thread(lambda: outds.to_netcdf(filepath, engine='netcdf4', compute=True))
                outds.close()

                fsize = filepath.stat().st_size
                if fsize > 1000:
                    saved.append(filepath)
                    logger.info(f"[BATCH] Saved: {filename} ({fsize/1024:.1f} KB)")
                    if status:
                         status.emit("download", f"✅ Saved: {filename}", None)
                else:
                    filepath.unlink()

        finally:
            # Close all opened datasets
            for ds in datasets_dict.values():
                try:
                    ds.close()
                except Exception:
                    pass

        return saved
    
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
        """Legacy method - kept for backwards compatibility but not used."""
        # Now handled by _download_daily_batch
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
            if status: status.emit("info", f"🗑️ Overwriting {filename}")
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
