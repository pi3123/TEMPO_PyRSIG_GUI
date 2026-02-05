"""Map plotter module for TEMPO Analyzer."""

import matplotlib
matplotlib.use('Agg')  # Non-interactive backend
import matplotlib.pyplot as plt
import matplotlib.colors as mcolors
from matplotlib.colors import LinearSegmentedColormap, Normalize
from matplotlib.lines import Line2D

import numpy as np
import xarray as xr
from pathlib import Path
import logging
import sys
from typing import Optional

try:
    import cartopy.crs as ccrs
    import cartopy.feature as cfeature
    import cartopy.io.shapereader as shapereader
    from pyproj import Proj
except ImportError:
    pass

from .constants import DEFAULT_BBOX, SITES

logger = logging.getLogger(__name__)

class MapPlotter:
    def __init__(self, cache_dir: Path):
        self.cache_dir = cache_dir / "plots"
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        self.geometry_cache = {}
        self._temp_file_counter = 0

    def generate_map(self,
                    dataset: xr.Dataset,
                    hour: int,
                    variable: str,
                    dataset_name: str,
                    bbox: list[float] = None,
                    road_detail: str = 'primary',
                    sites: dict[str, tuple[float, float]] = None,
                    font_size: int = 10,
                    title_size: int = 14,
                    colormap: str = None,
                    border_width: float = 1.5,
                    road_scale: float = 1.0,
                    vmin: float = None,
                    vmax: float = None) -> tuple[Optional[str], list[str]]:
        """
        Generate a map plot for a specific hour and variable.

        Args:
            dataset: xarray Dataset with 'NO2_TropVCD', 'HCHO_TotVCD', 'O3_TotVCD', or 'FNR'
            hour: UTC hour
            variable: 'NO2', 'HCHO', 'O3', or 'FNR'
            dataset_name: Name of dataset (for display)
            bbox: [west, south, east, north]
            road_detail: 'primary', 'major', or 'all'
            sites: Dict mapping site code to (lat, lon). If None, uses default SITES.
                   If empty dict {}, no markers shown.
            font_size: Base font size for labels (default 10)
            title_size: Title font size (default 14)
            colormap: Matplotlib colormap name (optional, overrides default)
            border_width: Width of political borders (default 1.5)
            road_scale: Scale factor for road widths (default 1.0)
            vmin: Minimum value for color scale
            vmax: Maximum value for color scale

        Returns:
            Tuple of (path to PNG file or None, list of warning/error messages)
        """
        messages = []  # Collect warnings and errors
        import tempfile
        import time

        try:
            logger.info(f"Generating map for {variable} @ H{hour}")
            if 'cartopy.crs' not in sys.modules:
                msg = "⚠️ Cartopy not loaded, using fallback."
                logger.warning(msg)
                messages.append(msg)
                return self._generate_dummy_map(variable, hour), messages

            # Extract date range and available hours info for title
            import pandas as pd
            date_range_str = ""
            
            # Extract data for the specific hour
            # Handle processed data with TIME (new), TSTEP (old datetime), or HOUR (aggregated) dims
            if 'TIME' in dataset.dims:
                timestamps = pd.to_datetime(dataset.TIME.values)
                date_range_str = f"{timestamps.min().strftime('%Y-%m-%d')} to {timestamps.max().strftime('%Y-%m-%d')}"
                available_hours = sorted(set(timestamps.hour.tolist()))
                
                # New format: TIME is datetime, need to extract by hour and average
                if hour not in dataset.TIME.dt.hour.values:
                    msg = f"❌ Hour {hour} not found in dataset TIME"
                    logger.error(msg)
                    messages.append(msg)
                    return None, messages
                ds_hour = dataset.sel(TIME=dataset.TIME.dt.hour == hour).mean(dim='TIME')
            elif 'TSTEP' in dataset.dims:
                timestamps = pd.to_datetime(dataset.TSTEP.values)
                date_range_str = f"{timestamps.min().strftime('%Y-%m-%d')} to {timestamps.max().strftime('%Y-%m-%d')}"
                available_hours = sorted(set(timestamps.hour.tolist()))
                
                # Old format: TSTEP is datetime, need to extract by hour
                if hour not in dataset.TSTEP.dt.hour.values:
                    msg = f"❌ Hour {hour} not found in dataset TSTEP"
                    logger.error(msg)
                    messages.append(msg)
                    return None, messages
                ds_hour = dataset.sel(TSTEP=dataset.TSTEP.dt.hour == hour).mean(dim='TSTEP')
            elif 'HOUR' in dataset.dims:
                available_hours = sorted(dataset.HOUR.values.tolist())
                
                # Aggregated format: dimension is integer hours
                if hour not in dataset.HOUR.values:
                    msg = f"❌ Hour {hour} not found in dataset HOURs"
                    logger.error(msg)
                    messages.append(msg)
                    return None, messages
                ds_hour = dataset.sel(HOUR=hour)
            elif 'hour' in dataset.dims:
                available_hours = sorted(dataset.hour.values.tolist())
                
                # Legacy format: dimension is integer hours
                if hour not in dataset.hour.values:
                    msg = f"❌ Hour {hour} not found in dataset hours"
                    logger.error(msg)
                    messages.append(msg)
                    return None, messages
                ds_hour = dataset.sel(hour=hour)
            else:
                msg = f"❌ Dataset has no recognized time dimension. Dims: {list(dataset.dims)}"
                logger.error(msg)
                messages.append(msg)
                return None, messages
            logger.debug(f"Extracted hour {hour} slice.")
            
            # Dynamic variable handling - check if variable exists in dataset
            if variable not in ds_hour:
                msg = f"❌ Variable '{variable}' not found in dataset. Available: {list(ds_hour.data_vars.keys())}"
                logger.error(msg)
                messages.append(msg)
                return None, messages

            # Extract data
            data = ds_hour[variable]

            # Mask fill values (typically -9.999e36)
            data = data.where(data > -1e30)

            # Get variable metadata from registry for better labels and colormaps
            try:
                from .variable_registry import VariableRegistry
                var_meta = None

                # Find matching variable in registry by output_var name
                for v in VariableRegistry.discover_variables():
                    if v.output_var == variable:
                        var_meta = v
                        break

                if var_meta:
                    # Use metadata from registry
                    label = f"{var_meta.display_name}"
                    if var_meta.unit:
                        label += f" ({var_meta.unit})"
                    default_cmap = var_meta.colormap
                else:
                    # Fallback for variables not in registry (like FNR)
                    label = variable
                    default_cmap = 'viridis'

            except Exception as e:
                logger.warning(f"Could not get metadata from registry: {e}")
                label = variable
                default_cmap = 'viridis'

            # Special handling for FNR variable - apply defaults independent of colormap
            if variable == 'FNR':
                label = 'FNR (HCHO/NO2)'
                # Filter positive values only
                data = data.where(data > 0)
                # Default FNR range (only if not explicitly set by user)
                if vmin is None:
                    vmin = 2
                if vmax is None:
                    vmax = 8

            # Apply colormap override if specified, otherwise use default
            if colormap:
                cmap = colormap
            else:
                # Use variable-specific default colormap
                if variable == 'FNR':
                    # Blue-Grey-Red colormap for FNR
                    colors = [(0.3, 0.5, 1), 'silver', (1, 0.4, 0.4)]
                    cmap = LinearSegmentedColormap.from_list('bgr', colors, N=256)
                else:
                    cmap = default_cmap

            # Setup normalization
            norm = Normalize(vmin=vmin, vmax=vmax) if (vmin is not None or vmax is not None) else None
                
            data = data.squeeze(drop=True)
            if data.isnull().all():
                msg = f"⚠️ Data for {variable} is all NaN (empty) for hour {hour}."
                logger.warning(msg)
                messages.append(msg)
                return None, messages

            # Setup plot
            fig = plt.figure(figsize=(9, 8))
            ax = fig.add_subplot(1, 1, 1, projection=ccrs.Mercator())
            
            # Coordinates
            if 'LAT' in ds_hour.coords:
                lats = ds_hour['LAT'].values
                lons = ds_hour['LON'].values
            else:
                # Fallback if no lat/lon arrays (projected)
                msg = "❌ No LAT/LON coordinates found in dataset"
                logger.error(msg)
                messages.append(msg)
                return None, messages

            if bbox is None:
                bbox = DEFAULT_BBOX
                
            ax.set_extent([bbox[0], bbox[2], bbox[1], bbox[3]], crs=ccrs.PlateCarree())
            ax.add_feature(cfeature.LAND, facecolor='white')
            ax.add_feature(cfeature.LAKES, facecolor='none', edgecolor='black')
            # Borders: Solid lines, thicker than roads (roads are max 1.0)
            ax.add_feature(cfeature.BORDERS, linestyle='-', linewidth=border_width, alpha=0.8, edgecolor='black')
            ax.add_feature(cfeature.STATES, linestyle='-', linewidth=border_width * 0.8, alpha=0.6, edgecolor='gray')
            
            # Add road overlays based on road_detail level
            try:
                self._add_road_overlay(ax, bbox, road_detail, road_scale)
            except Exception as road_err:
                msg = f"⚠️ Road overlay failed (non-fatal): {road_err}"
                logger.warning(msg)
                messages.append(msg)
            
            # Plot Data
            mesh = ax.pcolormesh(lons, lats, data, cmap=cmap, norm=norm, 
                               transform=ccrs.PlateCarree(), shading='auto')
                               
            # Plot Sites - use provided sites or fall back to default SITES
            sites_to_plot = SITES if sites is None else sites
            site_idx = 0
            # distinct colors
            marker_colors = ['#e6194b', '#3cb44b', '#ffe119', '#4363d8', '#f58231', '#911eb4', '#46f0f0', '#f032e6']
            
            legend_elements = []
            for name, (lat, lon) in sites_to_plot.items():
                if bbox[0] <= lon <= bbox[2] and bbox[1] <= lat <= bbox[3]:
                    c = marker_colors[site_idx % len(marker_colors)]
                    ax.plot(lon, lat, marker='*', markersize=14, markerfacecolor=c, 
                          markeredgecolor='black', transform=ccrs.PlateCarree())
                    legend_elements.append(Line2D([0], [0], marker='*', color='w', label=name,
                                                markerfacecolor=c, markeredgecolor='black', markersize=10))
                    site_idx += 1
            
            if legend_elements:
                ax.legend(handles=legend_elements, loc='upper right', title="Sites", 
                        fancybox=True, shadow=True, fontsize=font_size)
            
            # Decoration
            cbar = plt.colorbar(mesh, ax=ax, shrink=0.7, label=label)
            cbar.ax.tick_params(labelsize=font_size)
            cbar.set_label(label, size=font_size)
            ax.gridlines(draw_labels=True, linewidth=1, color='gray', alpha=0.3, linestyle='--')
            # Build title with extra info
            title_parts = [f"{variable} - {dataset_name}"]
            if date_range_str:
                title_parts.append(date_range_str)
            
            # Add current hour line
            title_parts.append(f"{hour:02d}:00 UTC")
            
            ax.set_title("\n".join(title_parts), fontsize=title_size)
            
            # Save to temp file with descriptive name + unique timestamp
            import time
            safe_dataset_name = "".join(c if c.isalnum() or c in "._-" else "_" for c in dataset_name)
            timestamp = int(time.time() * 1000)
            temp_filename = f"{safe_dataset_name}_{variable}_H{hour:02d}_{road_detail}_{timestamp}.png"
            temp_path = self.cache_dir / temp_filename
            plt.savefig(temp_path, dpi=100, bbox_inches='tight')
            plt.close(fig)

            logger.info(f"Map saved to {temp_path}")
            return str(temp_path), messages

        except Exception as e:
            msg = f"❌ Plotting failed: {e}"
            logger.error(msg)
            messages.append(msg)
            import traceback
            traceback.print_exc()
            plt.close('all')
            return None, messages

    def _generate_dummy_map(self, variable, hour):
        """Generate a placeholder image if Cartopy is missing."""
        import time
        fig, ax = plt.subplots(figsize=(5, 4))
        ax.text(0.5, 0.5, f"{variable} Map\nHour {hour}\n(Cartopy Missing)",
              ha='center', va='center')
        ax.axis('off')

        self._temp_file_counter += 1
        temp_filename = f"dummy_{int(time.time() * 1000)}_{self._temp_file_counter}.png"
        temp_path = self.cache_dir / temp_filename
        plt.savefig(temp_path)
        plt.close(fig)

        return str(temp_path), []

    def _add_road_overlay(self, ax, bbox: list[float], road_detail: str = 'primary', road_scale: float = 1.0):
        """
        Add road overlay to the map axis.
        
        Args:
            ax: Matplotlib/Cartopy axis
            bbox: [west, south, east, north]
            road_detail: 'primary' (interstates), 'major' (+highways), 'all' (+secondary)
            road_scale: Multiplier for road linewidths
        """
        # Use Natural Earth roads - automatically downloaded and cached by Cartopy
        # 10m resolution has the most detail for road features
        
        if road_detail == 'none':
            return
            
        # Interstate highways (always shown unless 'none')
        try:
            roads_10m = shapereader.natural_earth(
                resolution='10m',
                category='cultural',
                name='roads'
            )
            
            reader = shapereader.Reader(roads_10m)
            
            for record in reader.records():
                road_type = record.attributes.get('type', '')
                
                # Filter by road type based on detail level
                if road_detail == 'primary':
                    # Only major highways/interstates
                    if road_type not in ['Major Highway', 'Beltway', 'Interstate']:
                        continue
                    linewidth = 1.0 * road_scale
                    color = '#444444'
                    alpha = 0.7
                elif road_detail == 'major':
                    # Major highways + secondary
                    if road_type not in ['Major Highway', 'Beltway', 'Interstate', 'Secondary Highway']:
                        continue
                    linewidth = (0.8 if road_type in ['Major Highway', 'Beltway', 'Interstate'] else 0.5) * road_scale
                    color = '#444444' if road_type in ['Major Highway', 'Beltway', 'Interstate'] else '#666666'
                    alpha = 0.7 if road_type in ['Major Highway', 'Beltway', 'Interstate'] else 0.5
                else:  # 'all'
                    # Include all roads
                    if road_type in ['Major Highway', 'Beltway', 'Interstate']:
                        linewidth = 0.8 * road_scale
                        color = '#333333'
                        alpha = 0.8
                    elif road_type == 'Secondary Highway':
                        linewidth = 0.5 * road_scale
                        color = '#555555'
                        alpha = 0.6
                    else:
                        linewidth = 0.3 * road_scale
                        color = '#777777'
                        alpha = 0.4
                
                # Check if geometry intersects bbox (approximate)
                geom = record.geometry
                bounds = geom.bounds  # (minx, miny, maxx, maxy)
                if (bounds[2] < bbox[0] or bounds[0] > bbox[2] or 
                    bounds[3] < bbox[1] or bounds[1] > bbox[3]):
                    continue
                
                ax.add_geometries(
                    [geom],
                    ccrs.PlateCarree(),
                    facecolor='none',
                    edgecolor=color,
                    linewidth=linewidth,
                    alpha=alpha
                )
                
            print(f"DEBUG: Added road overlay (detail={road_detail})")
            
        except Exception as e:
            print(f"DEBUG: Natural Earth roads failed: {e}")
            # Fallback: try US states borders as visual reference
            try:
                ax.add_feature(cfeature.STATES, linewidth=0.5, edgecolor='gray')
            except Exception:
                pass

