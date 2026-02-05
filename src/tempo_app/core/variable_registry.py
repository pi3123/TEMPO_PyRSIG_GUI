"""Variable Registry - Central system for discovering and managing TEMPO products.

This module provides dynamic discovery of all available TEMPO variables from PyRSIG,
with caching, fallback to hardcoded metadata, and graceful degradation.
"""

import json
import logging
from dataclasses import dataclass, asdict
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional
from enum import Enum

logger = logging.getLogger(__name__)

# Cache configuration
CACHE_DIR = Path("data/cache")
CACHE_FILE = CACHE_DIR / "tempo_products.json"
CACHE_TTL_HOURS = 24


class VariableCategory(Enum):
    """Categories for organizing variables in the UI."""
    TRACE_GASES = "Trace Gases"
    AEROSOLS = "Aerosols"
    CLOUDS = "Clouds"
    OTHER = "Other"


@dataclass
class TempoVariable:
    """Metadata for a single TEMPO data product."""
    product_id: str              # "tempo.l2.no2.vertical_column_troposphere" (PRIMARY KEY)
    netcdf_var: Optional[str]    # "NO2_VERTICAL_CO" (validated) or None (needs discovery)
    output_var: str              # "NO2_TropVCD" (standardized output name)
    display_name: str            # "NO₂ Tropospheric VCD"
    category: str                # "Trace Gases" | "Aerosols" | "Clouds" | "Other"
    unit: str                    # "molecules/cm²"
    description: str
    colormap: str = "RdYlBu_r"

    # Discovery metadata
    verified: bool = True        # Has netcdf_var been human-verified?
    auto_discovered: bool = False  # Was this auto-discovered?
    discovery_date: Optional[str] = None  # When was it discovered?

    def to_dict(self) -> dict:
        """Convert to dictionary for JSON serialization."""
        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict) -> "TempoVariable":
        """Create from dictionary (JSON deserialization)."""
        return cls(**data)


# Hardcoded metadata for core TEMPO Level-2 products
# This provides rich metadata even when API is unavailable
# Variables marked verified=True have been tested and validated
CORE_VARIABLES = [
    TempoVariable(
        product_id="tempo.l2.no2.vertical_column_troposphere",
        netcdf_var="NO2_VERTICAL_CO",  # ✓ Validated 2025-01-26
        output_var="NO2_TropVCD",
        display_name="NO₂ Tropospheric VCD",
        category=VariableCategory.TRACE_GASES.value,
        unit="molecules/cm²",
        description="Nitrogen Dioxide tropospheric vertical column density",
        colormap="RdYlBu_r",
        verified=True
    ),
    TempoVariable(
        product_id="tempo.l2.hcho.vertical_column",
        netcdf_var="VERTICAL_COLUMN",  # ✓ Validated 2025-01-26
        output_var="HCHO_TotVCD",
        display_name="HCHO Total VCD",
        category=VariableCategory.TRACE_GASES.value,
        unit="molecules/cm²",
        description="Formaldehyde total vertical column density",
        colormap="YlOrRd",
        verified=True
    ),
    TempoVariable(
        product_id="tempo.l2.o3tot.column_amount_o3",
        netcdf_var="O3_COLUMN_AMOUN",  # ✓ Validated 2025-01-26 (truncated to 15 chars by NetCDF)
        output_var="O3_TotVCD",
        display_name="O₃ Total Column",
        category=VariableCategory.TRACE_GASES.value,
        unit="DU",
        description="Ozone total column amount",
        colormap="PuBu",
        verified=True
    ),
    TempoVariable(
        product_id="tempo.l2.no2.vertical_column_stratosphere",
        netcdf_var="NO2_VERTICAL_CO",  # ✓ Validated 2025-01-26 (same var name as trop - different product)
        output_var="NO2_StratVCD",
        display_name="NO₂ Stratospheric VCD",
        category=VariableCategory.TRACE_GASES.value,
        unit="molecules/cm²",
        description="Nitrogen Dioxide stratospheric vertical column density",
        colormap="RdYlBu_r",
        verified=True
    ),
    TempoVariable(
        product_id="tempo.l2.no2.vertical_column_total",
        netcdf_var="NO2_VERTICAL_CO",  # ✓ Validated 2025-01-26 (same var name as trop - different product)
        output_var="NO2_TotalVCD",
        display_name="NO₂ Total VCD",
        category=VariableCategory.TRACE_GASES.value,
        unit="molecules/cm²",
        description="Nitrogen Dioxide total vertical column density",
        colormap="RdYlBu_r",
        verified=True
    ),
    TempoVariable(
        product_id="tempo.l2.cloud.cloud_fraction",
        netcdf_var="CLOUD_FRACTION",  # ✓ Validated 2025-01-26
        output_var="CloudFrac",
        display_name="Cloud Fraction",
        category=VariableCategory.CLOUDS.value,
        unit="fraction (0-1)",
        description="Cloud fraction",
        colormap="gray",
        verified=True
    ),
    TempoVariable(
        product_id="tempo.l2.cloud.cloud_pressure",
        netcdf_var="CLOUD_PRESSURE",  # ✓ Validated 2025-01-26
        output_var="CloudPres",
        display_name="Cloud Pressure",
        category=VariableCategory.CLOUDS.value,
        unit="hPa",
        description="Cloud top pressure",
        colormap="viridis",
        verified=True
    ),
]

# Create lookup map by product_id
CORE_VARIABLES_MAP = {v.product_id: v for v in CORE_VARIABLES}


class VariableRegistry:
    """Central registry for TEMPO variable discovery and metadata."""

    _cache_in_memory: Optional[list[TempoVariable]] = None
    _cache_timestamp: Optional[datetime] = None

    @classmethod
    def discover_variables(cls, force_refresh: bool = False) -> list[TempoVariable]:
        """
        Discover all available TEMPO variables with multi-level caching.

        Cache strategy:
        1. Check in-memory cache (fastest)
        2. Check disk cache if < 24 hours old
        3. Query PyRSIG API for fresh data
        4. Fallback to hardcoded core variables if API fails

        Args:
            force_refresh: If True, skip cache and query API directly

        Returns:
            List of TempoVariable objects with metadata
        """
        # Level 1: In-memory cache
        if not force_refresh and cls._cache_in_memory and cls._cache_timestamp:
            age = datetime.now() - cls._cache_timestamp
            if age < timedelta(hours=CACHE_TTL_HOURS):
                logger.info(f"Using in-memory cache ({age.seconds//60}m old)")
                return cls._cache_in_memory

        # Level 2: Disk cache
        if not force_refresh:
            cached = cls._load_from_disk()
            if cached:
                cls._cache_in_memory = cached
                cls._cache_timestamp = datetime.now()
                return cached

        # Level 3: Query PyRSIG API
        try:
            logger.info("Discovering TEMPO variables from PyRSIG...")
            variables = cls._query_pyrsig_api()
            if variables:
                # Save to cache
                cls._save_to_disk(variables)
                cls._cache_in_memory = variables
                cls._cache_timestamp = datetime.now()
                logger.info(f"Discovered {len(variables)} TEMPO variables")
                return variables
        except Exception as e:
            logger.warning(f"Failed to discover variables from API: {e}")

        # Level 4: Fallback to hardcoded core variables
        logger.info(f"Using hardcoded core variables ({len(CORE_VARIABLES)} products)")
        cls._cache_in_memory = CORE_VARIABLES
        cls._cache_timestamp = datetime.now()
        return CORE_VARIABLES

    @classmethod
    def _query_pyrsig_api(cls) -> list[TempoVariable]:
        """Query PyRSIG for available TEMPO Level-2 products."""
        try:
            from pyrsig import RsigApi
        except ImportError:
            logger.warning("PyRSIG not installed, using core variables")
            return CORE_VARIABLES

        try:
            api = RsigApi()
            # Get all available keys
            keys = api.keys(offline=True)

            if keys is None or not keys:
                keys = api.keys(offline=False)

            # Convert to list
            if hasattr(keys, 'tolist'):
                keys = keys.tolist()
            elif not isinstance(keys, list):
                keys = list(keys) if keys else []

            # Filter to TEMPO Level-2 products
            tempo_keys = [k for k in keys if str(k).startswith('tempo.l2.')]

            logger.info(f"Found {len(tempo_keys)} TEMPO Level-2 products")

            # Map to TempoVariable objects
            variables = []
            for product_id in tempo_keys:
                # Check if we have hardcoded metadata
                if product_id in CORE_VARIABLES_MAP:
                    variables.append(CORE_VARIABLES_MAP[product_id])
                else:
                    # Create basic metadata for unknown products
                    var = cls._create_basic_variable(product_id)
                    if var:
                        variables.append(var)

            return variables

        except Exception as e:
            logger.error(f"PyRSIG query failed: {e}")
            raise

    @classmethod
    def _create_basic_variable(cls, product_id: str) -> Optional[TempoVariable]:
        """Create basic TempoVariable metadata for unknown products.

        Note: netcdf_var is set to None, requiring discovery on first use.
        """
        try:
            # Parse product_id to extract info
            # Format: tempo.l2.<sensor>.<variable>
            parts = product_id.split('.')
            if len(parts) < 4:
                return None

            sensor = parts[2].upper()
            var_name = parts[3].replace('_', ' ').title()

            # Guess output variable name (sensor + short name)
            output_var = f"{sensor}_{var_name.replace(' ', '')}"

            # Categorize based on sensor name
            if sensor.lower() in ['no2', 'hcho', 'o3', 'o3tot', 'so2']:
                category = VariableCategory.TRACE_GASES.value
            elif sensor.lower() in ['cloud']:
                category = VariableCategory.CLOUDS.value
            elif sensor.lower() in ['aerosol', 'aod']:
                category = VariableCategory.AEROSOLS.value
            else:
                category = VariableCategory.OTHER.value

            return TempoVariable(
                product_id=product_id,
                netcdf_var=None,  # Will be discovered on first use
                output_var=output_var,
                display_name=f"{sensor} {var_name}",
                category=category,
                unit="",  # Unknown
                description=f"TEMPO {sensor} {var_name}",
                colormap="viridis",
                verified=False  # Not validated
            )
        except Exception as e:
            logger.warning(f"Could not create variable for {product_id}: {e}")
            return None

    @classmethod
    def _load_from_disk(cls) -> Optional[list[TempoVariable]]:
        """Load variables from disk cache."""
        try:
            if not CACHE_FILE.exists():
                return None

            # Check cache age
            file_time = datetime.fromtimestamp(CACHE_FILE.stat().st_mtime)
            age = datetime.now() - file_time
            if age > timedelta(hours=CACHE_TTL_HOURS):
                logger.info(f"Disk cache expired ({age.seconds//3600}h old)")
                return None

            # Load from file
            with open(CACHE_FILE, 'r') as f:
                data = json.load(f)

            variables = [TempoVariable.from_dict(v) for v in data['variables']]
            logger.info(f"Loaded {len(variables)} variables from disk cache ({age.seconds//60}m old)")
            return variables

        except Exception as e:
            logger.warning(f"Failed to load disk cache: {e}")
            return None

    @classmethod
    def _save_to_disk(cls, variables: list[TempoVariable]) -> None:
        """Save variables to disk cache."""
        try:
            CACHE_DIR.mkdir(parents=True, exist_ok=True)

            data = {
                'timestamp': datetime.now().isoformat(),
                'version': '1.0',
                'variables': [v.to_dict() for v in variables]
            }

            with open(CACHE_FILE, 'w') as f:
                json.dump(data, f, indent=2)

            logger.info(f"Saved {len(variables)} variables to disk cache")

        except Exception as e:
            logger.warning(f"Failed to save disk cache: {e}")

    @classmethod
    def get_default_variables(cls) -> list[str]:
        """Return legacy 3 variables for backward compatibility."""
        return [
            "tempo.l2.no2.vertical_column_troposphere",
            "tempo.l2.hcho.vertical_column",
            "tempo.l2.o3tot.column_amount_o3",
        ]

    @classmethod
    def group_by_category(cls, variables: list[TempoVariable]) -> dict[str, list[TempoVariable]]:
        """Group variables by category for UI display."""
        grouped = {}
        for var in variables:
            category = var.category
            if category not in grouped:
                grouped[category] = []
            grouped[category].append(var)

        # Sort within each category by display_name
        for category in grouped:
            grouped[category].sort(key=lambda v: v.display_name)

        return grouped

    @classmethod
    def get_variable_by_id(cls, product_id: str) -> Optional[TempoVariable]:
        """Get variable metadata by product ID."""
        variables = cls.discover_variables()
        for var in variables:
            if var.product_id == product_id:
                return var
        return None

    @classmethod
    def clear_cache(cls) -> None:
        """Clear both in-memory and disk cache."""
        cls._cache_in_memory = None
        cls._cache_timestamp = None
        if CACHE_FILE.exists():
            CACHE_FILE.unlink()
            logger.info("Cleared variable cache")
