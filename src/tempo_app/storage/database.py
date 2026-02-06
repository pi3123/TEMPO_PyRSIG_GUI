"""SQLite database operations for TEMPO Analyzer.

Handles all CRUD operations for datasets, granules, and exports.
"""

import sqlite3
import json
import logging
from datetime import datetime, date
from pathlib import Path
from typing import Optional
from contextlib import contextmanager
import uuid

from .models import (
    Dataset, Granule, ExportRecord, Site, DatasetStatus, BoundingBox, SITES,
    BatchJob, BatchSite, BatchJobStatus, BatchSiteStatus, Analysis
)

logger = logging.getLogger(__name__)


def _parse_date(value) -> date:
    """Parse a date from database, handling both date and datetime strings."""
    if isinstance(value, date):
        return value
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, str):
        # Handle datetime strings like "2024-06-01 00:00:00"
        if ' ' in value:
            value = value.split(' ')[0]
        return date.fromisoformat(value)
    if isinstance(value, bytes):
        # Handle bytes from SQLite
        value_str = value.decode('utf-8')
        if ' ' in value_str:
            value_str = value_str.split(' ')[0]
        return date.fromisoformat(value_str)
    raise ValueError(f"Cannot parse date from {type(value)}: {value}")


def _robust_date_converter(val: bytes) -> date:
    """SQLite converter for DATE that handles malformed datetime strings."""
    val_str = val.decode('utf-8') if isinstance(val, bytes) else val
    # Handle datetime strings like "2024-06-01 00:00:00"
    if ' ' in val_str:
        val_str = val_str.split(' ')[0]
    return date.fromisoformat(val_str)


# Register our robust converter to override SQLite's built-in date converter
sqlite3.register_converter("DATE", _robust_date_converter)

class Database:
    """SQLite database manager for TEMPO Analyzer."""
    
    def __init__(self, db_path: Path):
        """Initialize database connection.
        
        Args:
            db_path: Path to SQLite database file
        """
        self.db_path = db_path
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._init_schema()
    
    @contextmanager
    def _get_connection(self):
        """Context manager for database connections."""
        conn = sqlite3.connect(self.db_path, detect_types=sqlite3.PARSE_DECLTYPES)
        conn.execute("PRAGMA foreign_keys = ON")
        conn.row_factory = sqlite3.Row
        try:
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()
    
    def _init_schema(self):
        """Create database tables if they don't exist."""
        with self._get_connection() as conn:
            conn.executescript("""
                -- Datasets table
                CREATE TABLE IF NOT EXISTS datasets (
                    id TEXT PRIMARY KEY,
                    name TEXT NOT NULL,
                    batch_job_id TEXT,
                    created_at TIMESTAMP NOT NULL,
                    bbox_west REAL NOT NULL,
                    bbox_south REAL NOT NULL,
                    bbox_east REAL NOT NULL,
                    bbox_north REAL NOT NULL,
                    date_start DATE NOT NULL,
                    date_end DATE NOT NULL,
                    day_filter TEXT NOT NULL,
                    hour_filter TEXT NOT NULL,
                    max_cloud REAL NOT NULL,
                    max_sza REAL NOT NULL,
                    selected_variables TEXT,
                    status TEXT NOT NULL DEFAULT 'pending',
                    file_path TEXT,
                    file_hash TEXT,
                    file_size_mb REAL DEFAULT 0,
                    last_accessed TIMESTAMP,
                    granule_count INTEGER DEFAULT 0,
                    granules_downloaded INTEGER DEFAULT 0
                );
                
                -- Granules table
                CREATE TABLE IF NOT EXISTS granules (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    dataset_id TEXT NOT NULL REFERENCES datasets(id) ON DELETE CASCADE,
                    date DATE NOT NULL,
                    hour INTEGER NOT NULL,
                    bbox_west REAL NOT NULL,
                    bbox_south REAL NOT NULL,
                    bbox_east REAL NOT NULL,
                    bbox_north REAL NOT NULL,
                    max_cloud REAL NOT NULL,
                    max_sza REAL NOT NULL,
                    downloaded BOOLEAN DEFAULT 0,
                    downloaded_at TIMESTAMP,
                    content_hash TEXT,
                    no2_valid_pixels INTEGER DEFAULT 0,
                    hcho_valid_pixels INTEGER DEFAULT 0,
                    o3_valid_pixels INTEGER DEFAULT 0,
                    no2_mean REAL,
                    hcho_mean REAL,
                    o3_mean REAL,
                    file_path TEXT,
                    file_size_bytes INTEGER DEFAULT 0,
                    UNIQUE(dataset_id, date, hour)
                );
                
                -- Exports table
                CREATE TABLE IF NOT EXISTS exports (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    dataset_id TEXT NOT NULL REFERENCES datasets(id) ON DELETE CASCADE,
                    format TEXT NOT NULL,
                    file_path TEXT NOT NULL,
                    created_at TIMESTAMP NOT NULL,
                    file_size_bytes INTEGER DEFAULT 0
                );
                
                -- Sites table
                CREATE TABLE IF NOT EXISTS sites (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    code TEXT NOT NULL UNIQUE,
                    name TEXT,
                    latitude REAL NOT NULL,
                    longitude REAL NOT NULL,
                    created_at TIMESTAMP NOT NULL
                );

                -- Batch Jobs table (for batch site imports)
                CREATE TABLE IF NOT EXISTS batch_jobs (
                    id TEXT PRIMARY KEY,
                    name TEXT NOT NULL,
                    created_at TIMESTAMP NOT NULL,
                    status TEXT NOT NULL DEFAULT 'pending',
                    source_file TEXT,
                    total_sites INTEGER DEFAULT 0,
                    completed_sites INTEGER DEFAULT 0,
                    failed_sites INTEGER DEFAULT 0,
                    default_radius_km REAL DEFAULT 10.0,
                    date_start DATE NOT NULL,
                    date_end DATE NOT NULL,
                    day_filter TEXT NOT NULL,
                    hour_filter TEXT NOT NULL,
                    max_cloud REAL DEFAULT 0.3,
                    max_sza REAL DEFAULT 70.0,
                    batch_size INTEGER DEFAULT 5,
                    last_processed_at TIMESTAMP,
                    error_message TEXT
                );

                -- Batch Sites table (individual sites within a batch job)
                CREATE TABLE IF NOT EXISTS batch_sites (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    batch_job_id TEXT NOT NULL REFERENCES batch_jobs(id) ON DELETE CASCADE,
                    site_name TEXT NOT NULL,
                    latitude REAL NOT NULL,
                    longitude REAL NOT NULL,
                    radius_km REAL DEFAULT 10.0,
                    bbox_west REAL NOT NULL,
                    bbox_south REAL NOT NULL,
                    bbox_east REAL NOT NULL,
                    bbox_north REAL NOT NULL,
                    custom_date_start DATE,
                    custom_date_end DATE,
                    custom_hour_start INTEGER,
                    custom_hour_end INTEGER,
                    custom_max_cloud REAL,
                    custom_max_sza REAL,
                    status TEXT NOT NULL DEFAULT 'pending',
                    dataset_id TEXT REFERENCES datasets(id),
                    error_message TEXT,
                    started_at TIMESTAMP,
                    completed_at TIMESTAMP,
                    sequence_number INTEGER NOT NULL,
                    UNIQUE(batch_job_id, sequence_number)
                );

                -- Indexes for common queries
                CREATE INDEX IF NOT EXISTS idx_granules_dataset ON granules(dataset_id);
                CREATE INDEX IF NOT EXISTS idx_granules_hash ON granules(content_hash);
                CREATE INDEX IF NOT EXISTS idx_exports_dataset ON exports(dataset_id);
                CREATE INDEX IF NOT EXISTS idx_batch_sites_job ON batch_sites(batch_job_id);
                CREATE INDEX IF NOT EXISTS idx_batch_sites_status ON batch_sites(status);

                -- Analyses table (for AI-generated chart analyses)
                CREATE TABLE IF NOT EXISTS analyses (
                    id TEXT PRIMARY KEY,
                    dataset_id TEXT NOT NULL,
                    name TEXT NOT NULL,
                    query TEXT NOT NULL,
                    code TEXT NOT NULL,
                    plot_path TEXT NOT NULL,
                    created_at TIMESTAMP NOT NULL,
                    updated_at TIMESTAMP NOT NULL,
                    error_message TEXT,
                    FOREIGN KEY (dataset_id) REFERENCES datasets(id) ON DELETE CASCADE
                );

                CREATE INDEX IF NOT EXISTS idx_analyses_dataset ON analyses(dataset_id);
                CREATE INDEX IF NOT EXISTS idx_analyses_created ON analyses(created_at DESC);

                -- Discovered Variables table (cache for auto-discovered variable names)
                CREATE TABLE IF NOT EXISTS discovered_variables (
                    product_id TEXT PRIMARY KEY,
                    netcdf_var TEXT NOT NULL,
                    discovered_at TIMESTAMP NOT NULL,
                    verified BOOLEAN DEFAULT 0,
                    notes TEXT
                );
            """)

            # Run migrations for existing databases
            self._run_migrations(conn)

    def _run_migrations(self, conn):
        """Run schema migrations for existing databases."""
        # Check if batch_job_id column exists in datasets table
        cursor = conn.execute("PRAGMA table_info(datasets)")
        columns = [row[1] for row in cursor.fetchall()]

        if "batch_job_id" not in columns:
            conn.execute("ALTER TABLE datasets ADD COLUMN batch_job_id TEXT")

        # Check if selected_variables column exists in datasets table
        if "selected_variables" not in columns:
            conn.execute("ALTER TABLE datasets ADD COLUMN selected_variables TEXT")
            logger.info("Added selected_variables column to datasets table")

        # Check if O3 columns exist in granules table
        cursor = conn.execute("PRAGMA table_info(granules)")
        granule_columns = [row[1] for row in cursor.fetchall()]

        if "o3_valid_pixels" not in granule_columns:
            conn.execute("ALTER TABLE granules ADD COLUMN o3_valid_pixels INTEGER DEFAULT 0")

        if "o3_mean" not in granule_columns:
            conn.execute("ALTER TABLE granules ADD COLUMN o3_mean REAL")

        # Check if batch_sites table exists and add missing columns
        try:
            cursor = conn.execute("PRAGMA table_info(batch_sites)")
            batch_site_columns = [row[1] for row in cursor.fetchall()]

            if "radius_km" not in batch_site_columns:
                conn.execute("ALTER TABLE batch_sites ADD COLUMN radius_km REAL DEFAULT 10.0")
                logger.info("Added radius_km column to batch_sites table")

            if "custom_date_start" not in batch_site_columns:
                conn.execute("ALTER TABLE batch_sites ADD COLUMN custom_date_start DATE")
                logger.info("Added custom_date_start column to batch_sites table")

            if "custom_date_end" not in batch_site_columns:
                conn.execute("ALTER TABLE batch_sites ADD COLUMN custom_date_end DATE")
                logger.info("Added custom_date_end column to batch_sites table")

            if "custom_hour_start" not in batch_site_columns:
                conn.execute("ALTER TABLE batch_sites ADD COLUMN custom_hour_start INTEGER")
                logger.info("Added custom_hour_start column to batch_sites table")

            if "custom_hour_end" not in batch_site_columns:
                conn.execute("ALTER TABLE batch_sites ADD COLUMN custom_hour_end INTEGER")
                logger.info("Added custom_hour_end column to batch_sites table")

            if "custom_max_cloud" not in batch_site_columns:
                conn.execute("ALTER TABLE batch_sites ADD COLUMN custom_max_cloud REAL")
                logger.info("Added custom_max_cloud column to batch_sites table")

            if "custom_max_sza" not in batch_site_columns:
                conn.execute("ALTER TABLE batch_sites ADD COLUMN custom_max_sza REAL")
                logger.info("Added custom_max_sza column to batch_sites table")
        except sqlite3.OperationalError:
            # batch_sites table doesn't exist yet (new database)
            pass

    # ==========================================================================
    # Dataset Operations
    # ==========================================================================
    
    def create_dataset(self, dataset: Dataset) -> Dataset:
        """Insert a new dataset into the database."""
        if not dataset.id:
            dataset.id = str(uuid.uuid4())
        
        with self._get_connection() as conn:
            conn.execute("""
                INSERT INTO datasets (
                    id, name, batch_job_id, created_at, bbox_west, bbox_south, bbox_east, bbox_north,
                    date_start, date_end, day_filter, hour_filter, max_cloud, max_sza,
                    selected_variables, status, file_path, file_hash, file_size_mb, last_accessed,
                    granule_count, granules_downloaded
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                dataset.id, dataset.name, dataset.batch_job_id, dataset.created_at,
                dataset.bbox.west, dataset.bbox.south, dataset.bbox.east, dataset.bbox.north,
                dataset.date_start, dataset.date_end,
                json.dumps(dataset.day_filter), json.dumps(dataset.hour_filter),
                dataset.max_cloud, dataset.max_sza,
                json.dumps(dataset.selected_variables) if dataset.selected_variables else None,
                dataset.status.value, dataset.file_path, dataset.file_hash, dataset.file_size_mb,
                dataset.last_accessed, dataset.granule_count, dataset.granules_downloaded
            ))
        return dataset
    
    def get_dataset(self, dataset_id: str) -> Optional[Dataset]:
        """Get a dataset by ID."""
        with self._get_connection() as conn:
            row = conn.execute(
                "SELECT * FROM datasets WHERE id = ?", (dataset_id,)
            ).fetchone()
            return self._row_to_dataset(row) if row else None
    
    def get_dataset_by_name(self, name: str) -> Optional[Dataset]:
        """Get a dataset by name."""
        with self._get_connection() as conn:
            row = conn.execute(
                "SELECT * FROM datasets WHERE name = ?", (name,)
            ).fetchone()
            return self._row_to_dataset(row) if row else None
    
    def get_all_datasets(self) -> list[Dataset]:
        """Get all datasets ordered by creation date (newest first)."""
        with self._get_connection() as conn:
            rows = conn.execute(
                "SELECT * FROM datasets ORDER BY created_at DESC"
            ).fetchall()
            return [self._row_to_dataset(row) for row in rows]
    
    def update_dataset(self, dataset: Dataset) -> None:
        """Update an existing dataset."""
        with self._get_connection() as conn:
            conn.execute("""
                UPDATE datasets SET
                    name = ?, status = ?, file_path = ?, file_hash = ?, file_size_mb = ?,
                    last_accessed = ?, granule_count = ?, granules_downloaded = ?,
                    selected_variables = ?
                WHERE id = ?
            """, (
                dataset.name, dataset.status.value, dataset.file_path, dataset.file_hash,
                dataset.file_size_mb, dataset.last_accessed, dataset.granule_count,
                dataset.granules_downloaded,
                json.dumps(dataset.selected_variables) if dataset.selected_variables else None,
                dataset.id
            ))
    
    def delete_dataset(self, dataset_id: str) -> None:
        """Delete a dataset and all its granules/exports (including files)."""
        dataset = self.get_dataset(dataset_id)
        if not dataset:
            return

        # Get all related files to delete
        files_to_delete = []
        if dataset.file_path:
            files_to_delete.append(Path(dataset.file_path))
            
        granules = self.get_granules_for_dataset(dataset_id)
        for g in granules:
            if g.file_path:
                files_to_delete.append(Path(g.file_path))
                
        exports = self.get_exports_for_dataset(dataset_id)
        for e in exports:
            if e.file_path:
                files_to_delete.append(Path(e.file_path))
        
        # Delete files from disk
        for file_path in files_to_delete:
            try:
                if file_path.exists():
                    file_path.unlink()
            except Exception as e:
                print(f"Error deleting file {file_path}: {e}")
        
        # Delete the dataset folder if it exists
        if dataset.file_path:
            dataset_dir = Path(dataset.file_path).parent
            try:
                if dataset_dir.exists() and dataset_dir.is_dir():
                    import shutil
                    shutil.rmtree(dataset_dir, ignore_errors=True)
                    print(f"Deleted dataset folder: {dataset_dir}")
            except Exception as e:
                print(f"Error deleting folder {dataset_dir}: {e}")

        # Delete from database (cascade will handle granules/exports)
        # But we must manually unlink batch_sites which don't cascade
        with self._get_connection() as conn:
            conn.execute("UPDATE batch_sites SET dataset_id = NULL, status = 'pending' WHERE dataset_id = ?", (dataset_id,))
            conn.execute("DELETE FROM datasets WHERE id = ?", (dataset_id,))
    
    def touch_dataset(self, dataset_id: str) -> None:
        """Update last_accessed timestamp."""
        with self._get_connection() as conn:
            conn.execute(
                "UPDATE datasets SET last_accessed = ? WHERE id = ?",
                (datetime.now(), dataset_id)
            )
    
    def _row_to_dataset(self, row: sqlite3.Row) -> Dataset:
        """Convert a database row to a Dataset object."""
        # Parse selected_variables if present
        selected_variables = None
        if "selected_variables" in row.keys() and row["selected_variables"]:
            try:
                selected_variables = json.loads(row["selected_variables"])
            except (json.JSONDecodeError, TypeError):
                selected_variables = None

        return Dataset(
            id=row["id"],
            name=row["name"],
            batch_job_id=row["batch_job_id"] if "batch_job_id" in row.keys() else None,
            created_at=row["created_at"] if isinstance(row["created_at"], datetime)
                       else datetime.fromisoformat(row["created_at"]),
            bbox=BoundingBox(
                row["bbox_west"], row["bbox_south"], row["bbox_east"], row["bbox_north"]
            ),
            date_start=_parse_date(row["date_start"]),
            date_end=_parse_date(row["date_end"]),
            day_filter=json.loads(row["day_filter"]),
            hour_filter=json.loads(row["hour_filter"]),
            max_cloud=row["max_cloud"],
            max_sza=row["max_sza"],
            selected_variables=selected_variables,  # NEW
            status=DatasetStatus(row["status"]),
            file_path=row["file_path"],
            file_hash=row["file_hash"],
            file_size_mb=row["file_size_mb"] or 0,
            last_accessed=row["last_accessed"],
            granule_count=row["granule_count"] or 0,
            granules_downloaded=row["granules_downloaded"] or 0,
        )
    
    # ==========================================================================
    # Granule Operations
    # ==========================================================================
    
    def create_granule(self, granule: Granule) -> Granule:
        """Insert a new granule."""
        # Compute hash if not set
        if not granule.content_hash:
            granule.content_hash = granule.compute_content_hash()
        
        with self._get_connection() as conn:
            cursor = conn.execute("""
                INSERT INTO granules (
                    dataset_id, date, hour, bbox_west, bbox_south, bbox_east, bbox_north,
                    max_cloud, max_sza, downloaded, downloaded_at, content_hash,
                    no2_valid_pixels, hcho_valid_pixels, o3_valid_pixels, no2_mean, hcho_mean, o3_mean,
                    file_path, file_size_bytes
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                granule.dataset_id, granule.date, granule.hour,
                granule.bbox_west, granule.bbox_south, granule.bbox_east, granule.bbox_north,
                granule.max_cloud, granule.max_sza, granule.downloaded, granule.downloaded_at,
                granule.content_hash, granule.no2_valid_pixels, granule.hcho_valid_pixels, granule.o3_valid_pixels,
                granule.no2_mean, granule.hcho_mean, granule.o3_mean, granule.file_path, granule.file_size_bytes
            ))
            granule.id = cursor.lastrowid
        return granule
    
    def create_granules_batch(self, granules: list[Granule]) -> None:
        """Insert multiple granules efficiently."""
        for g in granules:
            if not g.content_hash:
                g.content_hash = g.compute_content_hash()
        
        with self._get_connection() as conn:
            conn.executemany("""
                INSERT OR IGNORE INTO granules (
                    dataset_id, date, hour, bbox_west, bbox_south, bbox_east, bbox_north,
                    max_cloud, max_sza, downloaded, downloaded_at, content_hash,
                    no2_valid_pixels, hcho_valid_pixels, o3_valid_pixels, no2_mean, hcho_mean, o3_mean,
                    file_path, file_size_bytes
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, [
                (
                    g.dataset_id, g.date, g.hour,
                    g.bbox_west, g.bbox_south, g.bbox_east, g.bbox_north,
                    g.max_cloud, g.max_sza, g.downloaded, g.downloaded_at,
                    g.content_hash, g.no2_valid_pixels, g.hcho_valid_pixels, g.o3_valid_pixels,
                    g.no2_mean, g.hcho_mean, g.o3_mean, g.file_path, g.file_size_bytes
                )
                for g in granules
            ])
    
    def get_granules_for_dataset(self, dataset_id: str) -> list[Granule]:
        """Get all granules for a dataset."""
        with self._get_connection() as conn:
            rows = conn.execute(
                "SELECT * FROM granules WHERE dataset_id = ? ORDER BY date, hour",
                (dataset_id,)
            ).fetchall()
            return [self._row_to_granule(row) for row in rows]
    
    def get_pending_granules(self, dataset_id: str) -> list[Granule]:
        """Get granules that haven't been downloaded yet."""
        with self._get_connection() as conn:
            rows = conn.execute(
                "SELECT * FROM granules WHERE dataset_id = ? AND downloaded = 0 ORDER BY date, hour",
                (dataset_id,)
            ).fetchall()
            return [self._row_to_granule(row) for row in rows]
    
    def find_granule_by_hash(self, content_hash: str) -> Optional[Granule]:
        """Find a granule with matching content hash (for deduplication)."""
        with self._get_connection() as conn:
            row = conn.execute(
                "SELECT * FROM granules WHERE content_hash = ? AND downloaded = 1 LIMIT 1",
                (content_hash,)
            ).fetchone()
            return self._row_to_granule(row) if row else None
    
    def update_granule(self, granule: Granule) -> None:
        """Update a granule after download."""
        with self._get_connection() as conn:
            conn.execute("""
                UPDATE granules SET
                    downloaded = ?, downloaded_at = ?, no2_valid_pixels = ?,
                    hcho_valid_pixels = ?, o3_valid_pixels = ?, no2_mean = ?, hcho_mean = ?, o3_mean = ?,
                    file_path = ?, file_size_bytes = ?
                WHERE id = ?
            """, (
                granule.downloaded, granule.downloaded_at, granule.no2_valid_pixels,
                granule.hcho_valid_pixels, granule.o3_valid_pixels, granule.no2_mean, granule.hcho_mean, granule.o3_mean,
                granule.file_path, granule.file_size_bytes, granule.id
            ))
    
    def mark_granules_downloaded(self, dataset_id: str) -> None:
        """Mark all granules for a dataset as downloaded."""
        with self._get_connection() as conn:
            conn.execute("""
                UPDATE granules SET
                    downloaded = 1,
                    downloaded_at = ?
                WHERE dataset_id = ?
            """, (datetime.now(), dataset_id))
    
    def _row_to_granule(self, row: sqlite3.Row) -> Granule:
        """Convert a database row to a Granule object."""
        return Granule(
            id=row["id"],
            dataset_id=row["dataset_id"],
            date=_parse_date(row["date"]),
            hour=row["hour"],
            bbox_west=row["bbox_west"],
            bbox_south=row["bbox_south"],
            bbox_east=row["bbox_east"],
            bbox_north=row["bbox_north"],
            max_cloud=row["max_cloud"],
            max_sza=row["max_sza"],
            downloaded=bool(row["downloaded"]),
            downloaded_at=row["downloaded_at"],
            content_hash=row["content_hash"],
            no2_valid_pixels=row["no2_valid_pixels"] or 0,
            hcho_valid_pixels=row["hcho_valid_pixels"] or 0,
            o3_valid_pixels=row["o3_valid_pixels"] if "o3_valid_pixels" in row.keys() else 0,
            no2_mean=row["no2_mean"],
            hcho_mean=row["hcho_mean"],
            o3_mean=row["o3_mean"] if "o3_mean" in row.keys() else None,
            file_path=row["file_path"],
            file_size_bytes=row["file_size_bytes"] or 0,
        )
    
    # ==========================================================================
    # Export Operations
    # ==========================================================================
    
    def create_export(self, export: ExportRecord) -> ExportRecord:
        """Insert a new export record."""
        with self._get_connection() as conn:
            cursor = conn.execute("""
                INSERT INTO exports (dataset_id, format, file_path, created_at, file_size_bytes)
                VALUES (?, ?, ?, ?, ?)
            """, (
                export.dataset_id, export.format, export.file_path,
                export.created_at, export.file_size_bytes
            ))
            export.id = cursor.lastrowid
        return export
    
    def get_exports_for_dataset(self, dataset_id: str) -> list[ExportRecord]:
        """Get all exports for a dataset."""
        with self._get_connection() as conn:
            rows = conn.execute(
                "SELECT * FROM exports WHERE dataset_id = ? ORDER BY created_at DESC",
                (dataset_id,)
            ).fetchall()
            return [
                ExportRecord(
                    id=row["id"],
                    dataset_id=row["dataset_id"],
                    format=row["format"],
                    file_path=row["file_path"],
                    created_at=row["created_at"],
                    file_size_bytes=row["file_size_bytes"] or 0,
                )
                for row in rows
            ]
    
    # ==========================================================================
    # Site Operations
    # ==========================================================================
    
    def create_site(self, site: Site) -> Site:
        """Insert a new site."""
        with self._get_connection() as conn:
            cursor = conn.execute("""
                INSERT INTO sites (code, name, latitude, longitude, created_at)
                VALUES (?, ?, ?, ?, ?)
            """, (
                site.code, site.name, site.latitude, site.longitude, site.created_at
            ))
            site.id = cursor.lastrowid
        return site
    
    def get_all_sites(self) -> list[Site]:
        """Get all sites ordered by code."""
        with self._get_connection() as conn:
            rows = conn.execute(
                "SELECT * FROM sites ORDER BY code"
            ).fetchall()
            return [self._row_to_site(row) for row in rows]
    
    def get_sites_in_bbox(self, bbox: BoundingBox) -> list[Site]:
        """Get sites within a bounding box."""
        with self._get_connection() as conn:
            rows = conn.execute("""
                SELECT * FROM sites 
                WHERE latitude >= ? AND latitude <= ?
                  AND longitude >= ? AND longitude <= ?
                ORDER BY code
            """, (bbox.south, bbox.north, bbox.west, bbox.east)).fetchall()
            return [self._row_to_site(row) for row in rows]
    
    def get_sites_as_dict(self,  bbox: BoundingBox = None) -> dict[str, tuple[float, float]]:
        """Get sites as dict format for plotter compatibility.

        Returns:
            Dict mapping site code to (latitude, longitude) tuple
        """
        if bbox:
            sites = self.get_sites_in_bbox(bbox)
        else:
            sites = self.get_all_sites()
        return {s.code: s.to_tuple() for s in sites}

    def get_site_by_code(self, code: str) -> Optional[Site]:
        """Get a site by its code."""
        with self._get_connection() as conn:
            row = conn.execute(
                "SELECT * FROM sites WHERE code = ?", (code,)
            ).fetchone()
            return self._row_to_site(row) if row else None

    def delete_site(self, site_id: int) -> None:
        """Delete a site by ID."""
        with self._get_connection() as conn:
            conn.execute("DELETE FROM sites WHERE id = ?", (site_id,))
    
    def delete_site_by_code(self, code: str) -> None:
        """Delete a site by code."""
        with self._get_connection() as conn:
            conn.execute("DELETE FROM sites WHERE code = ?", (code,))
    
    def seed_default_sites(self) -> int:
        """Import hardcoded default sites from models.SITES.
        
        Skips sites that already exist (by code).
        
        Returns:
            Number of sites added
        """
        added = 0
        for code, (lat, lon) in SITES.items():
            try:
                site = Site(
                    code=code,
                    name="",  # No name in hardcoded data
                    latitude=lat,
                    longitude=lon,
                    created_at=datetime.now()
                )
                self.create_site(site)
                added += 1
            except sqlite3.IntegrityError:
                # Site already exists (code is UNIQUE)
                pass
        return added
    
    def _row_to_site(self, row: sqlite3.Row) -> Site:
        """Convert a database row to a Site object."""
        return Site(
            id=row["id"],
            code=row["code"],
            name=row["name"] or "",
            latitude=row["latitude"],
            longitude=row["longitude"],
            created_at=row["created_at"] if isinstance(row["created_at"], datetime)
                       else datetime.fromisoformat(row["created_at"]),
        )
    
    # ==========================================================================
    # Storage Analytics
    # ==========================================================================
    
    def get_storage_stats(self) -> dict:
        """Get storage usage statistics."""
        with self._get_connection() as conn:
            # Total dataset size
            dataset_size = conn.execute(
                "SELECT COALESCE(SUM(file_size_mb), 0) FROM datasets"
            ).fetchone()[0]
            
            # Total granule size
            granule_size = conn.execute(
                "SELECT COALESCE(SUM(file_size_bytes), 0) FROM granules"
            ).fetchone()[0] / (1024 * 1024)  # Convert to MB
            
            # Export size
            export_size = conn.execute(
                "SELECT COALESCE(SUM(file_size_bytes), 0) FROM exports"
            ).fetchone()[0] / (1024 * 1024)
            
            # Counts
            dataset_count = conn.execute("SELECT COUNT(*) FROM datasets").fetchone()[0]
            granule_count = conn.execute("SELECT COUNT(*) FROM granules").fetchone()[0]
            
            return {
                "dataset_size_mb": dataset_size,
                "granule_size_mb": granule_size,
                "export_size_mb": export_size,
                "total_size_mb": dataset_size + granule_size + export_size,
                "dataset_count": dataset_count,
                "granule_count": granule_count,
            }

    # ==========================================================================
    # Batch Job Operations
    # ==========================================================================

    def create_batch_job(self, job: BatchJob) -> BatchJob:
        """Insert a new batch job."""
        if not job.id:
            job.id = str(uuid.uuid4())

        with self._get_connection() as conn:
            conn.execute("""
                INSERT INTO batch_jobs (
                    id, name, created_at, status, source_file,
                    total_sites, completed_sites, failed_sites,
                    default_radius_km, date_start, date_end,
                    day_filter, hour_filter, max_cloud, max_sza, batch_size,
                    last_processed_at, error_message
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                job.id, job.name, job.created_at, job.status.value, job.source_file,
                job.total_sites, job.completed_sites, job.failed_sites,
                job.default_radius_km, job.date_start, job.date_end,
                json.dumps(job.day_filter), json.dumps(job.hour_filter),
                job.max_cloud, job.max_sza, job.batch_size,
                job.last_processed_at, job.error_message
            ))
        return job

    def get_batch_job(self, job_id: str) -> Optional[BatchJob]:
        """Get a batch job by ID."""
        with self._get_connection() as conn:
            row = conn.execute(
                "SELECT * FROM batch_jobs WHERE id = ?", (job_id,)
            ).fetchone()
            return self._row_to_batch_job(row) if row else None

    def get_all_batch_jobs(self) -> list[BatchJob]:
        """Get all batch jobs ordered by creation date (newest first)."""
        with self._get_connection() as conn:
            rows = conn.execute(
                "SELECT * FROM batch_jobs ORDER BY created_at DESC"
            ).fetchall()
            return [self._row_to_batch_job(row) for row in rows]

    def get_resumable_batch_jobs(self) -> list[BatchJob]:
        """Get batch jobs that can be resumed (PAUSED or ERROR status, not COMPLETED)."""
        with self._get_connection() as conn:
            rows = conn.execute(
                "SELECT * FROM batch_jobs WHERE status IN ('paused', 'error') ORDER BY created_at DESC"
            ).fetchall()
            return [self._row_to_batch_job(row) for row in rows]

    def update_batch_job(self, job: BatchJob) -> None:
        """Update an existing batch job."""
        with self._get_connection() as conn:
            conn.execute("""
                UPDATE batch_jobs SET
                    status = ?, total_sites = ?, completed_sites = ?, failed_sites = ?,
                    batch_size = ?, last_processed_at = ?, error_message = ?
                WHERE id = ?
            """, (
                job.status.value, job.total_sites, job.completed_sites, job.failed_sites,
                job.batch_size, job.last_processed_at, job.error_message, job.id
            ))

    def delete_batch_job(self, job_id: str) -> None:
        """Delete a batch job and all its sites (cascade)."""
        with self._get_connection() as conn:
            conn.execute("DELETE FROM batch_jobs WHERE id = ?", (job_id,))

    def delete_batch_job_full(self, job_id: str) -> None:
        """Delete a batch job, all associated datasets, files, and sites."""
        import shutil
        from pathlib import Path

        # Get the batch job to find the folder name
        job = self.get_batch_job(job_id)
        if not job:
            return

        # Get all datasets in this batch
        with self._get_connection() as conn:
            rows = conn.execute(
                "SELECT * FROM datasets WHERE batch_job_id = ?", (job_id,)
            ).fetchall()

        dataset_ids = [row["id"] for row in rows]

        # Delete dataset files and folders
        deleted_folders = set()
        for row in rows:
            file_path = row["file_path"]
            if file_path:
                fp = Path(file_path)
                # Delete the parent folder (site folder)
                if fp.parent.exists() and fp.parent not in deleted_folders:
                    try:
                        shutil.rmtree(fp.parent)
                        deleted_folders.add(fp.parent)
                    except Exception as e:
                        print(f"Error deleting folder {fp.parent}: {e}")

        # Try to delete the job folder (parent of site folders)
        if deleted_folders:
            job_folder = next(iter(deleted_folders)).parent
            if job_folder.exists() and not any(job_folder.iterdir()):
                try:
                    job_folder.rmdir()
                except Exception:
                    pass

        # Delete from database - use raw connection to disable FK checks
        conn = sqlite3.connect(self.db_path)
        try:
            conn.execute("PRAGMA foreign_keys = OFF")
            cursor = conn.cursor()
            # Delete granules for each dataset
            for ds_id in dataset_ids:
                cursor.execute("DELETE FROM granules WHERE dataset_id = ?", (ds_id,))
            # Delete exports for each dataset
            for ds_id in dataset_ids:
                cursor.execute("DELETE FROM exports WHERE dataset_id = ?", (ds_id,))
            # Delete batch sites
            cursor.execute("DELETE FROM batch_sites WHERE batch_job_id = ?", (job_id,))
            # Delete datasets
            cursor.execute("DELETE FROM datasets WHERE batch_job_id = ?", (job_id,))
            # Delete the batch job
            cursor.execute("DELETE FROM batch_jobs WHERE id = ?", (job_id,))
            conn.commit()
        finally:
            conn.close()

    def _row_to_batch_job(self, row: sqlite3.Row) -> BatchJob:
        """Convert a database row to a BatchJob object."""
        return BatchJob(
            id=row["id"],
            name=row["name"],
            created_at=row["created_at"] if isinstance(row["created_at"], datetime)
                       else datetime.fromisoformat(row["created_at"]),
            status=BatchJobStatus(row["status"]),
            source_file=row["source_file"],
            total_sites=row["total_sites"] or 0,
            completed_sites=row["completed_sites"] or 0,
            failed_sites=row["failed_sites"] or 0,
            default_radius_km=row["default_radius_km"] or 10.0,
            date_start=_parse_date(row["date_start"]),
            date_end=_parse_date(row["date_end"]),
            day_filter=json.loads(row["day_filter"]),
            hour_filter=json.loads(row["hour_filter"]),
            max_cloud=row["max_cloud"] or 0.3,
            max_sza=row["max_sza"] or 70.0,
            batch_size=row["batch_size"] or 5,
            last_processed_at=row["last_processed_at"],
            error_message=row["error_message"],
        )

    # ==========================================================================
    # Batch Site Operations
    # ==========================================================================

    def create_batch_sites(self, sites: list[BatchSite]) -> None:
        """Insert multiple batch sites efficiently."""
        with self._get_connection() as conn:
            conn.executemany("""
                INSERT INTO batch_sites (
                    batch_job_id, site_name, latitude, longitude, radius_km,
                    bbox_west, bbox_south, bbox_east, bbox_north,
                    custom_date_start, custom_date_end, custom_hour_start, custom_hour_end, custom_max_cloud, custom_max_sza,
                    status, dataset_id, error_message, started_at, completed_at, sequence_number
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, [
                (
                    s.batch_job_id, s.site_name, s.latitude, s.longitude, s.radius_km,
                    s.bbox_west, s.bbox_south, s.bbox_east, s.bbox_north,
                    s.custom_date_start, s.custom_date_end, s.custom_hour_start, s.custom_hour_end, s.custom_max_cloud, s.custom_max_sza,
                    s.status.value, s.dataset_id, s.error_message, s.started_at, s.completed_at, s.sequence_number
                )
                for s in sites
            ])

    def get_batch_sites(self, job_id: str) -> list[BatchSite]:
        """Get all sites for a batch job."""
        with self._get_connection() as conn:
            rows = conn.execute(
                "SELECT * FROM batch_sites WHERE batch_job_id = ? ORDER BY sequence_number",
                (job_id,)
            ).fetchall()
            return [self._row_to_batch_site(row) for row in rows]

    def get_pending_batch_sites(self, job_id: str) -> list[BatchSite]:
        """Get pending/queued sites for a batch job (for resume)."""
        with self._get_connection() as conn:
            rows = conn.execute("""
                SELECT * FROM batch_sites
                WHERE batch_job_id = ? AND status IN ('pending', 'queued')
                ORDER BY sequence_number
            """, (job_id,)).fetchall()
            return [self._row_to_batch_site(row) for row in rows]

    def update_batch_site(self, site: BatchSite) -> None:
        """Update a batch site status."""
        with self._get_connection() as conn:
            conn.execute("""
                UPDATE batch_sites SET
                    status = ?, dataset_id = ?, error_message = ?,
                    started_at = ?, completed_at = ?
                WHERE id = ?
            """, (
                site.status.value, site.dataset_id, site.error_message,
                site.started_at, site.completed_at, site.id
            ))

    def reset_interrupted_batch_sites(self, job_id: str) -> int:
        """Reset sites that were interrupted (downloading/processing) back to pending.

        Returns number of sites reset.
        """
        with self._get_connection() as conn:
            cursor = conn.execute("""
                UPDATE batch_sites SET
                    status = 'pending',
                    error_message = 'Interrupted by app restart'
                WHERE batch_job_id = ? AND status IN ('downloading', 'processing', 'queued')
            """, (job_id,))
            return cursor.rowcount

    def _row_to_batch_site(self, row: sqlite3.Row) -> BatchSite:
        """Convert a database row to a BatchSite object."""
        return BatchSite(
            id=row["id"],
            batch_job_id=row["batch_job_id"],
            site_name=row["site_name"],
            latitude=row["latitude"],
            longitude=row["longitude"],
            radius_km=row["radius_km"],
            bbox_west=row["bbox_west"],
            bbox_south=row["bbox_south"],
            bbox_east=row["bbox_east"],
            bbox_north=row["bbox_north"],
            custom_date_start=_parse_date(row["custom_date_start"]) if row["custom_date_start"] else None,
            custom_date_end=_parse_date(row["custom_date_end"]) if row["custom_date_end"] else None,
            custom_hour_start=row["custom_hour_start"],
            custom_hour_end=row["custom_hour_end"],
            custom_max_cloud=row["custom_max_cloud"],
            custom_max_sza=row["custom_max_sza"],
            status=BatchSiteStatus(row["status"]),
            dataset_id=row["dataset_id"],
            error_message=row["error_message"],
            started_at=row["started_at"],
            completed_at=row["completed_at"],
            sequence_number=row["sequence_number"],
        )

    # ==========================================================================
    # Analysis Operations (AI Chart Generation)
    # ==========================================================================

    def save_analysis(self, analysis: Analysis) -> None:
        """Save or update an analysis record."""
        with self._get_connection() as conn:
            conn.execute("""
                INSERT OR REPLACE INTO analyses
                (id, dataset_id, name, query, code, plot_path, created_at, updated_at, error_message)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                analysis.id,
                analysis.dataset_id,
                analysis.name,
                analysis.query,
                analysis.code,
                analysis.plot_path,
                analysis.created_at,
                analysis.updated_at,
                analysis.error_message
            ))

    def get_analyses_for_dataset(self, dataset_id: str) -> list[Analysis]:
        """Retrieve all analyses for a dataset, newest first."""
        with self._get_connection() as conn:
            rows = conn.execute("""
                SELECT id, dataset_id, name, query, code, plot_path,
                       created_at, updated_at, error_message
                FROM analyses
                WHERE dataset_id = ?
                ORDER BY created_at DESC
            """, (dataset_id,)).fetchall()
            return [self._row_to_analysis(row) for row in rows]

    def get_analysis(self, analysis_id: str) -> Optional[Analysis]:
        """Retrieve a specific analysis by ID."""
        with self._get_connection() as conn:
            row = conn.execute("""
                SELECT id, dataset_id, name, query, code, plot_path,
                       created_at, updated_at, error_message
                FROM analyses WHERE id = ?
            """, (analysis_id,)).fetchone()
            return self._row_to_analysis(row) if row else None

    def delete_analysis(self, analysis_id: str) -> None:
        """Delete an analysis and its plot file."""
        analysis = self.get_analysis(analysis_id)
        if analysis:
            plot_path = Path(analysis.plot_path)
            if plot_path.exists():
                plot_path.unlink()
            with self._get_connection() as conn:
                conn.execute("DELETE FROM analyses WHERE id = ?", (analysis_id,))

    def _row_to_analysis(self, row: sqlite3.Row) -> Analysis:
        """Convert a database row to an Analysis object."""
        return Analysis(
            id=row[0],
            dataset_id=row[1],
            name=row[2],
            query=row[3],
            code=row[4],
            plot_path=row[5],
            created_at=row[6] if isinstance(row[6], datetime) else datetime.fromisoformat(row[6]),
            updated_at=row[7] if isinstance(row[7], datetime) else datetime.fromisoformat(row[7]),
            error_message=row[8]
        )

    # ==========================================================================
    # Discovered Variables Operations (Variable Name Cache)
    # ==========================================================================

    def get_cached_variable(self, product_id: str) -> Optional[str]:
        """Get cached variable name from database.

        Args:
            product_id: TEMPO product ID (e.g., "tempo.l2.no2.vertical_column_troposphere")

        Returns:
            netcdf_var if found in cache, None otherwise
        """
        with self._get_connection() as conn:
            row = conn.execute(
                "SELECT netcdf_var FROM discovered_variables WHERE product_id = ?",
                (product_id,)
            ).fetchone()
            return row["netcdf_var"] if row else None

    def cache_discovered_variable(self, product_id: str, netcdf_var: str, verified: bool = False, notes: str = None):
        """Cache discovered variable name to database.

        Args:
            product_id: TEMPO product ID
            netcdf_var: Discovered NetCDF variable name
            verified: Whether this discovery has been human-verified
            notes: Optional notes about the discovery
        """
        with self._get_connection() as conn:
            conn.execute(
                """
                INSERT OR REPLACE INTO discovered_variables
                (product_id, netcdf_var, discovered_at, verified, notes)
                VALUES (?, ?, ?, ?, ?)
                """,
                (product_id, netcdf_var, datetime.now(), verified, notes)
            )

    def get_all_discovered_variables(self) -> list[dict]:
        """Get all discovered variables for review/management.

        Returns:
            List of dicts with keys: product_id, netcdf_var, discovered_at, verified, notes
        """
        with self._get_connection() as conn:
            rows = conn.execute(
                "SELECT product_id, netcdf_var, discovered_at, verified, notes FROM discovered_variables ORDER BY discovered_at DESC"
            ).fetchall()
            return [
                {
                    "product_id": row["product_id"],
                    "netcdf_var": row["netcdf_var"],
                    "discovered_at": row["discovered_at"],
                    "verified": bool(row["verified"]),
                    "notes": row["notes"]
                }
                for row in rows
            ]

    def mark_variable_verified(self, product_id: str, verified: bool = True):
        """Mark a discovered variable as verified (or unverified).

        Args:
            product_id: TEMPO product ID
            verified: True to mark as verified, False to mark as unverified
        """
        with self._get_connection() as conn:
            conn.execute(
                "UPDATE discovered_variables SET verified = ? WHERE product_id = ?",
                (verified, product_id)
            )
