# TEMPO Analyzer

**TEMPO Analyzer** is a powerful desktop application designed to streamline the retrieval, analysis, and visualization of satellite air quality data from NASA's [TEMPO (Tropospheric Emissions: Monitoring of Pollution)](https://tempo.si.edu/) mission. 

It provides an intuitive interface for researchers and data scientists to download NO₂, HCHO, and FNR (Formaldehyde-to-NO₂ Ratio) data, visualize it on interactive maps, extract site-specific time series, and export processed data for further analysis.

![TEMPO Analyzer Screenshot](docs/screenshot.png)

## Key Features

*   **centralized Library**: Manage all your downloaded datasets in one place. Easily search, filter, and access your data.
*   **Smart Dataset Creation**: 
    *   Define custom geographic regions of interest.
    *   Choose specific date ranges.
    *   Select data products (NO₂, HCHO) with automatic quality filtering.
*   **Batch Processing**: Efficiently bulk-import data for multiple date ranges or disjoint time periods using CSV configuration files.
*   **Unified Workspace**:
    *   **Interactive Maps**: High-resolution visualization of pollutant concentrations on improved base maps.
    *   **Site Analysis**: precise extraction of data at specific coordinates (e.g., monitoring stations).
    *   **Granule Inspection**: detailed view of individual data files.
*   **Flexible Export**: Export processed data to Excel or CSV formats, including daily averages and site-specific time series.
*   **Robust Architecture**: 
    *   Built with modern Python and **Flet** (Flutter for Python).
    *   Includes automatic job recovery and smart caching to handle large satellite datasets reliably.
    *   Supports parallel downloading for faster data retrieval.

## Installation

### Prerequisites

*   **Python 3.10** or higher.
*   **Git** (for cloning the repository).
*   Internet connection (for downloading satellite data).

### Setup

1.  **Clone the repository:**
    ```bash
    git clone https://github.com/your-username/tempo-analyzer.git
    cd tempo-analyzer
    ```

2.  **Create a virtual environment (Recommended):**
    ```bash
    # Windows
    python -m venv venv
    venv\Scripts\activate

    # macOS/Linux
    python3 -m venv venv
    source venv/bin/activate
    ```

3.  **Install dependencies:**
    ```bash
    pip install -r requirements.txt
    ```

## Usage

### Running the Application

To start the application, run the main module from the root directory:

```bash
python -m tempo_app.main
```

### Workflow

1.  **Create a Dataset**:
    *   Navigate to the **+ New Dataset** page.
    *   Select your region (e.g., "Los Angeles Basin").
    *   Choose the Product (NO₂ or HCHO).
    *   Select your Start and End dates.
    *   Click **Download** to begin fetching data from NASA RSIG.

2.  **Batch Import**:
    *   For complex queries involving multiple date ranges, use the **Batch Import** page.
    *   Load a CSV file specifying your desired date ranges and regions.
    *   The system will queue and process these jobs sequentially or in parallel.

3.  **Analyze in Workspace**:
    *   From the **Library**, click on a dataset to open its **Workspace**.
    *   **Map Tab**: View the spatial distribution of pollutants. Use the timeline slider to animate changes over time.
    *   **Styles**: Toggle between different map styles (Light, Dark, Satellite) and adjust data overlays.
    *   **Sites**: Add specific monitoring locations to see data values at those exact points.

4.  **Export Data**:
    *   Go to the **Export** tab within the Workspace.
    *   Choose your export format (e.g., Daily Averages, Raw Granules).
    *   Processed files will be saved to your configured output directory.

## Configuration

Access the **Settings** page to configure:

*   **Data Directory**: Where large satellite files will be stored.
*   **UI Scale**: Adjust text size for better readability on high-DPI screens.
*   **Download Workers**: Number of parallel connections for faster downloads (default: 8).

## How Downloading Works

The application uses the EPA's [Remote Sensing Information Gateway (RSIG)](https://www.epa.gov/hesc/remote-sensing-information-gateway) API via the `pyrsig` Python library to retrieve TEMPO satellite data.

### Architecture Overview

```
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐
│   User Request  │───▶│  RSIGDownloader  │───▶│   EPA RSIG API  │
│  (dates, bbox)  │    │  (parallel mgr)  │    │   (pyrsig lib)  │
└─────────────────┘    └──────────────────┘    └─────────────────┘
                              │
                    ┌─────────┼─────────┐
                    ▼         ▼         ▼
              ┌─────────┐ ┌─────────┐ ┌─────────┐
              │Worker 1 │ │Worker 2 │ │Worker N │  (parallel day downloads)
              └────┬────┘ └────┬────┘ └────┬────┘
                   │           │           │
                   ▼           ▼           ▼
              ┌───────────────────────────────────┐
              │       Per-Hour NetCDF Files       │
              │  tempo_2024-01-15_08.nc, etc.     │
              └───────────────────────────────────┘
```

### Daily Batch Strategy

Instead of making individual API calls for each hour (which would create excessive server load), the downloader uses a **daily batch strategy**:

1.  **Batch Request**: For each day, a single API request fetches all hours at once (e.g., 08:00–20:00 UTC).
2.  **Data Splitting**: The response is then split into per-hour NetCDF files for consistent downstream processing.
3.  **Parallel Workers**: Multiple days are downloaded in parallel using a configurable number of workers (1–8, controlled by the `Download Workers` setting).

### Worker Isolation

Each parallel worker maintains its own:
*   **Temporary directory**: Avoids file conflicts between concurrent downloads.
*   **API session**: Independent connection to the RSIG server with its own `RsigApi` instance.
*   **Progress tracking**: Thread-safe progress updates via async locks.

### Data Products Retrieved

For each granule, the downloader fetches:
*   **NO₂ Tropospheric Vertical Column** (`tempo.l2.no2.vertical_column_troposphere`)
*   **HCHO Total Vertical Column** (`tempo.l2.hcho.vertical_column`)

These are combined into a single NetCDF file with quality filtering applied:
*   `minimum_quality`: Normal quality or better
*   `maximum_cloud_fraction`: Configurable (default: 50%)
*   `maximum_solar_zenith_angle`: Configurable (default: 70°)

### API Authentication

The downloader supports two authentication modes:
*   **Anonymous**: Default mode, uses `"anonymous"` as the API key.
*   **User Key**: If configured in Settings, your personal RSIG API key is used for potentially higher rate limits.

### File Naming Convention

Downloaded files follow this naming pattern:
```
tempo_{date}_{hour}.nc
```
For example: `tempo_2024-06-15_14.nc` contains data for June 15, 2024 at 14:00 UTC.

## Development

### Project Structure

*   `src/tempo_app/`: Main application source code.
    *   `core/`: Backend logic (Downloader, Processor, Scheduler).
    *   `ui/`: Flet-based UI components and pages.
    *   `storage/`: Database and file system management.
*   `tests/`: Unit and integration tests.

### Contributing

Contributions are welcome! Please ensure you:
1.  Fork the repository.
2.  Create a feature branch.
3.  Submit a Pull Request with a clear description of your changes.

## License

This project is licensed under the MIT License. See `LICENSE` for details.
