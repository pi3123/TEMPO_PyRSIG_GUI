"""Global constants and configuration for TEMPO Analyzer."""

# Default bounding box (Southern California)
DEFAULT_BBOX = [-119.68, 32.23, -116.38, 35.73]

# List of sites for marking on the map
# Format: 'Code': (Latitude, Longitude)
SITES = {
    # Utah
    'BV': (40.903, -111.884), 'HW': (40.736, -111.872),
    'RB': (40.767, -111.828), 'ER': (40.601, -112.356),
    # Colorado
    'LC': (39.779, -105.005),
    # Arizona
    'PX': (33.504, -112.096),
    # Texas
    'HA': (29.9, -95.33), 'HB': (29.67, -95.5),
    # California
    'PR': (34.01, -118.069), 'BN': (33.921, -116.858),
    'PS': (33.853, -116.541), 'SB': (34.107, -117.274)
}

# FIPS codes for downloading Census road data
STATE_FIPS = {
    '06': 'California', 
    '48': 'Texas', 
    '49': 'Utah', 
    '04': 'Arizona',
    '08': 'Colorado', 
    '36': 'New York', 
    '12': 'Florida'
}

# Region presets for the UI
REGION_PRESETS = {
    'Southern California': ([-119.68, 32.23, -116.38, 35.73], '06'),
    'Utah (Salt Lake)': ([-112.8, 40.0, -111.5, 41.5], '49'),
    'Texas (Houston)': ([-96.5, 29.0, -94.5, 30.5], '48'),
    'Arizona (Phoenix)': ([-113.3, 32.8, -111.0, 34.2], '04'),
    'Colorado (Denver)': ([-105.5, 39.3, -104.3, 40.2], '08'),
}
