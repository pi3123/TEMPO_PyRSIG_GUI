import json
from pathlib import Path
from typing import Dict, Any, Optional

class ConfigManager:
    """Manages application configuration."""
    
    DEFAULT_CONFIG = {
        "data_dir": None,  # None means use default logic
        "font_scale": 1.0,
        "theme_mode": "light", # Reserved for future
        "download_workers": 8,  # Number of parallel download workers
        "rsig_api_key": "",  # NASA RSIG API key (optional but recommended)
    }
    
    def __init__(self, app_name: str = "tempo_analyzer"):
        self.config_dir = Path.home() / f".{app_name}"
        self.config_file = self.config_dir / "config.json"
        self._config = self._load_config()
        
    def _load_config(self) -> Dict[str, Any]:
        """Load config from file or return defaults."""
        if not self.config_file.exists():
            return self.DEFAULT_CONFIG.copy()
            
        try:
            with open(self.config_file, 'r') as f:
                saved_config = json.load(f)
                # Merge with defaults to ensure all keys exist
                config = self.DEFAULT_CONFIG.copy()
                config.update(saved_config)
                return config
        except Exception as e:
            print(f"Error loading config: {e}")
            return self.DEFAULT_CONFIG.copy()
            
    def save_config(self):
        """Save current config to file."""
        self.config_dir.mkdir(parents=True, exist_ok=True)
        try:
            with open(self.config_file, 'w') as f:
                json.dump(self._config, f, indent=4)
        except Exception as e:
            print(f"Error saving config: {e}")
            
    def get(self, key: str, default: Any = None) -> Any:
        return self._config.get(key, default)
        
    def set(self, key: str, value: Any):
        self._config[key] = value
        self.save_config()

    @property
    def data_dir(self) -> Optional[str]:
        return self._config.get("data_dir")
        
    @property
    def font_scale(self) -> float:
        return self._config.get("font_scale", 1.0)
    
    @property
    def download_workers(self) -> int:
        return self._config.get("download_workers", 4)

    @property
    def rsig_api_key(self) -> str:
        """Get the configured RSIG API key."""
        return self._config.get("rsig_api_key", "")
