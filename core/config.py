"""
Configuration management for the DataTest Pipeline Simulator.
"""
import os
import yaml
from pathlib import Path
from typing import Dict, Any, Optional
import logging
from copy import deepcopy

logger = logging.getLogger(__name__)

class Configuration:
    """Configuration manager for the pipeline simulator."""
    
    def __init__(self, config_dir: Optional[Path] = None):
        """Initialize the configuration system."""
        from config.settings import CONFIG_DIR, BASE_DIR
        
        self.config_dir = config_dir or CONFIG_DIR
        self.base_dir = BASE_DIR
        self.settings = self._load_settings()
        
    def _load_settings(self) -> Dict[str, Any]:
        """Load all settings from the settings module."""
        from config import settings
        
        # Get all uppercase attributes from settings module
        config_dict = {key: value for key, value in settings.__dict__.items() 
                      if key.isupper()}
        
        return config_dict
    
    def get(self, key: str, default: Any = None) -> Any:
        """Get a configuration value."""
        return self.settings.get(key, default)
    
    def get_nested(self, parent_key: str, child_key: str, default: Any = None) -> Any:
        """Get a nested configuration value."""
        parent = self.settings.get(parent_key, {})
        if isinstance(parent, dict):
            return parent.get(child_key, default)
        return default
    
    def load_pipeline_config(self, pipeline_name: str) -> Dict[str, Any]:
        """Load pipeline-specific configuration."""
        pipeline_config_path = self.config_dir / "pipelines" / f"{pipeline_name}.yaml"
        
        if not pipeline_config_path.exists():
            logger.warning(f"No configuration found for pipeline: {pipeline_name}")
            return {}
        
        with open(pipeline_config_path, "r") as f:
            return yaml.safe_load(f)
    
    def get_environment(self) -> str:
        """Get the current environment."""
        return os.environ.get("DATATEST_ENV", "development")