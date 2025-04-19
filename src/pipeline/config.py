import yaml
import os
from typing import Dict, Any, Optional, List
import logging

class PipelineConfig:
    """Configuration manager for pipelines."""
    
    def __init__(self, config_path: Optional[str] = None):
        self.logger = logging.getLogger("pipeline.config")
        self.config: Dict[str, Any] = {}
        
        if config_path:
            self.load_config(config_path)
    
    def load_config(self, config_path: str) -> Dict[str, Any]:
        """Load configuration from a YAML file."""
        if not os.path.exists(config_path):
            self.logger.error(f"Config file not found: {config_path}")
            raise FileNotFoundError(f"Config file not found: {config_path}")
            
        try:
            with open(config_path, 'r') as f:
                self.config = yaml.safe_load(f)
            self.logger.info(f"Loaded configuration from {config_path}")
            return self.config
        except Exception as e:
            self.logger.error(f"Failed to load config from {config_path}: {str(e)}")
            raise
    
    def get_pipeline_config(self, pipeline_name: str) -> Dict[str, Any]:
        """Get configuration for a specific pipeline."""
        pipelines = self.config.get("pipelines", {})
        if pipeline_name not in pipelines:
            self.logger.warning(f"No configuration found for pipeline: {pipeline_name}")
            return {}
        
        return pipelines[pipeline_name]
    
    def get_stage_configs(self, pipeline_name: str) -> List[Dict[str, Any]]:
        """Get stage configurations for a pipeline."""
        pipeline_config = self.get_pipeline_config(pipeline_name)
        return pipeline_config.get("stages", [])