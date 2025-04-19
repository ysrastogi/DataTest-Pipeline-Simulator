"""
Spark session management for the DataTest Pipeline Simulator.
"""
import logging
from typing import Dict, Any, Optional
from pyspark.sql import SparkSession
from contextlib import contextmanager

from core.config import Configuration

logger = logging.getLogger(__name__)

class SparkSessionManager:
    """Manager for PySpark sessions."""
    
    _instance = None
    
    def __new__(cls, *args, **kwargs):
        """Implement singleton pattern."""
        if cls._instance is None:
            cls._instance = super(SparkSessionManager, cls).__new__(cls)
            cls._instance._initialized = False
        return cls._instance
    
    def __init__(self, config: Optional[Configuration] = None):
        """Initialize the Spark session manager."""
        if self._initialized:
            return
            
        self.config = config or Configuration()
        self.spark_config = self.config.get("SPARK_CONFIG", {})
        self._session = None
        self._initialized = True
    
    def get_session(self) -> SparkSession:
        """Get or create a Spark session."""
        if self._session is None:
            self._create_session()
        return self._session
    
    def _create_session(self) -> None:
        """Create a new Spark session with configured settings."""
        app_name = self.spark_config.get("app_name", "DataTest Pipeline")
        master = self.spark_config.get("master", "local[*]")
        
        builder = SparkSession.builder.appName(app_name).master(master)
        
        # Apply additional configurations
        for key, value in self.spark_config.get("configs", {}).items():
            builder = builder.config(key, value)
        
        self._session = builder.getOrCreate()
        
        # Set log level
        log_level = self.spark_config.get("log_level", "WARN")
        self._session.sparkContext.setLogLevel(log_level)
        
        logger.info(f"Created Spark session: {app_name} on {master}")
    
    def stop(self) -> None:
        """Stop the current Spark session."""
        if self._session is not None:
            self._session.stop()
            self._session = None
            logger.info("Stopped Spark session")
    
    @contextmanager
    def session_context(self):
        """Context manager for Spark sessions."""
        try:
            yield self.get_session()
        finally:
            self.stop()