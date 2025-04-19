"""
Global settings for DataTest Pipeline Simulator.
"""
import os
import yaml
from pathlib import Path

# Base directories
BASE_DIR = Path(__file__).parent.parent
CONFIG_DIR = BASE_DIR / "config"
DATA_DIR = BASE_DIR / "data"
LOGS_DIR = BASE_DIR / "logs"

# Ensure required directories exist
for directory in [DATA_DIR, LOGS_DIR]:
    directory.mkdir(exist_ok=True, parents=True)

# Logging configuration
LOGGING = {
    "level": os.environ.get("DATATEST_LOG_LEVEL", "info"),
    "format": "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    "date_format": "%Y-%m-%d %H:%M:%S",
    "file": str(LOGS_DIR / "datatest.log"),
    "max_file_size": 10 * 1024 * 1024,  # 10MB
    "backup_count": 5,
    "console": True,
}

# Spark configuration
SPARK_CONFIG = {
    "app_name": "DataTest Pipeline Simulator",
    "master": "local[*]",
    "log_level": "WARN",
    "configs": {
        "spark.sql.execution.arrow.pyspark.enabled": "true",
        "spark.sql.shuffle.partitions": "16",
    }
}

# Load environment-specific settings
ENV = os.environ.get("DATATEST_ENV", "development")
env_file = CONFIG_DIR / f"environments/{ENV}.yaml"

if env_file.exists():
    with open(env_file, "r") as f:
        env_settings = yaml.safe_load(f)
        
    # Update settings with environment-specific values
    for key, value in env_settings.items():
        if isinstance(value, dict) and key in globals():
            globals()[key].update(value)
        else:
            globals()[key] = value