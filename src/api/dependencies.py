"""
Dependencies for the DataTest Pipeline Simulator API.
"""
from typing import Optional
from fastapi import Depends, HTTPException, status
from fastapi.security import APIKeyHeader
import os

from core.config import Configuration
from pathlib import Path

# API key authentication
API_KEY_NAME = "X-API-Key"
API_KEY = os.environ.get("DATATEST_API_KEY", "dev-key-for-testing")
api_key_header = APIKeyHeader(name=API_KEY_NAME)


async def get_api_key(api_key: str = Depends(api_key_header)):
    """Validate API key."""
    if api_key != API_KEY:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid API Key",
        )
    return api_key


def get_configuration(config_dir: Optional[str] = None):
    """Get configuration for the API."""
    config = Configuration(Path(config_dir) if config_dir else None)
    return config