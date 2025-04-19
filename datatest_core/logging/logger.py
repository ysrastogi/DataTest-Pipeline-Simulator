"""
Logging configuration and utilities for DataTest Pipeline Simulator.
"""
import logging
import os
import sys
from logging.handlers import RotatingFileHandler
from pathlib import Path

# Default log format
DEFAULT_LOG_FORMAT = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
DEFAULT_DATE_FORMAT = "%Y-%m-%d %H:%M:%S"

# Log levels
LOG_LEVELS = {
    "debug": logging.DEBUG,
    "info": logging.INFO,
    "warning": logging.WARNING,
    "error": logging.ERROR,
    "critical": logging.CRITICAL,
}

# Global configuration flag
_is_configured = False


def configure_logging(
    level="info",
    log_format=DEFAULT_LOG_FORMAT,
    date_format=DEFAULT_DATE_FORMAT,
    log_file=None,
    max_file_size=10 * 1024 * 1024,  # 10MB
    backup_count=5,
    console=True,
):
    """
    Configure the logging system for the application.
    
    Args:
        level (str): Log level (debug, info, warning, error, critical)
        log_format (str): Format string for log messages
        date_format (str): Format string for dates in log messages
        log_file (str): Path to log file (if None, file logging is disabled)
        max_file_size (int): Maximum size of log file before rotation
        backup_count (int): Number of backup log files to keep
        console (bool): Whether to log to console
        
    Returns:
        bool: True if logging was configured, False if it was already configured
    """
    global _is_configured
    
    if _is_configured:
        return False
    
    # Convert string level to logging level
    log_level = LOG_LEVELS.get(level.lower(), logging.INFO)
    
    # Configure root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(log_level)
    
    # Create formatter
    formatter = logging.Formatter(log_format, date_format)
    
    # Add console handler if requested
    if console:
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setFormatter(formatter)
        root_logger.addHandler(console_handler)
    
    # Add file handler if log_file is specified
    if log_file:
        # Create log directory if it doesn't exist
        log_path = Path(log_file)
        if not log_path.parent.exists():
            log_path.parent.mkdir(parents=True, exist_ok=True)
            
        file_handler = RotatingFileHandler(
            log_file,
            maxBytes=max_file_size,
            backupCount=backup_count
        )
        file_handler.setFormatter(formatter)
        root_logger.addHandler(file_handler)
    
    _is_configured = True
    
    # Log that the logging system has been configured
    root_logger.debug(f"Logging system configured with level: {level}")
    return True


def get_logger(name):
    """
    Get a logger with the given name. If logging has not been configured,
    configure it with default settings.
    
    Args:
        name (str): Logger name, typically the module name
        
    Returns:
        logging.Logger: Configured logger
    """
    global _is_configured
    
    # Auto-configure logging with defaults if not already configured
    if not _is_configured:
        log_dir = os.environ.get("DATATEST_LOG_DIR", "logs")
        log_level = os.environ.get("DATATEST_LOG_LEVEL", "info")
        log_file = os.path.join(log_dir, "datatest.log")
        
        configure_logging(
            level=log_level,
            log_file=log_file,
            console=True
        )
    
    return logging.getLogger(name)