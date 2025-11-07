"""
Logging configuration module.

Provides utilities to set up and configure application-wide logging using
a JSON configuration file with support for structured JSON logging.
"""

from pathlib import Path
import json
import logging.config
import threading
from typing import Literal, get_args

_logging_configured = False
_config_lock = threading.Lock()

CONFIG_FILE_TYPE = Literal["detailed", "json"]
_valid_config_types = get_args(CONFIG_FILE_TYPE)

def _setup_logging(type_file_logging: CONFIG_FILE_TYPE, name_file_log: str = "app", path_dir_logs: str | None = None) -> None:
    """
    Configure logging using a JSON configuration file.

    This function is thread-safe and will only configure logging once.

    Args:
        type_file_logging: Type of logging configuration to use.
        name_file_log: Base name for the log file without extension.
            Defaults to "app", resulting in "app.log.jsonl" or "app.log" depending on the formatter used.
        path_dir_logs: Directory path where log files will be created.
            If None, defaults to "./logs" relative to current working directory.

    Raises:
        ValueError: If type_file_logging is invalid, config is malformed, or name_file_log is empty.
        FileNotFoundError: If the configuration file cannot be found.
        OSError: If the log directory cannot be created.
    """
    if type_file_logging not in _valid_config_types:
        raise ValueError(f"Invalid type_file_logging: {type_file_logging}. Must be one of {_valid_config_types}")

    if not name_file_log or not name_file_log.strip():
        raise ValueError("name_file_log cannot be empty")

    global _logging_configured

    with _config_lock:
        if _logging_configured:
            return

        config_file = Path(__file__).parent / f"config_{type_file_logging}.json"
        
        if not config_file.exists():
            raise FileNotFoundError(f"Logging config not found at: {config_file}")
        
        # Load and parse config
        try:
            with open(config_file, encoding="utf-8") as f_in:
                config = json.load(f_in)
        except json.JSONDecodeError as e:
            raise ValueError(f"Invalid JSON in logging config '{config_file}': {e}")
        
        # Validate required config structure
        required_keys = ["formatters", "handlers"]
        missing_keys = [key for key in required_keys if key not in config]
        if missing_keys:
            raise ValueError(f"Missing required keys in config: {missing_keys}")
        
        if "file" not in config.get("handlers", {}):
            raise ValueError("Missing 'file' handler in config")
        
        # Handle JSON formatter setup
        if type_file_logging == "json":
            if "json" not in config.get("formatters", {}):
                raise ValueError("JSON formatter requested but 'json' formatter not found in config")
            
            # Set up JSON formatter class reference
            module_name = __name__.rsplit(".", 1)[0]
            config["formatters"]["json"]["()"] = f"{module_name}.formatter.JSONFormatter"

        # Determine and create log directory
        path_logs = Path(path_dir_logs) if path_dir_logs else Path(".") / "logs"

        try:
            path_logs.mkdir(exist_ok=True, parents=True)
        except OSError as e:
            raise OSError(f"Failed to create log directory '{path_logs}': {e}")
        
        # Set log file path with appropriate extension
        file_extension = ".log.jsonl" if type_file_logging == "json" else ".log"
        log_file_path = path_logs / f"{name_file_log.strip()}{file_extension}"
        config["handlers"]["file"]["filename"] = log_file_path.as_posix()

        # Apply configuration
        try:
            logging.config.dictConfig(config)
        except Exception as e:
            raise ValueError(f"Failed to apply logging configuration: {e}")
        
        _logging_configured = True


def setup_logging(type_file_logging: CONFIG_FILE_TYPE, name_file_log: str = "app", path_dir_logs: str | None = None) -> None:
    """
    Configure application logging.

    Args:
        type_file_logging: Type of logging configuration ("json" or "detailed").
        name_file_log: Base name for the log file without extension.
        path_dir_logs: Directory path where log files will be created.

    Raises:
        RuntimeError: If logging has already been configured.
        ValueError: If type_file_logging is invalid, config is malformed, or name_file_log is empty.
        FileNotFoundError: If the configuration file cannot be found.
        OSError: If log directory cannot be created.
    """
    global _logging_configured
    
    if _logging_configured:
        raise RuntimeError("Logging has already been configured. Cannot reconfigure.")

    _setup_logging(
        type_file_logging=type_file_logging,
        name_file_log=name_file_log,
        path_dir_logs=path_dir_logs
    )


def get_logger(name: str) -> logging.Logger:
    """
    Get a configured logger instance.

    Automatically initializes logging with JSON format on first call
    if setup_logging() was not explicitly called.

    Args:
        name: Logger name, typically `__name__` for module-level loggers.

    Returns:
        Configured Logger instance.

    Raises:
        ValueError: If name is empty or whitespace-only.
        TypeError: If name is not a string.

    Example:
        >>> logger = get_logger(__name__)
        >>> logger.info("Application started")
    """
    if not isinstance(name, str):
        raise TypeError("Logger 'name' must be a string")
    
    name = name.strip()
    if not name:
        raise ValueError("Logger 'name' cannot be empty")

    if not _logging_configured:
        _setup_logging(type_file_logging="json")

    return logging.getLogger(name)
