"""
Custom JSON formatter for structured logging.

This module provides a JSONFormatter class that outputs log records as
JSON strings, making logs machine-readable and suitable for log aggregation
systems.

Based on: https://github.com/mCodingLLC/VideosSampleCode/blob/master/videos/135_modern_logging/mylogger.py
"""

import datetime as dt
import json
import logging
from typing import override, Any, Dict

LOG_RECORD_BUILTIN_ATTRS = {
    "args",
    "asctime",
    "created",
    "exc_info",
    "exc_text",
    "filename",
    "funcName",
    "levelname",
    "levelno",
    "lineno",
    "module",
    "msecs",
    "message",
    "msg",
    "name",
    "pathname",
    "process",
    "processName",
    "relativeCreated",
    "stack_info",
    "thread",
    "threadName",
    "taskName",
}


class JSONFormatter(logging.Formatter):
    """
    JSON formatter for logging records.

    Converts LogRecord objects into JSON strings with configurable field
    mappings. Automatically includes exception info, stack traces, and
    custom fields added to log records.

    Attributes:
        fmt_keys: Dictionary mapping output JSON keys to LogRecord attribute names.
    """

    def __init__(self, *, fmt_keys: dict[str, str] | None = None) -> None:
        """
        Initialize the JSON formatter.

        Args:
            fmt_keys: Optional dictionary mapping output keys to LogRecord attributes.
                If None, only default fields (message, timestamp) are included.
                Example: {"level": "levelname", "logger": "name"}
        """
        super().__init__()
        self.fmt_keys = fmt_keys if fmt_keys is not None else {}

    @override
    def format(self, record: logging.LogRecord) -> str:
        """
        Format a log record as a JSON string.

        Args:
            record: The LogRecord to format.

        Returns:
            A JSON string representation of the log record.
        """
        message = self._prepare_log_dict(record)
        return json.dumps(message, default=str)

    def _prepare_log_dict(self, record: logging.LogRecord) -> Dict[str, Any]:
        """
        Prepare a dictionary representation of the log record.

        Private method that extracts and organizes log record data into a
        dictionary structure. Includes mapped fields, always-present fields
        (message, timestamp), exception info, and any custom attributes.

        Args:
            record: The LogRecord to convert.

        Returns:
            Dictionary containing all relevant log data, ready for JSON serialization.
        """
        tz_ = dt.timezone.utc
        try:
            local_now = dt.datetime.now(tz_).astimezone()
            tz = local_now.tzinfo if local_now.tzinfo is not None else tz_
        except (OSError, ValueError):
            # Fallback to UTC if local timezone cannot be determined
            tz = tz_

        always_fields = {
            "message": record.getMessage(),
            "timestamp": dt.datetime.fromtimestamp(
                record.created, tz=tz
            ).isoformat(),
        }
        if record.exc_info is not None:
            always_fields["exc_info"] = self.formatException(record.exc_info)

        if record.stack_info is not None:
            always_fields["stack_info"] = self.formatStack(record.stack_info)

        message = {
            key: msg_val
            if (msg_val := always_fields.pop(val, None)) is not None
            else getattr(record, val)
            for key, val in self.fmt_keys.items()
        }
        message.update(always_fields)

        for key, val in record.__dict__.items():
            if key not in LOG_RECORD_BUILTIN_ATTRS:
                message[key] = val

        return message
