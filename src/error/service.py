"""
Service error module.
Defines configuration and external service exceptions.
"""

from .base import BaseError


class ConfigurationError(BaseError):
    """
    Exception raised when there is a configuration error.
    
    Args:
        message (str): Error message. Defaults to "Configuration error".
        code (str): Error code. Defaults to "CONFIG_ERR".
    """

    def __init__(self, message="Configuration error", code="CONFIG_ERR"):
        super().__init__(message, code)


class ExternalServiceError(BaseError):
    """
    Exception raised when an external service fails.
    
    Args:
        message (str): Error message. Defaults to "External service error".
        code (str): Error code. Defaults to "EXT_ERR".
    """

    def __init__(self, message="External service error", code="EXT_ERR"):
        super().__init__(message, code)
