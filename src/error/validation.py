"""
Validation error module.
Defines validation-related exceptions.
"""

from .base import BaseError


class ValidationError(BaseError):
    """
    Exception raised when data validation fails.
    
    Args:
        message (str): Error message. Defaults to "Validation error".
        code (str): Error code. Defaults to "VAL_ERR".
    """

    def __init__(self, message="Validation error", code="VAL_ERR"):
        super().__init__(message, code)
