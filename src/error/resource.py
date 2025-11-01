"""
Resource error module.
Defines resource-related exceptions.
"""

from .base import BaseError


class NotFoundError(BaseError):
    """
    Exception raised when a resource is not found.
    
    Args:
        message (str): Error message. Defaults to "Resource not found".
        code (str): Error code. Defaults to "NOT_FOUND".
    """

    def __init__(self, message="Resource not found", code="NOT_FOUND"):
        super().__init__(message, code)
