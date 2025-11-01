"""
Database error module.
Defines database-related exceptions.
"""

from .base import BaseError


class DatabaseError(BaseError):
    """
    Exception raised when a database error occurs.
    
    Args:
        message (str): Error message. Defaults to "Database error".
        code (str): Error code. Defaults to "DB_ERR".
    """

    def __init__(self, message="Database error", code="DB_ERR"):
        super().__init__(message, code)
