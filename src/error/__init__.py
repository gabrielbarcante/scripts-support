"""
Custom exceptions module.
Imports and exports all custom error classes.
"""

from .base import BaseError
from .validation import ValidationError
from .resource import NotFoundError
from .database import DatabaseError
from .auth import AuthenticationError, AuthorizationError
from .service import ConfigurationError, ExternalServiceError
from .file import InvalidFileTypeError, FileProcessingError

__all__ = [
    "BaseError",
    "ValidationError",
    "NotFoundError",
    "DatabaseError",
    "AuthenticationError",
    "AuthorizationError",
    "ConfigurationError",
    "ExternalServiceError",
    "InvalidFileTypeError",
    "FileProcessingError",
]
