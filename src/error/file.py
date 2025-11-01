"""
File error module.
Defines file-related exceptions.
"""

from .base import BaseError


class InvalidFileTypeError(BaseError):
    """
    Exception raised when the file type is invalid.
    
    Args:
        message (str): Error message. Defaults to "Invalid file type".
        code (str): Error code. Defaults to "INVALID_FILE_TYPE".
    """

    def __init__(self, message="Invalid file type", code="INVALID_FILE_TYPE"):
        super().__init__(message, code)


class FileProcessingError(BaseError):
    """
    Exception raised when an error occurs during file processing.
    
    Args:
        message (str): Error message. Defaults to "File processing error".
        code (str): Error code. Defaults to "FILE_PROC_ERR".
    """

    def __init__(self, message="File processing error", code="FILE_PROC_ERR"):
        super().__init__(message, code)
