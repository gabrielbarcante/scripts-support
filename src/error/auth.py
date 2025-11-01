"""
Authentication and authorization error module.
Defines auth-related exceptions.
"""

from .base import BaseError


class AuthenticationError(BaseError):
    """
    Exception raised when authentication fails.
    
    Args:
        message (str): Error message. Defaults to "Authentication error".
        code (str): Error code. Defaults to "AUTH_ERR".
    """

    def __init__(self, message="Authentication error", code="AUTH_ERR"):
        super().__init__(message, code)


class AuthorizationError(BaseError):
    """
    Exception raised when there is no permission to access a resource.
    
    Args:
        message (str): Error message. Defaults to "Permission denied".
        code (str): Error code. Defaults to "AUTHZ_ERR".
    """

    def __init__(self, message="Permission denied", code="AUTHZ_ERR"):
        super().__init__(message, code)
