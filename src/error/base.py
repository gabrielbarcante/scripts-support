"""
Base exception module.
Defines the base class for all custom exceptions.
"""


class BaseError(Exception):
    """
    Base class for all custom exceptions.
    
    Args:
        message (str): Error message describing what went wrong.
        code (str, optional): Error code for categorization.
    """

    def __init__(self, message="Application error", code=None):
        self.message = message
        self.code = code
        super().__init__(self.message)

    def __str__(self):
        if self.code:
            return f"[{self.code}] {self.message}"
        return self.message
