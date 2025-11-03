import re

def remove_punctuation(text: str) -> str:
    """
    Remove punctuation from a string.
    
    Args:
        text: The string from which punctuation will be removed.
    
    Returns:
        The string without punctuation.
    
    Examples:
        >>> remove_punctuation("Hello, world!")
        'Hello world'
        >>> remove_punctuation("Python is great.")
        'Python is great'
    """
    if not isinstance(text, str):
        raise ValueError("Input must be a string")
    
    if not text:
        raise ValueError("Input string is empty")
    
    return re.sub(r"[^\w\s]", "", text)


def return_only_letters_numbers(text: str) -> str:
    """
    Return only letters and numbers from a string, removing all other characters.
    
    Args:
        text: The string to process.
    
    Returns:
        A string containing only alphanumeric characters (no spaces or special characters).
    
    Examples:
        >>> return_only_letters_numbers("Hello, World! 123")
        'HelloWorld123'
        >>> return_only_letters_numbers("Python_3.11")
        'Python311'
    """
    if not isinstance(text, str):
        raise ValueError("Input must be a string")
    
    if not text:
        raise ValueError("Input string is empty")
    
    return re.sub(r"[\W_]", "", text)



