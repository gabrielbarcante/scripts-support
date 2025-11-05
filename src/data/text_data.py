import re

def remove_special_characters(text: str, keep_unicode: bool = True, normalize_whitespace: bool = True, remove_whitespace: bool = False) -> str:
    """
    Remove punctuation and special characters from a string, keeping only alphanumeric characters and spaces.
    
    Args:
        text: The string from which special characters will be removed.
        keep_unicode: If True, keeps Unicode word characters (letters, digits from any language). 
                      If False, keeps only ASCII letters and digits. Default is True.
        normalize_whitespace: If True, collapses multiple consecutive spaces into one.
                              Ignored if remove_whitespace is True. Default is True.
        remove_whitespace: If True, removes all whitespace from the string.
                           If False, preserves spaces and applies normalization. Default is False.
    
    Returns:
        str: The cleaned string with special characters removed.
    
    Raises:
        ValueError: If input is not a string.
    
    Examples:
        >>> remove_special_characters("Hello, World!")
        "Hello World"
        >>> remove_special_characters("Hello___World", keep_unicode=True)
        "HelloWorld"
        >>> remove_special_characters("Price: $100", remove_whitespace=True)
        "Price100"
    """
    if normalize_whitespace and remove_whitespace:
        raise ValueError("normalize_whitespace cannot be True when remove_whitespace is True")
    
    if not isinstance(text, str):
        raise ValueError("Input must be a string")

    if keep_unicode:
        # Remove punctuation and special chars, but keep Unicode letters/digits
        text = re.sub(r"[^\w\s]|_", "", text)
    else:
        # Only keep ASCII letters, digits, and spaces
        text = re.sub(r"[^a-zA-Z0-9\s]", "", text)

    if remove_whitespace:
        text = re.sub(r"\s+", "", text)
    else:
        if normalize_whitespace:
            text = re.sub(r"\s+", " ", text)
        text = text.strip()

    return text

