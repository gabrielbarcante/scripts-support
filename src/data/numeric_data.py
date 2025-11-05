import re
from typing import Literal


def convert_string_to_float(number_text: str, raise_exception: bool = True, return_on_error: float | None = 0.0) -> float | None:
    """
    Convert a string representation of a number to a float.
    
    Handles various number formats by attempting conversions in this order:
    1. Standard Python float format: "123.45" → 123.45
    2. Brazilian format with thousand separators: "1.234,56" → 1234.56
    3. American format with thousand separators: "1,234.56" → 1234.56
    
    Note: Ambiguous formats without thousand separators (e.g., "1234" or "1234.56") are handled by standard Python float conversion in step 1.
    
    Args:
        number_text: String representation of the number to convert
        raise_exception: If True, raises exception on error; if False, returns return_on_error value
        return_on_error: Value to return when conversion fails and raise_exception is False
    
    Returns:
        The converted float value, or return_on_error if conversion fails and raise_exception is False
    
    Raises:
        ValueError: If input is not a string, is empty, or cannot be converted to float (when raise_exception is True)
    
    Examples:
        >>> convert_string_to_float("123.45")
        123.45
        >>> convert_string_to_float("1.234,56")  # Brazilian format
        1234.56
        >>> convert_string_to_float("1,234.56")  # American format
        1234.56
        >>> convert_string_to_float("invalid", raise_exception=False, return_on_error=None)
        None
    """
    try:
        if not isinstance(number_text, str):
            raise ValueError("Input must be a string")
        
        number_text = number_text.strip()
        if number_text == "":
            raise ValueError("Input string is empty")

        try:
            return float(number_text)
        except Exception:
            pass

        # Brazilian format: 1.234,56
        number = re.search(r"^[\d{3}\.]+\,?\d*$", number_text)
        if number:
            return float(number.group().strip().replace(".", "").replace(",", "."))

        # American format: 1,234.56
        number = re.search(r"^[\d{3},]+\.?\d*$", number_text)
        if number:
            return float(number.group().strip().replace(",", ""))

        raise ValueError(f"Cannot convert '{number_text}' to float")
    
    except Exception as e:
        if raise_exception:
            raise e
    
        return return_on_error


def convert_number_to_currency(value: int | float, symbol: str = "R$", decimal_places: int = 2, decimal_separator: Literal[",", "."] = ",") -> str:
    """
    Convert a number to a formatted currency string.
    
    Args:
        value: The numeric value to convert
        symbol: Currency symbol (default: "R$")
        decimal_places: Number of decimal places (default: 2)
        decimal_separator: Decimal separator - "," for Brazilian format or "." for American format (default: ",")
    
    Returns:
        Formatted currency string (e.g., "R$ 1.234,56" or "R$ 1,234.56")
    
    Raises:
        ValueError: If value is not a number (int or float)
    
    Examples:
        >>> convert_number_to_currency(1234.56)
        'R$ 1.234,56'
        >>> convert_number_to_currency(1234.56, symbol="$", decimal_separator=".")
        '$ 1,234.56'
        >>> convert_number_to_currency(-1234.56)
        '-R$ 1.234,56'
    """
    if not isinstance(value, (int, float)):
        raise ValueError("Input must be a number")
    
    absolute_value = abs(value)
    sign = "-" if value < 0 else ""
    
    formatted_value = f"{absolute_value:.{decimal_places}f}"

    if decimal_separator == ",":
        thousand_separator = "."
    else:
        thousand_separator = ","

    parts = formatted_value.split(".")
    integer_part = parts[0]
    decimal_part = parts[1] if len(parts) > 1 else "0"*decimal_places
    
    formatted_integer = ""
    for i, digit in enumerate(reversed(integer_part)):
        if i > 0 and i % 3 == 0:
            formatted_integer = thousand_separator + formatted_integer
        formatted_integer = digit + formatted_integer

    return f"{sign}{symbol} {formatted_integer}{decimal_separator if decimal_part else ''}{decimal_part}"
