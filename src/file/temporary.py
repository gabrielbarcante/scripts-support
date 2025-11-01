from pathlib import Path
from typing import Literal
import uuid
import secrets
import string
import tempfile
import random
from datetime import datetime


def generate_random_filename(extension: str, method: Literal["uuid", "secure", "timestamp", "simple"] = "uuid", length: int = 16, prefix: str = "", suffix: str = "") -> str:
    """
    Generate a random filename with specified parameters.

    Args:
        extension (str): File extension without the dot.
        method (RANDOM_FILENAME_METHOD): Method to generate random name. 
            Options: 'uuid', 'secure', 'timestamp', 'simple'. Defaults to "uuid".
        length (int): Length of the random name (for 'secure' and 'simple' methods). Defaults to 16.
        prefix (str): Optional prefix for the filename. Defaults to "".
        suffix (str): Optional suffix for the filename. Defaults to "".

    Returns:
        str: Complete filename with extension.

    Raises:
        ValueError: If method is not one of the supported options.

    Examples:
        >>> generate_random_filename("pdf")
        'a1b2c3d4-e5f6-7890-abcd-ef1234567890.pdf'
        
        >>> generate_random_filename("zip", "secure", 12)
        'aB3xY7mN9qR2.zip'
        
        >>> generate_random_filename("json", "timestamp")
        '20251031_143025_abc123.json'
    """
    if extension.startswith("."):
        extension = extension[1:]

    if method == "uuid":
        random_name = str(uuid.uuid4())

    elif method == "secure":
        chars = string.ascii_letters + string.digits
        random_name = "".join(secrets.choice(chars) for _ in range(length))

    elif method in ["timestamp", "simple"]:
        chars = string.ascii_lowercase + string.digits
        random_name = "".join(random.choices(chars, k=length))

        if method == "timestamp":
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            random_name = f"{timestamp}_{random_name}"

    else:
        raise ValueError(
            "Method must be 'uuid', 'secure', 'timestamp' or 'simple'")

    complete_name = f"{prefix}{random_name}{suffix}.{extension}"
    return complete_name


def generate_temp_file(filename: str | None = None, extension: str | None = None, prefix: str = "") -> Path:
    """
    Generate a temporary file with optional custom name or extension.

    Args:
        filename (str | None): Complete filename. If None, a random name will be generated. Defaults to None.
        extension (str | None): File extension without dot. Required if filename is None. Defaults to None.
        prefix (str): Prefix for the temporary file. Defaults to "".

    Returns:
        Path: Absolute path to the created temporary file.

    Raises:
        ValueError: If both filename and extension are None.

    Examples:
        >>> temp_file = generate_temp_file(extension="csv", prefix="test_")
        >>> print(temp_file)
        /tmp/test_abc123.csv
    """
    if filename is None:
        if extension is None:
            raise ValueError(
                "Either filename or extension must be provided to generate a temporary file."
            )
        filename = generate_random_filename(extension, prefix=prefix)

    with tempfile.NamedTemporaryFile(suffix=f".{extension}", prefix=prefix, delete=False) as tmp:
        return Path(tmp.name).resolve()