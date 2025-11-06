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
        length (int): Length of the random name (for 'secure', 'simple', and 'timestamp' methods). Defaults to 16.
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
        chars = string.ascii_letters + string.digits
        random_name = "".join(random.choices(chars, k=length))

        if method == "timestamp":
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            random_name = f"{timestamp}_{random_name}"

    else:
        raise ValueError(
            "Method must be 'uuid', 'secure', 'timestamp' or 'simple'")

    complete_name = f"{prefix}{random_name}{suffix}.{extension}"
    return complete_name


def generate_temp_file(filename: str | None = None, extension: str | None = None, unique: bool = True) -> Path:
    """
    Generate a temporary file with optional custom name or extension.

    Args:
        filename (str | None): Complete filename. If None, a random name will be generated. Defaults to None.
        extension (str | None): File extension without dot. Required if filename is None. Defaults to None.
        unique (bool): If True and filename exists, append a suffix to make it unique. If False, raise FileExistsError. Defaults to True.

    Returns:
        Path: Absolute path to the created temporary file.

    Raises:
        ValueError: If both filename and extension are None, or if filename has no extension and extension is not provided.
        FileExistsError: If unique is False and the file already exists.

    Examples:
        >>> temp_file = generate_temp_file(extension="csv")
        >>> print(temp_file)
        /tmp/abc123.csv
        
        >>> temp_file = generate_temp_file(filename="myfile.txt", unique=True)
        >>> print(temp_file)
        /tmp/myfile.txt  # or /tmp/myfile_1.txt if it exists
    """
    temp_dir = Path(tempfile.gettempdir())
    
    if not filename:
        if not extension:
            raise ValueError("Either filename or extension must be provided to generate a temporary file.")
        filename = generate_random_filename(extension=extension)
    else:
        filename_path = Path(filename)
        # Validate that filename has an extension
        if not filename_path.suffix:
            if not extension:
                raise ValueError("Filename must have an extension or extension parameter must be provided.")
            
            filename = f"{filename_path.stem}.{extension}"

    temp_path = temp_dir / filename
    
    # Try to create the file atomically
    try:
        temp_path.touch(exist_ok=False)
        return temp_path.resolve()
    except FileExistsError:
        if not unique:
            raise FileExistsError(f"Temporary file '{temp_path}' already exists.")
        
        # Generate unique filename with counter suffix
        base_path = Path(filename)
        stem = base_path.stem
        ext = base_path.suffix
        counter = 1
        
        while counter < 1000:  # Safety limit
            new_filename = f"{stem}_{counter}{ext}"
            temp_path = temp_dir / new_filename
            
            try:
                temp_path.touch(exist_ok=False)
                return temp_path.resolve()
            except FileExistsError:
                counter += 1
                continue
        
        raise RuntimeError(f"Failed to create unique temporary file after 1000 attempts.")
