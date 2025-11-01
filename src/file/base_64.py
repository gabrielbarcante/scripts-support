from pathlib import Path
import base64

from .temporary import generate_random_filename


def convert_file_to_base64(file_path: str, encoding="utf-8") -> str:
    """
    Convert a file to base64 encoded string.
    
    Args:
        file_path (str): Path to the file to convert.
        encoding (str): Text encoding to use. Defaults to 'utf-8'.
        
    Returns:
        str: Base64 encoded file content.
        
    Raises:
        ValueError: If file path is invalid.
        FileNotFoundError: If file doesn't exist.
        
    Examples:
        >>> base64_str = convert_file_to_base64("document.pdf")
        >>> print(base64_str[:50])
        'JVBERi0xLjQKJeLjz9MKMyAwIG9iago8PC9UeXBlIC9QYWdl...'
    """
    if not file_path:
        raise ValueError(
            "File path for base64 conversion is not valid")

    full_file_path = Path(file_path).resolve()

    if not full_file_path.is_file():
        raise FileNotFoundError(
            "Path for base64 conversion is not a file")

    with open(full_file_path, "rb") as f:
        content = base64.b64encode(f.read()).decode(encoding)

    return content


def save_file_base_64(base_64_content: str, save_path: str, filename: str | None = None, extension: str | None = None) -> Path:
    """
    Save base64 encoded content to a file.
    
    Args:
        base_64_content (str): Base64 encoded file content.
        save_path (str): Directory path to save the file.
        filename (str | None): Complete filename with extension. Defaults to None.
        extension (str | None): File extension without dot, used if filename is None. Defaults to None.
        
    Returns:
        Path: Full path to the saved file.
        
    Raises:
        ValueError: If neither filename nor extension is provided, or if extensions don't match.
        
    Examples:
        >>> saved_path = save_file_base_64(base64_content, "./output", extension="pdf")
        >>> print(saved_path)
        /path/to/output/abc123.pdf
    """

    if isinstance(extension, str):
        extension = extension.lstrip(".")

    if filename is None:
        if not extension:
            raise ValueError(
                "Either filename or extension must be provided to save the file."
            )
        filename = generate_random_filename(extension)

    full_file_path = Path(save_path) / filename
    
    if full_file_path.suffix == "":
        if extension is None:
            raise ValueError(
                "Either filename with extension or extension must be provided to save the file."
            )
        else:
            full_file_path = full_file_path.with_suffix(f".{extension}")

    elif extension is not None and full_file_path.suffix != f".{extension}":
        raise ValueError(
            "Filename extension does not match the provided extension."
        )

    with open(full_file_path, "wb") as f:
        f.write(base64.b64decode(base_64_content))

    return full_file_path.resolve()

