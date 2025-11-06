from pathlib import Path
from typing import List, Any

from .operations import separate_file_extension
from .temporary import generate_random_filename


def write_list_to_txt(full_file_path: str | Path, text_list: List[Any], new_line: bool = True, encoding: str = "utf-8") -> Path:
    """
    Write a list of items to a text file.

    Args:
        full_file_path: Full path to the output file or directory. If a directory is provided,
                       a random filename will be generated.
        text_list: List of items to write to the file. Non-string items will be converted to strings.
        new_line: If True, adds a newline character after each item. Defaults to True.
        encoding: Character encoding to use when writing the file. Defaults to 'utf-8'.

    Returns:
        Path: Path object of the created file.

    Raises:
        FileExistsError: If the file already exists.
        ValueError: If the file extension is not '.txt'.
    """
    if not isinstance(full_file_path, Path):
        full_file_path = Path(full_file_path)

    if full_file_path.exists() and full_file_path.is_file():
        raise FileExistsError(f"The file '{full_file_path}' already exists.")
    
    if full_file_path.is_dir():
        full_file_path = full_file_path / generate_random_filename(extension=".txt", method="uuid")

    name, extension = separate_file_extension(full_file_path)
    if extension.lower() != ".txt":
        raise ValueError("The file extension must be '.txt'.")

    text_list = [str(item) if not isinstance(item, str) else item for item in text_list]

    if new_line:
        text_list = list(map(lambda x: f"{x}\n", text_list))

    with open(full_file_path, mode="w", encoding=encoding) as f:
        f.writelines(text_list)

    return full_file_path


def read_txt_file(full_file_path: str | Path, encoding: str = "utf-8", create_if_not_exists: bool = False) -> str:
    """
    Read the contents of a text file.

    Args:
        full_file_path: Full path to the file to read. Can be a string or Path object.
        encoding: Character encoding to use when reading the file. Defaults to 'utf-8'.
        create_if_not_exists: If True, creates an empty file if it doesn't exist and returns
                             an empty string. Defaults to False.

    Returns:
        str: String containing the file contents. Returns an empty string if the file is empty
             or was just created.

    Raises:
        FileNotFoundError: If the file doesn't exist and create_if_not_exists is False.
    """
    mode = "a+" if create_if_not_exists else "r"
    with open(full_file_path, mode=mode, encoding=encoding) as f:
        f.seek(0)
        data = f.read()
    
    return data
