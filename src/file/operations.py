from pathlib import Path
import re
import shutil
from datetime import datetime
import time
from typing import Literal, List
from unidecode import unidecode

from ..date_time import get_now


OBJECT_TYPES = Literal["file", "directory"]

def check_object_exists(full_path: str | Path) -> bool:
    """
    Check if a file or directory exists at the given path.
    
    Args:
        full_path: Path to check for existence
    
    Returns:
        True if the path exists, False otherwise
    """
    if not isinstance(full_path, Path):
        full_path = Path(full_path)

    return full_path.exists()


def delete_object(object_path: str | Path, object_name: str | None = None) -> bool:
    """
    Delete a file or directory at the specified path.
    
    Args:
        object_path: Base path or full path to the object
        object_name: Optional name of object to append to object_path
    
    Returns:
        True if deletion was successful or object doesn't exist, False otherwise
    """
    if not isinstance(object_path, Path):
        object_path = Path(object_path)

    if object_name:
        object_path = object_path / object_name

    if check_object_exists(object_path):
        try:
            if object_path.is_dir():
                shutil.rmtree(object_path)
            else:
                object_path.unlink(missing_ok=True)
        except PermissionError:
            print(f"No permission to delete '{object_path}'")
            return False
        except OSError as e:
            print(f"Error deleting '{object_path}': {e}")
            return False
    else:
        return True # Object doesn't exist, considered deleted

    return not check_object_exists(object_path)


def delete_objects_in_directory(directory_path: str | Path, object_type: OBJECT_TYPES | None = None, file_extension: str | None = None) -> None:
    """
    Delete all objects within a directory, optionally filtered by type and extension.
    
    Args:
        directory_path: Path to the directory
        object_type: Type of object to delete ("file", "directory", or None for both)
        file_extension: File extension filter (only used when object_type is "file")
    
    Raises:
        ValueError: If object_type is not valid
    """
    if not isinstance(directory_path, Path):
        directory_path = Path(directory_path)

    valid_types = list(OBJECT_TYPES.__args__) + [None]
    if object_type not in valid_types:
        raise ValueError(f"Invalid object type: {object_type}. Must be: {valid_types}")
    
    for path_obj in directory_path.iterdir():
        if (not object_type == "file") and path_obj.is_file():
            continue
        elif (not object_type == "directory") and path_obj.is_dir():
            continue
        elif object_type == "file" and file_extension and file_extension != path_obj.suffix:
            continue
        
        delete_object(path_obj)

    return


def move_objects(source_path: str | Path, destination_path: str | Path, source_object: str | None = None, destination_object: str | None = None, overwrite: bool = False) -> Path:
    """
    Move a file or directory from source to destination.
    
    Args:
        source_path: Base source path
        destination_path: Base destination path
        source_object: Optional object name to append to source_path
        destination_object: Optional object name for destination (defaults to source name)
        overwrite: If True, overwrite destination if it exists
    
    Returns:
        Path to the moved object
    
    Raises:
        AssertionError: If source path doesn't exist
        FileExistsError: If destination exists and overwrite is False
    """
    if not isinstance(source_path, Path):
        source_path = Path(source_path)

    if not isinstance(destination_path, Path):
        destination_path = Path(destination_path)

    if source_object:
        source_path = source_path / source_object

    if not destination_object:
        destination_object = source_path.name
    destination_path = destination_path / destination_object

    assert check_object_exists(source_path), f"Source path '{source_path}' does not exist."
    if check_object_exists(destination_path):
        if overwrite:
            delete_object(destination_path)
        else:
            raise FileExistsError(f"Destination path '{destination_path}' already exists.")

    return Path(shutil.move(source_path, destination_path))


def create_directory(new_directory_path: str | Path, new_directory: str | None = None, create_parent: bool = True) -> Path:
    """
    Create a new directory at the specified path.
    
    Args:
        new_directory_path: Base path for the new directory
        new_directory: Optional directory name to append to new_directory_path
        create_parent: If True, create parent directories as needed
    
    Returns:
        Path to the created directory
    
    Raises:
        FileExistsError: If a file exists at the target path
        PermissionError: If lacking permissions to create directory
        OSError: If an OS error occurs during creation
    """
    if not isinstance(new_directory_path, Path):
        new_directory_path = Path(new_directory_path)

    if new_directory:
        new_directory_path = new_directory_path / new_directory

    if new_directory_path.is_file():
        raise FileExistsError(f"Path '{new_directory_path}' already exists as a file.")

    if new_directory_path.is_dir():
        return new_directory_path

    try:
        new_directory_path.mkdir(parents=create_parent, exist_ok=True)
    except PermissionError:
        raise PermissionError(f"No permission to create '{new_directory_path}'")
    except OSError as e:
        raise OSError(f"Error creating directory: {e}")

    return new_directory_path


def separate_file_extension(file_path: str | Path) -> tuple[str, str]:
    """
    Separate a file path into stem (name without extension) and extension.
    
    Args:
        file_path: Path to the file
    
    Returns:
        Tuple of (stem, extension)
    """
    if not isinstance(file_path, Path):
        file_path = Path(file_path)

    return file_path.stem, file_path.suffix


def get_last_n_path_levels(path, n):
    """
    Get the last n levels of a path.
    
    Args:
        path: Path to extract levels from
        n: Number of levels to return
    
    Returns:
        Path containing the last n levels
    """
    if not isinstance(path, Path):
        path = Path(path)

    return Path(*path.parts[-n:])


def find_object_in_directory(directory_path: str | Path, name_pattern: str | None = None, file_extension: str | None = None, first_file: bool = False, regex: bool = False, case_sensitive: bool = False, replace_special_characters: bool = False, search_subfolders: bool = False) -> list[Path]:
    """
    Find files or directories in a directory based on various filters.
    
    Args:
        directory_path: Path to the directory to search
        name_pattern: Pattern to match in object names
        file_extension: File extension to filter by (e.g., ".txt")
        first_file: If True, return only the first found object
        regex: If True, treat name_pattern as a regex pattern
        case_sensitive: If True, perform case-sensitive matching
        replace_special_characters: If True, remove accents/special characters before matching
        search_subfolders: If True, search recursively in subfolders
    
    Returns:
        List of Path objects matching the criteria
    
    Raises:
        FileNotFoundError: If the directory is empty
        ValueError: If searching for directories without providing name_pattern
    """
    found_objects_list = []

    if not isinstance(directory_path, Path):
        directory_path = Path(directory_path)
    
    if search_subfolders:
        objects_list = list(directory_path.rglob("*"))
    else:
        objects_list = list(directory_path.glob("*"))

    if len(objects_list) == 0:
        raise FileNotFoundError(f"Directory '{directory_path}' is empty.")
    
    if first_file:
        return [objects_list[0]]

    if not file_extension:
        search_folder = True
        if not name_pattern:
            raise ValueError("If 'file_extension' is not provided, 'name_pattern' must be provided to search for folders.")
    else:
        search_folder = False
        file_extension = file_extension.strip().lower()
        file_extension = file_extension if file_extension.startswith('.') else f".{file_extension}"

    if name_pattern:
        name_pattern = name_pattern.strip()
        if not case_sensitive: name_pattern = name_pattern.lower()
        if replace_special_characters: name_pattern = unidecode(name_pattern)

    for path_obj in objects_list:

        name, extension = separate_file_extension(path_obj)

        if not name_pattern:
            if extension == file_extension:
                found_objects_list.append(path_obj)
            continue
        
        name = name.strip()
        if not case_sensitive: name = name.lower()
        if replace_special_characters: name = unidecode(name)
        extension = extension.strip().lower()
        
        if (not regex and name_pattern in name) or (regex and re.search(name_pattern, name)):
            if (search_folder and path_obj.is_dir()) or ((not search_folder) and path_obj.is_file() and extension == file_extension):
                found_objects_list.append(path_obj)

    return found_objects_list


def wait_for_files(directory_path: str | Path, files_list: list[str | Path], timeout: int = 60, time_between_checks: int = 5, regex: bool = False, case_sensitive: bool = False) -> List[Path]:
    """
    Wait for specific files to appear in a directory within a timeout period.
    
    Args:
        directory_path: Path to the directory to monitor
        files_list: List of file names or paths to wait for
        timeout: Maximum time to wait in seconds (default: 60)
        time_between_checks: Time between directory checks in seconds (default: 5)
        regex: If True, treat file names as regex patterns
        case_sensitive: If True, perform case-sensitive matching
    
    Returns:
        List of Path objects for the found files
    
    Raises:
        ValueError: If unable to get current datetime
        TimeoutError: If timeout is reached before all files are found
    """
    if not isinstance(directory_path, Path):
        directory_path = Path(directory_path)

    found_files_list = []
    start_time = get_now(as_string=False)
    if not isinstance(start_time, datetime):
        raise ValueError("Error getting current date and time.")
    
    pending_files_list = files_list.copy()
    while True:
        found_indices = []
        for i, file in enumerate(pending_files_list):
            if not isinstance(file, Path):
                file = Path(file)
                files_list[i] = file
            
            found_files = find_object_in_directory(
                directory_path,
                name_pattern=file.stem,
                file_extension=file.suffix,
                regex=regex,
                case_sensitive=case_sensitive
            )

            if found_files:
                found_files_list.append(found_files[0])
                found_indices.append(i)
        
        if found_indices:
            for index in sorted(found_indices, reverse=True):
                del pending_files_list[index]
        
        if len(found_files_list) == len(files_list):
            return found_files_list
        
        else:
            current_time = get_now(as_string=False)
            if not isinstance(current_time, datetime):
                raise ValueError("Error getting current date and time.")
            
            if (current_time - start_time).total_seconds() > timeout:
                found_files_list = [f.name for f in found_files_list]
                missing_files = [f if isinstance(f, str) else f.name for f in files_list if (f if isinstance(f, str) else f.name) not in found_files_list]
                raise TimeoutError(f"Timeout reached. The following files were not found: {missing_files}")

            else:
                time.sleep(time_between_checks)
