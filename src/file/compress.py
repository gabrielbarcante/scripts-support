from pathlib import Path
from typing import List
import shutil
import zipfile
import tempfile

from ..error import InvalidFileTypeError


def write_zip_archive(filename_zip: str, save_path: str, path_dir_arquivos: str | None = None, lista_arquivos: List[str] = []) -> Path:
    """
    Create a ZIP archive from a directory or list of files.
    
    Args:
        filename_zip (str): Name for the ZIP file (with or without .zip extension).
        save_path (str): Directory path where the ZIP file will be saved.
        path_dir_arquivos (str | None): Path to directory containing files to compress. Defaults to None.
        lista_arquivos (List[str]): List of file paths to include in the archive. Defaults to [].
        
    Returns:
        Path: Full path to the created ZIP file.
        
    Raises:
        TypeError: If neither path_dir_arquivos nor lista_arquivos is specified.
        FileExistsError: If the ZIP file already exists at the destination.
        FileNotFoundError: If any file in lista_arquivos doesn't exist.
        
    Examples:
        >>> zip_path = write_zip_archive("backup.zip", "./output", path_dir_arquivos="./data")
        >>> print(zip_path)
        /path/to/output/backup.zip
        
        >>> zip_path = write_zip_archive("docs", "./output", lista_arquivos=["file1.txt", "file2.pdf"])
        >>> print(zip_path)
        /path/to/output/docs.zip
    """
    if not path_dir_arquivos and not lista_arquivos:
        raise TypeError(
            "Must specify one of the arguments: path_dir_arquivos or lista_arquivos")

    if Path(filename_zip).suffix.lower() != '.zip':
        filename_zip = f"{filename_zip.lstrip('.')}.zip"

    path_save_zip = Path(save_path) / filename_zip
    if path_save_zip.is_file():
        raise FileExistsError(
            f"The ZIP file {path_save_zip} already exists. Choose another name or path.")

    if path_dir_arquivos:
        list_files_zip = list(Path(path_dir_arquivos).iterdir())
    else:
        list_files_zip = []
        for arquivo in lista_arquivos:
            path_arquivo = Path(arquivo)
            if not path_arquivo.is_file():
                raise FileNotFoundError(f"File not found: {arquivo}")
            list_files_zip.append(path_arquivo)
        
    with zipfile.ZipFile(path_save_zip, mode="w") as f:
        for path_arquivo in list_files_zip:
            f.write(path_arquivo, arcname=path_arquivo.name)

    return path_save_zip.resolve()


def unarchive_compress_file(zip_file_path: str, dir_extract_path: str) -> Path:
    """
    Extract a compressed file to a temp folder within the specified directory.
    
    Args:
        zip_file_path (str): Path to the compressed file (ZIP, TAR, GZ, BZ2, XZ, etc.).
        dir_extract_path (str): Directory where files will be extracted.
        
    Returns:
        Path: Path to the temporary directory containing extracted content.
        
    Raises:
        FileNotFoundError: If the compressed file doesn't exist.
        InvalidFileTypeError: If the file is not a valid compressed archive format.
        NotADirectoryError: If dir_extract_path is not a valid directory.
        
    Examples:
        >>> extract_path = unarchive_compress_file("backup.zip", "./extracted")
        >>> print(extract_path)
        /path/to/extracted/tmp_abc123
        
        >>> extract_path = unarchive_compress_file("data.tar.gz", "./output")
        >>> print(extract_path)
        /path/to/output/tmp_xyz789
    """
    path_zip_file = Path(zip_file_path).resolve()
    if not path_zip_file.is_file():
        raise FileNotFoundError(f"The file '{path_zip_file}' was not found")
    
    unarchive_formats = get_unarchive_formats()
    if path_zip_file.suffix.lower() not in unarchive_formats:
        raise InvalidFileTypeError(f"The file '{path_zip_file}' is not a valid compressed archive")
    
    path_dir_extract = Path(dir_extract_path).resolve()
    if not path_dir_extract.is_dir():
        raise NotADirectoryError(f"The directory '{path_dir_extract}' was not found")

    path_dir_extract = Path(tempfile.mkdtemp(dir=path_dir_extract.as_posix()))

    if zipfile.is_zipfile(path_zip_file):
        with zipfile.ZipFile(path_zip_file, "r") as f:
            f.extractall(path=path_dir_extract)
    else:
        shutil.unpack_archive(path_zip_file, path_dir_extract)

    return path_dir_extract


def get_unarchive_formats() -> List[str]:
    """
    Get a list of supported archive formats for extraction.

    Returns:
        List[str]: List of supported archive file extensions (e.g., ['.zip', '.tar', '.gz']).
        
    Examples:
        >>> formats = get_unarchive_formats()
        >>> print(formats)
        ['.zip', '.tar', '.gz', '.bz2', '.xz']
    """
    unarchive_formats = []
    for i in shutil.get_unpack_formats():
        unarchive_formats += i[1]

    return unarchive_formats
