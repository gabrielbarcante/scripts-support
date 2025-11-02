from pathlib import Path
from typing import Sequence, Tuple
from PIL import Image

from .operations import separate_file_extension, delete_object


def get_image_extensions() -> list[str]:
    """
    Get a list of all supported image file extensions.

    Returns:
        List of supported image extensions (without dots)
    """
    return [".jpg", ".jpeg", ".png", ".gif", ".bmp", ".tiff", ".tif", ".webp", ".ico", ".svg"]


def is_image_file(file_path: str | Path) -> Tuple[bool, str]:
    """
    Check if a file is an image based on its byte signature (magic numbers).

    Args:
        file_path: Path to the file to check

    Returns:
        Tuple containing:
            - bool: True if the file is a valid image, False otherwise
            - str: Image format type (e.g., 'jpg', 'png') if valid, error message otherwise
    """
    if not isinstance(file_path, Path):
        file_path = Path(file_path)

    if not file_path.exists() or not file_path.is_file():
        return False, "File does not exist or is not a file"

    # Image magic numbers (file signatures)
    image_signatures = {
        b"\xFF\xD8\xFF": "jpg",  # JPEG
        b"\x89\x50\x4E\x47\x0D\x0A\x1A\x0A": "png",  # PNG
        b"\x47\x49\x46\x38\x37\x61": "gif",  # GIF87a
        b"\x47\x49\x46\x38\x39\x61": "gif",  # GIF89a
        b"\x42\x4D": "bmp",  # BMP
        b"\x49\x49\x2A\x00": "tiff",  # TIFF (little-endian)
        b"\x4D\x4D\x00\x2A": "tiff",  # TIFF (big-endian)
        b"\x52\x49\x46\x46": "webp",  # WEBP (needs further validation)
        b"\x00\x00\x01\x00": "ico",  # ICO
    }

    try:
        with open(file_path, "rb") as f:
            file_header = f.read(12)  # Read first 12 bytes

        # Check against known signatures
        for signature, format_type in image_signatures.items():
            if file_header.startswith(signature):
                # Special check for WEBP (RIFF header followed by WEBP)
                if format_type == "webp":
                    return_check = file_header[8:12] == b"WEBP"
                    return return_check, format_type if return_check else ""
                
                return True, format_type

        return False, "Type not recognized"

    except Exception:
        return False, "Error reading file"



def save_images_as_pdf(image_files: Sequence[str | Path], pdf_file_path: str | Path, delete_originals: bool = True, ignore_invalid_files: bool = False) -> Path:
    """
    Convert a sequence of image files into a single multi-page PDF document.
    
    The images are converted to RGB color mode and combined into a PDF where each
    image becomes a separate page. Invalid files can either raise errors or be
    silently skipped based on the ignore_invalid_files parameter.
    
    Args:
        image_files: Sequence of file paths to image files. Supported formats include
            jpg, jpeg, png, gif, bmp, tiff, tif, webp, ico, and svg.
        pdf_file_path: Destination path for the output PDF file. Must have a .pdf extension
            and must not already exist.
        delete_originals: If True, successfully processed image files are deleted after
            conversion. Default is True.
        ignore_invalid_files: If True, invalid or unreadable files are skipped silently.
            If False, any invalid file raises an exception. Default is False.
    
    Returns:
        Path object pointing to the created PDF file.
    
    Raises:
        ValueError: If pdf_file_path doesn't have a .pdf extension, or if no valid
            images are found after filtering.
        FileExistsError: If a file already exists at pdf_file_path.
        IsADirectoryError: If pdf_file_path points to an existing directory.
        FileNotFoundError: If any input image file doesn't exist and ignore_invalid_files
            is False.
        TypeError: If any input file is not a valid image format or doesn't match its
            extension, and ignore_invalid_files is False.
    
    Note:
        - At least one valid image file is required to create the PDF.
        - Images are processed in the order provided in image_files.
        - When delete_originals is True, files are deleted immediately after being
          successfully converted, not at the end of the entire process.
    """
    if not isinstance(pdf_file_path, Path):
        pdf_file_path = Path(pdf_file_path)
    
    if pdf_file_path.suffix.lower() != ".pdf":
        raise ValueError("The output file must have a .pdf extension")
    
    if pdf_file_path.is_file():
        raise FileExistsError(f"The output file already exists: {pdf_file_path}")
    
    if pdf_file_path.is_dir():
        raise IsADirectoryError(f"The output path is a directory: {pdf_file_path}")

    image_files_list = list(image_files).copy()
    
    invalid_files_list = []
    for i, image_file in enumerate(image_files_list):
        if not isinstance(image_file, Path):
            image_file = Path(image_file)
            image_files_list[i] = image_file

        if not image_file.is_file():
            if not ignore_invalid_files:
                raise FileNotFoundError(f"The following input file does not exist: {image_file}")
            else:
                invalid_files_list.append(image_file)
                continue
        
        file_name, file_extension = separate_file_extension(image_file)
        if file_extension.lower() not in get_image_extensions():
            if not ignore_invalid_files:
                raise TypeError(f"The following file type {file_extension} is not valid: {image_file}")
            else:
                invalid_files_list.append(image_file)
                continue
        
        if not is_image_file(image_file)[0]:
            if not ignore_invalid_files:
                raise TypeError(f"The following input file is not a valid image: {image_file}")
            else:
                invalid_files_list.append(image_file)
                continue
    
    for invalid_file in invalid_files_list:
        image_files_list.remove(invalid_file)

    images_list = []
    for image_file in image_files_list:
        img = Image.open(image_file)
        img = img.convert("RGB")
        images_list.append(img)

        if delete_originals:
            delete_object(image_file)

    img = images_list.pop(0)
    img.save(pdf_file_path, save_all=True, append_images=images_list)

    return pdf_file_path
