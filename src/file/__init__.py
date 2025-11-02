"""
Custom file module.
Imports and exports all custom file handling functions.
"""

from .base_64 import convert_file_to_base64, save_file_base_64
from .compress import write_zip_archive, unarchive_compress_file, get_unarchive_formats
from .temporary import generate_random_filename, generate_temp_file
from .operations import find_object_in_directory, check_object_exists, delete_object, delete_objects_in_directory, separate_file_extension, wait_for_files
from .plain_text import escrever_lista_txt, ler_arquivo_txt
from .image import is_image_file, get_image_extensions, save_images_as_pdf

__all__ = [
    "convert_file_to_base64",
    "save_file_base_64",
    "write_zip_archive",
    "unarchive_compress_file",
    "get_unarchive_formats",
    "generate_random_filename",
    "generate_temp_file",
    "find_object_in_directory",
    "check_object_exists",
    "delete_object",
    "delete_objects_in_directory",
    "separate_file_extension",
    "wait_for_files",
    "escrever_lista_txt",
    "ler_arquivo_txt",
    "is_image_file",
    "get_image_extensions",
    "save_images_as_pdf"
]
