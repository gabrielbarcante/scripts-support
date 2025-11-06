import pytest
from pathlib import Path
import tempfile
import zipfile
import shutil
import tarfile
import gzip

from src.file.compress import (
    write_zip_archive,
    unarchive_compress_file,
    get_unarchive_formats
)
from src.error import InvalidFileTypeError


class TestWriteZipArchive:
    """Test suite for write_zip_archive function."""

    @pytest.fixture(autouse=True)
    def temp_resources(self):
        """Fixture to track and cleanup temporary files and directories."""
        files = []
        dirs = []
        yield files, dirs
        
        # Cleanup: runs after test completes
        for file in files:
            if file.exists():
                file.unlink()
        for directory in dirs:
            if directory.exists():
                shutil.rmtree(directory)

    def test_create_zip_from_directory(self, temp_resources):
        """Test creating a ZIP archive from a directory."""
        files, dirs = temp_resources
        
        # Create temp directory with test files
        temp_dir = Path(tempfile.mkdtemp())
        dirs.append(temp_dir)
        
        test_file1 = temp_dir / "file1.txt"
        test_file2 = temp_dir / "file2.txt"
        test_file1.write_text("Content 1")
        test_file2.write_text("Content 2")
        
        # Create output directory
        output_dir = Path(tempfile.mkdtemp())
        dirs.append(output_dir)
        
        # Create ZIP archive
        result = write_zip_archive("test.zip", str(output_dir), path_dir_files=str(temp_dir))
        files.append(result)
        
        assert result.exists()
        assert result.is_file()
        assert result.suffix == ".zip"
        assert result.name == "test.zip"
        
        # Verify ZIP contents
        with zipfile.ZipFile(result, 'r') as zip_file:
            names = zip_file.namelist()
            assert "file1.txt" in names
            assert "file2.txt" in names
            assert len(names) == 2

    def test_create_zip_from_file_list(self, temp_resources):
        """Test creating a ZIP archive from a list of files."""
        files, dirs = temp_resources
        
        # Create temp files
        temp_dir = Path(tempfile.mkdtemp())
        dirs.append(temp_dir)
        
        file1 = temp_dir / "doc1.txt"
        file2 = temp_dir / "doc2.txt"
        file3 = temp_dir / "doc3.txt"
        file1.write_text("Document 1")
        file2.write_text("Document 2")
        file3.write_text("Document 3")
        
        # Create output directory
        output_dir = Path(tempfile.mkdtemp())
        dirs.append(output_dir)
        
        # Create ZIP from file list
        result = write_zip_archive(
            "documents.zip",
            str(output_dir),
            list_files=[str(file1), str(file2), str(file3)]
        )
        files.append(result)
        
        assert result.exists()
        assert result.is_file()
        assert result.name == "documents.zip"
        assert result.suffix == ".zip"
                
        # Verify ZIP contents
        with zipfile.ZipFile(result, 'r') as zip_file:
            names = zip_file.namelist()
            assert "doc1.txt" in names
            assert "doc2.txt" in names
            assert "doc3.txt" in names
            assert len(names) == 3

    def test_create_zip_with_filename_without_extension(self, temp_resources):
        """Test creating a ZIP archive when filename doesn't have .zip extension."""
        files, dirs = temp_resources
        
        # Create temp directory with test files
        temp_dir = Path(tempfile.mkdtemp())
        dirs.append(temp_dir)
        
        test_file = temp_dir / "file.txt"
        test_file.write_text("Content")
        
        # Create output directory
        output_dir = Path(tempfile.mkdtemp())
        dirs.append(output_dir)
        
        # Create ZIP without extension
        result = write_zip_archive("myarchive", str(output_dir), path_dir_files=str(temp_dir))
        files.append(result)
        
        assert result.exists()
        assert result.is_file()
        assert result.name == "myarchive.zip"
        assert result.suffix == ".zip"

    def test_create_zip_raises_error_if_file_exists(self, temp_resources):
        """Test that FileExistsError is raised if ZIP file already exists."""
        files, dirs = temp_resources
        
        # Create temp directory with test files
        temp_dir = Path(tempfile.mkdtemp())
        dirs.append(temp_dir)
        
        test_file = temp_dir / "file.txt"
        test_file.write_text("Content")
        
        # Create output directory
        output_dir = Path(tempfile.mkdtemp())
        dirs.append(output_dir)
        
        # Create existing ZIP file
        existing_zip = output_dir / "existing.zip"
        existing_zip.write_text("dummy")
        files.append(existing_zip)
        
        # Try to create ZIP with same name
        with pytest.raises(FileExistsError, match="already exists"):
            write_zip_archive("existing.zip", str(output_dir), path_dir_files=str(temp_dir))

    def test_create_zip_raises_error_without_source(self):
        """Test that TypeError is raised when neither directory nor file list is provided."""
        output_dir = Path(tempfile.gettempdir())
        
        with pytest.raises(TypeError, match="Must specify one of the arguments"):
            write_zip_archive("test.zip", str(output_dir))

    def test_create_zip_raises_error_for_nonexistent_file(self, temp_resources):
        """Test that FileNotFoundError is raised for non-existent file in list."""
        files, dirs = temp_resources
        
        output_dir = Path(tempfile.mkdtemp())
        dirs.append(output_dir)
        
        with pytest.raises(FileNotFoundError, match="File not found"):
            write_zip_archive(
                "test.zip",
                str(output_dir),
                list_files=["/nonexistent/file.txt"]
            )

    def test_create_zip_from_empty_directory(self, temp_resources):
        """Test creating a ZIP archive from an empty directory."""
        files, dirs = temp_resources
        
        # Create empty temp directory
        temp_dir = Path(tempfile.mkdtemp())
        dirs.append(temp_dir)
        
        # Create output directory
        output_dir = Path(tempfile.mkdtemp())
        dirs.append(output_dir)
        
        # Create ZIP archive
        result = write_zip_archive("empty.zip", str(output_dir), path_dir_files=str(temp_dir))
        files.append(result)
        
        assert result.exists()
        assert result.is_file()
        assert result.name == "empty.zip"
        assert result.suffix == ".zip"
        
        # Verify ZIP is empty
        with zipfile.ZipFile(result, 'r') as zip_file:
            names = zip_file.namelist()
            assert len(names) == 0

    def test_create_zip_from_empty_file_list(self, temp_resources):
        """Test that creating a ZIP archive from an empty file list raises TypeError."""
        files, dirs = temp_resources
        
        output_dir = Path(tempfile.mkdtemp())
        dirs.append(output_dir)
        
        # Empty list should raise TypeError since both parameters are effectively empty
        with pytest.raises(TypeError, match="Must specify one of the arguments"):
            write_zip_archive("empty.zip", str(output_dir), list_files=[])

    def test_create_zip_returns_resolved_path(self, temp_resources):
        """Test that function returns resolved absolute path."""
        files, dirs = temp_resources
        
        temp_dir = Path(tempfile.mkdtemp())
        dirs.append(temp_dir)
        
        test_file = temp_dir / "file.txt"
        test_file.write_text("Content")
        
        output_dir = Path(tempfile.mkdtemp())
        dirs.append(output_dir)
        
        result = write_zip_archive("test.zip", str(output_dir), path_dir_files=str(temp_dir))
        files.append(result)
        
        assert result.is_absolute()
        assert str(result) == str(result.resolve())

    def test_create_zip_with_various_file_types(self, temp_resources):
        """Test creating a ZIP archive with different file types."""
        files, dirs = temp_resources
        
        temp_dir = Path(tempfile.mkdtemp())
        dirs.append(temp_dir)
        
        # Create different file types
        (temp_dir / "text.txt").write_text("Text content")
        (temp_dir / "data.json").write_text('{"key": "value"}')
        (temp_dir / "script.py").write_text("print('Hello')")
        (temp_dir / "style.css").write_text("body { margin: 0; }")
        
        output_dir = Path(tempfile.mkdtemp())
        dirs.append(output_dir)
        
        result = write_zip_archive("mixed.zip", str(output_dir), path_dir_files=str(temp_dir))
        files.append(result)
        
        assert result.exists()
        assert result.is_file()
        assert result.name == "mixed.zip"
        assert result.suffix == ".zip"
        
        with zipfile.ZipFile(result, 'r') as zip_file:
            names = zip_file.namelist()
            assert "text.txt" in names
            assert "data.json" in names
            assert "script.py" in names
            assert "style.css" in names

    def test_create_zip_with_filename_with_trailing_dot(self, temp_resources):
        """Test creating a ZIP archive when filename has trailing dot."""
        files, dirs = temp_resources
        
        temp_dir = Path(tempfile.mkdtemp())
        dirs.append(temp_dir)
        
        test_file = temp_dir / "file.txt"
        test_file.write_text("Content")
        
        output_dir = Path(tempfile.mkdtemp())
        dirs.append(output_dir)
        
        result = write_zip_archive("archive.", str(output_dir), path_dir_files=str(temp_dir))
        files.append(result)
        
        assert result.exists()
        assert result.is_file()
        assert result.name == "archive.zip"
        assert result.suffix == ".zip"

    def test_create_zip_preserves_file_content(self, temp_resources):
        """Test that file content is preserved in ZIP archive."""
        files, dirs = temp_resources
        
        temp_dir = Path(tempfile.mkdtemp())
        dirs.append(temp_dir)
        
        test_file = temp_dir / "test.txt"
        expected_content = "This is test content with special chars: @#$%\nLine 2"
        test_file.write_text(expected_content, encoding="utf-8")
        
        output_dir = Path(tempfile.mkdtemp())
        dirs.append(output_dir)
        
        result = write_zip_archive("content.zip", str(output_dir), path_dir_files=str(temp_dir))
        files.append(result)
        
        # Extract and verify content
        with zipfile.ZipFile(result, "r") as zip_file:
            content = zip_file.read("test.txt").decode("utf-8")
            # Normalize line endings for comparison
            assert content.replace("\r\n", "\n") == expected_content

    def test_create_zip_with_single_file_in_list(self, temp_resources):
        """Test creating a ZIP archive with a single file in list."""
        files, dirs = temp_resources
        
        temp_dir = Path(tempfile.mkdtemp())
        dirs.append(temp_dir)
        
        test_file = temp_dir / "single.txt"
        test_file.write_text("Single file content")
        
        output_dir = Path(tempfile.mkdtemp())
        dirs.append(output_dir)
        
        result = write_zip_archive(
            "single.zip",
            str(output_dir),
            list_files=[str(test_file)]
        )
        files.append(result)
        
        assert result.exists()
        assert result.is_file()
        assert result.name == "single.zip"
        assert result.suffix == ".zip"

        with zipfile.ZipFile(result, "r") as zip_file:
            names = zip_file.namelist()
            assert len(names) == 1
            assert "single.txt" in names

    def test_create_zip_with_uppercase_extension(self, temp_resources):
        """Test creating a ZIP archive with uppercase .ZIP extension."""
        files, dirs = temp_resources
        
        temp_dir = Path(tempfile.mkdtemp())
        dirs.append(temp_dir)
        
        test_file = temp_dir / "file.txt"
        test_file.write_text("Content")
        
        output_dir = Path(tempfile.mkdtemp())
        dirs.append(output_dir)
        
        result = write_zip_archive("test.ZIP", str(output_dir), path_dir_files=str(temp_dir))
        files.append(result)
        
        assert result.exists()
        assert result.is_file()
        assert result.name == "test.ZIP"
        assert result.suffix == ".ZIP"

    def test_create_zip_returns_path_object(self, temp_resources):
        """Test creating a ZIP that returns a Path object."""
        files, dirs = temp_resources
        
        # Create temp directory with test files
        temp_dir = Path(tempfile.mkdtemp())
        dirs.append(temp_dir)
        
        test_file = temp_dir / "file.txt"
        test_file.write_text("Content")
        
        # Create output directory
        output_dir = Path(tempfile.mkdtemp())
        dirs.append(output_dir)
        
        # Create ZIP archive
        result = write_zip_archive("test.zip", str(output_dir), path_dir_files=str(temp_dir))
        files.append(result)
        
        assert isinstance(result, Path)


class TestUnarchiveCompressFile:
    """Test suite for unarchive_compress_file function."""

    @pytest.fixture(autouse=True)
    def temp_resources(self):
        """Fixture to track and cleanup temporary files and directories."""
        files = []
        dirs = []
        yield files, dirs
        
        # Cleanup: runs after test completes
        for file in files:
            if file.exists():
                file.unlink()
        for directory in dirs:
            if directory.exists():
                shutil.rmtree(directory)

    def test_unarchive_zip_file(self, temp_resources):
        """Test extracting a ZIP archive."""
        files, dirs = temp_resources
        
        # Create a ZIP file with test content
        temp_dir = Path(tempfile.mkdtemp())
        dirs.append(temp_dir)
        
        zip_path = temp_dir / "test.zip"
        files.append(zip_path)
        
        with zipfile.ZipFile(zip_path, "w") as zip_file:
            zip_file.writestr("file1.txt", "Content 1")
            zip_file.writestr("file2.txt", "Content 2")
        
        # Create extraction directory
        extract_dir = Path(tempfile.mkdtemp())
        dirs.append(extract_dir)
        
        # Extract the ZIP
        result = unarchive_compress_file(str(zip_path), str(extract_dir))
        dirs.append(result)
        
        assert result.exists()
        assert result.is_dir()
        assert result.parent == extract_dir
        
        # Verify extracted files
        assert (result / "file1.txt").exists()
        assert (result / "file2.txt").exists()
        assert (result / "file1.txt").read_text() == "Content 1"
        assert (result / "file2.txt").read_text() == "Content 2"

    def test_unarchive_tar_file(self, temp_resources):
        """Test extracting a TAR archive."""
        files, dirs = temp_resources
        
        # Create a TAR file with test content
        temp_dir = Path(tempfile.mkdtemp())
        dirs.append(temp_dir)
        
        # Create test files
        test_file1 = temp_dir / "test1.txt"
        test_file2 = temp_dir / "test2.txt"
        test_file1.write_text("Test content 1")
        test_file2.write_text("Test content 2")
        
        # Create TAR archive
        tar_path = temp_dir / "archive.tar"
        files.append(tar_path)
        
        with tarfile.open(tar_path, "w") as tar:
            tar.add(test_file1, arcname=test_file1.name)
            tar.add(test_file2, arcname=test_file2.name)
        
        # Create extraction directory
        extract_dir = Path(tempfile.mkdtemp())
        dirs.append(extract_dir)
        
        # Extract the TAR
        result = unarchive_compress_file(str(tar_path), str(extract_dir))
        dirs.append(result)
        
        assert result.exists()
        assert result.is_dir()
        assert result.parent == extract_dir        

        # Verify extracted files
        assert (result / "test1.txt").exists()
        assert (result / "test2.txt").exists()
        assert (result / "test1.txt").read_text() == "Test content 1"
        assert (result / "test2.txt").read_text() == "Test content 2"

    def test_unarchive_raises_error_for_nonexistent_file(self):
        """Test that FileNotFoundError is raised for non-existent archive."""
        extract_dir = Path(tempfile.gettempdir())
        
        with pytest.raises(FileNotFoundError, match="was not found"):
            unarchive_compress_file("/nonexistent/archive.zip", str(extract_dir))

    def test_unarchive_raises_error_for_invalid_format(self, temp_resources):
        """Test that InvalidFileTypeError is raised for invalid archive format."""
        files, dirs = temp_resources
        
        # Create a non-archive file
        temp_dir = Path(tempfile.mkdtemp())
        dirs.append(temp_dir)
        
        invalid_file = temp_dir / "not_archive.txt"
        invalid_file.write_text("This is not an archive")
        files.append(invalid_file)
        
        extract_dir = Path(tempfile.mkdtemp())
        dirs.append(extract_dir)
        
        with pytest.raises(InvalidFileTypeError, match="not a valid compressed archive"):
            unarchive_compress_file(str(invalid_file), str(extract_dir))

    def test_unarchive_raises_error_for_invalid_directory(self, temp_resources):
        """Test that NotADirectoryError is raised for invalid extraction path."""
        files, dirs = temp_resources
        
        # Create a ZIP file
        temp_dir = Path(tempfile.mkdtemp())
        dirs.append(temp_dir)
        
        zip_path = temp_dir / "test.zip"
        files.append(zip_path)
        
        with zipfile.ZipFile(zip_path, "w") as zip_file:
            zip_file.writestr("file.txt", "Content")
        
        # Try to extract to non-existent directory
        with pytest.raises(NotADirectoryError, match="was not found"):
            unarchive_compress_file(str(zip_path), "/nonexistent/directory")

    def test_unarchive_creates_temp_directory(self, temp_resources):
        """Test that extraction creates a temporary directory within specified path."""
        files, dirs = temp_resources
        
        # Create a ZIP file
        temp_dir = Path(tempfile.mkdtemp())
        dirs.append(temp_dir)
        
        zip_path = temp_dir / "test.zip"
        files.append(zip_path)
        
        with zipfile.ZipFile(zip_path, "w") as zip_file:
            zip_file.writestr("file.txt", "Content")
        
        # Create extraction directory
        extract_dir = Path(tempfile.mkdtemp())
        dirs.append(extract_dir)
        
        # Extract
        result = unarchive_compress_file(str(zip_path), str(extract_dir))
        dirs.append(result)
        
        # Verify temp directory is within extract_dir
        assert result.parent == extract_dir
        assert result.exists()
        assert result.is_dir()

    def test_unarchive_empty_zip(self, temp_resources):
        """Test extracting an empty ZIP archive."""
        files, dirs = temp_resources
        
        # Create an empty ZIP file
        temp_dir = Path(tempfile.mkdtemp())
        dirs.append(temp_dir)
        
        zip_path = temp_dir / "empty.zip"
        files.append(zip_path)
        
        with zipfile.ZipFile(zip_path, "w") as zip_file:
            pass  # Empty ZIP
        
        # Create extraction directory
        extract_dir = Path(tempfile.mkdtemp())
        dirs.append(extract_dir)
        
        # Extract the empty ZIP
        result = unarchive_compress_file(str(zip_path), str(extract_dir))
        dirs.append(result)
        
        assert result.parent == extract_dir
        assert result.exists()
        assert result.is_dir()
        
        # Verify directory is empty
        assert len(list(result.iterdir())) == 0

    def test_unarchive_preserves_file_content(self, temp_resources):
        """Test that file content is preserved during extraction."""
        files, dirs = temp_resources
        
        # Create a ZIP file with specific content
        temp_dir = Path(tempfile.mkdtemp())
        dirs.append(temp_dir)
        
        zip_path = temp_dir / "content.zip"
        files.append(zip_path)
        
        expected_content = "Special content: @#$%\nMultiple lines\nWith UTF-8: 日本語"
        with zipfile.ZipFile(zip_path, "w") as zip_file:
            zip_file.writestr("test.txt", expected_content)
        
        # Create extraction directory
        extract_dir = Path(tempfile.mkdtemp())
        dirs.append(extract_dir)
        
        # Extract
        result = unarchive_compress_file(str(zip_path), str(extract_dir))
        dirs.append(result)
        
        # Verify content
        extracted_file = result / "test.txt"
        assert extracted_file.exists()
        assert extracted_file.read_text(encoding="utf-8") == expected_content

    def test_unarchive_multiple_files(self, temp_resources):
        """Test extracting archive with multiple files."""
        files, dirs = temp_resources
        
        # Create a ZIP file with multiple files
        temp_dir = Path(tempfile.mkdtemp())
        dirs.append(temp_dir)
        
        zip_path = temp_dir / "multi.zip"
        files.append(zip_path)
        
        with zipfile.ZipFile(zip_path, "w") as zip_file:
            for i in range(10):
                zip_file.writestr(f"file{i}.txt", f"Content {i}")
        
        # Create extraction directory
        extract_dir = Path(tempfile.mkdtemp())
        dirs.append(extract_dir)
        
        # Extract
        result = unarchive_compress_file(str(zip_path), str(extract_dir))
        dirs.append(result)
        
        # Verify all files extracted
        for i in range(10):
            extracted_file = result / f"file{i}.txt"
            assert extracted_file.exists()
            assert extracted_file.read_text() == f"Content {i}"

    def test_unarchive_with_invalid_file_extension(self, temp_resources):
        """Test that files with unsupported extensions raise InvalidFileTypeError."""
        files, dirs = temp_resources
        
        # Create a file with unsupported extension
        temp_dir = Path(tempfile.mkdtemp())
        dirs.append(temp_dir)
        
        invalid_file = temp_dir / "file.docx"
        invalid_file.write_bytes(b"Not an archive")
        files.append(invalid_file)
        
        extract_dir = Path(tempfile.mkdtemp())
        dirs.append(extract_dir)
        
        with pytest.raises(InvalidFileTypeError, match="not a valid compressed archive"):
            unarchive_compress_file(str(invalid_file), str(extract_dir))

    def test_unarchive_returns_path_object(self, temp_resources):
        """Test that function returns a Path object."""
        files, dirs = temp_resources
        
        # Create a ZIP file
        temp_dir = Path(tempfile.mkdtemp())
        dirs.append(temp_dir)
        
        zip_path = temp_dir / "test.zip"
        files.append(zip_path)
        
        with zipfile.ZipFile(zip_path, "w") as zip_file:
            zip_file.writestr("file.txt", "Content")
        
        # Create extraction directory
        extract_dir = Path(tempfile.mkdtemp())
        dirs.append(extract_dir)
        
        # Extract
        result = unarchive_compress_file(str(zip_path), str(extract_dir))
        dirs.append(result)
        
        assert isinstance(result, Path)


class TestGetUnarchiveFormats:
    """Test suite for get_unarchive_formats function."""

    def test_returns_list(self):
        """Test that function returns a list."""
        result = get_unarchive_formats()
        assert isinstance(result, list)

    def test_returns_common_formats_zip_or_tar(self):
        """Test that function returns common archive formats."""
        result = get_unarchive_formats()

        assert ".zip" in result
        assert ".tar" in result

    def test_returns_non_empty_list(self):
        """Test that function returns a non-empty list."""
        result = get_unarchive_formats()
        assert len(result) > 0

    def test_all_items_are_strings(self):
        """Test that all items in the list are strings."""
        result = get_unarchive_formats()
        assert all(isinstance(fmt, str) for fmt in result)

    def test_formats_start_with_dot(self):
        """Test that all format extensions start with a dot."""
        result = get_unarchive_formats()
        assert all(fmt.startswith(".") for fmt in result)

    def test_returns_same_result_on_multiple_calls(self):
        """Test that function returns consistent results."""
        result1 = get_unarchive_formats()
        result2 = get_unarchive_formats()
        
        assert result1 == result2

    def test_no_duplicate_formats(self):
        """Test that there are no duplicate formats in the list."""
        result = get_unarchive_formats()
        assert len(result) == len(set(result))

    def test_mocked_shutil_get_unpack_formats(self, mocker):
        """Test that function correctly processes shutil.get_unpack_formats output."""
        
        mock_formats = [
            ('zip', ['.zip'], "ZIP file"),
            ('tar', ['.tar'], "uncompressed tar file"),
            ('gztar', ['.tar.gz', '.tgz'], "gzip'ed tar-file"),
        ]
        mocker.patch("src.file.compress.shutil.get_unpack_formats", return_value=mock_formats)
        
        result = get_unarchive_formats()
        
        assert ".zip" in result
        assert ".tar" in result
        assert ".tar.gz" in result
        assert ".tgz" in result
        assert len(result) == 4
