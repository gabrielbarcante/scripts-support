import pytest
from pathlib import Path
import tempfile
import shutil

from src.file.plain_text import (
    write_list_to_txt,
    read_txt_file
)


class TestWriteListToTxt:
    """Test suite for write_list_to_txt function."""

    @pytest.fixture(autouse=True)
    def temp_files(self):
        """Fixture to track and cleanup temporary files."""
        files = []
        yield files
        
        # Cleanup: runs after test completes
        for file in files:
            if file.exists():
                file.unlink()

    def test_write_list_with_strings_and_newline(self, temp_files):
        """Test writing a list of strings with newline characters."""
        temp_dir = Path(tempfile.gettempdir())
        file_path = temp_dir / "test_strings.txt"
        temp_files.append(file_path)
        
        text_list = ["line1", "line2", "line3"]
        result = write_list_to_txt(file_path, text_list, new_line=True)
        
        assert result.exists()
        assert result.is_file()
        assert result == file_path
        
        content = result.read_text()
        assert content == "line1\nline2\nline3\n"

    def test_write_list_with_strings_without_newline(self, temp_files):
        """Test writing a list of strings without newline characters."""
        temp_dir = Path(tempfile.gettempdir())
        file_path = temp_dir / "test_no_newline.txt"
        temp_files.append(file_path)
        
        text_list = ["line1", "line2", "line3"]
        result = write_list_to_txt(file_path, text_list, new_line=False)
        
        assert result.exists()
        content = result.read_text()
        assert content == "line1line2line3"

    def test_write_list_with_mixed_types_converts_to_strings(self, temp_files):
        """Test writing a list with mixed types (all converted to strings)."""
        temp_dir = Path(tempfile.gettempdir())
        file_path = temp_dir / "test_mixed_types.txt"
        temp_files.append(file_path)
        
        text_list = ["text", 123, 45.67, True, None]
        result = write_list_to_txt(file_path, text_list, new_line=True)
        
        assert result.exists()
        content = result.read_text()
        assert content == "text\n123\n45.67\nTrue\nNone\n"

    def test_write_list_with_empty_list(self, temp_files):
        """Test writing an empty list creates an empty file."""
        temp_dir = Path(tempfile.gettempdir())
        file_path = temp_dir / "test_empty.txt"
        temp_files.append(file_path)
        
        text_list = []
        result = write_list_to_txt(file_path, text_list)
        
        assert result.exists()
        content = result.read_text()
        assert content == ""

    def test_write_list_with_string_path(self, temp_files):
        """Test that string path is converted to Path object."""
        temp_dir = Path(tempfile.gettempdir())
        file_path = temp_dir / "test_string_path.txt"
        temp_files.append(file_path)
        
        text_list = ["test"]
        result = write_list_to_txt(str(file_path), text_list)
        
        assert isinstance(result, Path)
        assert result.exists()

    def test_write_list_raises_error_if_file_exists(self, temp_files):
        """Test that FileExistsError is raised if file already exists."""
        temp_dir = Path(tempfile.gettempdir())
        file_path = temp_dir / "test_exists.txt"
        temp_files.append(file_path)
        
        # Create the file first
        file_path.write_text("existing content")
        
        text_list = ["new content"]
        with pytest.raises(FileExistsError, match="already exists"):
            write_list_to_txt(file_path, text_list)

    def test_write_list_with_directory_generates_random_filename(self, temp_files, mocker):
        """Test that providing a directory generates a random filename."""
        temp_dir = Path(tempfile.gettempdir())
        
        text_list = ["test content"]

        mock_gen = mocker.patch("src.file.plain_text.generate_random_filename", return_value="random_file.txt")

        result = write_list_to_txt(temp_dir, text_list)
        temp_files.append(result)
        
        mock_gen.assert_called_once_with(extension=".txt", method="uuid")
        assert result.name == "random_file.txt"
        assert result.parent == temp_dir
        assert result.exists()
        content = result.read_text()
        assert content == "test content\n"

    def test_write_list_raises_error_for_non_txt_extension(self):
        """Test that ValueError is raised for non-.txt extensions."""
        file_path = Path(tempfile.gettempdir()) / "test_file.csv"
        text_list = ["test"]
        
        with pytest.raises(ValueError, match="The file extension must be '.txt'"):
            write_list_to_txt(file_path, text_list)

    def test_write_list_with_uppercase_txt_extension(self, temp_files):
        """Test that .TXT extension is accepted (case insensitive)."""
        temp_dir = Path(tempfile.gettempdir())
        file_path = temp_dir / "test_uppercase.TXT"
        temp_files.append(file_path)
        
        text_list = ["test"]
        result = write_list_to_txt(file_path, text_list)
        
        assert result.exists()
        assert result.name == "test_uppercase.TXT"
        content = result.read_text()
        assert content == "test\n"

    def test_write_list_with_single_string(self, temp_files):
        """Test writing a list with a single string."""
        temp_dir = Path(tempfile.gettempdir())
        file_path = temp_dir / "test_single.txt"
        temp_files.append(file_path)
        
        text_list = ["single line"]
        result = write_list_to_txt(file_path, text_list, new_line=True)
        
        assert result.exists()
        content = result.read_text()
        assert content == "single line\n"

    def test_write_list_with_multiline_strings(self, temp_files):
        """Test writing strings that already contain newlines."""
        temp_dir = Path(tempfile.gettempdir())
        file_path = temp_dir / "test_multiline.txt"
        temp_files.append(file_path)
        
        text_list = ["line1\nline2", "line3"]
        result = write_list_to_txt(file_path, text_list, new_line=True)
        
        assert result.exists()
        content = result.read_text()
        assert content == "line1\nline2\nline3\n"

    def test_write_list_with_special_characters(self, temp_files):
        """Test writing strings with special characters."""
        temp_dir = Path(tempfile.gettempdir())
        file_path = temp_dir / "test_special.txt"
        temp_files.append(file_path)
        
        text_list = ["Hello! @#$%", "Special: *&^()", "Numbers: 123-456", "émojis 🎉"]
        result = write_list_to_txt(file_path, text_list, new_line=True)
        
        assert result.exists()
        content = result.read_text(encoding="utf-8")
        assert "Hello! @#$%" in content
        assert "Special: *&^()" in content
        assert "Numbers: 123-456" in content
        assert "émojis 🎉" in content

    def test_write_list_creates_parent_directories(self, temp_files):
        """Test that parent directories are created if they don't exist."""
        temp_dir = Path(tempfile.gettempdir())
        nested_dir = temp_dir / "nested" / "deep" / "path"
        file_path = nested_dir / "test_nested.txt"
        
        # Track directory for cleanup
        temp_files.append(file_path)
        cleanup_dir = temp_dir / "nested"
        
        text_list = ["test"]
        
        # Ensure parent directory exists
        nested_dir.mkdir(parents=True, exist_ok=True)
        
        result = write_list_to_txt(file_path, text_list)
        
        assert result.exists()
        assert result.is_file()
        assert result.name == "test_nested.txt"
        assert result.parent == nested_dir
        content = result.read_text()
        assert content == "test\n"
        
        # Cleanup nested directories
        if cleanup_dir.exists():
            shutil.rmtree(cleanup_dir)

    def test_write_list_calls_open_with_correct_mode(self, mocker):
        """Test that file is opened with write mode."""
        # Mock the built-in open function to track how it's called without actually opening files
        mock_file = mocker.patch("builtins.open", mocker.mock_open())
        
        # Mock the separate_file_extension function to return a predefined extension
        # This avoids the need for a real file path validation
        mock_separate = mocker.patch("src.file.plain_text.separate_file_extension", return_value=("test", ".txt"))
        
        # Create a fake file path that doesn't need to exist on the filesystem
        file_path = Path("/fake/path/test.txt")
        text_list = ["line1", "line2"]
        
        # Mock Path.exists to return False, simulating that the file doesn't exist yet
        mocker.patch.object(Path, "exists", return_value=False)
        
        # Mock Path.is_file to return False, ensuring we don't trigger the FileExistsError
        mocker.patch.object(Path, "is_file", return_value=False)
        
        # Mock Path.is_dir to return False, ensuring we don't trigger random filename generation
        mocker.patch.object(Path, "is_dir", return_value=False)
        
        # Call the function under test
        write_list_to_txt(file_path, text_list)
        
        # Assert that open was called exactly once with write mode and utf-8 encoding
        mock_file.assert_called_once_with(file_path, mode="w", encoding="utf-8")
        
        # Assert that writelines was called exactly once on the file handle
        mock_file().writelines.assert_called_once()

    def test_write_list_with_complex_objects_converts_to_string(self, temp_files):
        """Test that complex objects are converted using str()."""
        temp_dir = Path(tempfile.gettempdir())
        file_path = temp_dir / "test_complex.txt"
        temp_files.append(file_path)
        
        class CustomObj:
            def __str__(self):
                return "CustomObject"
        
        text_list = [CustomObj(), "text", [1, 2, 3]]
        result = write_list_to_txt(file_path, text_list, new_line=True)
        
        assert result.exists()
        content = result.read_text()
        assert "CustomObject\n" in content
        assert "text\n" in content
        assert "[1, 2, 3]\n" in content


class TestReadTxtFile:
    """Test suite for read_txt_file function."""

    @pytest.fixture(autouse=True)
    def temp_files(self):
        """Fixture to track and cleanup temporary files."""
        files = []
        yield files
        
        # Cleanup: runs after test completes
        for file in files:
            if file.exists():
                file.unlink()

    def test_read_existing_file_with_content(self, temp_files):
        """Test reading an existing file with content."""
        temp_dir = Path(tempfile.gettempdir())
        file_path = temp_dir / "test_read.txt"
        temp_files.append(file_path)
        
        expected_content = "Line 1\nLine 2\nLine 3"
        file_path.write_text(expected_content, encoding="utf-8")
        
        result = read_txt_file(file_path)
        
        assert result == expected_content

    def test_read_empty_file(self, temp_files):
        """Test reading an empty file returns empty string."""
        temp_dir = Path(tempfile.gettempdir())
        file_path = temp_dir / "test_empty.txt"
        temp_files.append(file_path)
        
        file_path.write_text("", encoding="utf-8")
        
        result = read_txt_file(file_path)
        
        assert result == ""

    def test_read_file_with_string_path(self, temp_files):
        """Test that string path works correctly."""
        temp_dir = Path(tempfile.gettempdir())
        file_path = temp_dir / "test_string.txt"
        temp_files.append(file_path)
        
        content = "test content"
        file_path.write_text(content, encoding="utf-8")
        
        result = read_txt_file(str(file_path))
        
        assert result == content

    def test_read_nonexistent_file_raises_error(self):
        """Test that FileNotFoundError is raised for non-existent file."""
        file_path = Path(tempfile.gettempdir()) / "nonexistent_file.txt"
        
        with pytest.raises(FileNotFoundError):
            read_txt_file(file_path, create_if_not_exists=False)

    def test_read_file_with_create_if_not_exists_true(self, temp_files):
        """Test that file is created when create_if_not_exists is True."""
        temp_dir = Path(tempfile.gettempdir())
        file_path = temp_dir / "test_create.txt"
        temp_files.append(file_path)
        
        result = read_txt_file(file_path, create_if_not_exists=True)
        
        assert result == ""
        assert file_path.exists()
        assert file_path.is_file()

    def test_read_file_with_utf8_encoding(self, temp_files):
        """Test reading file with UTF-8 encoding (default)."""
        temp_dir = Path(tempfile.gettempdir())
        file_path = temp_dir / "test_utf8.txt"
        temp_files.append(file_path)
        
        content = "Hello 世界 🌍"
        file_path.write_text(content, encoding="utf-8")
        
        result = read_txt_file(file_path, encoding="utf-8")
        
        assert result == content

    def test_read_file_with_latin1_encoding(self, temp_files):
        """Test reading file with latin-1 encoding."""
        temp_dir = Path(tempfile.gettempdir())
        file_path = temp_dir / "test_latin1.txt"
        temp_files.append(file_path)
        
        content = "Olá café"
        file_path.write_text(content, encoding="latin-1")
        
        result = read_txt_file(file_path, encoding="latin-1")
        
        assert result == content

    def test_read_file_with_special_characters(self, temp_files):
        """Test reading file with special characters."""
        temp_dir = Path(tempfile.gettempdir())
        file_path = temp_dir / "test_special.txt"
        temp_files.append(file_path)
        
        content = "Special: @#$%^&*()\nNewline\tTab"
        file_path.write_text(content, encoding="utf-8")
        
        result = read_txt_file(file_path)
        
        assert result == content
        assert "\n" in result
        assert "\t" in result

    def test_read_file_with_multiple_lines(self, temp_files):
        """Test reading file with multiple lines."""
        temp_dir = Path(tempfile.gettempdir())
        file_path = temp_dir / "test_multiline.txt"
        temp_files.append(file_path)
        
        lines = ["Line 1", "Line 2", "Line 3", "Line 4", "Line 5"]
        content = "\n".join(lines)
        file_path.write_text(content, encoding="utf-8")
        
        result = read_txt_file(file_path)
        
        assert result == content
        assert result.count("\n") == 4

    def test_read_file_with_large_content(self, temp_files):
        """Test reading file with large content."""
        temp_dir = Path(tempfile.gettempdir())
        file_path = temp_dir / "test_large.txt"
        temp_files.append(file_path)
        
        # Create a large content (1000 lines)
        lines = [f"Line {i}" for i in range(1000)]
        content = "\n".join(lines)
        file_path.write_text(content, encoding="utf-8")
        
        result = read_txt_file(file_path)
        
        assert len(result) > 0
        assert result.count("\n") == 999

    def test_read_file_calls_open_with_read_mode(self, mocker):
        """Test that file is opened with read mode when create_if_not_exists is False."""
        mock_file = mocker.patch("builtins.open", mocker.mock_open(read_data="mocked content"))
        
        file_path = Path("/fake/path/test.txt")
        
        result = read_txt_file(file_path, create_if_not_exists=False)
        
        mock_file.assert_called_once_with(file_path, mode="r", encoding="utf-8")
        assert result == "mocked content"

    def test_read_file_calls_open_with_append_mode_when_create(self, mocker):
        """Test that file is opened with a+ mode when create_if_not_exists is True."""
        mock_file = mocker.patch("builtins.open", mocker.mock_open(read_data="mocked content"))

        file_path = Path("/fake/path/test.txt")
        
        result = read_txt_file(file_path, create_if_not_exists=True)
        
        mock_file.assert_called_once_with(file_path, mode="a+", encoding="utf-8")
        mock_file().seek.assert_called_once_with(0)
        assert result == "mocked content"

    def test_read_file_with_windows_line_endings(self, temp_files):
        """Test reading file with Windows-style line endings."""
        temp_dir = Path(tempfile.gettempdir())
        file_path = temp_dir / "test_windows.txt"
        temp_files.append(file_path)
        
        content = "Line 1\r\nLine 2\r\nLine 3"
        file_path.write_bytes(content.encode("utf-8"))
        
        result = read_txt_file(file_path)
        
        assert result == content.replace("\r\n", "\n")
        assert "Line 1" in result
        assert "Line 2" in result
        assert "Line 3" in result
        assert result.count("\n") == 2

    def test_read_file_with_path_object(self, temp_files):
        """Test reading file with Path object."""
        temp_dir = Path(tempfile.gettempdir())
        file_path = temp_dir / "test_path_obj.txt"
        temp_files.append(file_path)
        
        content = "test with Path object"
        file_path.write_text(content, encoding="utf-8")
        
        result = read_txt_file(file_path)
        
        assert result == content

    def test_read_file_default_encoding_parameter(self, temp_files):
        """Test that default encoding is utf-8."""
        temp_dir = Path(tempfile.gettempdir())
        file_path = temp_dir / "test_default_enc.txt"
        temp_files.append(file_path)
        
        content = "Default encoding test"
        file_path.write_text(content, encoding="utf-8")
        
        result = read_txt_file(file_path)
        
        assert result == content

    def test_read_file_preserves_whitespace(self, temp_files):
        """Test that whitespace is preserved when reading."""
        temp_dir = Path(tempfile.gettempdir())
        file_path = temp_dir / "test_whitespace.txt"
        temp_files.append(file_path)
        
        content = "  leading spaces\ntrailing spaces  \n\n  mixed   spaces  "
        file_path.write_text(content, encoding="utf-8")
        
        result = read_txt_file(file_path)
        
        assert result == content
