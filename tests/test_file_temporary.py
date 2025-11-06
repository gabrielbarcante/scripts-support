import pytest
from pathlib import Path
from unittest.mock import patch, MagicMock
import tempfile
import uuid
from datetime import datetime

from src.file.temporary import (
    generate_random_filename,
    generate_temp_file
)


class TestGenerateRandomFilename:
    """Test cases for generate_random_filename function"""

    def test_generate_uuid_method_default(self):
        """Test generate_random_filename with default uuid method"""
        filename = generate_random_filename("pdf")
        assert filename.endswith(".pdf")
        # UUID format: 8-4-4-4-12 characters
        assert len(filename) == 40  # 36 chars UUID + 4 chars ".pdf"
        assert filename.count("-") == 4

    def test_generate_uuid_method_explicit(self):
        """Test generate_random_filename with explicit uuid method"""
        filename = generate_random_filename("txt", method="uuid")
        assert filename.endswith(".txt")
        assert len(filename) == 40  # 36 chars UUID + 4 chars ".txt"

    def test_generate_secure_method_length_12(self):
        """Test generate_random_filename with secure method"""
        filename = generate_random_filename("zip", method="secure", length=12)
        assert filename.endswith(".zip")
        # 12 chars + 4 chars ".zip"
        assert len(filename) == 16
        # Should contain only alphanumeric characters (before extension)
        name_part = filename[:-4]
        assert name_part.isalnum()

    def test_generate_secure_method_default_length(self):
        """Test generate_random_filename with secure method and default length"""
        filename = generate_random_filename("json", method="secure")
        assert filename.endswith(".json")
        # 16 chars + 5 chars ".json"
        assert len(filename) == 21
        name_part = filename[:-5]
        assert name_part.isalnum()

    def test_generate_simple_method_length_8(self):
        """Test generate_random_filename with simple method"""
        filename = generate_random_filename("csv", method="simple", length=8)
        assert filename.endswith(".csv")
        # 8 chars + 4 chars ".csv"
        assert len(filename) == 12
        # Should contain only alphanumeric characters
        name_part = filename[:-4]
        assert name_part.isalnum()

    def test_generate_timestamp_method_length_6(self):
        """Test generate_random_filename with timestamp method"""
        with patch("src.file.temporary.datetime") as mock_datetime:
            mock_datetime.now.return_value = datetime(2025, 11, 6, 14, 30, 25)
            filename = generate_random_filename("log", method="timestamp", length=6)
            
            assert filename.endswith(".log")
            assert filename.startswith("20251106_143025_")
            # Format: YYYYMMDD_HHMMSS_random(6).log
            # 8 + 1 + 6 + 1 + 6 + 4 = 26 chars
            assert len(filename) == 26

    def test_generate_simple_method_with_prefix_and_length_8(self):
        """Test generate_random_filename with prefix"""
        filename = generate_random_filename("txt", method="simple", length=8, prefix="test_")
        assert filename.startswith("test_")
        assert filename.endswith(".txt")
        assert len(filename) == 17  # 5 (test_) + 8 (random) + 4 (.txt)

    def test_generate_simple_method_with_suffix_and_length_8(self):
        """Test generate_random_filename with suffix"""
        filename = generate_random_filename("txt", method="simple", length=8, suffix="_backup")
        assert filename.endswith("_backup.txt")
        assert len(filename) == 19  # 8 (random) + 7 (_backup) + 4 (.txt)

    def test_generate_simple_method_with_prefix_suffix_and_length_10(self):
        """Test generate_random_filename with both prefix and suffix"""
        filename = generate_random_filename("pdf", method="simple", length=10, prefix="report_", suffix="_final")
        assert filename.startswith("report_")
        assert filename.endswith("_final.pdf")
        assert len(filename) == 27  # 7 (report_) + 10 (random) + 6 (_final) + 4 (.pdf)

    def test_generate_default_extension_without_dot(self):
        """Test generate_random_filename with extension without dot"""
        filename = generate_random_filename("json")
        assert filename.endswith(".json")
        assert not filename.endswith("..json")

    def test_generate_default_extension_with_dot(self):
        """Test generate_random_filename with extension starting with dot"""
        filename = generate_random_filename(".json")
        assert filename.endswith(".json")
        assert not filename.endswith("..json")

    def test_generate_invalid_method(self):
        """Test generate_random_filename with invalid method raises ValueError"""
        with pytest.raises(ValueError, match="Method must be 'uuid', 'secure', 'timestamp' or 'simple'"):
            generate_random_filename("txt", method="invalid")  # type: ignore

    def test_generate_default_different_extensions(self):
        """Test generate_random_filename with various extensions"""
        extensions = ["pdf", "docx", "xlsx", "png", "jpg", "mp4", "zip"]
        for ext in extensions:
            filename = generate_random_filename(ext)
            assert filename.endswith(f".{ext}")

    def test_generate_secure_method_varied_length(self):
        """Test generate_random_filename with secure method and different lengths"""
        for length in [5, 10, 20, 32]:
            filename = generate_random_filename("txt", method="secure", length=length)
            name_part = filename[:-4]  # Remove ".txt"
            assert len(name_part) == length

    def test_generate_simple_method_varied_length(self):
        """Test generate_random_filename with simple method and different lengths"""
        for length in [5, 10, 20, 32]:
            filename = generate_random_filename("txt", method="simple", length=length)
            name_part = filename[:-4]  # Remove ".txt"
            assert len(name_part) == length

    def test_generate_uuid_uniqueness_with_100_trials(self):
        """Test that uuid method generates unique filenames"""
        filenames = [generate_random_filename("txt", method="uuid") for _ in range(100)]
        assert len(filenames) == len(set(filenames))  # All unique

    def test_generate_secure_uniqueness_with_100_trials(self):
        """Test that secure method generates unique filenames"""
        filenames = [generate_random_filename("txt", method="secure") for _ in range(100)]
        # Should be highly unique (allow small chance of collision)
        assert len(set(filenames)) >= 95

    @patch("src.file.temporary.uuid.uuid4")
    def test_generate_uuid_mocked(self, mock_uuid):
        """Test generate_random_filename with mocked uuid"""
        mock_uuid.return_value = uuid.UUID('12345678-1234-5678-1234-567812345678')
        filename = generate_random_filename("pdf", method="uuid")
        assert filename == "12345678-1234-5678-1234-567812345678.pdf"

    @patch("src.file.temporary.secrets.choice")
    def test_generate_secure_mocked_length_8(self, mock_choice):
        """Test generate_random_filename with mocked secure method"""
        mock_choice.side_effect = list("aBcDeFgH")
        filename = generate_random_filename("txt", method="secure", length=8)
        assert filename == "aBcDeFgH.txt"

    @patch("src.file.temporary.random.choices")
    def test_generate_simple_mocked_length_8(self, mock_choices):
        """Test generate_random_filename with mocked simple method"""
        mock_choices.return_value = list("abcd1234")
        filename = generate_random_filename("csv", method="simple", length=8)
        assert filename == "abcd1234.csv"

    def test_generate_empty_extension(self):
        """Test generate_random_filename with empty extension"""
        filename = generate_random_filename("")
        assert filename.endswith(".")
        # UUID without extension, just a dot
        assert len(filename) == 37  # 36 chars UUID + 1 char "."

    def test_generate_multipart_extension(self):
        """Test generate_random_filename with multi-part extension"""
        filename = generate_random_filename("tar.gz")
        assert filename.endswith(".tar.gz")


class TestGenerateTempFile:
    """Test suite for generate_temp_file function."""

    @pytest.fixture(autouse=True)
    def temp_files(self):
        """Fixture to track and cleanup temporary files."""
        files = []
        yield files
        
        # Cleanup: runs after test completes
        for file in files:
            if file.exists():
                file.unlink()
    
    def test_generate_with_extension_only(self, temp_files):
        """Test generating temp file with only extension parameter."""
        temp_file = generate_temp_file(extension="txt")
        temp_files.append(temp_file)
        
        assert temp_file.exists()
        assert temp_file.is_file()
        assert temp_file.suffix == ".txt"
        assert temp_file.parent == Path(tempfile.gettempdir())
    
    def test_generate_with_custom_filename(self, temp_files):
        """Test generating temp file with custom filename."""
        temp_file = generate_temp_file(filename="myfile.csv")
        temp_files.append(temp_file)
        
        assert temp_file.exists()
        assert temp_file.is_file()
        assert temp_file.name == "myfile.csv"
        assert temp_file.suffix == ".csv"
    
    def test_generate_with_filename_no_extension_and_extension_param(self, temp_files):
        """Test filename without extension uses extension parameter."""
        temp_file = generate_temp_file(filename="myfile", extension="json")
        temp_files.append(temp_file)
        
        assert temp_file.exists()
        assert temp_file.is_file()
        assert temp_file.name == "myfile.json"
        assert temp_file.suffix == ".json"
    
    def test_generate_with_unique_true_creates_new_file_on_collision(self, temp_files):
        """Test that unique=True creates numbered file when collision occurs."""
        # Create first file
        temp_file1 = generate_temp_file(filename="collision.txt", unique=True)
        temp_files.append(temp_file1)
        assert temp_file1.exists()
        assert temp_file1.is_file()
        assert temp_file1.name == "collision.txt"
        
        # Create second file with same name
        temp_file2 = generate_temp_file(filename="collision.txt", unique=True)
        temp_files.append(temp_file2)
        assert temp_file2.exists()
        assert temp_file2.is_file()
        assert temp_file2.name == "collision_1.txt"
        
        # Create third file
        temp_file3 = generate_temp_file(filename="collision.txt", unique=True)
        temp_files.append(temp_file3)
        assert temp_file3.exists()
        assert temp_file3.is_file()
        assert temp_file3.name == "collision_2.txt"
    
    def test_generate_with_unique_false_raises_error_on_collision(self, temp_files):
        """Test that unique=False raises FileExistsError on collision."""
        # Create first file
        temp_file = generate_temp_file(filename="exclusive.txt", unique=False)
        temp_files.append(temp_file)
        assert temp_file.exists()
        
        # Try to create second file with same name
        with pytest.raises(FileExistsError, match="already exists"):
            generate_temp_file(filename="exclusive.txt", unique=False)
    
    def test_generate_with_no_parameters_raises_error(self):
        """Test that missing both filename and extension raises ValueError."""
        with pytest.raises(ValueError, match="Either filename or extension must be provided"):
            generate_temp_file()
    
    def test_generate_with_filename_without_extension_and_no_extension_param_raises_error(self):
        """Test that filename without extension and no extension param raises ValueError."""
        with pytest.raises(ValueError, match="Filename must have an extension or extension parameter must be provided"):
            generate_temp_file(filename="noextension")
    
    def test_generate_check_file_created_in_temp_directory(self, temp_files):
        """Test that file is created in system temp directory."""
        temp_file = generate_temp_file(extension="tmp")
        temp_files.append(temp_file)
        temp_dir = Path(tempfile.gettempdir())
        
        assert temp_file.parent == temp_dir
        assert temp_file.exists()
        assert temp_file.suffix == ".tmp"
        assert temp_file.is_file()
    
    def test_generate_with_returns_resolved_path(self, temp_files):
        """Test that function returns resolved absolute path."""
        temp_file = generate_temp_file(extension="log")
        temp_files.append(temp_file)
        
        assert temp_file.is_absolute()
        assert str(temp_file) == str(temp_file.resolve())
    
    def test_generate_with_multiple_sequential_files_are_unique(self, temp_files):
        """Test that multiple files generated sequentially are unique."""
        files = []
        for _ in range(5):
            temp_file = generate_temp_file(extension="dat")
            files.append(temp_file)
            temp_files.append(temp_file)
        
        # Check all files exist and are unique
        filenames = [f.name for f in files]
        assert len(filenames) == len(set(filenames))
        assert all(f.exists() for f in files)
        assert all(f.is_file() for f in files)
        assert all(f.suffix == ".dat" for f in files)
    
    def test_generate_with_extension_with_dot_is_handled(self, temp_files):
        """Test that extension parameter works with or without leading dot."""
        temp_file1 = generate_temp_file(extension="txt")
        temp_files.append(temp_file1)
        temp_file2 = generate_temp_file(extension=".txt")
        temp_files.append(temp_file2)
        
        assert temp_file1.suffix == ".txt"
        assert temp_file1.exists()
        assert temp_file2.suffix == ".txt"
        assert temp_file2.exists()
    
    def test_generate_with_complex_filename_with_extension(self, temp_files):
        """Test filename with multiple dots and complex name."""
        temp_file = generate_temp_file(filename="my.complex.file.tar.gz")
        temp_files.append(temp_file)
        
        assert temp_file.exists()
        assert temp_file.name == "my.complex.file.tar.gz"
        assert temp_file.suffix == ".gz"
        assert temp_file.is_file()
    
    def test_generate_with_counter_limit_not_reached_in_normal_use(self, temp_files):
        """Test that counter works correctly for reasonable number of files."""
        files = []
        base_name = "counter_test.txt"
        
        # Create 10 files with same base name
        for _ in range(10):
            temp_file = generate_temp_file(filename=base_name, unique=True)
            files.append(temp_file)
            temp_files.append(temp_file)
            assert temp_file.exists()
        
        # Verify naming pattern
        assert all([filename.name.startswith("counter_test") for filename in temp_files])
        assert all([filename.name.endswith(".txt") for filename in temp_files])
    
    def test_generate_with_empty_filename_with_extension(self, temp_files):
        """Test that empty string filename is handled correctly."""
        temp_file = generate_temp_file(filename="", extension="txt")
        temp_files.append(temp_file)

        assert temp_file.exists()
        assert temp_file.suffix == ".txt"
        assert temp_file.is_file()
        
