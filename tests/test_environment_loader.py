import pytest
from pathlib import Path
from src.environment.loader import load_environment_variables, get_environment_variables


class TestLoadEnvironmentVariables:
    """Test cases for load_environment_variables function."""

    @pytest.fixture(autouse=True)
    def setup_mock(self, mocker, request):
        """Setup common mocks for Path and load_dotenv."""
        # Get parameters from the test, with defaults
        params = getattr(request, "param", {})
        exists = params.get("exists", True)
        path_instance_file = params.get("path_instance_file", "/fake/path/.env")

        # Create a mock Path instance to simulate file system operations
        self.mock_path_instance = mocker.MagicMock()
        
        # Mock the resolve() method to return the same instance (simulates path resolution)
        self.mock_path_instance.resolve.return_value = self.mock_path_instance

        # Mock the __truediv__ operator to handle "Path / filename" operations
        # Important: Return the same mock instance to maintain method chaining
        self.mock_path_instance.__truediv__.return_value = self.mock_path_instance
        
        # Simulate that the .env file exists or not in the file system
        self.mock_path_instance.exists.return_value = exists
        
        path_instance_file = Path(path_instance_file)

        # Mock the full path to the .env file as a POSIX string
        self.mock_path_instance.as_posix.return_value = path_instance_file.as_posix()

        # Mock the parent directory path
        self.mock_path_instance.parent.as_posix.return_value = path_instance_file.parent.as_posix()

        # Patch the Path class to return our mock instance when instantiated
        mocker.patch("src.environment.loader.Path", return_value=self.mock_path_instance)

    def test_load_environment_variables_success(self, mocker, capsys):
        """Test successful loading of environment variables."""        
        # Patch the load_dotenv function to prevent actual file loading
        mock_load_dotenv = mocker.patch("src.environment.loader.load_dotenv")

        # Execute the function under test
        load_environment_variables(".env", ".")

        # Verify that load_dotenv was called with the correct arguments
        mock_load_dotenv.assert_called_once_with(dotenv_path="/fake/path/.env", override=True)
        
        # Capture and verify the printed output
        captured = capsys.readouterr()
        assert "Environment variables loading completed." in captured.out

    @pytest.mark.parametrize("setup_mock", [{"exists": False}], indirect=True)
    def test_load_environment_variables_file_not_found(self, mocker):
        """Test FileNotFoundError when .env file doesn't exist."""
        with pytest.raises(AssertionError):
            load_environment_variables(".env", ".")

    @pytest.mark.parametrize("setup_mock", [{"path_instance_file": "/custom/path/.env.test" }], indirect=True)
    def test_load_environment_variables_custom_filename(self, mocker):
        """Test loading with custom filename."""
        mock_load_dotenv = mocker.patch("src.environment.loader.load_dotenv")

        load_environment_variables(".env.test", "/custom/path")

        mock_load_dotenv.assert_called_once()


class TestGetEnvironmentVariables:
    """Test cases for get_environment_variables function."""

    def test_get_single_environment_variable_as_string(self, monkeypatch):
        """Test retrieving a single environment variable passed as string."""
        monkeypatch.setenv("TEST_VAR", "test_value")
        
        result = get_environment_variables("TEST_VAR")
        
        assert result == ("test_value",)
        assert isinstance(result, tuple)

    def test_get_single_environment_variable_as_list(self, monkeypatch):
        """Test retrieving a single environment variable passed as list."""
        monkeypatch.setenv("TEST_VAR", "test_value")
        
        result = get_environment_variables(["TEST_VAR"])
        
        assert result == ("test_value",)
        assert isinstance(result, tuple)

    def test_get_multiple_environment_variables(self, monkeypatch):
        """Test retrieving multiple environment variables."""
        monkeypatch.setenv("VAR1", "value1")
        monkeypatch.setenv("VAR2", "value2")
        monkeypatch.setenv("VAR3", "value3")
        
        result = get_environment_variables(["VAR1", "VAR2", "VAR3"])
        
        assert result == ("value1", "value2", "value3")
        assert len(result) == 3

    def test_get_environment_variable_not_found(self):
        """Test KeyError when environment variable doesn't exist."""
        with pytest.raises(KeyError):
            get_environment_variables("NON_EXISTENT_VAR")

    def test_get_environment_variables_empty_list(self):
        """Test with empty list of environment variables."""
        result = get_environment_variables([])
        
        assert result == ()
        assert isinstance(result, tuple)


class TestIntegration:
    """Integration tests for the loader module."""

    @pytest.fixture
    def temp_env_file(self, tmp_path):
        """Fixture to create a temporary .env file for integration tests."""
        env_file = tmp_path / ".env"
        env_file.write_text("TEST_KEY=test_value\nANOTHER_KEY=another_value")
        return env_file

    def test_load_and_get_environment_variables(self, temp_env_file, monkeypatch):
        """Test the full workflow of loading and retrieving environment variables."""
        # Clear any existing environment variables
        monkeypatch.delenv("TEST_KEY", raising=False)
        monkeypatch.delenv("ANOTHER_KEY", raising=False)
        
        load_environment_variables(".env", str(temp_env_file.parent))
        
        result = get_environment_variables(["TEST_KEY", "ANOTHER_KEY"])
        
        assert result == ("test_value", "another_value")
