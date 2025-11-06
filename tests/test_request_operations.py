import pytest
import requests
import json

from src.request.operations import request, retry_request, get_filename_from_uri
from src.error.service import ExternalServiceError


class TestRequest:
    """Test cases for request function."""

    @pytest.fixture(autouse=True)
    def setup_mock_request(self, mocker, request):
        """Setup mock for requests.request."""
        params = getattr(request, "param", {})
        status_code = params.get("status_code", 200)
        response_content = params.get("response_content", b"")
        encoding = params.get("encoding", "utf-8")

        self.mock_response = mocker.MagicMock()
        self.mock_response.status_code = status_code
        self.mock_response.content = response_content
        self.mock_response.encoding = encoding
        try:
            self.mock_response.json.return_value = json.loads(response_content)
        except (json.JSONDecodeError, UnicodeDecodeError):
            self.mock_response.json.side_effect = requests.exceptions.JSONDecodeError("msg", "doc", 0)

        self.mock_requests = mocker.patch("src.request.operations.requests.request", return_value=self.mock_response)


    @pytest.mark.parametrize("setup_mock_request", [{"response_content": b'{"key": "value"}'}], indirect=True)
    def test_request_get_success(self):
        """Test successful GET request."""

        status_code, response_body = request("GET", "https://api.example.com/data")
        
        assert status_code == 200
        assert response_body == {"key": "value"}
        self.mock_requests.assert_called_once_with(
            method="GET",
            url="https://api.example.com/data",
            params=None,
            headers=None,
            auth=None,
            json=None,
            data=None,
            timeout=300,
            verify=True
        )

    @pytest.mark.parametrize("setup_mock_request", [{"status_code": 201, "response_content": b'{"id": 123}'}], indirect=True)
    def test_request_post_with_json(self):
        """Test POST request with JSON data."""
        json_data = {"name": "test", "value": 42}
        headers = {"Content-Type": "application/json"}
        
        status_code, response_body = request(
            "POST",
            "https://api.example.com/create",
            request_json=json_data,
            headers=headers
        )
        
        assert status_code == 201
        assert response_body == {"id": 123}
        self.mock_requests.assert_called_once()
        assert self.mock_requests.call_args.kwargs["json"] == json_data
        assert self.mock_requests.call_args.kwargs["headers"] == headers

    @pytest.mark.parametrize("setup_mock_request", [{"status_code": 200, "response_content":b'{"results": []}'}], indirect=True)
    def test_request_with_params(self):
        """Test request with URL parameters."""        
        params = {"page": 1, "limit": 10}
        status_code, response_body = request("GET", "https://api.example.com/items", params=params)
        
        assert status_code == 200
        self.mock_requests.assert_called_once()
        assert self.mock_requests.call_args.kwargs["params"] == params

    @pytest.mark.parametrize("setup_mock_request", [{"status_code": 200, "response_content": b'{"authenticated": true}'}], indirect=True)
    def test_request_with_auth(self):
        """Test request with authentication."""
        auth = ("username", "password")
        status_code, response_body = request("GET", "https://api.example.com/secure", auth=auth)
        
        assert status_code == 200
        self.mock_requests.assert_called_once()
        assert self.mock_requests.call_args.kwargs["auth"] == auth

    @pytest.mark.parametrize("setup_mock_request", [{"status_code": 200, "response_content": b'{"success": true}'}], indirect=True)
    def test_request_with_form_data(self):
        """Test POST request with form data."""        
        form_data = {"field1": "value1", "field2": "value2"}
        status_code, response_body = request("POST", "https://api.example.com/form", data=form_data)
        
        assert status_code == 200
        self.mock_requests.assert_called_once()
        assert self.mock_requests.call_args.kwargs["data"] == form_data

    @pytest.mark.parametrize("setup_mock_request", [{"status_code": 200, "response_content": b'{}'}], indirect=True)
    def test_request_with_custom_timeout(self):
        """Test request with custom timeout."""        
        status_code, response_body = request("GET", "https://api.example.com/slow", timeout=60)
        
        assert status_code == 200
        self.mock_requests.assert_called_once()
        assert self.mock_requests.call_args.kwargs["timeout"] == 60

    @pytest.mark.parametrize("setup_mock_request", [{"status_code": 200, "response_content": b'{}'}], indirect=True)
    def test_request_with_verify_false(self):
        """Test request with SSL verification disabled."""
        status_code, response_body = request("GET", "https://api.example.com/insecure", verify=False)
        
        assert status_code == 200
        self.mock_requests.assert_called_once()
        assert self.mock_requests.call_args.kwargs["verify"] is False

    @pytest.mark.parametrize("setup_mock_request", [{"status_code": 204, "response_content": b'', "json_response": None}], indirect=True)
    def test_request_empty_response(self):
        """Test request with empty response content."""        
        status_code, response_body = request("DELETE", "https://api.example.com/delete/123")
        
        assert status_code == 204
        assert response_body == {}
        self.mock_requests.assert_called_once()

    @pytest.mark.parametrize("setup_mock_request", [{"status_code": 200, "response_content": b'Plain text response'}], indirect=True)
    def test_request_non_json_response(self):
        """Test request with non-JSON response."""
        self.mock_response.text = "Plain text response"
        self.mock_response.json.side_effect = requests.exceptions.JSONDecodeError("msg", "doc", 0)
                
        status_code, response_body = request("GET", "https://api.example.com/text")
        
        assert status_code == 200
        assert response_body == {"content": "Plain text response"}
        self.mock_requests.assert_called_once()
        assert self.mock_response.text == "Plain text response"

    def test_request_invalid_method(self):
        """Test request with invalid HTTP method."""
        with pytest.raises(ValueError) as exc_info:
            request("INVALID", "https://api.example.com") # type: ignore
        
        assert "Invalid HTTP method 'INVALID'" in str(exc_info.value)

    @pytest.mark.parametrize("setup_mock_request", [{"status_code": 200, "response_content": b'{"updated": true}'}], indirect=True)
    def test_request_put_method(self):
        """Test PUT request."""        
        status_code, response_body = request("PUT", "https://api.example.com/update/123", request_json={"field": "value"})
        
        assert status_code == 200
        assert response_body == {"updated": True}
        self.mock_requests.assert_called_once()
        assert self.mock_requests.call_args.kwargs["json"] == {"field": "value"}

    @pytest.mark.parametrize("setup_mock_request", [{"status_code": 200, "response_content": b'{"deleted": true}'}], indirect=True)
    def test_request_delete_method(self):
        """Test DELETE request."""        
        status_code, response_body = request("DELETE", "https://api.example.com/delete/123")
        
        assert status_code == 200
        assert response_body == {"deleted": True}
        self.mock_requests.assert_called_once()

    @pytest.mark.parametrize("setup_mock_request", [{"status_code": 200, "response_content": b'{"patched": true}'}], indirect=True)
    def test_request_patch_method(self):
        """Test PATCH request."""        
        status_code, response_body = request("PATCH", "https://api.example.com/patch/123", request_json={"field": "new_value"})
        
        assert status_code == 200
        assert response_body == {"patched": True}
        self.mock_requests.assert_called_once()
        assert self.mock_requests.call_args.kwargs["json"] == {"field": "new_value"}

    @pytest.mark.parametrize("setup_mock_request", [{"status_code": 200, "response_content": b''}], indirect=True)
    def test_request_head_method(self):
        """Test HEAD request."""        
        status_code, response_body = request("HEAD", "https://api.example.com/check")
        
        assert status_code == 200
        assert response_body == {}
        self.mock_requests.assert_called_once()

    @pytest.mark.parametrize("setup_mock_request", [{"status_code": 200, "response_content": b'{"methods": ["GET", "POST"]}'}], indirect=True)
    def test_request_options_method(self):
        """Test OPTIONS request."""       
        status_code, response_body = request("OPTIONS", "https://api.example.com")
        
        assert status_code == 200
        assert response_body == {"methods": ["GET", "POST"]}
        self.mock_requests.assert_called_once()

    @pytest.mark.parametrize("setup_mock_request", [{"status_code": 200, "response_content": b'{}'}], indirect=True)
    def test_request_allow_redirects_false_stream_true(self):
        """Test request with additional keyword arguments: allow_redirects=False, stream=True."""
        status_code, response_body = request(
            "GET",
            "https://api.example.com",
            allow_redirects=False,
            stream=True
        )
        
        assert status_code == 200
        assert response_body == {}
        self.mock_requests.assert_called_once()
        assert self.mock_requests.call_args.kwargs["allow_redirects"] is False
        assert self.mock_requests.call_args.kwargs["stream"] is True

    def test_request_timeout_exception(self, mocker):
        """Test request raises ExternalServiceError on timeout."""
        mocker.patch("src.request.operations.requests.request", side_effect=requests.Timeout("Connection timeout"))
        
        with pytest.raises(ExternalServiceError) as exc_info:
            request("GET", "https://api.example.com/slow")
        
        assert "timed out after 300 seconds" in str(exc_info.value.message)
        assert exc_info.value.code == "REQUEST_TIMEOUT"

    def test_request_connection_error(self, mocker):
        """Test request raises ExternalServiceError on connection error."""
        mocker.patch("src.request.operations.requests.request", side_effect=requests.ConnectionError("Failed to connect"))
        
        with pytest.raises(ExternalServiceError) as exc_info:
            request("GET", "https://api.example.com/unreachable")
        
        assert "Failed to connect to" in str(exc_info.value.message)
        assert exc_info.value.code == "REQUEST_CONNECTION_ERROR"

    def test_request_general_exception(self, mocker):
        """Test request raises ExternalServiceError on general RequestException."""
        mocker.patch("src.request.operations.requests.request", side_effect=requests.RequestException("SSL error"))
        
        with pytest.raises(ExternalServiceError) as exc_info:
            request("GET", "https://api.example.com/data")
        
        assert "Request to" in str(exc_info.value.message)
        assert "failed" in str(exc_info.value.message)
        assert exc_info.value.code == "REQUEST_FAILED"

    @pytest.mark.parametrize("setup_mock_request", [{"status_code": 400, "response_content": b'{"error": "Bad request"}'}], indirect=True)
    def test_request_raise_for_status_true_with_400(self):
        """Test request raises ExternalServiceError when raise_for_status=True and status is 400."""
        with pytest.raises(ExternalServiceError) as exc_info:
            request("GET", "https://api.example.com/data", raise_for_status=True)
        
        assert "failed with status code 400" in str(exc_info.value.message)
        assert exc_info.value.code == "HTTP_STATUS_ERROR"

    @pytest.mark.parametrize("setup_mock_request", [{"status_code": 500, "response_content": b'{"error": "Server error"}'}], indirect=True)
    def test_request_raise_for_status_true_with_500(self):
        """Test request raises ExternalServiceError when raise_for_status=True and status is 500."""        
        with pytest.raises(ExternalServiceError) as exc_info:
            request("POST", "https://api.example.com/create", raise_for_status=True)
        
        assert "failed with status code 500" in str(exc_info.value.message)
        assert exc_info.value.code == "HTTP_STATUS_ERROR"

    @pytest.mark.parametrize("setup_mock_request", [{"status_code": 404, "response_content": b'{"error": "Not found"}'}], indirect=True)
    def test_request_raise_for_status_false_with_404(self):
        """Test request does NOT raise when raise_for_status=False with error status."""       
        status_code, response_body = request("GET", "https://api.example.com/missing", raise_for_status=False)
        
        assert status_code == 404
        assert response_body == {"error": "Not found"}
        self.mock_requests.assert_called_once()

    @pytest.mark.parametrize("setup_mock_request", [{"status_code": 201, "response_content": b'{"created": true}'}], indirect=True) 
    def test_request_raise_for_status_true_with_success(self):
        """Test request does NOT raise when raise_for_status=True with 2xx status."""
        status_code, response_body = request("POST", "https://api.example.com/create", raise_for_status=True)
        
        assert status_code == 201
        assert response_body == {"created": True}
        self.mock_requests.assert_called_once()

    @pytest.mark.parametrize("setup_mock_request", [{"status_code": 200, "response_content": b'<html>Not JSON</html>', "encoding": "utf-8"}], indirect=True)
    def test_request_non_json_response_with_warning(self, capsys):
        """Test that warning is printed for non-JSON responses."""        
        status_code, response_body = request("GET", "https://api.example.com/html")
        
        assert status_code == 200
        assert response_body == {"content": "<html>Not JSON</html>"}
        self.mock_requests.assert_called_once()
        
        captured = capsys.readouterr()
        assert "Warning: Response from https://api.example.com/html is not valid JSON" in captured.out

    @pytest.mark.parametrize("setup_mock_request", [{"status_code": 200, "response_content": "Café résumé".encode("latin-1"), "encoding": "latin-1"}], indirect=True)
    def test_request_response_with_different_encoding(self):
        """Test request handles different character encodings."""        
        status_code, response_body = request("GET", "https://api.example.com/text")
        
        assert status_code == 200
        assert "Café résumé" in response_body["content"]
        self.mock_requests.assert_called_once()

    @pytest.mark.parametrize("setup_mock_request", [{"status_code": 200, "response_content": b'Plain text', "encoding": None}], indirect=True)
    def test_request_response_with_no_encoding(self):
        """Test request handles response with no encoding specified."""        
        status_code, response_body = request("GET", "https://api.example.com/text")
        
        assert status_code == 200
        assert response_body == {"content": "Plain text"}
        self.mock_requests.assert_called_once()


class TestRetryRequest:
    """Test cases for retry_request function."""

    def test_retry_request_success_first_attempt(self, mocker, capsys):
        """Test successful request on first attempt."""
        mock_request = mocker.patch("src.request.operations.request", return_value=(200, {"success": True}))
        mock_sleep = mocker.patch("src.request.operations.sleep")
        
        status_code, response_body = retry_request("GET", "https://api.example.com/data")
        
        assert status_code == 200
        assert response_body == {"success": True}
        mock_request.assert_called_once()
        mock_sleep.assert_not_called()
        
        captured = capsys.readouterr()
        assert "Starting attempt 1 of 5" in captured.out
        assert "Status Code: 200" in captured.out
        assert "Response Body: {'success': True}" in captured.out

    def test_retry_request_success_after_3_retries_out_of_5(self, mocker, capsys):
        """Test successful request after 2 failed attempts in 5 attempts."""
        mock_request = mocker.patch("src.request.operations.request")
        mock_request.side_effect = [
            (500, {"error": "Server error"}),
            (503, {"error": "Service unavailable"}),
            (200, {"success": True})
        ]
        mock_sleep = mocker.patch("src.request.operations.sleep")
        
        status_code, response_body = retry_request("GET", "https://api.example.com/data", retry_delay=5, max_attempts=5)
        
        assert status_code == 200
        assert response_body == {"success": True}
        assert mock_request.call_count == 3
        assert mock_sleep.call_count == 2
        mock_sleep.assert_called_with(5)
        
        captured = capsys.readouterr()
        assert "Starting attempt 1 of 5" in captured.out
        assert "Starting attempt 2 of 5" in captured.out
        assert "Starting attempt 3 of 5" in captured.out

    def test_retry_request_all_attempts_fail(self, mocker):
        """Test when all retry attempts fail."""
        mock_request = mocker.patch("src.request.operations.request", return_value=(500, {"error": "Server error"}))
        mock_sleep = mocker.patch("src.request.operations.sleep")
        
        with pytest.raises(ExternalServiceError) as exc_info:
            retry_request("POST", "https://api.example.com/create", max_attempts=3, retry_delay=10)
        
        assert "Failed to get successful response after 3 attempts" in str(exc_info.value)
        assert "POST https://api.example.com/create" in str(exc_info.value)
        assert exc_info.value.code == "REQUEST_MAX_RETRIES_EXCEEDED"
        assert mock_request.call_count == 3
        assert mock_sleep.call_count == 2
        mock_sleep.assert_called_with(10)

    def test_retry_request_with_2_request_exception_3_attempts(self, mocker, capsys):
        """Test retry when RequestException is raised twice in 5 max attempts."""
        url = "https://api.example.com/data"
        timeout = 300
        
        mock_request = mocker.patch("src.request.operations.request")
        mock_request.side_effect = [
            ExternalServiceError(f"Failed to connect to {url}", "REQUEST_CONNECTION_ERROR"),
            ExternalServiceError(f"Request to {url} timed out after {timeout} seconds", "REQUEST_TIMEOUT"),
            (200, {"success": True})
        ]
        mock_sleep = mocker.patch("src.request.operations.sleep")
        
        status_code, response_body = retry_request("GET", url, retry_delay=2, max_attempts=5, timeout=timeout)
        
        assert status_code == 200
        assert response_body == {"success": True}
        assert mock_request.call_count == 3
        assert mock_sleep.call_count == 2
        mock_sleep.assert_called_with(2)
        
        captured = capsys.readouterr()
        assert "Attempt 1 failed with exception" in captured.out
        assert "Attempt 2 failed with exception" in captured.out

    def test_retry_request_custom_max_attempts(self, mocker):
        """Test retry with custom max attempts."""
        mock_request = mocker.patch("src.request.operations.request", return_value=(200, {"data": "test"}))
        
        status_code, response_body = retry_request("GET", "https://api.example.com", max_attempts=10)
        
        assert status_code == 200
        assert response_body == {"data": "test"}
        mock_request.assert_called_once()

    def test_retry_request_passes_kwargs(self, mocker):
        """Test that retry_request passes additional kwargs to request function."""
        mock_request = mocker.patch("src.request.operations.request", return_value=(200, {}))
        
        status_code, response_body = retry_request("POST",
                                                   "https://api.example.com/create",
                                                   headers={"Authorization": "Bearer token"},
                                                   request_json={"key": "value"},
                                                   timeout=60
                                                   )
        
        assert status_code == 200
        assert response_body == {}
        mock_request.assert_called_once_with(
            method="POST",
            url="https://api.example.com/create",
            headers={"Authorization": "Bearer token"},
            request_json={"key": "value"},
            timeout=60
        )

    def test_retry_request_201_status_code(self, mocker):
        """Test retry with 201 status code (successful creation)."""
        mock_request = mocker.patch("src.request.operations.request", return_value=(201, {"id": 1}))
        
        status_code, response_body = retry_request("POST", "https://api.example.com/create")
        
        assert status_code == 201
        assert response_body == {"id": 1}
        mock_request.assert_called_once()

    def test_retry_request_299_status_code(self, mocker):
        """Test retry with 299 status code (edge of 2xx range)."""
        mock_request = mocker.patch("src.request.operations.request", return_value=(299, {"data": "success"}))
        
        status_code, response_body = retry_request("GET", "https://api.example.com/data")
        
        assert status_code == 299
        assert response_body == {"data": "success"}
        mock_request.assert_called_once()

    def test_retry_request_no_delay_on_last_attempt(self, mocker, capsys):
        """Test that no delay occurs after the last failed attempt."""
        mock_request = mocker.patch("src.request.operations.request", return_value=(500, {"error": "Server error"}))
        mock_sleep = mocker.patch("src.request.operations.sleep")
        
        with pytest.raises(ExternalServiceError):
            retry_request("GET", "https://api.example.com", max_attempts=3, retry_delay=5)
        
        # Sleep should be called 2 times (not 3), since no delay after last attempt
        assert mock_sleep.call_count == 2
        
        captured = capsys.readouterr()
        # The last attempt should not print "Waiting X seconds"
        lines = captured.out.strip().split('\n')
        assert not any("Waiting" in line for line in lines[-2:] if "attempt 3" in lines[-3].lower())

    def test_retry_request_external_service_error_propagates(self, mocker):
        """Test that ExternalServiceError from request function is caught and retried."""
        mock_request = mocker.patch("src.request.operations.request")
        mock_request.side_effect = [
            ExternalServiceError("Connection failed", "CONNECTION_ERROR"),
            ExternalServiceError("Timeout", "REQUEST_TIMEOUT"),
            (200, {"success": True})
        ]
        mock_sleep = mocker.patch("src.request.operations.sleep")
        
        status_code, response_body = retry_request("GET", "https://api.example.com/data")
        
        assert status_code == 200
        assert response_body == {"success": True}
        assert mock_request.call_count == 3
        assert mock_sleep.call_count == 2

    def test_retry_request_with_max_attempts_1(self, mocker):
        """Test retry with max_attempts=1 (no retries)."""
        mock_request = mocker.patch("src.request.operations.request", return_value=(200, {"data": "test"}))
        mock_sleep = mocker.patch("src.request.operations.sleep")
        
        status_code, response_body = retry_request("GET", "https://api.example.com", max_attempts=1)
        
        assert status_code == 200
        assert response_body == {"data": "test"}
        mock_request.assert_called_once()
        mock_sleep.assert_not_called()

    def test_retry_request_prints_status_code_and_response_body(self, mocker, capsys):
        """Test that retry_request prints response body on each attempt."""
        mock_request = mocker.patch("src.request.operations.request", return_value=(200, {"key": "value"}))

        status_code, response_body = retry_request("GET", "https://api.example.com/data")

        assert status_code == 200
        assert response_body == {"key": "value"}
        mock_request.assert_called_once()
        
        captured = capsys.readouterr()
        assert "Status Code: 200" in captured.out
        assert "Response Body: {'key': 'value'}" in captured.out

    def test_retry_request_4xx_status_triggers_retry(self, mocker):
        """Test that 4xx status codes trigger retries."""
        mock_request = mocker.patch("src.request.operations.request")
        mock_request.side_effect = [
            (404, {"error": "Not found"}),
            (200, {"success": True})
        ]
        mock_sleep = mocker.patch("src.request.operations.sleep")
        
        status_code, response_body = retry_request("GET", "https://api.example.com/data", retry_delay=1)
        
        assert status_code == 200
        assert response_body == {"success": True}
        assert mock_request.call_count == 2
        assert mock_sleep.call_count == 1
        mock_sleep.assert_called_once_with(1)

    def test_retry_request_edge_status_199(self, mocker):
        """Test retry with status code 199 (just below 2xx range)."""
        mock_request = mocker.patch("src.request.operations.request")
        mock_request.side_effect = [
            (199, {"data": "not success"}),
            (200, {"data": "success"})
        ]
        mock_sleep = mocker.patch("src.request.operations.sleep")
        
        status_code, response_body = retry_request("GET", "https://api.example.com/data")
        
        assert status_code == 200
        assert mock_request.call_count == 2
        mock_sleep.assert_called_once()

    def test_retry_request_edge_status_300(self, mocker):
        """Test retry with status code 300 (just above 2xx range)."""
        mock_request = mocker.patch("src.request.operations.request")
        mock_request.side_effect = [
            (300, {"redirect": "multiple choices"}),
            (200, {"data": "success"})
        ]
        mock_sleep = mocker.patch("src.request.operations.sleep")
        
        status_code, response_body = retry_request("GET", "https://api.example.com/data")
        
        assert status_code == 200
        assert mock_request.call_count == 2
        mock_sleep.assert_called_once()


class TestGetFilenameFromUri:
    """Test cases for get_filename_from_uri function."""

    def test_get_filename_success(self):
        """Test extracting filename from URI."""
        uri = "https://example.com/path/to/file.txt"
        filename = get_filename_from_uri(uri)
        assert filename == "file.txt"

    def test_get_filename_no_path(self):
        """Test extracting filename from URI with no path."""
        uri = "https://example.com"
        filename = get_filename_from_uri(uri)
        assert filename == ""

    def test_get_filename_trailing_slash(self):
        """Test extracting filename from URI with trailing slash."""
        uri = "https://example.com/path/to/directory/"
        filename = get_filename_from_uri(uri)
        assert filename == ""

    def test_get_filename_root_path(self):
        """Test extracting filename from root path."""
        uri = "https://example.com"
        filename = get_filename_from_uri(uri)
        assert filename == ""

    def test_get_filename_with_anchor(self):
        """Test extracting filename with anchor/hash in URI."""
        uri = "https://example.com/docs/manual.pdf#page=10"
        filename = get_filename_from_uri(uri)
        assert filename == "manual.pdf"

    def test_get_filename_windows_path_encoded(self):
        """Test extracting filename from Windows-style path encoding."""
        uri = "file:///C:/Users/Documents/file.txt"
        filename = get_filename_from_uri(uri)
        assert filename == "file.txt"

    def test_get_filename_with_encoded_unicode(self):
        """Test extracting filename with various encoded unicode characters."""
        uri = "https://example.com/files/%E6%96%87%E4%BB%B6.txt"  # Chinese characters
        filename = get_filename_from_uri(uri)
        assert filename == "文件.txt"

    def test_get_filename_double_encoded(self):
        """Test extracting filename that might be double URL-encoded."""
        uri = "https://example.com/files/my%2520file.pdf"  # %20 encoded again
        filename = get_filename_from_uri(uri)
        assert filename == "my%20file.pdf"  # Only decodes once

    def test_get_filename_with_semicolon_params(self):
        """Test extracting filename with semicolon-style parameters."""
        uri = "https://example.com/files/report.pdf;jsessionid=123456"
        filename = get_filename_from_uri(uri)
        assert filename == "report.pdf"

    def test_get_filename_data_uri(self):
        """Test extracting filename from data URI (edge case). Data URIs don't have filenames"""
        uri = "data:text/plain;base64,SGVsbG8gV29ybGQ="
        filename = get_filename_from_uri(uri)
        assert filename == ""

    def test_get_filename_relative_path(self):
        """Test extracting filename from relative path."""
        uri = "../documents/file.pdf"
        filename = get_filename_from_uri(uri)
        assert filename == "file.pdf"

    def test_get_filename_hidden_file(self):
        """Test extracting hidden file (starting with dot)."""
        uri = "https://example.com/config/.env"
        filename = get_filename_from_uri(uri)
        assert filename == ".env"

    def test_get_filename_very_long_filename(self):
        """Test extracting very long filename."""
        long_name = "a" * 255 + ".txt"
        uri = f"https://example.com/files/{long_name}"
        filename = get_filename_from_uri(uri)
        assert filename == long_name

    def test_get_filename_with_mixed_encoding(self):
        """Test extracting filename with both encoded and non-encoded characters."""
        uri = "https://example.com/files/report%202024-final.pdf"
        filename = get_filename_from_uri(uri)
        assert filename == "report 2024-final.pdf"
