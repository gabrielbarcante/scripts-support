from pathlib import Path
import urllib.parse
import requests
from typing import Literal, Dict, Tuple
from time import sleep

from ..error.service import ExternalServiceError


METHODS = Literal["GET", "POST", "PUT", "DELETE", "PATCH", "HEAD", "OPTIONS"]

def request(method: METHODS, url: str, params: Dict | None = None, headers: Dict | None = None, request_json: Dict | None = None, data: Dict | None = None, auth: Tuple[str, str] | None = None, timeout: int = 300, verify: bool = True, raise_for_status: bool = False, **kwargs) -> Tuple[int, Dict]:
    """
    Make an HTTP request using the specified method to the given URL.

    Args:
        method (METHODS): The HTTP method to use (e.g., 'GET', 'POST', 'PUT', 'DELETE').
        url (str): The URL to which the request is sent.
        params (Dict | None, optional): URL parameters to append to the URL. Defaults to None.
        headers (Dict | None, optional): HTTP headers to send with the request. Defaults to None.
        request_json (Dict | None, optional): JSON data to send in the request body. Defaults to None.
        data (Dict | None, optional): Form data to send in the request body. Defaults to None.
        auth (Tuple[str, str] | None, optional): Authentication credentials (username, password). Defaults to None.
        timeout (int, optional): Request timeout in seconds. Defaults to 300.
        verify (bool, optional): Whether to verify SSL certificates. Defaults to True.
        raise_for_status (bool, optional): Whether to raise an exception for non-2xx responses. Defaults to False.
        **kwargs: Additional arguments to pass to the requests method.

    Returns:
        Tuple[int, Dict]: A tuple containing the HTTP status code and response body as a dictionary.
            - For successful JSON responses, returns the parsed JSON as a dictionary.
            - For empty responses, returns an empty dictionary {}.
            - For non-JSON responses, returns {"content": <decoded_text>}.

    Raises:
        ValueError: If an invalid HTTP method is provided.
        ExternalServiceError: If any of the following occurs:
            - REQUEST_TIMEOUT: Request exceeds the specified timeout duration.
            - REQUEST_CONNECTION_ERROR: Unable to establish connection to the URL.
            - REQUEST_FAILED: General request failure (e.g., SSL errors, DNS errors).
            - HTTP_STATUS_ERROR: Response status code is not 2xx when raise_for_status=True.

    Examples:
        >>> # Simple GET request
        >>> status, body = request('GET', 'https://api.example.com/users')
        >>> print(f"Status: {status}, Users: {body}")
        
        >>> # POST request with JSON data
        >>> status, body = request(
        ...     'POST',
        ...     'https://api.example.com/users',
        ...     request_json={'name': 'John', 'email': 'john@example.com'}
        ... )
        
        >>> # GET request with authentication and custom headers
        >>> status, body = request(
        ...     'GET',
        ...     'https://api.example.com/protected',
        ...     auth=('username', 'password'),
        ...     headers={'User-Agent': 'MyApp/1.0'}
        ... )
        
        >>> # Request with raise_for_status enabled
        >>> try:
        ...     status, body = request('GET', 'https://api.example.com/data', raise_for_status=True)
        ... except ExternalServiceError as e:
        ...     print(f"Request failed: {e.message}")

    Notes:
        - Non-JSON responses are automatically wrapped in a dictionary with a "content" key.
        - SSL certificate verification is enabled by default. Disable with verify=False (not recommended for production).
        - The function prints a warning when receiving non-JSON responses.
    """
    if method not in METHODS.__args__:
        raise ValueError(f"Invalid HTTP method '{method}'. Must be one of: {', '.join(METHODS.__args__)}")
    
    try:
        response = requests.request(method=method, 
                                    url=url, 
                                    params=params, 
                                    headers=headers, 
                                    auth=auth, 
                                    json=request_json, 
                                    data=data, 
                                    timeout=timeout, 
                                    verify=verify, 
                                    **kwargs
                                )
    except requests.Timeout as e:
        raise ExternalServiceError(
            message=f"Request to {url} timed out after {timeout} seconds",
            code="REQUEST_TIMEOUT"
        ) from e
    except requests.ConnectionError as e:
        raise ExternalServiceError(
            message=f"Failed to connect to {url}",
            code="REQUEST_CONNECTION_ERROR"
        ) from e
    except requests.RequestException as e:
        raise ExternalServiceError(
            message=f"Request to {url} failed: {str(e)}",
            code="REQUEST_FAILED"
        ) from e
    
    status_code = response.status_code
    if raise_for_status and not(200 <= status_code < 300):
        raise ExternalServiceError(
            message=f"Request to {method} {url} failed with status code {status_code}",
            code="HTTP_STATUS_ERROR",
        )

    try:
        response_body = response.json()
    except requests.exceptions.JSONDecodeError:
        if not response.content:
            response_body = {}
        else:
            print(f"Warning: Response from {url} is not valid JSON, returning raw content")
            response_body = {"content": response.content.decode(response.encoding or "utf-8", errors="replace")}

    return status_code, response_body


def retry_request(method: METHODS, url: str, max_attempts: int = 5, retry_delay: int = 30, **kwargs) -> Tuple[int, Dict]:
    """
    Attempt to make an HTTP request multiple times with a delay between attempts until successful or max attempts are exhausted.

    This function implements a simple retry mechanism with a fixed delay between attempts.
    It will retry the request if it fails with a non-2xx status code or if a RequestException occurs.

    Args:
        method (METHODS): The HTTP method to use (e.g., 'GET', 'POST', 'PUT', 'DELETE').
        url (str): The URL to which the request is sent.
        max_attempts (int, optional): The maximum number of attempts to make. Defaults to 5.
        retry_delay (int, optional): Time in seconds to wait between retry attempts. Defaults to 30.
        **kwargs: Additional arguments to pass to the request function (e.g., headers, params, data).

    Returns:
        Tuple[int, Dict]: A tuple containing the HTTP status code and response body as a dictionary.

    Raises:
        ExternalServiceError: If all retry attempts fail to return a successful response (status code 2xx).
    """
    for attempt in range(max_attempts):
        print(f"Starting attempt {attempt + 1} of {max_attempts}")
        try:
            status_code, response_body = request(method=method, url=url, **kwargs)
            print(f"Status Code: {status_code}")
            print(f"Response Body: {response_body}")

            if 200 <= status_code < 300:
                return status_code, response_body
        
        except ExternalServiceError as e:
            print(f"Attempt {attempt + 1} failed with exception: {e}")
        
        if attempt < max_attempts - 1:
            print(f"Waiting {retry_delay} seconds before next attempt...")
            sleep(retry_delay)

    raise ExternalServiceError(
        message=f"Failed to get successful response after {max_attempts} attempts to {method} {url}",
        code="REQUEST_MAX_RETRIES_EXCEEDED"
    )


def get_filename_from_uri(uri: str) -> str:
    """
    Extract the filename from a URI.

    This function parses a URI, decodes any URL-encoded characters, and returns
    the filename portion from the path component.

    Args:
        uri (str): The URI string from which to extract the filename.

    Returns:
        str: The filename extracted from the URI path.

    Example:
        >>> get_filename_from_uri("https://example.com/path/to/file.pdf")
        'file.pdf'
        >>> get_filename_from_uri("https://example.com/path/to/my%20document.pdf")
        'my document.pdf'
    """
    return Path(urllib.parse.urlsplit(urllib.parse.unquote(uri)).path).name
