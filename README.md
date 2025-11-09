# Scripts Support Library

- [Scripts Support Library](#scripts-support-library)
  - [Installation](#installation)
  - [Project Overview](#project-overview)
  - [Module Descriptions](#module-descriptions)
    - [`src/data/`](#srcdata)
    - [`src/file/`](#srcfile)
    - [`src/log/`](#srclog)
    - [`src/request/`](#srcrequest)
    - [`src/date_time/`](#srcdate_time)
    - [`src/db/`](#srcdb)
    - [`src/environment/`](#srcenvironment)
    - [`src/error/`](#srcerror)
  - [Usage Examples](#usage-examples)
    - [String Matching with Regex](#string-matching-with-regex)
    - [File Operations](#file-operations)
    - [HTTP Requests with Retry](#http-requests-with-retry)
    - [Logging Setup](#logging-setup)
    - [Environment Variables](#environment-variables)
    - [Database Operations](#database-operations)
  - [Testing](#testing)
  - [Licensing, Authors, Acknowledgements](#licensing-authors-acknowledgements)


## Installation<a name="installation"></a>

**Prerequisites**: Python >= 3.11, Git

**Clone Repository**:
```bash
git clone <repo-url>
cd scripts-support
```

**Environment Setup**:

Create Virtual Environment:
- Windows (PowerShell): `python -m venv .venv`
- macOS/Linux: `python -m venv .venv`

Activate Virtual Environment:
- Windows: `.\.venv\Scripts\Activate.ps1`
- macOS/Linux: `source .venv/bin/activate`

**Install Dependencies**:
```bash
pip install -r requirements.txt
```

**Optional Configuration**:
- Copy `.env.example` to `.env` and set environment variables as needed


## Project Overview<a name="overview"></a>

Scripts Support is a comprehensive Python utility library designed to streamline common development tasks across automation scripts and applications. It provides robust, tested modules for data manipulation, file operations, logging, HTTP requests, and error handling.

The library is built with a focus on:
- **Reliability**: Extensive test coverage with pytest
- **Type Safety**: Full type hints for better IDE support
- **Error Handling**: Custom exception hierarchy for precise error management
- **Flexibility**: Configurable components that adapt to various use cases


## Module Descriptions<a name="modules"></a>

### `src/data/`

Data manipulation and processing utilities:

- **`operations.py`**: String matching and regex pattern preparation with support for case-sensitive/insensitive matching, exact matching, and special character handling
- **`collection.py`**: Collection utilities including chunking sequences, flattening matrices, and filtering dictionaries/lists
- **`numeric_data.py`**: Numeric data processing operations
- **`text_data.py`**: Text data manipulation and formatting

### `src/file/`

Comprehensive file and directory management:

- **`operations.py`**: File/directory creation, deletion, moving, searching with regex support, and file monitoring with timeout capabilities
- **`plain_text.py`**: Plain text file reading and writing operations
- **`compress.py`**: File compression and decompression utilities
- **`excel.py`**: Excel file handling with openpyxl
- **`image.py`**: Image processing with Pillow
- **`base_64.py`**: Base64 encoding/decoding for files
- **`temporary.py`**: Temporary file and directory management

### `src/log/`

Advanced logging infrastructure:

- **`logger.py`**: Configurable logging setup with JSON and detailed formatters
- **`formatter.py`**: Custom log formatters including JSONFormatter
- **`reporter.py`**: Log reporting utilities
- **`reporter_df.py`**: DataFrame-based log analysis
- **Configuration files**: `config_json.json`, `config_detailed.json`

### `src/request/`

HTTP request handling:

- **`operations.py`**: Robust HTTP client with retry logic, timeout handling, and comprehensive error management for all HTTP methods (GET, POST, PUT, DELETE, etc.)

### `src/date_time/`

Date and time utilities:

- **`operations.py`**: Date/time formatting, parsing, and manipulation

### `src/db/`

Database connectivity and operations:

- **`sqlite.py`**: Safe SQLite database interface with automatic transaction management, CRUD operations using parameterized queries, context manager support, and pandas DataFrame integration for data type conversion and timestamp handling

### `src/environment/`

Environment configuration:

- **`loader.py`**: Environment variable loading from `.env` files with validation

### `src/error/`

Custom exception hierarchy for precise error handling:

- **`base.py`**: Base exception classes
- **`auth.py`**: Authentication-related exceptions
- **`database.py`**: Database operation exceptions
- **`file.py`**: File operation exceptions
- **`resource.py`**: Resource access exceptions
- **`service.py`**: External service exceptions
- **`validation.py`**: Data validation exceptions


## Usage Examples<a name="usage"></a>

### String Matching with Regex

```python
from src.data.operations import match_string, prepare_regex_pattern

# Simple string matching
match_string("test", "This is a test")  # True

# Case-sensitive exact match
match_string("TEST", "test", case_sensitive=True, exact_match=True)  # False

# Regex matching with special character escaping
match_string("test.com", "visit test.com", regex=True, prepare_search_value=True)  # True
```

### File Operations

```python
from src.file.operations import create_directory, find_object_in_directory, wait_for_files

# Create directory structure
create_directory("/path/to/project", "logs", create_parent=True)

# Find files with pattern matching
files = find_object_in_directory(
    "/path/to/search",
    name_pattern="report.*",
    file_extension=".pdf",
    regex=True
)

# Wait for files to appear (useful for download monitoring)
downloaded = wait_for_files(
    "/downloads",
    ["file1.pdf", "file2.csv"],
    timeout=60
)
```

### HTTP Requests with Retry

```python
from src.request.operations import request, retry_request

# Simple GET request
status, data = request('GET', 'https://api.example.com/users')

# POST with JSON and authentication
status, response = request(
    'POST',
    'https://api.example.com/data',
    request_json={'key': 'value'},
    auth=('user', 'pass'),
    raise_for_status=True
)

# Automatic retry on failure
status, data = retry_request(
    'GET',
    'https://api.example.com/unstable',
    max_attempts=5,
    retry_delay=30
)
```

### Logging Setup

```python
from src.log.logger import setup_logging, get_logger

# Configure JSON logging
setup_logging(type_file_logging="json", name_file_log="myapp", path_dir_logs="./logs")

# Get logger instance
logger = get_logger(__name__)
logger.info("Application started", extra={"user": "john", "action": "login"})
```

### Environment Variables

```python
from src.environment.loader import load_environment_variables, get_environment_variables

# Load from .env file
load_environment_variables(filename=".env", file_path=".")

# Retrieve variables
api_key, db_url = get_environment_variables(["API_KEY", "DATABASE_URL"])
```

### Database Operations

```python
from src.db.sqlite import SQLiteConnection
from datetime import datetime

# Using context manager for automatic connection management
with SQLiteConnection('app.db', primary_key_column='id') as db:
    # Create table
    db.execute("""
        CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            email TEXT UNIQUE,
            age INTEGER,
            created_at TEXT
        )
    """)
    
    # Insert records
    users = [
        {'name': 'Alice', 'email': 'alice@example.com', 'age': 30},
        {'name': 'Bob', 'email': 'bob@example.com', 'age': 25}
    ]
    result = db.insert('users', users)
    
    # Select with filters and type conversion
    df = db.select(
        'users',
        filters={'age': 30},
        order_by='name ASC',
        dtype={'age': 'int32', 'name': 'string'}
    )
    
    # Update records
    db.update(
        'users',
        parameters={'age': 31},
        filters={'name': 'Alice'}
    )
    
    # Delete records
    count = db.delete('users', filters={'email': 'bob@example.com'})
    
    # Work with timestamps
    db.insert('events', [
        {
            'name': 'Event 1',
            'created_at': datetime.now().isoformat()
        }
    ])
    
    # Select with date parsing
    events = db.select(
        'events',
        parse_dates={'created_at': '%Y-%m-%dT%H:%M:%S'}
    )
```


## Testing<a name="testing"></a>

The project includes comprehensive test coverage using pytest:

```bash
# Run all tests
pytest

# Run with coverage report
pytest --cov=src --cov-report=html

# Run specific test file
pytest tests/test_data_operations.py

# Run tests matching pattern
pytest -k "test_match_string"
```

Test files in `tests/` directory:
- `test_data_collection.py` - Collection utilities tests
- `test_data_numeric.py` - Numeric operations tests
- `test_data_operations.py` - String matching and regex tests
- `test_data_text.py` - Text processing tests
- `test_date_time.py` - Date/time operations tests
- `test_db_sqlite.py` - SQLite database operations tests (100+ test cases covering initialization, CRUD operations, transactions, timestamp handling, and dtype conversions)
- `test_environment_loader.py` - Environment loading tests
- `test_file_compress.py` - File compression tests
- `test_file_plain_text.py` - Text file operations tests
- `test_file_temporary.py` - Temporary file handling tests
- `test_request_operations.py` - HTTP request tests
<br><br>
> **⚠️ Note**: Some modules currently lack test coverage. Tests are missing for:
> - `src/file/excel.py`, `src/file/image.py`, `src/file/base_64.py`
> - `src/log/formatter.py`, `src/log/reporter.py`, `src/log/reporter_df.py`
> - All `src/error/` modules
> 
> Contributions to expand test coverage are welcome!


## Licensing, Authors, Acknowledgements<a name="licensing"></a>

- **License**: MIT (see LICENSE)
- **Authors**: Gabriel Barros
- **Acknowledgements**:
  - Open-source libraries: requests, pytest, pandas, openpyxl, Pillow, python-dotenv, Unidecode
  - Community contributors and testers
