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
    - [Direct SQLiteConnection Usage](#direct-sqliteconnection-usage)
    - [Direct MySQLConnection Usage](#direct-mysqlconnection-usage)
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

Database connectivity and operations with extensible architecture:

- **`base.py`**: Abstract base class defining the database connection interface with SQL identifier validation, context manager support, and standardized CRUD operations
- **`sqlite.py`**: SQLite implementation with safe parameterized queries, automatic transaction management, pandas DataFrame integration, and support for timestamp handling and dtype conversions
- **`mysql.py`**: MySQL implementation using SQLAlchemy with connection pooling, safe parameterized queries, batch insert operations, automatic transaction management, pandas DataFrame integration, and support for timezone handling
- **`factory.py`**: Factory pattern for creating database connections with support for multiple database types, extensible design for custom connectors, and centralized connection management

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
from src.db import create_connection, DatabaseFactory

# Method 1: Use convenience function with SQLite
with create_connection("sqlite", db_path="app.db", primary_key_column="id") as db:
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
    updated = db.update(
        'users',
        parameters={'age': 31},
        filters={'name': 'Alice'}
    )
    
    # Delete records
    count = db.delete('users', filters={'email': 'bob@example.com'})

# Method 2: Use MySQL connection
db = create_connection(
    db_type="mysql",
    host="localhost",
    port=3306,
    user="myuser",
    password="mypassword",
    database="mydb",
    primary_key_column="id"
)

with db:
    # Create table with MySQL-specific syntax
    db.execute("""
        CREATE TABLE IF NOT EXISTS products (
            id INT AUTO_INCREMENT PRIMARY KEY,
            name VARCHAR(255) NOT NULL,
            price DECIMAL(10, 2),
            stock INT DEFAULT 0,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    
    # Batch insert with automatic ID tracking
    products = [
        {'name': 'Laptop', 'price': 999.99, 'stock': 10},
        {'name': 'Mouse', 'price': 29.99, 'stock': 50},
        {'name': 'Keyboard', 'price': 79.99, 'stock': 30}
    ]
    inserted = db.insert('products', products, return_inserted=True)
    print(f"Inserted {len(inserted)} products")
    
    # Complex filtering with multiple conditions
    low_stock = db.select(
        'products',
        filters={'stock': 10},
        order_by='price DESC'
    )
    
    # Update with NULL filter
    db.update(
        'products',
        parameters={'stock': 0},
        filters={'discontinued_at': None}
    )
    
    # Get table schema information
    schema = db.get_table_info('products')
    print(schema[['name', 'type', 'notnull', 'pk']])

# Method 3: Use factory directly
db = DatabaseFactory.create_connection(
    db_type="mysql",
    host="localhost",
    port=3306,
    user="root",
    password="password",
    database="analytics"
)

with db:
    # Check if table exists
    if not db.table_exists('logs'):
        db.execute("""
            CREATE TABLE logs (
                id INT AUTO_INCREMENT PRIMARY KEY,
                message TEXT,
                level VARCHAR(20),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
    
    # Work with timestamps and timezone localization
    from datetime import datetime, timezone
    import pytz
    
    logs = [
        {
            'message': 'Application started',
            'level': 'INFO',
            'created_at': datetime.now(timezone.utc).isoformat()
        }
    ]
    db.insert('logs', logs)
    
    # Select with date parsing and timezone conversion
    recent_logs = db.select(
        'logs',
        filters={'level': 'ERROR'},
        parse_dates={'created_at': '%Y-%m-%dT%H:%M:%S%z'},
        localize_timezone=pytz.timezone('America/Sao_Paulo'),
        limit=100
    )

# Method 4: Register custom database connector
from src.db import DatabaseConnection, DatabaseFactory

class CustomDBConnection(DatabaseConnection):
    # Implement abstract methods
    def _connect_db(self, **kwargs):
        # Custom connection logic
        pass
    # ...other implementations...

DatabaseFactory.register_connector("customdb", CustomDBConnection)
db = create_connection("customdb", custom_param="value")
```

### Direct SQLiteConnection Usage

```python
from src.db.sqlite import SQLiteConnection
from pathlib import Path

# Initialize SQLite connection
db = SQLiteConnection(
    db_path="data/app.db",
    primary_key_column="id"
)

# Use as context manager for automatic connection handling
with db:
    # Create table
    db.execute("""
        CREATE TABLE IF NOT EXISTS tasks (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            title TEXT NOT NULL,
            description TEXT,
            status TEXT DEFAULT 'pending',
            created_at TEXT,
            updated_at TEXT
        )
    """)
    
    # Insert tasks
    tasks = [
        {
            'title': 'Complete project',
            'description': 'Finish the database module',
            'status': 'in_progress'
        },
        {
            'title': 'Write tests',
            'description': 'Add test coverage for new features',
            'status': 'pending'
        }
    ]
    
    # Insert with automatic ID retrieval
    inserted = db.insert('tasks', tasks, return_inserted=True)
    print(f"Inserted {len(inserted)} tasks with IDs: {inserted['id'].tolist()}")
    
    # Query with filtering and ordering
    pending_tasks = db.select(
        'tasks',
        columns=['id', 'title', 'status'],
        filters={'status': 'pending'},
        order_by='created_at DESC'
    )
    
    # Update task status
    db.update(
        'tasks',
        parameters={'status': 'completed', 'updated_at': datetime.now().isoformat()},
        filters={'id': inserted['id'].iloc[0]},
        return_updated_rows=False
    )
    
    # Delete completed tasks
    deleted_count = db.delete('tasks', filters={'status': 'completed'})
    print(f"Deleted {deleted_count} completed tasks")
    
    # Check table structure
    if db.table_exists('tasks'):
        schema = db.get_table_info('tasks')
        print("Table schema:")
        print(schema[['name', 'type', 'notnull', 'dflt_value']])

# Connection automatically closed after context manager exits
assert not db.is_connected()

# Manual connection management (not recommended)
db.connect()
try:
    result = db.select('tasks')
finally:
    db.disconnect()
```

### Direct MySQLConnection Usage

```python
from src.db.mysql import MySQLConnection
from datetime import datetime, timezone
import pytz

# Initialize MySQL connection
db = MySQLConnection(
    host="localhost",
    port=3306,
    user="app_user",
    password="secure_password",
    database="application_db",
    primary_key_column="id"
)

# Use context manager for transaction safety
with db:
    # Create orders table
    db.execute("""
        CREATE TABLE IF NOT EXISTS orders (
            id INT AUTO_INCREMENT PRIMARY KEY,
            customer_name VARCHAR(255) NOT NULL,
            total_amount DECIMAL(10, 2) NOT NULL,
            status VARCHAR(50) DEFAULT 'pending',
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            INDEX idx_status (status),
            INDEX idx_created_at (created_at)
        )
    """)
    
    # Batch insert orders
    orders = [
        {'customer_name': 'John Doe', 'total_amount': 150.50, 'status': 'pending'},
        {'customer_name': 'Jane Smith', 'total_amount': 89.99, 'status': 'pending'},
        {'customer_name': 'Bob Johnson', 'total_amount': 320.00, 'status': 'processing'}
    ]
    
    # Insert and retrieve inserted records
    inserted_orders = db.insert('orders', orders, return_inserted=True)
    print(f"Inserted {len(inserted_orders)} orders")
    
    # Query with complex filtering
    high_value_orders = db.select(
        'orders',
        filters={'status': 'pending'},
        order_by='total_amount DESC',
        limit=10,
        dtype={'total_amount': 'float64'}
    )
    
    # Update order status
    db.update(
        'orders',
        parameters={'status': 'shipped'},
        filters={'id': inserted_orders['id'].iloc[0]},
        return_updated_rows=True
    )
    
    # Query with date parsing and timezone conversion
    recent_orders = db.select(
        'orders',
        parse_dates={'created_at': '%Y-%m-%d %H:%M:%S'},
        localize_timezone=pytz.timezone('America/Sao_Paulo'),
        order_by='created_at DESC',
        limit=50
    )
    
    # Delete cancelled orders
    deleted = db.delete('orders', filters={'status': 'cancelled'})
    print(f"Deleted {deleted} cancelled orders")
    
    # Get table schema information
    if db.table_exists('orders'):
        info = db.get_table_info('orders')
        print("\nOrders table structure:")
        print(info[['name', 'type', 'notnull', 'pk']])
    
    # Execute custom query with parameters
    result = db.execute(
        """
        UPDATE orders 
        SET status = :new_status 
        WHERE total_amount > :min_amount 
        AND status = :old_status
        """,
        params={
            'new_status': 'priority',
            'min_amount': 200.00,
            'old_status': 'pending'
        },
        commit=True
    )
    print(f"Updated {result.rowcount} orders to priority status")

# Connection handling with error recovery
db = MySQLConnection(
    host="localhost",
    port=3306,
    user="app_user",
    password="secure_password",
    database="application_db"
)

try:
    with db:
        # Operations that might fail
        db.insert('orders', [{'customer_name': 'Test', 'total_amount': -10}])
except Exception as e:
    # Transaction automatically rolled back
    print(f"Operation failed: {e}")
    # Connection is safely closed

# Check connection status
if not db.is_connected():
    print("Connection safely closed after error")
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
- `test_db_sqlite.py` - SQLite database operations tests (100+ test cases covering initialization, CRUD operations, transactions, timestamp handling, dtype conversions, and context manager)
- `test_db_mysql.py` - MySQL database operations tests (150+ test cases covering connection management, CRUD operations with SQLAlchemy, batch inserts, transaction rollback, timezone handling, parameter validation, and SQL injection prevention)
- `test_db_factory.py` - Database factory pattern tests (covering connection creation, connector registration, type validation, and extensibility)
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
