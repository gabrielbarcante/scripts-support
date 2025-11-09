import pytest
import sqlite3
import pandas as pd
from pathlib import Path
import shutil
from datetime import datetime, timezone, timedelta

from src.db.sqlite import SQLiteConnection
from src.error.database import DatabaseError


@pytest.fixture
def temp_db_path(tmp_path):
    """Provide a temporary database path for testing"""
    path_db = tmp_path / "test.db"
    yield str(path_db)

    try:
        if path_db.exists():
            path_db.unlink()
        
        if tmp_path.exists():
            shutil.rmtree(tmp_path)
    except Exception:
        pass

@pytest.fixture
def db_connection(temp_db_path):
    """Provide a SQLiteConnection instance for testing"""
    return SQLiteConnection(temp_db_path, primary_key_column="id")


@pytest.fixture
def connected_db(temp_db_path):
    """Provide a connected SQLiteConnection instance with a test table"""
    db = SQLiteConnection(temp_db_path, primary_key_column="id")
    db._connect_db()
    db.execute("""
        CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            email TEXT,
            age INTEGER,
            active INTEGER DEFAULT 1,
            created_at TEXT
        )
    """)
    yield db
    db._disconnect_db()


@pytest.fixture
def connected_db_with_timestamps(temp_db_path):
    """Provide a connected SQLiteConnection instance with timestamp test tables"""
    db = SQLiteConnection(temp_db_path, primary_key_column="id")
    db._connect_db()
    
    # Create table with various timestamp columns
    db.execute("""
        CREATE TABLE IF NOT EXISTS events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            event_name TEXT NOT NULL,
            event_date TEXT,
            created_at TEXT NOT NULL,
            updated_at TEXT,
            scheduled_for TEXT,
            completed_at TEXT
        )
    """)
    
    # Create table for orders with timestamp tracking
    db.execute("""
        CREATE TABLE IF NOT EXISTS orders (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            order_number TEXT NOT NULL,
            customer_name TEXT,
            amount REAL,
            order_date TEXT NOT NULL,
            shipped_date TEXT,
            delivered_date TEXT
        )
    """)
    
    yield db
    db._disconnect_db()


class TestSQLiteConnectionInit:
    """Test cases for SQLiteConnection initialization"""
    
    def test_init_basic(self, temp_db_path):
        """Test basic initialization"""
        db = SQLiteConnection(temp_db_path)
        assert db.db_path == Path(temp_db_path)
        assert db.primary_key_column is None
        assert db.db_connection is None
        assert db.db_cursor is None
    
    def test_init_with_primary_key(self, temp_db_path):
        """Test initialization with primary key column"""
        db = SQLiteConnection(temp_db_path, primary_key_column="id")
        assert db.primary_key_column == "id"
    
    def test_init_invalid_primary_key(self, temp_db_path):
        """Test initialization with invalid primary key column name"""
        with pytest.raises(ValueError, match="Invalid primary key column name"):
            SQLiteConnection(temp_db_path, primary_key_column="id; DROP TABLE users;")
        
        with pytest.raises(ValueError, match="Invalid primary key column name"):
            SQLiteConnection(temp_db_path, primary_key_column="123invalid")


class TestSQLiteConnectionContextManager:
    """Test cases for context manager functionality"""
    
    def test_context_manager_enter_exit(self, temp_db_path):
        """Test context manager enters and exits correctly"""
        with SQLiteConnection(temp_db_path) as db:
            assert db.db_path == Path(temp_db_path)
            assert db.db_connection is not None
            assert db.db_cursor is not None
            assert db.is_connected()
        
        # After exiting, connection should be closed
        assert not db.is_connected()
    
    def test_context_manager_rollback_on_error(self, temp_db_path):
        """Test context manager rolls back on error"""
        db = SQLiteConnection(temp_db_path, primary_key_column="id")
        db._connect_db()
        db.execute("""
            CREATE TABLE IF NOT EXISTS users (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                email TEXT
            )
        """)
        db.execute("INSERT INTO users (name, email) VALUES ('Test', 'test@example.com')")
        db._disconnect_db()
        
        try:
            with SQLiteConnection(temp_db_path) as db2:
                # execute() commits automatically by default
                db2.execute("INSERT INTO users (name, email) VALUES ('Test2', 'test2@example.com')", commit=False)
                raise ValueError("Simulated error")
        except ValueError:
            pass
        
        # Reconnect and check - the second insert should be rolled back
        with SQLiteConnection(temp_db_path) as db3:
            result = db3.select("users")
            assert len(result) == 1
            assert result.iloc[0]["name"] == "Test"


class TestSQLiteConnectionConnect:
    """Test cases for database connection"""
    
    def test_connect_db_creates_connection(self, db_connection):
        """Test _connect_db creates connection and cursor"""
        db_connection._connect_db()
        
        assert db_connection.db_connection is not None
        assert db_connection.db_cursor is not None
        assert isinstance(db_connection.db_connection, sqlite3.Connection)
        assert isinstance(db_connection.db_cursor, sqlite3.Cursor)
    
    def test_connect_db_enables_foreign_keys(self, db_connection):
        """Test _connect_db enables foreign key constraints"""
        db_connection._connect_db()
        
        cursor = db_connection.db_connection.execute("PRAGMA foreign_keys")
        result = cursor.fetchone()
        assert result[0] == 1  # Foreign keys enabled
    
    def test_connect_db_sets_row_factory(self, db_connection):
        """Test _connect_db sets row factory to sqlite3.Row"""
        db_connection._connect_db()
        
        assert db_connection.db_connection.row_factory == sqlite3.Row
    
    def test_connect_db_returns_existing_connection(self, db_connection):
        """Test _connect_db returns existing connection if already connected"""
        db_connection._connect_db()
        first_connection = db_connection.db_connection
        
        db_connection._connect_db()
        second_connection = db_connection.db_connection
        
        assert first_connection is second_connection
    
    def test_connect_db_with_different_isolation_levels(self, db_connection):
        """Test _connect_db with different isolation levels"""
        # Test IMMEDIATE
        db_connection._connect_db(isolation_level="IMMEDIATE")
        assert db_connection.db_connection.isolation_level == "IMMEDIATE"
        db_connection._disconnect_db()
        
        # Test EXCLUSIVE
        db_connection._connect_db(isolation_level="EXCLUSIVE")
        assert db_connection.db_connection.isolation_level == "EXCLUSIVE"
        db_connection._disconnect_db()
        
        # Test None (autocommit)
        db_connection._connect_db(isolation_level=None)
        assert db_connection.db_connection.isolation_level is None
    
    def test_connect_db_error_handling(self, mocker):
        """Test _connect_db raises DatabaseError on connection failure"""
        # Mock sqlite3.connect to raise an error
        mocker.patch("sqlite3.connect", side_effect=sqlite3.Error("Connection failed"))
        
        db = SQLiteConnection("/invalid/path/db.db")
        with pytest.raises(DatabaseError, match="Failed to connect to database"):
            db._connect_db()


class TestSQLiteConnectionDisconnect:
    """Test cases for database disconnection"""
    
    def test_disconnect_db_closes_connection(self, db_connection):
        """Test _disconnect_db closes connection"""
        db_connection._connect_db()
        assert db_connection.is_connected()
        
        db_connection._disconnect_db()
        assert not db_connection.is_connected()
    
    def test_disconnect_db_when_not_connected(self, db_connection):
        """Test _disconnect_db when not connected does not raise error"""
        db_connection._disconnect_db()
        assert db_connection.db_connection is None


class TestSQLiteConnectionValidation:
    """Test cases for identifier validation"""
    
    def test_is_valid_identifier(self):
        """Test _is_valid_identifier validates SQL identifiers correctly"""
        # Valid identifiers
        assert SQLiteConnection._is_valid_identifier("table_name")
        assert SQLiteConnection._is_valid_identifier("_private")
        assert SQLiteConnection._is_valid_identifier("Column1")
        assert SQLiteConnection._is_valid_identifier("user_id_123")
        
        # Invalid identifiers
        assert not SQLiteConnection._is_valid_identifier("123invalid")
        assert not SQLiteConnection._is_valid_identifier("table-name")
        assert not SQLiteConnection._is_valid_identifier("drop; table")
        assert not SQLiteConnection._is_valid_identifier("user.id")
        assert not SQLiteConnection._is_valid_identifier("")
    
    def test_validate_identifiers_valid(self, db_connection):
        """Test _validate_identifiers with valid identifiers"""
        db_connection._validate_identifiers("table1", "column1", "column_2")

    def test_validate_identifiers_invalid(self, db_connection):
        """Test _validate_identifiers with invalid identifiers raises ValueError"""
        with pytest.raises(ValueError):
            db_connection._validate_identifiers("table1", "bad-column", "column2")


class TestSQLiteConnectionIsConnected:
    """Test cases for is_connected method"""
    
    def test_is_connected_when_not_connected(self, db_connection):
        """Test is_connected returns False when not connected"""
        assert not db_connection.is_connected()
    
    def test_is_connected_when_connected(self, connected_db):
        """Test is_connected returns True when connected"""
        assert connected_db.is_connected()
    
    def test_is_connected_after_disconnect(self, connected_db):
        """Test is_connected returns False after disconnection"""
        connected_db._disconnect_db()
        assert not connected_db.is_connected()


class TestSQLiteConnectionSelect:
    """Test cases for select method"""
    
    def test_select_all_columns(self, connected_db):
        """Test select with all columns"""
        connected_db.execute("INSERT INTO users (name, email, age) VALUES ('Alice', 'alice@test.com', 30)")
        connected_db.execute("INSERT INTO users (name, email, age) VALUES ('Bob', 'bob@test.com', 25)")
        
        result = connected_db.select("users")
        
        assert len(result) == 2
        assert "name" in result.columns
        assert "email" in result.columns
        assert "age" in result.columns
    
    def test_select_specific_columns(self, connected_db):
        """Test select with specific columns"""
        connected_db.execute("INSERT INTO users (name, email, age) VALUES ('Alice', 'alice@test.com', 30)")
        
        result = connected_db.select("users", columns=["name", "age"])
        
        assert len(result.columns) == 2
        assert "name" in result.columns
        assert "age" in result.columns
        assert "email" not in result.columns
    
    def test_select_with_filters(self, connected_db):
        """Test select with filter conditions"""
        connected_db.execute("INSERT INTO users (name, email, age) VALUES ('Alice', 'alice@test.com', 30)")
        connected_db.execute("INSERT INTO users (name, email, age) VALUES ('Bob', 'bob@test.com', 25)")
        connected_db.execute("INSERT INTO users (name, email, age) VALUES ('Charlie', 'charlie@test.com', 30)")
        
        result = connected_db.select("users", filters={"age": 30})
        
        assert len(result) == 2
        assert set(result["name"]) == {"Alice", "Charlie"}
    
    def test_select_with_null_filter(self, connected_db):
        """Test select with NULL filter"""
        connected_db.execute("INSERT INTO users (name, email, age) VALUES ('Alice', 'alice@test.com', 30)")
        connected_db.execute("INSERT INTO users (name, age) VALUES ('Bob', 25)")
        
        result = connected_db.select("users", filters={"email": None})
        
        assert len(result) == 1
        assert result.iloc[0]["name"] == "Bob"
    
    def test_select_with_multiple_filters(self, connected_db):
        """Test select with multiple filter conditions"""
        connected_db.execute("INSERT INTO users (name, email, age, active) VALUES ('Alice', 'alice@test.com', 30, 1)")
        connected_db.execute("INSERT INTO users (name, email, age, active) VALUES ('Bob', 'bob@test.com', 30, 0)")
        connected_db.execute("INSERT INTO users (name, email, age, active) VALUES ('Charlie', 'charlie@test.com', 25, 1)")
        
        result = connected_db.select("users", filters={"age": 30, "active": 1})
        
        assert len(result) == 1
        assert result.iloc[0]["name"] == "Alice"
    
    def test_select_with_order_by(self, connected_db):
        """Test select with ORDER BY clause"""
        connected_db.execute("INSERT INTO users (name, email, age) VALUES ('Charlie', 'charlie@test.com', 30)")
        connected_db.execute("INSERT INTO users (name, email, age) VALUES ('Alice', 'alice@test.com', 25)")
        connected_db.execute("INSERT INTO users (name, email, age) VALUES ('Bob', 'bob@test.com', 35)")
        
        result = connected_db.select("users", order_by="age ASC")
        
        assert list(result["name"]) == ["Alice", "Charlie", "Bob"]
    
    def test_select_with_limit(self, connected_db):
        """Test select with LIMIT clause"""
        connected_db.execute("INSERT INTO users (name, email, age) VALUES ('Alice', 'alice@test.com', 30)")
        connected_db.execute("INSERT INTO users (name, email, age) VALUES ('Bob', 'bob@test.com', 25)")
        connected_db.execute("INSERT INTO users (name, email, age) VALUES ('Charlie', 'charlie@test.com', 35)")
        
        result = connected_db.select("users", limit=2)
        
        assert len(result) == 2
    
    def test_select_with_invalid_limit_negative(self, connected_db):
        """Test select with invalid limit raises ValueError"""
        with pytest.raises(ValueError, match="limit must be a non-negative integer"):
            connected_db.select("users", limit=-1)

    def test_select_with_invalid_limit_zero(self, connected_db):
        """Test select with invalid limit raises ValueError"""
        connected_db.execute("INSERT INTO users (name, email, age) VALUES ('Alice', 'alice@test.com', 30)")
        
        result = connected_db.select("users", limit=0)

        assert len(result) == 0
    
    def test_select_empty_result(self, connected_db):
        """Test select returns empty DataFrame when no matches"""
        result = connected_db.select("users", filters={"name": "NonExistent"})
        
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 0
    
    def test_select_with_invalid_table_name(self, connected_db):
        """Test select with invalid table name raises ValueError"""
        with pytest.raises(ValueError, match="Invalid SQL identifier"):
            connected_db.select("invalid-table")
    
    def test_select_with_invalid_column_name(self, connected_db):
        """Test select with invalid column name raises ValueError"""
        with pytest.raises(ValueError, match="Invalid SQL identifier"):
            connected_db.select("users", columns=["name", "bad-column"])
    
    def test_select_database_error(self, connected_db):
        """Test select raises DatabaseError on query execution failure"""
        with pytest.raises(DatabaseError, match="Error executing SELECT"):
            connected_db.select("nonexistent_table")


class TestSQLiteConnectionInsert:
    """Test cases for insert method"""
    
    def test_insert_single_row(self, connected_db):
        """Test insert single row"""
        rows = [{"name": "Alice", "email": "alice@test.com", "age": 30}]
        result = connected_db.insert("users", rows)
        
        assert len(result) == 1
        assert result.iloc[0]["name"] == "Alice"
        assert result.iloc[0]["email"] == "alice@test.com"
        assert result.iloc[0]["age"] == 30
        assert "id" in result.columns
    
    def test_insert_multiple_rows(self, connected_db):
        """Test insert multiple rows"""
        rows = [
            {"name": "Alice", "email": "alice@test.com", "age": 30},
            {"name": "Bob", "email": "bob@test.com", "age": 25},
            {"name": "Charlie", "email": "charlie@test.com", "age": 35}
        ]
        result = connected_db.insert("users", rows)
        
        assert len(result) == 3
        assert set(result["name"]) == {"Alice", "Bob", "Charlie"}
    
    def test_insert_with_null_values(self, connected_db):
        """Test insert with NULL values"""
        rows = [{"name": "Alice", "email": None, "age": 30}]
        result = connected_db.insert("users", rows)
        
        assert len(result) == 1
        assert result.iloc[0]["name"] == "Alice"
        assert pd.isna(result.iloc[0]["email"])
    
    def test_insert_without_returning(self, connected_db):
        """Test insert without returning inserted records"""
        rows = [{"name": "Alice", "email": "alice@test.com", "age": 30}]
        result = connected_db.insert("users", rows, return_inserted=False)
        
        assert result is None
        
        # Verify data was inserted
        db_result = connected_db.select("users")
        assert len(db_result) == 1
        assert db_result.iloc[0]["name"] == "Alice"
    
    def test_insert_empty_rows(self, connected_db):
        """Test insert with empty rows list raises ValueError"""
        with pytest.raises(ValueError, match="rows cannot be empty"):
            connected_db.insert("users", [])
    
    def test_insert_inconsistent_columns(self, connected_db):
        """Test insert with inconsistent columns raises ValueError"""
        rows = [
            {"name": "Alice", "email": "alice@test.com"},
            {"name": "Bob", "age": 25}
        ]
        
        with pytest.raises(ValueError, match="All rows must have the same columns"):
            connected_db.insert("users", rows)
    
    def test_insert_invalid_table_name(self, connected_db):
        """Test insert with invalid table name raises ValueError"""
        rows = [{"name": "Alice"}]
        
        with pytest.raises(ValueError, match="Invalid SQL identifier"):
            connected_db.insert("invalid-table", rows)
    
    def test_insert_invalid_column_name(self, connected_db):
        """Test insert with invalid column name raises ValueError"""
        rows = [{"bad-column": "value"}]
        
        with pytest.raises(ValueError, match="Invalid SQL identifier"):
            connected_db.insert("users", rows)
    
    def test_insert_constraint_violation(self, connected_db):
        """Test insert with constraint violation raises DatabaseError"""
        # Create a table with unique constraint
        connected_db.execute("""
            CREATE TABLE test_unique (
                id INTEGER PRIMARY KEY,
                email TEXT UNIQUE NOT NULL
            )
        """)
        
        rows = [{"email": "test@test.com"}]
        connected_db.insert("test_unique", rows, return_inserted=False)
        
        # Try to insert duplicate
        with pytest.raises(DatabaseError, match="Error inserting data"):
            connected_db.insert("test_unique", rows, return_inserted=False)
    
    def test_insert_rollback_on_error(self, connected_db):
        """Test insert rolls back on error"""
        # Try to insert with constraint violation
        connected_db.execute("""
            CREATE TABLE test_unique (
                id INTEGER PRIMARY KEY,
                email TEXT UNIQUE NOT NULL
            )
        """)
        
        rows = [{"email": "test@test.com"}]
        connected_db.insert("test_unique", rows, return_inserted=False)
        
        # Try to insert duplicate - should rollback
        with pytest.raises(DatabaseError):
            connected_db.insert("test_unique", rows, return_inserted=False)
        
        # Verify only one row exists
        result = connected_db.select("test_unique")
        assert len(result) == 1


class TestSQLiteConnectionUpdate:
    """Test cases for update method"""
    
    def test_update_single_field(self, connected_db):
        """Test update single field"""
        connected_db.execute("INSERT INTO users (name, email, age) VALUES ('Alice', 'alice@test.com', 30)")
        connected_db.execute("INSERT INTO users (name, email, age) VALUES ('Bob', 'bob@test.com', 25)")
        
        result = connected_db.update(
            "users",
            parameters={"age": 31},
            filters={"name": "Alice"}
        )
        
        assert len(result) == 1
        assert result.iloc[0]["age"] == 31
        assert result.iloc[0]["name"] == "Alice"
    
    def test_update_multiple_fields(self, connected_db):
        """Test update multiple fields"""
        connected_db.execute("INSERT INTO users (name, email, age) VALUES ('Alice', 'alice@test.com', 30)")
        connected_db.execute("INSERT INTO users (name, email, age) VALUES ('Bob', 'bob@test.com', 25)")
        
        result = connected_db.update(
            "users",
            parameters={"email": "newalice@test.com", "age": 31},
            filters={"name": "Alice"}
        )
        
        assert len(result) == 1
        assert result.iloc[0]["email"] == "newalice@test.com"
        assert result.iloc[0]["age"] == 31
        assert result.iloc[0]["name"] == "Alice"
    
    def test_update_with_null_value(self, connected_db):
        """Test update field to NULL"""
        connected_db.execute("INSERT INTO users (name, email, age) VALUES ('Alice', 'alice@test.com', 30)")
        connected_db.execute("INSERT INTO users (name, email, age) VALUES ('Bob', 'bob@test.com', 25)")
        
        result = connected_db.update(
            "users",
            parameters={"email": None},
            filters={"name": "Alice"}
        )
        
        assert len(result) == 1
        assert pd.isna(result.iloc[0]["email"])
        assert result.iloc[0]["name"] == "Alice"
    
    def test_update_multiple_rows(self, connected_db):
        """Test update multiple rows"""
        connected_db.execute("INSERT INTO users (name, email, age) VALUES ('Alice', 'alice@test.com', 30)")
        connected_db.execute("INSERT INTO users (name, email, age) VALUES ('Bob', 'bob@test.com', 30)")
        connected_db.execute("INSERT INTO users (name, email, age) VALUES ('Charlie', 'charlie@test.com', 35)")
        
        result = connected_db.update(
            "users",
            parameters={"age": 31},
            filters={"age": 30}
        )
                
        assert len(result) == 2
        assert all(result["age"] == 31)
    
    def test_update_with_null_filter(self, connected_db):
        """Test update with NULL filter"""
        connected_db.execute("INSERT INTO users (name, age) VALUES ('Alice', 30)")
        connected_db.execute("INSERT INTO users (name, email, age) VALUES ('Bob', 'bob@test.com', 30)")
        
        result = connected_db.update(
            "users",
            parameters={"age": 31},
            filters={"email": None}
        )
        
        assert len(result) == 1
        assert result.iloc[0]["name"] == "Alice"
        assert result.iloc[0]["age"] == 31
    
    def test_update_without_returning(self, connected_db):
        """Test update without returning updated records"""
        connected_db.execute("INSERT INTO users (name, email, age) VALUES ('Alice', 'alice@test.com', 30)")
        
        result = connected_db.update(
            "users",
            parameters={"age": 31},
            filters={"name": "Alice"},
            return_updated_rows=False
        )
        
        assert result is None
        
        # Verify update occurred
        db_result = connected_db.select("users")
        assert db_result.iloc[0]["age"] == 31
    
    def test_update_no_matching_rows(self, connected_db):
        """Test update with no matching rows returns empty DataFrame"""
        connected_db.execute("INSERT INTO users (name, email, age) VALUES ('Alice', 'alice@test.com', 30)")
        
        result = connected_db.update(
            "users",
            parameters={"age": 31},
            filters={"name": "NonExistent"}
        )
        
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 0
    
    def test_update_empty_parameters(self, connected_db):
        """Test update with empty parameters raises ValueError"""
        with pytest.raises(ValueError, match="parameters and filters cannot be empty"):
            connected_db.update("users", parameters={}, filters={"name": "Alice"})
    
    def test_update_empty_filters(self, connected_db):
        """Test update with empty filters raises ValueError"""
        with pytest.raises(ValueError, match="parameters and filters cannot be empty"):
            connected_db.update("users", parameters={"age": 31}, filters={})
    
    def test_update_invalid_table_name(self, connected_db):
        """Test update with invalid table name raises ValueError"""
        with pytest.raises(ValueError, match="Invalid SQL identifier"):
            connected_db.update("invalid-table", parameters={"age": 31}, filters={"name": "Alice"})
    
    def test_update_rollback_on_error(self, connected_db):
        """Test update rolls back on error"""
        connected_db.execute("INSERT INTO users (name, email, age) VALUES ('Alice', 'alice@test.com', 30)")
        
        # Create constraint for testing
        connected_db.execute("CREATE UNIQUE INDEX idx_email_unique ON users(email)")
        connected_db.execute("INSERT INTO users (name, email, age) VALUES ('Bob', 'bob@test.com', 25)")
        
        # Try to update to duplicate email - should fail and rollback
        with pytest.raises(DatabaseError):
            connected_db.update("users", parameters={"email": "alice@test.com"}, filters={"name": "Bob"})
        
        # Verify Bob's email was not updated
        result = connected_db.select("users", filters={"name": "Bob"})
        assert result.iloc[0]["email"] == "bob@test.com"


class TestSQLiteConnectionDelete:
    """Test cases for delete method"""
    
    def test_delete_single_row(self, connected_db):
        """Test delete single row"""
        connected_db.execute("INSERT INTO users (name, email, age) VALUES ('Alice', 'alice@test.com', 30)")
        connected_db.execute("INSERT INTO users (name, email, age) VALUES ('Bob', 'bob@test.com', 25)")
        
        count = connected_db.delete("users", filters={"name": "Alice"})
        
        assert count == 1
        
        # Verify deletion
        result = connected_db.select("users")
        assert len(result) == 1
        assert result.iloc[0]["name"] == "Bob"
    
    def test_delete_multiple_rows(self, connected_db):
        """Test delete multiple rows"""
        connected_db.execute("INSERT INTO users (name, email, age) VALUES ('Alice', 'alice@test.com', 30)")
        connected_db.execute("INSERT INTO users (name, email, age) VALUES ('Bob', 'bob@test.com', 30)")
        connected_db.execute("INSERT INTO users (name, email, age) VALUES ('Charlie', 'charlie@test.com', 25)")
        
        count = connected_db.delete("users", filters={"age": 30})
        
        assert count == 2
        
        # Verify deletion
        result = connected_db.select("users")
        assert len(result) == 1
        assert result.iloc[0]["name"] == "Charlie"
    
    def test_delete_with_null_filter(self, connected_db):
        """Test delete with NULL filter"""
        connected_db.execute("INSERT INTO users (name, age) VALUES ('Alice', 30)")
        connected_db.execute("INSERT INTO users (name, email, age) VALUES ('Bob', 'bob@test.com', 30)")
        
        count = connected_db.delete("users", filters={"email": None})
        
        assert count == 1
        
        # Verify deletion
        result = connected_db.select("users")
        assert len(result) == 1
        assert result.iloc[0]["name"] == "Bob"
    
    def test_delete_no_matching_rows(self, connected_db):
        """Test delete with no matching rows returns 0"""
        connected_db.execute("INSERT INTO users (name, email, age) VALUES ('Alice', 'alice@test.com', 30)")
        
        count = connected_db.delete("users", filters={"name": "NonExistent"})
        
        assert count == 0
        
        # Verify nothing was deleted
        result = connected_db.select("users")
        assert len(result) == 1
        assert result.iloc[0]["name"] == "Alice"
    
    def test_delete_empty_filters(self, connected_db):
        """Test delete with empty filters raises ValueError"""
        with pytest.raises(ValueError, match="filters cannot be empty"):
            connected_db.delete("users", filters={})
    
    def test_delete_invalid_table_name(self, connected_db):
        """Test delete with invalid table name raises ValueError"""
        with pytest.raises(ValueError, match="Invalid SQL identifier"):
            connected_db.delete("invalid-table", filters={"name": "Alice"})
    
    def test_delete_rollback_on_error(self, connected_db):
        """Test delete with invalid table raises DatabaseError"""
        connected_db.execute("INSERT INTO users (name, email, age) VALUES ('Alice', 'alice@test.com', 30)")
        
        # Try to delete from non-existent table
        with pytest.raises(DatabaseError):
            connected_db.delete("nonexistent_table", filters={"name": "Alice"})
        
        # Verify user data is still intact
        result = connected_db.select("users")
        assert len(result) == 1
        assert result.iloc[0]["name"] == "Alice"


class TestSQLiteConnectionExecute:
    """Test cases for execute method"""
    
    def test_execute_create_table(self, connected_db):
        """Test execute CREATE TABLE query"""
        cursor = connected_db.execute("""
            CREATE TABLE test_table (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL
            )
        """)
        
        assert cursor is not None
        assert connected_db.table_exists("test_table")
    
    def test_execute_insert_with_params(self, connected_db):
        """Test execute INSERT with parameters"""
        cursor = connected_db.execute(
            "INSERT INTO users (name, email, age) VALUES (?, ?, ?)",
            params=("Alice", "alice@test.com", 30)
        )
        
        assert cursor is not None
        
        # Verify insertion
        result = connected_db.select("users")
        assert len(result) == 1
        assert result.iloc[0]["name"] == "Alice"
    
    def test_execute_select_with_params(self, connected_db):
        """Test execute SELECT with parameters"""
        connected_db.execute("INSERT INTO users (name, email, age) VALUES ('Alice', 'alice@test.com', 30)")
        connected_db.execute("INSERT INTO users (name, email, age) VALUES ('Bob', 'bob@test.com', 25)")
        
        cursor = connected_db.execute(
            "SELECT * FROM users WHERE age > ?",
            params=(28,),
            commit=False
        )
        
        results = cursor.fetchall()
        assert len(results) == 1
        assert results[0]["name"] == "Alice"
    
    def test_execute_without_commit(self, connected_db, temp_db_path):
        """Test execute without auto-commit"""
        connected_db.execute(
            "INSERT INTO users (name, email, age) VALUES ('Alice', 'alice@test.com', 30)",
            commit=False
        )

        # Should not be visible until commit. This behavior depends on isolation level
        # Open a second connection to verify data is NOT visible yet
        with SQLiteConnection(temp_db_path, primary_key_column="id") as db2:
            result_not_committed = db2.select("users")
            assert len(result_not_committed) == 0, "Data should not be visible from another connection before commit"
        
        # Now commit the transaction
        connected_db.db_connection.commit()
        
        # Verify data is now visible from the original connection
        result = connected_db.select("users")
        assert len(result) == 1
        assert result.iloc[0]["name"] == "Alice"

        # Verify data is also visible from a new connection
        with SQLiteConnection(temp_db_path, primary_key_column="id") as db3:
            result_new_connection = db3.select("users")
            assert len(result_new_connection) == 1
            assert result_new_connection.iloc[0]["name"] == "Alice"
    
    def test_execute_error_handling(self, connected_db):
        """Test execute raises DatabaseError on execution failure"""
        with pytest.raises(DatabaseError, match="Error executing query"):
            connected_db.execute("INVALID SQL QUERY")
    
    def test_execute_rollback_on_error_with_commit(self, connected_db, temp_db_path):
        """Test execute rolls back on error when commit=True"""
        connected_db.execute("INSERT INTO users (name, email, age) VALUES ('Alice', 'alice@test.com', 30)", commit=True)
        
        # Try to execute invalid SQL
        with pytest.raises(DatabaseError, match="Error executing query"):
            connected_db.execute("INVALID SQL COMMAND")
        
        # Verify that the previous insert is still present
        result = connected_db.select("users")
        assert len(result) == 1
        assert result.iloc[0]["name"] == "Alice"

        # Verify data is still visible from a new connection
        with SQLiteConnection(temp_db_path, primary_key_column="id") as db2:
            result_new_connection = db2.select("users")
            assert len(result_new_connection) == 1
            assert result_new_connection.iloc[0]["name"] == "Alice"

    def test_execute_rollback_without_commit(self, connected_db, temp_db_path):
        """Test execute without auto-commit allows rollback"""
        # Insert without committing
        connected_db.execute("INSERT INTO users (name, email, age) VALUES ('Alice', 'alice@test.com', 30)", commit=False)
        
        # Rollback the transaction
        connected_db.db_connection.rollback()
        
        # Verify data was not persisted
        result = connected_db.select("users")
        assert len(result) == 0, "Data should be rolled back"

        # Verify data is not visible from a new connection
        with SQLiteConnection(temp_db_path, primary_key_column="id") as db2:
            result_new_connection = db2.select("users")
            assert len(result_new_connection) == 0

class TestSQLiteConnectionTableInfo:
    """Test cases for get_table_info method"""
    
    def test_get_table_info(self, connected_db):
        """Test get_table_info returns table schema"""
        # First ensure the users table actually exists with data
        connected_db.execute("INSERT INTO users (name, email) VALUES ('Test', 'test@test.com')")
        
        info = connected_db.get_table_info("users")
        
        assert isinstance(info, pd.DataFrame)
        assert len(info) > 0
        assert "name" in info.columns
    
    def test_get_table_info_invalid_table(self, connected_db):
        """Test get_table_info with invalid table name raises ValueError"""
        with pytest.raises(ValueError, match="Invalid SQL identifier"):
            connected_db.get_table_info("invalid-table")

    def test_get_table_info_from_nonexistent_table(self, connected_db):
        """Test get_table_info with invalid table name raises ValueError"""
        with pytest.raises(DatabaseError, match="does not exist"):
            connected_db.get_table_info("nonexistent_table")
            

class TestSQLiteConnectionTableExists:
    """Test cases for table_exists method"""
    
    def test_table_exists_true(self, connected_db):
        """Test table_exists returns True for existing table"""
        assert connected_db.table_exists("users") is True
    
    def test_table_exists_false(self, connected_db):
        """Test table_exists returns False for non-existing table"""
        assert connected_db.table_exists("nonexistent_table") is False
    
    def test_table_exists_invalid_name(self, connected_db):
        """Test table_exists with invalid table name raises ValueError"""
        with pytest.raises(ValueError, match="Invalid SQL identifier"):
            connected_db.table_exists("invalid-table")


class TestSQLiteConnectionDestructor:
    """Test cases for destructor"""
    
    def test_destructor_closes_connection(self, temp_db_path):
        """Test destructor closes connection"""
        db = SQLiteConnection(temp_db_path)
        db._connect_db()
        assert db.is_connected()
        
        # Trigger destructor
        del db


class TestSQLiteConnectionIntegration:
    """Integration test cases"""
    
    def test_full_crud_workflow(self, connected_db):
        """Test complete CRUD workflow"""
        # Create
        rows = [
            {"name": "Alice", "email": "alice@test.com", "age": 30},
            {"name": "Bob", "email": "bob@test.com", "age": 25}
        ]
        insert_result = connected_db.insert("users", rows)
        assert len(insert_result) == 2
        
        # Read
        select_result = connected_db.select("users", filters={"name": "Alice"})
        assert len(select_result) == 1
        assert select_result.iloc[0]["name"] == "Alice"
        assert select_result.iloc[0]["age"] == 30
        alice_id = int(select_result.iloc[0]["id"])
        
        # Update - use return_updated_rows=False and verify separately
        connected_db.update(
            "users",
            parameters={"age": 31},
            filters={"id": alice_id},
            return_updated_rows=False
        )
        
        # Verify update
        update_result = connected_db.select("users", filters={"id": alice_id})
        assert len(update_result) == 1
        assert update_result.iloc[0]["name"] == "Alice"
        assert update_result.iloc[0]["age"] == 31
        
        # Delete
        delete_count = connected_db.delete("users", filters={"id": alice_id})
        assert delete_count == 1
        
        # Verify
        final_result = connected_db.select("users")
        assert len(final_result) == 1
        assert final_result.iloc[0]["name"] == "Bob"
    
    def test_transaction_rollback(self, connected_db):
        """Test transaction rollback on error"""
        connected_db.execute("INSERT INTO users (name, email, age) VALUES ('Alice', 'alice@test.com', 30)")
        
        # Create unique constraint
        connected_db.execute("CREATE UNIQUE INDEX idx_email ON users(email)")
        
        try:
            # Start transaction
            connected_db.execute("INSERT INTO users (name, email, age) VALUES ('Bob', 'bob@test.com', 25)", commit=False)
            # This should fail due to duplicate email constraint
            connected_db.execute("INSERT INTO users (name, email, age) VALUES ('Charlie', 'alice@test.com', 35)", commit=False)
            connected_db.db_connection.commit()
        except DatabaseError:
            connected_db.db_connection.rollback()
        
        # Verify only Alice exists (Bob should be rolled back)
        result = connected_db.select("users")
        assert len(result) == 1
        assert result.iloc[0]["name"] == "Alice"
    
    def test_concurrent_operations_with_context_manager(self, temp_db_path):
        """Test multiple operations using context manager"""
        with SQLiteConnection(temp_db_path, primary_key_column="id") as db:
            db.execute("""
                CREATE TABLE products (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    name TEXT NOT NULL,
                    price REAL
                )
            """)
            
            # Insert products
            products = [
                {"name": "Product A", "price": 10.99},
                {"name": "Product B", "price": 20.99},
                {"name": "Product C", "price": 15.99}
            ]
            db.insert("products", products, return_inserted=False)
            
            # Query and update
            result = db.select("products", filters={"name": "Product A"})
            product_id = int(result.iloc[0]["id"])
            
            # Update the price
            db.update("products", parameters={"price": 12.99}, filters={"id": product_id}, return_updated_rows=False)
            
            # Verify - disconnect and reconnect to ensure transaction is committed
        
        # Reopen connection to verify the update persisted
        with SQLiteConnection(temp_db_path, primary_key_column="id") as db:
            final = db.select("products", order_by="price ASC")
            assert final.iloc[0]["name"] == "Product A"
            assert final.iloc[0]["price"] == 12.99


class TestSQLiteConnectionTimestamps:
    """Test cases for handling timestamp data in SQLite"""
    
    def test_insert_timestamp_as_iso_string(self, connected_db_with_timestamps):
        """Test inserting timestamps as ISO 8601 strings"""
        now = datetime.now(timezone.utc)
        iso_timestamp = now.isoformat()
        
        rows = [{
            "event_name": "User Login",
            "created_at": iso_timestamp,
            "event_date": now.strftime("%Y-%m-%d")
        }]
        
        result = connected_db_with_timestamps.insert("events", rows)
        
        assert len(result) == 1
        assert result.iloc[0]["event_name"] == "User Login"
        assert result.iloc[0]["created_at"] == iso_timestamp
        assert result.iloc[0]["event_date"] == now.strftime("%Y-%m-%d")
    
    def test_insert_multiple_timestamps(self, connected_db_with_timestamps):
        """Test inserting records with multiple timestamp fields"""
        now = datetime.now(timezone.utc)
        future = now + timedelta(days=7)
        
        rows = [{
            "event_name": "Conference",
            "created_at": now.isoformat(),
            "updated_at": now.isoformat(),
            "scheduled_for": future.isoformat(),
            "event_date": future.strftime("%Y-%m-%d")
        }]
        
        result = connected_db_with_timestamps.insert("events", rows)
        
        assert len(result) == 1
        assert result.iloc[0]["created_at"] == now.isoformat()
        assert result.iloc[0]["updated_at"] == now.isoformat()
        assert result.iloc[0]["scheduled_for"] == future.isoformat()
        assert result.iloc[0]["event_date"] == future.strftime("%Y-%m-%d")
    
    def test_select_with_parse_dates(self, connected_db_with_timestamps):
        """Test selecting records with automatic date parsing"""
        now = datetime.now(timezone.utc)
        
        rows = [{
            "event_name": "System Backup",
            "created_at": now.isoformat(),
            "event_date": now.strftime("%Y-%m-%d")
        }]
        
        connected_db_with_timestamps.insert("events", rows, return_inserted=False)
        
        # Select with parse_dates
        result = connected_db_with_timestamps.select(
            "events",
            parse_dates={"created_at": "%Y-%m-%dT%H:%M:%S.%f%z"}
        )
        
        assert len(result) == 1
        assert isinstance(result.iloc[0]["created_at"], pd.Timestamp)
    
    def test_select_timestamps_with_timezone_localization(self, connected_db_with_timestamps):
        """Test selecting timestamps and localizing to specific timezone"""
        # Insert timestamp without timezone info
        naive_dt = datetime(2025, 1, 15, 10, 30, 0)
        
        rows = [{
            "event_name": "Meeting",
            "created_at": naive_dt.isoformat(),
            "event_date": naive_dt.strftime("%Y-%m-%d")
        }]
        
        connected_db_with_timestamps.insert("events", rows, return_inserted=False)
        
        # Select and localize to UTC
        result = connected_db_with_timestamps.select(
            "events",
            parse_dates={"created_at": "%Y-%m-%dT%H:%M:%S"},
            localize_timezone=timezone.utc
        )
        
        assert len(result) == 1
        assert isinstance(result.iloc[0]["created_at"], pd.Timestamp)
        assert result.iloc[0]["created_at"].tzinfo == timezone.utc
    
    def test_filter_by_timestamp_range(self, connected_db_with_timestamps):
        """Test filtering records by timestamp range"""
        base_time = datetime(2025, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
        
        # Insert events across different dates
        events = [
            {"event_name": "Event 1", "created_at": base_time.isoformat(), "event_date": "2025-01-01"},
            {"event_name": "Event 2", "created_at": (base_time + timedelta(days=5)).isoformat(), "event_date": "2025-01-06"},
            {"event_name": "Event 3", "created_at": (base_time + timedelta(days=10)).isoformat(), "event_date": "2025-01-11"},
        ]
        
        connected_db_with_timestamps.insert("events", events, return_inserted=False)
        
        # Filter by specific date using event_date
        result = connected_db_with_timestamps.select(
            "events",
            filters={"event_date": "2025-01-06"}
        )
        
        assert len(result) == 1
        assert result.iloc[0]["event_name"] == "Event 2"
    
    def test_update_timestamp_field(self, connected_db_with_timestamps):
        """Test updating timestamp fields"""
        initial_time = datetime(2025, 1, 1, 10, 0, 0, tzinfo=timezone.utc)
        updated_time = datetime(2025, 1, 2, 15, 30, 0, tzinfo=timezone.utc)
        
        rows = [{
            "event_name": "Task",
            "created_at": initial_time.isoformat(),
            "updated_at": initial_time.isoformat(),
            "event_date": "2025-01-01"
        }]
        
        connected_db_with_timestamps.insert("events", rows, return_inserted=False)
        
        # Update the timestamp
        result = connected_db_with_timestamps.update(
            "events",
            parameters={"updated_at": updated_time.isoformat()},
            filters={"event_name": "Task"}
        )
        
        assert len(result) == 1
        assert result.iloc[0]["updated_at"] == updated_time.isoformat()
        assert result.iloc[0]["created_at"] == initial_time.isoformat()  # Unchanged
    
    def test_update_with_parse_dates(self, connected_db_with_timestamps):
        """Test updating records with automatic date parsing"""
        initial_time = datetime(2025, 1, 1, 10, 0, 0, tzinfo=timezone.utc)
        updated_time = datetime(2025, 1, 2, 15, 30, 0, tzinfo=timezone.utc)
        
        rows = [{
            "event_name": "Task",
            "created_at": initial_time.isoformat(),
            "updated_at": initial_time.isoformat(),
            "event_date": "2025-01-01"
        }]
        
        connected_db_with_timestamps.insert("events", rows, return_inserted=False)
        
        # Update the timestamp
        result = connected_db_with_timestamps.update(
            "events",
            parameters={"updated_at": updated_time.isoformat()},
            filters={"event_name": "Task"},
            parse_dates={"updated_at": "%Y-%m-%dT%H:%M:%S%z"}
        )
        
        assert len(result) == 1
        assert isinstance(result.iloc[0]["updated_at"], pd.Timestamp)
        assert result.iloc[0]["created_at"] == initial_time.isoformat()

    def test_insert_null_timestamps(self, connected_db_with_timestamps):
        """Test inserting records with NULL timestamps for optional fields"""
        now = datetime.now(timezone.utc)
        
        rows = [{
            "event_name": "Pending Event",
            "created_at": now.isoformat(),
            "updated_at": None,  # NULL
            "completed_at": None,  # NULL
            "event_date": now.strftime("%Y-%m-%d")
        }]
        
        result = connected_db_with_timestamps.insert("events", rows)
        
        assert len(result) == 1
        assert pd.isna(result.iloc[0]["updated_at"])
        assert pd.isna(result.iloc[0]["completed_at"])
        assert not pd.isna(result.iloc[0]["created_at"])
    
    def test_update_timestamp_to_null(self, connected_db_with_timestamps):
        """Test updating timestamp to NULL value"""
        now = datetime.now(timezone.utc)
        
        rows = [{
            "event_name": "Scheduled Task",
            "created_at": now.isoformat(),
            "scheduled_for": (now + timedelta(days=1)).isoformat(),
            "event_date": now.strftime("%Y-%m-%d")
        }]
        
        connected_db_with_timestamps.insert("events", rows, return_inserted=False)
        
        result = connected_db_with_timestamps.update(
            "events",
            parameters={"scheduled_for": None},
            filters={"event_name": "Scheduled Task"}
        )
        
        assert len(result) == 1
        assert pd.isna(result.iloc[0]["scheduled_for"])
    
    def test_select_null_timestamps(self, connected_db_with_timestamps):
        """Test selecting records with NULL timestamps"""
        now = datetime.now(timezone.utc)
        
        events = [
            {"event_name": "Completed", "created_at": now.isoformat(), "completed_at": now.isoformat(), "event_date": "2025-01-01"},
            {"event_name": "Pending", "created_at": now.isoformat(), "completed_at": None, "event_date": "2025-01-01"},
        ]
        
        connected_db_with_timestamps.insert("events", events, return_inserted=False)
        
        # Select only pending (NULL completed_at)
        result = connected_db_with_timestamps.select(
            "events",
            filters={"completed_at": None}
        )
        
        assert len(result) == 1
        assert result.iloc[0]["event_name"] == "Pending"
    
    def test_order_by_timestamp(self, connected_db_with_timestamps):
        """Test ordering results by timestamp"""
        base_time = datetime(2025, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
        
        events = [
            {"event_name": "Third", "created_at": (base_time + timedelta(hours=20)).isoformat(), "event_date": "2025-01-01"},
            {"event_name": "First", "created_at": base_time.isoformat(), "event_date": "2025-01-01"},
            {"event_name": "Second", "created_at": (base_time + timedelta(hours=10)).isoformat(), "event_date": "2025-01-01"},
        ]
        
        connected_db_with_timestamps.insert("events", events, return_inserted=False)
        
        # Order by created_at ascending
        result = connected_db_with_timestamps.select(
            "events",
            order_by="created_at ASC"
        )
        
        assert len(result) == 3
        assert list(result["event_name"]) == ["First", "Second", "Third"]
    
    def test_delete_by_timestamp(self, connected_db_with_timestamps):
        """Test deleting records based on timestamp filters"""
        base_time = datetime(2025, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
        old_date = "2024-12-01"
        
        events = [
            {"event_name": "Old Event", "created_at": base_time.isoformat(), "event_date": old_date},
            {"event_name": "New Event", "created_at": base_time.isoformat(), "event_date": "2025-01-15"},
        ]
        
        connected_db_with_timestamps.insert("events", events, return_inserted=False)
        
        # Delete old events
        count = connected_db_with_timestamps.delete(
            "events",
            filters={"event_date": old_date}
        )
        
        assert count == 1
        
        # Verify only new event remains
        result = connected_db_with_timestamps.select("events")
        assert len(result) == 1
        assert result.iloc[0]["event_name"] == "New Event"
    
    def test_timestamp_workflow_order_tracking(self, connected_db_with_timestamps):
        """Test complete workflow with order lifecycle timestamps"""
        # Create order
        order_date = datetime(2025, 11, 1, 9, 0, 0, tzinfo=timezone.utc)
        order = [{
            "order_number": "ORD-12345",
            "customer_name": "John Doe",
            "amount": 199.99,
            "order_date": order_date.isoformat(),
            "shipped_date": None,
            "delivered_date": None
        }]
        
        result = connected_db_with_timestamps.insert("orders", order)
        order_id = int(result.iloc[0]["id"])
        
        # Ship order (update shipped_date)
        shipped_date = order_date + timedelta(days=2)
        connected_db_with_timestamps.update(
            "orders",
            parameters={"shipped_date": shipped_date.isoformat()},
            filters={"id": order_id},
            return_updated_rows=False
        )
        
        # Deliver order (update delivered_date)
        delivered_date = shipped_date + timedelta(days=3)
        result = connected_db_with_timestamps.update(
            "orders",
            parameters={"delivered_date": delivered_date.isoformat()},
            filters={"id": order_id}
        )
        
        # Verify complete lifecycle
        assert len(result) == 1
        assert result.iloc[0]["order_number"] == "ORD-12345"
        assert result.iloc[0]["order_date"] == order_date.isoformat()
        assert result.iloc[0]["shipped_date"] == shipped_date.isoformat()
        assert result.iloc[0]["delivered_date"] == delivered_date.isoformat()
    
    def test_timestamp_with_different_formats(self, connected_db_with_timestamps):
        """Test handling timestamps in different string formats"""
        # ISO format with timezone
        iso_with_tz = "2025-01-15T10:30:00+00:00"
        # ISO format without timezone
        iso_without_tz = "2025-01-15T10:30:00"
        # Date only
        date_only = "2025-01-15"
        
        events = [
            {"event_name": "Event 1", "created_at": iso_with_tz, "event_date": date_only},
            {"event_name": "Event 2", "created_at": iso_without_tz, "event_date": date_only},
        ]
        
        result = connected_db_with_timestamps.insert("events", events)
        
        assert len(result) == 2
        assert result.iloc[0]["created_at"] == iso_with_tz
        assert result.iloc[1]["created_at"] == iso_without_tz
        assert result.iloc[0]["event_date"] == date_only
        assert result.iloc[1]["event_date"] == date_only
    
    def test_timestamp_edge_cases(self, connected_db_with_timestamps):
        """Test edge cases with timestamps"""
        # Very old date
        old_date = datetime(1970, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
        # Future date
        future_date = datetime(2099, 12, 31, 23, 59, 59, tzinfo=timezone.utc)
        
        events = [
            {"event_name": "Old Event", "created_at": old_date.isoformat(), "event_date": "1970-01-01"},
            {"event_name": "Future Event", "created_at": future_date.isoformat(), "event_date": "2099-12-31"},
        ]
        
        result = connected_db_with_timestamps.insert("events", events)
        
        assert len(result) == 2
        assert result.iloc[0]["created_at"] == old_date.isoformat()
        assert result.iloc[1]["created_at"] == future_date.isoformat()
    
    def test_query_recent_events_with_timestamp_comparison(self, connected_db_with_timestamps):
        """Test querying recent events using raw SQL with timestamp comparison"""
        base_time = datetime(2025, 11, 1, 0, 0, 0, tzinfo=timezone.utc)
        
        events = [
            {"event_name": "Event 1", "created_at": (base_time - timedelta(days=10)).isoformat(), "event_date": "2025-10-22"},
            {"event_name": "Event 2", "created_at": (base_time - timedelta(days=3)).isoformat(), "event_date": "2025-10-29"},
            {"event_name": "Event 3", "created_at": base_time.isoformat(), "event_date": "2025-11-01"},
        ]
        
        connected_db_with_timestamps.insert("events", events, return_inserted=False)
        
        # Query events from last 5 days using execute
        cutoff_date = (base_time - timedelta(days=5)).isoformat()
        cursor = connected_db_with_timestamps.execute(
            "SELECT * FROM events WHERE created_at >= ? ORDER BY created_at DESC",
            params=(cutoff_date,),
            commit=False
        )
        
        results = cursor.fetchall()
        assert len(results) == 2
        assert results[0]["event_name"] == "Event 3"
        assert results[1]["event_name"] == "Event 2"
    
    def test_batch_insert_with_timestamps(self, connected_db_with_timestamps):
        """Test batch inserting multiple records with timestamps"""
        base_time = datetime(2025, 11, 9, 12, 0, 0, tzinfo=timezone.utc)
        
        # Create 10 events with incrementing timestamps
        events = [
            {
                "event_name": f"Event {i}",
                "created_at": (base_time + timedelta(minutes=i*5)).isoformat(),
                "event_date": (base_time + timedelta(minutes=i*5)).strftime("%Y-%m-%d")
            }
            for i in range(10)
        ]
        
        result = connected_db_with_timestamps.insert("events", events)
        
        assert len(result) == 10
        # Verify timestamps are sequential
        for i in range(10):
            expected_time = (base_time + timedelta(minutes=i*5)).isoformat()
            assert result.iloc[i]["created_at"] == expected_time


class TestSQLiteConnectionDtype:
    """Test cases for dtype parameter in SQLite operations"""
    
    @pytest.fixture
    def connected_db_with_mixed_types(self, temp_db_path):
        """Provide a connected SQLiteConnection instance with mixed data types table"""
        db = SQLiteConnection(temp_db_path, primary_key_column="id")
        db._connect_db()
        
        # Create table with various data types stored as TEXT/INTEGER/REAL
        db.execute("""
            CREATE TABLE IF NOT EXISTS products (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                price REAL,
                quantity INTEGER,
                is_available INTEGER,
                rating REAL,
                sku TEXT,
                discount_percent REAL,
                stock_level INTEGER,
                category_id INTEGER
            )
        """)
        
        yield db
        db._disconnect_db()
    
    def test_select_with_dtype_conversion_integers(self, connected_db_with_mixed_types):
        """Test select with dtype parameter converting to specific integer types"""
        # Insert test data
        products = [
            {"name": "Product A", "price": 19.99, "quantity": 100, "is_available": 1, "rating": 4.5, "sku": "SKU001"},
            {"name": "Product B", "price": 29.99, "quantity": 50, "is_available": 1, "rating": 4.8, "sku": "SKU002"},
            {"name": "Product C", "price": 9.99, "quantity": 200, "is_available": 0, "rating": 3.2, "sku": "SKU003"},
        ]
        connected_db_with_mixed_types.insert("products", products, return_inserted=False)
        
        # Select with dtype conversion
        dtype_map = {
            "quantity": "int32",
            "is_available": "int8",
        }
        
        result = connected_db_with_mixed_types.select("products", dtype=dtype_map)
        
        assert len(result) == 3
        assert result["quantity"].dtype == "int32"
        assert result["is_available"].dtype == "int8"
        assert result.iloc[0]["quantity"] == 100
        assert result.iloc[1]["is_available"] == 1
    
    def test_select_with_dtype_conversion_floats(self, connected_db_with_mixed_types):
        """Test select with dtype parameter converting to specific float types"""
        products = [
            {"name": "Product A", "price": 19.99, "quantity": 100, "rating": 4.5},
            {"name": "Product B", "price": 29.99, "quantity": 50, "rating": 4.8},
        ]
        connected_db_with_mixed_types.insert("products", products, return_inserted=False)
        
        # Select with float32 conversion
        dtype_map = {
            "price": "float32",
            "rating": "float64",
        }
        
        result = connected_db_with_mixed_types.select("products", dtype=dtype_map)
        
        assert len(result) == 2
        assert result["price"].dtype == "float32"
        assert result["rating"].dtype == "float64"
        assert abs(result.iloc[0]["price"] - 19.99) < 0.01
        assert result.iloc[1]["rating"] == 4.8
    
    def test_select_with_dtype_conversion_strings(self, connected_db_with_mixed_types):
        """Test select with dtype parameter converting to string/object types"""
        products = [
            {"name": "Product A", "price": 19.99, "quantity": 100, "sku": "SKU001"},
            {"name": "Product B", "price": 29.99, "quantity": 50, "sku": "SKU002"},
        ]
        connected_db_with_mixed_types.insert("products", products, return_inserted=False)
        
        # Select with string conversion
        dtype_map = {
            "name": "string",
            "sku": "object",
        }
        
        result = connected_db_with_mixed_types.select("products", dtype=dtype_map)
        
        assert len(result) == 2
        assert result["name"].dtype == "string"
        assert result["sku"].dtype == "object"
        assert result.iloc[0]["name"] == "Product A"
        assert result.iloc[1]["sku"] == "SKU002"
    
    def test_select_with_dtype_conversion_boolean(self, connected_db_with_mixed_types):
        """Test select with dtype parameter converting integers to boolean"""
        products = [
            {"name": "Product A", "price": 19.99, "is_available": 1},
            {"name": "Product B", "price": 29.99, "is_available": 0},
            {"name": "Product C", "price": 39.99, "is_available": 1},
        ]
        connected_db_with_mixed_types.insert("products", products, return_inserted=False)
        
        # Select with boolean conversion
        dtype_map = {
            "is_available": "bool",
        }
        
        result = connected_db_with_mixed_types.select("products", dtype=dtype_map)
        
        assert len(result) == 3
        assert result["is_available"].dtype == "bool"
        assert result.iloc[0]["is_available"] == True
        assert result.iloc[1]["is_available"] == False
        assert result.iloc[2]["is_available"] == True
    
    def test_select_with_dtype_multiple_columns(self, connected_db_with_mixed_types):
        """Test select with dtype parameter converting multiple columns simultaneously"""
        products = [
            {"name": "Product A", "price": 19.99, "quantity": 100, "is_available": 1, "rating": 4.5, "sku": "SKU001"},
            {"name": "Product B", "price": 29.99, "quantity": 50, "is_available": 0, "rating": 4.8, "sku": "SKU002"},
        ]
        connected_db_with_mixed_types.insert("products", products, return_inserted=False)
        
        # Select with multiple dtype conversions
        dtype_map = {
            "quantity": "int16",
            "is_available": "bool",
            "price": "float32",
            "name": "string",
            "rating": "float64",
        }
        
        result = connected_db_with_mixed_types.select("products", dtype=dtype_map)
        
        assert len(result) == 2
        assert result["quantity"].dtype == "int16"
        assert result["is_available"].dtype == "bool"
        assert result["price"].dtype == "float32"
        assert result["name"].dtype == "string"
        assert result["rating"].dtype == "float64"
        
        # Verify values are correct after conversion
        assert result.iloc[0]["quantity"] == 100
        assert result.iloc[0]["is_available"] == True
        assert abs(result.iloc[0]["price"] - 19.99) < 0.01
        assert result.iloc[0]["name"] == "Product A"
        assert result.iloc[1]["rating"] == 4.8
    
    def test_select_with_dtype_and_filters(self, connected_db_with_mixed_types):
        """Test select with dtype parameter combined with filters"""
        products = [
            {"name": "Product A", "price": 19.99, "quantity": 100, "is_available": 1},
            {"name": "Product B", "price": 29.99, "quantity": 50, "is_available": 1},
            {"name": "Product C", "price": 9.99, "quantity": 200, "is_available": 0},
        ]
        connected_db_with_mixed_types.insert("products", products, return_inserted=False)
        
        dtype_map = {
            "quantity": "int32",
            "is_available": "bool",
            "price": "float32",
        }
        
        result = connected_db_with_mixed_types.select(
            "products",
            filters={"is_available": 1},
            dtype=dtype_map
        )
        
        assert len(result) == 2
        assert result["is_available"].dtype == "bool"
        assert all(result["is_available"] == True)
        assert result["quantity"].dtype == "int32"
        assert result["price"].dtype == "float32"
    
    def test_select_with_dtype_and_columns(self, connected_db_with_mixed_types):
        """Test select with dtype parameter and specific column selection"""
        products = [
            {"name": "Product A", "price": 19.99, "quantity": 100, "rating": 4.5},
            {"name": "Product B", "price": 29.99, "quantity": 50, "rating": 4.8},
        ]
        connected_db_with_mixed_types.insert("products", products, return_inserted=False)
        
        dtype_map = {
            "quantity": "int16",
            "price": "float32",
        }
        
        result = connected_db_with_mixed_types.select(
            "products",
            columns=["name", "quantity", "price"],
            dtype=dtype_map
        )
        
        assert len(result) == 2
        assert len(result.columns) == 3
        assert result["quantity"].dtype == "int16"
        assert result["price"].dtype == "float32"
        assert "rating" not in result.columns
    
    def test_insert_with_dtype_return_converted(self, connected_db_with_mixed_types):
        """Test insert with dtype parameter to convert returned DataFrame"""
        products = [
            {"name": "Product A", "price": 19.99, "quantity": 100, "is_available": 1, "rating": 4.5},
            {"name": "Product B", "price": 29.99, "quantity": 50, "is_available": 0, "rating": 4.8},
        ]
        
        dtype_map = {
            "quantity": "int32",
            "is_available": "bool",
            "price": "float32",
        }
        
        result = connected_db_with_mixed_types.insert("products", products, dtype=dtype_map)
        
        assert len(result) == 2
        assert result["quantity"].dtype == "int32"
        assert result["is_available"].dtype == "bool"
        assert result["price"].dtype == "float32"
        assert result.iloc[0]["quantity"] == 100
        assert result.iloc[0]["is_available"] == True
        assert result.iloc[1]["is_available"] == False
        assert abs(result.iloc[1]["price"] - 29.99) < 0.01
    
    def test_insert_with_dtype_null_values(self, connected_db_with_mixed_types):
        """Test insert with dtype parameter and NULL values"""
        products = [
            {"name": "Product A", "price": 19.99, "quantity": 100, "rating": None},
            {"name": "Product B", "price": None, "quantity": 50, "rating": 4.8},
        ]
        
        dtype_map = {
            "quantity": "int32",
            "price": "float32",
            "rating": "float64",
        }
        
        result = connected_db_with_mixed_types.insert("products", products, dtype=dtype_map)
        
        assert len(result) == 2
        assert result["quantity"].dtype == "int32"
        assert result["price"].dtype == "float32"
        assert result["rating"].dtype == "float64"
        assert pd.isna(result.iloc[0]["rating"])
        assert pd.isna(result.iloc[1]["price"])
        assert abs(result.iloc[0]["price"] - 19.99) < 0.01
        assert result.iloc[0]["quantity"] == 100
        assert result.iloc[1]["rating"] == 4.8
        assert result.iloc[1]["quantity"] == 50
    
    def test_update_with_dtype_return_converted(self, connected_db_with_mixed_types):
        """Test update with dtype parameter to convert returned DataFrame"""
        products = [
            {"name": "Product A", "price": 19.99, "quantity": 100, "is_available": 1},
            {"name": "Product B", "price": 29.99, "quantity": 50, "is_available": 1},
        ]
        connected_db_with_mixed_types.insert("products", products, return_inserted=False)
        
        dtype_map = {
            "quantity": "int16",
            "is_available": "bool",
            "price": "float32",
        }
        
        result = connected_db_with_mixed_types.update(
            "products",
            parameters={"quantity": 150, "price": 24.99},
            filters={"name": "Product A"},
            dtype=dtype_map
        )
        
        assert len(result) == 1
        assert result["quantity"].dtype == "int16"
        assert result["is_available"].dtype == "bool"
        assert result["price"].dtype == "float32"
        assert result.iloc[0]["quantity"] == 150
        assert abs(result.iloc[0]["price"] - 24.99) < 0.01
    
    def test_update_with_dtype_multiple_rows(self, connected_db_with_mixed_types):
        """Test update with dtype parameter affecting multiple rows"""
        products = [
            {"name": "Product A", "price": 19.99, "quantity": 100, "category_id": 1},
            {"name": "Product B", "price": 29.99, "quantity": 50, "category_id": 1},
            {"name": "Product C", "price": 9.99, "quantity": 200, "category_id": 2},
        ]
        connected_db_with_mixed_types.insert("products", products, return_inserted=False)
        
        dtype_map = {
            "quantity": "int32",
            "category_id": "int16",
            "price": "float64",
        }
        
        result = connected_db_with_mixed_types.update(
            "products",
            parameters={"quantity": 75},
            filters={"category_id": 1},
            dtype=dtype_map
        )
        
        assert len(result) == 2
        assert result["quantity"].dtype == "int32"
        assert result["category_id"].dtype == "int16"
        assert result["price"].dtype == "float64"
        assert all(result["quantity"] == 75)
        assert all(result["category_id"] == 1)
    
    def test_dtype_with_category_type(self, connected_db_with_mixed_types):
        """Test dtype parameter with pandas category type"""
        products = [
            {"name": "Product A", "price": 19.99, "sku": "SKU001"},
            {"name": "Product B", "price": 29.99, "sku": "SKU002"},
            {"name": "Product C", "price": 9.99, "sku": "SKU001"},
        ]
        connected_db_with_mixed_types.insert("products", products, return_inserted=False)
        
        dtype_map = {
            "sku": "category",
        }
        
        result = connected_db_with_mixed_types.select("products", dtype=dtype_map)
        
        assert len(result) == 3
        assert result["sku"].dtype.name == "category"
        assert result["sku"].nunique() == 2
        assert list(result["sku"].cat.categories) == ["SKU001", "SKU002"]
    
    def test_dtype_with_large_integers(self, connected_db_with_mixed_types):
        """Test dtype parameter with different integer sizes"""
        products = [
            {"name": "Product A", "quantity": 1000000, "stock_level": 100},
            {"name": "Product B", "quantity": 2000000, "stock_level": 50},
        ]
        connected_db_with_mixed_types.insert("products", products, return_inserted=False)
        
        # Test int64 for large numbers
        dtype_map = {
            "quantity": "int64",
            "stock_level": "int8",
        }
        
        result = connected_db_with_mixed_types.select("products", dtype=dtype_map)
        
        assert len(result) == 2
        assert result["quantity"].dtype == "int64"
        assert result["stock_level"].dtype == "int8"
        assert result.iloc[0]["quantity"] == 1000000
        assert result.iloc[1]["stock_level"] == 50
    
    def test_dtype_conversion_with_order_by(self, connected_db_with_mixed_types):
        """Test dtype parameter combined with ORDER BY clause"""
        products = [
            {"name": "Product C", "price": 39.99, "quantity": 30},
            {"name": "Product A", "price": 19.99, "quantity": 100},
            {"name": "Product B", "price": 29.99, "quantity": 50},
        ]
        connected_db_with_mixed_types.insert("products", products, return_inserted=False)
        
        dtype_map = {
            "quantity": "int32",
            "price": "float32",
        }
        
        result = connected_db_with_mixed_types.select(
            "products",
            order_by="price ASC",
            dtype=dtype_map
        )
        
        assert len(result) == 3
        assert result["quantity"].dtype == "int32"
        assert result["price"].dtype == "float32"
        assert list(result["name"]) == ["Product A", "Product B", "Product C"]
        assert list(result["quantity"]) == [100, 50, 30]
    
    def test_dtype_conversion_with_limit(self, connected_db_with_mixed_types):
        """Test dtype parameter combined with LIMIT clause"""
        products = [
            {"name": f"Product {i}", "price": 10.0 + i, "quantity": 100 + i}
            for i in range(10)
        ]
        connected_db_with_mixed_types.insert("products", products, return_inserted=False)
        
        dtype_map = {
            "quantity": "int16",
            "price": "float32",
        }
        
        result = connected_db_with_mixed_types.select(
            "products",
            limit=5,
            dtype=dtype_map
        )
        
        assert len(result) == 5
        assert list(result["name"]) == [f"Product {i}" for i in range(5)]
        assert result["quantity"].dtype == "int16"
        assert result["price"].dtype == "float32"
    
    def test_dtype_empty_result(self, connected_db_with_mixed_types):
        """Test dtype parameter with query returning no results"""
        dtype_map = {
            "quantity": "int32",
            "price": "float32",
        }
        
        result = connected_db_with_mixed_types.select(
            "products",
            filters={"name": "NonExistent"},
            dtype=dtype_map
        )
        
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 0
        assert result["quantity"].dtype == "int32"
        assert result["price"].dtype == "float32"
    
    def test_dtype_with_discount_calculation(self, connected_db_with_mixed_types):
        """Test dtype parameter in a realistic scenario with calculations"""
        products = [
            {"name": "Product A", "price": 100.0, "discount_percent": 10.0, "quantity": 5},
            {"name": "Product B", "price": 200.0, "discount_percent": 15.0, "quantity": 3},
            {"name": "Product C", "price": 50.0, "discount_percent": 5.0, "quantity": 10},
        ]
        connected_db_with_mixed_types.insert("products", products, return_inserted=False)
        
        dtype_map = {
            "price": "float64",
            "discount_percent": "float32",
            "quantity": "int32",
        }
        
        result = connected_db_with_mixed_types.select("products", dtype=dtype_map)
        
        assert len(result) == 3
        assert result["price"].dtype == "float64"
        assert result["discount_percent"].dtype == "float32"
        assert result["quantity"].dtype == "int32"
        
        # Perform calculations with converted types
        result["discounted_price"] = result["price"] * (1 - result["discount_percent"] / 100)
        result["total_value"] = result["discounted_price"] * result["quantity"]
        
        assert abs(result.iloc[0]["discounted_price"] - 90.0) < 0.01
        assert abs(result.iloc[0]["total_value"] - 450.0) < 0.01
