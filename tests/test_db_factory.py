import pytest
import pandas as pd
from src.db import DatabaseFactory, create_connection, DatabaseConnection, SQLiteConnection, MySQLConnection
from src.error import DatabaseError


class TestDatabaseFactory:
    """Test suite for DatabaseFactory class."""
    
    def test_get_supported_types(self):
        """Test getting list of supported database types."""
        types = DatabaseFactory.get_supported_types()
        assert isinstance(types, list)
        assert "sqlite" in types
        assert "mysql" in types
    
    def test_is_supported(self):
        """Test checking if database type is supported."""
        assert DatabaseFactory.is_supported("sqlite") is True
        assert DatabaseFactory.is_supported("mongodb") is False
        assert DatabaseFactory.is_supported("invalid") is False
    
    def test_create_sqlite_connection(self, tmp_path):
        """Test creating SQLite connection through factory."""
        db_path = tmp_path / "test.db"

        db = DatabaseFactory.create_connection(
            db_type="sqlite",
            db_path=str(db_path),
            primary_key_column="id"
        )
        
        assert isinstance(db, SQLiteConnection)
        assert isinstance(db, DatabaseConnection)
        assert db.primary_key_column == "id"
        assert db.db_path == db_path

    def test_create_mysql_connection(self):
        """Test creating MySQL connection through factory."""

        db = DatabaseFactory.create_connection(
            db_type="mysql",
            host="localhost",
            port=3306,
            user="test_user",
            password="test_pass",
            database="test_db",
            primary_key_column="id"
        )
        
        assert isinstance(db, MySQLConnection)
        assert isinstance(db, DatabaseConnection)
        assert db.host == "localhost"
        assert db.port == 3306
        assert db.user == "test_user"
        assert db.password == "test_pass"
        assert db.database == "test_db"
        assert db.primary_key_column == "id"

    def test_create_connection_unsupported_type(self):
        """Test creating connection with unsupported database type."""
        with pytest.raises(ValueError) as exc_info:
            DatabaseFactory.create_connection(db_type="mongodb") # type: ignore
        
        assert f"Unsupported database type: 'mongodb'" in str(exc_info.value)
        assert "Supported types" in str(exc_info.value)
    
    def test_create_connection_missing_params(self):
        """Test creating connection with missing required parameters."""
        with pytest.raises(TypeError) as exc_info:
            DatabaseFactory.create_connection(db_type="sqlite")
        
        assert "Invalid connection parameters" in str(exc_info.value)
    
    def test_register_custom_connector(self):
        """Test registering a custom database connector."""
        
        class CustomDBConnection(DatabaseConnection):
            """Mock custom database connector."""
            
            def __init__(self, connection_string: str, primary_key_column: str | None = None):
                super().__init__(primary_key_column)
                self.connection_string = connection_string
            
            def _connect_db(self, **kwargs):
                return None, None
            
            def _disconnect_db(self):
                pass
            
            def _rollback(self):
                pass
            
            def is_connected(self):
                return False
            
            def select(self, table_name, columns=None, filters=None, order_by=None, limit=None, dtype=None, parse_dates=None, localize_timezone=None):
                return pd.DataFrame()
            
            def insert(self, table_name, rows, return_inserted=True, dtype=None, parse_dates=None, localize_timezone=None):
                return None
            
            def update(self, table_name, parameters, filters, return_updated_rows=True, dtype=None, parse_dates=None, localize_timezone=None):
                return None
            
            def delete(self, table_name, filters):
                return 0
            
            def execute(self, sql, params=None, commit=True):
                return None
            
            def table_exists(self, table_name):
                return False
            
            def get_table_info(self, table_name):
                return pd.DataFrame()
        
        # Register custom connector
        DatabaseFactory.register_connector("customdb", CustomDBConnection)
        
        # Verify registration
        assert DatabaseFactory.is_supported("customdb")
        assert "customdb" in DatabaseFactory.get_supported_types()
        
        # Create connection using custom connector
        db = DatabaseFactory.create_connection(
            db_type="customdb", # type: ignore
            connection_string="custom://localhost/test"
        )
        
        assert isinstance(db, CustomDBConnection)
        assert db.connection_string == "custom://localhost/test"
        
        # Cleanup: unregister for other tests
        DatabaseFactory._CONNECTORS.pop("customdb", None)
    
    def test_register_duplicate_connector(self):
        """Test that registering duplicate connector raises error."""
        
        class DuplicateConnection(DatabaseConnection):
            def _connect_db(self, **kwargs): pass # type: ignore
            def _disconnect_db(self): pass
            def _rollback(self): pass
            def is_connected(self): return False
            def select(self, *args, **kwargs): return pd.DataFrame()
            def insert(self, *args, **kwargs): return None
            def update(self, *args, **kwargs): return None
            def delete(self, *args, **kwargs): return 0
            def execute(self, *args, **kwargs): return None
            def table_exists(self, *args, **kwargs): return False
            def get_table_info(self, *args, **kwargs): return pd.DataFrame()
        
        with pytest.raises(ValueError, match="already registered") as exc_info:
            DatabaseFactory.register_connector("sqlite", DuplicateConnection)
    
    def test_register_invalid_connector_class(self):
        """Test that registering non-DatabaseConnection class raises error."""
        
        class InvalidConnection:
            """Not a DatabaseConnection subclass."""
            pass
        
        with pytest.raises(TypeError, match="must inherit from DatabaseConnection") as exc_info:
            DatabaseFactory.register_connector("invalid", InvalidConnection) # type: ignore


class TestCreateConnectionFunction:
    """Test suite for create_connection convenience function."""
    
    def test_create_connection_convenience(self, tmp_path):
        """Test convenience function for creating connections."""
        db_path = tmp_path / "test.db"
        
        db = create_connection(
            db_type="sqlite",
            db_path=str(db_path),
            primary_key_column="id"
        )
        
        assert isinstance(db, SQLiteConnection)
        assert db.primary_key_column == "id"
    
    def test_create_connection_with_context_manager(self, tmp_path):
        """Test convenience function with context manager."""
        db_path = tmp_path / "test.db"
        
        db = create_connection(db_type="sqlite", db_path=str(db_path))
        
        with db:
            # Create table
            db.execute("""
                CREATE TABLE test_table (
                    id INTEGER PRIMARY KEY,
                    name TEXT
                )
            """)
            
            # Insert data
            db.execute("INSERT INTO test_table (name) VALUES (?)", ("test",))
            
            # Query data
            df = db.select("test_table")
            assert len(df) == 1
            assert df.iloc[0]["name"] == "test"
        
        assert not db.is_connected()


class TestFactoryIntegration:
    """Integration tests for factory pattern with actual database operations."""
    
    @pytest.fixture
    def db(self, tmp_path):
        """Provide a test database connection."""
        db_path = tmp_path / "integration_test.db"
        db = create_connection(
            db_type="sqlite",
            db_path=str(db_path),
            primary_key_column="id"
        )
        
        with db:
            db.execute("""
                CREATE TABLE users (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    name TEXT NOT NULL,
                    email TEXT UNIQUE,
                    age INTEGER
                )
            """)
        
        return db
    
    def test_full_crud_cycle(self, db):
        """Test complete CRUD cycle using factory-created connection."""
        with db:
            # Create
            users = [
                {"name": "Alice", "email": "alice@test.com", "age": 30},
                {"name": "Bob", "email": "bob@test.com", "age": 25}
            ]
            inserted = db.insert("users", users)
            assert len(inserted) == 2
            
            # Read
            all_users = db.select("users", order_by="name ASC")
            assert len(all_users) == 2
            assert all_users.iloc[0]["name"] == "Alice"
            
            # Update
            updated = db.update(
                "users",
                parameters={"age": 31},
                filters={"name": "Alice"}
            )
            assert len(updated) == 1
            assert updated.loc[updated.name == "Alice", "age"].iloc[0] == 31
            
            # Delete
            deleted = db.delete("users", filters={"name": "Bob"})
            assert deleted == 1
            
            # Verify
            remaining = db.select("users")
            assert len(remaining) == 1
            assert remaining.iloc[0]["name"] == "Alice"
    
    def test_transaction_rollback_on_error(self, db):
        """Test that transactions rollback properly on error."""
        with db:
            # Insert initial data
            db.insert("users", [{"name": "Test User", "email": "test@test.com", "age": 25}])
            
            initial_count = len(db.select("users"))
        
        # Try to insert duplicate email (should fail)
        try:
            with db:
                db.insert("users", [
                    {"name": "Another User", "email": "test@test.com", "age": 30}  # Duplicate email
                ])
        except DatabaseError:
            pass  # Expected error
        
        # Verify rollback - count should remain the same
        with db:
            final_count = len(db.select("users"))
            assert final_count == initial_count
            assert db.select("users").iloc[0]["name"] == "Test User"

        assert not db.is_connected()
    
    def test_multiple_connections(self, tmp_path):
        """Test creating multiple independent connections."""
        db1_path = tmp_path / "db1.db"
        db2_path = tmp_path / "db2.db"
        
        db1 = create_connection(db_type="sqlite", db_path=str(db1_path))
        db2 = create_connection(db_type="sqlite", db_path=str(db2_path))
        
        # Create different tables in each database
        with db1:
            db1.execute("CREATE TABLE table1 (id INTEGER PRIMARY KEY, data TEXT)")
            db1.execute("INSERT INTO table1 (data) VALUES ('db1 data')")
        
        with db2:
            db2.execute("CREATE TABLE table2 (id INTEGER PRIMARY KEY, data TEXT)")
            db2.execute("INSERT INTO table2 (data) VALUES ('db2 data')")
        
        # Verify isolation
        with db1:
            assert db1.table_exists("table1")
            assert not db1.table_exists("table2")
        
        with db2:
            assert db2.table_exists("table2")
            assert not db2.table_exists("table1")
