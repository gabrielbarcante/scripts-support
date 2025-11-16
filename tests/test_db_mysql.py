import pytest
import pandas as pd
from datetime import timezone, datetime
import pytz
import sqlalchemy
import sqlalchemy.exc

from src.db.mysql import MySQLConnection
from src.error import DatabaseError


@pytest.fixture
def mysql_connection():
    """Fixture to create a MySQLConnection instance."""
    return MySQLConnection(
        host="localhost",
        port=3306,
        user="test_user",
        password="test_pass",
        database="test_db",
        primary_key_column="id"
    )


@pytest.fixture
def mock_engine(mocker):
    """Fixture to create a mock SQLAlchemy engine."""
    engine = mocker.MagicMock()
    mocker.patch("sqlalchemy.create_engine", return_value=engine)
    return engine


@pytest.fixture
def mock_connection(mock_engine, mocker):
    """Fixture to create a mock database connection."""
    connection = mocker.MagicMock()
    mock_engine.connect.return_value.__enter__.return_value = connection
    return connection


class TestMySQLConnectionInit:
    """Tests for MySQLConnection initialization."""
    
    def test_init_with_all_parameters(self):
        """Test initialization with all parameters."""
        conn = MySQLConnection(
            host="localhost",
            port=3306,
            user="user",
            password="pass",
            database="db",
            primary_key_column="id"
        )
        assert conn.host == "localhost"
        assert conn.port == 3306
        assert conn.user == "user"
        assert conn.password == "pass"
        assert conn.database == "db"
        assert conn.primary_key_column == "id"
        assert conn.db_engine is None
    
    def test_init_without_primary_key(self):
        """Test initialization without primary key column."""
        conn = MySQLConnection(
            host="localhost",
            port=3306,
            user="user",
            password="pass",
            database="db"
        )
        assert conn.primary_key_column is None


class TestConnectDB:
    """Tests for database connection."""
    
    def test_connect_db_success(self, mysql_connection, mock_engine, mocker):
        """Test successful database connection."""
        mock_create_engine = mocker.patch("sqlalchemy.create_engine", return_value=mock_engine)
        result = mysql_connection._connect_db()
        
        assert result == mock_engine
        assert mysql_connection.db_engine == mock_engine
        mock_create_engine.assert_called_once_with("mysql+pymysql://test_user:test_pass@localhost:3306/test_db")
    
    def test_connect_db_with_special_characters(self, mocker):
        """Test connection with special characters in credentials."""
        mock_engine = mocker.MagicMock()
        mock_create_engine = mocker.patch("sqlalchemy.create_engine", return_value=mock_engine)
        
        conn = MySQLConnection(
            host="localhost",
            port=3306,
            user="user@domain",
            password="p@ss:word!",
            database="db"
        )
        
        conn._connect_db()
        
        # Verify URL encoding
        call_args = mock_create_engine.call_args[0][0]
        assert "user%40domain" in call_args  # @ encoded
        assert "p%40ss%3Aword%21" in call_args  # Special chars encoded
        assert "mysql+pymysql://user%40domain:p%40ss%3Aword%21@localhost:3306/db" == call_args      
    
    def test_connect_db_already_connected(self, mysql_connection, mock_engine, mocker):
        """Test connecting when already connected."""
        mock_create_engine = mocker.patch("sqlalchemy.create_engine", return_value=mock_engine)

        mysql_connection._connect_db()
        first_engine = mysql_connection.db_engine
        
        # Call again
        result = mysql_connection._connect_db()
        
        assert result == first_engine
        assert mock_create_engine.call_count == 1
    
    def test_connect_db_failure(self, mysql_connection, mocker):
        """Test connection failure."""
        mocker.patch("sqlalchemy.create_engine", side_effect=sqlalchemy.exc.SQLAlchemyError("Connection failed"))
        
        with pytest.raises(DatabaseError) as exc_info:
            mysql_connection._connect_db()
        
        assert exc_info.value.code == "CONNECTION_ERROR"
        assert "Failed to connect to MySQL database: test_db" in str(exc_info.value)


class TestDisconnectDB:
    """Tests for database disconnection."""
    
    def test_disconnect_db_success(self, mysql_connection, mock_engine):
        """Test successful disconnection."""
        mysql_connection._connect_db()
        assert mysql_connection.db_engine is not None
        
        mysql_connection._disconnect_db()
        
        mock_engine.dispose.assert_called_once()
        assert mysql_connection.db_engine is None
    
    def test_disconnect_db_not_connected(self, mysql_connection):
        """Test disconnection when not connected."""
        mysql_connection._disconnect_db()
        assert mysql_connection.db_engine is None
    
    def test_disconnect_db_with_exception(self, mysql_connection, mock_engine):
        """Test disconnection handles exceptions gracefully."""
        mysql_connection._connect_db()
        mock_engine.dispose.side_effect = Exception("Dispose failed")
        
        mysql_connection._disconnect_db()
        
        assert mysql_connection.db_engine is None


class TestIsConnected:
    """Tests for connection status check."""
    
    def test_is_connected_true(self, mysql_connection, mock_engine):
        """Test is_connected returns True when connected."""
        mysql_connection._connect_db()
        assert mysql_connection.is_connected() is True
    
    def test_is_connected_false(self, mysql_connection):
        """Test is_connected returns False when not connected."""
        assert mysql_connection.is_connected() is False
    
    def test_is_connected_after_disconnect(self, mysql_connection, mock_engine):
        """Test is_connected returns False after disconnection."""
        mysql_connection._connect_db()
        assert mysql_connection.is_connected() is True
        
        mysql_connection._disconnect_db()
        assert mysql_connection.is_connected() is False
    
    def test_is_connected_with_disposed_engine(self, mysql_connection, mock_engine):
        """Test is_connected when engine is set but disposed."""
        mysql_connection._connect_db()
        # Simulate engine being disposed externally
        mysql_connection.db_engine = None
        assert mysql_connection.is_connected() is False
    
    def test_is_connected_multiple_checks(self, mysql_connection, mock_engine):
        """Test multiple consecutive is_connected calls."""
        assert mysql_connection.is_connected() is False
        assert mysql_connection.is_connected() is False
        
        mysql_connection._connect_db()
        assert mysql_connection.is_connected() is True
        assert mysql_connection.is_connected() is True


class TestRollback:
    """Tests for transaction rollback."""
    
    def test_rollback_success(self, mysql_connection, mock_engine, mock_connection):
        """Test successful rollback."""
        mysql_connection._connect_db()
        mysql_connection._rollback()
        
        mock_connection.rollback.assert_called_once()
    
    def test_rollback_not_connected(self, mysql_connection):
        """Test rollback when not connected."""
        mysql_connection._rollback()


class TestSelect:
    """Tests for SELECT operations."""
    
    def test_select_all_columns(self, mysql_connection, mock_engine, mock_connection, mocker):
        """Test selecting all columns."""
        mock_df = pd.DataFrame({"id": [1, 2], "name": ["John", "Jane"]})
        mock_read_sql = mocker.patch("pandas.read_sql", return_value=mock_df)
        
        result = mysql_connection.select("users")
        
        assert result.equals(mock_df)
        mock_read_sql.assert_called_once()
        call_args = mock_read_sql.call_args
        assert "SELECT * FROM users" in call_args[0][0]
    
    def test_select_specific_columns(self, mysql_connection, mock_engine, mock_connection, mocker):
        """Test selecting specific columns."""
        mock_df = pd.DataFrame({"id": [1, 2]})
        mock_read_sql = mocker.patch("pandas.read_sql", return_value=mock_df)
        
        result = mysql_connection.select("users", columns=["id"])
        
        assert result.equals(mock_df)
        mock_read_sql.assert_called_once()
        call_args = mock_read_sql.call_args
        assert "SELECT id FROM users" in call_args[0][0]
    
    def test_select_with_filters(self, mysql_connection, mock_engine, mock_connection, mocker):
        """Test SELECT with WHERE filters."""
        mock_df = pd.DataFrame({"id": [1], "active": [True]})
        mock_read_sql = mocker.patch("pandas.read_sql", return_value=mock_df)
        
        result = mysql_connection.select("users", filters={"id": 1, "active": True})
        
        call_args = mock_read_sql.call_args
        query = call_args[0][0]
        assert "WHERE" in query
        assert "id = %s" in query
        assert "active = %s" in query
        assert "AND" in query
        assert call_args[1]["params"] == (1, True)
    
    def test_select_with_null_filter(self, mysql_connection, mock_engine, mock_connection, mocker):
        """Test SELECT with NULL filter."""
        mock_df = pd.DataFrame({"id": [1]})
        mock_read_sql = mocker.patch("pandas.read_sql", return_value=mock_df)
        
        result = mysql_connection.select("users", filters={"deleted_at": None})
        
        call_args = mock_read_sql.call_args
        assert "deleted_at IS NULL" in call_args[0][0]
    
    def test_select_with_order_by(self, mysql_connection, mock_engine, mock_connection, mocker):
        """Test SELECT with ORDER BY."""
        mock_df = pd.DataFrame({"id": [2, 1]})
        mock_read_sql = mocker.patch("pandas.read_sql", return_value=mock_df)
        
        result = mysql_connection.select("users", order_by="id DESC")
        
        call_args = mock_read_sql.call_args
        assert "ORDER BY id DESC" in call_args[0][0]
    
    def test_select_with_limit(self, mysql_connection, mock_engine, mock_connection, mocker):
        """Test SELECT with LIMIT."""
        mock_df = pd.DataFrame({"id": [1]})
        mock_read_sql = mocker.patch("pandas.read_sql", return_value=mock_df)
        
        result = mysql_connection.select("users", limit=10)
        
        call_args = mock_read_sql.call_args
        assert "LIMIT 10" in call_args[0][0]
    
    def test_select_with_invalid_limit(self, mysql_connection):
        """Test SELECT with invalid limit."""
        with pytest.raises(ValueError, match="limit must be a non-negative integer"):
            mysql_connection.select("users", limit=-1)
        
        with pytest.raises(ValueError, match="limit must be a non-negative integer"):
            mysql_connection.select("users", limit="10")
    
    def test_select_with_dtype_and_parse_dates(self, mysql_connection, mock_engine, mock_connection, mocker):
        """Test SELECT with dtype and parse_dates."""
        mock_df = pd.DataFrame({"id": [1], "created_at": [datetime.now()]})
        mock_read_sql = mocker.patch("pandas.read_sql", return_value=mock_df)
        
        dtype = {"id": "int64"}
        parse_dates = {"created_at": "%Y-%m-%d %H:%M:%S.%f"}
        
        result = mysql_connection.select("users", dtype=dtype, parse_dates=parse_dates)
        
        call_args = mock_read_sql.call_args
        assert call_args[1]["dtype"] == dtype
        assert call_args[1]["parse_dates"] == parse_dates
    
    def test_select_with_timezone_localization(self, mysql_connection, mock_engine, mock_connection, mocker):
        """Test SELECT with timezone localization."""
        mock_df = pd.DataFrame({
            "id": [1],
            "created_at": [pd.Timestamp("2024-01-01 12:00:00")]
        })
        assert mock_df["created_at"].dt.tz is None

        mock_read_sql = mocker.patch("pandas.read_sql", return_value=mock_df)
        
        tz = timezone.utc
        parse_dates = {"created_at": "%Y-%m-%d %H:%M:%S.%f"}
        
        result = mysql_connection.select("users", parse_dates=parse_dates, localize_timezone=tz)
        
        assert not result.empty
        assert result["created_at"].dt.tz is not None
    
    def test_select_invalid_table_name(self, mysql_connection):
        """Test SELECT with invalid table name."""
        with pytest.raises(ValueError, match="Invalid SQL identifier"):
            mysql_connection.select("users; DROP TABLE users;")
    
    def test_select_database_error(self, mysql_connection, mock_engine, mocker):
        """Test SELECT with database error."""
        mocker.patch("pandas.read_sql", side_effect=Exception("Query failed"))
        
        with pytest.raises(DatabaseError) as exc_info:
            mysql_connection.select("users")
        
        assert exc_info.value.code == "SELECT_ERROR"
        assert "Error executing SELECT on 'users'" in str(exc_info.value)


class TestInsert:
    """Tests for INSERT operations."""
    
    def test_insert_single_row(self, mysql_connection, mock_connection, mocker):
        """Test inserting a single row."""
        mock_table = mocker.MagicMock()
        mocker.patch("sqlalchemy.Table", return_value=mock_table)
        
        mock_result = mocker.MagicMock()
        mock_result.lastrowid = 1
        mock_connection.execute.return_value = mock_result
        
        rows = [{"name": "John", "age": 30}]
        result = mysql_connection.insert("users", rows, return_inserted=False)
        
        assert result is None
        mock_connection.commit.assert_called_once()
        mock_connection.execute.assert_called_once()
    
    def test_insert_multiple_rows(self, mysql_connection, mock_connection, mocker):
        """Test inserting multiple rows."""
        mock_table = mocker.MagicMock()
        mocker.patch("sqlalchemy.Table", return_value=mock_table)
        
        mock_result = mocker.MagicMock()
        mock_result.lastrowid = 1
        mock_connection.execute.return_value = mock_result
        
        rows = [
            {"name": "John", "age": 30},
            {"name": "Jane", "age": 25}
        ]
        result = mysql_connection.insert("users", rows, return_inserted=False)
        
        assert result is None
        mock_connection.commit.assert_called_once()
        assert mock_connection.execute.call_count == 1
    
    def test_insert_with_return_inserted(self, mysql_connection, mock_connection, mocker):
        """Test insert with return_inserted=True."""
        mock_table = mocker.MagicMock()
        mocker.patch("sqlalchemy.Table", return_value=mock_table)
        
        mock_result = mocker.MagicMock()
        mock_result.lastrowid = 1
        mock_connection.execute.return_value = mock_result
        
        mock_df = pd.DataFrame({"id": [1, 2], "name": ["John", "Jane"]})
        mock_read_sql = mocker.patch("pandas.read_sql", return_value=mock_df)
        
        rows = [{"name": "John"}, {"name": "Jane"}]
        result = mysql_connection.insert("users", rows, return_inserted=True)
        
        assert result.equals(mock_df)
        mock_read_sql.assert_called_once()
        call_args = mock_read_sql.call_args
        assert "SELECT * FROM users WHERE id IN" in call_args[0][0]
    
    def test_insert_with_return_inserted_no_primary_key(self, mocker):
        """Test insert with return_inserted=True but no primary key configured."""
        mock_table = mocker.MagicMock()
        mocker.patch("sqlalchemy.Table", return_value=mock_table)

        conn = MySQLConnection(
            host="localhost",
            port=3306,
            user="test_user",
            password="test_pass",
            database="test_db"
        )
        
        mock_engine = mocker.MagicMock()
        mocker.patch("sqlalchemy.create_engine", return_value=mock_engine)
        connection = mocker.MagicMock()
        mock_engine.connect.return_value.__enter__.return_value = connection
        
        rows = [{"name": "John"}]
        
        assert conn.insert("users", rows, return_inserted=True) is None
    
    def test_insert_with_special_characters(self, mysql_connection, mock_connection, mocker):
        """Test inserting rows with special characters."""
        mock_table = mocker.MagicMock()
        mocker.patch("sqlalchemy.Table", return_value=mock_table)
        
        mock_result = mocker.MagicMock()
        mock_result.lastrowid = 1
        mock_connection.execute.return_value = mock_result
        
        rows = [{"name": "O'Brien", "email": "test@example.com"}]
        result = mysql_connection.insert("users", rows, return_inserted=False)
        
        assert result is None
        mock_connection.commit.assert_called_once()
    
    def test_insert_with_none_values(self, mysql_connection, mock_connection, mocker):
        """Test inserting rows with None values."""
        mock_table = mocker.MagicMock()
        mocker.patch("sqlalchemy.Table", return_value=mock_table)
        
        mock_result = mocker.MagicMock()
        mock_result.lastrowid = 1
        mock_connection.execute.return_value = mock_result
        
        rows = [{"name": "John", "middle_name": None, "age": 30}]
        result = mysql_connection.insert("users", rows, return_inserted=False)
        
        assert result is None
        mock_connection.commit.assert_called_once()
    
    def test_insert_empty_rows(self, mysql_connection):
        """Test insert with empty rows."""
        with pytest.raises(ValueError, match="rows cannot be empty"):
            mysql_connection.insert("users", [])
    
    def test_insert_inconsistent_columns(self, mysql_connection):
        """Test insert with inconsistent columns."""
        rows = [
            {"name": "John", "age": 30},
            {"name": "Jane"}  # Missing "age"
        ]
        with pytest.raises(ValueError, match="All rows must have the same columns"):
            mysql_connection.insert("users", rows)
    
    def test_insert_inconsistent_columns_different_keys(self, mysql_connection):
        """Test insert with completely different column sets."""
        rows = [
            {"name": "John", "age": 30},
            {"email": "jane@example.com", "status": "active"}
        ]
        with pytest.raises(ValueError, match="All rows must have the same columns"):
            mysql_connection.insert("users", rows)
    
    def test_insert_invalid_table_name(self, mysql_connection):
        """Test insert with invalid table name."""
        with pytest.raises(ValueError, match="Invalid SQL identifier"):
            mysql_connection.insert("users; DROP TABLE users;", [{"name": "John"}])
    
    def test_insert_with_invalid_column_names(self, mysql_connection):
        """Test insert with invalid column names."""
        rows = [{"name; DROP TABLE users;": "John"}]
        
        with pytest.raises(ValueError, match="Invalid SQL identifier"):
            mysql_connection.insert("users", rows)
    
    def test_insert_with_rollback_on_error(self, mysql_connection, mock_connection, mocker):
        """Test insert rolls back on error."""
        mock_table = mocker.MagicMock()
        mocker.patch("sqlalchemy.Table", return_value=mock_table)
        mock_connection.execute.side_effect = Exception("Insert failed")
        
        rows = [{"name": "John"}]
        
        with pytest.raises(DatabaseError) as exc_info:
            mysql_connection.insert("users", rows)
        
        assert exc_info.value.code == "INSERT_ERROR"
        assert "Error inserting into 'users'" in str(exc_info.value)
        mock_connection.rollback.assert_called_once()
    
    def test_insert_with_commit_failure(self, mysql_connection, mock_connection, mocker):
        """Test insert handles commit failure."""
        mock_table = mocker.MagicMock()
        mocker.patch("sqlalchemy.Table", return_value=mock_table)
        
        mock_result = mocker.MagicMock()
        mock_result.lastrowid = 1
        mock_connection.execute.return_value = mock_result
        mock_connection.commit.side_effect = Exception("Commit failed")
        
        rows = [{"name": "John"}]
        
        with pytest.raises(DatabaseError) as exc_info:
            mysql_connection.insert("users", rows)
        
        assert exc_info.value.code == "INSERT_ERROR"
        assert "Error inserting into 'users'" in str(exc_info.value)
        mock_connection.rollback.assert_called_once()
    
    def test_insert_large_batch(self, mysql_connection, mock_connection, mocker):
        """Test inserting a large batch of rows."""
        mock_table = mocker.MagicMock()
        mocker.patch("sqlalchemy.Table", return_value=mock_table)
        
        mock_result = mocker.MagicMock()
        mock_result.lastrowid = 1
        mock_connection.execute.return_value = mock_result
        
        rows = [{"name": f"User{i}", "age": 20 + i} for i in range(100)]
        result = mysql_connection.insert("users", rows, return_inserted=False)
        
        assert result is None
        mock_connection.commit.assert_called_once()
        assert mock_connection.execute.call_count == 1
    
    def test_insert_with_return_inserted_single_row(self, mysql_connection, mock_connection, mocker):
        """Test insert with return_inserted=True for single row."""
        mock_table = mocker.MagicMock()
        mocker.patch("sqlalchemy.Table", return_value=mock_table)
        
        mock_result = mocker.MagicMock()
        mock_result.lastrowid = 42
        mock_connection.execute.return_value = mock_result
        
        mock_df = pd.DataFrame({"id": [42], "name": ["John"]})
        mock_read_sql = mocker.patch("pandas.read_sql", return_value=mock_df)
        
        rows = [{"name": "John"}]
        result = mysql_connection.insert("users", rows, return_inserted=True)
        
        assert result.equals(mock_df)
        call_args = mock_read_sql.call_args
        assert "WHERE id IN (%s)" in call_args[0][0]
        assert call_args[1]["params"] == (42,)
    
    def test_insert_with_datetime_values(self, mysql_connection, mock_connection, mocker):
        """Test inserting rows with datetime values."""
        mock_table = mocker.MagicMock()
        mocker.patch("sqlalchemy.Table", return_value=mock_table)
        
        mock_result = mocker.MagicMock()
        mock_result.lastrowid = 1
        mock_connection.execute.return_value = mock_result
        
        rows = [{"name": "John", "created_at": datetime.now()}]
        result = mysql_connection.insert("users", rows, return_inserted=False)
        
        assert result is None
        mock_connection.commit.assert_called_once()
    
    def test_insert_with_boolean_values(self, mysql_connection, mock_connection, mocker):
        """Test inserting rows with boolean values."""
        mock_table = mocker.MagicMock()
        mocker.patch("sqlalchemy.Table", return_value=mock_table)
        
        mock_result = mocker.MagicMock()
        mock_result.lastrowid = 1
        mock_connection.execute.return_value = mock_result
        
        rows = [{"name": "John", "active": True, "verified": False}]
        result = mysql_connection.insert("users", rows, return_inserted=False)
        
        assert result is None
        mock_connection.commit.assert_called_once()
    
    def test_insert_with_numeric_types(self, mysql_connection, mock_connection, mocker):
        """Test inserting rows with various numeric types."""
        mock_table = mocker.MagicMock()
        mocker.patch("sqlalchemy.Table", return_value=mock_table)
        
        mock_result = mocker.MagicMock()
        mock_result.lastrowid = 1
        mock_connection.execute.return_value = mock_result
        
        rows = [{"age": 30, "salary": 50000.50, "score": 95}]
        result = mysql_connection.insert("users", rows, return_inserted=False)
        
        assert result is None
        mock_connection.commit.assert_called_once()
    
    def test_insert_with_empty_string_values(self, mysql_connection, mock_connection, mocker):
        """Test inserting rows with empty string values."""
        mock_table = mocker.MagicMock()
        mocker.patch("sqlalchemy.Table", return_value=mock_table)
        
        mock_result = mocker.MagicMock()
        mock_result.lastrowid = 1
        mock_connection.execute.return_value = mock_result
        
        rows = [{"name": "", "email": "test@example.com"}]
        result = mysql_connection.insert("users", rows, return_inserted=False)
        
        assert result is None
        mock_connection.commit.assert_called_once()
    
    def test_insert_table_reflection_error(self, mysql_connection, mock_connection, mocker):
        """Test insert when table reflection fails."""
        mocker.patch("sqlalchemy.Table", side_effect=Exception("Reflection failed"))
        
        rows = [{"name": "John"}]
        
        with pytest.raises(DatabaseError) as exc_info:
            mysql_connection.insert("users", rows)
        
        assert exc_info.value.code == "INSERT_ERROR"
        assert "Error inserting into 'users'" in str(exc_info.value)
        mock_connection.rollback.assert_called_once()
    
    def test_insert_with_return_inserted_empty_result(self, mysql_connection, mock_connection, mocker):
        """Test insert with return_inserted when query returns empty result."""
        mock_table = mocker.MagicMock()
        mocker.patch("sqlalchemy.Table", return_value=mock_table)
        
        mock_result = mocker.MagicMock()
        mock_result.lastrowid = 1
        mock_connection.execute.return_value = mock_result
        
        mock_df = pd.DataFrame()
        mock_read_sql = mocker.patch("pandas.read_sql", return_value=mock_df)
        
        rows = [{"name": "John"}]
        result = mysql_connection.insert("users", rows, return_inserted=True)
        
        assert result.empty
    
    def test_insert_with_dict_values(self, mysql_connection, mock_connection, mocker):
        """Test inserting rows containing dict-like values (should be serialized)."""
        mock_table = mocker.MagicMock()
        mocker.patch("sqlalchemy.Table", return_value=mock_table)
        
        mock_result = mocker.MagicMock()
        mock_result.lastrowid = 1
        mock_connection.execute.return_value = mock_result
        
        rows = [{"name": "John", "metadata": {"key": "value"}}]
        result = mysql_connection.insert("users", rows, return_inserted=False)
        
        assert result is None
        mock_connection.commit.assert_called_once()
    
    def test_insert_with_list_values(self, mysql_connection, mock_connection, mocker):
        """Test inserting rows containing list values."""
        mock_table = mocker.MagicMock()
        mocker.patch("sqlalchemy.Table", return_value=mock_table)
        
        mock_result = mocker.MagicMock()
        mock_result.lastrowid = 1
        mock_connection.execute.return_value = mock_result
        
        rows = [{"name": "John", "tags": ["python", "sql"]}]
        result = mysql_connection.insert("users", rows, return_inserted=False)
        
        assert result is None
        mock_connection.commit.assert_called_once()
    
    def test_insert_with_zero_lastrowid(self, mysql_connection, mock_connection, mocker):
        """Test insert when lastrowid is 0 (e.g., for tables without auto-increment)."""
        mock_table = mocker.MagicMock()
        mocker.patch("sqlalchemy.Table", return_value=mock_table)
        
        mock_result = mocker.MagicMock()
        mock_result.lastrowid = 0
        mock_connection.execute.return_value = mock_result
        
        rows = [{"uuid": "abc-123", "name": "John"}]
        result = mysql_connection.insert("users", rows, return_inserted=False)
        
        assert result is None
        mock_connection.commit.assert_called_once()
    
    def test_insert_with_return_inserted_multiple_ids(self, mysql_connection, mock_connection, mocker):
        """Test insert with return_inserted for multiple rows collects all IDs."""
        mock_table = mocker.MagicMock()
        mocker.patch("sqlalchemy.Table", return_value=mock_table)
        
        mock_result_1 = mocker.MagicMock()
        mock_result_1.lastrowid = 1
        mock_result_2 = mocker.MagicMock()
        mock_result_2.lastrowid = 2
        mock_result_3 = mocker.MagicMock()
        mock_result_3.lastrowid = 3
        
        mock_connection.execute.side_effect = [mock_result_1, mock_result_2, mock_result_3]
        
        mock_df = pd.DataFrame({"id": [1, 2, 3], "name": ["John", "Jane", "Bob"]})
        mock_read_sql = mocker.patch("pandas.read_sql", return_value=mock_df)
        
        rows = [{"name": "John"}, {"name": "Jane"}, {"name": "Bob"}]
        result = mysql_connection.insert("users", rows, return_inserted=True)
        
        assert result.equals(mock_df)
        call_args = mock_read_sql.call_args
        assert "WHERE id IN (%s, %s, %s)" in call_args[0][0]
        assert call_args[1]["params"] == (1, 2, 3)


class TestUpdate:
    """Tests for UPDATE operations."""
    
    def test_update_success(self, mysql_connection, mock_connection, mocker):
        """Test successful update."""
        mock_table = mocker.MagicMock()
        # Sets up the table's columns (c attribute) with id and name columns
        mock_table.c = {"id": mocker.MagicMock(), "name": mocker.MagicMock()}
        mocker.patch("sqlalchemy.Table", return_value=mock_table)
        
        # Creates a mock result object that simulates the database query execution result
        # Sets rowcount = 1 to indicate that 1 row was affected by the UPDATE
        # Configures the mock connection to return this result when execute() is called
        mock_result = mocker.MagicMock()
        mock_result.rowcount = 1
        mock_connection.execute.return_value = mock_result
        
        result = mysql_connection.update(
            "users",
            parameters={"name": "John Updated"},
            filters={"id": 1},
            return_updated_rows=False
        )
        
        assert result is None
        mock_connection.commit.assert_called_once()
    
    def test_update_with_return_updated(self, mysql_connection, mock_connection, mocker):
        """Test update with return_updated_rows=True."""
        mock_table = mocker.MagicMock()
        mock_table.c = {"id": mocker.MagicMock(), "name": mocker.MagicMock()}
        mocker.patch("sqlalchemy.Table", return_value=mock_table)
        
        mock_result = mocker.MagicMock()
        mock_result.rowcount = 1
        mock_connection.execute.return_value = mock_result
        
        mock_df = pd.DataFrame({"id": [1], "name": ["John Updated"]})
        mock_select = mocker.patch.object(mysql_connection, "select", return_value=mock_df)
        
        result = mysql_connection.update(
            "users",
            parameters={"name": "John Updated"},
            filters={"id": 1},
            return_updated_rows=True
        )
        
        assert result.equals(mock_df)
        mock_select.assert_called_once()
    
    def test_update_with_null_filter(self, mysql_connection, mock_connection, mocker):
        """Test update with NULL filter."""
        mock_table = mocker.MagicMock()
        mock_col = mocker.MagicMock()
        mock_table.c = {"deleted_at": mock_col, "status": mocker.MagicMock()}
        mocker.patch("sqlalchemy.Table", return_value=mock_table)
        
        mock_result = mocker.MagicMock()
        mock_result.rowcount = 0
        mock_connection.execute.return_value = mock_result
        
        result = mysql_connection.update(
            "users",
            parameters={"status": "active"},
            filters={"deleted_at": None},
            return_updated_rows=False
        )
        
        mock_col.is_.assert_called_once_with(None)
    
    def test_update_empty_parameters(self, mysql_connection):
        """Test update with empty parameters."""
        with pytest.raises(ValueError, match="parameters and filters cannot be empty"):
            mysql_connection.update("users", parameters={}, filters={"id": 1})
    
    def test_update_empty_filters(self, mysql_connection):
        """Test update with empty filters."""
        with pytest.raises(ValueError, match="parameters and filters cannot be empty"):
            mysql_connection.update("users", parameters={"name": "John"}, filters={})
    
    def test_update_with_rollback_on_error(self, mysql_connection, mock_connection, mocker):
        """Test update rolls back on error."""
        mock_table = mocker.MagicMock()
        mocker.patch("sqlalchemy.Table", return_value=mock_table)
        mock_connection.execute.side_effect = Exception("Update failed")
        
        with pytest.raises(DatabaseError) as exc_info:
            mysql_connection.update("users", {"name": "John"}, {"id": 1})
        
        assert exc_info.value.code == "UPDATE_ERROR"
        assert "Error updating data in 'users'" in str(exc_info.value)
    
    def test_update_with_dtype(self, mysql_connection, mock_connection, mocker):
        """Test update with dtype parameter for returned data."""
        mock_table = mocker.MagicMock()
        mock_table.c = {"id": mocker.MagicMock(), "age": mocker.MagicMock()}
        mocker.patch("sqlalchemy.Table", return_value=mock_table)
        
        mock_result = mocker.MagicMock()
        mock_result.rowcount = 1
        mock_connection.execute.return_value = mock_result
        
        mock_df = pd.DataFrame({"id": [1], "age": [30]})
        dtype = {"id": "int64", "age": "int32"}
        mock_select = mocker.patch.object(mysql_connection, "select", return_value=mock_df)
        
        result = mysql_connection.update(
            "users",
            parameters={"age": 30},
            filters={"id": 1},
            return_updated_rows=True,
            dtype=dtype
        )
        
        assert result.equals(mock_df)
        mock_select.assert_called_once()
        call_args = mock_select.call_args
        assert call_args[1]["dtype"] == dtype
    
    def test_update_with_parse_dates(self, mysql_connection, mock_connection, mocker):
        """Test update with parse_dates parameter for returned data."""
        mock_table = mocker.MagicMock()
        mock_table.c = {"id": mocker.MagicMock(), "updated_at": mocker.MagicMock()}
        mocker.patch("sqlalchemy.Table", return_value=mock_table)
        
        mock_result = mocker.MagicMock()
        mock_result.rowcount = 1
        mock_connection.execute.return_value = mock_result
        
        mock_df = pd.DataFrame({
            "id": [1],
            "updated_at": [pd.Timestamp("2024-01-01 12:00:00")]
        })
        parse_dates = {"updated_at": "%Y-%m-%d %H:%M:%S"}
        mock_select = mocker.patch.object(mysql_connection, "select", return_value=mock_df)
        
        result = mysql_connection.update(
            "users",
            parameters={"updated_at": datetime.now()},
            filters={"id": 1},
            return_updated_rows=True,
            parse_dates=parse_dates
        )
        
        assert result.equals(mock_df)
        mock_select.assert_called_once()
        call_args = mock_select.call_args
        assert call_args[1]["parse_dates"] == parse_dates
    
    def test_update_with_localize_timezone(self, mysql_connection, mock_connection, mocker):
        """Test update with localize_timezone parameter for returned data."""
        mock_table = mocker.MagicMock()
        mock_table.c = {"id": mocker.MagicMock(), "updated_at": mocker.MagicMock()}
        mocker.patch("sqlalchemy.Table", return_value=mock_table)
        
        mock_result = mocker.MagicMock()
        mock_result.rowcount = 1
        mock_connection.execute.return_value = mock_result
        
        mock_df = pd.DataFrame({
            "id": [1],
            "updated_at": [pd.Timestamp("2024-01-01 12:00:00", tz=pytz.timezone("America/Sao_Paulo"))]
        })
        assert mock_df["updated_at"].dt.tz == pytz.timezone("America/Sao_Paulo")

        parse_dates = {"updated_at": "%Y-%m-%d %H:%M:%S"}
        tz = timezone.utc
        mock_select = mocker.patch.object(mysql_connection, "select", return_value=mock_df)
        
        result = mysql_connection.update(
            "users",
            parameters={"updated_at": datetime.now()},
            filters={"id": 1},
            return_updated_rows=True,
            parse_dates=parse_dates,
            localize_timezone=tz
        )
        
        assert result.equals(mock_df)
        mock_select.assert_called_once()
        call_args = mock_select.call_args
        assert call_args[1]["parse_dates"] == parse_dates
        assert call_args[1]["localize_timezone"] == tz
        assert result["updated_at"].dt.tz == tz
    
    def test_update_with_all_data_type_parameters(self, mysql_connection, mock_connection, mocker):
        """Test update with dtype, parse_dates, and localize_timezone together."""
        mock_table = mocker.MagicMock()
        mock_table.c = {"id": mocker.MagicMock(), "name": mocker.MagicMock(), "created_at": mocker.MagicMock()}
        mocker.patch("sqlalchemy.Table", return_value=mock_table)
        
        mock_result = mocker.MagicMock()
        mock_result.rowcount = 2
        mock_connection.execute.return_value = mock_result
        
        mock_df = pd.DataFrame({
            "id": [1, 2],
            "name": ["John", "Jane"],
            "created_at": [
                pd.Timestamp("2024-01-01 12:00:00", tz=timezone.utc),
                pd.Timestamp("2024-01-02 12:00:00", tz=timezone.utc)
            ]
        })
        
        dtype = {"id": "int64"}
        parse_dates = {"created_at": "%Y-%m-%d %H:%M:%S"}
        tz = timezone.utc
        
        mock_select = mocker.patch.object(mysql_connection, "select", return_value=mock_df)
        
        result = mysql_connection.update(
            "users",
            parameters={"name": "Updated"},
            filters={"id": 1},
            return_updated_rows=True,
            dtype=dtype,
            parse_dates=parse_dates,
            localize_timezone=tz
        )
        
        assert result.equals(mock_df)
        mock_select.assert_called_once()
        call_args = mock_select.call_args
        assert call_args[1]["dtype"] == dtype
        assert call_args[1]["parse_dates"] == parse_dates
        assert call_args[1]["localize_timezone"] == tz
    
    def test_update_with_return_updated_rows_false_and_dtype(self, mysql_connection, mock_connection, mocker):
        """Test update with return_updated_rows=False ignores dtype/parse_dates."""
        mock_table = mocker.MagicMock()
        mock_table.c = {"id": mocker.MagicMock(), "name": mocker.MagicMock()}
        mocker.patch("sqlalchemy.Table", return_value=mock_table)
        
        mock_result = mocker.MagicMock()
        mock_result.rowcount = 1
        mock_connection.execute.return_value = mock_result
        
        dtype = {"id": "int64"}
        
        mock_select = mocker.patch.object(mysql_connection, "select")

        result = mysql_connection.update(
            "users",
            parameters={"name": "John Updated"},
            filters={"id": 1},
            return_updated_rows=False,
            dtype=dtype
        )
        
        assert result is None
        mock_connection.commit.assert_called_once()
        assert mock_select.call_count == 0
    
    def test_update_with_zero_rows_affected(self, mysql_connection, mock_connection, mocker):
        """Test update with zero rows affected returns None even with return_updated_rows=True."""
        mock_table = mocker.MagicMock()
        mock_table.c = {"id": mocker.MagicMock(), "name": mocker.MagicMock()}
        mocker.patch("sqlalchemy.Table", return_value=mock_table)
        
        mock_result = mocker.MagicMock()
        mock_result.rowcount = 0
        mock_connection.execute.return_value = mock_result
        
        result = mysql_connection.update(
            "users",
            parameters={"name": "John Updated"},
            filters={"id": 999},
            return_updated_rows=True
        )
        
        assert result is None
        mock_connection.commit.assert_called_once()
    
    def test_update_invalid_table_name(self, mysql_connection):
        """Test update with invalid table name."""
        with pytest.raises(ValueError, match="Invalid SQL identifier"):
            mysql_connection.update(
                "users; DROP TABLE users;",
                parameters={"name": "John"},
                filters={"id": 1}
            )
    
    def test_update_invalid_parameter_column_name(self, mysql_connection):
        """Test update with invalid parameter column name."""
        with pytest.raises(ValueError, match="Invalid SQL identifier"):
            mysql_connection.update(
                "users",
                parameters={"name; DROP TABLE users;": "John"},
                filters={"id": 1}
            )
    
    def test_update_invalid_filter_column_name(self, mysql_connection):
        """Test update with invalid filter column name."""
        with pytest.raises(ValueError, match="Invalid SQL identifier"):
            mysql_connection.update(
                "users",
                parameters={"name": "John"},
                filters={"id; DROP TABLE users;": 1}
            )
    
    def test_update_multiple_invalid_parameter_columns(self, mysql_connection):
        """Test update with multiple invalid parameter column names."""
        with pytest.raises(ValueError, match="Invalid SQL identifier"):
            mysql_connection.update(
                "users",
                parameters={
                    "name": "John",
                    "email' OR '1'='1": "test@example.com"
                },
                filters={"id": 1}
            )
    
    def test_update_multiple_invalid_filter_columns(self, mysql_connection):
        """Test update with multiple invalid filter column names."""
        with pytest.raises(ValueError, match="Invalid SQL identifier"):
            mysql_connection.update(
                "users",
                parameters={"name": "John"},
                filters={
                    "id": 1,
                    "status' OR '1'='1": "active"
                }
            )
    
    def test_update_sql_injection_attempt_in_table_name(self, mysql_connection):
        """Test update prevents SQL injection in table name."""
        with pytest.raises(ValueError, match="Invalid SQL identifier"):
            mysql_connection.update(
                "users WHERE 1=1--",
                parameters={"name": "John"},
                filters={"id": 1}
            )
    
    def test_update_sql_injection_attempt_in_columns(self, mysql_connection):
        """Test update prevents SQL injection in column names."""
        with pytest.raises(ValueError, match="Invalid SQL identifier"):
            mysql_connection.update(
                "users",
                parameters={"name' OR '1'='1' --": "John"},
                filters={"id": 1}
            )


class TestDelete:
    """Tests for DELETE operations."""
    
    def test_delete_success(self, mysql_connection, mock_connection, mocker):
        """Test successful delete."""
        mock_table = mocker.MagicMock()
        mock_table.c = {"id": mocker.MagicMock()}
        mocker.patch("sqlalchemy.Table", return_value=mock_table)
        
        mock_result = mocker.MagicMock()
        mock_result.rowcount = 1
        mock_connection.execute.return_value = mock_result
        
        result = mysql_connection.delete("users", filters={"id": 1})
        
        assert result == 1
        mock_connection.commit.assert_called_once()
    
    def test_delete_with_null_filter(self, mysql_connection, mock_connection, mocker):
        """Test delete with NULL filter."""
        mock_table = mocker.MagicMock()
        mock_col = mocker.MagicMock()
        mock_table.c = {"deleted_at": mock_col}
        mocker.patch("sqlalchemy.Table", return_value=mock_table)
        
        mock_result = mocker.MagicMock()
        mock_result.rowcount = 2
        mock_connection.execute.return_value = mock_result
        
        result = mysql_connection.delete("users", filters={"deleted_at": None})
        
        assert result == 2
        mock_col.is_.assert_called_once_with(None)
    
    def test_delete_empty_filters(self, mysql_connection):
        """Test delete with empty filters."""
        with pytest.raises(ValueError, match="filters cannot be empty"):
            mysql_connection.delete("users", filters={})
    
    def test_delete_with_rollback_on_error(self, mysql_connection, mock_connection, mocker):
        """Test delete rolls back on error."""
        mock_table = mocker.MagicMock()
        mocker.patch("sqlalchemy.Table", return_value=mock_table)
        mock_connection.execute.side_effect = Exception("Delete failed")
        
        with pytest.raises(DatabaseError) as exc_info:
            mysql_connection.delete("users", {"id": 1})
        
        assert exc_info.value.code == "DELETE_ERROR"
    
    def test_delete_invalid_table_name(self, mysql_connection):
        """Test delete with invalid table name."""
        with pytest.raises(ValueError, match="Invalid SQL identifier"):
            mysql_connection.delete(
                "users; DROP TABLE users;",
                filters={"id": 1}
            )
    
    def test_delete_invalid_filter_column_name(self, mysql_connection):
        """Test delete with invalid filter column name."""
        with pytest.raises(ValueError, match="Invalid SQL identifier"):
            mysql_connection.delete(
                "users",
                filters={"id; DROP TABLE users;": 1}
            )
    
    def test_delete_multiple_invalid_filter_columns(self, mysql_connection):
        """Test delete with multiple invalid filter column names."""
        with pytest.raises(ValueError, match="Invalid SQL identifier"):
            mysql_connection.delete(
                "users",
                filters={
                    "id": 1,
                    "status' OR '1'='1": "active"
                }
            )
    
    def test_delete_sql_injection_attempt_in_table_name(self, mysql_connection):
        """Test delete prevents SQL injection in table name."""
        with pytest.raises(ValueError, match="Invalid SQL identifier"):
            mysql_connection.delete(
                "users WHERE 1=1--",
                filters={"id": 1}
            )
    
    def test_delete_sql_injection_attempt_in_filter(self, mysql_connection):
        """Test delete prevents SQL injection in filter column names."""
        with pytest.raises(ValueError, match="Invalid SQL identifier"):
            mysql_connection.delete(
                "users",
                filters={"id' OR '1'='1' --": 1}
            )
    
    def test_delete_with_special_characters_in_column_name(self, mysql_connection):
        """Test delete with special characters in column name."""
        with pytest.raises(ValueError, match="Invalid SQL identifier"):
            mysql_connection.delete(
                "users",
                filters={"user@name": "test"}
            )
    
    def test_delete_with_backticks_in_table_name(self, mysql_connection):
        """Test delete with backticks in table name."""
        with pytest.raises(ValueError, match="Invalid SQL identifier"):
            mysql_connection.delete(
                "`users`; DROP TABLE users;",
                filters={"id": 1}
            )


class TestExecute:
    """Tests for custom SQL execution."""
    
    def test_execute_with_commit(self, mysql_connection, mock_connection, mocker):
        """Test execute with commit."""
        mock_result = mocker.MagicMock()
        mock_connection.execute.return_value = mock_result
        
        result = mysql_connection.execute("UPDATE users SET active = 1", commit=True)
        
        assert result == mock_result
        mock_connection.commit.assert_called_once()
    
    def test_execute_without_commit(self, mysql_connection, mock_connection, mocker):
        """Test execute without commit."""
        mock_result = mocker.MagicMock()
        mock_connection.execute.return_value = mock_result
        
        result = mysql_connection.execute("SELECT * FROM users", commit=False)
        
        assert result == mock_result
        mock_connection.commit.assert_not_called()
    
    def test_execute_with_parameters(self, mysql_connection, mock_connection, mocker):
        """Test execute with named parameters."""
        mock_result = mocker.MagicMock()
        mock_connection.execute.return_value = mock_result
        mock_text = mocker.patch("sqlalchemy.text")
        
        params = {"id": 1, "name": "John"}
        result = mysql_connection.execute(
            "UPDATE users SET name = :name WHERE id = :id",
            params=params
        )
        
        mock_text.assert_called_once()
        call_args = mock_connection.execute.call_args
        assert call_args[1]["parameters"] == params
    
    def test_execute_with_error_and_rollback(self, mysql_connection, mock_connection):
        """Test execute handles errors and rolls back."""
        mock_connection.execute.side_effect = Exception("Execution failed")
        
        with pytest.raises(DatabaseError) as exc_info:
            mysql_connection.execute("UPDATE users SET active = 1", commit=True)
        
        assert exc_info.value.code == "EXECUTE_SQL_ERROR"
        assert "Error executing query:" in str(exc_info.value)
        mock_connection.rollback.assert_called_once()

    def test_execute_with_error_not_commit(self, mysql_connection, mock_connection):
        """Test execute handles errors and rolls back."""
        mock_connection.execute.side_effect = Exception("Execution failed")
        
        with pytest.raises(DatabaseError) as exc_info:
            mysql_connection.execute("UPDATE users SET active = 1", commit=False)
        
        assert exc_info.value.code == "EXECUTE_SQL_ERROR"
        assert "Error executing query:" in str(exc_info.value)
        mock_connection.rollback.assert_not_called()


class TestTableExists:
    """Tests for table existence check."""
    
    def test_table_exists_true(self, mysql_connection, mock_connection, mocker):
        """Test table exists returns True."""
        mock_df = pd.DataFrame({"count": [1]})
        mock_read_sql = mocker.patch("pandas.read_sql", return_value=mock_df)
        
        result = mysql_connection.table_exists("users")
        
        assert result is True
        call_args = mock_read_sql.call_args
        assert "information_schema.tables" in call_args[0][0]
    
    def test_table_exists_false(self, mysql_connection, mock_connection, mocker):
        """Test table exists returns False."""
        mock_df = pd.DataFrame({"count": [0]})
        mocker.patch("pandas.read_sql", return_value=mock_df)
        
        result = mysql_connection.table_exists("nonexistent")
        
        assert result is False
    
    def test_table_exists_invalid_name(self, mysql_connection):
        """Test table exists with invalid name."""
        with pytest.raises(ValueError, match="Invalid SQL identifier"):
            mysql_connection.table_exists("users; DROP TABLE users;")
    
    def test_table_exists_error(self, mysql_connection, mock_engine, mocker):
        """Test table exists with database error."""
        mocker.patch("pandas.read_sql", side_effect=Exception("Query failed"))
        
        with pytest.raises(DatabaseError) as exc_info:
            mysql_connection.table_exists("users")
        
        assert exc_info.value.code == "TABLE_EXISTS_ERROR"
        assert "Error checking if table 'users' exists" in str(exc_info.value)


class TestGetTableInfo:
    """Tests for getting table schema information."""
    
    def test_get_table_info_success(self, mysql_connection, mock_engine, mocker):
        """Test getting table info successfully."""
        # Mock table_exists to return True
        mocker.patch.object(mysql_connection, "table_exists", return_value=True)
        
        mock_df = pd.DataFrame({
            "name": ["id", "name", "email"],
            "type": ["int", "varchar", "varchar"],
            "notnull": ["NO", "NO", "YES"],
            "dflt_value": [None, None, None],
            "pk": [1, 0, 0]
        })
        mocker.patch("pandas.read_sql", return_value=mock_df)
        
        result = mysql_connection.get_table_info("users")
        
        assert result.equals(mock_df)
        assert len(result) == 3
        assert result[result["pk"] == 1]["name"].iloc[0] == "id"
    
    def test_get_table_info_table_not_exists(self, mysql_connection, mocker):
        """Test get table info for non-existent table."""
        mocker.patch.object(mysql_connection, "table_exists", return_value=False)
        
        with pytest.raises(DatabaseError) as exc_info:
            mysql_connection.get_table_info("nonexistent")
        
        assert exc_info.value.code == "TABLE_NOT_FOUND"
        assert "Table 'nonexistent' does not exist" in str(exc_info.value)
    
    def test_get_table_info_invalid_name(self, mysql_connection):
        """Test get table info with invalid table name."""
        with pytest.raises(ValueError, match="Invalid SQL identifier"):
            mysql_connection.get_table_info("users; DROP TABLE users;")
    
    def test_get_table_info_error(self, mysql_connection, mock_engine, mocker):
        """Test get table info with database error."""
        mocker.patch.object(mysql_connection, "table_exists", return_value=True)
        mocker.patch("pandas.read_sql", side_effect=Exception("Query failed"))
        
        with pytest.raises(DatabaseError) as exc_info:
            mysql_connection.get_table_info("users")
        
        assert exc_info.value.code == "TABLE_INFO_ERROR"
        assert "Error getting table info for 'users'" in str(exc_info.value)
