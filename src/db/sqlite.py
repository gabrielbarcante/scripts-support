from pathlib import Path
import sqlite3
import pandas as pd
from datetime import timezone
from typing import Literal, Dict, Tuple, List, TypeAlias, Any

from ..error import DatabaseError
from .base import DatabaseConnection

ISOLATION_LEVEL: TypeAlias = Literal["DEFERRED", "IMMEDIATE", "EXCLUSIVE", None]

class SQLiteConnection(DatabaseConnection):
    """
    Safe interface for SQLite database operations with automatic transaction management.
    
    Provides CRUD operations using parameterized queries to prevent SQL injection.
    Supports context manager pattern for automatic connection lifecycle.
    
    Attributes:
        db_path (Path): Path to SQLite database file
        primary_key_column (str | None): Primary key column name for insert operations
        db_connection (sqlite3.Connection | None): Active database connection
        db_cursor (sqlite3.Cursor | None): Active database cursor
    
    Example:
        >>> with SQLiteConnection('app.db', primary_key_column='id') as db:
        ...     # Create table
        ...     db.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)")
        ...     # Insert data
        ...     db.insert('users', [{'name': 'John'}, {'name': 'Jane'}])
        ...     # Query data
        ...     df = db.select('users', filters={'name': 'John'})
    """
    
    def __init__(self, db_path: str, primary_key_column: str | None = None):
        """
        Initialize database connection interface.
        
        Args:
            db_path: Path to SQLite database file (created if doesn't exist)
            primary_key_column: Primary key column name (required for returning inserted records)
        
        Raises:
            ValueError: If primary_key_column contains invalid characters
        """
        super().__init__(primary_key_column)
        self.db_path = Path(db_path)
        self.db_connection: sqlite3.Connection | None = None
        self.db_cursor: sqlite3.Cursor | None = None

    def _rollback(self) -> None:
        """Rollback current transaction."""
        if self.db_connection:
            self.db_connection.rollback()

    def _connect_db(self, timeout: int = 10, isolation_level: ISOLATION_LEVEL = "DEFERRED", **kwargs) -> tuple[sqlite3.Connection, sqlite3.Cursor]:
        """
        Establish connection to SQLite database.
        
        Args:
            timeout: Lock acquisition timeout in seconds (default: 10)
            isolation_level: Transaction isolation level
                - None: autocommit mode
                - DEFERRED: lock acquired on first read/write (default, best for reads)
                - IMMEDIATE: lock acquired immediately (good for writes)
                - EXCLUSIVE: exclusive lock, blocks all clients
        
        Returns:
            Tuple of (connection, cursor) objects
        
        Raises:
            DatabaseError: If connection fails
        
        Note:
            Returns existing connection if already connected
        """
        if self.db_connection is not None and self.db_cursor is not None:
            return self.db_connection, self.db_cursor
        
        try:
            self.db_connection = sqlite3.connect(
                self.db_path, 
                timeout=timeout, 
                isolation_level=isolation_level
            )
            # Enable foreign key constraints enforcement in SQLite, so SQLite will:
            # Prevent inserting rows with invalid foreign key references; Prevent deleting parent rows that have dependent child rows; Enforce CASCADE, SET NULL, and other foreign key actions
            self.db_connection.execute("PRAGMA foreign_keys = ON")
            
            # Changes how query results are returned from tuples to dict-like objects. Access columns by name: row['name'] instead of row[0]
            self.db_connection.row_factory = sqlite3.Row
            
            self.db_cursor = self.db_connection.cursor()
            
            return self.db_connection, self.db_cursor
        
        except sqlite3.Error as e:
            raise DatabaseError(message=f"Failed to connect to database: {self.db_path}", code="CONNECTION_ERROR") from e

    def _disconnect_db(self) -> None:
        """Close database connection safely."""
        if "db_connection" in self.__dict__ and self.db_connection is not None:
            try:
                self.db_connection.close()
            except Exception:
                pass
            finally:
                self.db_connection = None
                self.db_cursor = None
    
    def is_connected(self) -> bool:
        """
        Check if database connection is active.
        
        Returns:
            True if connected and responsive
        """
        if self.db_connection is None:
            return False
        
        try:
            self.db_connection.execute("SELECT 1")
            return True
        except sqlite3.Error:
            return False

    def select(
        self, 
        table_name: str, 
        columns: list[str] | None = None, 
        filters: Dict[str, Any] | None = None, 
        order_by: str | None = None, 
        limit: int | None = None, 
        dtype: Dict | None = None, 
        parse_dates: Dict | None = None, 
        localize_timezone: timezone | None = None
    ) -> pd.DataFrame:
        """
        Query records from table with optional filtering and ordering.
        
        Args:
            table_name: Table to query
            columns: Columns to select (default: all columns)
            filters: WHERE conditions as {column: value}. Use None for IS NULL
            order_by: ORDER BY clause (e.g., 'name ASC', 'age DESC, name')
            limit: Maximum number of records to return
            dtype: Pandas dtype mapping for result columns
            parse_dates: Date columns to parse {column: format}
            localize_timezone: Timezone for datetime localization
        
        Returns:
            DataFrame with query results (empty if no matches)
        
        Raises:
            ValueError: If table_name or column names are invalid
            DatabaseError: If query execution fails
        
        Example:
            >>> # Select all active users ordered by name
            >>> df = db.select('users', 
            ...                filters={'active': 1}, 
            ...                order_by='name ASC',
            ...                limit=10)
            >>> 
            >>> # Select specific columns with NULL check
            >>> df = db.select('orders',
            ...                columns=['id', 'total'],
            ...                filters={'shipped_date': None})
        """
        self._validate_identifiers(table_name)
        if columns:
            self._validate_identifiers(*columns)

        columns_str = ",".join(columns) if columns else "*"
        query = f"SELECT {columns_str} FROM {table_name}"
        params = []
        
        if filters:
            self._validate_identifiers(*filters.keys())
            
            conditions = []
            for column, value in filters.items():
                if value is None:
                    conditions.append(f"{column} IS NULL")
                else:
                    conditions.append(f"{column} = ?")
                    params.append(value)
            query += " WHERE " + " AND ".join(conditions)
        
        if order_by:
            query += f" ORDER BY {order_by}"
        
        if limit or limit == 0:
            if not isinstance(limit, int) or limit < 0:
                raise ValueError("limit must be a non-negative integer")
            query += f" LIMIT {limit}"
        
        try:
            self._connect_db(isolation_level="DEFERRED")
            assert self.db_connection is not None, "Database connection is not established"
            df = pd.read_sql(query, self.db_connection, params=params, dtype=dtype, parse_dates=parse_dates)
            
            if localize_timezone and parse_dates and not df.empty:
                df = self.adjust_datetime_timezone(df, localize_timezone, list(parse_dates.keys()))
            
            return df
        
        except Exception as e:
            raise DatabaseError(message=f"Error executing SELECT on '{table_name}'", code="SELECT_ERROR") from e

    def insert(
        self, 
        table_name: str, 
        rows: list[Dict[str, Any]], 
        return_inserted: bool = True, 
        dtype: Dict | None = None, 
        parse_dates: Dict | None = None, 
        localize_timezone: timezone | None = None
    ) -> pd.DataFrame | None:
        """
        Insert one or more rows into table.
        
        Args:
            table_name: Table to insert into
            rows: List of row dictionaries {column: value}
            return_inserted: Whether to return inserted records (requires primary_key_column)
            dtype: Pandas dtype mapping for returned DataFrame
            parse_dates: Date columns to parse in returned DataFrame
            localize_timezone: Timezone for datetime localization in returned DataFrame
        
        Returns:
            DataFrame with inserted records if return_inserted=True, else None
        
        Raises:
            ValueError: If rows is empty, columns inconsistent, or identifiers invalid
            DatabaseError: If insertion fails (e.g., constraint violation)
        
        Example:
            >>> # Insert multiple users
            >>> rows = [
            ...     {'name': 'Alice', 'email': 'alice@example.com', 'age': 30},
            ...     {'name': 'Bob', 'email': 'bob@example.com', 'age': 25}
            ... ]
            >>> result = db.insert('users', rows)
            >>> print(result[['id', 'name']])
        
        Note:
            - All rows must have identical column names
            - Uses executemany for efficient batch insertion
            - Transaction committed automatically on success
            - Automatic rollback on error
        """
        if not rows:
            raise ValueError("rows cannot be empty")
        
        self._validate_identifiers(table_name)
        
        all_columns = set()
        for row in rows:
            all_columns.update(row.keys())
        self._validate_identifiers(*all_columns)
        
        first_row_keys = set(rows[0].keys())
        if not all(set(row.keys()) == first_row_keys for row in rows):
            raise ValueError("All rows must have the same columns")
        
        self._connect_db(isolation_level="IMMEDIATE")
        assert self.db_connection is not None and self.db_cursor is not None, "Database connection is not established"
        
        try:
            first_id = None
            if return_inserted and self.primary_key_column:
                cursor = self.db_cursor.execute(
                    f"SELECT MAX({self.primary_key_column}) FROM {table_name}"
                )
                max_id = cursor.fetchone()[0]
                first_id = (max_id or 0) + 1
            
            columns = list(rows[0].keys())
            placeholders = ','.join(['?' for _ in columns])
            query = f"INSERT INTO {table_name} ({','.join(columns)}) VALUES ({placeholders})"
            
            values_list = [list(row.values()) for row in rows]
            self.db_cursor.executemany(query, values_list)
            self.db_connection.commit()
            
            if return_inserted and self.primary_key_column and first_id is not None:
                last_id = first_id + len(rows) - 1
                inserted_ids = list(range(first_id, last_id + 1))
                
                placeholders_ids = ','.join(['?' for _ in inserted_ids])
                query = f"SELECT * FROM {table_name} WHERE {self.primary_key_column} IN ({placeholders_ids})"
                df = pd.read_sql(query, self.db_connection, params=tuple(inserted_ids), dtype=dtype, parse_dates=parse_dates)
                if localize_timezone and parse_dates and not df.empty:
                    df = self.adjust_datetime_timezone(df, localize_timezone, list(parse_dates.keys()))
                return df
            
            return None
            
        except Exception as e:
            self.db_connection.rollback()
            raise DatabaseError(message=f"Error inserting data into '{table_name}'", code="INSERT_ERROR") from e

    def update(
        self, 
        table_name: str, 
        parameters: Dict[str, Any], 
        filters: Dict[str, Any], 
        return_updated_rows: bool = True,
        dtype: Dict | None = None, 
        parse_dates: Dict | None = None, 
        localize_timezone: timezone | None = None
    ) -> pd.DataFrame | None:
        """
        Update records in table matching filter criteria.
        
        Args:
            table_name: Table to update
            parameters: Values to update {column: new_value}. Use None for NULL
            filters: WHERE conditions {column: value}. Use None for IS NULL
            return_updated_rows: Whether to return updated records
            dtype: Pandas dtype mapping for returned DataFrame
            parse_dates: Date columns to parse in returned DataFrame
            localize_timezone: Timezone for datetime localization in returned DataFrame
        
        Returns:
            DataFrame with updated records if return_updated_rows=True, else None
        
        Raises:
            ValueError: If parameters/filters empty or identifiers invalid
            DatabaseError: If update fails
        
        Example:
            >>> # Update user status and last login
            >>> db.update('users',
            ...          parameters={'status': 'active', 'last_login': datetime.now()},
            ...          filters={'email': 'user@example.com'})
            >>> 
            >>> # Set field to NULL for inactive users
            >>> db.update('users',
            ...          parameters={'last_login': None},
            ...          filters={'status': 'inactive'})
        
        Note:
            - Transaction committed automatically on success
            - Automatic rollback on error
        """
        if not parameters or not filters:
            raise ValueError("parameters and filters cannot be empty")
        
        self._validate_identifiers(table_name)
        self._validate_identifiers(*parameters.keys())
        self._validate_identifiers(*filters.keys())
        
        self._connect_db(isolation_level="IMMEDIATE")
        assert self.db_connection is not None and self.db_cursor is not None, "Database connection is not established"
        
        updated_rows = []
        try:
            set_clauses = []
            set_values = []
            for column, value in parameters.items():
                if value is None:
                    set_clauses.append(f"{column} = NULL")
                else:
                    set_clauses.append(f"{column} = ?")
                    set_values.append(value)
            
            where_clauses = []
            where_values = []
            for column, value in filters.items():
                if value is None:
                    where_clauses.append(f"{column} IS NULL")
                else:
                    where_clauses.append(f"{column} = ?")
                    where_values.append(value)

            query = f"UPDATE {table_name} SET {', '.join(set_clauses)} WHERE {' AND '.join(where_clauses)}{' RETURNING *' if return_updated_rows else ''}"
            params = set_values + where_values
            
            self.db_cursor.execute(query, params)
            if return_updated_rows: updated_rows = self.db_cursor.fetchall()
            self.db_connection.commit()
            
            if return_updated_rows and updated_rows:
                updated_rows = pd.DataFrame(updated_rows, columns=[desc[0] for desc in self.db_cursor.description])
                if dtype:
                    updated_rows = updated_rows.astype(dtype)
                if parse_dates:
                    for col, fmt in parse_dates.items():
                        updated_rows[col] = pd.to_datetime(updated_rows[col], format=fmt, errors="coerce")
                if localize_timezone and parse_dates and not updated_rows.empty:
                    updated_rows = self.adjust_datetime_timezone(updated_rows, localize_timezone, list(parse_dates.keys()))
                return updated_rows

            return None if not return_updated_rows else pd.DataFrame()
            
        except Exception as e:
            self.db_connection.rollback()
            raise DatabaseError(message=f"Error updating data in '{table_name}'", code="UPDATE_ERROR") from e

    def delete(self, table_name: str, filters: Dict[str, Any]) -> int:
        """
        Delete records from table matching filter criteria.
        
        Args:
            table_name: Table to delete from
            filters: WHERE conditions {column: value}. Use None for IS NULL.
                    Cannot be empty (prevents accidental full table deletion)
        
        Returns:
            Number of rows deleted
        
        Raises:
            ValueError: If filters empty or identifiers invalid
            DatabaseError: If deletion fails
        
        Example:
            >>> # Delete specific record
            >>> count = db.delete('users', filters={'id': 123})
            >>> print(f"Deleted {count} record(s)")
            >>> 
            >>> # Delete old logs with NULL status
            >>> count = db.delete('logs', 
            ...                   filters={'created_date': '2020-01-01', 'status': None})
        
        Warning:
            Filters required to prevent accidental deletion of all records.
            To delete all records, use execute() with explicit DELETE query.
        
        Note:
            - Transaction committed automatically on success
            - Automatic rollback on error
        """
        if not filters:
            raise ValueError("filters cannot be empty to prevent DELETE without WHERE clause")
        
        self._validate_identifiers(table_name)
        self._validate_identifiers(*filters.keys())
        
        self._connect_db(isolation_level="IMMEDIATE")
        assert self.db_connection is not None and self.db_cursor is not None, "Database connection is not established"
        
        try:
            where_clauses = []
            values = []
            for column, value in filters.items():
                if value is None:
                    where_clauses.append(f"{column} IS NULL")
                else:
                    where_clauses.append(f"{column} = ?")
                    values.append(value)
            
            query = f"DELETE FROM {table_name} WHERE {' AND '.join(where_clauses)}"
            
            self.db_cursor.execute(query, values)
            affected_rows = self.db_cursor.rowcount
            self.db_connection.commit()
            
            return affected_rows
            
        except Exception as e:
            self.db_connection.rollback()
            raise DatabaseError(message=f"Error deleting data from '{table_name}'", code="DELETE_ERROR") from e

    def execute(
        self, 
        sql: str, 
        params: Tuple | List | None = None, 
        commit: bool = True
    ) -> sqlite3.Cursor:
        """
        Execute custom SQL query with parameters.
        
        Args:
            sql: SQL query with ? placeholders for parameters
            params: Query parameters (tuple or list)
            commit: Whether to commit automatically after execution
        
        Returns:
            Cursor with query results (use fetchall(), fetchone(), etc.)
        
        Raises:
            DatabaseError: If query execution fails
        
        Example:
            >>> # Create table
            >>> db.execute('''
            ...     CREATE TABLE IF NOT EXISTS users (
            ...         id INTEGER PRIMARY KEY,
            ...         name TEXT NOT NULL
            ...     )
            ... ''')
            >>> 
            >>> # Complex query with parameters
            >>> cursor = db.execute(
            ...     "SELECT * FROM users WHERE age > ? AND status = ?",
            ...     params=(18, 'active')
            ... )
            >>> results = cursor.fetchall()
            >>> 
            >>> # Transaction with manual commit
            >>> db.execute("INSERT INTO logs (msg) VALUES (?)", ('log1',), commit=False)
            >>> db.execute("INSERT INTO logs (msg) VALUES (?)", ('log2',), commit=False)
            >>> db.db_connection.commit()
        
        Warning:
            Bypasses identifier validation. Always use parameterized queries
            with untrusted input to prevent SQL injection.
        
        Note:
            - Uses IMMEDIATE isolation if commit=True, DEFERRED if commit=False
            - Automatic rollback on error if commit=True
        """
        self._connect_db(isolation_level="IMMEDIATE" if commit else "DEFERRED")
        assert self.db_connection is not None and self.db_cursor is not None, "Database connection is not established"
        
        try:
            if params:
                self.db_cursor.execute(sql, params)
            else:
                self.db_cursor.execute(sql)
            
            if commit:
                self.db_connection.commit()
            
            return self.db_cursor
            
        except Exception as e:
            if commit:
                self.db_connection.rollback()
            raise DatabaseError(message=f"Error executing query: {sql[:100]}...", code="EXECUTE_SQL_ERROR") from e
    
    def get_table_info(self, table_name: str) -> pd.DataFrame:
        """
        Get table schema information.
        
        Args:
            table_name: Table to inspect
        
        Returns:
            DataFrame with columns: cid, name, type, notnull, dflt_value, pk
        
        Raises:
            ValueError: If table_name invalid
            DatabaseError: If query fails
        
        Example:
            >>> info = db.get_table_info('users')
            >>> print(info[['name', 'type', 'notnull']])
        """
        self._validate_identifiers(table_name)

        if not self.table_exists(table_name): # Ensure table exists
            raise DatabaseError(message=f"Table '{table_name}' does not exist", code="TABLE_NOT_FOUND")
        
        try:
            self._connect_db(isolation_level="DEFERRED")
            query = f"PRAGMA table_info({table_name})"
            assert self.db_connection is not None, "Database connection is not established"
            return pd.read_sql(query, self.db_connection)
        except Exception as e:
            raise DatabaseError(message=f"Error getting table info for '{table_name}'", code="TABLE_INFO_ERROR") from e
    
    def table_exists(self, table_name: str) -> bool:
        """
        Check if table exists in database.
        
        Args:
            table_name: Table name to check
        
        Returns:
            True if table exists
        
        Raises:
            ValueError: If table_name invalid
        
        Example:
            >>> if db.table_exists('users'):
            ...     print("Users table found")
            ... else:
            ...     db.execute("CREATE TABLE users (...)")
        """
        self._validate_identifiers(table_name)
        
        self._connect_db(isolation_level="DEFERRED")
        cursor = self.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
            params=(table_name,),
            commit=False
        )
        return cursor.fetchone() is not None

