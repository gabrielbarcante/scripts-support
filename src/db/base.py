from abc import ABC, abstractmethod
import pandas as pd
from datetime import timezone
from typing import Dict, Tuple, List, Any
import re
import pandas as pd


class DatabaseConnection(ABC):
    """
    Abstract base class for database connections.
    
    Defines the interface that all database connectors must implement.
    Supports context manager pattern for automatic resource management.
    
    This class cannot be instantiated directly. Subclasses must implement
    all abstract methods to provide database-specific functionality.
    
    Attributes:
        primary_key_column (str | None): Primary key column name for insert operations
    
    Example:
        >>> class MyDBConnection(DatabaseConnection):
        ...     def _connect_db(self, **kwargs): ...
        ...     def _disconnect_db(self): ...
        ...     # ... implement other abstract methods
        >>> 
        >>> with MyDBConnection('id') as db:
        ...     df = db.select('users', filters={'active': 1})
    """
    
    _IDENTIFIER_PATTERN = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*$")
    
    def __init__(self, primary_key_column: str | None = None):
        """
        Initialize database connection interface.
        
        Args:
            primary_key_column: Primary key column name (required for returning inserted records)
        
        Raises:
            ValueError: If primary_key_column contains invalid characters
        """
        if primary_key_column and not self._is_valid_identifier(primary_key_column):
            raise ValueError(f"Invalid primary key column name: {primary_key_column}")
        self.primary_key_column = primary_key_column

    def __enter__(self):
        """Enter context manager - establish database connection."""
        self._connect_db()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Exit context manager - close connection and rollback on error."""
        if exc_type is not None:
            self._rollback()
        self._disconnect_db()
        return False

    def __del__(self):
        """Destructor - ensure connection is closed."""
        self._disconnect_db()

    @abstractmethod
    def _connect_db(self, **kwargs) -> Any:
        """
        Establish connection to database.
        
        Subclasses must implement this method to create and return
        database-specific connection objects.
        
        Args:
            **kwargs: Database-specific connection parameters
        
        Returns:
            Database connection object(s) (implementation-specific)
        
        Raises:
            DatabaseError: If connection fails
        """
        pass

    @abstractmethod
    def _disconnect_db(self) -> None:
        """
        Close database connection safely.
        
        Subclasses must implement this method to properly clean up
        and close database connections. Should handle cases where
        connection is already closed or None.
        """
        pass

    @abstractmethod
    def _rollback(self) -> None:
        """
        Rollback current transaction.
        
        Subclasses must implement this method to rollback any pending
        transactions. Called automatically on error in context manager.
        """
        pass

    @staticmethod
    def _is_valid_identifier(identifier: str) -> bool:
        """
        Validate SQL identifier to prevent injection attacks.
        
        Args:
            identifier: Table or column name to validate
        
        Returns:
            True if valid (alphanumeric and underscore only, starts with letter/underscore)
        """
        return bool(DatabaseConnection._IDENTIFIER_PATTERN.match(identifier))
    
    def _validate_identifiers(self, *identifiers: str) -> None:
        """
        Validate multiple SQL identifiers.
        
        Args:
            *identifiers: Table/column names to validate
        
        Raises:
            ValueError: If any identifier is invalid
        """
        for identifier in identifiers:
            if not self._is_valid_identifier(identifier):
                raise ValueError(f"Invalid SQL identifier: '{identifier}'. Only alphanumeric characters and underscores allowed.")

    @abstractmethod
    def is_connected(self) -> bool:
        """
        Check if database connection is active.
        
        Subclasses must implement this method to verify the connection
        is established and responsive.
        
        Returns:
            True if connected and responsive, False otherwise
        """
        pass

    @abstractmethod
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
        
        Subclasses must implement this method using parameterized queries
        to prevent SQL injection attacks.
        
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
            DataFrame with query results (empty DataFrame if no matches)
        
        Raises:
            ValueError: If table_name or column names are invalid
            DatabaseError: If query execution fails
        """
        pass

    @abstractmethod
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
        
        Subclasses must implement this method with automatic transaction
        management (commit on success, rollback on error).
        
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
        """
        pass

    @abstractmethod
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
        
        Subclasses must implement this method with automatic transaction
        management (commit on success, rollback on error).
        
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
        """
        pass

    @abstractmethod
    def delete(self, table_name: str, filters: Dict[str, Any]) -> int:
        """
        Delete records from table matching filter criteria.
        
        Subclasses must implement this method with automatic transaction
        management (commit on success, rollback on error). Must enforce
        non-empty filters to prevent accidental full table deletion.
        
        Args:
            table_name: Table to delete from
            filters: WHERE conditions {column: value}. Use None for IS NULL.
                    Cannot be empty (prevents accidental full table deletion)
        
        Returns:
            Number of rows deleted
        
        Raises:
            ValueError: If filters empty or identifiers invalid
            DatabaseError: If deletion fails
        """
        pass

    @abstractmethod
    def execute(
        self, 
        sql: str, 
        params: Any | None = None, 
        commit: bool = True
    ) -> Any:
        """
        Execute custom SQL query with parameters.
        
        Subclasses must implement this method for executing arbitrary SQL.
        Should use parameterized queries when params are provided.
        
        Args:
            sql: SQL query with database-specific placeholders for parameters
            params: Query parameters (tuple or list)
            commit: Whether to commit automatically after execution
        
        Returns:
            Database-specific result object (cursor, DataFrame, etc.)
        
        Raises:
            DatabaseError: If query execution fails
        
        Warning:
            This method bypasses identifier validation. Use with caution
            and always use parameterized queries for user input.
        """
        pass

    @abstractmethod
    def table_exists(self, table_name: str) -> bool:
        """
        Check if table exists in database.
        
        Subclasses must implement this method using database-specific
        metadata queries.
        
        Args:
            table_name: Table name to check
        
        Returns:
            True if table exists, False otherwise
        
        Raises:
            ValueError: If table_name invalid
            DatabaseError: If metadata query fails
        """
        pass

    @abstractmethod
    def get_table_info(self, table_name: str) -> pd.DataFrame:
        """
        Get table schema information.
        
        Subclasses must implement this method to query database metadata
        and return column information (names, types, constraints, etc.).
        
        Args:
            table_name: Table to inspect
        
        Returns:
            DataFrame with table schema information (columns: name, type, 
            notnull, dflt_value, pk, etc.)
        
        Raises:
            ValueError: If table_name invalid
            DatabaseError: If metadata query fails
        """
        pass

    @staticmethod
    def adjust_datetime_timezone(df: pd.DataFrame, tz: timezone, dt_columns: list[str]) -> pd.DataFrame:
        """
        Adjust datetime columns in DataFrame to specified timezone.
        
        Args:
            df: DataFrame containing datetime columns
            tz: Timezone to localize to
            dt_columns: List of datetime column names to localize
        
        Returns:
            DataFrame with localized datetime columns
        """
        for column in dt_columns:
            if column in df.columns:
                df[column] = df[column].dt.tz_localize(tz)
        
        return df
