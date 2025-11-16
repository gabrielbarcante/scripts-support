import sqlalchemy
import sqlalchemy.exc

from urllib.parse import quote_plus
import pandas as pd
from datetime import timezone
from typing import Dict, List, Any

from ..error import DatabaseError
from .base import DatabaseConnection


class MySQLConnection(DatabaseConnection):
    """
    MySQL database connection implementation.
    
    Provides methods for connecting to a MySQL database, executing queries,
    and performing CRUD operations. Implements the DatabaseConnection interface.
    
    Attributes:
        host (str): Database host address
        port (int): Database port number
        user (str): Database username
        password (str): Database password
        database (str): Database name
    """
    
    def __init__(self, host: str, port: int, user: str, password: str, database: str, primary_key_column: str | None = None):
        """
        Initialize MySQL database connection.
        
        Args:
            host: Database host address
            port: Database port number
            user: Database username
            password: Database password
            database: Database name
            primary_key_column: Primary key column name (required for returning inserted records)
        """
        super().__init__(primary_key_column=primary_key_column)
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.database = database

        self.db_engine: sqlalchemy.engine.base.Engine | None = None

    def _connect_db(self, **kwargs) -> sqlalchemy.engine.base.Engine:
        """
        Establish connection to MySQL database.
        
        Creates and returns a SQLAlchemy engine for the MySQL database.
        URL-encodes username and password to handle special characters.
        
        Args:
            **kwargs: Additional keyword arguments (currently unused)
            
        Returns:
            sqlalchemy.engine.base.Engine: Database engine instance
            
        Raises:
            DatabaseError: If connection to database fails
        """
        
        if self.db_engine is not None:
            return self.db_engine
        
        try:
            # URL-encode username and password to handle special characters
            encoded_user = quote_plus(self.user)
            encoded_password = quote_plus(self.password)
            
            self.db_engine = sqlalchemy.create_engine(f"mysql+pymysql://{encoded_user}:{encoded_password}@{self.host}:{self.port}/{self.database}")

            return self.db_engine
        
        except sqlalchemy.exc.SQLAlchemyError as e:
            raise DatabaseError(message=f"Failed to connect to MySQL database: {self.database}", code="CONNECTION_ERROR") from e

    def _rollback(self) -> None:
        """
        Rollback current transaction.
        
        Rolls back any uncommitted changes in the current transaction.
        This method is called when an error occurs during a database operation.
        """
        if self.db_engine:
            with self.db_engine.connect() as connection:
                connection.rollback()

    def _disconnect_db(self) -> None:
        """
        Close MySQL database connection safely.
        
        Disposes of the database engine and cleans up resources.
        Sets the db_engine to None after disconnection.
        Suppresses any exceptions that occur during disconnection.
        """
        try:
            if self.db_engine:
                self.db_engine.dispose()
        except Exception as e:
            pass
        finally:
            self.db_engine = None

    def is_connected(self) -> bool:
        """
        Check if the database connection is active and usable.
        
        Performs an actual connection test by executing a simple query
        to verify the connection is not only initialized but also functional.
        
        Returns:
            bool: True if connected and connection is active, False otherwise
        """
        if self.db_engine is None:
            return False
        
        try:
            # Test the connection with a simple query
            with self.db_engine.connect() as connection:
                connection.execute(sqlalchemy.text("SELECT 1"))
            return True
        except Exception:
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
            table_name: Name of the table to query
            columns: List of column names to select. If None, selects all columns (SELECT *)
            filters: Dictionary of column-value pairs for WHERE clause. None values use IS NULL
            order_by: Column name(s) for ORDER BY clause (e.g., 'id DESC', 'name, age')
            limit: Maximum number of rows to return. Must be non-negative integer
            dtype: Dictionary mapping column names to pandas data types
            parse_dates: Dictionary of columns to parse as datetime objects
            localize_timezone: Timezone to localize parsed datetime columns to
            
        Returns:
            pd.DataFrame: Query results as a pandas DataFrame. Empty DataFrame if no results
            
        Raises:
            ValueError: If limit is not a non-negative integer or table/column identifiers are invalid
            DatabaseError: If database connection fails or query execution fails
            
        Example:
            >>> # Select specific columns with filtering
            >>> conn.select('users', columns=['id', 'name'], filters={'active': 1}, limit=10)
            >>> 
            >>> # Select all with NULL filter
            >>> conn.select('orders', filters={'cancelled_at': None})
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
                    conditions.append(f"{column} = %s")
                    params.append(value)
            query += " WHERE " + " AND ".join(conditions)
        
        if order_by:
            query += f" ORDER BY {order_by}"
        
        if limit or limit == 0:
            if not isinstance(limit, int) or limit < 0:
                raise ValueError("limit must be a non-negative integer")
            query += f" LIMIT {limit}"
        
        try:
            self._connect_db()
            assert self.db_engine is not None, "Database engine is not initialized"

            with self.db_engine.connect() as connection:
                df = pd.read_sql(query, connection, params=tuple(params), dtype=dtype, parse_dates=parse_dates)
            
            if localize_timezone and parse_dates and not df.empty:
                df = self.adjust_datetime_timezone(df, localize_timezone, list(parse_dates.keys()))
            
            return df
        
        except Exception as e:
            raise DatabaseError(message=f"Error executing SELECT on '{table_name}'", code="SELECT_ERROR") from e

    def insert(
        self, 
        table_name: str, 
        rows: List[Dict[str, Any]], 
        return_inserted: bool = True, 
        dtype: Dict | None = None, 
        parse_dates: Dict | None = None, 
        localize_timezone: timezone | None = None
    ) -> pd.DataFrame | None:
        """
        Insert one or more rows into table.
        
        Inserts multiple rows in a single batch operation. If return_inserted is True,
        fetches and returns the inserted rows using the primary key column.
        
        Args:
            table_name: Name of the table to insert into
            rows: List of dictionaries representing rows to insert. All rows must have identical column sets
            return_inserted: If True, returns the inserted rows. Requires primary_key_column to be set
            dtype: Dictionary mapping column names to pandas data types for returned data
            parse_dates: Dictionary of columns to parse as datetime objects in returned data
            localize_timezone: Timezone to localize parsed datetime columns to in returned data
            
        Returns:
            pd.DataFrame | None: DataFrame containing inserted rows if return_inserted is True and 
                                 primary_key_column is set, otherwise None
            
        Raises:
            ValueError: If rows is empty, columns are inconsistent across rows, 
                       primary_key_column not set when return_inserted is True, or identifiers are invalid
            DatabaseError: If database connection fails or insert operation fails
            
        Example:
            >>> # Insert multiple rows
            >>> rows = [
            ...     {'name': 'John', 'age': 30, 'email': 'john@example.com'},
            ...     {'name': 'Jane', 'age': 25, 'email': 'jane@example.com'}
            ... ]
            >>> inserted_df = conn.insert('users', rows, return_inserted=True)
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
        
        self._connect_db()
        assert self.db_engine is not None, "Database engine is not initialized"
        
        try:
            table_sql = sqlalchemy.Table(table_name, sqlalchemy.MetaData(), autoload_with=self.db_engine)
            
            with self.db_engine.connect() as connection:
                results = connection.execute(table_sql.insert(), rows) 
                connection.commit()
                
                if return_inserted and self.primary_key_column:                    
                    # Get the last inserted ID and calculate all IDs
                    first_id = results.lastrowid
                    num_rows = len(rows)
                    inserted_ids = list(range(first_id, first_id + num_rows))

                    # Fetch all inserted rows
                    placeholders = ", ".join(["%s"] * num_rows)
                    query = f"SELECT * FROM {table_name} WHERE {self.primary_key_column} IN ({placeholders})"
                    
                    df = pd.read_sql(
                        query, 
                        connection, 
                        params=tuple(inserted_ids), 
                        dtype=dtype, 
                        parse_dates=parse_dates
                    )
                    
                    if localize_timezone and parse_dates and not df.empty:
                        df = self.adjust_datetime_timezone(df, localize_timezone, list(parse_dates.keys()))
                
                    return df
            
            return None
            
        except Exception as e:
            self._rollback()
            raise DatabaseError(message=f"Error inserting into '{table_name}'", code="INSERT_ERROR") from e
        
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
        
        Updates all rows matching the filter conditions with the provided parameter values.
        Multiple filter conditions are combined with AND logic.
        
        Args:
            table_name: Name of the table to update
            parameters: Dictionary of column-value pairs to update. Column names must exist in table
            filters: Dictionary of column-value pairs for WHERE clause. None values use IS NULL comparison
            return_updated_rows: If True, returns the updated rows after the update operation
            dtype: Dictionary mapping column names to pandas data types for returned data
            parse_dates: Dictionary of columns to parse as datetime objects in returned data
            localize_timezone: Timezone to localize parsed datetime columns to in returned data
            
        Returns:
            pd.DataFrame | None: DataFrame containing updated rows if return_updated_rows is True 
                                 and rows were affected, otherwise None
            
        Raises:
            ValueError: If parameters or filters are empty or column/table identifiers are invalid
            DatabaseError: If database connection fails or update operation fails
            
        Example:
            >>> # Update user age and email
            >>> conn.update(
            ...     'users', 
            ...     parameters={'age': 31, 'email': 'newemail@example.com'}, 
            ...     filters={'name': 'John'}
            ... )
            >>> 
            >>> # Update with NULL filter
            >>> conn.update('orders', {'status': 'pending'}, {'processed_at': None})
        """
        if not parameters or not filters:
            raise ValueError("parameters and filters cannot be empty")
        
        self._validate_identifiers(table_name)
        self._validate_identifiers(*parameters.keys())
        self._validate_identifiers(*filters.keys())
        
        self._connect_db()
        assert self.db_engine is not None, "Database engine is not initialized"
        
        try:
            table_sql = sqlalchemy.Table(table_name, sqlalchemy.MetaData(), autoload_with=self.db_engine)
            
            # Build WHERE clause conditions
            where_conditions = []
            for column, value in filters.items():
                col_obj = table_sql.c[column]
                if value is None:
                    where_conditions.append(col_obj.is_(None))
                else:
                    where_conditions.append(col_obj == value)
            
            # Combine conditions with AND
            where_clause = sqlalchemy.and_(*where_conditions) if len(where_conditions) > 1 else where_conditions[0]
            
            # Build and execute update statement
            update_stmt = table_sql.update().where(where_clause).values(**parameters)
            
            with self.db_engine.connect() as connection:
                results = connection.execute(update_stmt)
                connection.commit()
            
            if return_updated_rows and results.rowcount > 0:
                df =  self.select(table_name, dtype=dtype, parse_dates=parse_dates, localize_timezone=localize_timezone, filters=filters)
                if localize_timezone and parse_dates and not df.empty:
                    df = self.adjust_datetime_timezone(df, localize_timezone, list(parse_dates.keys()))
            
                return df
            
            return None
            
        except Exception as e:
            self._rollback()
            raise DatabaseError(message=f"Error updating data in '{table_name}'", code="UPDATE_ERROR") from e

    def delete(self, table_name: str, filters: Dict[str, Any]) -> int:
        """
        Delete records from table matching filter criteria.
        
        Deletes all rows matching the filter conditions. Multiple filter conditions 
        are combined with AND logic. Requires at least one filter to prevent 
        accidental deletion of all table records.
        
        Args:
            table_name: Name of the table to delete from
            filters: Dictionary of column-value pairs for WHERE clause. None values use IS NULL comparison.
                    Cannot be empty as a safety measure
            
        Returns:
            int: Number of rows deleted from the table
            
        Raises:
            ValueError: If filters is empty (prevents accidental deletion of all records) 
                       or table/column identifiers are invalid
            DatabaseError: If database connection fails or delete operation fails
            
        Example:
            >>> # Delete specific user
            >>> deleted_count = conn.delete('users', {'id': 123})
            >>> print(f"Deleted {deleted_count} rows")
            >>> 
            >>> # Delete with multiple conditions
            >>> conn.delete('logs', {'user_id': 456, 'created_at': None})
        """
        if not filters:
            raise ValueError("filters cannot be empty (prevents accidental deletion of all records)")
        
        self._validate_identifiers(table_name)
        self._validate_identifiers(*filters.keys())
        
        self._connect_db()
        assert self.db_engine is not None, "Database engine is not initialized"
        
        try:
            table_sql = sqlalchemy.Table(table_name, sqlalchemy.MetaData(), autoload_with=self.db_engine)

            # Build WHERE clause conditions
            where_conditions = []
            for column, value in filters.items():
                col_obj = table_sql.c[column]
                if value is None:
                    where_conditions.append(col_obj.is_(None))
                else:
                    where_conditions.append(col_obj == value)

            # Combine conditions with AND
            where_clause = sqlalchemy.and_(*where_conditions) if len(where_conditions) > 1 else where_conditions[0]
            
            with self.db_engine.connect() as connection:
                result = connection.execute(table_sql.delete().where(where_clause))
                affected_rows = result.rowcount
                connection.commit()
            
            return affected_rows
            
        except Exception as e:
            self._rollback()
            raise DatabaseError(message=f"Error deleting data from '{table_name}'", code="DELETE_ERROR") from e
    
    def execute(
        self, 
        sql: str, 
        params: Dict[str, Any] | None = None, 
        commit: bool = True
    ) -> sqlalchemy.engine.Result:
        """
        Execute custom SQL query with optional named parameters.
        
        Executes raw SQL queries with parameter binding for safe query execution.
        Use named parameters with colon prefix (e.g., :param_name) in the SQL string.
        
        Args:
            sql: SQL query string to execute. Use :param_name for named parameters
            params: Dictionary of named parameters to bind to the query (without colon prefix in keys)
            commit: If True, commits the transaction after execution. Set to False for SELECT queries
            
        Returns:
            sqlalchemy.engine.Result: Query execution result object containing rows, metadata, etc.
            
        Raises:
            DatabaseError: If database connection fails or query execution fails
            
        Example:
            >>> # Execute UPDATE with parameters
            >>> result = conn.execute(
            ...     "UPDATE users SET age = :age WHERE id = :id", 
            ...     params={'age': 30, 'id': 123}
            ... )
            >>> 
            >>> # Execute SELECT without commit
            >>> result = conn.execute("SELECT * FROM users WHERE active = :active", 
            ...                       params={'active': 1}, commit=False)
        """
        self._connect_db()
        assert self.db_engine is not None, "Database engine is not initialized"
        
        try:            
            with self.db_engine.connect() as connection:
                if params:
                    result = connection.execute(sqlalchemy.text(sql), parameters=params)
                else:
                    result = connection.execute(sqlalchemy.text(sql))
                
                if commit:
                    connection.commit()
        
            return result
            
        except Exception as e:
            if commit:
                self._rollback()
            raise DatabaseError(message=f"Error executing query: {sql[:100]}...", code="EXECUTE_SQL_ERROR") from e
    
    def table_exists(self, table_name: str) -> bool:
        """
        Check if table exists in the current database.
        
        Queries the MySQL information_schema to determine if the specified 
        table exists in the currently connected database.
        
        Args:
            table_name: Name of the table to check for existence
            
        Returns:
            bool: True if table exists in the database, False otherwise
            
        Raises:
            ValueError: If table_name contains invalid characters
            DatabaseError: If database connection fails or the check query fails
            
        Example:
            >>> if conn.table_exists('users'):
            ...     print('Users table exists')
            ... else:
            ...     print('Users table does not exist')
        """
        self._validate_identifiers(table_name)
        
        try:
            self._connect_db()
            assert self.db_engine is not None, "Database engine is not initialized"
            
            query = """
                SELECT COUNT(*) as count
                FROM information_schema.tables 
                WHERE table_schema = %s 
                AND table_name = %s
            """
            
            with self.db_engine.connect() as connection:
                result = pd.read_sql(query, connection, params=(self.database, table_name))
            
            return bool(result["count"].iloc[0] > 0)
            
        except Exception as e:
            raise DatabaseError(message=f"Error checking if table '{table_name}' exists", code="TABLE_EXISTS_ERROR") from e
    
    def get_table_info(self, table_name: str) -> pd.DataFrame:
        """
        Get comprehensive table schema information.
        
        Retrieves detailed column information including name, data type, nullability, 
        default value, and primary key status from the MySQL information_schema.
        
        Args:
            table_name: Name of the table to get schema information for
            
        Returns:
            pd.DataFrame: DataFrame with columns:
                - name: Column name
                - type: MySQL data type (e.g., 'int', 'varchar', 'datetime')
                - notnull: 'YES' if nullable, 'NO' if NOT NULL
                - dflt_value: Default value for the column (None if no default)
                - pk: 1 if column is primary key, 0 otherwise
            
        Raises:
            ValueError: If table_name contains invalid characters
            DatabaseError: If table doesn't exist or query fails
            
        Example:
            >>> info = conn.get_table_info('users')
            >>> print(info[['name', 'type', 'pk']])
            >>> 
            >>> # Check for primary key columns
            >>> pk_columns = info[info['pk'] == 1]['name'].tolist()
        """
        self._validate_identifiers(table_name)

        if not self.table_exists(table_name): # Ensure table exists
            raise DatabaseError(message=f"Table '{table_name}' does not exist", code="TABLE_NOT_FOUND")
        
        try:
            self._connect_db()
            assert self.db_engine is not None, "Database engine is not initialized"
            
            query = """
                SELECT COLUMN_NAME as name,
                       DATA_TYPE as type,
                       IS_NULLABLE as notnull,
                       COLUMN_DEFAULT as dflt_value,
                       CASE WHEN COLUMN_KEY = 'PRI' THEN 1 ELSE 0 END as pk
                FROM information_schema.columns
                WHERE table_schema = %s
                AND table_name = %s
                ORDER BY ORDINAL_POSITION
            """
            
            with self.db_engine.connect() as connection:
                df = pd.read_sql(query, connection, params=(self.database, table_name))
            
            return df
            
        except Exception as e:
            raise DatabaseError(message=f"Error getting table info for '{table_name}'", code="TABLE_INFO_ERROR") from e

