from typing import Literal, Dict, Any
from .base import DatabaseConnection
from .sqlite import SQLiteConnection
from .mysql import MySQLConnection


DatabaseType = Literal["sqlite", "mysql"]

class DatabaseFactory:
    """
    Factory class for creating database connection instances.
    
    Provides centralized database connector creation with support for multiple
    database types. Extensible design allows easy addition of new database types.
    
    Example:
        >>> # Create SQLite connection
        >>> db = DatabaseFactory.create_connection(
        ...     db_type="sqlite",
        ...     db_path="app.db",
        ...     primary_key_column="id"
        ... )
        >>> with db:
        ...     df = db.select("users")
        >>> 
        >>> # Use factory to get connection type
        >>> db_config = {"type": "sqlite", "path": "data.db"}
        >>> db = DatabaseFactory.create_connection(
        ...     db_type=db_config["type"],
        ...     db_path=db_config["path"]
        ... )
    """
    
    _CONNECTORS: Dict[str, type[DatabaseConnection]] = {
        "sqlite": SQLiteConnection,
        "mysql": MySQLConnection
    }
    
    @classmethod
    def create_connection(cls, db_type: DatabaseType, **connection_params: Any) -> DatabaseConnection:
        """
        Create database connection instance based on database type.
        
        Args:
            db_type: Type of database ("sqlite", "mysql")
            **connection_params: Database-specific connection parameters
                For SQLite:
                    - db_path (str): Path to database file
                    - primary_key_column (str | None): Primary key column name
                For MySQL (future):
                    - host, port, user, password, database, etc.
        
        Returns:
            Configured database connection instance
        
        Raises:
            ValueError: If db_type is not supported
            TypeError: If required connection parameters are missing
        
        Example:
            >>> # SQLite connection
            >>> db = DatabaseFactory.create_connection(
            ...     db_type="sqlite",
            ...     db_path="app.db",
            ...     primary_key_column="id"
            ... )
            >>> 
            >>> # Future MySQL connection
            >>> # db = DatabaseFactory.create_connection(
            >>> #     db_type="mysql",
            >>> #     host="localhost",
            >>> #     user="root",
            >>> #     password="secret",
            >>> #     database="mydb"
            >>> # )
        """
        if db_type not in cls._CONNECTORS:
            supported = ", ".join(cls._CONNECTORS.keys())
            raise ValueError(f"Unsupported database type: '{db_type}'. Supported types: {supported}")
        
        connector_class = cls._CONNECTORS[db_type]
        
        try:
            return connector_class(**connection_params)
        except TypeError as e:
            raise TypeError(f"Invalid connection parameters for {db_type}: {str(e)}") from e
    
    @classmethod
    def register_connector(cls, db_type: str, connector_class: type[DatabaseConnection]) -> None:
        """
        Register a new database connector type.
        
        Allows dynamic registration of custom database connectors without
        modifying the factory class.
        
        Args:
            db_type: Database type identifier (e.g., "mongodb", "redis")
            connector_class: DatabaseConnection subclass implementing the connector
        
        Raises:
            ValueError: If db_type already registered
            TypeError: If connector_class doesn't inherit from DatabaseConnection
        
        Example:
            >>> class CustomDBConnection(DatabaseConnection):
            ...     # Implementation here
            ...     pass
            >>> 
            >>> DatabaseFactory.register_connector("customdb", CustomDBConnection)
            >>> db = DatabaseFactory.create_connection(
            ...     db_type="customdb",
            ...     custom_param="value"
            ... )
        """
        if db_type in cls._CONNECTORS:
            raise ValueError(f"Database type '{db_type}' is already registered")
        
        if not issubclass(connector_class, DatabaseConnection):
            raise TypeError(f"Connector class must inherit from DatabaseConnection, got {connector_class.__name__}")
        
        cls._CONNECTORS[db_type] = connector_class
    
    @classmethod
    def get_supported_types(cls) -> list[str]:
        """
        Get list of supported database types.
        
        Returns:
            List of registered database type identifiers
        
        Example:
            >>> types = DatabaseFactory.get_supported_types()
            >>> print(f"Supported databases: {', '.join(types)}")
        """
        return list(cls._CONNECTORS.keys())
    
    @classmethod
    def is_supported(cls, db_type: str) -> bool:
        """
        Check if a database type is supported.
        
        Args:
            db_type: Database type to check
        
        Returns:
            True if the database type is registered
        
        Example:
            >>> if DatabaseFactory.is_supported("sqlite"):
            ...     db = DatabaseFactory.create_connection(db_type="sqlite", ...)
        """
        return db_type in cls._CONNECTORS


def create_connection(db_type: DatabaseType, **connection_params: Any) -> DatabaseConnection:
    """
    Convenience function for creating database connections.
    
    Wrapper around DatabaseFactory.create_connection() for simpler imports.
    
    Args:
        db_type: Type of database ("sqlite", "mysql", "postgresql")
        **connection_params: Database-specific connection parameters
    
    Returns:
        Configured database connection instance
    
    Raises:
        ValueError: If db_type is not supported
        TypeError: If required connection parameters are missing
    
    Example:
        >>> from src.db import create_connection
        >>> 
        >>> db = create_connection("sqlite", db_path="app.db", primary_key_column="id")
        >>> with db:
        ...     users = db.select("users")
    """
    return DatabaseFactory.create_connection(db_type, **connection_params)
