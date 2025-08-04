import sqlite3
import time
import logging
import threading
from contextlib import contextmanager
from typing import Generator, Dict, Any, List, Callable, Optional
from dataclasses import dataclass

logger = logging.getLogger(__name__)

@dataclass
class DatabaseConfig:
    """Configuration for database connections."""
    connection: sqlite3.Connection
    name: str
    max_retries: int = 5
    retry_delay: float = 0.1
    timeout: float = 30.0

class RobustSQLiteHandler:
    """
    A robust SQLite handler that manages transactions across multiple databases
    with automatic retry logic and proper lock handling.
    """
    
    def __init__(self):
        self._db_configs: Dict[str, DatabaseConfig] = {}
        self._lock = threading.RLock()
    
    def register_database(self, name: str, connection: sqlite3.Connection, 
                         max_retries: int = 5, retry_delay: float = 0.1, 
                         timeout: float = 30.0):
        """Register a database connection with the handler."""
        with self._lock:
            # Configure connection for better concurrency
            connection.execute("PRAGMA journal_mode=WAL")  # Write-Ahead Logging
            connection.execute("PRAGMA synchronous=NORMAL")  # Balance safety/performance
            connection.execute("PRAGMA busy_timeout=30000")  # 30 second timeout
            connection.execute("PRAGMA temp_store=MEMORY")  # Use memory for temp tables
            connection.commit()
            
            self._db_configs[name] = DatabaseConfig(
                connection=connection,
                name=name,
                max_retries=max_retries,
                retry_delay=retry_delay,
                timeout=timeout
            )
    
    def _execute_with_retry(self, db_config: DatabaseConfig, operation: Callable, 
                           operation_name: str = "database operation") -> Any:
        """Execute a database operation with retry logic."""
        last_exception = None
        
        for attempt in range(db_config.max_retries):
            try:
                return operation()
            except sqlite3.OperationalError as e:
                last_exception = e
                error_msg = str(e).lower()
                
                if "database is locked" in error_msg or "database is busy" in error_msg:
                    if attempt < db_config.max_retries - 1:
                        # Exponential backoff with jitter
                        delay = db_config.retry_delay * (2 ** attempt) + (time.time() % 0.1)
                        logger.warning(
                            f"Database {db_config.name} is locked/busy on {operation_name}, "
                            f"retrying in {delay:.3f}s (attempt {attempt + 1}/{db_config.max_retries})"
                        )
                        time.sleep(delay)
                        continue
                else:
                    # Non-retryable error
                    raise
            except Exception as e:
                # Non-retryable error
                logger.error(f"Non-retryable error in {operation_name}: {e}")
                raise
        
        # All retries exhausted
        logger.error(
            f"Failed to execute {operation_name} on {db_config.name} "
            f"after {db_config.max_retries} attempts"
        )
        raise last_exception

    @contextmanager
    def single_transaction(self, db_name: str) -> Generator[sqlite3.Cursor, None, None]:
        """Context manager for a single database transaction with retry logic."""
        if db_name not in self._db_configs:
            raise ValueError(f"Database '{db_name}' not registered")
        
        db_config = self._db_configs[db_name]
        cursor = None
        
        def begin_transaction():
            nonlocal cursor
            cursor = db_config.connection.cursor()
            db_config.connection.execute("BEGIN IMMEDIATE")
            return cursor
        
        try:
            cursor = self._execute_with_retry(db_config, begin_transaction, "BEGIN transaction")
            yield cursor
            
            def commit_transaction():
                db_config.connection.commit()
            
            self._execute_with_retry(db_config, commit_transaction, "COMMIT transaction")
            
        except Exception:
            try:
                if cursor:
                    def rollback_transaction():
                        db_config.connection.rollback()
                    
                    self._execute_with_retry(db_config, rollback_transaction, "ROLLBACK transaction")
            except Exception as rollback_error:
                logger.error(f"Failed to rollback transaction on {db_name}: {rollback_error}")
            raise
        finally:
            if cursor:
                cursor.close()

    @contextmanager
    def coordinated_transaction(self, *db_names: str) -> Generator[Dict[str, sqlite3.Cursor], None, None]:
        """
        Context manager for coordinated transactions across multiple databases.
        Uses a two-phase commit approach to maintain consistency.
        """
        if not db_names:
            raise ValueError("At least one database name must be provided")
        
        # Validate all databases are registered
        for db_name in db_names:
            if db_name not in self._db_configs:
                raise ValueError(f"Database '{db_name}' not registered")
        
        # Sort database names for consistent ordering to prevent deadlocks
        sorted_db_names = sorted(db_names)
        cursors: Dict[str, sqlite3.Cursor] = {}
        begun_transactions: List[str] = []
        
        try:
            # Phase 1: Begin all transactions in sorted order
            for db_name in sorted_db_names:
                db_config = self._db_configs[db_name]
                
                def begin_transaction():
                    cursor = db_config.connection.cursor()
                    db_config.connection.execute("BEGIN IMMEDIATE")
                    return cursor
                
                cursor = self._execute_with_retry(
                    db_config, begin_transaction, f"BEGIN transaction on {db_name}"
                )
                cursors[db_name] = cursor
                begun_transactions.append(db_name)
            
            # Yield cursors for the application to use
            yield cursors
            
            # Phase 2: Commit all transactions in the same order
            for db_name in sorted_db_names:
                db_config = self._db_configs[db_name]
                
                def commit_transaction():
                    db_config.connection.commit()
                
                self._execute_with_retry(
                    db_config, commit_transaction, f"COMMIT transaction on {db_name}"
                )
            
        except Exception as e:
            logger.error(f"Error in coordinated transaction: {e}")
            
            # Rollback all begun transactions in reverse order
            for db_name in reversed(begun_transactions):
                try:
                    db_config = self._db_configs[db_name]
                    
                    def rollback_transaction():
                        db_config.connection.rollback()
                    
                    self._execute_with_retry(
                        db_config, rollback_transaction, f"ROLLBACK transaction on {db_name}"
                    )
                except Exception as rollback_error:
                    logger.error(f"Failed to rollback transaction on {db_name}: {rollback_error}")
            raise
        finally:
            # Close all cursors
            for cursor in cursors.values():
                try:
                    cursor.close()
                except Exception as close_error:
                    logger.error(f"Error closing cursor: {close_error}")

    def execute_with_retry(self, db_name: str, query: str, params: Optional[tuple] = None) -> sqlite3.Cursor:
        """Execute a single query with retry logic (for read operations)."""
        if db_name not in self._db_configs:
            raise ValueError(f"Database '{db_name}' not registered")
        
        db_config = self._db_configs[db_name]
        
        def execute_query():
            cursor = db_config.connection.cursor()
            if params:
                return cursor.execute(query, params)
            else:
                return cursor.execute(query)
        
        return self._execute_with_retry(db_config, execute_query, f"execute query on {db_name}")


# Global instance - initialize this once in your application
sqlite_handler = RobustSQLiteHandler()

def initialize_sqlite_handler(main_conn: sqlite3.Connection, path_conn: sqlite3.Connection):
    """Initialize the global SQLite handler with your database connections."""
    sqlite_handler.register_database("main", main_conn)
    sqlite_handler.register_database("path", path_conn)

# Convenience functions that match your existing API
@contextmanager
def transaction(connection: sqlite3.Connection) -> Generator[sqlite3.Cursor, None, None]:
    """
    Legacy compatibility function. 
    Note: This doesn't provide the same retry logic as the new handler.
    """
    cursor = connection.cursor()
    try:
        connection.execute("BEGIN IMMEDIATE")
        yield cursor
        connection.commit()
    except Exception:
        connection.rollback()
        raise
    finally:
        cursor.close()

@contextmanager
def robust_single_transaction(db_name: str) -> Generator[sqlite3.Cursor, None, None]:
    """Use the robust handler for single database transactions."""
    with sqlite_handler.single_transaction(db_name) as cursor:
        yield cursor

@contextmanager
def robust_coordinated_transaction(*db_names: str) -> Generator[Dict[str, sqlite3.Cursor], None, None]:
    """Use the robust handler for coordinated multi-database transactions."""
    with sqlite_handler.coordinated_transaction(*db_names) as cursors:
        yield cursors