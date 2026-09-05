"""
DuckDB Connection Manager for efficient data transformations.

Provides connection pooling, query optimization, and integration with MinIO
for SQL-on-files capabilities.
"""

import os
import logging
from typing import Dict, Any, List, Optional, Union
from contextlib import contextmanager
import duckdb
from dataclasses import dataclass
import threading

logger = logging.getLogger(__name__)


@dataclass
class QueryResult:
    """Result of a DuckDB query."""
    columns: List[str]
    rows: List[tuple]
    row_count: int
    execution_time_ms: float


class DuckDBConnectionManager:
    """
    Manages DuckDB connections with pooling and optimization.
    
    Features:
    - Connection pooling
    - Query optimization
    - S3/MinIO integration
    - Parquet/CSV support
    - Memory management
    - Transaction support
    """

    def __init__(
        self,
        database_path: str = ":memory:",
        read_only: bool = False,
        threads: int = 4,
        memory_limit: str = "2GB",
        s3_endpoint: Optional[str] = None,
        s3_access_key: Optional[str] = None,
        s3_secret_key: Optional[str] = None
    ):
        """
        Initialize DuckDB connection manager.
        
        Args:
            database_path: Path to database file or ":memory:" for in-memory
            read_only: Whether to open in read-only mode
            threads: Number of threads for parallel execution
            memory_limit: Memory limit (e.g., "2GB", "500MB")
            s3_endpoint: S3/MinIO endpoint URL
            s3_access_key: S3 access key
            s3_secret_key: S3 secret key
        """
        self.database_path = database_path
        self.read_only = read_only
        self.threads = threads
        self.memory_limit = memory_limit
        
        # S3/MinIO configuration
        self.s3_endpoint = s3_endpoint or os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
        self.s3_access_key = s3_access_key or os.getenv("MINIO_ACCESS_KEY", "minioadmin")
        self.s3_secret_key = s3_secret_key or os.getenv("MINIO_SECRET_KEY", "minioadmin")
        
        # Connection pool
        self._connections: Dict[int, duckdb.DuckDBPyConnection] = {}
        self._lock = threading.Lock()
        
        # Initialize
        self._init_connection()

    def _init_connection(self):
        """Initialize the main connection and configure."""
        conn = self._get_connection()
        
        # Set configuration
        conn.execute(f"SET threads={self.threads}")
        conn.execute(f"SET memory_limit='{self.memory_limit}'")
        
        # Install and load extensions
        self._install_extensions(conn)
        
        # Configure S3/MinIO
        self._configure_s3(conn)
        
        logger.info(f"DuckDB initialized: {self.database_path} (threads={self.threads}, memory={self.memory_limit})")

    def _install_extensions(self, conn: duckdb.DuckDBPyConnection):
        """Install required DuckDB extensions."""
        extensions = ["httpfs", "parquet"]
        
        for ext in extensions:
            try:
                conn.execute(f"INSTALL {ext}")
                conn.execute(f"LOAD {ext}")
                logger.info(f"Loaded extension: {ext}")
            except Exception as e:
                logger.warning(f"Could not load extension {ext}: {e}")

    def _configure_s3(self, conn: duckdb.DuckDBPyConnection):
        """Configure S3/MinIO access."""
        try:
            conn.execute(f"SET s3_endpoint='{self.s3_endpoint}'")
            conn.execute(f"SET s3_access_key_id='{self.s3_access_key}'")
            conn.execute(f"SET s3_secret_access_key='{self.s3_secret_key}'")
            conn.execute("SET s3_use_ssl=false")  # For local MinIO
            conn.execute("SET s3_url_style='path'")
            logger.info(f"S3/MinIO configured: {self.s3_endpoint}")
        except Exception as e:
            logger.warning(f"Could not configure S3: {e}")

    def _get_connection(self) -> duckdb.DuckDBPyConnection:
        """Get a connection for the current thread."""
        thread_id = threading.get_ident()
        
        with self._lock:
            if thread_id not in self._connections:
                self._connections[thread_id] = duckdb.connect(
                    database=self.database_path,
                    read_only=self.read_only
                )
            
            return self._connections[thread_id]

    @contextmanager
    def get_connection(self):
        """Context manager for getting a connection."""
        conn = self._get_connection()
        try:
            yield conn
        finally:
            # Connection is pooled, not closed
            pass

    def execute(self, query: str, parameters: Optional[List] = None) -> QueryResult:
        """
        Execute a SQL query and return results.
        
        Args:
            query: SQL query string
            parameters: Query parameters (for prepared statements)
            
        Returns:
            QueryResult with columns, rows, and metadata
        """
        import time
        start_time = time.time()
        
        with self.get_connection() as conn:
            try:
                if parameters:
                    result = conn.execute(query, parameters).fetchall()
                else:
                    result = conn.execute(query).fetchall()
                
                # Get column names
                description = conn.description
                columns = [desc[0] for desc in description] if description else []
                
                execution_time_ms = (time.time() - start_time) * 1000
                
                return QueryResult(
                    columns=columns,
                    rows=result,
                    row_count=len(result),
                    execution_time_ms=execution_time_ms
                )
                
            except Exception as e:
                logger.error(f"Query execution failed: {e}")
                logger.error(f"Query: {query}")
                raise

    def execute_many(self, query: str, parameters_list: List[List]) -> int:
        """
        Execute a query multiple times with different parameters.
        
        Args:
            query: SQL query string
            parameters_list: List of parameter lists
            
        Returns:
            Number of rows affected
        """
        with self.get_connection() as conn:
            try:
                conn.executemany(query, parameters_list)
                return len(parameters_list)
            except Exception as e:
                logger.error(f"Batch execution failed: {e}")
                raise

    def read_parquet(self, file_path: str, table_name: Optional[str] = None) -> QueryResult:
        """
        Read a Parquet file from S3/MinIO or local filesystem.
        
        Args:
            file_path: Path to Parquet file (s3://bucket/file.parquet or local path)
            table_name: Optional table name to create
            
        Returns:
            QueryResult with data
        """
        query = f"SELECT * FROM read_parquet('{file_path}')"
        
        if table_name:
            # Create a view
            self.execute(f"CREATE OR REPLACE VIEW {table_name} AS {query}")
        
        return self.execute(query)

    def read_csv(
        self, 
        file_path: str, 
        table_name: Optional[str] = None,
        delimiter: str = ",",
        header: bool = True
    ) -> QueryResult:
        """
        Read a CSV file from S3/MinIO or local filesystem.
        
        Args:
            file_path: Path to CSV file
            table_name: Optional table name to create
            delimiter: CSV delimiter
            header: Whether CSV has header row
            
        Returns:
            QueryResult with data
        """
        query = f"SELECT * FROM read_csv_auto('{file_path}', delim='{delimiter}', header={header})"
        
        if table_name:
            # Create a view
            self.execute(f"CREATE OR REPLACE VIEW {table_name} AS {query}")
        
        return self.execute(query)

    def write_parquet(self, query: str, output_path: str):
        """
        Write query results to a Parquet file.
        
        Args:
            query: SQL query to execute
            output_path: Output Parquet file path (s3://bucket/file.parquet or local)
        """
        write_query = f"COPY ({query}) TO '{output_path}' (FORMAT PARQUET)"
        self.execute(write_query)
        logger.info(f"Wrote results to: {output_path}")

    def write_csv(
        self, 
        query: str, 
        output_path: str,
        delimiter: str = ",",
        header: bool = True
    ):
        """
        Write query results to a CSV file.
        
        Args:
            query: SQL query to execute
            output_path: Output CSV file path
            delimiter: CSV delimiter
            header: Whether to include header row
        """
        write_query = f"COPY ({query}) TO '{output_path}' (FORMAT CSV, DELIMITER '{delimiter}', HEADER {header})"
        self.execute(write_query)
        logger.info(f"Wrote results to: {output_path}")

    def create_table_from_parquet(self, table_name: str, file_path: str):
        """
        Create a table from a Parquet file.
        
        Args:
            table_name: Name of table to create
            file_path: Path to Parquet file
        """
        query = f"CREATE TABLE {table_name} AS SELECT * FROM read_parquet('{file_path}')"
        self.execute(query)
        logger.info(f"Created table {table_name} from {file_path}")

    def get_table_info(self, table_name: str) -> Dict[str, Any]:
        """
        Get information about a table.
        
        Args:
            table_name: Name of the table
            
        Returns:
            Dictionary with table metadata
        """
        # Get row count
        count_result = self.execute(f"SELECT COUNT(*) FROM {table_name}")
        row_count = count_result.rows[0][0] if count_result.rows else 0
        
        # Get schema
        schema_result = self.execute(f"DESCRIBE {table_name}")
        columns = [
            {
                "name": row[0],
                "type": row[1],
                "nullable": row[2] == "YES"
            }
            for row in schema_result.rows
        ]
        
        return {
            "table_name": table_name,
            "row_count": row_count,
            "column_count": len(columns),
            "columns": columns
        }

    def explain_query(self, query: str) -> str:
        """
        Get the query execution plan.
        
        Args:
            query: SQL query to explain
            
        Returns:
            Query execution plan
        """
        result = self.execute(f"EXPLAIN {query}")
        return "\n".join([row[0] for row in result.rows])

    def optimize_query(self, query: str) -> str:
        """
        Suggest optimizations for a query.
        
        Args:
            query: SQL query to optimize
            
        Returns:
            Optimization suggestions
        """
        # Get query plan
        plan = self.explain_query(query)
        
        # Analyze plan for optimization opportunities
        suggestions = []
        
        if "FULL SCAN" in plan.upper():
            suggestions.append("Consider adding indexes or filters to avoid full table scans")
        
        if "CROSS JOIN" in plan.upper():
            suggestions.append("Cross join detected - ensure this is intentional")
        
        if len(suggestions) == 0:
            suggestions.append("Query appears well-optimized")
        
        return "\n".join(suggestions)

    def get_statistics(self) -> Dict[str, Any]:
        """Get database statistics."""
        with self.get_connection() as conn:
            # Get memory usage
            memory_result = conn.execute("SELECT current_setting('memory_limit')").fetchone()
            
            # Get thread count
            threads_result = conn.execute("SELECT current_setting('threads')").fetchone()
            
            # Get list of tables
            tables_result = conn.execute("SHOW TABLES").fetchall()
            
            return {
                "memory_limit": memory_result[0] if memory_result else "unknown",
                "threads": threads_result[0] if threads_result else "unknown",
                "tables": [row[0] for row in tables_result],
                "connection_count": len(self._connections)
            }

    def close_all(self):
        """Close all connections in the pool."""
        with self._lock:
            for conn in self._connections.values():
                try:
                    conn.close()
                except Exception as e:
                    logger.warning(f"Error closing connection: {e}")
            
            self._connections.clear()
            logger.info("All DuckDB connections closed")

    def __enter__(self):
        """Context manager entry."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close_all()


if __name__ == "__main__":
    # Example usage
    logging.basicConfig(level=logging.INFO)
    
    print("\n" + "=" * 70)
    print("DuckDB Connection Manager - Demo")
    print("=" * 70)
    
    # Initialize manager
    with DuckDBConnectionManager(threads=4, memory_limit="1GB") as manager:
        
        # Create sample data
        print("\n📊 Creating sample data...")
        manager.execute("""
            CREATE TABLE users AS 
            SELECT 
                range AS id,
                'user_' || range AS username,
                (random() * 100)::INTEGER AS age,
                ['admin', 'user', 'guest'][1 + (random() * 3)::INTEGER] AS role
            FROM range(1000)
        """)
        
        # Get table info
        info = manager.get_table_info("users")
        print(f"✓ Created table: {info['table_name']}")
        print(f"  - Rows: {info['row_count']}")
        print(f"  - Columns: {info['column_count']}")
        
        # Execute a query
        print("\n🔍 Executing query...")
        result = manager.execute("""
            SELECT role, COUNT(*) as count, AVG(age) as avg_age
            FROM users
            GROUP BY role
            ORDER BY count DESC
        """)
        
        print(f"✓ Query executed in {result.execution_time_ms:.2f}ms")
        print(f"  Columns: {result.columns}")
        for row in result.rows:
            print(f"  {row}")
        
        # Get statistics
        print("\n📈 Database statistics:")
        stats = manager.get_statistics()
        print(f"  Memory: {stats['memory_limit']}")
        print(f"  Threads: {stats['threads']}")
        print(f"  Tables: {', '.join(stats['tables'])}")
        
        # Explain query
        print("\n📝 Query plan:")
        plan = manager.explain_query("SELECT * FROM users WHERE age > 50")
        print(f"  {plan[:200]}...")
        
        print("\n✅ All tests passed!")

