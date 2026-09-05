#!/usr/bin/env python3
"""
Table Size and Change Rate Estimation
Queries source databases for table metadata to inform HITL decisions.

Implements the plan's "B.1) Table size + change-rate estimation" section.
"""

import logging
from typing import Dict, Any, List, Optional
from dataclasses import dataclass

from .cdc_pipeline_model import SourceType

logger = logging.getLogger(__name__)


@dataclass
class TableSizeEstimate:
    """Table size and change rate estimate"""
    table_name: str
    estimated_rows: int
    data_size_bytes: int
    index_size_bytes: int
    total_size_bytes: int
    
    # Change rate (if available)
    inserts_since_reset: int = 0
    updates_since_reset: int = 0
    deletes_since_reset: int = 0
    
    @property
    def data_size_mb(self) -> float:
        return self.data_size_bytes / (1024 * 1024)
    
    @property
    def total_size_mb(self) -> float:
        return self.total_size_bytes / (1024 * 1024)
    
    @property
    def total_changes_since_reset(self) -> int:
        return self.inserts_since_reset + self.updates_since_reset + self.deletes_since_reset


class TableSizeEstimator:
    """
    Estimates table sizes and change rates from source databases.
    """
    
    def __init__(self, db_connection):
        """
        Args:
            db_connection: Database connection (could be SQLAlchemy engine, psycopg2, etc.)
        """
        self.db_connection = db_connection
    
    def estimate_mysql_table(self, database: str, table: str) -> Optional[TableSizeEstimate]:
        """
        Estimate MySQL table size using information_schema.
        
        Query:
        SELECT 
          table_name,
          table_rows AS estimated_rows,
          data_length AS data_size_bytes,
          index_length AS index_size_bytes,
          (data_length + index_length) AS total_size_bytes
        FROM information_schema.tables
        WHERE table_schema = %s AND table_name = %s
        """
        try:
            query = """
                SELECT 
                    table_name,
                    COALESCE(table_rows, 0) AS estimated_rows,
                    COALESCE(data_length, 0) AS data_size_bytes,
                    COALESCE(index_length, 0) AS index_size_bytes,
                    COALESCE(data_length + index_length, 0) AS total_size_bytes
                FROM information_schema.tables
                WHERE table_schema = %s AND table_name = %s
            """
            
            # This is a placeholder - actual implementation would use db_connection
            # result = self.db_connection.execute(query, (database, table))
            # row = result.fetchone()
            
            # For now, return None to indicate this needs actual DB connection
            logger.info(f"Would estimate MySQL table: {database}.{table}")
            return None
            
        except Exception as e:
            logger.error(f"Failed to estimate MySQL table {database}.{table}: {e}")
            return None
    
    def estimate_postgres_table(self, schema: str, table: str) -> Optional[TableSizeEstimate]:
        """
        Estimate Postgres table size using pg_stat_user_tables and pg_total_relation_size.
        
        Queries:
        1. Size:
           SELECT pg_total_relation_size('schema.table') AS total_size_bytes
        
        2. Stats:
           SELECT 
             schemaname,
             relname AS table_name,
             n_live_tup AS estimated_rows,
             n_tup_ins AS inserts,
             n_tup_upd AS updates,
             n_tup_del AS deletes
           FROM pg_stat_user_tables
           WHERE schemaname = %s AND relname = %s
        """
        try:
            # Query 1: Size
            size_query = """
                SELECT 
                    pg_total_relation_size(%s || '.' || %s) AS total_size_bytes,
                    pg_relation_size(%s || '.' || %s) AS data_size_bytes,
                    pg_total_relation_size(%s || '.' || %s) - pg_relation_size(%s || '.' || %s) AS index_size_bytes
            """
            
            # Query 2: Stats
            stats_query = """
                SELECT 
                    relname AS table_name,
                    n_live_tup AS estimated_rows,
                    n_tup_ins AS inserts,
                    n_tup_upd AS updates,
                    n_tup_del AS deletes
                FROM pg_stat_user_tables
                WHERE schemaname = %s AND relname = %s
            """
            
            # Placeholder - actual implementation would execute queries
            logger.info(f"Would estimate Postgres table: {schema}.{table}")
            return None
            
        except Exception as e:
            logger.error(f"Failed to estimate Postgres table {schema}.{table}: {e}")
            return None
    
    def estimate_table(
        self,
        source_type: SourceType,
        table_name: str,
    ) -> Optional[TableSizeEstimate]:
        """
        Estimate table size based on source database type.
        
        Args:
            source_type: Database type
            table_name: Fully qualified table name (db.table or schema.table)
            
        Returns:
            TableSizeEstimate or None if estimation fails
        """
        parts = table_name.split(".", 1)
        if len(parts) != 2:
            logger.error(f"Invalid table name format: {table_name}. Expected db.table or schema.table")
            return None
        
        db_or_schema, table = parts
        
        if source_type == SourceType.MYSQL:
            return self.estimate_mysql_table(db_or_schema, table)
        elif source_type in (SourceType.POSTGRESQL, SourceType.POSTGRES):
            return self.estimate_postgres_table(db_or_schema, table)
        else:
            logger.warning(f"Table size estimation not implemented for {source_type}")
            return None
    
    def estimate_all_tables(
        self,
        source_type: SourceType,
        tables: List[str],
    ) -> Dict[str, TableSizeEstimate]:
        """
        Estimate sizes for multiple tables.
        
        Args:
            source_type: Database type
            tables: List of fully qualified table names
            
        Returns:
            Dict mapping table names to their estimates
        """
        estimates = {}
        
        for table in tables:
            estimate = self.estimate_table(source_type, table)
            if estimate:
                estimates[table] = estimate
        
        return estimates


class ChangeRateEstimator:
    """
    Estimates change rates from database statistics.
    Note: Rates are relative to stats reset time, not absolute.
    """
    
    def __init__(self, db_connection):
        self.db_connection = db_connection
    
    def estimate_postgres_change_rate(
        self,
        schema: str,
        table: str,
        time_window_hours: float = 24.0,
    ) -> Optional[int]:
        """
        Estimate Postgres change rate using pg_stat_user_tables.
        
        Returns approximate changes per hour.
        Note: This is a rough estimate based on stats since last reset.
        """
        try:
            # Get stats and last stats reset time
            query = """
                SELECT 
                    n_tup_ins + n_tup_upd + n_tup_del AS total_changes,
                    EXTRACT(EPOCH FROM (NOW() - stats_reset)) / 3600 AS hours_since_reset
                FROM pg_stat_user_tables
                WHERE schemaname = %s AND relname = %s
            """
            
            # Placeholder
            logger.info(f"Would estimate change rate for: {schema}.{table}")
            return None
            
        except Exception as e:
            logger.error(f"Failed to estimate Postgres change rate for {schema}.{table}: {e}")
            return None
    
    def estimate_change_rate(
        self,
        source_type: SourceType,
        table_name: str,
        time_window_hours: float = 24.0,
    ) -> Optional[int]:
        """
        Estimate change rate (changes per hour) for a table.
        
        Args:
            source_type: Database type
            table_name: Fully qualified table name
            time_window_hours: Time window for rate estimation
            
        Returns:
            Estimated changes per hour, or None if unavailable
        """
        parts = table_name.split(".", 1)
        if len(parts) != 2:
            return None
        
        db_or_schema, table = parts
        
        if source_type in (SourceType.POSTGRESQL, SourceType.POSTGRES):
            return self.estimate_postgres_change_rate(db_or_schema, table, time_window_hours)
        else:
            # MySQL and others don't have built-in change tracking
            logger.warning(f"Change rate estimation not available for {source_type}")
            return None


def format_size_for_display(size_bytes: int) -> str:
    """Format size in bytes for human-readable display"""
    if size_bytes < 1024:
        return f"{size_bytes} B"
    elif size_bytes < 1024 * 1024:
        return f"{size_bytes / 1024:.1f} KB"
    elif size_bytes < 1024 * 1024 * 1024:
        return f"{size_bytes / (1024 * 1024):.1f} MB"
    else:
        return f"{size_bytes / (1024 * 1024 * 1024):.2f} GB"


def estimate_snapshot_time(
    estimated_rows: int,
    throughput_rows_per_second: int = 10000,
) -> str:
    """
    Estimate initial snapshot time.
    
    Args:
        estimated_rows: Number of rows in table
        throughput_rows_per_second: Assumed snapshot throughput
        
    Returns:
        Human-readable time estimate
    """
    if estimated_rows == 0:
        return "< 1 minute"
    
    seconds = estimated_rows / throughput_rows_per_second
    
    if seconds < 60:
        return f"~{int(seconds)} seconds"
    elif seconds < 3600:
        return f"~{int(seconds / 60)} minutes"
    else:
        hours = seconds / 3600
        return f"~{hours:.1f} hours"

