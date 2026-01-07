"""DuckDB warehouse for storing and querying wiki changes."""
import logging
from datetime import datetime
from pathlib import Path
from typing import Optional

import duckdb

from ..scraper.models import WikiChange, ChangeLog, ScrapeState

logger = logging.getLogger(__name__)

# Schema definitions
SCHEMA_SQL = """
-- Raw changes staging table (landing zone from API)
CREATE TABLE IF NOT EXISTS raw_wiki_changes (
    change_id VARCHAR PRIMARY KEY,  -- {pageid}_{revid} for idempotency
    revid BIGINT NOT NULL,
    old_revid BIGINT,
    rcid BIGINT NOT NULL,
    pageid BIGINT NOT NULL,
    title VARCHAR NOT NULL,
    namespace INTEGER DEFAULT 0,
    timestamp TIMESTAMP NOT NULL,
    user_name VARCHAR,
    comment VARCHAR,
    old_len INTEGER,
    new_len INTEGER,
    size_diff INTEGER,
    is_minor BOOLEAN DEFAULT FALSE,
    is_bot BOOLEAN DEFAULT FALSE,
    is_new BOOLEAN DEFAULT FALSE,
    ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    run_id VARCHAR
);

-- Scrape state table for incremental loading
CREATE TABLE IF NOT EXISTS scrape_state (
    id INTEGER PRIMARY KEY DEFAULT 1,
    last_processed_timestamp TIMESTAMP,
    last_processed_rcid BIGINT,
    last_run_at TIMESTAMP,
    total_changes_processed BIGINT DEFAULT 0,
    CHECK (id = 1)  -- Singleton pattern
);

-- Initialize state if not exists
INSERT INTO scrape_state (id) 
SELECT 1 WHERE NOT EXISTS (SELECT 1 FROM scrape_state);

-- Quality metrics table for monitoring
CREATE TABLE IF NOT EXISTS ingestion_metrics (
    run_id VARCHAR PRIMARY KEY,
    started_at TIMESTAMP,
    completed_at TIMESTAMP,
    changes_fetched INTEGER,
    changes_inserted INTEGER,
    changes_duplicate INTEGER,
    status VARCHAR,
    error_message VARCHAR
);

-- Create indexes for common queries
CREATE INDEX IF NOT EXISTS idx_changes_timestamp ON raw_wiki_changes(timestamp);
CREATE INDEX IF NOT EXISTS idx_changes_pageid ON raw_wiki_changes(pageid);
CREATE INDEX IF NOT EXISTS idx_changes_title ON raw_wiki_changes(title);
"""


class DuckDBWarehouse:
    """DuckDB-based data warehouse for wiki changes."""
    
    def __init__(self, db_path: str = "./data/warehouse.duckdb"):
        """
        Initialize the warehouse.
        
        Args:
            db_path: Path to the DuckDB database file
        """
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        
        self.conn = duckdb.connect(str(self.db_path))
        self._initialize_schema()
    
    def _initialize_schema(self) -> None:
        """Create tables and indexes if they don't exist."""
        self.conn.execute(SCHEMA_SQL)
        logger.info(f"Initialized DuckDB schema at {self.db_path}")
    
    def upsert_changes(self, change_log: ChangeLog) -> dict:
        """
        Upsert changes from a change log into the warehouse.
        
        Uses change_id as primary key for idempotent inserts.
        
        Args:
            change_log: ChangeLog containing changes to insert
            
        Returns:
            Dict with insert statistics
        """
        stats = {
            "run_id": change_log.run_id,
            "changes_fetched": len(change_log.changes),
            "changes_inserted": 0,
            "changes_duplicate": 0,
        }
        
        for change in change_log.changes:
            try:
                self.conn.execute("""
                    INSERT INTO raw_wiki_changes (
                        change_id, revid, old_revid, rcid, pageid, title,
                        namespace, timestamp, user_name, comment,
                        old_len, new_len, size_diff, is_minor, is_bot, is_new,
                        ingested_at, run_id
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT (change_id) DO NOTHING
                """, [
                    change.change_id,
                    change.revid,
                    change.old_revid,
                    change.rcid,
                    change.pageid,
                    change.title,
                    change.namespace,
                    change.timestamp,
                    change.user,
                    change.comment,
                    change.old_len,
                    change.new_len,
                    change.size_diff,
                    change.is_minor,
                    change.is_bot,
                    change.is_new,
                    datetime.utcnow(),
                    change_log.run_id,
                ])
                stats["changes_inserted"] += 1
            except duckdb.ConstraintException:
                stats["changes_duplicate"] += 1
        
        logger.info(
            f"Upserted {stats['changes_inserted']} changes, "
            f"{stats['changes_duplicate']} duplicates skipped"
        )
        
        return stats
    
    def get_state(self) -> ScrapeState:
        """
        Get the current scrape state.
        
        Returns:
            ScrapeState with last processed info
        """
        result = self.conn.execute("""
            SELECT 
                last_processed_timestamp,
                last_processed_rcid,
                last_run_at,
                total_changes_processed
            FROM scrape_state
            WHERE id = 1
        """).fetchone()
        
        if result:
            return ScrapeState(
                last_processed_timestamp=result[0],
                last_processed_rcid=result[1],
                last_run_at=result[2],
                total_changes_processed=result[3] or 0,
            )
        return ScrapeState()
    
    def update_state(self, state: ScrapeState) -> None:
        """
        Update the scrape state.
        
        Args:
            state: New state to persist
        """
        self.conn.execute("""
            UPDATE scrape_state SET
                last_processed_timestamp = ?,
                last_processed_rcid = ?,
                last_run_at = ?,
                total_changes_processed = ?
            WHERE id = 1
        """, [
            state.last_processed_timestamp,
            state.last_processed_rcid,
            state.last_run_at,
            state.total_changes_processed,
        ])
        logger.info(f"Updated scrape state: {state}")
    
    def record_metrics(
        self,
        run_id: str,
        started_at: datetime,
        completed_at: datetime,
        stats: dict,
        status: str = "success",
        error_message: Optional[str] = None,
    ) -> None:
        """
        Record ingestion metrics for monitoring.
        
        Args:
            run_id: Unique run identifier
            started_at: When the run started
            completed_at: When the run completed
            stats: Statistics from upsert operation
            status: "success" or "failed"
            error_message: Error message if failed
        """
        self.conn.execute("""
            INSERT INTO ingestion_metrics (
                run_id, started_at, completed_at,
                changes_fetched, changes_inserted, changes_duplicate,
                status, error_message
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT (run_id) DO UPDATE SET
                completed_at = excluded.completed_at,
                changes_fetched = excluded.changes_fetched,
                changes_inserted = excluded.changes_inserted,
                changes_duplicate = excluded.changes_duplicate,
                status = excluded.status,
                error_message = excluded.error_message
        """, [
            run_id,
            started_at,
            completed_at,
            stats.get("changes_fetched", 0),
            stats.get("changes_inserted", 0),
            stats.get("changes_duplicate", 0),
            status,
            error_message,
        ])
    
    def get_recent_changes(self, limit: int = 100) -> list[dict]:
        """
        Get recent changes from the warehouse.
        
        Args:
            limit: Maximum number of changes to return
            
        Returns:
            List of change dictionaries
        """
        result = self.conn.execute(f"""
            SELECT 
                change_id, title, user_name, timestamp,
                size_diff, is_minor, is_bot, is_new, comment
            FROM raw_wiki_changes
            ORDER BY timestamp DESC
            LIMIT {limit}
        """).fetchall()
        
        columns = [
            "change_id", "title", "user_name", "timestamp",
            "size_diff", "is_minor", "is_bot", "is_new", "comment"
        ]
        
        return [dict(zip(columns, row)) for row in result]
    
    def get_change_summary(self, since: Optional[datetime] = None) -> dict:
        """
        Get a summary of changes.
        
        Args:
            since: Only include changes after this timestamp
            
        Returns:
            Dictionary with change statistics
        """
        where_clause = ""
        params = []
        if since:
            where_clause = "WHERE timestamp >= ?"
            params = [since]
        
        result = self.conn.execute(f"""
            SELECT
                COUNT(*) as total_changes,
                COUNT(DISTINCT pageid) as unique_pages,
                COUNT(DISTINCT user_name) as unique_users,
                SUM(CASE WHEN is_new THEN 1 ELSE 0 END) as new_pages,
                SUM(CASE WHEN size_diff > 0 THEN size_diff ELSE 0 END) as bytes_added,
                SUM(CASE WHEN size_diff < 0 THEN ABS(size_diff) ELSE 0 END) as bytes_removed,
                MIN(timestamp) as earliest_change,
                MAX(timestamp) as latest_change
            FROM raw_wiki_changes
            {where_clause}
        """, params).fetchone()
        
        return {
            "total_changes": result[0],
            "unique_pages": result[1],
            "unique_users": result[2],
            "new_pages": result[3],
            "bytes_added": result[4],
            "bytes_removed": result[5],
            "earliest_change": result[6],
            "latest_change": result[7],
        }
    
    def close(self) -> None:
        """Close the database connection."""
        self.conn.close()
    
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
