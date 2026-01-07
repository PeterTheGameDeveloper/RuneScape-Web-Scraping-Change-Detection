"""Unit tests for the storage module."""
import pytest
import tempfile
import os
from pathlib import Path
from datetime import datetime

from src.storage.object_store import ObjectStore
from src.storage.warehouse import DuckDBWarehouse
from src.scraper.models import WikiChange, ChangeLog


class TestObjectStore:
    """Tests for ObjectStore."""
    
    @pytest.fixture
    def temp_store(self):
        """Create a temporary object store."""
        with tempfile.TemporaryDirectory() as tmpdir:
            store = ObjectStore(
                storage_mode="local",
                base_path=tmpdir,
            )
            yield store
    
    def test_write_and_read_json(self, temp_store):
        """Test writing and reading JSON data."""
        data = {"test": "data", "count": 42}
        
        path = temp_store.write_json(
            data=data,
            data_type="test",
            timestamp=datetime(2024, 1, 15, 12, 0, 0),
        )
        
        assert "test" in path
        assert "2024" in path
        
        read_data = temp_store.read_json(path)
        assert read_data == data
    
    def test_partitioned_path_format(self, temp_store):
        """Test date-partitioned path format."""
        timestamp = datetime(2024, 3, 15, 14, 30, 45)
        path = temp_store._get_partitioned_path("changes", timestamp)
        
        assert "changes/" in path
        assert "year=2024" in path
        assert "month=03" in path
        assert "day=15" in path
    
    def test_list_files(self, temp_store):
        """Test listing files."""
        # Write some test files
        for i in range(3):
            temp_store.write_json(
                data={"index": i},
                data_type="test",
                timestamp=datetime(2024, 1, 15, i, 0, 0),
            )
        
        files = temp_store.list_files(data_type="test")
        assert len(files) == 3
    
    def test_state_persistence(self, temp_store):
        """Test state write and read."""
        state = {"last_timestamp": "2024-01-15T12:00:00", "count": 100}
        
        temp_store.write_state(state, "test_state")
        read_state = temp_store.read_state("test_state")
        
        assert read_state == state


class TestDuckDBWarehouse:
    """Tests for DuckDBWarehouse."""
    
    @pytest.fixture
    def temp_db(self):
        """Create a temporary warehouse."""
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = os.path.join(tmpdir, "test.duckdb")
            with DuckDBWarehouse(db_path=db_path) as db:
                yield db
    
    def test_schema_initialization(self, temp_db):
        """Test that schema is created correctly."""
        # Query should not fail if tables exist
        result = temp_db.conn.execute(
            "SELECT COUNT(*) FROM raw_wiki_changes"
        ).fetchone()
        assert result[0] == 0
    
    def test_upsert_changes(self, temp_db):
        """Test idempotent upsert of changes."""
        changes = [
            WikiChange(
                revid=12345,
                rcid=67890,
                pageid=100,
                title="Test Page",
                timestamp=datetime.utcnow(),
                user="TestUser",
            )
        ]
        
        change_log = ChangeLog(
            run_id="test_run",
            changes=changes,
        )
        
        # First insert
        stats1 = temp_db.upsert_changes(change_log)
        assert stats1["changes_inserted"] == 1
        assert stats1["changes_duplicate"] == 0
        
        # Second insert (should be duplicate)
        stats2 = temp_db.upsert_changes(change_log)
        assert stats2["changes_inserted"] == 0
        assert stats2["changes_duplicate"] == 1
    
    def test_state_management(self, temp_db):
        """Test state get and update."""
        # Initial state should be empty
        state = temp_db.get_state()
        assert state.last_processed_timestamp is None
        
        # Update state
        from src.scraper.models import ScrapeState
        new_state = ScrapeState(
            last_processed_timestamp=datetime.utcnow(),
            last_processed_rcid=12345,
            total_changes_processed=100,
        )
        temp_db.update_state(new_state)
        
        # Read back
        read_state = temp_db.get_state()
        assert read_state.last_processed_rcid == 12345
        assert read_state.total_changes_processed == 100
    
    def test_get_recent_changes(self, temp_db):
        """Test querying recent changes."""
        # Insert test data
        changes = [
            WikiChange(
                revid=i,
                rcid=i,
                pageid=i,
                title=f"Page {i}",
                timestamp=datetime.utcnow(),
                user="User",
            )
            for i in range(5)
        ]
        
        change_log = ChangeLog(run_id="test", changes=changes)
        temp_db.upsert_changes(change_log)
        
        recent = temp_db.get_recent_changes(limit=3)
        assert len(recent) == 3
    
    def test_get_change_summary(self, temp_db):
        """Test change summary statistics."""
        changes = [
            WikiChange(
                revid=1,
                rcid=1,
                pageid=1,
                title="Page 1",
                timestamp=datetime.utcnow(),
                user="User1",
                old_len=100,
                new_len=200,
            ),
            WikiChange(
                revid=2,
                rcid=2,
                pageid=2,
                title="Page 2",
                timestamp=datetime.utcnow(),
                user="User2",
                old_len=200,
                new_len=150,
            ),
        ]
        
        change_log = ChangeLog(run_id="test", changes=changes)
        temp_db.upsert_changes(change_log)
        
        summary = temp_db.get_change_summary()
        assert summary["total_changes"] == 2
        assert summary["unique_pages"] == 2
        assert summary["unique_users"] == 2
