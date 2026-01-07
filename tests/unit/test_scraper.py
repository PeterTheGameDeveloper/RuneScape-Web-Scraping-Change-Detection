"""Unit tests for the scraper module."""
import pytest
from datetime import datetime, timedelta
from src.scraper.models import WikiChange, ChangeLog, ScrapeState
from src.scraper.change_detector import ChangeDetector


class TestWikiChange:
    """Tests for WikiChange model."""
    
    def test_change_id_generation(self):
        """Test unique change ID generation."""
        change = WikiChange(
            revid=12345,
            rcid=67890,
            pageid=100,
            title="Test Page",
            timestamp=datetime.utcnow(),
            user="TestUser",
        )
        assert change.change_id == "100_12345"
    
    def test_size_diff_calculation(self):
        """Test size difference calculation."""
        change = WikiChange(
            revid=12345,
            rcid=67890,
            pageid=100,
            title="Test Page",
            timestamp=datetime.utcnow(),
            user="TestUser",
            old_len=1000,
            new_len=1500,
        )
        assert change.size_diff == 500


class TestChangeDetector:
    """Tests for ChangeDetector logic."""
    
    def test_content_hash_consistency(self):
        """Test that same content produces same hash."""
        content = "This is test content"
        hash1 = ChangeDetector.compute_content_hash(content)
        hash2 = ChangeDetector.compute_content_hash(content)
        assert hash1 == hash2
    
    def test_content_hash_difference(self):
        """Test that different content produces different hash."""
        hash1 = ChangeDetector.compute_content_hash("Content A")
        hash2 = ChangeDetector.compute_content_hash("Content B")
        assert hash1 != hash2
    
    def test_filter_new_changes_empty_state(self):
        """Test filtering with empty state returns all changes."""
        changes = [
            WikiChange(
                revid=1,
                rcid=1,
                pageid=1,
                title="Test",
                timestamp=datetime.utcnow(),
                user="User",
            )
        ]
        state = ScrapeState()
        
        filtered = ChangeDetector.filter_new_changes(changes, state)
        assert len(filtered) == 1
    
    def test_filter_new_changes_with_state(self):
        """Test filtering removes old changes."""
        now = datetime.utcnow()
        old_time = now - timedelta(hours=1)
        
        changes = [
            WikiChange(
                revid=1,
                rcid=1,
                pageid=1,
                title="Old",
                timestamp=old_time,
                user="User",
            ),
            WikiChange(
                revid=2,
                rcid=2,
                pageid=2,
                title="New",
                timestamp=now,
                user="User",
            ),
        ]
        
        state = ScrapeState(
            last_processed_timestamp=old_time,
            last_processed_rcid=1,
        )
        
        filtered = ChangeDetector.filter_new_changes(changes, state)
        assert len(filtered) == 1
        assert filtered[0].title == "New"
    
    def test_update_state(self):
        """Test state update with new changes."""
        now = datetime.utcnow()
        changes = [
            WikiChange(
                revid=1,
                rcid=1,
                pageid=1,
                title="Test",
                timestamp=now,
                user="User",
            ),
        ]
        
        old_state = ScrapeState(total_changes_processed=10)
        new_state = ChangeDetector.update_state(old_state, changes)
        
        assert new_state.last_processed_timestamp == now
        assert new_state.last_processed_rcid == 1
        assert new_state.total_changes_processed == 11
    
    def test_categorize_changes(self):
        """Test change categorization."""
        changes = [
            WikiChange(
                revid=1,
                rcid=1,
                pageid=1,
                title="New Page",
                timestamp=datetime.utcnow(),
                user="User1",
                is_new=True,
                old_len=0,
                new_len=500,
            ),
            WikiChange(
                revid=2,
                rcid=2,
                pageid=2,
                title="Minor Edit",
                timestamp=datetime.utcnow(),
                user="User2",
                is_minor=True,
                old_len=1000,
                new_len=900,
            ),
        ]
        
        categories = ChangeDetector.categorize_changes(changes)
        
        assert categories["new_pages"] == 1
        assert categories["edits"] == 1
        assert categories["minor_edits"] == 1
        assert categories["total_bytes_added"] == 500
        assert categories["total_bytes_removed"] == 100
        assert categories["unique_pages_count"] == 2
        assert categories["unique_users_count"] == 2


class TestChangeLog:
    """Tests for ChangeLog model."""
    
    def test_total_changes_count(self):
        """Test that total_changes is set correctly."""
        changes = [
            WikiChange(
                revid=i,
                rcid=i,
                pageid=i,
                title=f"Test {i}",
                timestamp=datetime.utcnow(),
                user="User",
            )
            for i in range(5)
        ]
        
        log = ChangeLog(
            run_id="test_run",
            changes=changes,
        )
        
        assert log.total_changes == 5
