"""Change detection logic for wiki content."""
import hashlib
import logging
from datetime import datetime
from typing import Optional

from .models import WikiChange, PageSnapshot, ScrapeState

logger = logging.getLogger(__name__)


class ChangeDetector:
    """Detects and tracks changes between wiki snapshots."""
    
    @staticmethod
    def compute_content_hash(content: str) -> str:
        """
        Compute a SHA256 hash of page content.
        
        Args:
            content: Page content string
            
        Returns:
            Hex digest of the SHA256 hash
        """
        return hashlib.sha256(content.encode("utf-8")).hexdigest()
    
    @staticmethod
    def has_content_changed(old_snapshot: PageSnapshot, new_content: str) -> bool:
        """
        Check if content has changed between snapshots.
        
        Args:
            old_snapshot: Previous page snapshot
            new_content: New content to compare
            
        Returns:
            True if content has changed
        """
        new_hash = ChangeDetector.compute_content_hash(new_content)
        return old_snapshot.content_hash != new_hash
    
    @staticmethod
    def create_snapshot(pageid: int, title: str, revid: int, content: str) -> PageSnapshot:
        """
        Create a new page snapshot.
        
        Args:
            pageid: Wiki page ID
            title: Page title
            revid: Current revision ID
            content: Page content
            
        Returns:
            PageSnapshot instance
        """
        return PageSnapshot(
            pageid=pageid,
            title=title,
            revid=revid,
            content=content,
            content_hash=ChangeDetector.compute_content_hash(content),
            captured_at=datetime.utcnow()
        )
    
    @staticmethod
    def filter_new_changes(
        changes: list[WikiChange],
        state: ScrapeState
    ) -> list[WikiChange]:
        """
        Filter changes to only include those newer than last processed.
        
        This handles late-arriving data by using both timestamp and rcid.
        
        Args:
            changes: List of changes from API
            state: Current scrape state
            
        Returns:
            Filtered list of new changes only
        """
        if state.last_processed_timestamp is None:
            return changes
        
        new_changes = []
        for change in changes:
            # Change is new if:
            # 1. Timestamp is newer than last processed, OR
            # 2. Same timestamp but higher rcid (for late-arriving data)
            if change.timestamp > state.last_processed_timestamp:
                new_changes.append(change)
            elif (
                change.timestamp == state.last_processed_timestamp
                and state.last_processed_rcid is not None
                and change.rcid > state.last_processed_rcid
            ):
                new_changes.append(change)
        
        return new_changes
    
    @staticmethod
    def update_state(state: ScrapeState, changes: list[WikiChange]) -> ScrapeState:
        """
        Update scrape state with the latest processed change info.
        
        Args:
            state: Current scrape state
            changes: Changes that were just processed
            
        Returns:
            Updated ScrapeState
        """
        if not changes:
            return ScrapeState(
                last_processed_timestamp=state.last_processed_timestamp,
                last_processed_rcid=state.last_processed_rcid,
                last_run_at=datetime.utcnow(),
                total_changes_processed=state.total_changes_processed
            )
        
        # Find the newest change (highest timestamp/rcid)
        newest = max(changes, key=lambda c: (c.timestamp, c.rcid))
        
        return ScrapeState(
            last_processed_timestamp=newest.timestamp,
            last_processed_rcid=newest.rcid,
            last_run_at=datetime.utcnow(),
            total_changes_processed=state.total_changes_processed + len(changes)
        )
    
    @staticmethod
    def categorize_changes(changes: list[WikiChange]) -> dict:
        """
        Categorize changes by type for summary reporting.
        
        Args:
            changes: List of wiki changes
            
        Returns:
            Dict with categorized change counts
        """
        categories = {
            "new_pages": 0,
            "edits": 0,
            "minor_edits": 0,
            "bot_edits": 0,
            "total_bytes_added": 0,
            "total_bytes_removed": 0,
            "unique_pages": set(),
            "unique_users": set(),
        }
        
        for change in changes:
            if change.is_new:
                categories["new_pages"] += 1
            else:
                categories["edits"] += 1
            
            if change.is_minor:
                categories["minor_edits"] += 1
            if change.is_bot:
                categories["bot_edits"] += 1
            
            if change.size_diff > 0:
                categories["total_bytes_added"] += change.size_diff
            else:
                categories["total_bytes_removed"] += abs(change.size_diff)
            
            categories["unique_pages"].add(change.pageid)
            categories["unique_users"].add(change.user)
        
        # Convert sets to counts for JSON serialization
        categories["unique_pages_count"] = len(categories["unique_pages"])
        categories["unique_users_count"] = len(categories["unique_users"])
        del categories["unique_pages"]
        del categories["unique_users"]
        
        return categories
