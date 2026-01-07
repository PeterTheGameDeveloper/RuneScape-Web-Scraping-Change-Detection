"""Integration tests for the OSRS Wiki pipeline."""
import pytest
from datetime import datetime, timedelta

from src.scraper import OSRSWikiClient


class TestOSRSWikiAPI:
    """Integration tests against the live OSRS Wiki API."""
    
    @pytest.fixture
    def client(self):
        """Create a rate-limited client for testing."""
        return OSRSWikiClient(rate_limit_rps=0.5)  # Very conservative rate limit
    
    @pytest.mark.integration
    def test_fetch_recent_changes(self, client):
        """Test fetching recent changes from the live API."""
        # Fetch changes from the last hour
        since = datetime.utcnow() - timedelta(hours=1)
        
        change_log = client.get_recent_changes(
            since=since,
            limit=10,
        )
        
        # The wiki is actively edited, so we should get some results
        # (though not guaranteed)
        assert change_log.run_id is not None
        assert change_log.scraped_at is not None
        
        # If we got changes, verify structure
        if change_log.changes:
            change = change_log.changes[0]
            assert change.revid > 0
            assert change.title
            assert change.timestamp
    
    @pytest.mark.integration
    def test_fetch_page_content(self, client):
        """Test fetching page content from a popular page."""
        # Fetch a well-known page
        result = client.get_page_content("Abyssal whip")
        
        # This page definitely exists
        assert result is not None
        assert result["title"] == "Abyssal whip"
        assert result["pageid"] > 0
        assert len(result["content"]) > 100  # Page has substantial content
    
    @pytest.mark.integration
    def test_nonexistent_page(self, client):
        """Test handling of nonexistent pages."""
        result = client.get_page_content("ThisPageDefinitelyDoesNotExist12345")
        assert result is None
    
    @pytest.mark.integration
    def test_rate_limiting(self, client):
        """Test that rate limiting works."""
        import time
        
        start = time.time()
        
        # Make 3 requests - should take at least 4 seconds with 0.5 rps
        for _ in range(3):
            client.get_recent_changes(limit=1)
        
        elapsed = time.time() - start
        assert elapsed >= 4.0  # 3 requests at 0.5 rps = 4 seconds minimum
