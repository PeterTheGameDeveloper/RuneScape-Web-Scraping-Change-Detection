"""OSRS Wiki API client for fetching recent changes."""
import time
import logging
from datetime import datetime, timedelta
from typing import Optional
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from .models import WikiChange, ChangeLog

logger = logging.getLogger(__name__)

# OSRS Wiki API endpoint
OSRS_WIKI_API = "https://oldschool.runescape.wiki/api.php"

# User-Agent as required by Wiki API policy
USER_AGENT = "OSRSChangeDetector/1.0 (https://github.com/osrs-change-detector; contact@example.com)"


class OSRSWikiClient:
    """Client for interacting with the OSRS Wiki MediaWiki API."""
    
    def __init__(
        self,
        rate_limit_rps: float = 1.0,
        max_retries: int = 3,
        timeout: int = 30
    ):
        """
        Initialize the Wiki client.
        
        Args:
            rate_limit_rps: Maximum requests per second
            max_retries: Number of retries for failed requests
            timeout: Request timeout in seconds
        """
        self.rate_limit_rps = rate_limit_rps
        self.timeout = timeout
        self._last_request_time: Optional[float] = None
        
        # Setup session with retry logic
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": USER_AGENT})
        
        retry_strategy = Retry(
            total=max_retries,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504],
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        self.session.mount("https://", adapter)
        self.session.mount("http://", adapter)
    
    def _rate_limit(self) -> None:
        """Enforce rate limiting between requests."""
        if self._last_request_time is not None:
            elapsed = time.time() - self._last_request_time
            min_interval = 1.0 / self.rate_limit_rps
            if elapsed < min_interval:
                time.sleep(min_interval - elapsed)
        self._last_request_time = time.time()
    
    def _make_request(self, params: dict) -> dict:
        """Make a rate-limited request to the API."""
        self._rate_limit()
        
        # Always request JSON format
        params["format"] = "json"
        
        logger.debug(f"Making API request with params: {params}")
        response = self.session.get(OSRS_WIKI_API, params=params, timeout=self.timeout)
        response.raise_for_status()
        
        return response.json()
    
    def get_recent_changes(
        self,
        since: Optional[datetime] = None,
        until: Optional[datetime] = None,
        limit: int = 500,
        namespace: int = 0,  # Main namespace only by default
        run_id: Optional[str] = None
    ) -> ChangeLog:
        """
        Fetch recent changes from the wiki.
        
        Args:
            since: Start timestamp (exclusive, older boundary)
            until: End timestamp (inclusive, newer boundary)
            limit: Maximum number of changes to fetch (max 500)
            namespace: Namespace to filter (0 = main articles)
            run_id: Unique identifier for this scrape run
        
        Returns:
            ChangeLog containing all fetched changes
        """
        if run_id is None:
            run_id = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        
        all_changes: list[WikiChange] = []
        continue_token: Optional[str] = None
        
        # MediaWiki API returns results in reverse chronological order
        # rcstart = newer timestamp, rcend = older timestamp
        params = {
            "action": "query",
            "list": "recentchanges",
            "rcprop": "title|ids|sizes|flags|user|timestamp|comment",
            "rclimit": min(limit, 500),
            "rcnamespace": namespace,
            "rctype": "edit|new",  # Only edits and new pages
        }
        
        if until:
            params["rcstart"] = until.strftime("%Y-%m-%dT%H:%M:%SZ")
        if since:
            params["rcend"] = since.strftime("%Y-%m-%dT%H:%M:%SZ")
        
        while True:
            if continue_token:
                params["rccontinue"] = continue_token
            
            data = self._make_request(params)
            
            # Parse changes
            for rc in data.get("query", {}).get("recentchanges", []):
                change = WikiChange(
                    revid=rc["revid"],
                    old_revid=rc.get("old_revid", 0),
                    rcid=rc["rcid"],
                    pageid=rc["pageid"],
                    title=rc["title"],
                    namespace=rc.get("ns", 0),
                    timestamp=datetime.fromisoformat(rc["timestamp"].replace("Z", "+00:00")),
                    user=rc.get("user", "Unknown"),
                    comment=rc.get("comment", ""),
                    old_len=rc.get("oldlen", 0),
                    new_len=rc.get("newlen", 0),
                    is_minor="minor" in rc,
                    is_bot="bot" in rc,
                    is_new="new" in rc,
                )
                all_changes.append(change)
            
            # Check for continuation
            if "continue" in data and len(all_changes) < limit:
                continue_token = data["continue"].get("rccontinue")
                if not continue_token:
                    break
            else:
                break
        
        logger.info(f"Fetched {len(all_changes)} changes from OSRS Wiki")
        
        return ChangeLog(
            run_id=run_id,
            scraped_at=datetime.utcnow(),
            changes=all_changes,
            since_timestamp=since,
            until_timestamp=until,
            total_changes=len(all_changes)
        )
    
    def get_page_content(self, title: str) -> Optional[dict]:
        """
        Fetch the current content of a wiki page.
        
        Args:
            title: Page title to fetch
            
        Returns:
            Dict with page content and metadata, or None if not found
        """
        params = {
            "action": "query",
            "titles": title,
            "prop": "revisions",
            "rvprop": "ids|content|timestamp",
            "rvslots": "main",
        }
        
        data = self._make_request(params)
        pages = data.get("query", {}).get("pages", {})
        
        for page_id, page_data in pages.items():
            if page_id == "-1":
                return None  # Page not found
            
            revisions = page_data.get("revisions", [])
            if not revisions:
                return None
            
            rev = revisions[0]
            content = rev.get("slots", {}).get("main", {}).get("*", "")
            
            return {
                "pageid": int(page_id),
                "title": page_data.get("title", title),
                "revid": rev.get("revid"),
                "content": content,
                "timestamp": rev.get("timestamp"),
            }
        
        return None
    
    def get_page_diff(self, from_revid: int, to_revid: int) -> Optional[str]:
        """
        Get the diff between two revisions.
        
        Args:
            from_revid: Old revision ID
            to_revid: New revision ID
            
        Returns:
            HTML diff string or None if unavailable
        """
        params = {
            "action": "compare",
            "fromrev": from_revid,
            "torev": to_revid,
        }
        
        data = self._make_request(params)
        return data.get("compare", {}).get("*")
