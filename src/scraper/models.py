"""Pydantic models for OSRS Wiki change detection."""
from datetime import datetime
from typing import Optional
from pydantic import BaseModel, Field


class WikiChange(BaseModel):
    """Represents a single change from the Recent Changes API."""
    
    revid: int = Field(..., description="Revision ID - unique identifier for this change")
    old_revid: int = Field(0, description="Previous revision ID")
    rcid: int = Field(..., description="Recent change ID")
    pageid: int = Field(..., description="Page ID")
    title: str = Field(..., description="Page title")
    namespace: int = Field(0, description="Namespace ID (0 = main)")
    timestamp: datetime = Field(..., description="When the change occurred")
    user: str = Field(..., description="Username who made the change")
    comment: str = Field("", description="Edit summary/comment")
    old_len: int = Field(0, description="Old page length in bytes")
    new_len: int = Field(0, description="New page length in bytes")
    is_minor: bool = Field(False, description="Whether this is a minor edit")
    is_bot: bool = Field(False, description="Whether this was made by a bot")
    is_new: bool = Field(False, description="Whether this creates a new page")
    
    @property
    def size_diff(self) -> int:
        """Calculate the size difference of this edit."""
        return self.new_len - self.old_len
    
    @property
    def change_id(self) -> str:
        """Generate a unique change ID for idempotency."""
        return f"{self.pageid}_{self.revid}"
    
    class Config:
        frozen = True


class PageSnapshot(BaseModel):
    """Represents a snapshot of a wiki page's content."""
    
    pageid: int = Field(..., description="Page ID")
    title: str = Field(..., description="Page title")
    revid: int = Field(..., description="Current revision ID")
    content: str = Field(..., description="Page content (wikitext)")
    content_hash: str = Field(..., description="SHA256 hash of content")
    captured_at: datetime = Field(default_factory=datetime.utcnow)
    
    class Config:
        frozen = True


class ChangeLog(BaseModel):
    """A batch of changes captured in a single scrape run."""
    
    run_id: str = Field(..., description="Unique identifier for this scrape run")
    scraped_at: datetime = Field(default_factory=datetime.utcnow)
    changes: list[WikiChange] = Field(default_factory=list)
    since_timestamp: Optional[datetime] = Field(None, description="Timestamp we started from")
    until_timestamp: Optional[datetime] = Field(None, description="Timestamp we ended at")
    total_changes: int = Field(0, description="Total number of changes in this batch")
    
    def __init__(self, **data):
        super().__init__(**data)
        object.__setattr__(self, 'total_changes', len(self.changes))
    
    class Config:
        frozen = True


class ScrapeState(BaseModel):
    """Tracks the state of the scraper for incremental loading."""
    
    last_processed_timestamp: Optional[datetime] = None
    last_processed_rcid: Optional[int] = None
    last_run_at: Optional[datetime] = None
    total_changes_processed: int = 0
