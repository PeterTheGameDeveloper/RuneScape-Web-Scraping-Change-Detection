"""Scraper module for OSRS Wiki API."""
from .wiki_client import OSRSWikiClient
from .change_detector import ChangeDetector
from .models import WikiChange, PageSnapshot, ChangeLog

__all__ = ["OSRSWikiClient", "ChangeDetector", "WikiChange", "PageSnapshot", "ChangeLog"]
