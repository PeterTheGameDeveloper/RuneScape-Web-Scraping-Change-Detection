"""CLI entry point for manual pipeline runs."""
import argparse
import logging
import sys
from datetime import datetime, timedelta
from pathlib import Path

from rich.console import Console
from rich.table import Table
from rich.logging import RichHandler

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format="%(message)s",
    handlers=[RichHandler(rich_tracebacks=True)],
)
logger = logging.getLogger(__name__)
console = Console()


def run_scrape(args):
    """Run a single scrape cycle."""
    from src.scraper import OSRSWikiClient, ChangeDetector
    from src.storage import ObjectStore, DuckDBWarehouse
    
    console.print("[bold blue]OSRS Wiki Change Detection Pipeline[/bold blue]")
    console.print()
    
    # Initialize components
    client = OSRSWikiClient(rate_limit_rps=1.0)
    store = ObjectStore(storage_mode="local", base_path="./data/raw")
    db = DuckDBWarehouse(db_path="./data/warehouse.duckdb")
    
    try:
        # Get state
        state = db.get_state()
        since = state.last_processed_timestamp
        if since is None:
            since = datetime.utcnow() - timedelta(hours=args.lookback)
        
        console.print(f"[dim]Fetching changes since {since}[/dim]")
        
        # Fetch changes
        change_log = client.get_recent_changes(since=since, limit=args.limit)
        
        console.print(f"[green][OK][/green] Fetched {change_log.total_changes} changes")
        
        # Store to object storage
        raw_data = {
            "run_id": change_log.run_id,
            "scraped_at": change_log.scraped_at.isoformat(),
            "total_changes": change_log.total_changes,
            "changes": [c.model_dump() for c in change_log.changes],
        }
        storage_path = store.write_json(raw_data, "changes", change_log.scraped_at)
        console.print(f"[green][OK][/green] Stored raw data to {storage_path}")
        
        # Insert into warehouse
        stats = db.upsert_changes(change_log)
        console.print(
            f"[green][OK][/green] Upserted {stats['changes_inserted']} changes "
            f"({stats['changes_duplicate']} duplicates)"
        )
        
        # Update state
        if change_log.changes:
            new_state = ChangeDetector.update_state(state, change_log.changes)
            db.update_state(new_state)
            console.print(f"[green][OK][/green] Updated state")
        
        # Show summary
        if change_log.changes:
            categories = ChangeDetector.categorize_changes(change_log.changes)
            
            table = Table(title="Change Summary")
            table.add_column("Metric", style="cyan")
            table.add_column("Value", style="green")
            
            table.add_row("Total Changes", str(change_log.total_changes))
            table.add_row("New Pages", str(categories["new_pages"]))
            table.add_row("Edits", str(categories["edits"]))
            table.add_row("Unique Pages", str(categories["unique_pages_count"]))
            table.add_row("Unique Users", str(categories["unique_users_count"]))
            table.add_row("Bytes Added", f"+{categories['total_bytes_added']:,}")
            table.add_row("Bytes Removed", f"-{categories['total_bytes_removed']:,}")
            
            console.print(table)
        
    finally:
        db.close()


def show_status(args):
    """Show pipeline status and recent changes."""
    from src.storage import DuckDBWarehouse
    
    db = DuckDBWarehouse(db_path="./data/warehouse.duckdb")
    
    try:
        # Get state
        state = db.get_state()
        
        console.print("[bold]Pipeline Status[/bold]")
        console.print(f"Last processed: {state.last_processed_timestamp or 'Never'}")
        console.print(f"Last run: {state.last_run_at or 'Never'}")
        console.print(f"Total changes processed: {state.total_changes_processed:,}")
        console.print()
        
        # Get summary
        summary = db.get_change_summary()
        
        console.print("[bold]Database Summary[/bold]")
        console.print(f"Total changes in DB: {summary['total_changes']:,}")
        console.print(f"Unique pages: {summary['unique_pages']:,}")
        console.print(f"Unique users: {summary['unique_users']:,}")
        console.print()
        
        # Show recent changes
        if args.recent:
            changes = db.get_recent_changes(limit=args.recent)
            
            table = Table(title=f"Recent {args.recent} Changes")
            table.add_column("Page", style="cyan", max_width=40)
            table.add_column("User", style="yellow")
            table.add_column("Time", style="dim")
            table.add_column("Size", style="green")
            
            for change in changes:
                size = change["size_diff"]
                size_str = f"+{size}" if size > 0 else str(size)
                table.add_row(
                    change["title"][:40],
                    change["user_name"],
                    str(change["timestamp"])[:19],
                    size_str,
                )
            
            console.print(table)
        
    finally:
        db.close()


def main():
    """Main CLI entry point."""
    parser = argparse.ArgumentParser(
        description="OSRS Wiki Change Detection Pipeline",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    
    subparsers = parser.add_subparsers(dest="command", help="Commands")
    
    # Scrape command
    scrape_parser = subparsers.add_parser("scrape", help="Run a scrape cycle")
    scrape_parser.add_argument(
        "--lookback", type=int, default=1,
        help="Hours to look back if no previous state (default: 1)"
    )
    scrape_parser.add_argument(
        "--limit", type=int, default=500,
        help="Maximum changes to fetch (default: 500)"
    )
    
    # Status command
    status_parser = subparsers.add_parser("status", help="Show pipeline status")
    status_parser.add_argument(
        "--recent", type=int, default=10,
        help="Number of recent changes to show (default: 10)"
    )
    
    args = parser.parse_args()
    
    if args.command == "scrape":
        run_scrape(args)
    elif args.command == "status":
        show_status(args)
    else:
        parser.print_help()
        sys.exit(1)


if __name__ == "__main__":
    main()
