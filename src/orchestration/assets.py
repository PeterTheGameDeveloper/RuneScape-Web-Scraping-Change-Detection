"""Dagster assets for the OSRS Wiki change detection pipeline."""
import json
import logging
from datetime import datetime, timedelta
from typing import Optional

from dagster import (
    asset,
    AssetExecutionContext,
    MetadataValue,
    Output,
    AssetKey,
)

from ..scraper import OSRSWikiClient, ChangeDetector
from ..scraper.models import ChangeLog, ScrapeState
from ..storage import ObjectStore, DuckDBWarehouse
from .resources import WikiClientResource, ObjectStoreResource, WarehouseResource

logger = logging.getLogger(__name__)


@asset(
    description="Fetch recent changes from OSRS Wiki API",
    group_name="ingestion",
    compute_kind="api",
)
def raw_wiki_changes(
    context: AssetExecutionContext,
    wiki_client: WikiClientResource,
    object_store: ObjectStoreResource,
    warehouse: WarehouseResource,
) -> Output[dict]:
    """
    Fetch recent changes from the OSRS Wiki API.
    
    This asset implements incremental loading by tracking the last
    processed timestamp and only fetching newer changes.
    """
    run_id = context.run_id
    started_at = datetime.utcnow()
    
    # Initialize resources
    client = wiki_client.create_client()
    store = object_store.create_store()
    db = warehouse.create_warehouse()
    
    try:
        # Get current state for incremental loading
        state = db.get_state()
        
        # Calculate time range
        # Default to last hour if no previous state
        since = state.last_processed_timestamp
        if since is None:
            since = datetime.utcnow() - timedelta(hours=1)
        
        context.log.info(f"Fetching changes since {since}")
        
        # Fetch changes from API
        change_log = client.get_recent_changes(
            since=since,
            run_id=run_id,
        )
        
        # Store raw data to object storage
        raw_data = {
            "run_id": change_log.run_id,
            "scraped_at": change_log.scraped_at.isoformat(),
            "since_timestamp": since.isoformat() if since else None,
            "total_changes": change_log.total_changes,
            "changes": [c.model_dump() for c in change_log.changes],
        }
        
        storage_path = store.write_json(
            data=raw_data,
            data_type="changes",
            timestamp=change_log.scraped_at,
        )
        
        context.log.info(f"Stored {change_log.total_changes} changes to {storage_path}")
        
        # Prepare output
        output_data = {
            "run_id": run_id,
            "changes_count": change_log.total_changes,
            "storage_path": storage_path,
            "since_timestamp": since.isoformat() if since else None,
        }
        
        return Output(
            output_data,
            metadata={
                "changes_count": change_log.total_changes,
                "storage_path": storage_path,
                "since_timestamp": since.isoformat() if since else "N/A",
            }
        )
        
    finally:
        db.close()


@asset(
    description="Load raw changes into DuckDB warehouse",
    group_name="ingestion",
    compute_kind="duckdb",
    deps=[AssetKey("raw_wiki_changes")],
)
def staged_wiki_changes(
    context: AssetExecutionContext,
    warehouse: WarehouseResource,
    object_store: ObjectStoreResource,
) -> Output[dict]:
    """
    Load raw changes from object storage into the DuckDB warehouse.
    
    Implements idempotent upserts using change_id as primary key.
    """
    run_id = context.run_id
    started_at = datetime.utcnow()
    
    db = warehouse.create_warehouse()
    store = object_store.create_store()
    
    try:
        # Find the latest raw data file
        # In a full implementation, this would use asset dependencies
        # For now, we read from the most recent file
        files = store.list_files(
            data_type="changes",
            since=datetime.utcnow() - timedelta(hours=1),
        )
        
        if not files:
            context.log.warning("No raw data files found")
            return Output(
                {"run_id": run_id, "changes_inserted": 0},
                metadata={"changes_inserted": 0}
            )
        
        total_inserted = 0
        total_duplicates = 0
        
        for file_path in files:
            raw_data = store.read_json(file_path)
            if not raw_data:
                continue
            
            # Reconstruct ChangeLog from raw data
            from ..scraper.models import WikiChange
            
            changes = []
            for c in raw_data.get("changes", []):
                # Handle datetime parsing
                if isinstance(c.get("timestamp"), str):
                    c["timestamp"] = datetime.fromisoformat(c["timestamp"].replace("Z", "+00:00"))
                changes.append(WikiChange(**c))
            
            change_log = ChangeLog(
                run_id=raw_data.get("run_id", run_id),
                scraped_at=datetime.fromisoformat(raw_data["scraped_at"]),
                changes=changes,
            )
            
            # Upsert into warehouse
            stats = db.upsert_changes(change_log)
            total_inserted += stats["changes_inserted"]
            total_duplicates += stats["changes_duplicate"]
            
            # Update state
            if changes:
                new_state = ChangeDetector.update_state(db.get_state(), changes)
                db.update_state(new_state)
        
        # Record metrics
        db.record_metrics(
            run_id=run_id,
            started_at=started_at,
            completed_at=datetime.utcnow(),
            stats={"changes_inserted": total_inserted, "changes_duplicate": total_duplicates},
        )
        
        context.log.info(f"Inserted {total_inserted} changes, {total_duplicates} duplicates")
        
        return Output(
            {
                "run_id": run_id,
                "changes_inserted": total_inserted,
                "changes_duplicate": total_duplicates,
            },
            metadata={
                "changes_inserted": total_inserted,
                "changes_duplicate": total_duplicates,
            }
        )
        
    finally:
        db.close()


@asset(
    description="Run dbt models to transform staged data",
    group_name="transformation",
    compute_kind="dbt",
    deps=[AssetKey("staged_wiki_changes")],
)
def dbt_models(
    context: AssetExecutionContext,
) -> Output[dict]:
    """
    Run dbt models to transform staged data into analytics-ready tables.
    
    This asset triggers dbt run and dbt test.
    """
    import subprocess
    import os
    
    dbt_project_dir = os.path.join(
        os.path.dirname(os.path.dirname(os.path.dirname(__file__))),
        "dbt_project"
    )
    
    # Adjust for package installation location
    if not os.path.exists(dbt_project_dir):
        dbt_project_dir = "./dbt_project"
    
    context.log.info(f"Running dbt from {dbt_project_dir}")
    
    try:
        # Run dbt models
        run_result = subprocess.run(
            ["dbt", "run", "--profiles-dir", dbt_project_dir],
            cwd=dbt_project_dir,
            capture_output=True,
            text=True,
        )
        
        context.log.info(f"dbt run output: {run_result.stdout}")
        if run_result.returncode != 0:
            context.log.error(f"dbt run error: {run_result.stderr}")
        
        # Run dbt tests
        test_result = subprocess.run(
            ["dbt", "test", "--profiles-dir", dbt_project_dir],
            cwd=dbt_project_dir,
            capture_output=True,
            text=True,
        )
        
        context.log.info(f"dbt test output: {test_result.stdout}")
        if test_result.returncode != 0:
            context.log.warning(f"dbt test warnings: {test_result.stderr}")
        
        return Output(
            {
                "dbt_run_success": run_result.returncode == 0,
                "dbt_test_success": test_result.returncode == 0,
            },
            metadata={
                "dbt_run_success": run_result.returncode == 0,
                "dbt_test_success": test_result.returncode == 0,
            }
        )
        
    except FileNotFoundError:
        context.log.warning("dbt not found in PATH, skipping transformation")
        return Output(
            {"dbt_run_success": False, "error": "dbt not found"},
            metadata={"error": "dbt not installed"}
        )
