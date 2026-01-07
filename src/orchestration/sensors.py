"""Dagster schedules and sensors for the OSRS Wiki pipeline."""
import os
from datetime import timedelta

from dagster import (
    ScheduleDefinition,
    sensor,
    SensorEvaluationContext,
    RunRequest,
    SkipReason,
    DefaultSensorStatus,
    DefaultScheduleStatus,
    define_asset_job,
    AssetSelection,
)


# Define the main ingestion job
wiki_ingestion_job = define_asset_job(
    name="wiki_ingestion_job",
    selection=AssetSelection.all(),
    description="Fetch, store, and transform OSRS Wiki changes",
)


# Schedule to run every 15 minutes
wiki_changes_schedule = ScheduleDefinition(
    job=wiki_ingestion_job,
    cron_schedule="*/15 * * * *",  # Every 15 minutes
    description="Fetch OSRS Wiki changes every 15 minutes",
    default_status=DefaultScheduleStatus.STOPPED,  # Start stopped, enable manually
)


@sensor(
    job=wiki_ingestion_job,
    minimum_interval_seconds=60,
    default_status=DefaultSensorStatus.STOPPED,
    description="Sensor that triggers on new data in object storage",
)
def new_data_sensor(context: SensorEvaluationContext):
    """
    Sensor that checks for new data files in object storage.
    
    This provides an alternative to scheduled runs - trigger
    processing when new data arrives.
    """
    from ..storage import ObjectStore
    from datetime import datetime
    
    store = ObjectStore(
        storage_mode=os.getenv("STORAGE_MODE", "local"),
        base_path=os.getenv("DATA_DIR", "./data") + "/raw",
    )
    
    # Check for files created in the last 5 minutes
    recent_files = store.list_files(
        data_type="changes",
        since=datetime.utcnow() - timedelta(minutes=5),
    )
    
    if recent_files:
        # Use the most recent file as the run key to avoid duplicate runs
        latest_file = max(recent_files)
        
        # Check if we've already processed this file
        cursor = context.cursor or ""
        if latest_file != cursor:
            context.update_cursor(latest_file)
            return RunRequest(run_key=latest_file)
    
    return SkipReason("No new data files found")


@sensor(
    job=wiki_ingestion_job,
    minimum_interval_seconds=300,  # Check every 5 minutes
    default_status=DefaultSensorStatus.STOPPED,
    description="Alert sensor for pipeline failures",
)
def failure_alert_sensor(context: SensorEvaluationContext):
    """
    Sensor that monitors for pipeline failures and sends alerts.
    
    In a production environment, this would integrate with
    Slack, PagerDuty, or email for alerting.
    """
    from dagster import DagsterInstance
    from dagster._core.storage.pipeline_run import DagsterRunStatus
    
    instance = context.instance
    
    # Get recent failed runs
    runs = instance.get_runs(
        limit=10,
    )
    
    failed_runs = [
        run for run in runs 
        if run.status == DagsterRunStatus.FAILURE
        and run.job_name == "wiki_ingestion_job"
    ]
    
    if failed_runs:
        latest_failure = failed_runs[0]
        cursor = context.cursor or ""
        
        if latest_failure.run_id != cursor:
            context.update_cursor(latest_failure.run_id)
            
            # Log the failure (in production, send to alerting system)
            context.log.error(
                f"Pipeline failure detected! Run ID: {latest_failure.run_id}"
            )
            
            # Could trigger a recovery job here
            # return RunRequest(run_key=f"recovery_{latest_failure.run_id}")
    
    return SkipReason("No new failures detected")
