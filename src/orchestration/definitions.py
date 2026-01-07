"""Dagster definitions for the OSRS Wiki pipeline."""
from dagster import Definitions, load_assets_from_modules

from . import assets
from .resources import get_default_resources
from .sensors import (
    wiki_changes_schedule,
    wiki_ingestion_job,
    new_data_sensor,
    failure_alert_sensor,
)

# Load all assets
all_assets = load_assets_from_modules([assets])

# Create Dagster definitions
defs = Definitions(
    assets=all_assets,
    jobs=[wiki_ingestion_job],
    schedules=[wiki_changes_schedule],
    sensors=[new_data_sensor, failure_alert_sensor],
    resources=get_default_resources(),
)
