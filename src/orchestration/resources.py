"""Dagster resources for the OSRS Wiki pipeline."""
import os
from dagster import ConfigurableResource, InitResourceContext
from pydantic import Field

from ..scraper import OSRSWikiClient
from ..storage import ObjectStore, DuckDBWarehouse


class WikiClientResource(ConfigurableResource):
    """Dagster resource for OSRS Wiki API client."""
    
    rate_limit_rps: float = Field(
        default=1.0,
        description="Maximum requests per second"
    )
    max_retries: int = Field(
        default=3,
        description="Number of retries for failed requests"
    )
    timeout: int = Field(
        default=30,
        description="Request timeout in seconds"
    )
    
    def create_client(self) -> OSRSWikiClient:
        """Create a new Wiki client instance."""
        return OSRSWikiClient(
            rate_limit_rps=self.rate_limit_rps,
            max_retries=self.max_retries,
            timeout=self.timeout,
        )


class ObjectStoreResource(ConfigurableResource):
    """Dagster resource for object storage."""
    
    storage_mode: str = Field(
        default="local",
        description="Storage mode: 'local' or 'minio'"
    )
    base_path: str = Field(
        default="./data/raw",
        description="Base path for local storage"
    )
    minio_endpoint: str = Field(
        default="localhost:9000",
        description="MinIO endpoint"
    )
    minio_access_key: str = Field(
        default="minioadmin",
        description="MinIO access key"
    )
    minio_secret_key: str = Field(
        default="minioadmin",
        description="MinIO secret key"
    )
    minio_bucket: str = Field(
        default="osrs-wiki-changes",
        description="MinIO bucket name"
    )
    minio_secure: bool = Field(
        default=False,
        description="Use HTTPS for MinIO"
    )
    
    def create_store(self) -> ObjectStore:
        """Create a new object store instance."""
        return ObjectStore(
            storage_mode=self.storage_mode,
            base_path=self.base_path,
            minio_endpoint=self.minio_endpoint,
            minio_access_key=self.minio_access_key,
            minio_secret_key=self.minio_secret_key,
            minio_bucket=self.minio_bucket,
            minio_secure=self.minio_secure,
        )


class WarehouseResource(ConfigurableResource):
    """Dagster resource for DuckDB warehouse."""
    
    db_path: str = Field(
        default="./data/warehouse.duckdb",
        description="Path to DuckDB database"
    )
    
    def create_warehouse(self) -> DuckDBWarehouse:
        """Create a new warehouse instance."""
        return DuckDBWarehouse(db_path=self.db_path)


# Default resource configuration
def get_default_resources() -> dict:
    """Get default resource configuration from environment variables."""
    return {
        "wiki_client": WikiClientResource(
            rate_limit_rps=float(os.getenv("RATE_LIMIT_REQUESTS_PER_SECOND", "1.0")),
        ),
        "object_store": ObjectStoreResource(
            storage_mode=os.getenv("STORAGE_MODE", "local"),
            base_path=os.getenv("DATA_DIR", "./data") + "/raw",
            minio_endpoint=os.getenv("MINIO_ENDPOINT", "localhost:9000"),
            minio_access_key=os.getenv("MINIO_ACCESS_KEY", "minioadmin"),
            minio_secret_key=os.getenv("MINIO_SECRET_KEY", "minioadmin"),
            minio_bucket=os.getenv("MINIO_BUCKET", "osrs-wiki-changes"),
            minio_secure=os.getenv("MINIO_SECURE", "false").lower() == "true",
        ),
        "warehouse": WarehouseResource(
            db_path=os.getenv("DUCKDB_PATH", "./data/warehouse.duckdb"),
        ),
    }
