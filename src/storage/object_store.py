"""Object storage abstraction for raw data storage."""
import json
import logging
import os
from datetime import datetime
from pathlib import Path
from typing import Optional, Union

logger = logging.getLogger(__name__)


class ObjectStore:
    """
    Abstraction for object storage (local filesystem or MinIO).
    
    Provides a consistent interface for storing raw JSON data with
    date-partitioned paths for easy querying.
    """
    
    def __init__(
        self,
        storage_mode: str = "local",
        base_path: str = "./data/raw",
        minio_endpoint: Optional[str] = None,
        minio_access_key: Optional[str] = None,
        minio_secret_key: Optional[str] = None,
        minio_bucket: Optional[str] = None,
        minio_secure: bool = False,
    ):
        """
        Initialize the object store.
        
        Args:
            storage_mode: "local" for filesystem, "minio" for MinIO
            base_path: Base path for local storage
            minio_*: MinIO configuration parameters
        """
        self.storage_mode = storage_mode
        self.base_path = Path(base_path)
        
        if storage_mode == "minio":
            try:
                from minio import Minio
                self.minio_client = Minio(
                    minio_endpoint,
                    access_key=minio_access_key,
                    secret_key=minio_secret_key,
                    secure=minio_secure,
                )
                self.bucket = minio_bucket
                
                # Ensure bucket exists
                if not self.minio_client.bucket_exists(self.bucket):
                    self.minio_client.make_bucket(self.bucket)
                    logger.info(f"Created MinIO bucket: {self.bucket}")
            except ImportError:
                raise ImportError("MinIO mode requires 'minio' package. Install with: pip install minio")
        else:
            # Local mode - ensure base directory exists
            self.base_path.mkdir(parents=True, exist_ok=True)
            self.minio_client = None
            self.bucket = None
    
    def _get_partitioned_path(self, data_type: str, timestamp: datetime) -> str:
        """
        Generate a date-partitioned path for data.
        
        Format: {data_type}/year={yyyy}/month={mm}/day={dd}/{timestamp}.json
        
        Args:
            data_type: Type of data (e.g., "changes", "snapshots")
            timestamp: Timestamp for partitioning
            
        Returns:
            Partitioned path string
        """
        return (
            f"{data_type}/"
            f"year={timestamp.year}/"
            f"month={timestamp.month:02d}/"
            f"day={timestamp.day:02d}/"
            f"{timestamp.strftime('%Y%m%d_%H%M%S')}.json"
        )
    
    def write_json(
        self,
        data: Union[dict, list],
        data_type: str,
        timestamp: Optional[datetime] = None,
        custom_path: Optional[str] = None,
    ) -> str:
        """
        Write JSON data to storage.
        
        Args:
            data: Data to serialize and store
            data_type: Type of data for path partitioning
            timestamp: Timestamp for partitioning (defaults to now)
            custom_path: Optional custom path override
            
        Returns:
            Path where data was written
        """
        timestamp = timestamp or datetime.utcnow()
        path = custom_path or self._get_partitioned_path(data_type, timestamp)
        
        json_data = json.dumps(data, default=str, indent=2)
        
        if self.storage_mode == "minio":
            from io import BytesIO
            data_bytes = json_data.encode("utf-8")
            self.minio_client.put_object(
                self.bucket,
                path,
                BytesIO(data_bytes),
                len(data_bytes),
                content_type="application/json",
            )
            full_path = f"s3://{self.bucket}/{path}"
        else:
            full_path = self.base_path / path
            full_path.parent.mkdir(parents=True, exist_ok=True)
            full_path.write_text(json_data, encoding="utf-8")
            full_path = str(full_path)
        
        logger.info(f"Wrote data to {full_path}")
        return full_path
    
    def read_json(self, path: str) -> Optional[Union[dict, list]]:
        """
        Read JSON data from storage.
        
        Args:
            path: Path to read (relative for local, full key for MinIO)
            
        Returns:
            Parsed JSON data or None if not found
        """
        try:
            if self.storage_mode == "minio":
                response = self.minio_client.get_object(self.bucket, path)
                data = json.loads(response.read().decode("utf-8"))
                response.close()
                response.release_conn()
            else:
                full_path = self.base_path / path if not Path(path).is_absolute() else Path(path)
                data = json.loads(full_path.read_text(encoding="utf-8"))
            
            return data
        except Exception as e:
            logger.warning(f"Failed to read {path}: {e}")
            return None
    
    def list_files(
        self,
        data_type: str,
        prefix: Optional[str] = None,
        since: Optional[datetime] = None,
    ) -> list[str]:
        """
        List files in storage.
        
        Args:
            data_type: Type of data to list
            prefix: Additional path prefix
            since: Only list files newer than this timestamp
            
        Returns:
            List of file paths
        """
        search_prefix = data_type
        if prefix:
            search_prefix = f"{search_prefix}/{prefix}"
        
        files = []
        
        if self.storage_mode == "minio":
            objects = self.minio_client.list_objects(
                self.bucket,
                prefix=search_prefix,
                recursive=True,
            )
            for obj in objects:
                if since and obj.last_modified < since:
                    continue
                files.append(obj.object_name)
        else:
            search_path = self.base_path / search_prefix
            if search_path.exists():
                for file_path in search_path.rglob("*.json"):
                    if since:
                        mtime = datetime.fromtimestamp(file_path.stat().st_mtime)
                        if mtime < since:
                            continue
                    files.append(str(file_path.relative_to(self.base_path)))
        
        return sorted(files)
    
    def write_state(self, state: dict, state_name: str = "scrape_state") -> str:
        """
        Write scraper state for incremental loading.
        
        Args:
            state: State dictionary to persist
            state_name: Name of the state file
            
        Returns:
            Path where state was written
        """
        return self.write_json(
            state,
            data_type="state",
            custom_path=f"state/{state_name}.json",
        )
    
    def read_state(self, state_name: str = "scrape_state") -> Optional[dict]:
        """
        Read scraper state for incremental loading.
        
        Args:
            state_name: Name of the state file
            
        Returns:
            State dictionary or None if not found
        """
        return self.read_json(f"state/{state_name}.json")
