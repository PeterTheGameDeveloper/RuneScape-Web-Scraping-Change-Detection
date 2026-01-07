"""Storage module for object storage and warehouse."""
from .object_store import ObjectStore
from .warehouse import DuckDBWarehouse

__all__ = ["ObjectStore", "DuckDBWarehouse"]
