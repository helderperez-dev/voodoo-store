"""Python bindings for Voodoo Store."""

from ._native import (
    AlreadyOpenError,
    CorruptionError,
    DiskFullError,
    NamespaceQuotaError,
    IoError,
    ReservedKeyError,
    Store,
    StoreBusyError,
    StoreClosedError,
    Transaction,
    TransactionFinishedError,
    VerificationReport,
    VoodooStoreError,
)
from .backup import create_backup_manifest, verify_backup_manifest
from .migrations import MigrationConflict, MigrationError, MigrationReport, migrate_collection
from .objects import iter_object_chunks

__all__ = [
    "AlreadyOpenError",
    "CorruptionError",
    "DiskFullError",
    "NamespaceQuotaError",
    "IoError",
    "ReservedKeyError",
    "Store",
    "StoreBusyError",
    "StoreClosedError",
    "Transaction",
    "TransactionFinishedError",
    "VerificationReport",
    "VoodooStoreError",
    "create_backup_manifest",
    "verify_backup_manifest",
    "MigrationConflict",
    "MigrationError",
    "MigrationReport",
    "migrate_collection",
    "iter_object_chunks",
]

__version__ = "0.3.1"
