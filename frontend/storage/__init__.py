"""Storage abstraction entrypoints (evidence and future retention/archive hooks)."""

from __future__ import annotations

from functools import lru_cache
from pathlib import Path

from frontend.storage.providers.local import LocalStorageProvider


@lru_cache(maxsize=1)
def get_evidence_storage() -> LocalStorageProvider:
    """Evidence artifacts root = FRONTEND_UPLOAD_DIR (same as historical behavior)."""
    from config import FRONTEND_UPLOAD_DIR

    return LocalStorageProvider(Path(FRONTEND_UPLOAD_DIR))


def reset_evidence_storage_cache_for_tests() -> None:
    get_evidence_storage.cache_clear()
