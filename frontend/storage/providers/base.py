from __future__ import annotations

from abc import ABC, abstractmethod
from pathlib import Path
from typing import BinaryIO


class StorageProvider(ABC):
    """
    Pluggable blob storage for evidence files.
    Local disk today; object storage can swap in later without changing evidence routes.
    """

    @abstractmethod
    def generate_path(self, *segments: str) -> str:
        """Return a relative POSIX key under this provider's logical root (no leading slash)."""

    @abstractmethod
    def save_file(self, source: Path | BinaryIO, relative_dest: str) -> int:
        """Persist bytes at relative_dest; create parent dirs. Return byte size written."""

    @abstractmethod
    def delete_file(self, relative_path: str) -> None:
        """Best-effort delete; ignore missing keys."""

    @abstractmethod
    def file_exists(self, relative_path: str) -> bool:
        """Return True if object exists."""

    @abstractmethod
    def open_file(self, relative_path: str) -> BinaryIO:
        """Open binary stream for reading; caller must close."""

    def try_get_local_path(self, relative_path: str | None) -> Path | None:
        """
        If this provider is filesystem-backed, return an absolute Path for efficient reads.
        Object-storage backends return None and callers fall back to open_file().
        """
        return None
