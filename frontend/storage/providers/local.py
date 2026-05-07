from __future__ import annotations

import shutil
from pathlib import Path
from typing import BinaryIO

from frontend.storage.providers.base import StorageProvider


class LocalStorageProvider(StorageProvider):
    def __init__(self, root: Path) -> None:
        self._root = Path(root).resolve()

    def _safe_join(self, relative_posix: str) -> Path:
        raw = str(relative_posix or "").replace("\\", "/").strip().lstrip("/")
        if not raw or ".." in raw.split("/"):
            raise ValueError("Invalid storage path")
        candidate = (self._root / raw).resolve()
        try:
            candidate.relative_to(self._root)
        except Exception as exc:
            raise ValueError("Path escapes storage root") from exc
        return candidate

    def generate_path(self, *segments: str) -> str:
        cleaned = [str(s).strip().strip("/").replace("\\", "/") for s in segments if str(s).strip()]
        if any(".." in part.split("/") for part in cleaned):
            raise ValueError("Invalid path segment")
        return "/".join(cleaned)

    def save_file(self, source: Path | BinaryIO, relative_dest: str) -> int:
        dest = self._safe_join(relative_dest)
        dest.parent.mkdir(parents=True, exist_ok=True)
        if isinstance(source, Path):
            shutil.copy2(source, dest)
            return int(dest.stat().st_size)
        with dest.open("wb") as out:
            shutil.copyfileobj(source, out)
        return int(dest.stat().st_size)

    def delete_file(self, relative_path: str) -> None:
        try:
            p = self._safe_join(relative_path)
        except ValueError:
            return
        try:
            if p.is_file():
                p.unlink()
        except Exception:
            pass

    def file_exists(self, relative_path: str) -> bool:
        try:
            return self._safe_join(relative_path).is_file()
        except ValueError:
            return False

    def open_file(self, relative_path: str) -> BinaryIO:
        return self._safe_join(relative_path).open("rb")

    def try_get_local_path(self, relative_path: str | None) -> Path | None:
        if not relative_path:
            return None
        try:
            p = self._safe_join(relative_path)
            return p if p.is_file() else None
        except ValueError:
            return None

    def absolute_path_for_write(self, relative_dest: str) -> Path:
        """Ensure parent dirs exist and return absolute path (werkzeug upload compatibility)."""
        p = self._safe_join(relative_dest)
        p.parent.mkdir(parents=True, exist_ok=True)
        return p
