"""Lightweight presence helpers based on last_seen timestamps (no realtime infra)."""

from __future__ import annotations

from datetime import datetime

DEFAULT_ONLINE_WINDOW_SECONDS = 300


def is_online_from_last_seen(
    last_seen_at: datetime | None,
    *,
    now: datetime | None = None,
    window_seconds: int = DEFAULT_ONLINE_WINDOW_SECONDS,
) -> bool:
    """True if last_seen_at falls within the sliding window ending at now (UTC)."""
    if last_seen_at is None:
        return False
    current = now if now is not None else datetime.utcnow()
    try:
        delta = (current - last_seen_at).total_seconds()
    except Exception:
        return False
    return 0 <= delta <= float(window_seconds)
