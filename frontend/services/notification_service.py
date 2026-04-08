from __future__ import annotations

from datetime import datetime
from typing import Any


def create_notification(
    session: Any,
    NotificationModel: Any,
    *,
    user_id: int,
    title: str,
    message: str,
    notification_type: str,
    link: str | None = None,
) -> Any:
    row = NotificationModel(
        user_id=int(user_id),
        title=str(title or "").strip(),
        message=str(message or "").strip(),
        type=str(notification_type or "info").strip() or "info",
        link=str(link or "").strip() or None,
        is_read=False,
        created_at=datetime.utcnow(),
    )
    session.add(row)
    return row


def notify_users(
    session: Any,
    NotificationModel: Any,
    user_ids: list[int],
    *,
    title: str,
    message: str,
    notification_type: str,
    link: str | None = None,
) -> None:
    seen: set[int] = set()
    for user_id in user_ids:
        uid = int(user_id)
        if uid in seen:
            continue
        seen.add(uid)
        create_notification(
            session,
            NotificationModel,
            user_id=uid,
            title=title,
            message=message,
            notification_type=notification_type,
            link=link,
        )


def unread_count(NotificationModel: Any, *, user_id: int) -> int:
    return int(
        NotificationModel.query.filter_by(user_id=int(user_id), is_read=False).count()
    )


def recent_notifications(NotificationModel: Any, *, user_id: int, limit: int = 10) -> list[Any]:
    return (
        NotificationModel.query.filter_by(user_id=int(user_id))
        .order_by(NotificationModel.created_at.desc())
        .limit(max(1, int(limit)))
        .all()
    )


def mark_all_read(session: Any, NotificationModel: Any, *, user_id: int) -> int:
    rows = NotificationModel.query.filter_by(user_id=int(user_id), is_read=False).all()
    for row in rows:
        row.is_read = True
    return len(rows)


def mark_one_read(session: Any, NotificationModel: Any, *, user_id: int, notification_id: int) -> bool:
    row = NotificationModel.query.filter_by(id=int(notification_id), user_id=int(user_id)).first()
    if row is None:
        return False
    row.is_read = True
    return True
