from __future__ import annotations

from datetime import datetime
from typing import Any

from sqlalchemy import or_


def _normalize_company_name(value: Any) -> str:
    return " ".join(str(value or "").strip().lower().split())


def thread_id_for_users(user_a_id: int, user_b_id: int) -> str:
    left, right = sorted((int(user_a_id), int(user_b_id)))
    return f"{left}:{right}"


def can_message_user(current_user: Any, other_user: Any) -> bool:
    if not current_user or not other_user:
        return False
    if int(current_user.id) == int(other_user.id):
        return False
    if getattr(current_user, "is_admin", False):
        return True
    return _normalize_company_name(getattr(current_user, "company_name", "")) == _normalize_company_name(
        getattr(other_user, "company_name", "")
    )


def available_contacts(UserModel: Any, current_user: Any, *, limit: int = 50) -> list[Any]:
    base_query = UserModel.query.filter(UserModel.id != int(current_user.id))
    if not getattr(current_user, "is_admin", False):
        base_query = base_query.filter(
            UserModel.company_name == getattr(current_user, "company_name", "")
        )
    return base_query.order_by(UserModel.email.asc()).limit(max(1, int(limit))).all()


def list_conversations(MessageModel: Any, UserModel: Any, current_user: Any, *, limit: int = 20) -> list[dict[str, Any]]:
    rows = (
        MessageModel.query.filter(
            or_(
                MessageModel.sender_id == int(current_user.id),
                MessageModel.receiver_id == int(current_user.id),
            )
        )
        .order_by(MessageModel.created_at.desc())
        .all()
    )
    seen_threads: set[str] = set()
    conversations: list[dict[str, Any]] = []
    for row in rows:
        if row.thread_id in seen_threads:
            continue
        other_user_id = row.receiver_id if int(row.sender_id) == int(current_user.id) else row.sender_id
        other_user = UserModel.query.get(int(other_user_id))
        if other_user is None or not can_message_user(current_user, other_user):
            continue
        seen_threads.add(row.thread_id)
        unread_count = (
            MessageModel.query.filter_by(
                thread_id=row.thread_id,
                receiver_id=int(current_user.id),
                is_read=False,
            ).count()
        )
        conversations.append(
            {
                "thread_id": row.thread_id,
                "other_user": other_user,
                "last_message": row,
                "unread_count": int(unread_count),
            }
        )
        if len(conversations) >= max(1, int(limit)):
            break
    return conversations


def fetch_thread_messages(MessageModel: Any, UserModel: Any, current_user: Any, *, other_user_id: int, limit: int = 200) -> tuple[Any, list[Any]]:
    other_user = UserModel.query.get(int(other_user_id))
    if other_user is None or not can_message_user(current_user, other_user):
        raise ValueError("Messaging is not allowed for this user.")
    thread_id = thread_id_for_users(current_user.id, other_user.id)
    rows = (
        MessageModel.query.filter_by(thread_id=thread_id)
        .order_by(MessageModel.created_at.asc())
        .limit(max(1, int(limit)))
        .all()
    )
    return other_user, rows


def send_message(
    session: Any,
    MessageModel: Any,
    UserModel: Any,
    current_user: Any,
    *,
    receiver_id: int,
    body: str,
) -> Any:
    other_user = UserModel.query.get(int(receiver_id))
    text = str(body or "").strip()
    if not text:
        raise ValueError("Message cannot be empty.")
    if other_user is None or not can_message_user(current_user, other_user):
        raise ValueError("Messaging is not allowed for this user.")
    row = MessageModel(
        thread_id=thread_id_for_users(current_user.id, other_user.id),
        sender_id=int(current_user.id),
        receiver_id=int(other_user.id),
        message=text,
        created_at=datetime.utcnow(),
        is_read=False,
    )
    session.add(row)
    return row


def mark_thread_read(
    session: Any,
    MessageModel: Any,
    UserModel: Any,
    current_user: Any,
    *,
    other_user_id: int,
) -> int:
    other_user = UserModel.query.get(int(other_user_id))
    if other_user is None or not can_message_user(current_user, other_user):
        return 0
    thread_id = thread_id_for_users(current_user.id, other_user.id)
    rows = MessageModel.query.filter_by(
        thread_id=thread_id,
        receiver_id=int(current_user.id),
        is_read=False,
    ).all()
    for row in rows:
        row.is_read = True
    return len(rows)


def unread_count(MessageModel: Any, current_user: Any) -> int:
    return int(
        MessageModel.query.filter_by(receiver_id=int(current_user.id), is_read=False).count()
    )
