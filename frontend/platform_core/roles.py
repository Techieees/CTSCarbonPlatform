"""User role normalization (no app.py dependency)."""

from __future__ import annotations

USER_ROLES: tuple[str, ...] = ("owner", "super_admin", "admin", "manager", "auditor", "user")
USER_ROLES_SET = frozenset(USER_ROLES)
ROLES_WITH_ADMIN_ACCESS = frozenset({"owner", "super_admin", "admin", "manager"})


def normalize_user_role(raw: str | None) -> str:
    r = (raw or "user").strip().lower()
    return r if r in USER_ROLES_SET else "user"


def is_owner_user(user: object | None) -> bool:
    return normalize_user_role(getattr(user, "role", None)) == "owner"


def is_auditor_user(user: object | None) -> bool:
    return normalize_user_role(getattr(user, "role", None)) == "auditor"


def is_readonly_user(user: object | None) -> bool:
    return is_auditor_user(user)
