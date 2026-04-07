#!/usr/bin/env python3
"""
Create or update a user with a chosen role (default: admin).
Roles: owner, super_admin, admin, manager, user
"""

import argparse

from app import (
    USER_ROLES,
    app,
    db,
    normalize_user_role,
    sync_user_admin_flag,
    User,
    _ensure_db_tables,
)
from werkzeug.security import generate_password_hash


def create_admin_user(email: str, password: str, company_name: str = "Admin", role: str = "admin") -> None:
    """Create or update a user; sets role and syncs is_admin."""
    role_norm = normalize_user_role(role)
    if role_norm not in USER_ROLES:
        raise ValueError(f"Invalid role. Choose one of: {', '.join(USER_ROLES)}")

    with app.app_context():
        db.create_all()
        _ensure_db_tables()

        email_norm = (email or "").strip().lower()
        existing_user = User.query.filter(db.func.lower(User.email) == email_norm).first()
        if existing_user:
            existing_user.password_hash = generate_password_hash(password)
            existing_user.company_name = company_name
            existing_user.is_profile_complete = True
            existing_user.role = role_norm
            sync_user_admin_flag(existing_user)
            if not (existing_user.first_name or "").strip():
                existing_user.first_name = "Admin"
            if not (existing_user.last_name or "").strip():
                existing_user.last_name = "User"
            db.session.commit()
            print("Existing user updated successfully!")
            print(f"Email: {email_norm}")
            print(f"Company: {company_name}")
            print(f"Role: {existing_user.role}")
            print(f"Admin access (is_admin): {existing_user.is_admin}")
            return

        admin_user = User(
            email=email_norm,
            password_hash=generate_password_hash(password),
            company_name=company_name,
            is_profile_complete=True,
            first_name="Admin",
            last_name="User",
            role=role_norm,
        )
        sync_user_admin_flag(admin_user)

        db.session.add(admin_user)
        db.session.commit()

        print("User created successfully!")
        print(f"Email: {email_norm}")
        print(f"Company: {company_name}")
        print(f"Role: {admin_user.role}")
        print(f"Admin access (is_admin): {admin_user.is_admin}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Create or update a user with a role.")
    parser.add_argument("--email", help="User email address")
    parser.add_argument("--password", help="Password (will be hashed)")
    parser.add_argument("--company", default="Admin", help="Company name (default: Admin)")
    parser.add_argument(
        "--role",
        default="",
        help=f"Role (default: admin). One of: {', '.join(USER_ROLES)}",
    )
    args = parser.parse_args()

    print("CTS Carbon Platform — user creation")
    print("=" * 60)

    email = (args.email or input("email: ")).strip()
    password = args.password or input("password: ").strip()
    company = (args.company or "Admin").strip() or "Admin"
    role_raw = args.role if args.role else input("role: ").strip()

    if not email or not password:
        print("Email and password are required!")
        raise SystemExit(1)

    role = role_raw if role_raw else "admin"

    try:
        create_admin_user(email, password, company, role=role)
    except ValueError as e:
        print(str(e))
        raise SystemExit(2)
