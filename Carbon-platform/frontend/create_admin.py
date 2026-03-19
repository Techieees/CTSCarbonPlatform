#!/usr/bin/env python3
"""
Create an admin user
"""

import argparse

from app import app, db, User
from werkzeug.security import generate_password_hash

def create_admin_user(email, password, company_name="Admin"):
    """Create an admin user"""
    with app.app_context():
        #  Create a database
        db.create_all()
        
        #  Check if the user already exists
        existing_user = User.query.filter_by(email=email).first()
        if existing_user:
            existing_user.password_hash = generate_password_hash(password)
            existing_user.company_name = company_name
            existing_user.is_admin = True
            db.session.commit()
            print("Existing user updated successfully!")
            print(f"Email: {email}")
            print(f"Company: {company_name}")
            print("Admin role: Yes")
            return
        
        #  Create a new admin user
        admin_user = User(
            email=email,
            password_hash=generate_password_hash(password),
            company_name=company_name,
            is_admin=True
        )
        
        db.session.add(admin_user)
        db.session.commit()
        
        print(f"Admin user created successfully!")
        print(f"Email: {email}")
        print(f"Company: {company_name}")
        print(f"Admin role: Yes")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Create or reset an admin user.")
    parser.add_argument("--email", help="Admin email address")
    parser.add_argument("--password", help="Admin password (will be hashed)")
    parser.add_argument("--company", default="Admin", help="Company name (default: Admin)")
    args = parser.parse_args()

    print("GHG Data Collection System - Admin User Creation")
    print("=" * 60)

    email = args.email or input("Admin email address: ").strip()
    password = args.password or input("Admin password: ").strip()
    company = (args.company or "Admin").strip() or "Admin"

    if not email or not password:
        print("Email and password are required!")
        raise SystemExit(1)

    create_admin_user(email, password, company)