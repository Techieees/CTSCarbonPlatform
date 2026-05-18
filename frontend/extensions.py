"""Shared Flask extensions (import-safe; no app factory side effects)."""

from flask_sqlalchemy import SQLAlchemy

db = SQLAlchemy()
