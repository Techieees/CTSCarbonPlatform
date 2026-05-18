"""SQLite schema upgrades for sustainability methodology tables."""

from __future__ import annotations


def ensure_sustainability_schema() -> None:
    from sqlalchemy import inspect, text

    from frontend.extensions import db

    db.create_all()
    inspector = inspect(db.engine)

    def add_column(table: str, column: str, ddl: str) -> None:
        if not inspector.has_table(table):
            return
        cols = {c["name"] for c in inspector.get_columns(table)}
        if column not in cols:
            db.session.execute(text(f"ALTER TABLE {table} ADD COLUMN {ddl}"))
            db.session.commit()

    add_column("eol_scenarios", "company_key", "company_key VARCHAR(120)")
    add_column("eol_scenarios", "methodology_type", "methodology_type VARCHAR(40) DEFAULT 'disposal_ratios'")
    add_column("eol_component_profiles", "waste_kg_literal", "waste_kg_literal FLOAT")
    add_column("business_questionnaire_answers", "business_function", "business_function VARCHAR(120)")
